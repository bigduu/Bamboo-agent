//! Test support for the actual Schedule and Connect new-run resolver builders.
//!
//! This exercises those returned closures with managed registry generations and
//! loopback providers, not a complete session, approval, or memory-recall run.

use std::sync::Arc;
use std::time::Duration;

use bamboo_agent_core::Message;
use bamboo_engine::AuxiliaryModelConfig;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};
use futures::StreamExt;
use serde_json::json;
use tokio::sync::RwLock;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

type Resolver = Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>;
const LIMIT: Duration = Duration::from_secs(5);

pub(crate) struct Fixture {
    pub(crate) config: Arc<RwLock<Config>>,
    pub(crate) registry: Arc<ProviderRegistry>,
    home: tempfile::TempDir,
    server_a: MockServer,
    server_b: MockServer,
    initial: Config,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let server_a = tokio::time::timeout(LIMIT, MockServer::start())
            .await
            .expect("start synthetic account A");
        let server_b = tokio::time::timeout(LIMIT, MockServer::start())
            .await
            .expect("start synthetic account B");
        for server in [&server_a, &server_b] {
            tokio::time::timeout(
                LIMIT,
                Mock::given(method("POST"))
                    .and(path("/chat/completions"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "text/event-stream")
                            .set_body_string(concat!(
                                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\n",
                                "data: [DONE]\n\n",
                            )),
                    )
                    .mount(server),
            )
            .await
            .expect("mount synthetic provider");
        }

        let home = tempfile::tempdir().expect("isolated data directory");
        let mut initial = Config::default();
        initial.provider = "account-a".into();
        initial.default_provider_instance = Some("account-a".into());
        initial.defaults = None;
        // Non-loopback destinations fail closed; these synthetic providers use
        // the client's existing loopback no-proxy exception.
        initial.http_proxy = "http://127.0.0.1:9".into();
        initial.https_proxy.clear();
        initial.proxy_auth = None;
        initial
            .memory_mut()
            .get_or_insert_with(Default::default)
            .background_model = Some("memory-only".into());
        for (name, server, model, fast) in [
            ("account-a", &server_a, "chat-a", "fast-a"),
            ("account-b", &server_b, "chat-b", "fast-b"),
        ] {
            initial.provider_instances.insert(
                name.into(),
                serde_json::from_value(json!({
                    "provider_type": "openai", "enabled": true,
                    "api_key": "synthetic-only", "base_url": server.uri(),
                    "model": model, "fast_model": fast,
                }))
                .expect("synthetic provider config"),
            );
        }
        let registry = Arc::new(
            tokio::time::timeout(
                LIMIT,
                ProviderRegistry::from_config(&initial, home.path().to_path_buf()),
            )
            .await
            .expect("initial registry timeout")
            .expect("initial managed registry"),
        );
        Self {
            config: Arc::new(RwLock::new(initial.clone())),
            registry,
            home,
            server_a,
            server_b,
            initial,
        }
    }

    // Appended to the shared test fixture's impl Fixture by the generator.
    // The resolver comes from the Connect approval-resume constructor. This does
    // not launch an approval session or prove the per-execution Agent override.
    pub(crate) async fn verify_resume_background(
        self,
        resolver: Arc<dyn Fn() -> Option<bamboo_llm::ResolvedModel> + Send + Sync>,
    ) {
        let initial = resolver();
        assert_eq!(
            initial.as_ref().map(|target| target.model_name.as_str()),
            Some("memory-only")
        );
        assert!(initial.as_ref().is_some_and(|target| {
            self.registry
                .get("account-b")
                .is_some_and(|provider| Arc::ptr_eq(&target.provider, &provider))
        }));
        let initial_call = dispatch(background_target(initial)).await;
        let initial_counts = self.counts().await;

        // The session is bound to B while the global default remains A. An
        // explicit allowlist for B excludes the legacy memory model.
        let mut denied = self.initial.clone();
        denied
            .provider_instances
            .get_mut("account-b")
            .unwrap()
            .extra
            .insert("runtime_models".into(), json!(["chat-b", "fast-b"]));
        let denied_generation = self.generation(&denied).await;
        let mut guard = tokio::time::timeout(LIMIT, self.config.write())
            .await
            .expect("config write timeout");
        *guard = denied.clone();
        let task_resolver = resolver.clone();
        let mut task = tokio::task::spawn_blocking(move || task_resolver());
        let blocked_result = tokio::time::timeout(LIMIT, &mut task).await;
        self.registry.replace_with(denied_generation);
        drop(guard);
        let blocked = match blocked_result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => panic!("lock-busy resolver task failed: {error}"),
            Err(_) => {
                let _ = tokio::time::timeout(LIMIT, task).await;
                panic!("lock-busy resolver blocked instead of failing closed");
            }
        };
        let blocked = background_target(blocked);
        let blocked_pair = pair(&blocked);
        let blocked_call = dispatch(blocked).await;
        let blocked_counts = self.counts().await;

        let rejected = background_target(resolver());
        let rejected_pair = pair(&rejected);
        let rejected_call = dispatch(rejected).await;
        let rejected_counts = self.counts().await;

        let mut allowed = denied;
        allowed
            .provider_instances
            .get_mut("account-b")
            .unwrap()
            .extra
            .insert(
                "runtime_models".into(),
                json!(["chat-b", "fast-b", "memory-only"]),
            );
        let generation = self.generation(&allowed).await;
        self.publish(allowed.clone(), generation).await;
        let restored = background_target(resolver());
        let restored_pair = pair(&restored);
        let restored_b = same_provider(
            &restored.background_model_provider,
            &self.registry.get("account-b"),
        );
        let restored_call = dispatch(restored).await;
        let restored_counts = self.counts().await;

        let mut revoked = allowed;
        revoked
            .provider_instances
            .get_mut("account-b")
            .unwrap()
            .enabled = false;
        let generation = self.generation(&revoked).await;
        self.publish(revoked, generation).await;
        let disabled = background_target(resolver());
        let disabled_pair = pair(&disabled);
        let disabled_call = dispatch(disabled).await;
        let disabled_counts = self.counts().await;

        assert_eq!((initial_call, initial_counts), (Ok(true), (0, 1)));
        assert_eq!(
            (blocked_pair, blocked_call, blocked_counts),
            ((None, false), Ok(false), (0, 1)),
        );
        assert_eq!(
            (rejected_pair, rejected_call, rejected_counts),
            ((None, false), Ok(false), (0, 1)),
        );
        assert_eq!(
            (restored_pair, restored_b, restored_call, restored_counts),
            ((Some("memory-only".into()), true), true, Ok(true), (0, 2)),
        );
        assert_eq!(
            (disabled_pair, disabled_call, disabled_counts),
            ((None, false), Ok(false), (0, 2)),
        );
        let requests = tokio::time::timeout(LIMIT, self.server_b.received_requests())
            .await
            .expect("model observation timeout")
            .expect("account B requests");
        for request in requests {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["model"], "memory-only");
        }
    }

    async fn generation(&self, config: &Config) -> ProviderRegistry {
        tokio::time::timeout(
            LIMIT,
            ProviderRegistry::from_config(config, self.home.path().to_path_buf()),
        )
        .await
        .expect("registry generation timeout")
        .expect("managed registry generation")
    }

    async fn publish(&self, config: Config, generation: ProviderRegistry) {
        let mut guard = tokio::time::timeout(LIMIT, self.config.write())
            .await
            .expect("config publication lock timeout");
        *guard = config;
        self.registry.replace_with(generation);
    }

    async fn counts(&self) -> (usize, usize) {
        let a = tokio::time::timeout(LIMIT, self.server_a.received_requests())
            .await
            .expect("account A observation timeout")
            .expect("account A requests");
        let b = tokio::time::timeout(LIMIT, self.server_b.received_requests())
            .await
            .expect("account B observation timeout")
            .expect("account B requests");
        (a.len(), b.len())
    }

    /// Check the legacy background adapter, whose other roles stay fixed.
    pub(crate) async fn verify(self, resolver: Resolver) {
        self.verify_background(resolver, false).await;
    }

    /// Check a full live auxiliary resolver through the same background matrix.
    pub(crate) async fn verify_live(self, resolver: Resolver) {
        self.verify_background(resolver, true).await;
    }

    async fn verify_background(self, resolver: Resolver, all_roles_live: bool) {
        let initial = resolver();
        assert_eq!(initial.fast_model_name.as_deref(), Some("fast-a"));
        assert_eq!(initial.summarization_model_name.as_deref(), Some("fast-a"));
        assert_eq!(
            initial.background_model_name.as_deref(),
            Some("memory-only")
        );
        assert!(same_provider(
            &initial.background_model_provider,
            &self.registry.get("account-a")
        ));
        let initial_call = dispatch(initial.clone()).await;
        let initial_counts = self.counts().await;

        let mut denied = self.initial.clone();
        // Changing the global default must not switch the existing run to B.
        denied.default_provider_instance = Some("account-b".into());
        denied
            .provider_instances
            .get_mut("account-a")
            .unwrap()
            .extra
            .insert("runtime_models".into(), json!(["chat-a", "fast-a"]));
        let denied_generation = self.generation(&denied).await;
        let mut guard = tokio::time::timeout(LIMIT, self.config.write())
            .await
            .expect("config write timeout");
        *guard = denied.clone();
        // If an implementation accidentally blocks instead of try_read, never
        // leave the write guard held when timing out or asserting.
        let task_resolver = resolver.clone();
        let mut task = tokio::task::spawn_blocking(move || task_resolver());
        let blocked_result = tokio::time::timeout(LIMIT, &mut task).await;
        self.registry.replace_with(denied_generation);
        drop(guard);
        let blocked = match blocked_result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => panic!("lock-busy resolver task failed: {error}"),
            Err(_) => {
                let _ = tokio::time::timeout(LIMIT, task).await;
                panic!("lock-busy resolver blocked instead of failing closed");
            }
        };
        let blocked_pair = pair(&blocked);
        let blocked_other = same_other_roles(&initial, &blocked);
        if all_roles_live {
            assert!(blocked.summarization_model_provider.is_none());
            assert!(blocked.summarization_model_error.is_some());
            let provider = blocked
                .fast_model_provider
                .as_ref()
                .expect("busy Fast must reject");
            let result = tokio::time::timeout(
                LIMIT,
                provider.chat_stream(
                    &[Message::user("synthetic busy Fast")],
                    &[],
                    Some(32),
                    blocked.fast_model_name.as_deref().unwrap_or("fast-a"),
                ),
            )
            .await
            .expect("busy Fast should fail promptly");
            assert!(matches!(result, Err(bamboo_llm::LLMError::Api(_))));
        }
        let blocked_call = dispatch(blocked).await;
        let blocked_counts = self.counts().await;

        let rejected = resolver();
        let rejected_pair = pair(&rejected);
        let rejected_other = same_other_roles(&initial, &rejected);
        if all_roles_live {
            assert_eq!(rejected.fast_model_name.as_deref(), Some("fast-a"));
            assert_eq!(rejected.summarization_model_name.as_deref(), Some("fast-a"));
        }
        let rejected_call = dispatch(rejected).await;
        let rejected_counts = self.counts().await;

        let mut allowed = denied.clone();
        allowed
            .provider_instances
            .get_mut("account-a")
            .unwrap()
            .extra
            .insert(
                "runtime_models".into(),
                json!(["chat-a", "fast-a", "memory-only"]),
            );
        let generation = self.generation(&allowed).await;
        self.publish(allowed.clone(), generation).await;
        let restored = resolver();
        let restored_pair = pair(&restored);
        let restored_a = same_provider(
            &restored.background_model_provider,
            &self.registry.get("account-a"),
        );
        let restored_other = same_other_roles(&initial, &restored);
        let restored_call = dispatch(restored).await;
        let restored_counts = self.counts().await;

        let mut revoked = allowed;
        revoked
            .provider_instances
            .get_mut("account-a")
            .unwrap()
            .enabled = false;
        let generation = self.generation(&revoked).await;
        self.publish(revoked, generation).await;
        let disabled = resolver();
        let disabled_pair = pair(&disabled);
        let disabled_other = same_other_roles(&initial, &disabled);
        if all_roles_live {
            assert!(disabled.summarization_model_provider.is_none());
            assert!(disabled.summarization_model_error.is_some());
            assert!(disabled.fast_model_provider.is_some());
        }
        let disabled_call = dispatch(disabled).await;
        let disabled_counts = self.counts().await;

        assert_eq!(initial_call, Ok(true));
        assert_eq!(initial_counts, (1, 0));
        assert_eq!(
            (blocked_pair, blocked_call, blocked_counts),
            ((None, false), Ok(false), (1, 0))
        );
        assert_eq!(
            (rejected_pair, rejected_call, rejected_counts),
            ((None, false), Ok(false), (1, 0))
        );
        assert_eq!(
            (restored_pair, restored_a, restored_call, restored_counts),
            ((Some("memory-only".into()), true), true, Ok(true), (2, 0)),
        );
        assert_eq!(
            (disabled_pair, disabled_call, disabled_counts),
            ((None, false), Ok(false), (2, 0))
        );
        if !all_roles_live {
            assert!(blocked_other && rejected_other && restored_other && disabled_other);
        }

        let requests = tokio::time::timeout(LIMIT, self.server_a.received_requests())
            .await
            .expect("model observation timeout")
            .expect("account A requests");
        for request in requests {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["model"], "memory-only");
        }
    }
}

// A dispatch adapter for the optional complete target; this mirrors pair
// consumption, and does not reproduce the Agent's round/summary behavior.
fn background_target(target: Option<bamboo_llm::ResolvedModel>) -> AuxiliaryModelConfig {
    let mut result = AuxiliaryModelConfig::default();
    if let Some(target) = target {
        result.background_model_name = Some(target.model_name);
        result.background_model_provider = Some(target.provider);
    }
    result
}

fn pair(config: &AuxiliaryModelConfig) -> (Option<String>, bool) {
    (
        config.background_model_name.clone(),
        config.background_model_provider.is_some(),
    )
}

fn same_provider(a: &Option<Arc<dyn LLMProvider>>, b: &Option<Arc<dyn LLMProvider>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

fn same_other_roles(a: &AuxiliaryModelConfig, b: &AuxiliaryModelConfig) -> bool {
    a.fast_model_name == b.fast_model_name
        && same_provider(&a.fast_model_provider, &b.fast_model_provider)
        && a.planning_model_name == b.planning_model_name
        && a.search_model_name == b.search_model_name
        && a.summarization_model_name == b.summarization_model_name
        && same_provider(
            &a.summarization_model_provider,
            &b.summarization_model_provider,
        )
        && a.summarization_model_error == b.summarization_model_error
}

async fn dispatch(config: AuxiliaryModelConfig) -> Result<bool, String> {
    match (
        config.background_model_name,
        config.background_model_provider,
    ) {
        (None, None) => Ok(false),
        (Some(model), Some(provider)) => tokio::time::timeout(LIMIT, async move {
            let mut stream = provider
                .chat_stream(
                    &[Message::user("synthetic memory query")],
                    &[],
                    Some(32),
                    &model,
                )
                .await
                .map_err(|error| error.to_string())?;
            while let Some(chunk) = stream.next().await {
                chunk.map_err(|error| error.to_string())?;
            }
            Ok(true)
        })
        .await
        .map_err(|_| "synthetic dispatch timed out".to_string())?,
        _ => Err("background name and provider were not paired".into()),
    }
}
