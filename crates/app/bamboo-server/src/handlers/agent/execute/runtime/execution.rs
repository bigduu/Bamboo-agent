//! Agent execution spawner — thin adapter over the runtime crate.
//!
//! Converts server-side `SpawnAgentExecution` args into the crate-agnostic
//! `SessionExecutionArgs` and delegates to
//! `bamboo_engine::execution::spawn_session_execution`.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock as StdRwLock};

use tokio::sync::mpsc;

use crate::app_state::AppState;
use crate::permission_audit::record_bamboo_runtime_permission_metadata;
use crate::tools::ToolSurface;
use bamboo_engine::session_app::provider_model::session_effective_model_ref;

use bamboo_engine::config::GoldConfig;
use bamboo_engine::execution::agent_spawn::{SessionExecutionArgs, SessionExecutionReservation};
use bamboo_engine::{
    AuxiliaryModelConfig, DisabledFilterResolver, ImageFallbackConfig, ModelRoster,
};
use bamboo_llm::{Config, LLMProvider};

use super::session_state;
#[path = "gold_route.rs"]
mod gold_route;

pub(crate) struct SpawnAgentExecution {
    pub(crate) state: actix_web::web::Data<AppState>,
    pub(crate) session_id: String,
    pub(crate) session: bamboo_agent_core::Session,
    pub(crate) execution_reservation: SessionExecutionReservation,
    pub(crate) is_child_session: bool,
    /// Provider routing key for the primary model. Mirrors
    /// `model_roster.provider_name` but kept as a required `String` because the
    /// auxiliary-model resolver is keyed on it; the value is also threaded into
    /// the roster.
    pub(crate) provider_name: String,
    pub(crate) provider_override: Option<Arc<dyn LLMProvider>>,
    /// Opaque managed-origin proof captured with the selected provider handle.
    pub(crate) provider_override_managed_registry:
        Option<Arc<bamboo_llm::provider_registry::ManagedProviderOrigin>>,
    /// Cohesive primary + auxiliary model/provider selection.
    pub(crate) model_roster: ModelRoster,
    pub(crate) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(crate) reasoning_effort_source: String,
    pub(crate) disabled_tools: BTreeSet<String>,
    pub(crate) disabled_skill_ids: BTreeSet<String>,
    pub(crate) mpsc_tx: mpsc::Sender<bamboo_agent_core::AgentEvent>,
    pub(crate) history_commit_barrier: bamboo_engine::execution::HistoryCommitBarrier,
    pub(crate) image_fallback: Option<ImageFallbackConfig>,
    pub(crate) gold_config: Option<GoldConfig>,
    pub(crate) app_data_dir: Option<std::path::PathBuf>,
    /// Optional per-run resource guardrail override from the `POST /execute`
    /// request body (issue #221). `None` uses the config-level default.
    pub(crate) run_budget: Option<bamboo_config::RunBudgetConfig>,
}

pub(super) fn execution_tool_surface(is_child_session: bool) -> ToolSurface {
    if is_child_session {
        ToolSurface::Child
    } else {
        ToolSurface::Root
    }
}

pub(super) fn tools_for_execution(
    state: &AppState,
    is_child_session: bool,
) -> Arc<dyn bamboo_agent_core::tools::ToolExecutor> {
    state.tools_for(execution_tool_surface(is_child_session))
}

fn read_config_snapshot(
    config: &Arc<tokio::sync::RwLock<Config>>,
    cached_config: &StdRwLock<Config>,
) -> Config {
    if let Ok(config_guard) = config.try_read() {
        let snapshot = config_guard.clone();

        if let Ok(mut cached_guard) = cached_config.try_write() {
            *cached_guard = snapshot.clone();
        }

        snapshot
    } else {
        cached_config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

pub(crate) fn make_auxiliary_model_resolver(
    state: &actix_web::web::Data<AppState>,
    provider_name: &str,
) -> Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync> {
    bamboo_engine::model_areas::live_auxiliary_model_resolver(
        state.config.clone(),
        state.provider_registry.clone(),
        provider_name.to_string(),
    )
}

/// Build the per-round resolver for the LIVE disabled tool/skill sets (#136).
/// Returns the CURRENT `(disabled_tools, disabled_skill_ids)` from the live global
/// config each call, so disabling/re-enabling a tool mid-run takes effect on the
/// next round. Reuses `read_config_snapshot` (non-blocking `try_read` + cached
/// fallback). NB: returns the live GLOBAL set (not unioned with the request-time
/// snapshot), because that snapshot IS just the old global set — unioning it would
/// freeze it as a floor and break re-enable.
pub(crate) fn make_disabled_filter_resolver(
    state: &actix_web::web::Data<AppState>,
) -> DisabledFilterResolver {
    let config = state.config.clone();
    let cached_config = Arc::new(StdRwLock::new(
        config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default(),
    ));
    Arc::new(move || {
        let config_snapshot = read_config_snapshot(&config, cached_config.as_ref());
        (
            config_snapshot
                .disabled_tool_references()
                .into_iter()
                .collect(),
            config_snapshot.disabled_skill_ids().into_iter().collect(),
        )
    })
}

#[cfg(test)]
pub(crate) fn spawn_agent_execution(args: SpawnAgentExecution) {
    args.spawn_with_inputs(None);
}

impl SpawnAgentExecution {
    /// Separate untrusted transport parameter; the existing argument layout and
    /// ordinary spawner remain compatible and default to absence.
    #[cfg(test)]
    pub(crate) fn spawn_with_inputs(
        self,
        inputs: Option<bamboo_engine::config::UntrustedExecutionInputs>,
    ) {
        spawn_agent_execution_with_inputs(self, inputs, None, Default::default(), false, false);
    }

    pub(crate) fn spawn_with_native_inputs(
        self,
        inputs: Option<bamboo_engine::config::UntrustedExecutionInputs>,
        native_binding: Option<crate::skill_runtime::NativeExecutionBinding>,
    ) {
        spawn_agent_execution_with_inputs(
            self,
            inputs,
            native_binding,
            Default::default(),
            false,
            false,
        );
    }

    pub(crate) fn spawn_with_native_inputs_and_retry(
        self,
        inputs: Option<bamboo_engine::config::UntrustedExecutionInputs>,
        native_binding: Option<crate::skill_runtime::NativeExecutionBinding>,
        upstream_retry: bamboo_llm::retry::UpstreamRetry,
        live_bootstrap_resume: bool,
    ) {
        spawn_agent_execution_with_inputs(
            self,
            inputs,
            native_binding,
            upstream_retry,
            live_bootstrap_resume,
            true,
        );
    }
}

fn spawn_agent_execution_with_inputs(
    mut args: SpawnAgentExecution,
    inputs: Option<bamboo_engine::config::UntrustedExecutionInputs>,
    native_binding: Option<crate::skill_runtime::NativeExecutionBinding>,
    upstream_retry: bamboo_llm::retry::UpstreamRetry,
    live_bootstrap_resume: bool,
    image_fallback_from_config: bool,
) {
    let session_model_ref = session_effective_model_ref(&args.session);
    let provider_override = match (session_model_ref.as_ref(), args.provider_override.take()) {
        (Some(_), Some(provider)) => Some(provider),
        (Some(model_ref), None) => {
            tracing::error!(
                session_id = %args.session_id,
                provider = %model_ref.provider,
                model = %model_ref.model,
                "session has an explicit provider target without a pre-resolved provider; refusing to fall back"
            );
            return;
        }
        (None, provider) => provider,
    };

    if let Some(config) = args.state.permission_checker.permission_config() {
        config.set_session_workspace(args.session_id.clone(), args.session.workspace.clone());
        if let Err(error) =
            record_bamboo_runtime_permission_metadata(&mut args.session, config.as_ref())
        {
            tracing::error!(%error, session_id = %args.session_id, "agent permission audit failed closed");
            return;
        }
    }
    let live_retry = (live_bootstrap_resume && !args.is_child_session)
        .then(|| {
            crate::app_state::live_retry::bind(
                args.state.as_ref(),
                &args.execution_reservation,
                &args.session,
            )
        })
        .flatten();
    let (agent, tools) = native_binding.unwrap_or_else(|| {
        (
            args.state.agent.clone(),
            tools_for_execution(args.state.as_ref(), args.is_child_session),
        )
    });
    let agent = if image_fallback_from_config {
        Arc::new(agent.with_config_derived_image_fallback())
    } else {
        agent
    };
    let gold_target_resolver = gold_route::make_gold_target_resolver(
        args.state.as_ref(),
        &args.provider_name,
        provider_override.as_ref(),
        args.provider_override_managed_registry.as_ref(),
    );
    let agent = if let Some(resolver) = gold_target_resolver {
        Arc::new(agent.with_gold_target_resolver(resolver))
    } else {
        agent
    };
    let agent = match live_retry {
        Some(host) => Arc::new(agent.with_live_bootstrap_host(host)),
        None => agent,
    };
    let tools_override = Some(tools);

    let selected_skill_ids = session_state::selected_skill_ids_for_session(&args.session);
    let selected_skill_mode = session_state::selected_skill_mode_for_session(&args.session);
    let auxiliary_model_resolver = make_auxiliary_model_resolver(&args.state, &args.provider_name);

    // The resolved provider name is the authoritative routing key; thread it into
    // the roster so it matches the value the old `provider_name` field carried
    // (and the resolver above is keyed on).
    let mut model_roster = args.model_roster;
    model_roster.provider_name = Some(args.provider_name);

    #[cfg(test)]
    crate::handlers::agent::execute::tests::observe_inputs(&args.session_id, inputs.as_ref());

    bamboo_engine::execution::agent_spawn::spawn_session_execution_with_upstream_retry(
        SessionExecutionArgs {
            agent,
            session_id: args.session_id,
            session: args.session,
            execution_reservation: args.execution_reservation,
            tools_override,
            provider_override,
            model_roster,
            reasoning_effort: args.reasoning_effort,
            reasoning_effort_source: args.reasoning_effort_source,
            auxiliary_model_resolver: Some(auxiliary_model_resolver),
            // Live per-round disabled-set resolver (#136): a tool disabled/re-enabled
            // mid-run takes effect on the next round of this (long-running) agent loop.
            disabled_filter_resolver: Some(make_disabled_filter_resolver(&args.state)),
            disabled_tools: Some(args.disabled_tools),
            disabled_skill_ids: Some(args.disabled_skill_ids),
            selected_skill_ids,
            selected_skill_mode,
            mpsc_tx: args.mpsc_tx,
            history_commit_barrier: args.history_commit_barrier,
            image_fallback: args.image_fallback,
            gold_config: args.gold_config,
            // The guardian reviewer spawner is always available; the terminal gate
            // stays inert until `guardian_config` is enabled. (TODO: surface a
            // guardian config on the request, mirroring `gold_config`.)
            guardian_config: None,
            guardian_spawner: Some(args.state.guardian_spawner.clone()),
            bash_resume_hook: Some(args.state.bash_resume_hook.clone()),
            // The completion coordinator also implements `BashCompletionSink`: a
            // finished background shell pushes its result into this loop (injected at
            // the next round boundary, issue #84 Phase 2b follow-up).
            bash_completion_sink: Some(args.state.child_completion_coordinator.clone()),
            app_data_dir: args.app_data_dir,
            run_budget: args.run_budget,
            runners: args.state.agent_runners.clone(),
            sessions_cache: args.state.sessions.clone(),
            on_complete: None,
            // Resumed/child sessions finishing on this path wake their waiting
            // parent through the completion coordinator (issue #546); the
            // publish is gated on kind=Child inside `spawn_session_execution`.
            child_completion_handler: Some(args.state.child_completion_coordinator.clone()),
        },
        inputs,
        upstream_retry,
    );
}

#[cfg(test)]
mod tests {
    use super::read_config_snapshot;
    use bamboo_llm::Config;
    use std::sync::{Arc, RwLock as StdRwLock};

    // Real Host resolver with a config-managed registry and two synthetic
    // accounts. Only Fast uses the loopback HTTP peer; the main provider is
    // an in-process counter that reveals an accidental fallback.
    #[tokio::test]
    async fn host_fast_resolver_rejects_stale_config_and_busy_lock() {
        use super::make_auxiliary_model_resolver;
        use crate::app_state::AppState;
        use actix_web::web::Data;
        use bamboo_agent_core::{Message, ToolSchema};
        use bamboo_engine::AuxiliaryModelConfig;
        use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream};
        use futures::StreamExt;
        use serde_json::json;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        #[derive(Default)]
        struct MainCounter(AtomicUsize);

        #[async_trait::async_trait]
        impl LLMProvider for MainCounter {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> Result<LLMStream, LLMError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(LLMError::Api("synthetic main provider was called".into()))
            }
        }

        // This mirrors the runner's selected-provider and selected-model
        // fallback. It is not a full agent-loop integration test.
        async fn dispatch_selected(
            models: AuxiliaryModelConfig,
            main: Arc<dyn LLMProvider>,
        ) -> Result<(), LLMError> {
            let model = models.fast_model_name.unwrap_or_else(|| "main-a".into());
            let provider = models.fast_model_provider.unwrap_or(main);
            let options = LLMRequestOptions {
                request_purpose: Some("task_evaluation".into()),
                ..Default::default()
            };
            let mut stream = provider
                .chat_stream_with_options(
                    &[Message::user("synthetic Host routing test")],
                    &[],
                    Some(32),
                    &model,
                    Some(&options),
                )
                .await?;
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(())
        }

        let mock = MockServer::start().await;
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
            .mount(&mock)
            .await;

        let temp_dir = tempfile::tempdir().expect("isolated Host data directory");
        let mut config = Config::default();
        config.provider = "chat-a".into();
        config.default_provider_instance = Some("chat-a".into());
        // Force any proxy use to a local dead endpoint; loopback is excluded by
        // Bamboo's HTTP client. All configured provider URLs are local mocks.
        config.http_proxy = "http://127.0.0.1:9".into();
        config.https_proxy.clear();
        config.proxy_auth = None;
        let memory = config.memory_mut().get_or_insert_with(Default::default);
        memory.auto_dream_enabled = false;
        memory.gardener_enabled = false;
        memory.dedup_gardener_enabled = false;
        memory.ledger_gardener_enabled = false;
        memory.ledger_distillation_enabled = false;
        memory.granularity_freshness_gardener_enabled = false;
        config.defaults = Some(
            serde_json::from_value(json!({
                "chat": {"provider": "chat-a", "model": "main-a"},
                "fast": {"provider": "fast-b", "model": "cheap-b"}
            }))
            .unwrap(),
        );
        for (id, model) in [("chat-a", "main-a"), ("fast-b", "cheap-b")] {
            config.provider_instances.insert(
                id.into(),
                serde_json::from_value(json!({
                    "provider_type": "openai",
                    "enabled": true,
                    "api_key": "synthetic-host-only",
                    "base_url": mock.uri(),
                    "model": model,
                    "runtime_models": [model]
                }))
                .unwrap(),
            );
        }

        let primary = Arc::new(MainCounter::default());
        let main: Arc<dyn LLMProvider> = primary.clone();
        let mut app_state =
            AppState::new_with_provider(temp_dir.path().to_path_buf(), config, main.clone())
                .await
                .expect("synthetic Host should initialize");
        app_state.stop_config_watcher_for_test();
        let state = Data::new(app_state);
        let baseline = state.config.read().await.clone();
        let old_fast = state.provider_registry.get("fast-b").expect("managed Fast");
        let resolver = make_auxiliary_model_resolver(&state, "chat-a");
        let mut expected_requests = 0;

        for flag in [None, Some(false), Some(true)] {
            for change in ["disabled", "removed"] {
                let mut current = baseline.clone();
                current.features = serde_json::from_value(match flag {
                    None => json!({}),
                    Some(value) => json!({"provider_model_ref": value}),
                })
                .unwrap();
                *state.config.write().await = current;

                let selected = resolver();
                assert_eq!(selected.fast_model_name.as_deref(), Some("cheap-b"));
                tokio::time::timeout(
                    Duration::from_secs(5),
                    dispatch_selected(selected, main.clone()),
                )
                .await
                .expect("valid Fast should respond promptly")
                .expect("valid Fast should reach the loopback provider");
                expected_requests += 1;
                let requests = mock.received_requests().await.unwrap();
                assert_eq!(requests.len(), expected_requests);
                let body: serde_json::Value =
                    serde_json::from_slice(&requests.last().unwrap().body).unwrap();
                assert_eq!(body["model"], "cheap-b");
                assert_eq!(primary.0.load(Ordering::SeqCst), 0);

                {
                    let mut current = state.config.write().await;
                    match change {
                        "disabled" => {
                            current
                                .provider_instances
                                .get_mut("fast-b")
                                .unwrap()
                                .enabled = false
                        }
                        "removed" => {
                            current.provider_instances.remove("fast-b");
                        }
                        _ => unreachable!(),
                    }
                }
                assert!(Arc::ptr_eq(
                    &old_fast,
                    &state.provider_registry.get("fast-b").unwrap()
                ));
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    dispatch_selected(resolver(), main.clone()),
                )
                .await
                .expect("rejected route should finish promptly");
                assert!(
                    matches!(result, Err(LLMError::Auth(_))),
                    "flag={flag:?}, change={change}"
                );
                assert_eq!(
                    mock.received_requests().await.unwrap().len(),
                    expected_requests
                );
                assert_eq!(primary.0.load(Ordering::SeqCst), 0);
            }
        }

        // Prime the existing cached snapshot with valid config, then hold the
        // live write lock across selection. Drop it before awaiting dispatch.
        *state.config.write().await = baseline;
        assert_eq!(resolver().fast_model_name.as_deref(), Some("cheap-b"));
        let write_guard = state.config.write().await;
        let blocked = resolver();
        drop(write_guard);
        let result = tokio::time::timeout(Duration::from_secs(5), dispatch_selected(blocked, main))
            .await
            .expect("busy config should fail promptly");
        assert!(matches!(result, Err(LLMError::Api(_))));
        assert_eq!(
            mock.received_requests().await.unwrap().len(),
            expected_requests
        );
        assert_eq!(primary.0.load(Ordering::SeqCst), 0);
    }

    // Deterministically pause the normal provider-publication ordering after
    // installing the new Config but before replacing its managed registry.
    // Dispatch mirrors external-memory's requirement for a background name and
    // selected provider; the endpoint and all credentials are synthetic.
    #[tokio::test]
    async fn host_legacy_memory_rejects_cached_route_during_provider_publication() {
        use super::make_auxiliary_model_resolver;
        use crate::app_state::AppState;
        use actix_web::web::Data;
        use bamboo_agent_core::{Message, ToolSchema};
        use bamboo_engine::AuxiliaryModelConfig;
        use bamboo_llm::{LLMError, LLMProvider, LLMStream, ProviderRegistry};
        use futures::StreamExt;
        use serde_json::json;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        #[derive(Default)]
        struct MainCounter(AtomicUsize);
        #[async_trait::async_trait]
        impl LLMProvider for MainCounter {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> Result<LLMStream, LLMError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(LLMError::Api("unexpected main provider call".into()))
            }
        }

        async fn dispatch_background(models: AuxiliaryModelConfig) -> Result<bool, LLMError> {
            match (
                models.background_model_name,
                models.background_model_provider,
            ) {
                (None, None) => Ok(false),
                (Some(model), Some(provider)) => {
                    let mut stream = provider
                        .chat_stream(
                            &[Message::user("synthetic memory query")],
                            &[],
                            Some(32),
                            &model,
                        )
                        .await?;
                    while let Some(chunk) = stream.next().await {
                        chunk?;
                    }
                    Ok(true)
                }
                _ => panic!("background name and provider must remain paired"),
            }
        }

        let mock = MockServer::start().await;
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
            .mount(&mock)
            .await;
        let home = tempfile::tempdir().expect("isolated Host data directory");
        let mut config = Config::default();
        config.provider = "account-a".into();
        config.default_provider_instance = Some("account-a".into());
        config.defaults = None;
        config.http_proxy = "http://127.0.0.1:9".into();
        config.https_proxy.clear();
        config.proxy_auth = None;
        let memory = config.memory_mut().get_or_insert_with(Default::default);
        memory.auto_dream_enabled = false;
        memory.gardener_enabled = false;
        memory.dedup_gardener_enabled = false;
        memory.ledger_gardener_enabled = false;
        memory.ledger_distillation_enabled = false;
        memory.granularity_freshness_gardener_enabled = false;
        memory.background_model = Some("memory-only".into());
        config.provider_instances.insert(
            "account-a".into(),
            serde_json::from_value(json!({
                "provider_type":"openai", "enabled":true,
                "api_key":"synthetic-host-only", "base_url":mock.uri(), "model":"chat-a"
            }))
            .unwrap(),
        );
        let primary = Arc::new(MainCounter::default());
        let main: Arc<dyn LLMProvider> = primary.clone();
        let mut app_state = AppState::new_with_provider(home.path().to_path_buf(), config, main)
            .await
            .expect("synthetic Host should initialize");
        app_state.stop_config_watcher_for_test();
        let state = Data::new(app_state);
        let baseline = state.config.read().await.clone();
        let old_handle = state.provider_registry.get("account-a").unwrap();
        let resolver = make_auxiliary_model_resolver(&state, "account-a");
        let baseline_selection = resolver();
        assert_eq!(
            baseline_selection.background_model_name.as_deref(),
            Some("memory-only")
        );
        assert!(Arc::ptr_eq(
            baseline_selection
                .background_model_provider
                .as_ref()
                .unwrap(),
            &old_handle
        ));

        let mut denied = baseline.clone();
        denied
            .provider_instances
            .get_mut("account-a")
            .unwrap()
            .extra
            .insert("runtime_models".into(), json!(["chat-a"]));
        let denied_registry = ProviderRegistry::from_config(&denied, home.path().to_path_buf())
            .await
            .expect("explicit denial should make a valid generation");
        // Match config_runtime's publication: live config changes while the
        // write guard is held; the registry is replaced before releasing it.
        let mut write_guard = state.config.write().await;
        *write_guard = denied.clone();
        let blocked = resolver();
        let blocked_name = blocked.background_model_name.clone();
        let blocked_has_provider = blocked.background_model_provider.is_some();
        state.provider_registry.replace_with(denied_registry);
        drop(write_guard);
        let blocked_dispatched =
            tokio::time::timeout(Duration::from_secs(5), dispatch_background(blocked))
                .await
                .expect("blocked dispatch should finish")
                .expect("synthetic peer should respond");
        let blocked_requests = mock.received_requests().await.unwrap().len();

        let denied_selection = resolver();
        let denied_name = denied_selection.background_model_name.clone();
        let denied_dispatched = tokio::time::timeout(
            Duration::from_secs(5),
            dispatch_background(denied_selection),
        )
        .await
        .expect("denied dispatch should finish")
        .expect("synthetic peer should respond");
        let denied_requests = mock.received_requests().await.unwrap().len();

        // A subsequent valid explicit allowlist and matching registry restore
        // the same model/account after the publication guard is released.
        let mut allowed = denied;
        allowed
            .provider_instances
            .get_mut("account-a")
            .unwrap()
            .extra
            .insert("runtime_models".into(), json!(["chat-a", "memory-only"]));
        let allowed_registry = ProviderRegistry::from_config(&allowed, home.path().to_path_buf())
            .await
            .expect("allowing generation");
        let mut write_guard = state.config.write().await;
        *write_guard = allowed;
        state.provider_registry.replace_with(allowed_registry);
        drop(write_guard);
        let allowed_selection = resolver();
        let allowed_name = allowed_selection.background_model_name.clone();
        let allowed_handle = state.provider_registry.get("account-a").unwrap();
        let allowed_same_account = allowed_selection
            .background_model_provider
            .as_ref()
            .is_some_and(|provider| Arc::ptr_eq(provider, &allowed_handle));
        let allowed_dispatched = tokio::time::timeout(
            Duration::from_secs(5),
            dispatch_background(allowed_selection),
        )
        .await
        .expect("allowed dispatch should finish")
        .expect("synthetic peer should respond");
        let requests = mock.received_requests().await.unwrap();
        let allowed_requests = requests.len() - denied_requests;
        let final_model = requests.last().map(|request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            body["model"].as_str().unwrap().to_string()
        });

        // One combined assertion preserves all evidence even when the expected
        // RED route dispatches once before the restored positive control.
        assert_eq!(
            (
                (
                    blocked_name,
                    blocked_has_provider,
                    blocked_dispatched,
                    blocked_requests
                ),
                (denied_name, denied_dispatched, denied_requests),
                (
                    allowed_name,
                    allowed_same_account,
                    allowed_dispatched,
                    allowed_requests,
                    final_model
                ),
                primary.0.load(Ordering::SeqCst)
            ),
            (
                (None, false, false, 0),
                (None, false, 0),
                (
                    Some("memory-only".to_string()),
                    true,
                    true,
                    1,
                    Some("memory-only".to_string())
                ),
                0
            ),
        );
    }

    #[test]
    fn read_config_snapshot_refreshes_cached_snapshot_from_live_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let config = Arc::new(tokio::sync::RwLock::new(Config::default()));
            config.write().await.provider = "copilot".to_string();
            let cached_config = StdRwLock::new(Config::default());

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "copilot");
            assert_eq!(
                cached_config.read().expect("cached snapshot lock").provider,
                "copilot"
            );
        });
    }

    #[test]
    fn read_config_snapshot_uses_cached_snapshot_when_live_lock_is_busy() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let mut cached_snapshot = Config::default();
            cached_snapshot.provider = "cached-provider".to_string();

            let config = Arc::new(tokio::sync::RwLock::new(Config::default()));
            let cached_config = StdRwLock::new(cached_snapshot);
            let _write_guard = config.write().await;

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "cached-provider");
        });
    }
}
