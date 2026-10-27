//! Synthetic application context for requests made by real model consumers.
use super::*;
use bamboo_agent_core::{Message, ToolCall, ToolSchema};
use bamboo_llm::{LLMChunk, LLMError, LLMRequestOptions, LLMStream, ProviderModelRouter};
use std::sync::Mutex;

#[derive(Clone, Copy)]
pub(crate) enum Failure {
    Auth,
    Quota,
    Network,
}

#[derive(Default)]
pub(crate) struct CaptureProvider {
    pub(crate) requests: Mutex<Vec<(String, String)>>,
    pub(crate) failure: Mutex<Option<Failure>>,
}

impl CaptureProvider {
    fn respond(
        &self,
        model: &str,
        tools: &[ToolSchema],
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let purpose = options
            .and_then(|options| options.request_purpose.as_deref())
            .unwrap_or("unspecified");
        self.requests
            .lock()
            .unwrap()
            .push((model.into(), purpose.into()));
        if let Some(failure) = *self.failure.lock().unwrap() {
            return Err(match failure {
                Failure::Auth => LLMError::Auth("synthetic authentication refusal".into()),
                Failure::Quota => LLMError::Api("synthetic quota exhausted 429".into()),
                Failure::Network => {
                    LLMError::RequestFailed("synthetic terminal network failure".into())
                }
            });
        }
        let chunk = if purpose == "final_evidence_check" {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: "synthetic-evidence".into(),
                tool_type: "function".into(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: tools[0].function.name.clone(),
                    arguments: serde_json::json!({"verdict":"supported", "corrected_answer":"", "reason":"synthetic routing witness", "evidence_ids":["check-1"]}).to_string(),
                },
            }])
        } else if purpose == "gold_evaluation" {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: "synthetic-gold".into(),
                tool_type: "function".into(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: tools[0].function.name.clone(),
                    arguments: serde_json::json!({"decision":"continue", "confidence":"high", "reasoning":"synthetic routing witness", "missing_information":[], "next_action":"Continue the synthetic task"}).to_string(),
                },
            }])
        } else {
            LLMChunk::Token("Synthetic routing title".into())
        };
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[async_trait]
impl LLMProvider for CaptureProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        tools: &[ToolSchema],
        _max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.respond(model, tools, None)
    }

    async fn chat_stream_with_options(
        &self,
        _messages: &[Message],
        tools: &[ToolSchema],
        _max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.respond(model, tools, options)
    }
}

pub(crate) struct RoutingContext {
    pub(crate) config: Arc<RwLock<Config>>,
    pub(crate) registry: Arc<ProviderRegistry>,
    pub(crate) providers: HashMap<String, Arc<CaptureProvider>>,
    events: broadcast::Sender<AgentEvent>,
}

impl RoutingContext {
    pub(crate) fn new(flag: Option<bool>) -> Arc<Self> {
        let mut config = Config::default();
        config.features = serde_json::from_value(match flag {
            None => serde_json::json!({}),
            Some(enabled) => serde_json::json!({"provider_model_ref":enabled}),
        })
        .unwrap();
        config.default_provider_instance = Some("chat-a".into());
        config.defaults = Some(
            serde_json::from_value(serde_json::json!({
                "chat":{"provider":"chat-a", "model":"main-a"},
                "fast":{"provider":"fast-b", "model":"cheap-b"},
                "memory_background":{"provider":"memory-c", "model":"background-c"}
            }))
            .unwrap(),
        );
        let mut providers = HashMap::new();
        let mut runtime_models = HashMap::new();
        for (id, model) in [
            ("chat-a", "main-a"),
            ("fast-b", "cheap-b"),
            ("memory-c", "background-c"),
        ] {
            let provider = Arc::new(CaptureProvider::default());
            providers.insert(id.to_string(), provider);
            let models = if id == "chat-a" {
                vec![model, "legacy-fast-a", "explicit-gold"]
            } else {
                vec![model]
            };
            runtime_models.insert(
                id.to_string(),
                models.iter().map(|model| model.to_string()).collect(),
            );
            config.provider_instances.insert(
                id.into(),
                serde_json::from_value(serde_json::json!({
                    "provider_type":"openai", "enabled":true, "model":model,
                    "fast_model":if id == "chat-a" {Some("legacy-fast-a")} else {None},
                    "runtime_models":models
                }))
                .unwrap(),
            );
        }
        let registry = Arc::new(ProviderRegistry::new(
            providers
                .iter()
                .map(|(id, provider)| (id.clone(), provider.clone() as Arc<dyn LLMProvider>))
                .collect(),
            "chat-a".into(),
        ));
        registry.set_runtime_models(runtime_models);
        Arc::new(Self {
            config: Arc::new(RwLock::new(config)),
            registry,
            providers,
            events: broadcast::channel(32).0,
        })
    }

    pub(crate) fn session() -> Session {
        let mut session = Session::new("synthetic-routing-session", "main-a");
        session.model_ref = Some(ProviderModelRef::new("chat-a", "main-a"));
        session.add_message(Message::user("Synthetic goal for routing validation"));
        session
    }

    pub(crate) fn requests(&self, id: &str) -> Vec<(String, String)> {
        self.providers[id].requests.lock().unwrap().clone()
    }

    pub(crate) fn models(&self, id: &str) -> Vec<String> {
        self.requests(id)
            .into_iter()
            .map(|(model, _)| model)
            .collect()
    }

    pub(crate) fn assert_no_requests(&self) {
        for provider in self.providers.values() {
            assert!(provider.requests.lock().unwrap().is_empty());
        }
    }
}

#[async_trait]
impl AgentSessionContext for RoutingContext {
    fn sessions(&self) -> &crate::SessionCache {
        panic!("routing consumer must not access session storage")
    }
    fn storage(&self) -> &Arc<dyn Storage> {
        panic!("routing consumer must not access storage")
    }
    fn persistence(&self) -> &Arc<LockedSessionStore> {
        panic!("routing consumer must not access persistence")
    }
    fn agent_runners(&self) -> &Arc<RwLock<HashMap<String, AgentRunner>>> {
        panic!("routing consumer must not spawn an agent")
    }
    fn account_sink(&self) -> &Arc<AccountEventSink> {
        panic!("routing consumer must not mutate the account")
    }
    fn config(&self) -> &Arc<RwLock<Config>> {
        &self.config
    }
    fn provider_registry(&self) -> &Arc<ProviderRegistry> {
        &self.registry
    }
    async fn get_session_event_sender(&self, _id: &str) -> broadcast::Sender<AgentEvent> {
        self.events.clone()
    }
    async fn load_session_merged(&self, _id: &str) -> Option<Session> {
        panic!("unexpected session load")
    }
    async fn save_and_cache_session(&self, _session: &mut Session) {
        panic!("unexpected session write")
    }
    async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        self.providers["chat-a"].clone()
    }
    fn get_provider_for_model_ref(
        &self,
        target: &ProviderModelRef,
    ) -> Option<Arc<dyn LLMProvider>> {
        ProviderModelRouter::new(self.registry.clone())
            .route(target)
            .ok()
    }
    async fn get_provider_for_endpoint(&self, provider: &str) -> Option<Arc<dyn LLMProvider>> {
        self.registry.get(provider)
    }
    fn title_gen_acquire(&self, _id: &str) -> bool {
        true
    }
    fn title_gen_release(&self, _id: &str) {}
}
