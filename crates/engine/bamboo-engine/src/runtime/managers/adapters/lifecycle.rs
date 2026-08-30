use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_llm::LLMProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::lifecycle::LifecycleManager;
use crate::runtime::runner::state_bridge;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_metrics::MetricsCollector;

/// Default lifecycle manager that delegates to existing runner functions.
pub struct DefaultLifecycleManager {
    llm: Arc<dyn LLMProvider>,
}

impl DefaultLifecycleManager {
    pub fn new(llm: Arc<dyn LLMProvider>) -> Self {
        Self { llm }
    }
}

#[async_trait]
impl LifecycleManager for DefaultLifecycleManager {
    fn initialize_run(&self, _session: &Session, config: &AgentLoopConfig) -> AgentRuntimeState {
        // `AgentRuntimeState::run_id` is the existing per-execution identity on
        // this adapter path. Give every initialized run a fresh value so round
        // counters can safely restart for the same session.
        let mut state =
            AgentRuntimeState::new(crate::runtime::runner::round_prelude::new_execution_id());
        state.llm.model_name = config.model_name.clone();
        state.llm.provider_name = config.provider_name.clone();
        state.llm.fast_model_name = config.fast_model_name.clone();
        state.llm.background_model_name = config.background_model_name.clone();
        state.round.max_rounds = config.max_rounds as u32;
        state.status = AgentStatusState::Initializing;
        state
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_round(
        &self,
        session: &mut Session,
        task_context: &mut Option<TaskLoopContext>,
        runtime_state: &mut AgentRuntimeState,
        round: usize,
        max_rounds: usize,
        config: &AgentLoopConfig,
        cancel_token: &CancellationToken,
        metrics_collector: Option<&MetricsCollector>,
        session_id: &str,
        model_name: &str,
        tools: &dyn ToolExecutor,
        _llm: &dyn LLMProvider,
    ) -> Result<String, AgentError> {
        let execution_id = runtime_state.run_id.clone();
        crate::runtime::runner::round_prelude::prepare_round(
            session,
            task_context,
            runtime_state,
            config,
            self.llm.clone(),
            tools,
            &crate::runtime::runner::round_prelude::RoundPreludeFrame {
                execution_id: &execution_id,
                round,
                max_rounds,
                debug_enabled: false, // debug logging handled at runner level, not via adapter
                cancel_token,
                metrics_collector,
                session_id,
                model_name,
            },
        )
        .await
    }

    async fn handle_round_outcome(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        _task_context: &mut Option<TaskLoopContext>,
        round: usize,
        should_break: bool,
    ) -> Result<bool, AgentError> {
        runtime_state.round.current_round = round as u32;

        if should_break {
            runtime_state.status = AgentStatusState::Finalizing;
        } else if round as u32 >= runtime_state.round.max_rounds {
            tracing::info!(
                "[{}] Reached max rounds ({})",
                session.id,
                runtime_state.round.max_rounds
            );
            return Ok(true);
        }

        state_bridge::write_runtime_state(session, runtime_state);
        Ok(should_break)
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_run(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: &mpsc::Sender<AgentEvent>,
        session_id: &str,
        config: &AgentLoopConfig,
        metrics_collector: Option<&MetricsCollector>,
        task_context: Option<TaskLoopContext>,
    ) {
        runtime_state.status = AgentStatusState::Completed;
        state_bridge::write_runtime_state(session, runtime_state);

        crate::runtime::runner::session_finalize::finalize_session(
            task_context,
            session,
            event_tx,
            session_id,
            config,
            metrics_collector,
            false,
            runtime_state,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
    use bamboo_agent_core::Message;
    use bamboo_domain::{
        SessionActivationPolicy, SessionInboxLimits, SessionInboxPort, SessionMessageEnvelope,
    };
    use bamboo_llm::provider::LLMStream;
    use futures::stream;

    struct UnusedProvider;

    #[async_trait]
    impl LLMProvider for UnusedProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            Ok(Box::pin(stream::iter(vec![Ok(bamboo_llm::LLMChunk::Done)])))
        }
    }

    struct EmptyTools;

    #[async_trait]
    impl ToolExecutor for EmptyTools {
        async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound(call.function.name.clone()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[test]
    fn initialize_run_assigns_a_fresh_execution_identity() {
        let manager = DefaultLifecycleManager::new(Arc::new(UnusedProvider));
        let session = Session::new("same-session", "model");
        let config = AgentLoopConfig::default();

        let first = manager.initialize_run(&session, &config);
        let second = manager.initialize_run(&session, &config);

        assert!(!first.run_id.is_empty());
        assert_ne!(first.run_id, session.id);
        assert_ne!(first.run_id, second.run_id);
    }

    #[tokio::test]
    async fn adapter_admits_typed_inbox_input_then_cancels_before_prompt_context() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store,
            SessionInboxLimits::default(),
        ));
        let mut persisted = Session::new("adapter-boundary-before-recall", "model");
        persisted.add_message(Message::system("base prompt"));
        storage.save_session(&persisted).await.unwrap();

        let mut running = persisted;
        let content = "admit this before observing cancellation";
        let envelope = SessionMessageEnvelope::user_input(&running.id, content);
        let receipt = inbox.deliver(&envelope).await.unwrap();
        inbox
            .mark_activation_eligible(
                &running.id,
                receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let config = AgentLoopConfig {
            storage: Some(storage),
            persistence: Some(persistence),
            session_inbox: Some(inbox),
            app_data_dir: Some(directory.path().to_path_buf()),
            ..AgentLoopConfig::default()
        };
        let provider: Arc<dyn LLMProvider> = Arc::new(UnusedProvider);
        let manager = DefaultLifecycleManager::new(provider.clone());
        let mut runtime_state = manager.initialize_run(&running, &config);
        let mut task_context = None;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let session_id = running.id.clone();

        let error = manager
            .prepare_round(
                &mut running,
                &mut task_context,
                &mut runtime_state,
                0,
                1,
                &config,
                &cancel,
                None,
                &session_id,
                "model",
                &EmptyTools,
                provider.as_ref(),
            )
            .await
            .expect_err("a cancelled adapter round must stop before prompt context");

        assert!(matches!(error, AgentError::Cancelled));
        assert!(running.messages.iter().any(|message| {
            message.id == envelope.id.as_str()
                && message.role == bamboo_domain::Role::User
                && message.content == content
        }));
        assert!(!running
            .metadata
            .contains_key(crate::runtime::runner::prompt_context::PROMPT_MEMORY_OBSERVABILITY_KEY));
    }

    #[tokio::test]
    async fn adapter_typed_inbox_input_drives_current_round_memory_recall() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store,
            SessionInboxLimits::default(),
        ));

        let memory = bamboo_memory::memory_store::MemoryStore::new(directory.path());
        memory
            .write_memory(
                bamboo_memory::memory_store::MemoryScope::Global,
                None,
                bamboo_memory::memory_store::DurableMemoryType::Reference,
                "Adapter silver heron rule",
                "The silver heron request must use the lifecycle memory boundary.",
                &["silver".to_string(), "heron".to_string()],
                Some("adapter-typed-inbox-recall"),
                "model",
                false,
                None,
            )
            .await
            .unwrap();

        let mut persisted = Session::new("adapter-typed-inbox-recall", "model");
        persisted.add_message(Message::system("base prompt"));
        persisted.add_message(Message::user("unrelated earlier request"));
        storage.save_session(&persisted).await.unwrap();

        let mut running = persisted;
        let query = "what is the adapter silver heron rule?";
        let envelope = SessionMessageEnvelope::user_input(&running.id, query);
        let receipt = inbox.deliver(&envelope).await.unwrap();
        inbox
            .mark_activation_eligible(
                &running.id,
                receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let config = AgentLoopConfig {
            storage: Some(storage),
            persistence: Some(persistence),
            session_inbox: Some(inbox),
            app_data_dir: Some(directory.path().to_path_buf()),
            prompt_memory_flags: crate::runtime::config::PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: true,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            model_name: Some("model".to_string()),
            max_rounds: 1,
            ..AgentLoopConfig::default()
        };
        let provider: Arc<dyn LLMProvider> = Arc::new(UnusedProvider);
        let manager = DefaultLifecycleManager::new(provider.clone());
        let mut runtime_state = manager.initialize_run(&running, &config);
        let mut task_context = None;
        let cancel = CancellationToken::new();
        let session_id = running.id.clone();

        manager
            .prepare_round(
                &mut running,
                &mut task_context,
                &mut runtime_state,
                0,
                1,
                &config,
                &cancel,
                None,
                &session_id,
                "model",
                &EmptyTools,
                provider.as_ref(),
            )
            .await
            .expect("typed inbox recall should prepare the current round");

        assert!(running.messages.iter().any(|message| {
            message.id == envelope.id.as_str()
                && message.role == bamboo_domain::Role::User
                && message.content == query
        }));
        let rendered =
            crate::runtime::runner::prompt_context::render_external_memory_section(&running)
                .expect("current-round recall should render external memory");
        assert!(rendered.contains("Adapter silver heron rule"));
        assert!(
            rendered.contains("The silver heron request must use the lifecycle memory boundary.")
        );

        let observability: bamboo_agent_core::PromptMemoryObservability = serde_json::from_str(
            running
                .metadata
                .get(crate::runtime::runner::prompt_context::PROMPT_MEMORY_OBSERVABILITY_KEY)
                .expect("prompt memory refresh should persist observability"),
        )
        .unwrap();
        assert!(observability.latest_user_query_present);
        assert_eq!(observability.relevant_memory_status, "lexical");
        assert_eq!(observability.relevant_memory_count, 1);
    }
}
