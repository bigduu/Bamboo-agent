use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_llm::LLMProvider;
use bamboo_metrics::MetricsCollector;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::tool::{ToolManager, ToolRoundResult};
use crate::runtime::task_context::TaskLoopContext;

/// Default tool manager that delegates to existing runner functions.
pub struct DefaultToolManager {
    tools: Arc<dyn ToolExecutor>,
    llm: Arc<dyn LLMProvider>,
}

impl DefaultToolManager {
    pub fn new(tools: Arc<dyn ToolExecutor>, llm: Arc<dyn LLMProvider>) -> Self {
        Self { tools, llm }
    }
}

#[async_trait]
impl ToolManager for DefaultToolManager {
    fn resolve_tool_schemas(&self, config: &AgentLoopConfig, session: &Session) -> Vec<ToolSchema> {
        crate::runtime::runner::session_setup::tool_schemas::resolve_available_tool_schemas_for_session(
            config,
            self.tools.as_ref(),
            session,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_calls(
        &self,
        tool_calls: &[ToolCall],
        event_tx: &mpsc::Sender<AgentEvent>,
        metrics_collector: Option<&MetricsCollector>,
        session_id: &str,
        round_id: &str,
        round: usize,
        session: &mut Session,
        config: &AgentLoopConfig,
        task_context: &mut Option<TaskLoopContext>,
        tool_schemas: &[ToolSchema],
        cancel: &CancellationToken,
    ) -> Result<ToolRoundResult, AgentError> {
        let frame = crate::runtime::runner::round_frame::RoundFrame {
            session_id,
            round_id,
            turn: round,
            debug_enabled: false,
            event_tx,
            metrics_collector,
            config,
            llm: &self.llm,
            tools: &self.tools,
        };
        let mut runtime_state = session
            .agent_runtime_state
            .clone()
            .unwrap_or_else(|| bamboo_domain::AgentRuntimeState::new(session_id));
        let effective_callable_set =
            crate::runtime::runner::tool_execution::legacy_effective_callable_set(tool_schemas);

        // Mirror the live pipeline's #30 biased-cancel wrap so a cancel issued
        // DURING tool execution (e.g. a long foreground Bash run) is honored on
        // this adapter path too, not only between rounds. `biased` checks
        // cancellation first; on cancel the in-flight tool futures are dropped
        // (true cancellation — foreground Bash is kill_on_drop). The per-tool
        // `tokio::time::timeout` inside `execute_round_tool_calls` is preserved;
        // cancel is strictly an additional early-exit. #104.
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(AgentError::Cancelled),
            result = crate::runtime::runner::tool_execution::execute_round_tool_calls(
                crate::runtime::runner::tool_execution::RoundToolExecution {
                    tool_calls,
                    frame: &frame,
                    session,
                    runtime_state: &mut runtime_state,
                    task_context,
                    compression_model_name: config
                        .summarization_model_name
                        .as_deref()
                        .or(config.background_model_name.as_deref()),
                    compression_model_provider: config
                        .summarization_model_provider
                        .as_ref()
                        .or(config.background_model_provider.as_ref()),
                    tool_schemas,
                    effective_callable_set: &effective_callable_set,
                },
            ) => result?,
        };
        if !config.hook_runner.is_empty() {
            session.agent_runtime_state = Some(runtime_state);
        }

        Ok(ToolRoundResult {
            awaiting_clarification: result.awaiting_clarification,
            should_break: false,
            tool_calls_count: tool_calls.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, ToolError, ToolExecutionContext, ToolResult};
    use bamboo_agent_core::Message;
    use bamboo_llm::provider::LLMStream;

    // Stubs that PANIC if invoked — they prove the biased cancel arm returns BEFORE
    // any LLM or tool work runs.
    struct PanicProvider;
    #[async_trait]
    impl bamboo_llm::LLMProvider for PanicProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("LLM must not be invoked when the run is already cancelled");
        }
    }

    struct PanicExecutor;
    #[async_trait]
    impl ToolExecutor for PanicExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("tools must not run when the run is already cancelled");
        }
        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.execute(call).await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn execute_tool_calls_short_circuits_to_cancelled_when_token_already_fired() {
        let mgr = DefaultToolManager::new(Arc::new(PanicExecutor), Arc::new(PanicProvider));
        let (event_tx, _rx) = mpsc::channel(8);
        let mut session = Session::new("s1", "model");
        let config = AgentLoopConfig::default();
        let mut task_context = None;
        // A non-empty tool call: without the cancel short-circuit, execute_round
        // would route it to PanicExecutor and the test would panic.
        let tool_calls = vec![ToolCall {
            id: "c1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "anything".to_string(),
                arguments: "{}".to_string(),
            },
        }];

        let cancel = CancellationToken::new();
        cancel.cancel(); // already cancelled before the call

        let result = mgr
            .execute_tool_calls(
                &tool_calls,
                &event_tx,
                None,
                "s1",
                "r1",
                0,
                &mut session,
                &config,
                &mut task_context,
                &[],
                &cancel,
            )
            .await;

        assert!(
            matches!(result, Err(AgentError::Cancelled)),
            "a pre-cancelled token returns Cancelled before touching tools/LLM; got {result:?}"
        );
    }
}
