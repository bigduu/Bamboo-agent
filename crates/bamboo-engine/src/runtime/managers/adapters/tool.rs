use std::sync::Arc;

use crate::metrics::MetricsCollector;
use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_infrastructure::LLMProvider;
use tokio::sync::mpsc;

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

        let result = crate::runtime::runner::tool_execution::execute_round_tool_calls(
            tool_calls,
            &frame,
            session,
            task_context,
            config
                .summarization_model_name
                .as_deref()
                .or(config.background_model_name.as_deref()),
            config
                .summarization_model_provider
                .as_ref()
                .or(config.background_model_provider.as_ref()),
            tool_schemas,
        )
        .await?;

        Ok(ToolRoundResult {
            awaiting_clarification: result.awaiting_clarification,
            should_break: false,
            tool_calls_count: tool_calls.len(),
        })
    }
}
