use bamboo_metrics::MetricsCollector;
use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCall, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;

/// Result of a round's tool execution.
#[derive(Debug, Clone)]
pub struct ToolRoundResult {
    pub awaiting_clarification: bool,
    pub should_break: bool,
    pub tool_calls_count: usize,
}

/// Manages tool surface, schemas, routing, execution, and output processing.
#[async_trait]
pub trait ToolManager: Send + Sync {
    /// Resolve available tool schemas for the session.
    fn resolve_tool_schemas(&self, config: &AgentLoopConfig, session: &Session) -> Vec<ToolSchema>;

    /// Execute tool calls for a round.
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
    ) -> Result<ToolRoundResult, AgentError>;
}
