use std::sync::Arc;

use tokio::sync::mpsc;

use bamboo_application_agent::tools::{ToolCall, ToolExecutor, ToolResult};
use bamboo_application_agent::{AgentEvent, Session};
use crate::config::AgentLoopConfig;
use crate::task_context::TaskLoopContext;
use bamboo_application_metrics::MetricsCollector;

use super::loop_state::RoundExecutionState;

mod error_path;
mod success_path;
mod workspace;

pub(super) struct SuccessPathContext<'a> {
    pub tool_call: &'a ToolCall,
    pub result: &'a ToolResult,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub session: &'a mut Session,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    pub task_context: &'a mut Option<TaskLoopContext>,
    pub state: &'a mut RoundExecutionState,
    pub tool_duration: std::time::Duration,
}

pub(super) async fn handle_successful_tool_result(ctx: SuccessPathContext<'_>) -> bool {
    success_path::handle_successful_tool_result(ctx).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_tool_execution_error(
    tool_call: &ToolCall,
    error_message: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    round: usize,
    session: &mut Session,
    state: &mut RoundExecutionState,
) {
    error_path::handle_tool_execution_error(
        tool_call,
        error_message,
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        round,
        session,
        state,
    )
    .await;
}
