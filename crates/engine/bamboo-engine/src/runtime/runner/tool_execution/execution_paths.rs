use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolExecutionSessionFlags, ToolExecutor, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_metrics::MetricsCollector;

use super::loop_state::RoundExecutionState;

mod error_path;
mod goal;
mod success_path;
mod workspace;

pub(super) enum TaskProgressResult {
    Ordinary,
    Skip,
    CommandOutcome(ToolResult),
}

impl TaskProgressResult {
    pub(super) fn result<'a>(&'a self, ordinary: &'a ToolResult) -> Option<&'a ToolResult> {
        match self {
            Self::Ordinary => Some(ordinary),
            Self::Skip => None,
            Self::CommandOutcome(result) => Some(result),
        }
    }
}

pub(super) struct SuccessPathContext<'a> {
    pub permission_replay_origin:
        Option<&'a crate::session_app::approval_replay::PermissionReplayOrigin>,
    pub tool_call: &'a ToolCall,
    pub result: &'a ToolResult,
    /// Task progress uses observed command completion rather than shell-tool
    /// transport success. The model-visible result remains unchanged.
    pub task_progress_result: TaskProgressResult,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub session: &'a mut Session,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub session_flags: ToolExecutionSessionFlags,
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
