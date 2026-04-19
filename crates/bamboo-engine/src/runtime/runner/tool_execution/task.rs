use tokio::sync::mpsc;

use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;

mod progress;
mod taskwrite;

pub(super) async fn track_task_progress(
    task_context: &mut Option<TaskLoopContext>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    tool_call: &ToolCall,
    result: &ToolResult,
    round: usize,
) {
    progress::track_task_progress(task_context, event_tx, session_id, tool_call, result, round)
        .await;
}

pub(super) async fn maybe_handle_taskwrite(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    session_id: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
) {
    taskwrite::maybe_handle_taskwrite(
        tool_call,
        result,
        session,
        session_id,
        event_tx,
        config,
        task_context,
    )
    .await;
}

#[cfg(test)]
mod tests;
