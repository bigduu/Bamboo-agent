use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;

mod progress;
mod todowrite;

pub(super) async fn track_todo_progress(
    todo_context: &mut Option<TodoLoopContext>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    tool_call: &ToolCall,
    result: &ToolResult,
    round: usize,
) {
    progress::track_todo_progress(todo_context, event_tx, session_id, tool_call, result, round)
        .await;
}

pub(super) async fn maybe_handle_todowrite(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    session_id: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    todo_context: &mut Option<TodoLoopContext>,
) {
    todowrite::maybe_handle_todowrite(
        tool_call,
        result,
        session,
        session_id,
        event_tx,
        config,
        todo_context,
    )
    .await;
}

#[cfg(test)]
mod tests;
