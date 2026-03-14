use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, TodoItemStatus};
use crate::agent::loop_module::todo_context::TodoLoopContext;

pub(super) async fn track_todo_progress(
    todo_context: &mut Option<TodoLoopContext>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    tool_call: &ToolCall,
    result: &ToolResult,
    round: usize,
) {
    let Some(ctx) = todo_context else {
        return;
    };

    // IMPORTANT: First auto-update status (may set active_item)
    // Then track tool execution (so first tool is recorded)
    ctx.auto_update_status(&tool_call.function.name, result);
    ctx.track_tool_execution(&tool_call.function.name, result, round as u32);

    if let Some(event) = build_progress_event(ctx, session_id) {
        let _ = event_tx.send(event).await;
    }
}

fn build_progress_event(ctx: &TodoLoopContext, session_id: &str) -> Option<AgentEvent> {
    let target_item = if let Some(ref active_id) = ctx.active_item_id {
        ctx.items.iter().find(|item| &item.id == active_id)
    } else {
        ctx.items
            .iter()
            .find(|item| item.status == TodoItemStatus::Completed)
    }?;

    Some(AgentEvent::TodoListItemProgress {
        session_id: session_id.to_string(),
        item_id: target_item.id.clone(),
        status: target_item.status.clone(),
        tool_calls_count: target_item.tool_calls.len(),
        version: ctx.version,
    })
}
