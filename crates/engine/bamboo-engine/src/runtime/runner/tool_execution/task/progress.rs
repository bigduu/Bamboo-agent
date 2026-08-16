use tokio::sync::mpsc;

use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::TaskItemStatus;

pub(super) async fn track_task_progress(
    task_context: &mut Option<TaskLoopContext>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    tool_call: &ToolCall,
    result: &ToolResult,
    round: usize,
) {
    let Some(ctx) = task_context else {
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

fn build_progress_event(ctx: &TaskLoopContext, session_id: &str) -> Option<AgentEvent> {
    let target_item = if let Some(ref active_id) = ctx.active_item_id {
        ctx.items.iter().find(|item| &item.id == active_id)
    } else {
        ctx.items
            .iter()
            .find(|item| item.status == TaskItemStatus::Completed)
    }?;

    Some(AgentEvent::TaskListItemProgress {
        session_id: session_id.to_string(),
        item_id: target_item.id.clone(),
        status: target_item.status.clone(),
        tool_calls_count: target_item.tool_calls.len(),
        version: ctx.version,
        item: ctx.task_item_snapshot(&target_item.id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task_context::TaskLoopItem;
    use bamboo_domain::{TaskBlocker, TaskBlockerKind};
    use chrono::Utc;

    #[test]
    fn progress_event_carries_the_rich_item_and_monotonic_version() {
        let now = Utc::now();
        let ctx = TaskLoopContext {
            session_id: "root".to_string(),
            items: vec![TaskLoopItem {
                id: "deploy".to_string(),
                description: "Deploy release".to_string(),
                status: TaskItemStatus::Blocked,
                blockers: vec![TaskBlocker {
                    kind: TaskBlockerKind::External,
                    summary: "Release approval pending".to_string(),
                    waiting_on: Some("operator".to_string()),
                }],
                ..TaskLoopItem::default()
            }],
            active_item_id: Some("deploy".to_string()),
            current_round: 3,
            max_rounds: 20,
            created_at: now,
            updated_at: now,
            version: 8,
            task_list_dirty: false,
        };

        let event = build_progress_event(&ctx, "child").expect("progress event");
        assert!(matches!(
            event,
            AgentEvent::TaskListItemProgress {
                session_id,
                version: 8,
                item: Some(item),
                ..
            } if session_id == "child"
                && item.blockers[0].waiting_on.as_deref() == Some("operator")
        ));
    }
}
