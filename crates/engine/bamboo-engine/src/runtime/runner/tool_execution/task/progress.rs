use tokio::sync::mpsc;

use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::AgentEvent;

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
    let target_id = ctx.auto_update_status(&tool_call.function.name, result);
    ctx.track_tool_execution(&tool_call.function.name, result, round as u32);

    if let Some(event) = target_id
        .as_deref()
        .and_then(|item_id| build_progress_event(ctx, session_id, item_id))
    {
        let _ = event_tx.send(event).await;
    }
}

fn build_progress_event(
    ctx: &TaskLoopContext,
    session_id: &str,
    item_id: &str,
) -> Option<AgentEvent> {
    let target_item = ctx.items.iter().find(|item| item.id == item_id)?;

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
    use bamboo_agent_core::tools::FunctionCall;
    use bamboo_domain::{TaskBlocker, TaskBlockerKind, TaskItemStatus};
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
            max_rounds: Some(20),
            created_at: now,
            updated_at: now,
            version: 8,
            task_list_dirty: false,
        };

        let event = build_progress_event(&ctx, "child", "deploy").expect("progress event");
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

    #[tokio::test]
    async fn progress_reports_the_item_just_completed_when_active_focus_clears() {
        let now = Utc::now();
        let mut context = Some(TaskLoopContext {
            session_id: "root".into(),
            items: vec![
                TaskLoopItem {
                    id: "prepare".into(),
                    status: TaskItemStatus::Completed,
                    ..Default::default()
                },
                TaskLoopItem {
                    id: "read".into(),
                    description: "Read fixture".into(),
                    status: TaskItemStatus::InProgress,
                    ..Default::default()
                },
            ],
            active_item_id: Some("read".into()),
            current_round: 0,
            max_rounds: None,
            created_at: now,
            updated_at: now,
            version: 1,
            task_list_dirty: false,
        });
        let call = ToolCall {
            id: "reader".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: "{}".into(),
            },
        };
        let result = ToolResult::text(true, "fixture");
        let (tx, mut rx) = mpsc::channel(8);
        for round in 1..=4 {
            track_task_progress(&mut context, &tx, "root", &call, &result, round).await;
        }
        assert!(context.as_ref().unwrap().active_item_id.is_none());
        let mut last = None;
        while let Ok(event) = rx.try_recv() {
            last = Some(event);
        }
        assert!(
            matches!(&last, Some(AgentEvent::TaskListItemProgress {
                session_id, item_id, status: TaskItemStatus::Completed, item: Some(item), ..
            }) if session_id == "root" && item_id == "read" && item.id == "read"
                && item.status == TaskItemStatus::Completed),
            "expected a completion event for read, got {last:?}"
        );

        track_task_progress(&mut context, &tx, "root", &call, &result, 5).await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
