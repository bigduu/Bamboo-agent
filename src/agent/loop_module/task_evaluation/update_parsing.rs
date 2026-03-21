use crate::agent::core::tools::ToolCall;
use crate::agent::core::TaskItemStatus;

use super::TaskItemUpdate;

fn status_to_wire_value(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in_progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
    }
}

pub(super) fn summarize_updates(updates: &[TaskItemUpdate]) -> String {
    if updates.is_empty() {
        return "No task status changes needed.".to_string();
    }

    let details: Vec<String> = updates
        .iter()
        .map(|update| {
            format!(
                "{} -> {}",
                update.item_id,
                status_to_wire_value(&update.status)
            )
        })
        .collect();

    format!(
        "Applied {} task update(s): {}",
        updates.len(),
        details.join(", ")
    )
}

pub(super) fn parse_item_updates_from_tool_calls(tool_calls: &[ToolCall]) -> Vec<TaskItemUpdate> {
    let mut updates = Vec::new();

    for tool_call in tool_calls {
        if tool_call.function.name != "update_task_item" {
            continue;
        }

        let Ok(args) = serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
        else {
            continue;
        };

        let Some(item_id) = args["item_id"].as_str() else {
            continue;
        };
        let Some(status_str) = args["status"].as_str() else {
            continue;
        };

        let status = match status_str {
            "completed" => TaskItemStatus::Completed,
            "blocked" => TaskItemStatus::Blocked,
            _ => continue,
        };

        updates.push(TaskItemUpdate {
            item_id: item_id.to_string(),
            status,
            notes: args["notes"].as_str().map(String::from),
        });
    }

    updates
}
