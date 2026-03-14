use crate::agent::core::tools::ToolCall;
use crate::agent::core::TodoItemStatus;

use super::TodoItemUpdate;

fn status_to_wire_value(status: &TodoItemStatus) -> &'static str {
    match status {
        TodoItemStatus::Pending => "pending",
        TodoItemStatus::InProgress => "in_progress",
        TodoItemStatus::Completed => "completed",
        TodoItemStatus::Blocked => "blocked",
    }
}

pub(super) fn summarize_updates(updates: &[TodoItemUpdate]) -> String {
    if updates.is_empty() {
        return "No todo status changes needed.".to_string();
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
        "Applied {} todo update(s): {}",
        updates.len(),
        details.join(", ")
    )
}

pub(super) fn parse_item_updates_from_tool_calls(tool_calls: &[ToolCall]) -> Vec<TodoItemUpdate> {
    let mut updates = Vec::new();

    for tool_call in tool_calls {
        if tool_call.function.name != "update_todo_item" {
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
            "completed" => TodoItemStatus::Completed,
            "blocked" => TodoItemStatus::Blocked,
            _ => continue,
        };

        updates.push(TodoItemUpdate {
            item_id: item_id.to_string(),
            status,
            notes: args["notes"].as_str().map(String::from),
        });
    }

    updates
}
