use crate::agent::core::tools::ToolCall;
use crate::agent::core::TaskItemStatus;
use std::collections::HashSet;

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

        let criteria_met = args
            .get("criteria_met")
            .or_else(|| args.get("criteriaMet"))
            .and_then(serde_json::Value::as_array)
            .and_then(|values| parse_criteria_met_from_array(values));

        updates.push(TaskItemUpdate {
            item_id: item_id.to_string(),
            status,
            notes: args["notes"].as_str().map(String::from),
            evidence: args["evidence"].as_str().map(String::from),
            blocker: args["blocker"].as_str().map(String::from),
            criteria_met,
        });
    }

    updates
}

fn parse_criteria_met_from_array(values: &[serde_json::Value]) -> Option<Vec<String>> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::new();
    for value in values {
        let Some(text) = value.as_str().map(str::trim) else {
            continue;
        };
        if text.is_empty() {
            continue;
        }
        if seen.insert(text.to_string()) {
            parsed.push(text.to_string());
        }
    }
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::FunctionCall;

    fn update_call(arguments: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "update_task_item".to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn parse_structured_update_fields() {
        let updates = parse_item_updates_from_tool_calls(&[update_call(serde_json::json!({
            "item_id": "task_1",
            "status": "blocked",
            "notes": "waiting on fixtures",
            "evidence": "pytest auth suite failed on setup",
            "blocker": "need seed database for test env",
            "criteria_met": ["seed db exists", "seed db exists", "  "]
        }))]);

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].item_id, "task_1");
        assert_eq!(updates[0].status, TaskItemStatus::Blocked);
        assert_eq!(updates[0].notes.as_deref(), Some("waiting on fixtures"));
        assert_eq!(
            updates[0].evidence.as_deref(),
            Some("pytest auth suite failed on setup")
        );
        assert_eq!(
            updates[0].blocker.as_deref(),
            Some("need seed database for test env")
        );
        assert_eq!(
            updates[0].criteria_met.as_ref(),
            Some(&vec!["seed db exists".to_string()])
        );
    }

    #[test]
    fn parse_structured_update_fields_accepts_camel_case_criteria() {
        let updates = parse_item_updates_from_tool_calls(&[update_call(serde_json::json!({
            "item_id": "task_2",
            "status": "completed",
            "criteriaMet": ["All tests pass", "All tests pass"]
        }))]);

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].item_id, "task_2");
        assert_eq!(updates[0].status, TaskItemStatus::Completed);
        assert_eq!(
            updates[0].criteria_met.as_ref(),
            Some(&vec!["All tests pass".to_string()])
        );
    }
}
