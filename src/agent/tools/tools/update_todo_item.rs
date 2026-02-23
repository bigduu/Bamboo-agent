use agent_core::tools::{Tool, ToolError, ToolResult};
use agent_core::{TodoItemStatus, TodoList};
use async_trait::async_trait;
use serde_json::json;

/// Tool for updating a todo item status
pub struct UpdateTodoItemTool;

impl UpdateTodoItemTool {
    pub fn new() -> Self {
        Self
    }

    /// Update a todo item in the list
    pub fn update_item(
        todo_list: &mut TodoList,
        item_id: &str,
        status: TodoItemStatus,
        notes: Option<&str>,
    ) -> Result<String, String> {
        let item = todo_list
            .items
            .iter_mut()
            .find(|i| i.id == item_id)
            .ok_or_else(|| format!("Todo item '{}' not found", item_id))?;

        let old_status = item.status.clone();
        item.status = status.clone();

        if let Some(n) = notes {
            if !item.notes.is_empty() {
                item.notes.push('\n');
            }
            item.notes.push_str(n);
        }

        todo_list.updated_at = chrono::Utc::now();

        let status_str = match status {
            TodoItemStatus::Pending => "Pending",
            TodoItemStatus::InProgress => "In Progress",
            TodoItemStatus::Completed => "Completed",
            TodoItemStatus::Blocked => "Blocked",
        };

        Ok(format!(
            "Updated item '{}' from {:?} to {}",
            item_id, old_status, status_str
        ))
    }

    /// Format the current todo list status
    pub fn format_todo_list(list: &TodoList) -> String {
        let mut output = format!("# {} - Current Status\n\n", list.title);

        for item in &list.items {
            let status_icon = match item.status {
                TodoItemStatus::Pending => "[ ]",
                TodoItemStatus::InProgress => "[/]",
                TodoItemStatus::Completed => "[x]",
                TodoItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!(
                "{} {}: {}\n",
                status_icon, item.id, item.description
            ));

            if !item.notes.is_empty() {
                output.push_str(&format!("    Notes: {}\n", item.notes));
            }
        }

        let completed = list
            .items
            .iter()
            .filter(|i| i.status == TodoItemStatus::Completed)
            .count();
        let total = list.items.len();
        let percentage = if total > 0 {
            (completed * 100) / total
        } else {
            0
        };

        output.push_str(&format!(
            "\nProgress: {}/{} ({}%)",
            completed, total, percentage
        ));

        output
    }
}

impl Default for UpdateTodoItemTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for UpdateTodoItemTool {
    fn name(&self) -> &str {
        "update_todo_item"
    }

    fn description(&self) -> &str {
        "Update task status in the todo list. Can mark tasks as in progress, completed, blocked, etc., and add notes"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "item_id": {
                    "type": "string",
                    "description": "The task ID to update"
                },
                "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed", "blocked"],
                    "description": "New status: pending=to do, in_progress=in progress, completed=completed, blocked=blocked"
                },
                "notes": {
                    "type": "string",
                    "description": "Optional notes, will be appended to the task's notes"
                }
            },
            "required": ["item_id", "status"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let item_id = args["item_id"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'item_id' parameter".to_string()))?;

        let status_str = args["status"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'status' parameter".to_string()))?;

        let status = match status_str {
            "pending" => TodoItemStatus::Pending,
            "in_progress" => TodoItemStatus::InProgress,
            "completed" => TodoItemStatus::Completed,
            "blocked" => TodoItemStatus::Blocked,
            _ => {
                return Ok(ToolResult {
                    success: false,
                    result: format!("Invalid status: {}", status_str),
                    display_preference: Some("error".to_string()),
                })
            }
        };

        let notes = args["notes"].as_str();

        // Note: The actual todo_list will be retrieved from and stored back to the session
        // by the caller. This tool just validates the operation.
        let message = format!(
            "Ready to update item '{}' to status '{:?}'{}",
            item_id,
            status,
            if notes.is_some() {
                " with notes"
            } else {
                ""
            }
        );

        Ok(ToolResult {
            success: true,
            result: message,
            display_preference: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_todo_item_tool_name() {
        let tool = UpdateTodoItemTool::new();
        assert_eq!(tool.name(), "update_todo_item");
    }
}
