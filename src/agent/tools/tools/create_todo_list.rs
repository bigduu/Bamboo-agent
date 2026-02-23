use agent_core::tools::{Tool, ToolError, ToolResult};
use agent_core::{TodoItem, TodoItemStatus, TodoList};
use async_trait::async_trait;
use serde_json::json;

/// Tool for creating a todo list for the current session
pub struct CreateTodoListTool;

impl CreateTodoListTool {
    pub fn new() -> Self {
        Self
    }

    /// Build a `TodoList` from tool arguments.
    ///
    /// This is shared by:
    /// - the tool implementation (to format the list for display), and
    /// - the agent loop runner (to persist the list in the session).
    ///
    /// Keeping this logic in one place avoids drift between the tool schema/validation and the
    /// persistence layer.
    pub fn todo_list_from_args(
        args: &serde_json::Value,
        session_id: &str,
    ) -> Result<TodoList, ToolError> {
        let title = args["title"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'title' parameter".to_string()))?;

        let items_array = args["items"].as_array().ok_or_else(|| {
            ToolError::InvalidArguments("Missing 'items' parameter".to_string())
        })?;

        let mut items = Vec::with_capacity(items_array.len());
        for item_val in items_array {
            let id = item_val["id"]
                .as_str()
                .ok_or_else(|| ToolError::InvalidArguments("Item missing 'id'".to_string()))?
                .to_string();

            let description = item_val["description"]
                .as_str()
                .ok_or_else(|| {
                    ToolError::InvalidArguments("Item missing 'description'".to_string())
                })?
                .to_string();

            let depends_on: Vec<String> = item_val["depends_on"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();

            items.push(TodoItem {
                id,
                description,
                status: TodoItemStatus::Pending,
                depends_on,
                notes: String::new(),
            });
        }

        Ok(TodoList {
            session_id: session_id.to_string(),
            title: title.to_string(),
            items,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }

    /// Format todo list as string for display
    pub fn format_todo_list(list: &TodoList) -> String {
        let mut output = format!("# {}\n\n", list.title);

        for item in &list.items {
            let status_icon = match item.status {
                TodoItemStatus::Pending => "[ ]",
                TodoItemStatus::InProgress => "[/]",
                TodoItemStatus::Completed => "[x]",
                TodoItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!("{} {}: {}\n", status_icon, item.id, item.description));

            if !item.notes.is_empty() {
                output.push_str(&format!("    Notes: {}\n", item.notes));
            }

            if !item.depends_on.is_empty() {
                output.push_str(&format!("    Depends on: {}\n", item.depends_on.join(", ")));
            }
        }

        output.push_str(&format!(
            "\nProgress: {}/{} completed",
            list.items.iter().filter(|i| i.status == TodoItemStatus::Completed).count(),
            list.items.len()
        ));

        output
    }
}

impl Default for CreateTodoListTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for CreateTodoListTool {
    fn name(&self) -> &str {
        "create_todo_list"
    }

    fn description(&self) -> &str {
        "Create a structured todo list to track multi-step task progress. \
        IMPORTANT: When the user requests multiple tasks or complex work, \
        you MUST use this tool to create a formal todo list instead of writing markdown checklists. \
        This enables real-time progress tracking and automatic status updates. \
        After creation, the todo list will be displayed in the UI and the system will track progress automatically."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Title of the todo list"
                },
                "items": {
                    "type": "array",
                    "description": "List of tasks",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string",
                                "description": "Unique identifier for the task, e.g., '1', '2', 'analyze-code'"
                            },
                            "description": {
                                "type": "string",
                                "description": "Task description"
                            },
                            "depends_on": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "IDs of other tasks that this task depends on"
                            }
                        },
                        "required": ["id", "description"]
                    }
                }
            },
            "required": ["title", "items"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        // `session_id` is set by the agent loop when persisting the list.
        let todo_list = Self::todo_list_from_args(&args, "current")?;
        let formatted = Self::format_todo_list(&todo_list);

        Ok(ToolResult {
            success: true,
            result: formatted,
            display_preference: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_create_todo_list_tool_name() {
        let tool = CreateTodoListTool::new();
        assert_eq!(tool.name(), "create_todo_list");
    }

    #[test]
    fn todo_list_from_args_builds_expected_list() {
        let args = json!({
            "title": "Test List",
            "items": [
                { "id": "1", "description": "First task" },
                { "id": "2", "description": "Second task", "depends_on": ["1"] }
            ]
        });

        let list = CreateTodoListTool::todo_list_from_args(&args, "session_123").unwrap();

        assert_eq!(list.session_id, "session_123");
        assert_eq!(list.title, "Test List");
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].status, TodoItemStatus::Pending);
        assert_eq!(list.items[1].depends_on, vec!["1"]);
    }
}
