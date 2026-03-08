use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use crate::agent::core::{TodoItem, TodoItemStatus, TodoList};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct TodoWriteArgs {
    todos: Vec<TodoWriteItem>,
}

#[derive(Debug, Deserialize)]
struct TodoWriteItem {
    content: String,
    status: String,
    #[serde(rename = "activeForm")]
    active_form: String,
}

pub struct TodoWriteTool;

impl TodoWriteTool {
    pub fn new() -> Self {
        Self
    }

    pub fn todo_list_from_args(
        args: &serde_json::Value,
        session_id: &str,
    ) -> Result<TodoList, ToolError> {
        let parsed: TodoWriteArgs = serde_json::from_value(args.clone())
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid TodoWrite args: {}", e)))?;

        let mut items = Vec::with_capacity(parsed.todos.len());
        for (idx, todo) in parsed.todos.into_iter().enumerate() {
            let status = match todo.status.as_str() {
                "pending" => TodoItemStatus::Pending,
                "in_progress" => TodoItemStatus::InProgress,
                "completed" => TodoItemStatus::Completed,
                _ => {
                    return Err(ToolError::InvalidArguments(format!(
                        "Invalid todo status '{}' (expected pending/in_progress/completed)",
                        todo.status
                    )))
                }
            };

            items.push(TodoItem {
                id: format!("todo_{}", idx + 1),
                description: todo.content,
                status,
                depends_on: Vec::new(),
                notes: todo.active_form,
            });
        }

        Ok(TodoList {
            session_id: session_id.to_string(),
            title: "Todo List".to_string(),
            items,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }
}

impl Default for TodoWriteTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TodoWriteTool {
    fn name(&self) -> &str {
        "TodoWrite"
    }

    fn description(&self) -> &str {
        "Create and manage a structured task list for the current coding session"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "todos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": { "type": "string", "minLength": 1 },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed"]
                            },
                            "activeForm": { "type": "string", "minLength": 1 }
                        },
                        "required": ["content", "status", "activeForm"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["todos"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: TodoWriteArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid TodoWrite args: {}", e)))?;

        let completed = parsed
            .todos
            .iter()
            .filter(|item| item.status == "completed")
            .count();

        Ok(ToolResult {
            success: true,
            result: format!(
                "Todo list updated: {} items ({} completed)",
                parsed.todos.len(),
                completed
            ),
            display_preference: Some("Default".to_string()),
        })
    }
}
