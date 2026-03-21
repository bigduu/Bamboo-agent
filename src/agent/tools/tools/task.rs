use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use crate::agent::core::{TaskItem, TaskItemStatus, TaskList};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct TaskArgsRaw {
    tasks: Vec<TaskWriteItem>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct TaskWriteItem {
    content: String,
    status: String,
    #[serde(rename = "activeForm")]
    active_form: Option<String>,
}

fn normalize_required_text(value: Option<String>, field_name: &str) -> Result<String, ToolError> {
    let Some(value) = value else {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(trimmed.to_string())
}

pub struct TaskTool;

impl TaskTool {
    pub fn new() -> Self {
        Self
    }

    pub fn task_list_from_args(
        args: &serde_json::Value,
        session_id: &str,
    ) -> Result<TaskList, ToolError> {
        let parsed: TaskArgsRaw = serde_json::from_value(args.clone())
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {e}")))?;

        let items_source = if parsed.tasks.is_empty() {
            return Err(ToolError::InvalidArguments(
                "Task requires a non-empty `tasks` array".to_string(),
            ));
        } else {
            parsed.tasks
        };

        let mut items = Vec::with_capacity(items_source.len());
        for (idx, task) in items_source.into_iter().enumerate() {
            let description = normalize_required_text(Some(task.content), "tasks[].content")?;
            let status = match task.status.as_str() {
                "pending" => TaskItemStatus::Pending,
                "in_progress" => TaskItemStatus::InProgress,
                "completed" => TaskItemStatus::Completed,
                "blocked" => TaskItemStatus::Blocked,
                _ => {
                    return Err(ToolError::InvalidArguments(format!(
                        "Invalid task status '{}' (expected pending/in_progress/completed/blocked)",
                        task.status
                    )))
                }
            };

            items.push(TaskItem {
                id: format!("task_{}", idx + 1),
                description: description.clone(),
                status,
                depends_on: Vec::new(),
                notes: task.active_form.unwrap_or(description),
            });
        }

        Ok(TaskList {
            session_id: session_id.to_string(),
            title: "Task List".to_string(),
            items,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }
}

impl Default for TaskTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TaskTool {
    fn name(&self) -> &str {
        "Task"
    }

    fn description(&self) -> &str {
        "Create or update the shared task list for the current root session tree. Child sessions write to the same task list as their parent/root session."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "description": "Canonical task items for the shared task list.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": { "type": "string", "minLength": 1 },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed", "blocked"]
                            },
                            "activeForm": { "type": "string" }
                        },
                        "required": ["content", "status"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["tasks"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: TaskArgsRaw = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {e}")))?;
        let count = parsed.tasks.len();
        if count == 0 {
            return Err(ToolError::InvalidArguments(
                "Task requires a non-empty `tasks` array".to_string(),
            ));
        }

        Ok(ToolResult {
            success: true,
            result: format!("Task list updated with {count} items"),
            display_preference: Some("Default".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn task_execute_accepts_tasks_payload() {
        let tool = TaskTool::new();
        let result = tool
            .execute(json!({
                "tasks": [
                    {
                        "content": "Summarize parser entrypoints",
                        "status": "in_progress",
                        "activeForm": "Summarizing parser entrypoints"
                    }
                ]
            }))
            .await
            .expect("Task should validate payload");

        assert!(result.success);
        assert!(result.result.contains("1 items"));
    }

    #[tokio::test]
    async fn task_execute_rejects_empty_payload() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({}))
            .await
            .expect_err("Task should reject empty payload");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("tasks")));
    }

    #[tokio::test]
    async fn task_execute_rejects_legacy_todos_field() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({
                "todos": [
                    {
                        "content": "Legacy path",
                        "status": "pending"
                    }
                ]
            }))
            .await
            .expect_err("Task should reject legacy todos field");

        assert!(
            matches!(err, ToolError::InvalidArguments(msg) if msg.contains("Invalid Task args"))
        );
    }

    #[test]
    fn task_list_from_args_supports_blocked_status() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "content": "Waiting on API token",
                        "status": "blocked",
                        "activeForm": "Blocked by missing API token"
                    }
                ]
            }),
            "session_1",
        )
        .expect("blocked status should be accepted");

        assert_eq!(list.session_id, "session_1");
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].status, TaskItemStatus::Blocked);
    }
}
