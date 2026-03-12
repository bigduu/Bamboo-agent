use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct TaskArgsRaw {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: String,
    #[serde(default)]
    responsibility: Option<String>,
    prompt: String,
    subagent_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskArgs {
    title: String,
    responsibility: String,
    prompt: String,
    subagent_type: String,
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

fn normalize_title(title: Option<String>, legacy_description: String) -> Result<String, ToolError> {
    let title = title.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let legacy_description = {
        let trimmed = legacy_description.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };
    normalize_required_text(title.or(legacy_description), "title")
}

fn normalize_task_args(raw: TaskArgsRaw) -> Result<TaskArgs, ToolError> {
    Ok(TaskArgs {
        title: normalize_title(raw.title, raw.description)?,
        responsibility: normalize_required_text(raw.responsibility, "responsibility")?,
        prompt: normalize_required_text(Some(raw.prompt), "prompt")?,
        subagent_type: normalize_required_text(Some(raw.subagent_type), "subagent_type")?,
    })
}

pub struct TaskTool;

impl TaskTool {
    pub fn new() -> Self {
        Self
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
        "Delegate a sub-session (sub task/team agent/parallel worker) for non-trivial work. Always include title and responsibility."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Short title for the child session."
                },
                "description": {
                    "type": "string",
                    "description": "Legacy alias of title; prefer title."
                },
                "responsibility": {
                    "type": "string",
                    "description": "Single explicit responsibility for the child session."
                },
                "prompt": {
                    "type": "string",
                    "description": "The task for the agent to perform"
                },
                "subagent_type": {
                    "type": "string",
                    "description": "The type of specialized agent to use"
                }
            },
            "oneOf": [
                {"required": ["title", "responsibility", "prompt", "subagent_type"]},
                {"required": ["description", "responsibility", "prompt", "subagent_type"]}
            ],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: TaskArgsRaw = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {}", e)))?;
        let parsed = normalize_task_args(parsed)?;

        Err(ToolError::Execution(format!(
            "Task '{}' ({}) with responsibility '{}' is unavailable in this executor context. Prompt: {}. This tool requires server overlay execution.",
            parsed.title,
            parsed.subagent_type,
            parsed.responsibility,
            parsed.prompt
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn task_execute_returns_execution_error() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({
                "title": "quick task",
                "responsibility": "summarize parser entrypoints",
                "prompt": "do something",
                "subagent_type": "default"
            }))
            .await
            .expect_err("task tool should be unavailable in builtin executor");

        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("unavailable")));
    }

    #[tokio::test]
    async fn task_execute_accepts_legacy_description_alias() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({
                "description": "quick task",
                "responsibility": "summarize parser entrypoints",
                "prompt": "do something",
                "subagent_type": "default"
            }))
            .await
            .expect_err("task tool should be unavailable in builtin executor");

        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("quick task")));
    }

    #[tokio::test]
    async fn task_execute_rejects_missing_responsibility() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({
                "title": "quick task",
                "prompt": "do something",
                "subagent_type": "default"
            }))
            .await
            .expect_err("task tool should reject missing responsibility");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("responsibility")));
    }
}
