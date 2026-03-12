use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::path::{Path, PathBuf};

use super::workspace_state;

/// Set process working directory.
pub struct SetWorkspaceTool;

impl SetWorkspaceTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SetWorkspaceTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SetWorkspaceTool {
    fn name(&self) -> &str {
        "SetWorkspace"
    }

    fn description(&self) -> &str {
        "Set current workspace directory for the current session"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path of the workspace directory"
                }
            },
            "required": ["path"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("SetWorkspace"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("SetWorkspace requires a session_id in tool context".to_string())
        })?;

        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        if path.trim().is_empty() {
            return Err(ToolError::InvalidArguments(
                "path must be a non-empty string".to_string(),
            ));
        }
        let base = workspace_state::workspace_or_process_cwd(Some(session_id));
        let raw_path = Path::new(path.trim());
        let path_obj: PathBuf = if raw_path.is_absolute() {
            raw_path.to_path_buf()
        } else {
            base.join(raw_path)
        };
        if !path_obj.exists() {
            return Ok(ToolResult {
                success: false,
                result: format!("Path does not exist: {}", path_obj.display()),
                display_preference: Some("error".to_string()),
            });
        }
        if !path_obj.is_dir() {
            return Ok(ToolResult {
                success: false,
                result: format!("Path is not a directory: {}", path_obj.display()),
                display_preference: Some("error".to_string()),
            });
        }

        let absolute_path = path_obj
            .canonicalize()
            .map_err(|e| ToolError::Execution(format!("Failed to canonicalize path: {e}")))?;

        workspace_state::set_workspace(session_id, absolute_path.clone());

        Ok(ToolResult {
            success: true,
            result: json!({
                "session_id": session_id,
                "workspace": crate::core::paths::path_to_display_string(&absolute_path)
            })
            .to_string(),
            display_preference: Some("json".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_workspace_rejects_missing_path() {
        let tool = SetWorkspaceTool::new();
        let result = tool
            .execute_with_context(
                json!({"path": "/tmp/bamboo-no-such-workspace"}),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: "call_1",
                    event_tx: None,
                },
            )
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.result.contains("does not exist"));
    }

    #[tokio::test]
    async fn set_workspace_requires_session_context() {
        let tool = SetWorkspaceTool::new();
        let err = tool
            .execute(json!({ "path": "/" }))
            .await
            .expect_err("missing session should fail");
        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("session_id")));
    }

    #[tokio::test]
    async fn set_workspace_resolves_relative_to_current_session_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        let nested = base.join("nested");
        tokio::fs::create_dir_all(&nested).await.unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());
        let tool = SetWorkspaceTool::new();

        let first = tool
            .execute_with_context(
                json!({ "path": base }),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_1",
                    event_tx: None,
                },
            )
            .await
            .unwrap();
        assert!(first.success);

        let second = tool
            .execute_with_context(
                json!({ "path": "nested" }),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_2",
                    event_tx: None,
                },
            )
            .await
            .unwrap();
        assert!(second.success);

        let payload: serde_json::Value = serde_json::from_str(&second.result).unwrap();
        let expected = nested.canonicalize().unwrap();
        assert_eq!(
            payload["workspace"].as_str().unwrap(),
            crate::core::paths::path_to_display_string(&expected)
        );
    }
}
