use bamboo_agent_core::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::path::{Path, PathBuf};

use super::workspace_state;

/// Unified workspace tool: get or set the session working directory.
///
/// - When called **without** `path`  → returns the current workspace directory.
/// - When called **with** `path`     → sets the workspace and returns the new path.
///
/// This replaces the previous `GetCurrentDir` + `SetWorkspace` pair.
pub struct WorkspaceTool;

impl WorkspaceTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WorkspaceTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WorkspaceTool {
    fn name(&self) -> &str {
        "Workspace"
    }

    fn description(&self) -> &str {
        "Get or set the current session workspace directory. Call without 'path' to get the current workspace; call with 'path' to change it."
    }

    fn mutability(&self) -> crate::ToolMutability {
        crate::ToolMutability::Mutating
    }

    fn call_mutability(&self, args: &serde_json::Value) -> crate::ToolMutability {
        let has_path = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .is_some_and(|v| !v.is_empty());
        if has_path {
            crate::ToolMutability::Mutating
        } else {
            crate::ToolMutability::ReadOnly
        }
    }

    fn call_concurrency_safe(&self, args: &serde_json::Value) -> bool {
        self.call_mutability(args) == crate::ToolMutability::ReadOnly
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path of the workspace directory to set. Omit to just read the current workspace."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Workspace"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let path_arg = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());

        match path_arg {
            // ── SET mode ──────────────────────────────────────────────
            Some(path) => {
                let session_id = ctx.session_id.ok_or_else(|| {
                    ToolError::Execution(
                        "Workspace(set) requires a session_id in tool context".to_string(),
                    )
                })?;

                let base = workspace_state::workspace_or_process_cwd(Some(session_id));
                let raw_path = Path::new(path);
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

                let absolute_path = path_obj.canonicalize().map_err(|e| {
                    ToolError::Execution(format!("Failed to canonicalize path: {e}"))
                })?;

                workspace_state::set_workspace(session_id, absolute_path.clone());

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "workspace": bamboo_infrastructure::paths::path_to_display_string(&absolute_path)
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            }

            // ── GET mode ──────────────────────────────────────────────
            None => {
                if let Some(session_id) = ctx.session_id {
                    if let Some(workspace) = workspace_state::get_workspace(session_id) {
                        return Ok(ToolResult {
                            success: true,
                            result: bamboo_infrastructure::paths::path_to_display_string(&workspace),
                            display_preference: None,
                        });
                    }
                }

                match std::env::current_dir() {
                    Ok(dir) => Ok(ToolResult {
                        success: true,
                        result: bamboo_infrastructure::paths::path_to_display_string(&dir),
                        display_preference: None,
                    }),
                    Err(error) => Ok(ToolResult {
                        success: false,
                        result: format!("Failed to get current directory: {error}"),
                        display_preference: Some("error".to_string()),
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn workspace_get_returns_non_empty_path() {
        let tool = WorkspaceTool::new();
        let result = tool.execute(json!({})).await.unwrap();
        assert!(result.success);
        assert!(!result.result.trim().is_empty());
    }

    #[tokio::test]
    async fn workspace_get_prefers_session_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());
        workspace_state::set_workspace(&session, workspace.clone());

        let tool = WorkspaceTool::new();
        let result = tool
            .execute_with_context(
                json!({}),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(
            result.result,
            bamboo_infrastructure::paths::path_to_display_string(&workspace)
        );
    }

    #[tokio::test]
    async fn workspace_set_changes_session_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("ws");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());

        let tool = WorkspaceTool::new();
        let result = tool
            .execute_with_context(
                json!({"path": workspace.to_string_lossy()}),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap();
        assert!(result.success);

        // Verify get mode now returns the new workspace
        let get_result = tool
            .execute_with_context(
                json!({}),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_2",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap();
        assert!(get_result.success);
        let expected = workspace.canonicalize().unwrap();
        assert_eq!(
            get_result.result,
            bamboo_infrastructure::paths::path_to_display_string(&expected)
        );
    }

    #[tokio::test]
    async fn workspace_set_rejects_missing_path() {
        let tool = WorkspaceTool::new();
        let result = tool
            .execute_with_context(
                json!({"path": "/tmp/bamboo-no-such-workspace-xyz-99999"}),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: "call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.result.contains("does not exist"));
    }

    #[tokio::test]
    async fn workspace_set_requires_session_context() {
        let tool = WorkspaceTool::new();
        let err = tool
            .execute(json!({"path": "/"}))
            .await
            .expect_err("missing session should fail");
        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("session_id")));
    }
}
