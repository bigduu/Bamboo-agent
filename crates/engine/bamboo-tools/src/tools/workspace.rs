use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
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

    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        let has_path = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .is_some_and(|v| !v.is_empty());
        if has_path {
            ToolClass::MUTATING_SERIAL
        } else {
            ToolClass::READONLY_PARALLEL
        }
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

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let path_arg = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());

        match path_arg {
            // ── SET mode ──────────────────────────────────────────────
            Some(path) => {
                let session_id = ctx.session_id().ok_or_else(|| {
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
                    return Ok(ToolOutcome::Completed(ToolResult {
                        success: false,
                        result: format!("Path does not exist: {}", path_obj.display()),
                        display_preference: Some("error".to_string()),
                        images: Vec::new(),
                    }));
                }
                if !path_obj.is_dir() {
                    return Ok(ToolOutcome::Completed(ToolResult {
                        success: false,
                        result: format!("Path is not a directory: {}", path_obj.display()),
                        display_preference: Some("error".to_string()),
                        images: Vec::new(),
                    }));
                }

                let absolute_path = path_obj.canonicalize().map_err(|e| {
                    ToolError::Execution(format!("Failed to canonicalize path: {e}"))
                })?;

                // `set_workspace` returns the FINAL stored path, which may
                // differ from `absolute_path` when workspace-root
                // confinement (#217) relocated it — report the truth rather
                // than the pre-relocation request.
                let stored = workspace_state::set_workspace(session_id, absolute_path.clone());
                let relocated = stored != absolute_path;

                let mut payload = json!({
                    "session_id": session_id,
                    "workspace": bamboo_config::paths::path_to_display_string(&stored)
                });
                if relocated {
                    payload["relocated_from"] =
                        json!(bamboo_config::paths::path_to_display_string(&absolute_path));
                }

                Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: payload.to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                }))
            }

            // ── GET mode ──────────────────────────────────────────────
            None => {
                if let Some(session_id) = ctx.session_id() {
                    if let Some(workspace) = workspace_state::get_workspace(session_id) {
                        return Ok(ToolOutcome::Completed(ToolResult {
                            success: true,
                            result: bamboo_config::paths::path_to_display_string(&workspace),
                            display_preference: None,
                            images: Vec::new(),
                        }));
                    }
                }

                match std::env::current_dir() {
                    Ok(dir) => Ok(ToolOutcome::Completed(ToolResult {
                        success: true,
                        result: bamboo_config::paths::path_to_display_string(&dir),
                        display_preference: None,
                        images: Vec::new(),
                    })),
                    Err(error) => Ok(ToolOutcome::Completed(ToolResult {
                        success: false,
                        result: format!("Failed to get current directory: {error}"),
                        display_preference: Some("error".to_string()),
                        images: Vec::new(),
                    })),
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
        let out = tool
            .invoke(json!({}), ToolCtx::none("Workspace"))
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
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
        let out = tool
            .invoke(
                json!({}),
                ToolCtx {
                    session_id: Some(std::sync::Arc::from(session.as_str())),
                    tool_call_id: std::sync::Arc::from("call_1"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
        assert_eq!(
            result.result,
            bamboo_config::paths::path_to_display_string(&workspace)
        );
    }

    #[tokio::test]
    async fn workspace_set_changes_session_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("ws");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());

        let tool = WorkspaceTool::new();
        let out = tool
            .invoke(
                json!({"path": workspace.to_string_lossy()}),
                ToolCtx {
                    session_id: Some(std::sync::Arc::from(session.as_str())),
                    tool_call_id: std::sync::Arc::from("call_1"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);

        // Verify get mode now returns the new workspace
        let get_out = tool
            .invoke(
                json!({}),
                ToolCtx {
                    session_id: Some(std::sync::Arc::from(session.as_str())),
                    tool_call_id: std::sync::Arc::from("call_2"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(get_result) = get_out else {
            panic!("expected Completed")
        };
        assert!(get_result.success);
        let expected = workspace.canonicalize().unwrap();
        assert_eq!(
            get_result.result,
            bamboo_config::paths::path_to_display_string(&expected)
        );
    }

    #[tokio::test]
    async fn workspace_set_rejects_missing_path() {
        let tool = WorkspaceTool::new();
        let out = tool
            .invoke(
                json!({"path": "/tmp/bamboo-no-such-workspace-xyz-99999"}),
                ToolCtx {
                    session_id: Some(std::sync::Arc::from("session_1")),
                    tool_call_id: std::sync::Arc::from("call_1"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(!result.success);
        assert!(result.result.contains("does not exist"));
    }

    #[tokio::test]
    async fn workspace_set_requires_session_context() {
        let tool = WorkspaceTool::new();
        let err = tool
            .invoke(json!({"path": "/"}), ToolCtx::none("Workspace"))
            .await
            .expect_err("missing session should fail");
        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("session_id")));
    }

    // NOTE: the end-to-end test exercising `set_workspace_root_provider`
    // (issue #217) lives in `tests/workspace_root_provider.rs`, NOT here.
    // That `OnceLock` is process-global and first-registration-wins across
    // this ENTIRE lib's unit-test binary (bash/glob/grep/slash_command/
    // workspace tests all share one process) — registering it in-line here
    // would non-deterministically poison every other test in the binary that
    // assumes the pre-#217 unconfined default (e.g.
    // `workspace_set_changes_session_workspace` below, which sets an
    // arbitrary outside-any-root tempdir and expects it stored verbatim). A
    // separate `tests/*.rs` integration file compiles to its own process, so
    // it can safely register the provider without affecting anything here.
}
