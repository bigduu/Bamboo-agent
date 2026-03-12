use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde_json::json;

use super::workspace_state;

/// Return process current working directory.
pub struct GetCurrentDirTool;

impl GetCurrentDirTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GetCurrentDirTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GetCurrentDirTool {
    fn name(&self) -> &str {
        "GetCurrentDir"
    }

    fn description(&self) -> &str {
        "Get the current working directory"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(json!({}), ToolExecutionContext::none("GetCurrentDir"))
            .await
    }

    async fn execute_with_context(
        &self,
        _args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        if let Some(session_id) = ctx.session_id {
            if let Some(workspace) = workspace_state::get_workspace(session_id) {
                return Ok(ToolResult {
                    success: true,
                    result: crate::core::paths::path_to_display_string(&workspace),
                    display_preference: None,
                });
            }
        }

        match std::env::current_dir() {
            Ok(dir) => Ok(ToolResult {
                success: true,
                result: crate::core::paths::path_to_display_string(&dir),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_current_dir_tool_returns_non_empty_path() {
        let tool = GetCurrentDirTool::new();
        let result = tool.execute(json!({})).await.unwrap();
        assert!(result.success);
        assert!(!result.result.trim().is_empty());
    }

    #[tokio::test]
    async fn get_current_dir_prefers_session_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());
        super::workspace_state::set_workspace(&session, workspace.clone());

        let tool = GetCurrentDirTool::new();
        let result = tool
            .execute_with_context(
                json!({}),
                ToolExecutionContext {
                    session_id: Some(&session),
                    tool_call_id: "call_1",
                    event_tx: None,
                },
            )
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(
            result.result,
            crate::core::paths::path_to_display_string(&workspace)
        );
    }
}
