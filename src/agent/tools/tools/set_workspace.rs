use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::path::Path;

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
        "Set current working directory for this process"
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
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        if path.trim().is_empty() {
            return Err(ToolError::InvalidArguments(
                "path must be a non-empty string".to_string(),
            ));
        }
        if path.contains("..") {
            return Err(ToolError::InvalidArguments(
                "Invalid path: contains '..'".to_string(),
            ));
        }

        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Ok(ToolResult {
                success: false,
                result: format!("Path does not exist: {path}"),
                display_preference: Some("error".to_string()),
            });
        }
        if !path_obj.is_dir() {
            return Ok(ToolResult {
                success: false,
                result: format!("Path is not a directory: {path}"),
                display_preference: Some("error".to_string()),
            });
        }

        let absolute_path = path_obj
            .canonicalize()
            .map_err(|e| ToolError::Execution(format!("Failed to canonicalize path: {e}")))?;

        #[cfg(windows)]
        let dir_for_process =
            std::path::PathBuf::from(crate::core::paths::path_to_display_string(&absolute_path));
        #[cfg(not(windows))]
        let dir_for_process = absolute_path.clone();

        std::env::set_current_dir(&dir_for_process)
            .map_err(|e| ToolError::Execution(format!("Failed to set workspace: {e}")))?;

        Ok(ToolResult {
            success: true,
            result: json!({
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
            .execute(json!({"path": "/tmp/bamboo-no-such-workspace"}))
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.result.contains("does not exist"));
    }
}
