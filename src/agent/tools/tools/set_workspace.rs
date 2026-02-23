use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::env;
use std::path::Path;

/// Tool for setting the current workspace directory
pub struct SetWorkspaceTool;

impl SetWorkspaceTool {
    pub fn new() -> Self {
        Self
    }

    /// Set the workspace directory
    pub async fn set_workspace(path: &str) -> Result<String, String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let path_obj = Path::new(path);

        // Check if path exists and is a directory
        if !path_obj.exists() {
            return Err(format!("Path does not exist: {}", path));
        }

        if !path_obj.is_dir() {
            return Err(format!("Path is not a directory: {}", path));
        }

        // Get absolute path
        let absolute_path = path_obj
            .canonicalize()
            .map_err(|e| format!("Failed to canonicalize path: {}", e))?;

        // Set as current directory for this process
        env::set_current_dir(&absolute_path)
            .map_err(|e| format!("Failed to set workspace: {}", e))?;

        Ok(absolute_path.to_string_lossy().to_string())
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
        "set_workspace"
    }

    fn description(&self) -> &str {
        "Set the current working directory (workspace). Subsequent file operations and command execution will be based on this directory. Path must be an existing directory"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the workspace directory, must be an existing directory"
                }
            },
            "required": ["path"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        match Self::set_workspace(path).await {
            Ok(absolute_path) => Ok(ToolResult {
                success: true,
                result: format!("Workspace set to: {}", absolute_path),
                display_preference: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: Some("error".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_workspace_tool_name() {
        let tool = SetWorkspaceTool::new();
        assert_eq!(tool.name(), "set_workspace");
    }
}
