use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;

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
}
