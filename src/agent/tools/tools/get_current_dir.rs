use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;

/// Tool for getting the current working directory
pub struct GetCurrentDirTool;

impl GetCurrentDirTool {
    pub fn new() -> Self {
        Self
    }

    /// Internal implementation for getting current directory
    pub async fn get_current_dir() -> Result<String, String> {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .map_err(|e| format!("Failed to get current directory: {}", e))
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
        "get_current_dir"
    }

    fn description(&self) -> &str {
        "Get the absolute path of the current working directory"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {}
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
        match Self::get_current_dir().await {
            Ok(dir) => Ok(ToolResult {
                success: true,
                result: dir,
                display_preference: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_current_dir() {
        let tool = GetCurrentDirTool::new();
        let result = tool.execute(json!({})).await.unwrap();

        assert!(result.success);
        // Just verify it returns a non-empty path
        assert!(!result.result.is_empty());
        // Should be an absolute path (starts with / on Unix)
        assert!(result.result.starts_with('/'));
    }
}
