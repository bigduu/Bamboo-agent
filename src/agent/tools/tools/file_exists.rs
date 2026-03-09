use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Check whether a path exists on disk.
pub struct FileExistsTool;

impl FileExistsTool {
    pub fn new() -> Self {
        Self
    }

    async fn exists(path: &str) -> Result<bool, String> {
        if path.trim().is_empty() {
            return Err("path must be a non-empty string".to_string());
        }
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }
        Ok(fs::metadata(path).await.is_ok())
    }
}

impl Default for FileExistsTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for FileExistsTool {
    fn name(&self) -> &str {
        "FileExists"
    }

    fn description(&self) -> &str {
        "Check if a file or directory exists at a given path"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or relative path to check"
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

        match Self::exists(path).await {
            Ok(exists) => Ok(ToolResult {
                success: true,
                result: json!({
                    "path": path,
                    "exists": exists
                })
                .to_string(),
                display_preference: Some("json".to_string()),
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                result: error,
                display_preference: Some("error".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_exists_returns_false_for_missing_path() {
        let tool = FileExistsTool::new();
        let result = tool
            .execute(json!({"path": "/tmp/bamboo-file-exists-missing-xyz"}))
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.result.contains("\"exists\":false"));
    }
}
