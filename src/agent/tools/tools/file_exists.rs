use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Tool for checking if a file or directory exists
pub struct FileExistsTool;

impl FileExistsTool {
    pub fn new() -> Self {
        Self
    }

    /// Internal implementation for checking file existence
    pub async fn file_exists(path: &str) -> Result<bool, String> {
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
        "file_exists"
    }

    fn description(&self) -> &str {
        "Check if a file or directory exists"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file or directory"
                }
            },
            "required": ["path"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = if let Some(p) = args.get("path").and_then(|v| v.as_str()) {
            if !p.is_empty() {
                p.to_string()
            } else {
                std::env::current_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .map_err(|e| {
                        ToolError::InvalidArguments(format!(
                            "Missing 'path' parameter and failed to resolve current dir: {}",
                            e
                        ))
                    })?
            }
        } else {
            std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .map_err(|e| {
                    ToolError::InvalidArguments(format!(
                        "Missing 'path' parameter and failed to resolve current dir: {}",
                        e
                    ))
                })?
        };

        match Self::file_exists(&path).await {
            Ok(exists) => Ok(ToolResult {
                success: true,
                result: if exists { "true" } else { "false" }.to_string(),
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
    use tokio::fs;

    #[tokio::test]
    async fn test_file_exists_true() {
        let test_path = "/tmp/test_file_exists.txt";

        // Setup: create test file
        fs::write(test_path, "test").await.unwrap();

        let tool = FileExistsTool::new();
        let result = tool.execute(json!({"path": test_path})).await.unwrap();

        assert!(result.success);
        assert_eq!(result.result, "true");

        // Cleanup
        let _ = fs::remove_file(test_path).await;
    }

    #[tokio::test]
    async fn test_file_exists_false() {
        let tool = FileExistsTool::new();
        let result = tool
            .execute(json!({"path": "/tmp/nonexistent_file_xyz.txt"}))
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.result, "false");
    }

    #[tokio::test]
    async fn test_file_exists_path_traversal() {
        let tool = FileExistsTool::new();
        let result = tool
            .execute(json!({"path": "/etc/../etc/passwd"}))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("Invalid path"));
    }
}
