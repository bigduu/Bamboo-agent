use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use std::path::Path;
use tokio::fs;

/// Tool for writing file contents
pub struct WriteFileTool;

impl WriteFileTool {
    pub fn new() -> Self {
        Self
    }

    /// Internal implementation for writing files
    pub async fn write_file(path: &str, content: &str) -> Result<(), String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        // Ensure parent directory exists
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create directory '{}': {}", parent.display(), e))?;
        }

        fs::write(path, content)
            .await
            .map_err(|e| format!("Failed to write file '{}': {}", path, e))
    }
}

impl Default for WriteFileTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WriteFileTool {
    fn name(&self) -> &str {
        "write_file"
    }

    fn description(&self) -> &str {
        "Write file content. If the file does not exist, it will be created automatically, including parent directories. Path must be absolute"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file"
                },
                "content": {
                    "type": "string",
                    "description": "Content to write"
                }
            },
            "required": ["path", "content"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;
        let content = args["content"].as_str().ok_or_else(|| {
            ToolError::InvalidArguments("Missing 'content' parameter".to_string())
        })?;

        match Self::write_file(path, content).await {
            Ok(_) => Ok(ToolResult {
                success: true,
                result: format!("File written successfully: {}", path),
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
    async fn test_write_file_success() {
        let test_path = "/tmp/test_write_file.txt";
        let test_content = "Hello, WriteFileTool!";

        let tool = WriteFileTool::new();
        let result = tool
            .execute(json!({"path": test_path, "content": test_content}))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("File written successfully"));

        // Verify content
        let content = fs::read_to_string(test_path).await.unwrap();
        assert_eq!(content, test_content);

        // Cleanup
        let _ = fs::remove_file(test_path).await;
    }

    #[tokio::test]
    async fn test_write_file_creates_parent_dirs() {
        let test_path = "/tmp/test_nested_dir/test_write_file.txt";
        let test_content = "Hello from nested dir!";

        // Cleanup if exists
        let _ = fs::remove_dir_all("/tmp/test_nested_dir").await;

        let tool = WriteFileTool::new();
        let result = tool
            .execute(json!({"path": test_path, "content": test_content}))
            .await
            .unwrap();

        assert!(result.success);

        // Verify content
        let content = fs::read_to_string(test_path).await.unwrap();
        assert_eq!(content, test_content);

        // Cleanup
        let _ = fs::remove_dir_all("/tmp/test_nested_dir").await;
    }

    #[tokio::test]
    async fn test_write_file_path_traversal() {
        let tool = WriteFileTool::new();
        let result = tool
            .execute(json!({"path": "/etc/../etc/passwd", "content": "test"}))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("Invalid path"));
    }
}
