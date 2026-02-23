use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Tool for listing directory contents
pub struct ListDirectoryTool;

impl ListDirectoryTool {
    pub fn new() -> Self {
        Self
    }

    /// Internal implementation for listing directories
    pub async fn list_directory(path: &str) -> Result<Vec<String>, String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let mut entries = vec![];
        let mut dir = fs::read_dir(path)
            .await
            .map_err(|e| format!("Failed to read directory '{}': {}", path, e))?;

        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read directory entry: {}", e))?
        {
            let file_name = entry.file_name().to_string_lossy().to_string();
            let file_type = if entry.file_type().await.map_err(|e| e.to_string())?.is_dir() {
                "[DIR]"
            } else {
                "[FILE]"
            };
            entries.push(format!("{} {}", file_type, file_name));
        }

        Ok(entries)
    }
}

impl Default for ListDirectoryTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ListDirectoryTool {
    fn name(&self) -> &str {
        "list_directory"
    }

    fn description(&self) -> &str {
        "List all files and subdirectories in a directory"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the directory"
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

        match Self::list_directory(&path).await {
            Ok(entries) => Ok(ToolResult {
                success: true,
                result: entries.join("\n"),
                display_preference: Some("markdown".to_string()),
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
    async fn test_list_directory_success() {
        let tool = ListDirectoryTool::new();
        let result = tool.execute(json!({"path": "/tmp"})).await.unwrap();

        assert!(result.success);
        // Just verify it returns something (tmp should have entries)
        assert!(!result.result.is_empty());
    }

    #[tokio::test]
    async fn test_list_directory_path_traversal() {
        let tool = ListDirectoryTool::new();
        let result = tool.execute(json!({"path": "/etc/../etc"})).await.unwrap();

        assert!(!result.success);
        assert!(result.result.contains("Invalid path"));
    }

    #[tokio::test]
    async fn test_list_directory_uses_cwd_when_empty() {
        let tool = ListDirectoryTool::new();
        // Test with empty path - should use current directory
        let result = tool.execute(json!({"path": ""})).await.unwrap();

        // Should succeed using current directory
        assert!(result.success);
    }
}
