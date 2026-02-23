use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Tool for getting detailed file information
pub struct GetFileInfoTool;

impl GetFileInfoTool {
    pub fn new() -> Self {
        Self
    }

    /// Internal implementation for getting file info
    pub async fn get_file_info(path: &str) -> Result<String, String> {
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let metadata = fs::metadata(path)
            .await
            .map_err(|e| format!("Failed to get file info '{}': {}", path, e))?;

        let size = metadata.len();
        let is_file = metadata.is_file();
        let is_dir = metadata.is_dir();
        let modified = metadata
            .modified()
            .map_err(|e| e.to_string())?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();

        Ok(format!(
            "Path: {}\nType: {}\nSize: {} bytes\nModified: {} UTC",
            path,
            if is_file {
                "File"
            } else if is_dir {
                "Directory"
            } else {
                "Other"
            },
            size,
            chrono::DateTime::from_timestamp(modified as i64, 0)
                .map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339())
                .unwrap_or_else(|| "Unknown".to_string())
        ))
    }
}

impl Default for GetFileInfoTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GetFileInfoTool {
    fn name(&self) -> &str {
        "get_file_info"
    }

    fn description(&self) -> &str {
        "Get detailed file information (size, type, modification time, etc.)"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file"
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

        match Self::get_file_info(&path).await {
            Ok(info) => Ok(ToolResult {
                success: true,
                result: info,
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
    async fn test_get_file_info_success() {
        let test_path = "/tmp/test_file_info.txt";
        let test_content = "Hello, GetFileInfoTool!";

        // Setup: create test file
        fs::write(test_path, test_content).await.unwrap();

        let tool = GetFileInfoTool::new();
        let result = tool.execute(json!({"path": test_path})).await.unwrap();

        assert!(result.success);
        assert!(result.result.contains("Path:"));
        assert!(result.result.contains("Type: File"));
        assert!(result.result.contains("Size:"));
        assert!(result.result.contains("Modified:"));

        // Cleanup
        let _ = fs::remove_file(test_path).await;
    }

    #[tokio::test]
    async fn test_get_file_info_directory() {
        let tool = GetFileInfoTool::new();
        let result = tool.execute(json!({"path": "/tmp"})).await.unwrap();

        assert!(result.success);
        assert!(result.result.contains("Type: Directory"));
    }

    #[tokio::test]
    async fn test_get_file_info_path_traversal() {
        let tool = GetFileInfoTool::new();
        let result = tool
            .execute(json!({"path": "/etc/../etc/passwd"}))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("Invalid path"));
    }
}
