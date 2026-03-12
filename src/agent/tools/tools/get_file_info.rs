use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Return metadata for a file or directory.
pub struct GetFileInfoTool;

impl GetFileInfoTool {
    pub fn new() -> Self {
        Self
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
        "GetFileInfo"
    }

    fn description(&self) -> &str {
        "Get file metadata such as type, size, and modification time"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or relative path"
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

        let metadata = match fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    result: format!("Failed to read metadata for '{path}': {error}"),
                    display_preference: Some("error".to_string()),
                });
            }
        };

        let modified_unix = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs());

        Ok(ToolResult {
            success: true,
            result: json!({
                "path": path,
                "is_file": metadata.is_file(),
                "is_dir": metadata.is_dir(),
                "size_bytes": metadata.len(),
                "modified_unix": modified_unix
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
    async fn get_file_info_returns_metadata_for_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("demo.txt");
        tokio::fs::write(&file_path, "hello").await.unwrap();

        let tool = GetFileInfoTool::new();
        let result = tool
            .execute(json!({"path": file_path.to_string_lossy()}))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("\"is_file\":true"));
    }
}
