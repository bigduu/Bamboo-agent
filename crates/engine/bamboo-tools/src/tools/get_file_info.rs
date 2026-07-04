use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
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
        "Get file metadata such as type, size, modification time, and existence without loading file contents."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
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

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
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
                // When the path simply does not exist, return a structured
                // `exists: false` response so this tool subsumes FileExists.
                if error.kind() == std::io::ErrorKind::NotFound {
                    return Ok(ToolOutcome::Completed(ToolResult {
                        success: true,
                        result: json!({
                            "path": path,
                            "exists": false
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                        images: Vec::new(),
                    }));
                }
                return Ok(ToolOutcome::Completed(ToolResult {
                    success: false,
                    result: format!("Failed to read metadata for '{path}': {error}"),
                    display_preference: Some("error".to_string()),
                    images: Vec::new(),
                }));
            }
        };

        let modified_unix = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs());

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "path": path,
                "exists": true,
                "is_file": metadata.is_file(),
                "is_dir": metadata.is_dir(),
                "size_bytes": metadata.len(),
                "modified_unix": modified_unix
            })
            .to_string(),
            display_preference: Some("json".to_string()),
            images: Vec::new(),
        }))
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
        let out = tool
            .invoke(
                json!({"path": file_path.to_string_lossy()}),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        assert!(result.result.contains("\"is_file\":true"));
        assert!(result.result.contains("\"exists\":true"));
    }

    #[tokio::test]
    async fn get_file_info_returns_exists_false_for_missing_path() {
        let tool = GetFileInfoTool::new();
        let out = tool
            .invoke(
                json!({"path": "/tmp/bamboo-file-info-missing-xyz-98765"}),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["exists"], false);
    }
}
