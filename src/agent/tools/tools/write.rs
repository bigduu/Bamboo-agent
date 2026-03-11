use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::{file_change, read_tracker};

#[derive(Debug, Deserialize)]
struct WriteArgs {
    file_path: String,
    content: String,
}

pub struct WriteTool;

impl WriteTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for WriteTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "Write"
    }

    fn description(&self) -> &str {
        "Write a file to the local filesystem"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to write"
                },
                "content": {
                    "type": "string",
                    "description": "The content to write to the file"
                }
            },
            "required": ["file_path", "content"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Write"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: WriteArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Write args: {}", e)))?;

        let file_path = parsed.file_path.trim();
        let path = Path::new(file_path);

        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        if path.exists() {
            if let Some(session_id) = ctx.session_id {
                if !read_tracker::has_read(session_id, file_path).await {
                    return Err(ToolError::Execution(
                        "Write requires reading the target file first via Read".to_string(),
                    ));
                }
            }
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ToolError::Execution(format!("Failed to create parent directory: {}", e))
            })?;
        }

        let previous_bytes = file_change::read_existing_bytes(path).await?;
        let checkpoint = file_change::create_checkpoint(path, previous_bytes.as_deref()).await?;
        let next_content = parsed.content;

        tokio::fs::write(path, &next_content)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to write file: {}", e)))?;

        let previous_text = file_change::bytes_to_lossy_text(previous_bytes.as_deref());
        let payload = file_change::build_file_change_payload(
            "Write",
            path,
            format!("Wrote file: {}", file_path),
            checkpoint,
            &previous_text,
            &next_content,
        );

        Ok(ToolResult {
            success: true,
            result: payload,
            display_preference: Some("Default".to_string()),
        })
    }
}
