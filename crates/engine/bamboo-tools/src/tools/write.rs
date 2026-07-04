use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolError, ToolExecutionContext, ToolResult};
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::read_tracker::ReadState;
use super::{content_diagnostics, file_change, read_tracker};

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
        "Write a local file (create or replace full content). IMPORTANT: for existing files, call Read first in this session or Write will fail."
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
                match read_tracker::read_state(session_id, file_path).await {
                    ReadState::Unread => {
                        return Err(ToolError::Execution(
                            "Write requires reading the target file first via Read".to_string(),
                        ));
                    }
                    ReadState::Stale => {
                        return Err(ToolError::Execution(
                            "Target file changed after last Read; call Read again before Write"
                                .to_string(),
                        ));
                    }
                    ReadState::Fresh => {}
                }
            }
        }

        let previous_bytes = file_change::read_existing_bytes(path).await?;
        let checkpoint = file_change::create_checkpoint(path, previous_bytes.as_deref()).await?;
        let next_content = parsed.content;

        file_change::atomic_write_text(path, &next_content).await?;

        let previous_text = file_change::bytes_to_lossy_text(previous_bytes.as_deref());
        let mut payload = file_change::build_file_change_payload_value(
            "Write",
            path,
            format!("Wrote file: {}", file_path),
            checkpoint,
            &previous_text,
            &next_content,
        );
        content_diagnostics::attach_file_diagnostics(&mut payload, path, &next_content);

        Ok(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("Default".to_string()),
            images: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ReadTool;
    use serde_json::json;

    fn ctx<'a>(session_id: &'a str) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
    }

    #[tokio::test]
    async fn write_requires_fresh_read_for_existing_files() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let write_tool = WriteTool::new();
        let read_tool = ReadTool::new();

        let denied = write_tool
            .execute_with_context(
                json!({"file_path": file.path(), "content": "v2"}),
                ctx("session_a"),
            )
            .await;
        assert!(matches!(denied, Err(ToolError::Execution(_))));

        let _ = read_tool
            .execute_with_context(json!({"file_path": file.path()}), ctx("session_a"))
            .await
            .unwrap();

        tokio::fs::write(file.path(), "external change")
            .await
            .unwrap();

        let stale = write_tool
            .execute_with_context(
                json!({"file_path": file.path(), "content": "v3"}),
                ctx("session_a"),
            )
            .await;
        assert!(matches!(stale, Err(ToolError::Execution(msg)) if msg.contains("changed")));

        let _ = read_tool
            .execute_with_context(json!({"file_path": file.path()}), ctx("session_a"))
            .await
            .unwrap();
        let ok = write_tool
            .execute_with_context(
                json!({"file_path": file.path(), "content": "final"}),
                ctx("session_a"),
            )
            .await
            .unwrap();
        assert!(ok.success);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_rejects_symlinked_path_components() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        let link = dir.path().join("link");
        tokio::fs::create_dir_all(&real).await.unwrap();
        symlink(&real, &link).unwrap();

        let write_tool = WriteTool::new();
        let result = write_tool
            .execute(json!({
                "file_path": link.join("test.txt"),
                "content": "hello"
            }))
            .await;
        assert!(matches!(result, Err(ToolError::Execution(msg)) if msg.contains("symlinked")));
    }

    #[tokio::test]
    async fn write_includes_json_diagnostics_for_invalid_content() {
        let file = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
        let write_tool = WriteTool::new();

        let result = write_tool
            .execute(json!({
                "file_path": file.path(),
                "content": "{"
            }))
            .await
            .unwrap();

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["diagnostics"]["format"], "json");
        assert_eq!(payload["diagnostics"]["valid"], false);
    }
}
