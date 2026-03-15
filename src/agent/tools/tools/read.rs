use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::read_tracker;

#[derive(Debug, Deserialize)]
struct ReadArgs {
    file_path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

pub struct ReadTool;

impl ReadTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ReadTool {
    fn default() -> Self {
        Self::new()
    }
}

fn render_with_line_numbers(content: &str, offset: usize, limit: Option<usize>) -> String {
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return String::new();
    }

    let start = offset.min(lines.len());
    let end = limit
        .map(|value| start.saturating_add(value).min(lines.len()))
        .unwrap_or(lines.len());

    lines[start..end]
        .iter()
        .enumerate()
        .map(|(idx, line)| format!("{:>6}\t{}", start + idx + 1, line))
        .collect::<Vec<_>>()
        .join("\n")
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "Read a local file with line-numbered output (supports offset/limit). Use this before Edit/Write on existing files."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to read"
                },
                "offset": {
                    "type": "number",
                    "description": "The line number offset to start reading from"
                },
                "limit": {
                    "type": "number",
                    "description": "The number of lines to read"
                }
            },
            "required": ["file_path"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Read"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ReadArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Read args: {}", e)))?;

        let path = Path::new(parsed.file_path.trim());
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read file: {}", e)))?;

        if let Some(session_id) = ctx.session_id {
            read_tracker::mark_read(session_id, parsed.file_path.trim()).await;
        }

        if bytes.contains(&0) {
            return Ok(ToolResult {
                success: true,
                result: "[Binary file omitted]".to_string(),
                display_preference: Some("Collapsible".to_string()),
            });
        }

        let content = String::from_utf8_lossy(&bytes).to_string();
        let rendered = render_with_line_numbers(&content, parsed.offset.unwrap_or(0), parsed.limit);

        Ok(ToolResult {
            success: true,
            result: rendered,
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::WriteTool;

    #[tokio::test]
    async fn binary_read_still_marks_file_as_read_for_session_write_gate() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), vec![0_u8, 1, 2, 3])
            .await
            .unwrap();
        let file_path = file.path().to_string_lossy().to_string();
        let ctx = ToolExecutionContext {
            session_id: Some("session_binary_read"),
            tool_call_id: "call_1",
            event_tx: None,
        };

        let read_tool = ReadTool::new();
        let read_result = read_tool
            .execute_with_context(json!({ "file_path": file_path }), ctx)
            .await
            .unwrap();
        assert!(read_result.success);
        assert!(read_result.result.contains("Binary file omitted"));

        let write_tool = WriteTool::new();
        let write_result = write_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "content": "now text"
                }),
                ctx,
            )
            .await
            .unwrap();
        assert!(write_result.success);
    }
}
