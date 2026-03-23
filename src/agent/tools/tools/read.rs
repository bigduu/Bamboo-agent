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

fn slice_bounds(total: usize, offset: usize, limit: Option<usize>) -> (usize, usize) {
    let start = offset.min(total);
    let end = limit
        .map(|value| start.saturating_add(value).min(total))
        .unwrap_or(total);
    (start, end)
}

fn continuation_hint(
    noun: &str,
    start: usize,
    end: usize,
    total: usize,
    limit: Option<usize>,
) -> Option<String> {
    if end >= total {
        return None;
    }

    let shown = end.saturating_sub(start);
    let limit_fragment = match limit {
        Some(value) => format!(", limit={value}"),
        None => String::new(),
    };

    if shown == 0 {
        return Some(format!(
            "[TRUNCATED] No {noun} returned. Continue with offset={end}{limit_fragment}"
        ));
    }

    Some(format!(
        "[TRUNCATED] Showing {noun} {first}-{end} of {total}. Continue with offset={end}{limit_fragment}",
        first = start + 1
    ))
}

fn render_file_with_line_numbers(content: &str, offset: usize, limit: Option<usize>) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let (start, end) = slice_bounds(lines.len(), offset, limit);

    let mut rendered = lines[start..end]
        .iter()
        .enumerate()
        .map(|(idx, line)| format!("{:>6}\t{}", start + idx + 1, line))
        .collect::<Vec<_>>()
        .join("\n");

    if let Some(hint) = continuation_hint("lines", start, end, lines.len(), limit) {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&hint);
    }

    rendered
}

fn render_directory_entries(entries: &[String], offset: usize, limit: Option<usize>) -> String {
    let (start, end) = slice_bounds(entries.len(), offset, limit);
    let mut rendered = entries[start..end]
        .iter()
        .enumerate()
        .map(|(idx, entry)| format!("{:>6}\t{}", start + idx + 1, entry))
        .collect::<Vec<_>>()
        .join("\n");

    if let Some(hint) = continuation_hint("entries", start, end, entries.len(), limit) {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&hint);
    }

    rendered
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "Read a local file or directory with line-numbered output (supports offset/limit). Use this before Edit/Write on existing files."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file or directory to read"
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

        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read path: {}", e)))?;

        if metadata.is_dir() {
            let mut dir = tokio::fs::read_dir(path)
                .await
                .map_err(|e| ToolError::Execution(format!("Failed to read directory: {}", e)))?;
            let mut entries = Vec::new();
            while let Some(entry) = dir
                .next_entry()
                .await
                .map_err(|e| ToolError::Execution(format!("Failed to iterate directory: {}", e)))?
            {
                let mut name = entry.file_name().to_string_lossy().to_string();
                if entry
                    .file_type()
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to inspect entry: {}", e)))?
                    .is_dir()
                {
                    name.push('/');
                }
                entries.push(name);
            }
            entries.sort();

            let rendered =
                render_directory_entries(&entries, parsed.offset.unwrap_or(0), parsed.limit);
            return Ok(ToolResult {
                success: true,
                result: rendered,
                display_preference: Some("Collapsible".to_string()),
            });
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
        let rendered =
            render_file_with_line_numbers(&content, parsed.offset.unwrap_or(0), parsed.limit);

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
    use serde_json::json;

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

    #[tokio::test]
    async fn read_directory_supports_offset_limit_and_marks_subdirs() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("b-dir"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("a.txt"), "a")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("c.txt"), "c")
            .await
            .unwrap();

        let tool = ReadTool::new();
        let result = tool
            .execute(json!({
                "file_path": dir.path(),
                "offset": 1,
                "limit": 1
            }))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("b-dir/"));
        assert!(result.result.contains("TRUNCATED"));
    }

    #[tokio::test]
    async fn read_file_adds_continuation_hint_when_truncated() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "l1\nl2\nl3\n").await.unwrap();

        let tool = ReadTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "offset": 0,
                "limit": 1
            }))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("l1"));
        assert!(result.result.contains("Continue with offset=1"));
    }
}
