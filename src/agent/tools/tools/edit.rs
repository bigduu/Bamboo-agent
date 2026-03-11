use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::{file_change, read_tracker};

#[derive(Debug, Deserialize)]
struct EditArgs {
    file_path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: Option<bool>,
}

pub struct EditTool;

impl EditTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EditTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "Edit"
    }

    fn description(&self) -> &str {
        "Perform exact string replacements in files"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to modify"
                },
                "old_string": {
                    "type": "string",
                    "description": "The text to replace"
                },
                "new_string": {
                    "type": "string",
                    "description": "The replacement text"
                },
                "replace_all": {
                    "type": "boolean",
                    "default": false,
                    "description": "Replace all occurrences"
                }
            },
            "required": ["file_path", "old_string", "new_string"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Edit"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: EditArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Edit args: {}", e)))?;

        let file_path = parsed.file_path.trim();
        let path = Path::new(file_path);
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        if parsed.old_string == parsed.new_string {
            return Err(ToolError::InvalidArguments(
                "new_string must be different from old_string".to_string(),
            ));
        }

        if let Some(session_id) = ctx.session_id {
            if !read_tracker::has_read(session_id, file_path).await {
                return Err(ToolError::Execution(
                    "Edit requires reading the target file first via Read".to_string(),
                ));
            }
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read file: {}", e)))?;

        let matches: Vec<usize> = content
            .match_indices(&parsed.old_string)
            .map(|(index, _)| index)
            .collect();

        if matches.is_empty() {
            return Err(ToolError::Execution(
                "old_string not found in target file".to_string(),
            ));
        }

        let replace_all = parsed.replace_all.unwrap_or(false);
        if !replace_all && matches.len() != 1 {
            return Err(ToolError::Execution(format!(
                "old_string matched {} times; provide a more specific old_string or set replace_all=true",
                matches.len()
            )));
        }

        let updated = if replace_all {
            content.replace(&parsed.old_string, &parsed.new_string)
        } else {
            let first = matches[0];
            let mut next = String::with_capacity(content.len() + parsed.new_string.len());
            next.push_str(&content[..first]);
            next.push_str(&parsed.new_string);
            next.push_str(&content[first + parsed.old_string.len()..]);
            next
        };

        let checkpoint = file_change::create_checkpoint(path, Some(content.as_bytes())).await?;

        tokio::fs::write(path, &updated)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to write file: {}", e)))?;

        let payload = file_change::build_file_change_payload(
            "Edit",
            path,
            format!(
                "Edited file: {} (replacements: {})",
                file_path,
                if replace_all { matches.len() } else { 1 }
            ),
            checkpoint,
            &content,
            &updated,
        );

        Ok(ToolResult {
            success: true,
            result: payload,
            display_preference: Some("Default".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::ReadTool;
    use serde_json::json;

    #[tokio::test]
    async fn edit_requires_unique_match_without_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar"
            }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn edit_supports_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar",
                "replace_all": true
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "bar\nbar\n");
    }

    #[tokio::test]
    async fn edit_requires_read_first_when_session_context_exists() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world\n")
            .await
            .unwrap();
        let call_id = "call_1";

        let edit_tool = EditTool::new();
        let read_tool = ReadTool::new();

        let denied = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await;
        assert!(denied.is_err());

        let _ = read_tool
            .execute_with_context(
                json!({"file_path": file.path()}),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        let allowed = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        assert!(allowed.success);
    }
}
