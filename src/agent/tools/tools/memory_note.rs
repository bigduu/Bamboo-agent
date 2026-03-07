//! Persistent external memory note tool.
//!
//! This tool lets the model store (and later retrieve) a per-session note that
//! is loaded into the system prompt at the start of each round.

use async_trait::async_trait;
use serde_json::json;

use crate::agent::core::memory::ExternalMemory;
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};

const MAX_NOTE_CHARS: usize = 12_000;

#[derive(Debug, Default)]
pub struct MemoryNoteTool;

impl MemoryNoteTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for MemoryNoteTool {
    fn name(&self) -> &str {
        "memory_note"
    }

    fn description(&self) -> &str {
        "Read or update the persistent per-session memory note (markdown). Use this to store durable facts/preferences/decisions across turns. If the note would exceed the length limit, compress it (rewrite more concisely) and try again."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Operation to perform on the note.",
                    "enum": ["read", "append", "replace", "clear"]
                },
                "content": {
                    "type": "string",
                    "description": "Note content to append/replace (markdown). Required for append/replace."
                }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "memory_note must be executed with ToolExecutionContext (session_id required)"
                .to_string(),
        ))
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let Some(session_id) = ctx.session_id else {
            return Err(ToolError::Execution(
                "missing session_id in tool context".to_string(),
            ));
        };

        let action = args
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();

        let memory = ExternalMemory::with_defaults();

        match action.as_str() {
            "read" => {
                let content = memory
                    .read_note(session_id)
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to read note: {e}")))?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "exists": content.is_some(),
                        "content": content.unwrap_or_default(),
                        "max_chars": MAX_NOTE_CHARS
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            }
            "clear" => {
                let deleted = memory
                    .delete_note(session_id)
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to delete note: {e}")))?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "deleted": deleted
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            }
            "replace" | "append" => {
                let content = args
                    .get("content")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "content is required for action=append|replace".to_string(),
                        )
                    })?;

                if action == "replace" {
                    if content.chars().count() > MAX_NOTE_CHARS {
                        return Err(ToolError::Execution(format!(
                            "memory note too long (>{} chars). Compress it (rewrite more concisely) and call memory_note with action=replace again.",
                            MAX_NOTE_CHARS
                        )));
                    }

                    let path = memory
                        .save_note(session_id, content)
                        .await
                        .map_err(|e| ToolError::Execution(format!("Failed to write note: {e}")))?;

                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "session_id": session_id,
                            "action": "replace",
                            "path": path,
                            "length_chars": content.chars().count(),
                            "max_chars": MAX_NOTE_CHARS
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                    })
                } else {
                    let existing = memory
                        .read_note(session_id)
                        .await
                        .map_err(|e| ToolError::Execution(format!("Failed to read note: {e}")))?;

                    let mut next = existing.unwrap_or_default();
                    if !next.is_empty() {
                        next.push_str("\n\n");
                    }
                    next.push_str(content);

                    let next_len = next.chars().count();
                    if next_len > MAX_NOTE_CHARS {
                        return Err(ToolError::Execution(format!(
                            "memory note would exceed the limit ({}>{} chars). Compress the existing note (use memory_note action=read), then call memory_note action=replace with a shorter version, then append again if needed.",
                            next_len, MAX_NOTE_CHARS
                        )));
                    }

                    let path = memory
                        .save_note(session_id, &next)
                        .await
                        .map_err(|e| ToolError::Execution(format!("Failed to write note: {e}")))?;

                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "session_id": session_id,
                            "action": "append",
                            "path": path,
                            "length_chars": next_len,
                            "max_chars": MAX_NOTE_CHARS
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                    })
                }
            }
            _ => Err(ToolError::InvalidArguments(
                "action must be one of: read, append, replace, clear".to_string(),
            )),
        }
    }
}
