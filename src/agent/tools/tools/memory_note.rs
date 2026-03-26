//! Persistent external memory note tool.
//!
//! This tool lets the model store (and later retrieve) per-session notes that
//! are loaded into the system prompt at the start of each round.
//!
//! Supports multiple **topics** per session so the model can track separate
//! workstreams without clobbering each other.

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::json;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

use crate::agent::core::memory::{ExternalMemory, DEFAULT_TOPIC};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};

const MAX_NOTE_CHARS: usize = 12_000;

fn note_locks() -> &'static DashMap<String, Arc<Mutex<()>>> {
    static NOTE_LOCKS: OnceLock<DashMap<String, Arc<Mutex<()>>>> = OnceLock::new();
    NOTE_LOCKS.get_or_init(DashMap::new)
}

fn session_lock(session_id: &str) -> Arc<Mutex<()>> {
    note_locks()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

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
        "Read or update the persistent per-session memory note (markdown). Use this to store durable facts/preferences/decisions across turns. Hard limit: 12000 characters; compress before append/replace if needed."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Operation to perform on the note.",
                    "enum": ["read", "append", "replace", "clear", "list_topics"]
                },
                "content": {
                    "type": "string",
                    "description": "Note content to append/replace (markdown). Required for append/replace."
                },
                "topic": {
                    "type": "string",
                    "description": "Optional topic name (alphanumeric/dash/underscore, max 50 chars). Defaults to 'default'. Use separate topics for unrelated workstreams."
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

        let topic = args
            .get("topic")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or(DEFAULT_TOPIC);

        let memory = ExternalMemory::with_defaults();
        let session_guard = session_lock(session_id);
        let _guard = session_guard.lock().await;

        match action.as_str() {
            "read" => {
                let content = memory
                    .read_topic(session_id, topic)
                    .await
                    .map_err(|e| {
                        ToolError::Execution(format!(
                            "Failed to read note: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"read\",\"topic\":\"{topic}\"}}."
                        ))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "topic": topic,
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
                    .delete_topic(session_id, topic)
                    .await
                    .map_err(|e| {
                        ToolError::Execution(format!(
                            "Failed to delete note: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"clear\",\"topic\":\"{topic}\"}}."
                        ))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "topic": topic,
                        "deleted": deleted
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            }
            "list_topics" => {
                let topics = memory
                    .list_topics(session_id)
                    .await
                    .map_err(|e| {
                        ToolError::Execution(format!(
                            "Failed to list topics: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"list_topics\"}}."
                        ))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "topics": topics,
                        "count": topics.len()
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
                            "content is required for action=append|replace. Rewrite the memory_note call with valid JSON and include non-empty content."
                                .to_string(),
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
                        .save_topic(session_id, topic, content)
                        .await
                        .map_err(|e| {
                            ToolError::Execution(format!(
                                "Failed to write note: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"replace\",\"topic\":\"{topic}\",\"content\":\"...\"}}."
                            ))
                        })?;

                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "session_id": session_id,
                            "topic": topic,
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
                        .read_topic(session_id, topic)
                        .await
                        .map_err(|e| {
                            ToolError::Execution(format!(
                                "Failed to read note: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"append\",\"topic\":\"{topic}\",\"content\":\"...\"}}."
                            ))
                        })?;

                    let mut next = existing.unwrap_or_default();
                    if !next.is_empty() {
                        next.push_str("\n\n");
                    }
                    next.push_str(content);

                    let next_len = next.chars().count();
                    if next_len > MAX_NOTE_CHARS {
                        return Err(ToolError::Execution(format!(
                            "memory note would exceed the limit ({}>{} chars). Compress the existing note (use memory_note action=read topic={topic}), then call memory_note action=replace with a shorter version, then append again if needed.",
                            next_len, MAX_NOTE_CHARS
                        )));
                    }

                    let path = memory
                        .save_topic(session_id, topic, &next)
                        .await
                        .map_err(|e| {
                            ToolError::Execution(format!(
                                "Failed to write note: {e}. Rewrite and retry memory_note with valid JSON, e.g. {{\"action\":\"append\",\"topic\":\"{topic}\",\"content\":\"...\"}}."
                            ))
                        })?;

                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "session_id": session_id,
                            "topic": topic,
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
                "action must be one of: read, append, replace, clear, list_topics. Rewrite the memory_note call with valid JSON."
                    .to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_note_schema_requires_action() {
        let tool = MemoryNoteTool::new();
        let schema = tool.parameters_schema();
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["read", "append", "replace", "clear", "list_topics"])
        );
    }

    #[test]
    fn memory_note_schema_has_topic_field() {
        let tool = MemoryNoteTool::new();
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["topic"].is_object());
        assert_eq!(schema["properties"]["topic"]["type"], "string");
    }

    #[tokio::test]
    async fn memory_note_requires_session_context() {
        let tool = MemoryNoteTool::new();
        let result = tool
            .execute_with_context(
                json!({"action": "read"}),
                ToolExecutionContext::none("tool_call"),
            )
            .await;

        assert!(matches!(
            result,
            Err(ToolError::Execution(msg)) if msg.contains("session_id")
        ));
    }

    #[tokio::test]
    async fn memory_note_validates_action_and_content_before_io() {
        let tool = MemoryNoteTool::new();
        let ctx = ToolExecutionContext {
            session_id: Some("session-1"),
            tool_call_id: "tool_call",
            event_tx: None,
        };

        let unknown = tool
            .execute_with_context(json!({"action": "unknown"}), ctx)
            .await;
        assert!(matches!(
            unknown,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("action must be one of")
        ));

        let missing_content = tool
            .execute_with_context(json!({"action": "replace"}), ctx)
            .await;
        assert!(matches!(
            missing_content,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("content is required")
        ));
    }
}
