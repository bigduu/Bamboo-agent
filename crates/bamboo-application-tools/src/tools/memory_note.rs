//! Persistent session-scoped note tool.
//!
//! Canonical tool name: `session_note`.
//! The legacy `memory_note` name is accepted via executor-level alias routing.
//!
//! This tool lets the model store (and later retrieve) per-session notes that
//! are loaded into the system prompt at the start of each round.
//!
//! Supports multiple **topics** per session so the model can track separate
//! workstreams without clobbering each other.

use async_trait::async_trait;
use serde_json::json;

use bamboo_application_memory::memory_store::MemoryStore;
use bamboo_application_agent::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::tools::session_memory::{
    execute_session_memory_action, parse_session_note_action, SESSION_NOTE_ACTION_NAMES,
};

const TOOL_NAME: &str = "session_note";
const TOOL_DESCRIPTION: &str = "Read or update the persistent session-scoped note (markdown). Use this for durable local context, user preferences, constraints, and compression-resistant reminders within the current session/workstream. Do not use it as the primary long-term knowledge base. Hard limit: 12000 characters; compress before append/replace if needed.";

#[derive(Debug, Clone)]
pub struct SessionNoteTool {
    memory_store: MemoryStore,
}

impl SessionNoteTool {
    pub fn new() -> Self {
        Self {
            memory_store: MemoryStore::with_defaults(),
        }
    }

    #[cfg(test)]
    fn with_memory_store(memory_store: MemoryStore) -> Self {
        Self { memory_store }
    }
}

impl Default for SessionNoteTool {
    fn default() -> Self {
        Self::new()
    }
}

/// Deprecated compatibility alias for older code paths. The canonical tool type
/// and registered tool name is [`SessionNoteTool`] / `session_note`.
#[allow(dead_code)]
pub type MemoryNoteTool = SessionNoteTool;

#[async_trait]
impl Tool for SessionNoteTool {
    fn name(&self) -> &str {
        TOOL_NAME
    }

    fn description(&self) -> &str {
        TOOL_DESCRIPTION
    }

    fn mutability(&self) -> crate::ToolMutability {
        crate::ToolMutability::Mutating
    }

    fn call_mutability(&self, args: &serde_json::Value) -> crate::ToolMutability {
        let action = args
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match action.as_str() {
            "read" | "list_topics" => crate::ToolMutability::ReadOnly,
            _ => crate::ToolMutability::Mutating,
        }
    }

    fn call_concurrency_safe(&self, args: &serde_json::Value) -> bool {
        matches!(
            self.call_mutability(args),
            crate::ToolMutability::ReadOnly
        )
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
        Err(ToolError::Execution(format!(
            "{TOOL_NAME} must be executed with ToolExecutionContext (session_id required)"
        )))
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

        let action_raw = args.get("action").and_then(|v| v.as_str()).unwrap_or("");
        let action = parse_session_note_action(action_raw)?;
        let topic = args.get("topic").and_then(|v| v.as_str());
        let content = args.get("content").and_then(|v| v.as_str());

        execute_session_memory_action(
            &self.memory_store,
            session_id,
            action,
            topic,
            content,
            None,
            SESSION_NOTE_ACTION_NAMES,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_note_schema_requires_action() {
        let tool = SessionNoteTool::new();
        let schema = tool.parameters_schema();
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(tool.name(), TOOL_NAME);
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["read", "append", "replace", "clear", "list_topics"])
        );
    }

    #[test]
    fn session_note_schema_has_topic_field() {
        let tool = SessionNoteTool::new();
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["topic"].is_object());
        assert_eq!(schema["properties"]["topic"]["type"], "string");
    }

    #[tokio::test]
    async fn session_note_requires_session_context() {
        let tool = SessionNoteTool::new();
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
    async fn session_note_validates_action_and_content_before_io() {
        let tool = SessionNoteTool::new();

        let unknown = tool
            .execute_with_context(
                json!({"action": "unknown"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_unknown",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await;
        assert!(matches!(
            unknown,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("action must be one of")
        ));

        let missing_content = tool
            .execute_with_context(
                json!({"action": "replace"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_replace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await;
        assert!(matches!(
            missing_content,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("content is required")
        ));
    }

    #[tokio::test]
    async fn session_note_uses_memory_store_session_topics() {
        let dir = tempfile::tempdir().unwrap();
        let tool = SessionNoteTool::with_memory_store(MemoryStore::new(dir.path()));

        let append = tool
            .execute_with_context(
                json!({"action": "append", "topic": "backend", "content": "API finalized"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_append",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("append should succeed");
        let append_json: serde_json::Value = serde_json::from_str(&append.result).unwrap();
        assert_eq!(append_json["action"], "append");
        assert_eq!(append_json["length_chars"], "API finalized".chars().count());

        let read = tool
            .execute_with_context(
                json!({"action": "read", "topic": "backend"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_read",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("read should succeed");
        let read_json: serde_json::Value = serde_json::from_str(&read.result).unwrap();
        assert_eq!(read_json["action"], "read");
        assert_eq!(read_json["content"], "API finalized");
        assert_eq!(read_json["length_chars"], "API finalized".chars().count());
        assert_eq!(read_json["body_truncated"], false);

        let list = tool
            .execute_with_context(
                json!({"action": "list_topics"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_list",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list should succeed");
        let list_json: serde_json::Value = serde_json::from_str(&list.result).unwrap();
        assert_eq!(list_json["topics"][0], "backend");
        assert_eq!(list_json["count"], 1);

        let clear = tool
            .execute_with_context(
                json!({"action": "clear", "topic": "backend"}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "tool_call_clear",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("clear should succeed");
        let clear_json: serde_json::Value = serde_json::from_str(&clear.result).unwrap();
        assert_eq!(clear_json["action"], "clear");
        assert_eq!(clear_json["deleted"], true);
    }

    #[tokio::test]
    async fn session_note_read_reports_truncation_and_append_enforces_limit() {
        let dir = tempfile::tempdir().unwrap();
        let tool = SessionNoteTool::with_memory_store(MemoryStore::new(dir.path()));
        let long_content = "x".repeat(32);

        tool.execute_with_context(
            json!({"action": "replace", "topic": "default", "content": long_content}),
            ToolExecutionContext {
                session_id: Some("session-2"),
                tool_call_id: "tool_call_replace_long",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("replace should succeed");

        let read = tool
            .execute_with_context(
                json!({"action": "read", "topic": "default"}),
                ToolExecutionContext {
                    session_id: Some("session-2"),
                    tool_call_id: "tool_call_read_long",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("read should succeed");
        let read_json: serde_json::Value = serde_json::from_str(&read.result).unwrap();
        assert_eq!(read_json["length_chars"], 32);
        assert_eq!(read_json["body_truncated"], false);

        tool.execute_with_context(
            json!({"action": "replace", "topic": "limit", "content": "x".repeat(crate::tools::session_memory::MAX_SESSION_NOTE_CHARS - 1)}),
            ToolExecutionContext {
                session_id: Some("session-3"),
                tool_call_id: "tool_call_replace_limit",
                event_tx: None,
                available_tool_schemas: None,
            },
        )
        .await
        .expect("replace near limit should succeed");

        let append_err = tool
            .execute_with_context(
                json!({"action": "append", "topic": "limit", "content": "y"}),
                ToolExecutionContext {
                    session_id: Some("session-3"),
                    tool_call_id: "tool_call_append_limit",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect_err("append should exceed limit");
        assert!(append_err
            .to_string()
            .contains("session note would exceed the limit"));
    }
}
