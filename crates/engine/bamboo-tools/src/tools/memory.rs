//! Persistent session-scoped note tool.
//!
//! Canonical tool name: `memory`.
//!
//! This tool lets the model store (and later retrieve) per-session notes that
//! are loaded into the system prompt at the start of each round.
//!
//! Supports multiple **topics** per session so the model can track separate
//! workstreams without clobbering each other.

use async_trait::async_trait;
use serde_json::json;

use crate::tools::session_memory::{
    execute_session_memory_action, parse_memory_session_action, MEMORY_SESSION_ACTION_NAMES,
};
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome};
use bamboo_memory::memory_store::MemoryStore;

const TOOL_NAME: &str = "memory";
const TOOL_DESCRIPTION: &str = "Read or update Session notes through session_* actions. Keep concise working context and reminders, up to 12000 characters per topic.";

#[derive(Debug, Clone)]
pub struct SessionMemoryTool {
    memory_store: MemoryStore,
}

impl SessionMemoryTool {
    pub fn new() -> Self {
        Self {
            memory_store: MemoryStore::with_defaults(),
        }
    }

    pub fn with_memory_store(memory_store: MemoryStore) -> Self {
        Self { memory_store }
    }
}

impl Default for SessionMemoryTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SessionMemoryTool {
    fn name(&self) -> &str {
        TOOL_NAME
    }

    fn description(&self) -> &str {
        TOOL_DESCRIPTION
    }

    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        let action = args
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match action.as_str() {
            "session_read" | "session_list_topics" => ToolClass::READONLY_PARALLEL,
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Operation to perform on the note.",
                    "enum": ["session_read", "session_append", "session_replace", "session_clear", "session_list_topics"]
                },
                "content": {
                    "type": "string",
                    "description": "Note content to append/replace (markdown). Required for session_append/session_replace."
                },
                "topic": {
                    "type": "string",
                    "description": "Session note topic. Defaults to 'default'."
                },
                "options": {
                    "type": "object",
                    "properties": {
                        "max_chars": {"type": "integer", "minimum": 1, "maximum": 12000}
                    },
                    "additionalProperties": false
                }
            },
            "required": ["action"]
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let Some(session_id) = ctx.session_id() else {
            return Err(ToolError::Execution(
                "missing session_id in tool context".to_string(),
            ));
        };

        let action_raw = args.get("action").and_then(|v| v.as_str()).unwrap_or("");
        let action = parse_memory_session_action(action_raw)?;
        let topic = args.get("topic").and_then(|v| v.as_str());
        let content = args.get("content").and_then(|v| v.as_str());
        let max_chars = args
            .pointer("/options/max_chars")
            .map(|value| serde_json::from_value::<usize>(value.clone()))
            .transpose()
            .map_err(|error| {
                ToolError::InvalidArguments(format!("Invalid options.max_chars: {error}"))
            })?;

        let result = execute_session_memory_action(
            &self.memory_store,
            session_id,
            action,
            topic,
            content,
            max_chars,
            MEMORY_SESSION_ACTION_NAMES,
        )
        .await?;
        Ok(ToolOutcome::Completed(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_reads_existing_session_topics() {
        let directory = tempfile::tempdir().unwrap();
        let store = MemoryStore::new(directory.path());
        store
            .write_session_topic("existing-session", "decisions", "Existing durable note")
            .await
            .unwrap();
        let tool = SessionMemoryTool::with_memory_store(store);
        let mut ctx = ToolCtx::none("read-existing");
        ctx.session_id = Some(std::sync::Arc::from("existing-session"));
        let ToolOutcome::Completed(result) = tool
            .invoke(
                json!({"action":"session_read","topic":"decisions","options":{"max_chars":8}}),
                ctx,
            )
            .await
            .unwrap()
        else {
            panic!("expected completed read");
        };
        let body: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(body["action"], "session_read");
        assert_eq!(body["content"], "Existing");
        assert_eq!(body["body_truncated"], true);
    }

    #[test]
    fn memory_schema_requires_action() {
        let tool = SessionMemoryTool::new();
        let schema = tool.parameters_schema();
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(tool.name(), TOOL_NAME);
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!([
                "session_read",
                "session_append",
                "session_replace",
                "session_clear",
                "session_list_topics"
            ])
        );
    }

    #[test]
    fn memory_schema_has_topic_field() {
        let tool = SessionMemoryTool::new();
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["topic"].is_object());
        assert_eq!(schema["properties"]["topic"]["type"], "string");
    }

    #[tokio::test]
    async fn memory_requires_session_context() {
        let tool = SessionMemoryTool::new();
        let result = tool
            .invoke(
                json!({"action": "session_read"}),
                ToolCtx::none("tool_call"),
            )
            .await;

        assert!(matches!(
            result,
            Err(ToolError::Execution(msg)) if msg.contains("session_id")
        ));
    }

    #[tokio::test]
    async fn memory_validates_action_and_content_before_io() {
        let tool = SessionMemoryTool::new();

        let unknown = tool
            .invoke(
                json!({"action": "unknown"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_unknown"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await;
        assert!(matches!(
            unknown,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("action must be one of")
        ));

        let missing_content = tool
            .invoke(
                json!({"action": "session_replace"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_replace"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await;
        assert!(matches!(
            missing_content,
            Err(ToolError::InvalidArguments(msg)) if msg.contains("content is required")
        ));
    }

    #[tokio::test]
    async fn memory_uses_memory_store_session_topics() {
        let dir = tempfile::tempdir().unwrap();
        let tool = SessionMemoryTool::with_memory_store(MemoryStore::new(dir.path()));

        let append = tool
            .invoke(
                json!({"action": "session_append", "topic": "backend", "content": "API finalized"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_append"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect("append should succeed");
        let ToolOutcome::Completed(append) = append else {
            panic!("expected Completed")
        };
        let append_json: serde_json::Value = serde_json::from_str(&append.result).unwrap();
        assert_eq!(append_json["action"], "session_append");
        assert_eq!(append_json["length_chars"], "API finalized".chars().count());

        let read = tool
            .invoke(
                json!({"action": "session_read", "topic": "backend"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_read"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect("read should succeed");
        let ToolOutcome::Completed(read) = read else {
            panic!("expected Completed")
        };
        let read_json: serde_json::Value = serde_json::from_str(&read.result).unwrap();
        assert_eq!(read_json["action"], "session_read");
        assert_eq!(read_json["content"], "API finalized");
        assert_eq!(read_json["length_chars"], "API finalized".chars().count());
        assert_eq!(read_json["body_truncated"], false);

        let list = tool
            .invoke(
                json!({"action": "session_list_topics"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_list"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect("list should succeed");
        let ToolOutcome::Completed(list) = list else {
            panic!("expected Completed")
        };
        let list_json: serde_json::Value = serde_json::from_str(&list.result).unwrap();
        assert_eq!(list_json["topics"][0], "backend");
        assert_eq!(list_json["count"], 1);

        let clear = tool
            .invoke(
                json!({"action": "session_clear", "topic": "backend"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-1")),
                    tool_call_id: std::sync::Arc::from("tool_call_clear"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect("clear should succeed");
        let ToolOutcome::Completed(clear) = clear else {
            panic!("expected Completed")
        };
        let clear_json: serde_json::Value = serde_json::from_str(&clear.result).unwrap();
        assert_eq!(clear_json["action"], "session_clear");
        assert_eq!(clear_json["deleted"], true);
    }

    #[tokio::test]
    async fn memory_read_reports_truncation_and_append_enforces_limit() {
        let dir = tempfile::tempdir().unwrap();
        let tool = SessionMemoryTool::with_memory_store(MemoryStore::new(dir.path()));
        let long_content = "x".repeat(32);

        tool.invoke(
            json!({"action": "session_replace", "topic": "default", "content": long_content}),
            ToolCtx {
                executing_caller: None,
                executing_supervisor: None,
                session_id: Some(std::sync::Arc::from("session-2")),
                tool_call_id: std::sync::Arc::from("tool_call_replace_long"),
                event_tx: None,
                available_tool_schemas: std::sync::Arc::from(Vec::new()),
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                async_completion_sink: None,
                bash_completion_sink: None,
            },
        )
        .await
        .expect("replace should succeed");

        let read = tool
            .invoke(
                json!({"action": "session_read", "topic": "default"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-2")),
                    tool_call_id: std::sync::Arc::from("tool_call_read_long"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect("read should succeed");
        let ToolOutcome::Completed(read) = read else {
            panic!("expected Completed")
        };
        let read_json: serde_json::Value = serde_json::from_str(&read.result).unwrap();
        assert_eq!(read_json["length_chars"], 32);
        assert_eq!(read_json["body_truncated"], false);

        tool.invoke(
            json!({"action": "session_replace", "topic": "limit", "content": "x".repeat(crate::tools::session_memory::MAX_SESSION_NOTE_CHARS - 1)}),
            ToolCtx {
                executing_caller: None,
                executing_supervisor: None,
                session_id: Some(std::sync::Arc::from("session-3")),
                tool_call_id: std::sync::Arc::from("tool_call_replace_limit"),
                event_tx: None,
                available_tool_schemas: std::sync::Arc::from(Vec::new()),
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                async_completion_sink: None,
                bash_completion_sink: None,
            },
        )
        .await
        .expect("replace near limit should succeed");

        let append_err = tool
            .invoke(
                json!({"action": "session_append", "topic": "limit", "content": "y"}),
                ToolCtx {
                    executing_caller: None,
                    executing_supervisor: None,
                    session_id: Some(std::sync::Arc::from("session-3")),
                    tool_call_id: std::sync::Arc::from("tool_call_append_limit"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .expect_err("append should exceed limit");
        assert!(append_err
            .to_string()
            .contains("session note would exceed the limit"));
    }
}
