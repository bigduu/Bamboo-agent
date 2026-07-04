use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCtx, ToolExecutionContext};
use bamboo_storage::LockedSessionStore;
use tokio::sync::RwLock;

#[derive(Default)]
struct TestStorage {
    sessions: RwLock<HashMap<String, Session>>,
}

#[async_trait]
impl Storage for TestStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        Ok(self.sessions.read().await.get(session_id).cloned())
    }

    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        Ok(self.sessions.write().await.remove(session_id).is_some())
    }
}

fn test_context(session_id: &str) -> ToolCtx {
    ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    }
    .to_tool_ctx()
}

fn build_memory_tool(data_dir: &std::path::Path) -> MemoryTool {
    let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let session_repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    MemoryTool::new(session_repo, data_dir)
}

#[tokio::test]
async fn memory_session_actions_share_read_shape_and_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());

    tool.invoke(
        json!({"action":"session_replace","topic":"default","content":"x".repeat(32)}),
        test_context("session-1"),
    )
    .await
    .expect("session replace should succeed");

    let out = tool
        .invoke(
            json!({"action":"session_read","topic":"default","options":{"max_chars":8}}),
            test_context("session-1"),
        )
        .await
        .expect("session read should succeed");
    let ToolOutcome::Completed(read) = out else {
        panic!("expected Completed")
    };
    let value: serde_json::Value = serde_json::from_str(&read.result).expect("valid json");
    assert_eq!(value["action"], "session_read");
    assert_eq!(value["length_chars"], 32);
    assert_eq!(value["body_truncated"], true);
    assert_eq!(value["content"].as_str().unwrap().chars().count(), 8);
}

#[tokio::test]
async fn memory_session_append_enforces_shared_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());

    tool.invoke(
        json!({
            "action":"session_replace",
            "topic":"limit",
            "content":"x".repeat(bamboo_tools::tools::session_memory::MAX_SESSION_NOTE_CHARS - 1)
        }),
        test_context("session-2"),
    )
    .await
    .expect("session replace near limit should succeed");

    let err = tool
        .invoke(
            json!({"action":"session_append","topic":"limit","content":"y"}),
            test_context("session-2"),
        )
        .await
        .expect_err("session append should fail");
    let message = err.to_string();
    assert!(message.contains("session note would exceed the limit"));
    assert!(message.contains("action=session_read"));
    assert!(message.contains("action=session_replace"));
}

#[tokio::test]
async fn memory_session_list_topics_includes_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());

    tool.invoke(
        json!({"action":"session_append","topic":"alpha","content":"A"}),
        test_context("session-3"),
    )
    .await
    .expect("session append should succeed");
    tool.invoke(
        json!({"action":"session_append","topic":"beta","content":"B"}),
        test_context("session-3"),
    )
    .await
    .expect("session append should succeed");

    let out = tool
        .invoke(
            json!({"action":"session_list_topics"}),
            test_context("session-3"),
        )
        .await
        .expect("session list topics should succeed");
    let ToolOutcome::Completed(list) = out else {
        panic!("expected Completed")
    };
    let value: serde_json::Value = serde_json::from_str(&list.result).expect("valid json");
    assert_eq!(value["action"], "session_list_topics");
    assert_eq!(value["count"], 2);
}

#[test]
fn parse_granularity_accepts_known_values_case_insensitively() {
    assert_eq!(
        MemoryTool::parse_granularity(Some("Week")).unwrap(),
        Some(bamboo_memory::memory_store::TemporalGranularity::Week)
    );
    assert_eq!(
        MemoryTool::parse_granularity(Some("  YEAR ")).unwrap(),
        Some(bamboo_memory::memory_store::TemporalGranularity::Year)
    );
}

#[test]
fn parse_granularity_none_or_empty_is_none() {
    assert_eq!(MemoryTool::parse_granularity(None).unwrap(), None);
    assert_eq!(MemoryTool::parse_granularity(Some("   ")).unwrap(), None);
}

#[test]
fn parse_granularity_rejects_unknown_value() {
    assert!(MemoryTool::parse_granularity(Some("decade")).is_err());
}
