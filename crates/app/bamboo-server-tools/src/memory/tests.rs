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

async fn build_memory_tool_with_session(
    data_dir: &std::path::Path,
    session: Session,
) -> MemoryTool {
    let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session).await.expect("save session");
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

#[test]
fn parse_query_filters_granularity_absent_or_empty_means_no_filtering() {
    let (_, _, granularity) = MemoryTool::parse_query_filters(None).unwrap();
    assert_eq!(granularity, None);

    let filters = args::QueryFilters {
        r#type: Vec::new(),
        status: Vec::new(),
        granularity: Vec::new(),
    };
    let (_, _, granularity) = MemoryTool::parse_query_filters(Some(&filters)).unwrap();
    assert_eq!(granularity, None);
}

#[test]
fn parse_query_filters_parses_known_granularities_case_insensitively() {
    let filters = args::QueryFilters {
        r#type: Vec::new(),
        status: Vec::new(),
        granularity: vec!["Week".to_string(), " year ".to_string()],
    };
    let (_, _, granularity) = MemoryTool::parse_query_filters(Some(&filters)).unwrap();
    let granularity = granularity.expect("filter should be Some");
    assert_eq!(granularity.len(), 2);
    assert!(granularity.contains(&bamboo_memory::memory_store::TemporalGranularity::Week));
    assert!(granularity.contains(&bamboo_memory::memory_store::TemporalGranularity::Year));
}

#[test]
fn parse_query_filters_partial_filter_object_leaves_other_dimensions_unfiltered() {
    // Regression guard: setting ONLY `granularity` on a `filters` object must not
    // implicitly turn the omitted `type`/`status` sub-lists into an empty (and
    // therefore match-nothing) filter.
    let filters = args::QueryFilters {
        r#type: Vec::new(),
        status: Vec::new(),
        granularity: vec!["week".to_string()],
    };
    let (filter_types, filter_statuses, filter_granularity) =
        MemoryTool::parse_query_filters(Some(&filters)).unwrap();
    assert_eq!(
        filter_types, None,
        "empty type sub-list must stay unfiltered"
    );
    assert_eq!(
        filter_statuses, None,
        "empty status sub-list must stay unfiltered"
    );
    assert!(filter_granularity.is_some());
}

#[test]
fn parse_query_filters_rejects_unknown_granularity() {
    let filters = args::QueryFilters {
        r#type: Vec::new(),
        status: Vec::new(),
        granularity: vec!["decade".to_string()],
    };
    assert!(MemoryTool::parse_query_filters(Some(&filters)).is_err());
}

#[tokio::test]
async fn query_action_filters_by_granularity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());

    tool.invoke(
        json!({
            "action": "write",
            "scope": "global",
            "type": "project",
            "title": "This week's sprint priorities",
            "content": "Ship the granularity filter end to end.",
            "granularity": "week"
        }),
        test_context("session-granularity"),
    )
    .await
    .expect("write week memory should succeed");
    tool.invoke(
        json!({
            "action": "write",
            "scope": "global",
            "type": "project",
            "title": "Long-term architecture direction",
            "content": "Move to a modular workspace layout over the year.",
            "granularity": "year"
        }),
        test_context("session-granularity"),
    )
    .await
    .expect("write year memory should succeed");

    let out = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "global",
                "filters": {"granularity": ["week"]}
            }),
            test_context("session-granularity"),
        )
        .await
        .expect("filtered query should succeed");
    let ToolOutcome::Completed(result) = out else {
        panic!("expected Completed")
    };
    let value: serde_json::Value = serde_json::from_str(&result.result).expect("valid json");
    let items = value["data"]["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "only the week-granularity memory matches");
    assert_eq!(items[0]["title"], "This week's sprint priorities");

    // A `filters` object that sets ONLY `granularity` (leaving `type`/`status` at
    // their empty defaults) must not accidentally filter by the absent fields too
    // — regression guard for a bug this test caught during development, where
    // `filter_types`/`filter_statuses` became `Some(<empty set>)` (matching
    // nothing) whenever `filters` was present at all, regardless of whether their
    // own sub-lists were populated.
    assert_eq!(value["data"]["matched_count"], 1);

    // Absent filter = old behavior: both memories are returned.
    let out = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "global"
            }),
            test_context("session-granularity"),
        )
        .await
        .expect("unfiltered query should succeed");
    let ToolOutcome::Completed(result) = out else {
        panic!("expected Completed")
    };
    let value: serde_json::Value = serde_json::from_str(&result.result).expect("valid json");
    assert_eq!(value["data"]["matched_count"], 2);
}

#[tokio::test]
async fn malformed_project_identity_cannot_read_path_derived_legacy_memory() {
    let dir = tempfile::tempdir().expect("memory dir");
    let workspace = tempfile::tempdir().expect("legacy workspace");
    let legacy_key = bamboo_memory::memory_store::project_key_from_path(workspace.path());
    let store = MemoryStore::new(dir.path());
    store
        .write_memory(
            MemoryScope::Project,
            Some(&legacy_key),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Legacy secret",
            "MUST NOT LEAK THROUGH MALFORMED PROJECT ID",
            &[],
            Some("seed-session"),
            "test",
            false,
            None,
        )
        .await
        .expect("seed legacy memory");
    let mut session = Session::new("malformed-project-memory-tool", "model");
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    session.set_project_id_meta("../malformed".to_string());
    let tool = build_memory_tool_with_session(dir.path(), session).await;

    let error = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "project",
                "project_key": legacy_key,
                "query": "Legacy secret"
            }),
            test_context("malformed-project-memory-tool"),
        )
        .await
        .expect_err("malformed Project identity must fail before legacy lookup");
    assert!(
        matches!(error, ToolError::InvalidArguments(ref message) if message.contains("invalid Project identity"))
    );
    assert!(!error
        .to_string()
        .contains("MUST NOT LEAK THROUGH MALFORMED PROJECT ID"));
}
