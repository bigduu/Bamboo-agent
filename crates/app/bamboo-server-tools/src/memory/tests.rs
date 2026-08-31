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
        root_session_id: None,
        tool_call_id: "tool-call-1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
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
async fn assigned_project_memory_uses_only_the_canonical_project_store() {
    let dir = tempfile::tempdir().expect("memory dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let project_id =
        bamboo_domain::ProjectId::parse("project-memory-tool").expect("valid Project identity");
    let mut session = Session::new("assigned-project-memory-tool", "model");
    session.set_project_id_meta(project_id.to_string());
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    let tool = build_memory_tool_with_session(dir.path(), session).await;

    let out = tool
        .invoke(
            json!({
                "action": "write",
                "scope": "project",
                "project_key": project_id.as_str(),
                "type": "project",
                "title": "Canonical Project memory",
                "content": "Project memory is keyed only by the assigned ProjectId."
            }),
            test_context("assigned-project-memory-tool"),
        )
        .await
        .expect("assigned Project write should succeed");
    let ToolOutcome::Completed(result) = out else {
        panic!("expected Completed")
    };
    let value: serde_json::Value = serde_json::from_str(&result.result).expect("valid json");
    let path = value["memory"]["path"]
        .as_str()
        .expect("memory path in response");
    assert!(std::path::Path::new(path).starts_with(
        dir.path()
            .join("projects")
            .join(project_id.as_str())
            .join("memory")
            .join("v1")
    ));

    let docs = MemoryStore::new(dir.path())
        .for_project(&project_id)
        .list_memory_documents(MemoryScope::Project, Some(project_id.as_str()))
        .await
        .expect("read canonical Project memory");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].frontmatter.title, "Canonical Project memory");

    let error = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "project",
                "project_key": "project-other"
            }),
            test_context("assigned-project-memory-tool"),
        )
        .await
        .expect_err("an explicit key must not override the assigned Project");
    assert!(matches!(
        error,
        ToolError::InvalidArguments(ref message)
            if message.contains("cannot override the session's assigned Project")
    ));
}

#[tokio::test]
async fn unassigned_workspace_cannot_select_or_discover_project_memory() {
    let dir = tempfile::tempdir().expect("memory dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let project_id =
        bamboo_domain::ProjectId::parse("project-private").expect("valid Project identity");
    let seeded = MemoryStore::new(dir.path())
        .for_project(&project_id)
        .write_memory(
            MemoryScope::Project,
            Some(project_id.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Private Project memory",
            "MUST NOT LEAK TO AN UNASSIGNED WORKSPACE SESSION",
            &[],
            Some("seed-session"),
            "test",
            false,
            None,
        )
        .await
        .expect("seed canonical Project memory");
    let mut session = Session::new("unassigned-workspace-memory-tool", "model");
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    let tool = build_memory_tool_with_session(dir.path(), session).await;

    let query_error = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "project",
                "query": "Private Project memory"
            }),
            test_context("unassigned-workspace-memory-tool"),
        )
        .await
        .expect_err("workspace metadata must not create Project memory scope");
    assert!(matches!(
        query_error,
        ToolError::InvalidArguments(ref message)
            if message.contains("requires an assigned Project")
    ));

    let explicit_error = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "global",
                "project_key": project_id.as_str(),
                "query": "Private Project memory"
            }),
            test_context("unassigned-workspace-memory-tool"),
        )
        .await
        .expect_err("explicit project_key must not create Project memory scope");
    assert!(matches!(
        explicit_error,
        ToolError::InvalidArguments(ref message)
            if message.contains("requires the session to be assigned")
    ));

    let get_error = tool
        .invoke(
            json!({"action": "get", "id": seeded.frontmatter.id}),
            test_context("unassigned-workspace-memory-tool"),
        )
        .await
        .expect_err("unscoped get must not enumerate Project directories");
    assert!(get_error.to_string().contains("memory not found"));
    assert!(!get_error
        .to_string()
        .contains("MUST NOT LEAK TO AN UNASSIGNED WORKSPACE SESSION"));
}

#[tokio::test]
async fn assigned_project_cannot_get_memory_from_an_unrelated_project() {
    let dir = tempfile::tempdir().expect("memory dir");
    let assigned_id =
        bamboo_domain::ProjectId::parse("project-assigned").expect("valid assigned Project");
    let unrelated_id =
        bamboo_domain::ProjectId::parse("project-unrelated").expect("valid unrelated Project");
    let unrelated = MemoryStore::new(dir.path())
        .for_project(&unrelated_id)
        .write_memory(
            MemoryScope::Project,
            Some(unrelated_id.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Unrelated Project memory",
            "MUST NOT LEAK ACROSS PROJECTS",
            &[],
            Some("seed-session"),
            "test",
            false,
            None,
        )
        .await
        .expect("seed unrelated Project memory");
    let mut session = Session::new("assigned-isolation-memory-tool", "model");
    session.set_project_id_meta(assigned_id.to_string());
    let tool = build_memory_tool_with_session(dir.path(), session).await;

    let error = tool
        .invoke(
            json!({"action": "get", "id": unrelated.frontmatter.id}),
            test_context("assigned-isolation-memory-tool"),
        )
        .await
        .expect_err("unscoped get must stay within the assigned Project and Global");
    assert!(error.to_string().contains("memory not found"));
    assert!(!error.to_string().contains("MUST NOT LEAK ACROSS PROJECTS"));
}

#[tokio::test]
async fn malformed_project_identity_cannot_read_canonical_project_memory() {
    let dir = tempfile::tempdir().expect("memory dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let project_id =
        bamboo_domain::ProjectId::parse("project-secret").expect("valid Project identity");
    MemoryStore::new(dir.path())
        .for_project(&project_id)
        .write_memory(
            MemoryScope::Project,
            Some(project_id.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Project secret",
            "MUST NOT LEAK THROUGH MALFORMED PROJECT ID",
            &[],
            Some("seed-session"),
            "test",
            false,
            None,
        )
        .await
        .expect("seed canonical Project memory");
    let mut session = Session::new("malformed-project-memory-tool", "model");
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    session.set_project_id_meta("../malformed".to_string());
    let tool = build_memory_tool_with_session(dir.path(), session).await;

    let error = tool
        .invoke(
            json!({
                "action": "query",
                "scope": "project",
                "project_key": project_id.as_str(),
                "query": "Project secret"
            }),
            test_context("malformed-project-memory-tool"),
        )
        .await
        .expect_err("malformed Project identity must fail before Project lookup");
    assert!(
        matches!(error, ToolError::InvalidArguments(ref message) if message.contains("invalid Project identity"))
    );
    assert!(!error
        .to_string()
        .contains("MUST NOT LEAK THROUGH MALFORMED PROJECT ID"));
}
