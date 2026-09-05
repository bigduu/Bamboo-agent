use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCtx, ToolExecutionContext};
use bamboo_metrics::storage::MetricsStorage;
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
    let sessions: bamboo_engine::SessionCache = Arc::default();
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let session_repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    MemoryTool::new(session_repo, data_dir)
}

async fn build_memory_tool_with_session(
    data_dir: &std::path::Path,
    session: Session,
) -> MemoryTool {
    let sessions: bamboo_engine::SessionCache = Arc::default();
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session).await.expect("save session");
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let session_repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    MemoryTool::new(session_repo, data_dir)
}

fn completed_json(outcome: ToolOutcome) -> serde_json::Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected Completed")
    };
    serde_json::from_str(&result.result).expect("valid tool JSON")
}

async fn invoke_json(
    tool: &MemoryTool,
    args: serde_json::Value,
    session_id: &str,
) -> serde_json::Value {
    completed_json(
        tool.invoke(args, test_context(session_id))
            .await
            .expect("memory action should succeed"),
    )
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

#[test]
fn memory_schema_teaches_the_llm_native_recall_and_authority_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());
    let description = tool.description().to_ascii_lowercase();

    assert!(description.contains("short lexical query"));
    assert!(description.contains("compact top-3"));
    assert!(description.contains("no bodies"));
    assert!(description.contains("get only selected ids"));
    assert!(description.contains("query before write/merge"));
    assert!(description.contains("one atomic confirmed fact"));
    assert!(description.contains("trusted scope authority"));
    assert!(description.contains("verify live state"));
    assert!(description.contains("embedding-free"));

    let schema = tool.parameters_schema();
    let properties = &schema["properties"];
    assert_eq!(
        properties["query"]["maxLength"],
        json!(MAX_MEMORY_QUERY_CHARS)
    );
    assert_eq!(properties["id"]["minLength"], 1);
    assert_eq!(
        properties["options"]["properties"]["limit"]["default"],
        json!(DEFAULT_QUERY_LIMIT)
    );
    assert_eq!(
        properties["options"]["properties"]["limit"]["maximum"],
        json!(MAX_QUERY_LIMIT)
    );
    assert_eq!(properties["tags"]["maxItems"], json!(MAX_MEMORY_TAGS));
    assert_eq!(
        properties["keywords"]["maxItems"],
        json!(MAX_EXPLICIT_MEMORY_KEYWORDS)
    );
    assert_eq!(
        properties["entities"]["maxItems"],
        json!(MAX_EXPLICIT_MEMORY_ENTITIES)
    );
    assert_eq!(
        properties["pieces"]["items"]["properties"]["keywords"]["maxItems"],
        json!(MAX_EXPLICIT_MEMORY_KEYWORDS)
    );
    assert!(properties["project_key"]["description"]
        .as_str()
        .expect("project authority description")
        .contains("cannot grant access or switch projects"));
}

#[tokio::test]
async fn lexical_query_defaults_to_compact_top_three_without_bodies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());
    let session_id = "compact-query-session";

    for index in 0..4 {
        invoke_json(
            &tool,
            json!({
                "action": "write",
                "scope": "global",
                "type": "project",
                "title": format!("Release freeze fact {index}"),
                "content": format!("Confirmed release freeze detail number {index}."),
                "keywords": ["release-freeze-alias"]
            }),
            session_id,
        )
        .await;
    }

    let value = invoke_json(
        &tool,
        json!({
            "action": "query",
            "scope": "global",
            "query": "release-freeze-alias"
        }),
        session_id,
    )
    .await;
    let items = value["data"]["items"].as_array().expect("query items");
    assert_eq!(value["data"]["matched_count"], 4);
    assert_eq!(value["data"]["returned_count"], DEFAULT_QUERY_LIMIT);
    assert_eq!(items.len(), DEFAULT_QUERY_LIMIT);
    for item in items {
        let item = item.as_object().expect("compact query item");
        assert!(item.get("id").and_then(|value| value.as_str()).is_some());
        assert!(item
            .get("summary")
            .and_then(|value| value.as_str())
            .is_some());
        assert!(!item.contains_key("body"));
        assert!(!item.contains_key("path"));
        assert!(!item.contains_key("frontmatter"));
        assert!(!item.contains_key("keywords"));
        assert!(!item.contains_key("entities"));
    }

    let listing = invoke_json(
        &tool,
        json!({
            "action": "query",
            "scope": "global",
            "query": "  ",
            "options": {"limit": 4}
        }),
        session_id,
    )
    .await;
    assert_eq!(listing["data"]["matched_count"], 4);
    assert_eq!(
        listing["data"]["items"]
            .as_array()
            .expect("management listing")
            .len(),
        4
    );
}

#[tokio::test]
async fn multilingual_retrieval_metadata_round_trips_through_query_and_get() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());
    let session_id = "multilingual-memory-session";
    let round_id = "management-only-round";
    let metrics_storage = Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
        dir.path().join("metrics.db"),
    ));
    metrics_storage.init().await.expect("metrics init");
    let collector = bamboo_metrics::MetricsCollector::spawn(metrics_storage.clone(), 7);
    collector.session_started(session_id, "test-model", chrono::Utc::now());
    collector.round_started(round_id, session_id, "test-model", chrono::Utc::now());

    let written = invoke_json(
        &tool,
        json!({
            "action": "write",
            "scope": "global",
            "type": "reference",
            "title": "Suzaku transport alias",
            "content": "The confirmed transport alias for 朱雀 is Suzaku.",
            "tags": ["MCP 认证", "Transport"],
            "keywords": ["transport-alias", "朱雀别名", "MCP-朱雀"],
            "entities": ["ＡＰＩ 网关", "Project Suzaku"]
        }),
        session_id,
    )
    .await;
    let id = written["memory"]["id"]
        .as_str()
        .expect("written memory id")
        .to_string();

    let queried = invoke_json(
        &tool,
        json!({
            "action": "query",
            "scope": "global",
            "query": "朱雀别名"
        }),
        session_id,
    )
    .await;
    assert_eq!(queried["data"]["items"][0]["id"], id);
    assert!(queried["data"]["items"][0].get("body").is_none());

    let fetched = invoke_json(&tool, json!({"action": "get", "id": id}), session_id).await;
    let frontmatter = &fetched["memory"]["frontmatter"];
    let tags = frontmatter["tags"].as_array().expect("tags");
    let keywords = frontmatter["retrieval"]["keywords"]
        .as_array()
        .expect("keywords");
    let entities = frontmatter["retrieval"]["entities"]
        .as_array()
        .expect("entities");
    assert!(tags.contains(&json!("mcp-认证")));
    assert!(tags.contains(&json!("transport")));
    assert!(keywords.contains(&json!("transport-alias")));
    assert!(keywords.contains(&json!("朱雀别名")));
    assert!(keywords.contains(&json!("MCP-朱雀")));
    assert!(entities.contains(&json!("API 网关")));
    assert!(entities.contains(&json!("Project Suzaku")));
    assert_eq!(fetched["memory"]["body_truncated"], false);
    assert_eq!(fetched["memory"]["retrieval_metadata_truncated"], false);
    invoke_json(
        &tool,
        json!({"action": "inspect", "scope": "global"}),
        session_id,
    )
    .await;
    invoke_json(
        &tool,
        json!({"action": "purge", "id": id, "mode": "archived"}),
        session_id,
    )
    .await;

    collector.session_message_count(session_id, 1, chrono::Utc::now());
    let mut barrier_reached = false;
    for _ in 0..100 {
        if metrics_storage
            .session_detail(session_id)
            .await
            .expect("metrics barrier query")
            .is_some_and(|detail| detail.session.message_count == 1)
        {
            barrier_reached = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(barrier_reached, "metrics FIFO barrier did not persist");
    assert!(
        metrics_storage
            .prompt_memory_exposure(round_id)
            .await
            .expect("query management exposure")
            .is_none(),
        "MemoryTool management actions must not count as provider prompt exposure"
    );
}

#[tokio::test]
async fn get_bounds_body_and_retrieval_metadata_independently_without_rewriting_canonical() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = MemoryStore::new(dir.path());
    let tool = build_memory_tool(dir.path());
    let session_id = "bounded-get-session";

    let body_doc = store
        .write_memory_with_retrieval(
            MemoryScope::Global,
            None,
            bamboo_memory::memory_store::DurableMemoryType::Reference,
            "Long body",
            &"body".repeat(32),
            &["bounded".to_string()],
            &MemoryRetrievalInput {
                keywords: vec!["body-bound".to_string()],
                entities: vec!["Body Entity".to_string()],
            },
            Some(session_id),
            "test",
            false,
            None,
        )
        .await
        .expect("write long body");
    let body_result = invoke_json(
        &tool,
        json!({
            "action": "get",
            "id": body_doc.frontmatter.id,
            "options": {"max_chars": 8}
        }),
        session_id,
    )
    .await;
    assert_eq!(body_result["memory"]["body_truncated"], true);
    assert_eq!(body_result["memory"]["retrieval_metadata_truncated"], false);
    assert_eq!(
        body_result["memory"]["body"]
            .as_str()
            .expect("bounded body")
            .chars()
            .count(),
        8
    );

    let mut metadata_doc = store
        .write_memory(
            MemoryScope::Global,
            None,
            bamboo_memory::memory_store::DurableMemoryType::Reference,
            "Oversized legacy metadata",
            "short body",
            &[],
            Some(session_id),
            "test",
            false,
            None,
        )
        .await
        .expect("write metadata fixture");
    metadata_doc.frontmatter.tags = (0..MAX_MEMORY_TAGS + 4)
        .map(|index| format!("legacy tag {index}"))
        .collect();
    metadata_doc.frontmatter.retrieval.keywords = (0..MAX_MEMORY_KEYWORDS + 4)
        .map(|index| format!("legacy keyword {index} {}", "x".repeat(120)))
        .collect();
    metadata_doc.frontmatter.retrieval.entities = (0..MAX_MEMORY_ENTITIES + 4)
        .map(|index| format!("legacy entity {index} {}", "y".repeat(120)))
        .collect();
    let canonical = format!(
        "---\n{}\n---\n{}\n",
        serde_json::to_string_pretty(&metadata_doc.frontmatter).expect("serialize fixture"),
        metadata_doc.body
    );
    std::fs::write(&metadata_doc.path, &canonical).expect("write oversized canonical fixture");

    let metadata_result = invoke_json(
        &tool,
        json!({"action": "get", "id": metadata_doc.frontmatter.id}),
        session_id,
    )
    .await;
    assert_eq!(metadata_result["memory"]["body_truncated"], false);
    assert_eq!(
        metadata_result["memory"]["retrieval_metadata_truncated"],
        true
    );
    let bounded = &metadata_result["memory"]["frontmatter"];
    assert_eq!(
        bounded["tags"].as_array().expect("bounded tags").len(),
        MAX_MEMORY_TAGS
    );
    assert_eq!(
        bounded["retrieval"]["keywords"]
            .as_array()
            .expect("bounded keywords")
            .len(),
        MAX_MEMORY_KEYWORDS
    );
    assert_eq!(
        bounded["retrieval"]["entities"]
            .as_array()
            .expect("bounded entities")
            .len(),
        MAX_MEMORY_ENTITIES
    );
    assert!(bounded["retrieval"]["keywords"]
        .as_array()
        .expect("bounded keywords")
        .iter()
        .all(|value| value.as_str().expect("keyword").chars().count() <= MAX_RETRIEVAL_TERM_CHARS));
    assert_eq!(
        std::fs::read_to_string(&metadata_doc.path).expect("read canonical after get"),
        canonical,
        "response bounding must not rewrite canonical memory"
    );
}

#[tokio::test]
async fn retrieval_metadata_is_forwarded_through_all_model_driven_mutations() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tool = build_memory_tool(dir.path());
    let session_id = "retrieval-mutation-session";

    let written = invoke_json(
        &tool,
        json!({
            "action": "write",
            "scope": "global",
            "type": "reference",
            "title": "Canonical release channel",
            "content": "The confirmed release channel is stable.",
            "keywords": ["write-forward-alias"],
            "entities": ["Release Channel"]
        }),
        session_id,
    )
    .await;
    let target_id = written["memory"]["id"]
        .as_str()
        .expect("write id")
        .to_string();

    let duplicates = invoke_json(
        &tool,
        json!({
            "action": "find_duplicates",
            "scope": "global",
            "title": "Candidate with different prose",
            "content": "A separate wording supplied for duplicate review.",
            "keywords": ["write-forward-alias"],
            "entities": ["Release Channel"]
        }),
        session_id,
    )
    .await;
    assert!(duplicates["candidates"]
        .as_array()
        .expect("duplicate candidates")
        .iter()
        .any(|candidate| candidate["id"] == target_id));

    invoke_json(
        &tool,
        json!({
            "action": "merge",
            "id": target_id,
            "content": "The stable channel also carries signed artifacts.",
            "keywords": ["merge-forward-alias"],
            "entities": ["Signed Artifact"]
        }),
        session_id,
    )
    .await;
    let merged = invoke_json(&tool, json!({"action": "get", "id": target_id}), session_id).await;
    assert!(merged["memory"]["frontmatter"]["retrieval"]["keywords"]
        .as_array()
        .expect("merged keywords")
        .contains(&json!("merge-forward-alias")));
    assert!(merged["memory"]["frontmatter"]["retrieval"]["entities"]
        .as_array()
        .expect("merged entities")
        .contains(&json!("Signed Artifact")));

    let source = invoke_json(
        &tool,
        json!({
            "action": "write",
            "scope": "global",
            "type": "reference",
            "title": "Two independent transport facts",
            "content": "Transport alpha is local. Transport beta is remote."
        }),
        session_id,
    )
    .await;
    let source_id = source["memory"]["id"]
        .as_str()
        .expect("split source id")
        .to_string();
    let split = invoke_json(
        &tool,
        json!({
            "action": "split",
            "id": source_id,
            "pieces": [
                {
                    "title": "Local transport",
                    "content": "Transport alpha is local.",
                    "keywords": ["split-alpha-alias"],
                    "entities": ["Transport Alpha"]
                },
                {
                    "title": "Remote transport",
                    "content": "Transport beta is remote.",
                    "keywords": ["split-beta-alias"],
                    "entities": ["Transport Beta"]
                }
            ]
        }),
        session_id,
    )
    .await;
    let split_ids = split["data"]["new_ids"].as_array().expect("split ids");
    assert_eq!(split_ids.len(), 2);
    for (index, expected_keyword) in ["split-alpha-alias", "split-beta-alias"]
        .into_iter()
        .enumerate()
    {
        let split_doc = invoke_json(
            &tool,
            json!({"action": "get", "id": split_ids[index]}),
            session_id,
        )
        .await;
        assert!(split_doc["memory"]["frontmatter"]["retrieval"]["keywords"]
            .as_array()
            .expect("split keywords")
            .contains(&json!(expected_keyword)));
    }

    let first = invoke_json(
        &tool,
        json!({
            "action": "write",
            "scope": "global",
            "type": "reference",
            "title": "Legacy endpoint name",
            "content": "The endpoint was called relay-v1."
        }),
        session_id,
    )
    .await;
    let second = invoke_json(
        &tool,
        json!({
            "action": "write",
            "scope": "global",
            "type": "reference",
            "title": "Current endpoint name",
            "content": "The endpoint is called relay-v2."
        }),
        session_id,
    )
    .await;
    let consolidated = invoke_json(
        &tool,
        json!({
            "action": "consolidate",
            "ids": [first["memory"]["id"], second["memory"]["id"]],
            "title": "Canonical endpoint name",
            "content": "The confirmed endpoint name is relay-v2.",
            "type": "reference",
            "keywords": ["consolidate-forward-alias"],
            "entities": ["Relay Endpoint"]
        }),
        session_id,
    )
    .await;
    let consolidated_doc = invoke_json(
        &tool,
        json!({"action": "get", "id": consolidated["data"]["new_id"]}),
        session_id,
    )
    .await;
    assert!(
        consolidated_doc["memory"]["frontmatter"]["retrieval"]["keywords"]
            .as_array()
            .expect("consolidated keywords")
            .contains(&json!("consolidate-forward-alias"))
    );
    assert!(
        consolidated_doc["memory"]["frontmatter"]["retrieval"]["entities"]
            .as_array()
            .expect("consolidated entities")
            .contains(&json!("Relay Endpoint"))
    );
}
