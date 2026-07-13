use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCtx, ToolExecutionContext};
use bamboo_agent_core::Session;
use bamboo_domain::{TaskItem, TaskList};
use bamboo_storage::LockedSessionStore;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
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

fn build_tool(data_dir: &std::path::Path) -> (LedgerTool, Arc<dyn Storage>) {
    let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let session_repo = bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    (LedgerTool::new(session_repo, data_dir), storage)
}

async fn invoke(tool: &LedgerTool, session_id: &str, args: Value) -> Value {
    let outcome = tool.invoke(args, test_context(session_id)).await.unwrap();
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected Completed outcome");
    };
    assert!(result.success);
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn upsert_get_transition_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, _storage) = build_tool(dir.path());

    let created = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "upsert",
            "title": "Renew passport",
            "kind": "todo",
            "due_at": "2026-08-01",
            "priority": "high",
            "tags": ["Errands"],
            "excerpt": "I must renew my passport before August"
        }),
    )
    .await;
    assert_eq!(created["result"], "create");
    let id = created["data"]["record"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["data"]["record"]["scope"], "global");
    assert_eq!(created["data"]["record"]["tags"][0], "errands");

    // Update by id: only provided fields change.
    let updated = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "upsert",
            "id": id,
            "body": "Bring the old passport and two photos."
        }),
    )
    .await;
    assert_eq!(updated["result"], "update");
    assert_eq!(updated["data"]["record"]["title"], "Renew passport");
    assert_eq!(updated["data"]["body"], "Bring the old passport and two photos.");

    let done = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "transition",
            "id": id,
            "status": "done",
            "reason": "picked it up"
        }),
    )
    .await;
    assert_eq!(done["data"]["record"]["status"], "done");

    let fetched = invoke(&tool, "session-1", serde_json::json!({"action": "get", "id": id})).await;
    assert_eq!(fetched["data"]["record"]["status"], "done");
}

#[tokio::test]
async fn query_filters_by_status_and_time_window() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, _storage) = build_tool(dir.path());

    for (title, due) in [
        ("Early", "2026-07-14T09:00:00Z"),
        ("Late", "2026-09-01T09:00:00Z"),
    ] {
        invoke(
            &tool,
            "session-1",
            serde_json::json!({"action": "upsert", "title": title, "due_at": due}),
        )
        .await;
    }

    let windowed = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "query",
            "due_before": "2026-08-01T00:00:00Z"
        }),
    )
    .await;
    assert_eq!(windowed["returned"], 1);
    assert_eq!(windowed["records"][0]["title"], "Early");

    let open = invoke(&tool, "session-1", serde_json::json!({"action": "query"})).await;
    assert_eq!(open["returned"], 2);
}

#[tokio::test]
async fn agenda_returns_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, _storage) = build_tool(dir.path());

    invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "upsert",
            "title": "Yesterday's thing",
            "due_at": "2020-01-01T09:00:00Z"
        }),
    )
    .await;

    let agenda = invoke(&tool, "session-1", serde_json::json!({"action": "agenda"})).await;
    assert_eq!(agenda["agenda"]["overdue"][0]["title"], "Yesterday's thing");
}

#[tokio::test]
async fn decompose_creates_children_under_parent_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, _storage) = build_tool(dir.path());

    let parent = invoke(
        &tool,
        "session-1",
        serde_json::json!({"action": "upsert", "title": "Plan the trip", "kind": "todo"}),
    )
    .await;
    let parent_id = parent["data"]["record"]["id"].as_str().unwrap().to_string();

    let decomposed = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "decompose",
            "parent_id": parent_id,
            "children": [
                {"title": "Book flights", "due_at": "2026-07-20", "priority": "high"},
                {"title": "Reserve hotel"}
            ]
        }),
    )
    .await;
    assert_eq!(decomposed["created"].as_array().unwrap().len(), 2);
    assert_eq!(
        decomposed["created"][0]["relations"]["parent_id"],
        parent_id
    );

    let fetched = invoke(
        &tool,
        "session-1",
        serde_json::json!({"action": "get", "id": parent_id}),
    )
    .await;
    assert_eq!(fetched["children"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn promote_lifts_incomplete_task_items_and_remaps_relations() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, storage) = build_tool(dir.path());

    let mut session = Session::new("session-1", "test-model");
    session.task_list = Some(TaskList {
        session_id: "session-1".to_string(),
        title: "work".to_string(),
        items: vec![
            TaskItem {
                id: "t1".to_string(),
                description: "Design the schema".to_string(),
                status: TaskItemStatus::Completed,
                ..TaskItem::default()
            },
            TaskItem {
                id: "t2".to_string(),
                description: "Implement the store".to_string(),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            },
            TaskItem {
                id: "t3".to_string(),
                description: "Write the docs".to_string(),
                depends_on: vec!["t2".to_string(), "t1".to_string()],
                ..TaskItem::default()
            },
        ],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    storage.save_session(&session).await.unwrap();

    let promoted = invoke(&tool, "session-1", serde_json::json!({"action": "promote"})).await;
    let created = promoted["created"].as_array().unwrap();
    // Completed t1 is skipped; t2 and t3 promote.
    assert_eq!(created.len(), 2);
    let impl_record = created
        .iter()
        .find(|record| record["title"] == "Implement the store")
        .unwrap();
    assert_eq!(impl_record["status"], "in_progress");
    let docs_record = created
        .iter()
        .find(|record| record["title"] == "Write the docs")
        .unwrap();
    // The t2 dependency is remapped onto the new record id; unpromoted t1 drops.
    assert_eq!(
        docs_record["relations"]["depends_on"],
        serde_json::json!([impl_record["id"]])
    );
}

/// A bridge that records calls: upsert with remind_at syncs schedules onto the
/// record; a terminal transition releases them.
struct RecordingBridge {
    synced: AsyncMutex<Vec<String>>,
    released: AsyncMutex<Vec<String>>,
}

#[async_trait]
impl LedgerScheduleBridge for RecordingBridge {
    async fn sync_record_schedules(&self, record: &LedgerRecord) -> Result<Vec<String>, String> {
        self.synced.lock().await.push(record.id.clone());
        Ok(vec![format!("sched_for_{}", record.id)])
    }

    async fn release_schedules(&self, schedule_ids: &[String]) -> Result<(), String> {
        self.released.lock().await.extend(schedule_ids.iter().cloned());
        Ok(())
    }
}

#[tokio::test]
async fn schedule_bridge_syncs_on_upsert_and_releases_on_terminal_transition() {
    let dir = tempfile::tempdir().unwrap();
    let (tool, _storage) = build_tool(dir.path());
    let bridge = Arc::new(RecordingBridge {
        synced: AsyncMutex::new(Vec::new()),
        released: AsyncMutex::new(Vec::new()),
    });
    let tool = tool.with_schedule_bridge(bridge.clone());

    let created = invoke(
        &tool,
        "session-1",
        serde_json::json!({
            "action": "upsert",
            "title": "Take medication",
            "kind": "reminder",
            "remind_at": ["2099-01-01T09:00:00Z"]
        }),
    )
    .await;
    let id = created["data"]["record"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        created["data"]["record"]["schedule_ids"],
        serde_json::json!([format!("sched_for_{id}")])
    );
    assert_eq!(bridge.synced.lock().await.len(), 1);

    invoke(
        &tool,
        "session-1",
        serde_json::json!({"action": "transition", "id": id, "status": "cancelled"}),
    )
    .await;
    assert_eq!(
        *bridge.released.lock().await,
        vec![format!("sched_for_{id}")]
    );

    // The record's schedule ids are cleared after release.
    let fetched = invoke(&tool, "session-1", serde_json::json!({"action": "get", "id": id})).await;
    assert!(fetched["data"]["record"]["schedule_ids"]
        .as_array()
        .map(|ids| ids.is_empty())
        .unwrap_or(true));
}
