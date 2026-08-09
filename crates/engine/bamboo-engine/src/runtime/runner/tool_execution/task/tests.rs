use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::maybe_handle_taskwrite;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::{AgentRuntimeState, RuntimeSessionPersistence, TaskItemStatus};

#[derive(Default)]
struct RecordingPersistence {
    full_saves: AtomicUsize,
    control_plane_saves: AtomicUsize,
    task_patches: AtomicUsize,
    control_planes: Mutex<Vec<Session>>,
    patched_task_sessions: Mutex<Vec<Session>>,
    sessions: Mutex<HashMap<String, Session>>,
}

impl RecordingPersistence {
    fn seed(&self, session: Session) {
        self.sessions
            .lock()
            .expect("recording persistence lock")
            .insert(session.id.clone(), session);
    }
}

#[async_trait]
impl RuntimeSessionPersistence for RecordingPersistence {
    async fn save_runtime_session(&self, _session: &mut Session) -> io::Result<()> {
        self.full_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn save_runtime_control_plane(&self, session: &mut Session) -> io::Result<()> {
        self.control_plane_saves.fetch_add(1, Ordering::SeqCst);
        self.control_planes
            .lock()
            .expect("recording persistence lock")
            .push(session.clone());
        Ok(())
    }

    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> io::Result<bool> {
        self.task_patches.fetch_add(1, Ordering::SeqCst);
        let patched = {
            let mut sessions = self.sessions.lock().expect("recording persistence lock");
            let Some(session) = sessions.get_mut(session_id) else {
                return Ok(false);
            };
            session.set_task_list(task_list.clone());
            session.set_task_list_version_meta(version.to_string());
            session.clone()
        };
        self.patched_task_sessions
            .lock()
            .expect("recorded Task patches lock")
            .push(patched);
        Ok(true)
    }
}

struct RebasingTaskPersistence {
    authoritative_task_list: bamboo_domain::TaskList,
    local_saves: Mutex<Vec<(String, bamboo_domain::TaskList, String)>>,
    root_patches: Mutex<Vec<(String, bamboo_domain::TaskList, String)>>,
    full_saves: AtomicUsize,
}

impl RebasingTaskPersistence {
    fn new(authoritative_task_list: bamboo_domain::TaskList) -> Self {
        Self {
            authoritative_task_list,
            local_saves: Mutex::new(Vec::new()),
            root_patches: Mutex::new(Vec::new()),
            full_saves: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl RuntimeSessionPersistence for RebasingTaskPersistence {
    async fn save_runtime_session(&self, _session: &mut Session) -> io::Result<()> {
        self.full_saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn save_runtime_control_plane(&self, session: &mut Session) -> io::Result<()> {
        let candidate = session.task_list.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "candidate Task list is missing",
            )
        })?;
        let candidate_version = session.task_list_version_meta().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "candidate Task version is missing",
            )
        })?;
        self.local_saves
            .lock()
            .expect("local save recording lock")
            .push((session.id.clone(), candidate, candidate_version));

        // Model the real LockedSessionStore conflict-rebase contract: a
        // competing evaluator already made E/v2 durable, so the local A/v1
        // caller is rewritten to that authoritative snapshot before save
        // returns successfully.
        session.set_task_list(self.authoritative_task_list.clone());
        session.set_task_list_version_meta("2");
        Ok(())
    }

    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> io::Result<bool> {
        self.root_patches
            .lock()
            .expect("root patch recording lock")
            .push((
                session_id.to_string(),
                task_list.clone(),
                version.to_string(),
            ));
        Ok(true)
    }
}

struct SaveOnlyPersistence {
    full_saves: AtomicUsize,
    storage: Arc<dyn Storage>,
}

struct RootCasPauseStorage {
    inner: Arc<bamboo_storage::SessionStoreV2>,
    root_id: String,
    root_cas_reached: Arc<tokio::sync::Barrier>,
    release_root_cas: Arc<tokio::sync::Barrier>,
    full_saves: AtomicUsize,
}

#[async_trait]
impl Storage for RootCasPauseStorage {
    async fn save_session(&self, session: &Session) -> io::Result<()> {
        self.full_saves.fetch_add(1, Ordering::SeqCst);
        self.inner.save_session(session).await
    }

    async fn load_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        self.inner.load_session(session_id).await
    }

    async fn delete_session(&self, session_id: &str) -> io::Result<bool> {
        self.inner.delete_session(session_id).await
    }

    async fn save_runtime_state(&self, session: &Session) -> io::Result<()> {
        self.inner.save_runtime_state(session).await
    }

    async fn load_runtime_control_plane(&self, session_id: &str) -> io::Result<Option<Session>> {
        self.inner.load_runtime_control_plane(session_id).await
    }

    async fn save_task_control_plane_if_matches(
        &self,
        original: &Session,
        updated: &Session,
    ) -> io::Result<bool> {
        if original.id == self.root_id {
            self.root_cas_reached.wait().await;
            self.release_root_cas.wait().await;
        }
        self.inner
            .save_task_control_plane_if_matches(original, updated)
            .await
    }
}

#[derive(Default)]
struct FullSessionPersistence {
    full_saves: AtomicUsize,
    full_loads: AtomicUsize,
    sessions: Mutex<HashMap<String, Session>>,
}

impl FullSessionPersistence {
    fn seed(&self, session: Session) {
        self.sessions
            .lock()
            .expect("full-session persistence lock")
            .insert(session.id.clone(), session);
    }

    fn get(&self, session_id: &str) -> Session {
        self.sessions
            .lock()
            .expect("full-session persistence lock")
            .get(session_id)
            .cloned()
            .expect("persisted session")
    }
}

#[async_trait]
impl RuntimeSessionPersistence for FullSessionPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        self.full_saves.fetch_add(1, Ordering::SeqCst);
        self.sessions
            .lock()
            .expect("full-session persistence lock")
            .insert(session.id.clone(), session.clone());
        Ok(())
    }

    async fn load_runtime_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        self.full_loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .sessions
            .lock()
            .expect("full-session persistence lock")
            .get(session_id)
            .cloned())
    }
}

impl SaveOnlyPersistence {
    fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            full_saves: AtomicUsize::new(0),
            storage,
        }
    }
}

#[async_trait]
impl RuntimeSessionPersistence for SaveOnlyPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        self.full_saves.fetch_add(1, Ordering::SeqCst);
        self.storage.save_session(session).await
    }
}

fn task_call_and_result() -> (ToolCall, ToolResult) {
    (
        ToolCall {
            id: "task-call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Task".to_string(),
                arguments: serde_json::json!({
                    "tasks": [{
                        "content": "Refactor module",
                        "status": "in_progress",
                        "activeForm": "Refactoring module"
                    }]
                })
                .to_string(),
            },
        },
        ToolResult {
            success: true,
            result: "ok".to_string(),
            display_preference: None,
            images: Vec::new(),
        },
    )
}

fn task_list_value(session: &Session) -> serde_json::Value {
    serde_json::to_value(&session.task_list).expect("serialize task list")
}

#[tokio::test]
async fn root_taskwrite_uses_control_plane_save_and_preserves_event_and_context_behavior() {
    let (tool_call, result) = task_call_and_result();

    let mut session = Session::new("session-1", "model");
    let mut task_context: Option<TaskLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);
    let persistence = Arc::new(RecordingPersistence::default());
    let mut config = AgentLoopConfig::default();
    config.persistence = Some(persistence.clone());

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    let task_list = session.task_list.as_ref().expect("task list should be set");
    assert_eq!(task_list.items.len(), 1);
    assert_eq!(task_list.items[0].status, TaskItemStatus::InProgress);
    assert_eq!(session.task_list_version_meta().as_deref(), Some("1"));
    assert!(
        task_context
            .as_ref()
            .is_some_and(|context| context.task_list_dirty),
        "Task context must still be reinitialized and marked dirty"
    );
    assert_eq!(persistence.full_saves.load(Ordering::SeqCst), 0);
    assert_eq!(persistence.control_plane_saves.load(Ordering::SeqCst), 1);
    assert_eq!(
        persistence
            .control_planes
            .lock()
            .expect("recorded control planes")
            .as_slice()
            .first()
            .map(|saved| saved.id.as_str()),
        Some("session-1")
    );

    let event = rx.recv().await.expect("task update event");
    match event {
        AgentEvent::TaskListUpdated { task_list } => {
            assert_eq!(task_list.items.len(), 1);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn child_taskwrite_saves_child_and_shared_root_control_planes_without_full_history() {
    let (tool_call, result) = task_call_and_result();
    let mut root = Session::new("root-1", "model");
    root.add_message(Message::user("durable root history"));

    let persistence = Arc::new(RecordingPersistence::default());
    persistence.seed(root);
    let mut config = AgentLoopConfig::default();
    config.persistence = Some(persistence.clone());

    let mut child = Session::new_child("child-1", "root-1", "model", "Child");
    child.add_message(Message::user("live child history"));
    let mut task_context: Option<TaskLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut child,
        "child-1",
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    assert_eq!(persistence.full_saves.load(Ordering::SeqCst), 0);
    assert_eq!(
        persistence.control_plane_saves.load(Ordering::SeqCst),
        1,
        "the executing child must use a control-plane save"
    );
    assert_eq!(
        persistence.task_patches.load(Ordering::SeqCst),
        1,
        "the shared root must use one atomic Task patch"
    );

    let saves = persistence
        .control_planes
        .lock()
        .expect("recorded control planes");
    let child_save = saves
        .iter()
        .find(|saved| saved.id == "child-1")
        .expect("child control-plane save");
    let patches = persistence
        .patched_task_sessions
        .lock()
        .expect("recorded Task patches");
    let root_save = patches
        .iter()
        .find(|saved| saved.id == "root-1")
        .expect("root atomic Task patch");
    assert_eq!(child_save.task_list_version_meta().as_deref(), Some("1"));
    assert_eq!(root_save.task_list_version_meta().as_deref(), Some("1"));
    assert_eq!(task_list_value(child_save), task_list_value(root_save));
    assert_eq!(
        root_save.messages.len(),
        1,
        "atomic patch must preserve unrelated root fields"
    );

    assert!(
        task_context
            .as_ref()
            .is_some_and(|context| context.task_list_dirty),
        "Task context must still be reinitialized and marked dirty"
    );
    assert!(matches!(
        rx.recv().await,
        Some(AgentEvent::TaskListUpdated { .. })
    ));
}

#[tokio::test]
async fn child_taskwrite_publishes_snapshot_rebased_by_local_save_everywhere() {
    let root_id = "rebased-task-root";
    let child_id = "rebased-task-child";
    let now = chrono::Utc::now();
    let authoritative_task_list = bamboo_domain::TaskList {
        session_id: root_id.to_string(),
        title: "Evaluator authority".to_string(),
        items: vec![bamboo_domain::TaskItem {
            id: "evaluated-task".to_string(),
            description: "Evaluator-owned task".to_string(),
            status: TaskItemStatus::Completed,
            ..bamboo_domain::TaskItem::default()
        }],
        created_at: now,
        updated_at: now,
    };
    let persistence = Arc::new(RebasingTaskPersistence::new(
        authoritative_task_list.clone(),
    ));
    let mut config = AgentLoopConfig::default();
    config.persistence = Some(persistence.clone());
    let mut child = Session::new_child(child_id, root_id, "model", "Child");
    let mut task_context = None;
    let (tx, mut rx) = mpsc::channel(4);
    let (tool_call, result) = task_call_and_result();

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut child,
        child_id,
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    let local_saves = persistence
        .local_saves
        .lock()
        .expect("local save recording lock")
        .clone();
    assert_eq!(local_saves.len(), 1);
    let (saved_child_id, stale_candidate, stale_version) = &local_saves[0];
    assert_eq!(saved_child_id, child_id);
    assert_eq!(stale_version, "1");
    assert_eq!(stale_candidate.items[0].description, "Refactor module");
    assert_ne!(
        serde_json::to_value(stale_candidate).expect("serialize stale candidate"),
        serde_json::to_value(&authoritative_task_list).expect("serialize authority")
    );

    let root_patches = persistence
        .root_patches
        .lock()
        .expect("root patch recording lock")
        .clone();
    assert_eq!(root_patches.len(), 1);
    let (patched_root_id, patched_task_list, patched_version) = &root_patches[0];
    assert_eq!(patched_root_id, root_id);
    assert_eq!(patched_version, "2");
    assert_eq!(
        serde_json::to_value(patched_task_list).expect("serialize root patch"),
        serde_json::to_value(&authoritative_task_list).expect("serialize authority")
    );
    assert_eq!(persistence.full_saves.load(Ordering::SeqCst), 0);

    assert_eq!(child.task_list_version_meta().as_deref(), Some("2"));
    assert_eq!(
        serde_json::to_value(&child.task_list).expect("serialize live child Task list"),
        serde_json::to_value(Some(&authoritative_task_list)).expect("serialize authority")
    );
    let context = task_context.expect("rebased Task context");
    assert!(context.task_list_dirty);
    assert_eq!(context.version, 2);
    assert_eq!(
        serde_json::to_value(
            context.to_task_list_with_title(authoritative_task_list.title.clone())
        )
        .expect("serialize Task context"),
        serde_json::to_value(&authoritative_task_list).expect("serialize authority")
    );
    let event_task_list = match rx.recv().await.expect("rebased Task event") {
        AgentEvent::TaskListUpdated { task_list } => task_list,
        other => panic!("unexpected event: {other:?}"),
    };
    assert_eq!(
        serde_json::to_value(event_task_list).expect("serialize Task event"),
        serde_json::to_value(authoritative_task_list).expect("serialize authority")
    );
}

#[tokio::test]
async fn child_taskwrite_custom_fallback_full_load_and_save_preserve_message_histories() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
            .await
            .expect("SessionStoreV2"),
    );
    let mut root = Session::new("fallback-root", "model");
    root.add_message(Message::user("root history must survive"));
    store.save_session(&root).await.expect("seed root");

    let storage: Arc<dyn Storage> = store.clone();
    let persistence = Arc::new(SaveOnlyPersistence::new(storage.clone()));
    let mut child = Session::new_child("fallback-child", "fallback-root", "model", "Child");
    child.add_message(Message::user("child history must survive"));
    let mut config = AgentLoopConfig::default();
    config.storage = Some(storage);
    config.persistence = Some(persistence.clone());
    let mut task_context = None;
    let (tx, _rx) = mpsc::channel(4);
    let (tool_call, result) = task_call_and_result();

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut child,
        "fallback-child",
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    assert_eq!(
        persistence.full_saves.load(Ordering::SeqCst),
        2,
        "default control-plane fallback must full-save child and root"
    );
    let saved_child = store
        .load_session("fallback-child")
        .await
        .expect("load child")
        .expect("saved child");
    let saved_root = store
        .load_session("fallback-root")
        .await
        .expect("load root")
        .expect("saved root");
    assert_eq!(saved_child.messages.len(), 1);
    assert_eq!(
        saved_child.messages[0].content,
        "child history must survive"
    );
    assert_eq!(saved_root.messages.len(), 1);
    assert_eq!(saved_root.messages[0].content, "root history must survive");
    assert_eq!(task_list_value(&saved_child), task_list_value(&saved_root));
    assert_eq!(
        saved_root.task_list_version_meta(),
        saved_child.task_list_version_meta()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_taskwrite_root_cas_conflict_refreshes_repository_without_full_save_fallback() {
    let directory = tempfile::tempdir().expect("tempdir");
    let home = directory.path().to_path_buf();
    let first_inner = Arc::new(
        bamboo_storage::SessionStoreV2::new(home.clone())
            .await
            .expect("first SessionStoreV2"),
    );
    let root_id = "taskwrite-cas-conflict-root";
    let child_id = "taskwrite-cas-conflict-child";
    let mut root = Session::new(root_id, "model");
    root.add_message(Message::user("root transcript must survive"));
    first_inner.save_session(&root).await.expect("seed root");
    let second_inner = Arc::new(
        bamboo_storage::SessionStoreV2::new(home)
            .await
            .expect("second SessionStoreV2"),
    );

    let root_cas_reached = Arc::new(tokio::sync::Barrier::new(2));
    let release_root_cas = Arc::new(tokio::sync::Barrier::new(2));
    let gated_storage = Arc::new(RootCasPauseStorage {
        inner: first_inner.clone(),
        root_id: root_id.to_string(),
        root_cas_reached: root_cas_reached.clone(),
        release_root_cas: release_root_cas.clone(),
        full_saves: AtomicUsize::new(0),
    });
    let storage: Arc<dyn Storage> = gated_storage.clone();
    let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repository = Arc::new(crate::SessionRepository::new(
        Default::default(),
        storage.clone(),
        locked,
    ));
    repository.load(root_id).await.expect("cache root");

    let mut child = Session::new_child(child_id, root_id, "model", "Child");
    child.add_message(Message::user("child transcript"));
    let mut config = AgentLoopConfig::default();
    config.storage = Some(storage);
    config.persistence = Some(repository.clone());
    let (tx, mut rx) = mpsc::channel(4);
    let (tool_call, result) = task_call_and_result();
    let taskwrite = tokio::spawn(async move {
        let mut task_context = None;
        maybe_handle_taskwrite(
            &tool_call,
            &result,
            &mut child,
            child_id,
            &tx,
            &config,
            &mut task_context,
        )
        .await;
        (child, task_context)
    });

    root_cas_reached.wait().await;
    let stale_child_candidate = first_inner
        .load_runtime_control_plane(child_id)
        .await
        .expect("load staged child candidate")
        .expect("staged child candidate exists");
    assert_eq!(
        stale_child_candidate.task_list_version_meta().as_deref(),
        Some("1")
    );
    let original = second_inner
        .load_runtime_control_plane(root_id)
        .await
        .expect("load competing root")
        .expect("competing root exists");
    let now = chrono::Utc::now();
    let mut winner = original.clone();
    winner.task_list = Some(bamboo_domain::TaskList {
        session_id: root_id.to_string(),
        title: "concurrent winner".to_string(),
        items: Vec::new(),
        created_at: now,
        updated_at: now,
    });
    winner.set_task_list_version_meta("1");
    assert!(second_inner
        .save_task_control_plane_if_matches(&original, &winner)
        .await
        .expect("commit competing root Task"));
    release_root_cas.wait().await;
    let (child, task_context) = taskwrite.await.expect("Taskwrite task");

    assert_eq!(
        gated_storage.full_saves.load(Ordering::SeqCst),
        0,
        "an unconditional CAS conflict is an error, not the Ok(false) legacy fallback signal"
    );
    let durable_root = first_inner
        .load_session(root_id)
        .await
        .expect("load durable root")
        .expect("durable root exists");
    let durable_child = first_inner
        .load_session(child_id)
        .await
        .expect("load durable child")
        .expect("durable child exists");
    let cached_root = repository.load(root_id).await.expect("cached root");
    let cached_child = repository.load(child_id).await.expect("cached child");
    for (tier, session) in [
        ("durable root", &durable_root),
        ("durable child", &durable_child),
        ("cache root", &cached_root),
        ("cache child", &cached_child),
        ("live child", &child),
    ] {
        assert_eq!(session.task_list_version_meta().as_deref(), Some("1"));
        assert_eq!(
            session.task_list.as_ref().map(|list| list.title.as_str()),
            Some("concurrent winner"),
            "tier={tier}"
        );
    }
    assert_eq!(durable_root.messages.len(), 1);
    let winner_task_list = durable_root.task_list.as_ref().expect("winner task list");
    let context = task_context.expect("Task context follows authoritative winner");
    assert!(context.task_list_dirty);
    assert_eq!(
        serde_json::to_value(context.to_task_list_with_title(winner_task_list.title.clone()))
            .expect("serialize Task context"),
        serde_json::to_value(winner_task_list).expect("serialize winner task list")
    );
    let event_task_list = match rx.recv().await.expect("authoritative Task event") {
        AgentEvent::TaskListUpdated { task_list } => task_list,
        other => panic!("unexpected event: {other:?}"),
    };
    assert_eq!(
        serde_json::to_value(event_task_list).expect("serialize event task list"),
        serde_json::to_value(winner_task_list).expect("serialize winner task list")
    );

    // A real evaluator result produced from the losing A/v1 snapshot must not
    // be allowed to overwrite the reconciled B/v1 pair merely because the
    // numeric generation is unchanged.
    let stale_task_list = stale_child_candidate
        .task_list
        .expect("staged child candidate task list");
    let mut stale_evaluation = stale_task_list.clone();
    stale_evaluation.title = "evaluation based on losing candidate".to_string();
    stale_evaluation.updated_at = chrono::Utc::now();
    assert!(
        !RuntimeSessionPersistence::update_task_list_control_planes_if_version(
            repository.as_ref(),
            child_id,
            root_id,
            "1",
            &stale_task_list,
            &stale_evaluation,
            "2",
        )
        .await
        .expect("stale evaluator CAS returns a clean conflict")
    );
    for id in [child_id, root_id] {
        let durable = first_inner
            .load_session(id)
            .await
            .expect("reload after stale evaluator")
            .expect("session remains");
        assert_eq!(durable.task_list_version_meta().as_deref(), Some("1"));
        assert_eq!(
            durable.task_list.as_ref().map(|list| list.title.as_str()),
            Some("concurrent winner")
        );
    }
}

#[tokio::test]
async fn child_taskwrite_default_atomic_port_preserves_custom_full_session_state() {
    let persistence = Arc::new(FullSessionPersistence::default());
    let mut root = Session::new("default-port-root", "model");
    root.add_message(Message::user("root history"));
    root.agent_runtime_state = Some(AgentRuntimeState::new("root-runtime"));
    persistence.seed(root);

    let mut child = Session::new_child("default-port-child", "default-port-root", "model", "Child");
    child.add_message(Message::user("child history"));
    let mut config = AgentLoopConfig::default();
    config.persistence = Some(persistence.clone());
    let mut task_context = None;
    let (tx, _rx) = mpsc::channel(4);
    let (tool_call, result) = task_call_and_result();

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut child,
        "default-port-child",
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    assert_eq!(persistence.full_loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        persistence.full_saves.load(Ordering::SeqCst),
        2,
        "executing child and shared root use safe full-save defaults"
    );
    let saved_root = persistence.get("default-port-root");
    assert_eq!(saved_root.messages.len(), 1);
    assert_eq!(
        saved_root
            .agent_runtime_state
            .as_ref()
            .map(|state| state.run_id.as_str()),
        Some("root-runtime")
    );
    assert_eq!(saved_root.task_list_version_meta().as_deref(), Some("1"));
    assert_eq!(task_list_value(&saved_root), task_list_value(&child));
}

#[tokio::test]
async fn child_task_control_planes_reload_without_rewriting_history_or_unrelated_root_state() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
            .await
            .expect("SessionStoreV2"),
    );
    let mut durable_root = Session::new("reload-root", "model");
    durable_root.add_message(Message::user("durable root transcript"));
    durable_root.agent_runtime_state = Some(AgentRuntimeState::new("latest-root-run"));
    durable_root
        .metadata
        .insert("durable.unrelated".to_string(), "keep".to_string());
    store.save_session(&durable_root).await.expect("seed root");

    let mut durable_child = Session::new_child("reload-child", "reload-root", "model", "Child");
    durable_child.add_message(Message::user("durable child transcript"));
    store
        .save_session(&durable_child)
        .await
        .expect("seed child");

    let locked = Arc::new(bamboo_storage::LockedSessionStore::new(store.clone()));
    let repository =
        crate::SessionRepository::new(Default::default(), store.clone(), locked.clone());
    repository
        .load("reload-root")
        .await
        .expect("backfill root cache");
    repository
        .load("reload-child")
        .await
        .expect("backfill child cache");
    {
        let cached_root = repository.cache().get("reload-root").expect("cached root");
        cached_root
            .write()
            .metadata
            .insert("cache.concurrent".to_string(), "preserve".to_string());
    }

    let mut live_child = durable_child.clone();
    live_child.add_message(Message::assistant(
        "uncheckpointed child message must not be rewritten by Task",
        None,
    ));
    let mut config = AgentLoopConfig::default();
    config.storage = Some(store.clone());
    config.persistence = Some(Arc::new(repository.clone()));
    let mut task_context = None;
    let (tx, mut rx) = mpsc::channel(4);
    let (tool_call, result) = task_call_and_result();

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut live_child,
        "reload-child",
        &tx,
        &config,
        &mut task_context,
    )
    .await;

    let reloaded_root = store
        .load_session("reload-root")
        .await
        .expect("normal root reload")
        .expect("root exists");
    let reloaded_child = store
        .load_session("reload-child")
        .await
        .expect("normal child reload")
        .expect("child exists");
    assert_eq!(
        task_list_value(&reloaded_root),
        task_list_value(&live_child)
    );
    assert_eq!(
        task_list_value(&reloaded_child),
        task_list_value(&live_child)
    );
    assert_eq!(reloaded_root.task_list_version_meta().as_deref(), Some("1"));
    assert_eq!(
        reloaded_child.task_list_version_meta().as_deref(),
        Some("1")
    );
    assert_eq!(
        reloaded_root.messages.len(),
        1,
        "root Task patch must not rewrite session.json messages"
    );
    assert_eq!(reloaded_root.messages[0].content, "durable root transcript");
    assert_eq!(
        reloaded_child.messages.len(),
        1,
        "child control-plane save must not write its uncheckpointed message"
    );
    assert_eq!(
        reloaded_child.messages[0].content,
        "durable child transcript"
    );
    assert_eq!(
        reloaded_root
            .agent_runtime_state
            .as_ref()
            .map(|state| state.run_id.as_str()),
        Some("latest-root-run")
    );
    assert_eq!(
        reloaded_root
            .metadata
            .get("durable.unrelated")
            .map(String::as_str),
        Some("keep")
    );

    let cached_root = repository
        .load("reload-root")
        .await
        .expect("cache-visible root");
    assert_eq!(task_list_value(&cached_root), task_list_value(&live_child));
    assert_eq!(
        cached_root.messages.len(),
        1,
        "atomic Task cache patch must preserve the cached transcript"
    );
    assert_eq!(
        cached_root
            .metadata
            .get("cache.concurrent")
            .map(String::as_str),
        Some("preserve"),
        "atomic Task cache patch must preserve concurrent unrelated fields"
    );
    assert_eq!(
        cached_root
            .agent_runtime_state
            .as_ref()
            .map(|state| state.run_id.as_str()),
        Some("latest-root-run")
    );
    assert!(task_context
        .as_ref()
        .is_some_and(|context: &TaskLoopContext| context.task_list_dirty));
    assert!(matches!(
        rx.recv().await,
        Some(AgentEvent::TaskListUpdated { .. })
    ));
}

#[tokio::test]
async fn maybe_handle_taskwrite_ignores_non_task_calls() {
    let tool_call = ToolCall {
        id: "read-call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Read".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: "ok".to_string(),
        display_preference: None,
        images: Vec::new(),
    };

    let mut session = Session::new("session-1", "model");
    let mut task_context: Option<TaskLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &AgentLoopConfig::default(),
        &mut task_context,
    )
    .await;

    assert!(session.task_list.is_none());
    assert!(task_context.is_none());
    assert!(rx.try_recv().is_err());
}
