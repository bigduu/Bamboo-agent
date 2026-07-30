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

struct SaveOnlyPersistence {
    full_saves: AtomicUsize,
    storage: Arc<dyn Storage>,
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
