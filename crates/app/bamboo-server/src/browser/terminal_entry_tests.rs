//! Existing SubAgent/Plan deletion port against isolated canonical fixtures.

use super::*;
use crate::app_state::AppState;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};
use bamboo_agent_core::{Session, Storage};
use bamboo_engine::session_app::child_session::ChildSessionPort;

pub(super) async fn delete_via_root_tool(state: &AppState, parent: &str, child: &str) -> Value {
    let call = ToolCall {
        id: "synthetic-browser-child-delete".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: json!({"action":"delete","child_session_id":child}).to_string(),
        },
    };
    let result = state
        .tool_factory
        .get(crate::tools::ToolSurface::Root)
        .execute_with_context(
            &call,
            ToolExecutionContext {
                executing_supervisor: None,
                session_id: Some(parent),
                root_session_id: None,
                tool_call_id: "synthetic-browser-child-delete",
                event_tx: None,
                available_tool_schemas: None,
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            },
        )
        .await
        .unwrap();
    serde_json::from_str(&result.result).unwrap()
}

fn seed(root: &std::path::Path, id: &str) -> PathBuf {
    let owner = profile::acquire(root, id).unwrap();
    let path = owner.path.clone();
    std::fs::write(path.join("Cookies"), b"synthetic child login").unwrap();
    path
}

fn adapter(state: &AppState) -> crate::tools::ChildSessionAdapter {
    crate::tools::ChildSessionAdapter {
        session_store: state.session_store.clone(),
        storage: state.storage.clone(),
        persistence: state.persistence.clone(),
        session_messenger: Some(state.session_messenger.clone()),
        scheduler: state.spawn_scheduler.clone(),
        sessions_cache: state.sessions.clone(),
        agent_runners: state.agent_runners.clone(),
        session_event_senders: state.session_event_senders.clone(),
        subagent_model_resolver: None,
        config: state.config.clone(),
        project_store: Some(state.project_store.clone()),
        workspace_resolver: state.workspace_resolver.clone(),
        parent_wait_slots: Arc::default(),
        recovered_launches: Arc::default(),
    }
}

#[tokio::test]
async fn subagent_delete_purges_child_profile_and_preserves_parent_state() {
    let home = tempfile::tempdir().unwrap();
    let state = AppState::new(home.path().into()).await.unwrap();
    let parent = Session::new("adapter-parent", "synthetic-model");
    let child = Session::new_child_of("adapter-child", &parent, "synthetic-model", "fixture child");
    state.storage.save_session(&parent).await.unwrap();
    state.storage.save_session(&child).await.unwrap();
    let root = home.path().join("browser-profiles");
    let parent_profile = seed(&root, &parent.id);
    let child_profile = seed(&root, &child.id);
    let result = delete_via_root_tool(&state, &parent.id, &child.id).await;
    let purged = !child_profile.exists();
    let fenced = matches!(
        profile::acquire(&root, &child.id),
        Err(BrowserError::NotOpen)
    );
    let parent_kept = parent_profile.join("Cookies").is_file()
        && state
            .storage
            .load_session(&parent.id)
            .await
            .unwrap()
            .is_some();
    let absent = state
        .storage
        .load_session(&child.id)
        .await
        .unwrap()
        .is_none();
    state.shutdown().await;
    assert_eq!(result["deleted"], true);
    assert!(absent && parent_kept);
    assert!(
        purged && fenced,
        "SubAgent deletion retained the child's site state"
    );
}

#[tokio::test]
async fn child_adapter_preserves_unindexed_canonical_child_profile_and_cache() {
    let home = tempfile::tempdir().unwrap();
    let parent = Session::new("adapter-parent", "synthetic-model");
    let child = Session::new_child_of("adapter-child", &parent, "synthetic-model", "fixture child");
    let store = bamboo_storage::SessionStoreV2::new(home.path().into())
        .await
        .unwrap();
    store.save_session(&parent).await.unwrap();
    store.save_session(&child).await.unwrap();
    store.flush_search_index().await;
    drop(store);
    let index_path = home.path().join("sessions.json");
    let mut index: Value = serde_json::from_slice(&std::fs::read(&index_path).unwrap()).unwrap();
    assert!(index["sessions"]
        .as_object_mut()
        .unwrap()
        .remove(&child.id)
        .is_some());
    std::fs::write(index_path, serde_json::to_vec(&index).unwrap()).unwrap();
    let state = AppState::new(home.path().into()).await.unwrap();
    assert!(state
        .session_store
        .get_index_entry(&child.id)
        .await
        .is_none());
    state.sessions.insert(
        child.id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(child.clone())),
    );
    let path = seed(&home.path().join("browser-profiles"), &child.id);
    let result = adapter(&state)
        .delete_child_session(&parent.id, &child.id)
        .await;
    let kept = path.join("Cookies").is_file() && state.sessions.contains_key(&child.id);
    let canonical = home
        .path()
        .join("sessions")
        .join(&parent.id)
        .join("children")
        .join(&child.id)
        .join("session.json")
        .is_file();
    state.shutdown().await;
    assert!(!result.unwrap().deleted);
    assert!(
        canonical && kept,
        "an index miss must not discard canonical child state"
    );
}

#[tokio::test]
async fn child_adapter_reports_and_retries_profile_cleanup_after_storage_deletion() {
    let home = tempfile::tempdir().unwrap();
    let state = AppState::new(home.path().into()).await.unwrap();
    let parent = Session::new("adapter-parent", "synthetic-model");
    let child = Session::new_child_of("adapter-child", &parent, "synthetic-model", "fixture child");
    state.storage.save_session(&parent).await.unwrap();
    state.storage.save_session(&child).await.unwrap();
    let root = home.path().join("browser-profiles");
    let foreign = profile::acquire(&root, &child.id).unwrap();
    let path = foreign.path.clone();
    std::fs::write(path.join("Cookies"), b"synthetic state").unwrap();
    let port = adapter(&state);
    let first = port.delete_child_session(&parent.id, &child.id).await;
    let absent = state
        .storage
        .load_session(&child.id)
        .await
        .unwrap()
        .is_none();
    let protected = path.join("Cookies").is_file();
    drop(foreign);
    let retry = port.delete_child_session(&parent.id, &child.id).await;
    let purged = !path.exists();
    state.shutdown().await;
    assert!(
        first.is_err(),
        "cannot claim success while another owner retains child data"
    );
    assert!(absent && protected);
    assert!(
        !retry.unwrap().deleted && purged,
        "missing-session retry must finish browser cleanup"
    );
}

#[cfg(unix)]
#[test]
fn subagent_delete_reaps_active_child_host() {
    super::reset_tests::check_owned_host_entry("subagent");
}
