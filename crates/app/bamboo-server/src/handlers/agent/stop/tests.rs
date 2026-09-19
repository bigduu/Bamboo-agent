use std::time::Duration;

use actix_web::web;

use super::{cancel_session, handler::mark_runner_cancelled};
use crate::app_state::{AgentRunner, AgentStatus, AppState};

#[test]
fn stop_cancels_running_status() {
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;

    runner.cancel_token.cancel();

    assert!(runner.cancel_token.is_cancelled());
}

#[test]
fn completed_status_not_cancellable() {
    let status = AgentStatus::Completed;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn cancelled_status_can_be_set() {
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Cancelled;

    assert!(matches!(runner.status, AgentStatus::Cancelled));
}

#[test]
fn runner_has_cancel_token() {
    let runner = AgentRunner::new();
    let _token_clone = runner.cancel_token.clone();
}

#[tokio::test]
async fn modern_stop_bypasses_locked_legacy_registry_and_is_idempotent() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    let token = runner.cancel_token.clone();
    state
        .agent_runners
        .write()
        .await
        .insert("modern".to_string(), runner);

    // A modern runner never consults this compatibility registry. Holding its
    // writer proves Stop cannot queue behind unrelated legacy bookkeeping.
    let legacy_guard = state.cancel_tokens.write().await;
    let first = tokio::time::timeout(Duration::from_millis(250), cancel_session(&state, "modern"))
        .await
        .expect("modern cancellation must not wait for the legacy registry");
    assert!(first);
    assert!(token.is_cancelled());

    let duplicate =
        tokio::time::timeout(Duration::from_millis(250), cancel_session(&state, "modern"))
            .await
            .expect("duplicate confirmation must use the same fast path");
    assert!(duplicate);
    drop(legacy_guard);
}

#[tokio::test]
async fn non_running_runners_are_not_reported_as_active_cancellations() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    for (session_id, status) in [
        ("pending", AgentStatus::Pending),
        ("done", AgentStatus::Completed),
        ("failed", AgentStatus::Error("boom".to_string())),
    ] {
        let mut runner = AgentRunner::new();
        runner.status = status;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);

        assert!(!cancel_session(&state, session_id).await);
    }
}

#[tokio::test]
async fn stale_stop_cannot_mark_a_successor_run_cancelled() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data_dir.path().to_path_buf()).await.unwrap());
    let mut successor = AgentRunner::new();
    successor.status = AgentStatus::Running;
    let successor_run_id = successor.run_id.clone();
    state
        .agent_runners
        .write()
        .await
        .insert("reused-session".to_string(), successor);

    mark_runner_cancelled(&state, "reused-session", Some("previous-run")).await;

    let runners = state.agent_runners.read().await;
    let successor = runners.get("reused-session").unwrap();
    assert_eq!(successor.run_id, successor_run_id);
    assert!(matches!(successor.status, AgentStatus::Running));
}
