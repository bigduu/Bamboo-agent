//! Runner lifecycle helpers for background agent execution.
//!
//! Provides shared implementations for:
//! - Runner reservation (check existing → create new with cancel token)
//! - Runner finalization (map execution result to `AgentStatus`)
//! - Status mapping

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::AgentEvent;

use super::runner_state::{AgentRunner, AgentStatus};

/// Try to reserve a runner for the given session.
///
/// If a runner with `Running` status already exists, returns `None`
/// (caller should skip execution).
///
/// Otherwise removes any stale runner and inserts a fresh one, returning
/// the associated `CancellationToken`.
pub async fn try_reserve_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    event_sender: &broadcast::Sender<AgentEvent>,
) -> Option<CancellationToken> {
    let mut guard = runners.write().await;
    if let Some(runner) = guard.get(session_id) {
        if matches!(runner.status, AgentStatus::Running) {
            tracing::debug!("[{}] Runner already running, skipping", session_id);
            return None;
        }
    }

    guard.remove(session_id);

    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    runner.event_sender = event_sender.clone();
    let cancel_token = runner.cancel_token.clone();

    guard.insert(session_id.to_string(), runner);
    Some(cancel_token)
}

/// Map an execution result to `AgentStatus`.
pub fn status_from_execution_result<E>(result: &Result<(), E>) -> AgentStatus
where
    E: std::fmt::Display,
{
    match result {
        Ok(_) => AgentStatus::Completed,
        Err(error) if error.to_string().contains("cancelled") => AgentStatus::Cancelled,
        Err(error) => AgentStatus::Error(error.to_string()),
    }
}

/// Update a runner's terminal status and completion timestamp.
pub async fn finalize_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    result: &Result<(), impl std::fmt::Display>,
) {
    let mut guard = runners.write().await;
    if let Some(runner) = guard.get_mut(session_id) {
        runner.status = status_from_execution_result(result);
        runner.completed_at = Some(Utc::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_runners() -> Arc<RwLock<HashMap<String, AgentRunner>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn new_broadcaster() -> broadcast::Sender<AgentEvent> {
        broadcast::channel(100).0
    }

    #[tokio::test]
    async fn try_reserve_runner_creates_runner_with_running_status() {
        let runners = new_runners();
        let tx = new_broadcaster();
        let token = try_reserve_runner(&runners, "s1", &tx).await;
        assert!(token.is_some());

        let guard = runners.read().await;
        let runner = guard.get("s1").unwrap();
        assert!(matches!(runner.status, AgentStatus::Running));
    }

    #[tokio::test]
    async fn try_reserve_runner_returns_none_when_already_running() {
        let runners = new_runners();
        let tx = new_broadcaster();
        let _ = try_reserve_runner(&runners, "s1", &tx).await;
        let second = try_reserve_runner(&runners, "s1", &tx).await;
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn try_reserve_runner_replaces_completed_runner() {
        let runners = new_runners();
        let tx = new_broadcaster();
        let _ = try_reserve_runner(&runners, "s1", &tx).await;

        {
            let mut guard = runners.write().await;
            let runner = guard.get_mut("s1").unwrap();
            runner.status = AgentStatus::Completed;
        }

        let second = try_reserve_runner(&runners, "s1", &tx).await;
        assert!(second.is_some());
    }

    #[test]
    fn status_from_execution_result_maps_correctly() {
        let ok_result: Result<(), String> = Ok(());
        assert!(matches!(
            status_from_execution_result(&ok_result),
            AgentStatus::Completed
        ));

        let cancelled = Err("task cancelled".to_string());
        assert!(matches!(
            status_from_execution_result(&cancelled),
            AgentStatus::Cancelled
        ));

        let failed = Err("network error".to_string());
        match status_from_execution_result(&failed) {
            AgentStatus::Error(message) => assert!(message.contains("network error")),
            other => panic!("unexpected status: {other:?}"),
        }
    }
}
