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

use bamboo_agent_core::{AgentError, AgentEvent};

use super::runner_state::{AgentRunner, AgentStatus};

/// Reservation result from `try_reserve_runner`.
#[derive(Debug, Clone)]
pub struct RunnerReservation {
    pub cancel_token: CancellationToken,
    pub run_id: String,
}

/// Try to reserve a runner for the given session.
///
/// If a runner with `Running` status already exists, returns `None`
/// (caller should skip execution). The `AlreadyRunning` case is surfaced
/// by the caller via `ExecuteResponse` with the *existing* runner's `run_id`
/// so the frontend can correlate subsequent SSE events.
///
/// Otherwise removes any stale runner and inserts a fresh one, returning
/// the associated `CancellationToken` and the new `run_id`.
///
/// Unlike the server's `reserve_runner`, this does NOT re-assert `event_sender`
/// into the `session_event_senders` map after the idle-eviction sweep (#346).
/// It is only reached for spawn / schedule sessions, which are single-shot
/// (sub-agents, guardians) or fresh-uuid-per-fire (scheduler) and thus never
/// re-executed under the same id, so the evict-then-re-execute race the server
/// path guards against is unreachable here.
pub async fn try_reserve_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    event_sender: &broadcast::Sender<AgentEvent>,
) -> Option<RunnerReservation> {
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
    let reservation = RunnerReservation {
        cancel_token: runner.cancel_token.clone(),
        run_id: runner.run_id.clone(),
    };

    guard.insert(session_id.to_string(), runner);
    Some(reservation)
}

/// Map an execution result to `AgentStatus`.
pub fn status_from_execution_result(result: &Result<(), AgentError>) -> AgentStatus {
    match result {
        Ok(_) => AgentStatus::Completed,
        Err(error) if error.is_cancelled() => AgentStatus::Cancelled,
        Err(error) => AgentStatus::Error(error.to_string()),
    }
}

/// Update a runner's terminal status and completion timestamp.
pub async fn finalize_runner(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    result: &Result<(), AgentError>,
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
        let ok_result: Result<(), AgentError> = Ok(());
        assert!(matches!(
            status_from_execution_result(&ok_result),
            AgentStatus::Completed
        ));

        // Cancellation is detected by matching the `AgentError::Cancelled`
        // variant, not by substring-matching the (display) message — note the
        // variant's message is "Cancelled", which would not even contain the
        // lowercase "cancelled" the old code searched for.
        let cancelled: Result<(), AgentError> = Err(AgentError::Cancelled);
        assert!(matches!(
            status_from_execution_result(&cancelled),
            AgentStatus::Cancelled
        ));

        let failed: Result<(), AgentError> = Err(AgentError::LLM("network error".to_string()));
        match status_from_execution_result(&failed) {
            AgentStatus::Error(message) => assert!(message.contains("network error")),
            other => panic!("unexpected status: {other:?}"),
        }
    }
}
