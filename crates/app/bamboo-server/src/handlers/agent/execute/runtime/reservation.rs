use tokio_util::sync::CancellationToken;

use crate::app_state::{AgentRunner, AgentStatus, AppState};
use bamboo_agent_core::AgentEvent;

pub(in crate::handlers::agent::execute) enum RunnerReservation {
    Started(CancellationToken, String),
    AlreadyRunning(String),
}

pub(in crate::handlers::agent::execute) async fn reserve_runner(
    state: &AppState,
    session_id: &str,
    session_tx: &tokio::sync::broadcast::Sender<AgentEvent>,
) -> RunnerReservation {
    let mut runners = state.agent_runners.write().await;

    // Check if there's already a running runner for this session.
    if let Some(runner) = runners.get(session_id) {
        if matches!(runner.status, AgentStatus::Running) {
            tracing::debug!(
                "[{}] Runner already running, returning status: already_running",
                session_id
            );
            return RunnerReservation::AlreadyRunning(runner.run_id.clone());
        }
        tracing::debug!(
            "[{}] Existing runner with status {:?}, will restart",
            session_id,
            runner.status
        );
    }

    // Remove stale runner and insert new one atomically.
    runners.remove(session_id);

    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    runner.event_sender = session_tx.clone();
    let cancel_token = runner.cancel_token.clone();
    let run_id = runner.run_id.clone();

    runners.insert(session_id.to_string(), runner);

    // Re-assert the session sender into the map, still holding the runners write
    // lock. The idle-eviction sweep (issue #346) may have removed this session's
    // sender between the caller's `get_session_event_sender` and this reservation
    // (it evicts an idle terminal session's runner+sender together). Without this
    // re-assert, a session re-executed after >5min idle would publish to the
    // `session_tx` clone we hold here while a later subscriber's
    // `get_or_create_event_sender` mints a *different* channel and sees nothing.
    // `or_insert_with` restores the SAME channel, so it is a no-op in the common
    // case. Lock order (runners ⊃ senders) matches the sweep, so this cannot
    // deadlock.
    state
        .session_event_senders
        .write()
        .await
        .entry(session_id.to_string())
        .or_insert_with(|| session_tx.clone());

    RunnerReservation::Started(cancel_token, run_id)
}
