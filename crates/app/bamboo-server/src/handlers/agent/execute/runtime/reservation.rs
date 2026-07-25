use crate::app_state::AppState;
use bamboo_agent_core::AgentEvent;
use bamboo_engine::execution::{
    reserve_session_execution, SessionExecutionReservation, SessionExecutionReserveOutcome,
};

pub(in crate::handlers::agent::execute) enum RunnerReservation {
    Started(SessionExecutionReservation),
    AlreadyRunning(String),
}

/// Reserve a runner for the fresh-execute path.
///
/// Delegates to the engine's [`reserve_runner_core`], which performs the
/// check-existing → replace-stale → insert-fresh sequence AND re-asserts the
/// session sender into `session_event_senders` under the runners write lock
/// (closing the idle-eviction TOCTOU, #346). Sharing that core with the engine's
/// `try_reserve_runner` (used by the resume paths) guarantees the re-assert
/// cannot drift between the two reservation entry points.
pub(in crate::handlers::agent::execute) async fn reserve_runner(
    state: &AppState,
    session_id: &str,
    session_tx: &tokio::sync::broadcast::Sender<AgentEvent>,
) -> RunnerReservation {
    match reserve_session_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        session_id,
        session_tx,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => {
            RunnerReservation::Started(reservation)
        }
        SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
            tracing::debug!(
                "[{}] Runner already running, returning status: already_running",
                session_id
            );
            RunnerReservation::AlreadyRunning(run_id)
        }
    }
}
