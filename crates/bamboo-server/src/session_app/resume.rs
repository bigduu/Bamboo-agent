//! Resume execution use case.
//!
//! Provides the application-layer logic for resuming agent execution on an
//! existing session (e.g. after a user responds to a pending question).
//! The server layer implements `ResumeExecutionPort` to supply the
//! infrastructure operations.

use async_trait::async_trait;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::Session;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::execute::{consume_pending_conclusion_with_options_resume, has_pending_user_message};
use super::types::{ResumeConfigSnapshot, ResumeOutcome};
use bamboo_engine::execution::runner_lifecycle::RunnerReservation;

// ---------------------------------------------------------------------------
// Port trait
// ---------------------------------------------------------------------------

/// Adapter trait for resume execution infrastructure.
///
/// Implementations bridge the use case to server-specific concerns
/// (storage, runner lifecycle, agent spawning).
#[async_trait]
pub trait ResumeExecutionPort: Send + Sync {
    /// Load a session by ID. Returns `None` if not found.
    async fn load_session(&self, session_id: &str) -> Option<Session>;

    /// Persist a session and update any caches.
    async fn save_and_cache_session(&self, session: &Session);

    /// Try to reserve a runner slot for the given session.
    /// Returns `None` if a runner is already active.
    async fn try_reserve_runner(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<RunnerReservation>;

    /// Get or create the long-lived broadcast sender for session events.
    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent>;

    /// Spawn the resume execution loop in the background.
    ///
    /// The adapter creates the mpsc channel, spawns the event forwarder,
    /// and calls the server's agent execution spawner.
    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest);
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// Request captured for the adapter to spawn a resumed agent execution.
///
/// This bundles everything the server-side spawner needs, keeping the
/// application layer free of `AppState` and server-specific types.
pub struct ResumeSpawnRequest {
    pub session_id: String,
    pub session: Session,
    pub cancel_token: CancellationToken,
    pub run_id: String,
    pub event_sender: broadcast::Sender<AgentEvent>,
    pub config: ResumeConfigSnapshot,
}

// ---------------------------------------------------------------------------
// Use case
// ---------------------------------------------------------------------------

/// Resume agent execution on an existing session.
///
/// Returns the outcome of the resume attempt:
/// - `Started` — execution spawned successfully
/// - `AlreadyRunning` — a runner is already active
/// - `Completed` — no pending user message
/// - `NotFound` — session not found
pub async fn resume_session_execution(
    port: &dyn ResumeExecutionPort,
    session_id: &str,
    config: ResumeConfigSnapshot,
) -> ResumeOutcome {
    // Load session.
    let Some(mut session) = port.load_session(session_id).await else {
        return ResumeOutcome::NotFound;
    };

    if !has_pending_user_message(&session) {
        return ResumeOutcome::Completed;
    }

    // Reserve runner slot.
    let event_sender = port.get_or_create_event_sender(session_id).await;
    let Some(reservation) = port.try_reserve_runner(session_id, &event_sender).await else {
        return ResumeOutcome::AlreadyRunning;
    };

    consume_pending_conclusion_with_options_resume(&mut session);
    port.save_and_cache_session(&session).await;

    port.spawn_resume_execution(ResumeSpawnRequest {
        session_id: session_id.to_string(),
        session,
        cancel_token: reservation.cancel_token,
        run_id: reservation.run_id,
        event_sender,
        config,
    })
    .await;

    ResumeOutcome::Started
}
