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

use super::execute::{consume_pending_clarification_resume, has_pending_user_message};
use super::types::{ResumeConfigSnapshot, ResumeOutcome};
use crate::execution::{SessionExecutionReservation, SessionExecutionReserveOutcome};

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
    ///
    /// Implementations may merge concurrent UI edits to title/pinned/title_version
    /// from disk back into `session` (which is why this takes `&mut`).
    async fn save_and_cache_session(&self, session: &mut Session);

    /// Reserve the shared runner/router ownership for the given session.
    async fn reserve_session_execution(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> SessionExecutionReserveOutcome;

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
    pub execution_reservation: SessionExecutionReservation,
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
    let reservation = match port
        .reserve_session_execution(session_id, &event_sender)
        .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
            return ResumeOutcome::AlreadyRunning { run_id };
        }
    };
    let run_id = reservation.run_id().to_string();

    consume_pending_clarification_resume(&mut session);
    port.save_and_cache_session(&mut session).await;

    port.spawn_resume_execution(ResumeSpawnRequest {
        session_id: session_id.to_string(),
        session,
        execution_reservation: reservation,
        event_sender,
        config,
    })
    .await;

    ResumeOutcome::Started { run_id }
}
