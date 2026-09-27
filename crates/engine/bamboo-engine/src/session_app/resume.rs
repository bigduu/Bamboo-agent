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

use super::execute::has_pending_user_message;
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
    /// Implementations may merge concurrent UI edits to
    /// title/title_generated/pinned/title_version
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

    /// Transfer a prepared resume request to an execution owner whose lifetime
    /// is independent of the caller. Implementations that support response
    /// handoffs return `Ok(())` only after the request (including its exact
    /// runner reservation) has moved into a detached task.
    ///
    /// The default preserves source compatibility for external port adapters;
    /// they retain the original awaited `spawn_resume_execution` behavior until
    /// they opt into cancellation-safe dispatch.
    #[allow(clippy::result_large_err)]
    fn dispatch_resume_execution(
        &self,
        request: ResumeSpawnRequest,
    ) -> Result<(), ResumeSpawnRequest> {
        Err(request)
    }
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

/// Runner ownership reserved before consuming a pending clarification. This
/// closes the idle-check -> response-CAS -> resume-reserve gap: no competing
/// entrypoint can take the successor slot after the answer is durably cleared.
pub struct ResponseResumeHandoff {
    execution_reservation: SessionExecutionReservation,
    event_sender: broadcast::Sender<AgentEvent>,
}

impl ResponseResumeHandoff {
    pub fn subscribe(&self) -> broadcast::Receiver<AgentEvent> {
        self.event_sender.subscribe()
    }

    pub fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_sender.send(event);
    }

    pub async fn abandon(self) {
        self.execution_reservation.abandon().await;
    }
}

/// Atomically acquire the successor runner slot before a response transaction.
/// An existing suspended owner is allowed to finish; timeout returns its last
/// run id without consuming the pending question.
pub async fn reserve_response_resume_handoff(
    port: &dyn ResumeExecutionPort,
    session_id: &str,
    timeout: std::time::Duration,
) -> Result<ResponseResumeHandoff, ResumeOutcome> {
    let event_sender = port.get_or_create_event_sender(session_id).await;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match port
            .reserve_session_execution(session_id, &event_sender)
            .await
        {
            SessionExecutionReserveOutcome::Reserved(execution_reservation) => {
                return Ok(ResponseResumeHandoff {
                    execution_reservation,
                    event_sender,
                });
            }
            SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ResumeOutcome::AlreadyRunning { run_id });
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Start a resumed run using ownership acquired before the pending response
/// was consumed. The supplied session is the exact durable CAS result.
pub async fn resume_session_execution_with_handoff(
    port: &dyn ResumeExecutionPort,
    session_id: &str,
    session: Session,
    config: ResumeConfigSnapshot,
    handoff: ResponseResumeHandoff,
) -> ResumeOutcome {
    if !has_pending_user_message(&session) {
        tokio::spawn(async move {
            handoff.abandon().await;
        });
        return ResumeOutcome::Completed;
    }

    let ResponseResumeHandoff {
        execution_reservation,
        event_sender,
    } = handoff;
    let run_id = execution_reservation.run_id().to_string();
    let request = ResumeSpawnRequest {
        session_id: session_id.to_string(),
        session,
        execution_reservation,
        event_sender,
        config,
    };
    if let Err(request) = port.dispatch_resume_execution(request) {
        port.spawn_resume_execution(request).await;
    }
    ResumeOutcome::Started { run_id }
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
    let Some(session) = port.load_session(session_id).await else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{reserve_runner_core, AgentRunner, ReserveOutcome};
    use bamboo_agent_core::Message;
    use std::collections::{BTreeSet, HashMap};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify, RwLock};

    struct CancellingResumePort {
        durable: Mutex<Session>,
        runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        spawn_entered: Arc<Notify>,
        detached_release: Arc<Notify>,
        detached_finished: Arc<AtomicBool>,
        saw_marker_at_adapter: Arc<AtomicBool>,
        save_calls: AtomicUsize,
    }

    #[async_trait]
    impl ResumeExecutionPort for CancellingResumePort {
        async fn load_session(&self, _session_id: &str) -> Option<Session> {
            Some(self.durable.lock().await.clone())
        }

        async fn save_and_cache_session(&self, session: &mut Session) {
            self.save_calls.fetch_add(1, Ordering::SeqCst);
            *self.durable.lock().await = session.clone();
        }

        async fn reserve_session_execution(
            &self,
            session_id: &str,
            event_sender: &broadcast::Sender<AgentEvent>,
        ) -> SessionExecutionReserveOutcome {
            match reserve_runner_core(&self.runners, &self.senders, session_id, event_sender).await
            {
                ReserveOutcome::Reserved(reservation) => SessionExecutionReserveOutcome::Reserved(
                    SessionExecutionReservation::from_pending_registration(
                        session_id,
                        reservation,
                        None,
                        self.runners.clone(),
                    ),
                ),
                ReserveOutcome::AlreadyRunning(run_id) => {
                    SessionExecutionReserveOutcome::AlreadyRunning { run_id }
                }
            }
        }

        async fn get_or_create_event_sender(
            &self,
            session_id: &str,
        ) -> broadcast::Sender<AgentEvent> {
            if let Some(sender) = self.senders.read().await.get(session_id).cloned() {
                return sender;
            }
            let sender = broadcast::channel(16).0;
            self.senders
                .write()
                .await
                .insert(session_id.to_string(), sender.clone());
            sender
        }

        async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
            self.saw_marker_at_adapter.store(
                request
                    .session
                    .metadata
                    .contains_key("execute.startup_handoff_at"),
                Ordering::SeqCst,
            );
            self.spawn_entered.notify_one();
            std::future::pending::<()>().await;
        }

        fn dispatch_resume_execution(
            &self,
            request: ResumeSpawnRequest,
        ) -> Result<(), ResumeSpawnRequest> {
            self.saw_marker_at_adapter.store(
                request
                    .session
                    .metadata
                    .contains_key("execute.startup_handoff_at"),
                Ordering::SeqCst,
            );
            let spawn_entered = self.spawn_entered.clone();
            let detached_release = self.detached_release.clone();
            let detached_finished = self.detached_finished.clone();
            tokio::spawn(async move {
                let mut reservation = request.execution_reservation;
                reservation
                    .ensure_registered()
                    .await
                    .expect("detached owner registers exact successor");
                spawn_entered.notify_one();
                detached_release.notified().await;
                detached_finished.store(true, Ordering::SeqCst);
            });
            Ok(())
        }
    }

    fn test_resume_config() -> ResumeConfigSnapshot {
        ResumeConfigSnapshot {
            provider_name: "test".to_string(),
            provider_type: None,
            fast_model: None,
            fast_model_ref: None,
            background_model: None,
            background_model_ref: None,
            background_model_provider: None,
            summarization_model: None,
            summarization_model_ref: None,
            summarization_model_provider: None,
            disabled_tools: BTreeSet::new(),
            disabled_skill_ids: BTreeSet::new(),
            image_fallback: None,
            gold_config: None,
        }
    }

    #[tokio::test]
    async fn caller_cancellation_after_dispatch_keeps_detached_resume_owner() {
        let mut session = Session::new("resume-cancel", "model");
        session.add_message(Message::tool_result("call-1", "Selected response: A"));
        session.metadata.insert(
            "clarification_resume_pending".to_string(),
            "true".to_string(),
        );
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            "2026-08-10T00:00:00.000Z".to_string(),
        );
        let port = Arc::new(CancellingResumePort {
            durable: Mutex::new(session),
            runners: Arc::new(RwLock::new(HashMap::new())),
            senders: Arc::new(RwLock::new(HashMap::new())),
            spawn_entered: Arc::new(Notify::new()),
            detached_release: Arc::new(Notify::new()),
            detached_finished: Arc::new(AtomicBool::new(false)),
            saw_marker_at_adapter: Arc::new(AtomicBool::new(false)),
            save_calls: AtomicUsize::new(0),
        });

        let handoff = reserve_response_resume_handoff(
            port.as_ref(),
            "resume-cancel",
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("reserve exact response successor");
        let committed_session = port.durable.lock().await.clone();
        let task_port = port.clone();
        let resume = tokio::spawn(async move {
            let outcome = resume_session_execution_with_handoff(
                task_port.as_ref(),
                "resume-cancel",
                committed_session,
                test_resume_config(),
                handoff,
            )
            .await;
            assert!(matches!(outcome, ResumeOutcome::Started { .. }));
            std::future::pending::<()>().await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            port.spawn_entered.notified(),
        )
        .await
        .expect("detached resume owner must register the successor");
        resume.abort();
        let _ = resume.await;
        port.detached_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !port.detached_finished.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached owner must survive caller cancellation");

        assert!(port.saw_marker_at_adapter.load(Ordering::SeqCst));
        assert_eq!(port.save_calls.load(Ordering::SeqCst), 0);
        let durable = port.durable.lock().await;
        assert_eq!(
            durable
                .metadata
                .get("clarification_resume_pending")
                .map(String::as_str),
            Some("true")
        );
        assert!(durable.metadata.contains_key("execute.startup_handoff_at"));
    }
}
