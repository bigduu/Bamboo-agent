//! Human-owned fixed-interval turns in an existing Session. Each tick reserves
//! the ordinary execution owner and releases it between turns.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bamboo_domain::session::PACED_LOOP_METADATA_KEY;
use bamboo_domain::{AgentStatusState, Session, SessionInboxPort, SessionKind, Storage};
use bamboo_engine::{ChildCompletionCoordinator, SessionCache, SessionSnapshot};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_PROMPT_BYTES: usize = 16 * 1024;
const LIFETIME: TimeDelta = TimeDelta::days(7);
const RETRY_DELAY: TimeDelta = TimeDelta::seconds(5);
const MAX_ATTEMPTS_PER_TICK: usize = 4;
const ADMISSION_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

const SYSTEM_GUIDANCE: &str = "This turn was triggered by the user's fixed-interval Bamboo loop. Work on the scheduled request for this iteration, using relevant context from this session. Re-check the current state before repeating work with side effects. Use the normal tool and permission policy. If an action fails or requires input, briefly report what happened and what remains. Avoid repeatedly retrying an unavailable action within this turn. Finish the turn normally: Bamboo owns the timer and will attempt later iterations, including after errors. Do not wait for the next tick in this turn or create another schedule to continue this loop.";

pub(crate) enum PacedLoopCommand {
    Status,
    Start {
        interval_seconds: u64,
        prompt: String,
    },
    Stop,
    Resume,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PacedLoopError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
    #[error("Session not found")]
    NotFound,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum LoopStatus {
    Scheduled,
    Paused,
    Stopped,
    Expired,
}

#[derive(Serialize)]
pub(crate) struct PacedLoopRecord {
    id: Uuid,
    prompt: String,
    interval_seconds: u64,
    state: LoopStatus,
    next_fire_at: Option<DateTime<Utc>>,
    expires_at: DateTime<Utc>,
    iteration: u64,
    last_run_id: Option<String>,
    last_run_status: Option<&'static str>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PacedLoopState {
    version: u32,
    id: Uuid,
    host_instance: Uuid,
    interval_seconds: u64,
    prompt: String,
    next_fire_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    stopped: bool,
    iteration: u64,
    last_run_id: Option<String>,
    dispatch_failed: bool,
}

impl PacedLoopState {
    fn read(session: &Session) -> Result<Option<Self>, PacedLoopError> {
        let Some(raw) = session.metadata.get(PACED_LOOP_METADATA_KEY) else {
            return Ok(None);
        };
        let state = serde_json::from_str::<Self>(raw)
            .map_err(|_| PacedLoopError::Internal("Stored session loop is unreadable".into()))?;
        if state.version != 1 || validate_start(state.interval_seconds, &state.prompt).is_err() {
            return Err(PacedLoopError::Internal(
                "Stored session loop is invalid".into(),
            ));
        }
        Ok(Some(state))
    }

    fn status(&self, host_instance: Uuid, now: DateTime<Utc>) -> LoopStatus {
        if self.stopped {
            LoopStatus::Stopped
        } else if self.expires_at <= now {
            LoopStatus::Expired
        } else if self.host_instance != host_instance {
            LoopStatus::Paused
        } else {
            LoopStatus::Scheduled
        }
    }

    fn last_run_status(&self, session: &Session) -> Option<&'static str> {
        if self.dispatch_failed {
            return Some("dispatch_failed");
        }
        let run_id = self.last_run_id.as_deref()?;
        let outcome = session
            .metadata
            .get(bamboo_domain::session::PACED_LOOP_OUTCOME_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
        if let Some(status) = outcome
            .as_ref()
            .filter(|outcome| outcome["run_id"].as_str() == Some(run_id))
            .and_then(|outcome| outcome["status"].as_str())
            .and_then(|status| match status {
                "completed" => Some("completed"),
                "failed" => Some("failed"),
                "cancelled" => Some("cancelled"),
                "suspended" => Some("suspended"),
                _ => None,
            })
        {
            return Some(status);
        }
        if session.agent_runtime_state.as_ref().is_some_and(|runtime| {
            runtime.run_id == run_id
                && matches!(
                    runtime.status,
                    AgentStatusState::Initializing
                        | AgentStatusState::Running
                        | AgentStatusState::Finalizing
                )
        }) {
            Some("running")
        } else {
            Some("unconfirmed")
        }
    }

    fn project(
        &self,
        session: &Session,
        host_instance: Uuid,
        now: DateTime<Utc>,
    ) -> PacedLoopRecord {
        let state = self.status(host_instance, now);
        PacedLoopRecord {
            id: self.id,
            prompt: self.prompt.clone(),
            interval_seconds: self.interval_seconds,
            state,
            next_fire_at: matches!(state, LoopStatus::Scheduled)
                .then_some(self.next_fire_at)
                .filter(|at| *at < self.expires_at),
            expires_at: self.expires_at,
            iteration: self.iteration,
            last_run_id: self.last_run_id.clone(),
            last_run_status: self.last_run_status(session),
        }
    }

    fn advance(&mut self, now: DateTime<Utc>) {
        let interval = self.interval_seconds as i64;
        let elapsed = (now - self.next_fire_at).num_seconds().max(0);
        let cadence_steps = elapsed / interval + 1;
        self.next_fire_at += TimeDelta::seconds(cadence_steps * interval);
        self.iteration += 1;
    }
}

fn validate_start(interval_seconds: u64, prompt: &str) -> Result<(), PacedLoopError> {
    if !(60..=86_400).contains(&interval_seconds) {
        return Err(PacedLoopError::Invalid(
            "Loop interval must be between 1 minute and 1 day".into(),
        ));
    }
    if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return Err(PacedLoopError::Invalid(
            "Loop prompt must contain 1 to 16384 UTF-8 bytes".into(),
        ));
    }
    Ok(())
}

fn supported_root(session: &Session) -> bool {
    session.kind == SessionKind::Root
        && session.authority_identity.is_ordinary()
        && session.parent_session_id.is_none()
        && session.root_thinking_mode() == bamboo_domain::session::RootThinkingMode::Standard
        && !session
            .metadata
            .contains_key(bamboo_engine::ticket_runtime::TICKET_DISPATCH_KEY)
}

fn waiting_for_existing_work(session: &Session) -> bool {
    session.has_pending_question()
        || session.metadata.contains_key("runtime.suspend_reason")
        || session
            .metadata
            .contains_key(bamboo_domain::ONCE_RESPONSE_HANDOFF_KEY)
        || bamboo_engine::session_app::execute::has_pending_user_message(session)
        || session.agent_runtime_state.as_ref().is_some_and(|runtime| {
            runtime.suspension.is_some()
                || runtime.waiting_for_children.is_some()
                || runtime.waiting_for_bash.is_some()
                || matches!(
                    runtime.status,
                    AgentStatusState::Initializing
                        | AgentStatusState::Running
                        | AgentStatusState::Finalizing
                        | AgentStatusState::Suspended
                )
        })
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Due {
    generation: Uuid,
    at: DateTime<Utc>,
}

pub(super) struct PacedLoops {
    host_instance: Uuid,
    // Process-local hints only. The canonical Session record controls admission.
    due: Mutex<HashMap<String, Due>>,
    store: Arc<SessionStoreV2>,
    persistence: Arc<LockedSessionStore>,
    sessions: SessionCache,
    inbox: Arc<dyn SessionInboxPort>,
    coordinator: Arc<ChildCompletionCoordinator>,
}

impl PacedLoops {
    pub(super) fn new(
        store: Arc<SessionStoreV2>,
        persistence: Arc<LockedSessionStore>,
        sessions: SessionCache,
        inbox: Arc<dyn SessionInboxPort>,
        coordinator: Arc<ChildCompletionCoordinator>,
    ) -> Self {
        Self {
            host_instance: Uuid::new_v4(),
            due: Mutex::new(HashMap::new()),
            store,
            persistence,
            sessions,
            inbox,
            coordinator,
        }
    }

    fn schedule(&self, id: &str, state: &PacedLoopState, now: DateTime<Utc>) {
        let mut due = self.due.lock().expect("paced loop hints poisoned");
        if matches!(state.status(self.host_instance, now), LoopStatus::Scheduled) {
            due.insert(
                id.to_owned(),
                Due {
                    generation: state.id,
                    at: state.next_fire_at.min(state.expires_at),
                },
            );
        } else {
            due.remove(id);
        }
    }

    async fn commit(
        &self,
        session: &Session,
        state: &PacedLoopState,
    ) -> Result<Session, PacedLoopError> {
        let value = serde_json::to_string(state)
            .map_err(|_| PacedLoopError::Internal("Could not serialize session loop".into()))?;
        let saved = self.store.save_paced_loop_metadata(session, value).await.map_err(|error| {
            match error.kind() {
                std::io::ErrorKind::NotFound => PacedLoopError::NotFound,
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::AlreadyExists => {
                    PacedLoopError::Conflict("Session changed while updating the loop. Try again.".into())
                }
                _ => {
                    tracing::warn!(session_id = %session.id, %error, "could not persist session loop");
                    PacedLoopError::Internal("Could not persist the session loop".into())
                }
            }
        })?;
        self.sessions.insert(
            saved.id.clone(),
            Arc::new(SessionSnapshot::new(saved.clone())),
        );
        Ok(saved)
    }

    async fn load(&self, id: &str) -> Result<Session, PacedLoopError> {
        self.store
            .load_session(id)
            .await
            .map_err(|error| {
                tracing::warn!(session_id = id, %error, "could not load loop Session");
                PacedLoopError::Internal("Could not load the session".into())
            })?
            .ok_or(PacedLoopError::NotFound)
    }

    async fn apply(
        &self,
        id: &str,
        command: PacedLoopCommand,
    ) -> Result<Option<PacedLoopRecord>, PacedLoopError> {
        let _guard = self.persistence.acquire_lock(id).await;
        let session = self.load(id).await?;
        let now = Utc::now();
        let previous = PacedLoopState::read(&session)?;
        let state = match command {
            PacedLoopCommand::Status => {
                return Ok(previous.map(|state| state.project(&session, self.host_instance, now)));
            }
            PacedLoopCommand::Start {
                interval_seconds,
                prompt,
            } => {
                validate_start(interval_seconds, &prompt)?;
                if !supported_root(&session) {
                    return Err(PacedLoopError::Conflict(
                        "Loops require a Standard ordinary Root session".into(),
                    ));
                }
                if previous.as_ref().is_some_and(|state| {
                    matches!(
                        state.status(self.host_instance, now),
                        LoopStatus::Scheduled | LoopStatus::Paused
                    )
                }) {
                    return Err(PacedLoopError::Conflict(
                        "This session already has a loop. Stop or resume it first.".into(),
                    ));
                }
                PacedLoopState {
                    version: 1,
                    id: Uuid::new_v4(),
                    host_instance: self.host_instance,
                    interval_seconds,
                    prompt,
                    next_fire_at: now + TimeDelta::seconds(interval_seconds as i64),
                    expires_at: now + LIFETIME,
                    stopped: false,
                    iteration: 0,
                    last_run_id: None,
                    dispatch_failed: false,
                }
            }
            PacedLoopCommand::Stop => {
                let Some(mut state) = previous else {
                    return Ok(None);
                };
                if matches!(
                    state.status(self.host_instance, now),
                    LoopStatus::Stopped | LoopStatus::Expired
                ) {
                    return Ok(Some(state.project(&session, self.host_instance, now)));
                }
                state.stopped = true;
                state
            }
            PacedLoopCommand::Resume => {
                let Some(mut state) = previous else {
                    return Err(PacedLoopError::Conflict(
                        "There is no loop to resume".into(),
                    ));
                };
                if !supported_root(&session)
                    || !matches!(state.status(self.host_instance, now), LoopStatus::Paused)
                {
                    return Err(PacedLoopError::Conflict(
                        "Only a paused loop in a Standard ordinary Root session can resume".into(),
                    ));
                }
                state.host_instance = self.host_instance;
                state.next_fire_at = now + TimeDelta::seconds(state.interval_seconds as i64);
                state
            }
        };
        // Register a provisional wake before a Start/Resume write whose
        // acknowledgement could be lost. The tick re-reads the exact canonical
        // generation and Host instance before it can reserve a turn.
        if matches!(state.status(self.host_instance, now), LoopStatus::Scheduled) {
            self.schedule(id, &state, now);
        }
        let saved = self.commit(&session, &state).await?;
        self.schedule(id, &state, now);
        Ok(Some(state.project(&saved, self.host_instance, now)))
    }

    pub(super) async fn tick(&self, now: DateTime<Utc>) {
        let mut ready: Vec<_> = self
            .due
            .lock()
            .expect("paced loop hints poisoned")
            .iter()
            .filter(|(_, due)| due.at <= now)
            .map(|(id, due)| (id.clone(), *due))
            .collect();
        ready.sort_unstable_by(|a, b| (a.1.at, &a.0).cmp(&(b.1.at, &b.0)));
        for (id, expected) in ready.into_iter().take(MAX_ATTEMPTS_PER_TICK) {
            let retry = match self.fire(&id, expected, now).await {
                Ok(retry) => retry,
                Err(PacedLoopError::NotFound) => false,
                Err(error) => {
                    tracing::warn!(session_id = id, %error, "session loop will retry");
                    true
                }
            };
            // A concurrent command or a successful occurrence may have already
            // replaced the hint. Never revive a stale generation on error.
            let mut due = self.due.lock().expect("paced loop hints poisoned");
            if due.get(&id) == Some(&expected) {
                if retry {
                    due.insert(
                        id,
                        Due {
                            at: now + RETRY_DELAY,
                            ..expected
                        },
                    );
                } else {
                    due.remove(&id);
                }
            }
        }
    }

    async fn fire(&self, id: &str, due: Due, now: DateTime<Utc>) -> Result<bool, PacedLoopError> {
        // Read behind any provisional start/resume under the same lock. This
        // keeps a slow, unacknowledged write from losing its only wake hint.
        // Release the lock before asking the router for an execution owner.
        let initial = {
            let Ok(_guard) =
                tokio::time::timeout(ADMISSION_WAIT, self.persistence.acquire_lock(id)).await
            else {
                return Ok(true);
            };
            self.load(id).await?
        };
        let Some(state) = PacedLoopState::read(&initial)?.filter(|state| {
            state.id == due.generation
                && matches!(state.status(self.host_instance, now), LoopStatus::Scheduled)
        }) else {
            return Ok(false);
        };
        if state.next_fire_at > now {
            // A prior write may have committed before its acknowledgement
            // failed. Rebuild the hint from canonical state, never replay it.
            let mut hints = self.due.lock().expect("paced loop hints poisoned");
            if hints.get(id) == Some(&due) {
                hints.insert(
                    id.to_owned(),
                    Due {
                        generation: state.id,
                        at: state.next_fire_at.min(state.expires_at),
                    },
                );
            }
            return Ok(false);
        }
        if !supported_root(&initial) || waiting_for_existing_work(&initial) {
            return Ok(true);
        }
        // An unrelated in-progress router handoff must not pin the Host's
        // shared wake tick. No durable loop update has happened at this stage.
        let Ok(Some(reservation)) =
            tokio::time::timeout(ADMISSION_WAIT, self.coordinator.reserve_paced_loop_turn(id))
                .await
        else {
            return Ok(true);
        };
        let Ok(guard) =
            tokio::time::timeout(ADMISSION_WAIT, self.persistence.acquire_lock(id)).await
        else {
            drop(reservation);
            return Ok(true);
        };
        let result = self.admit(id, due, now, state.iteration, reservation).await;
        drop(guard);
        result
    }

    fn current_due(
        &self,
        session: &Session,
        due: Due,
        now: DateTime<Utc>,
    ) -> Result<Option<PacedLoopState>, PacedLoopError> {
        Ok(PacedLoopState::read(session)?.filter(|state| {
            state.id == due.generation
                && state.next_fire_at <= now
                && matches!(state.status(self.host_instance, now), LoopStatus::Scheduled)
        }))
    }

    async fn admit(
        &self,
        id: &str,
        due: Due,
        now: DateTime<Utc>,
        previous_iteration: u64,
        mut reservation: bamboo_engine::execution::SessionExecutionReservation,
    ) -> Result<bool, PacedLoopError> {
        let prepared = async {
            let session = self.load(id).await?;
            let Some(state) = self.current_due(&session, due, now.max(Utc::now()))? else {
                return Ok(None);
            };
            if state.iteration != previous_iteration
                || !supported_root(&session)
                || waiting_for_existing_work(&session)
            {
                return Ok(None);
            }
            let backlog = self.inbox.inspect(id).await.map_err(|_| {
                PacedLoopError::Internal("Session Inbox is temporarily unavailable".into())
            })?;
            if backlog.activation_pending() {
                return Ok(None);
            }
            self.coordinator
                .prepare_paced_loop_turn(&mut reservation, &session)
                .await
                .map_err(|error| {
                    tracing::warn!(session_id = id, %error, "could not bind session loop turn");
                    PacedLoopError::Internal("Could not prepare the session loop turn".into())
                })?;
            Ok(Some((session, state)))
        }
        .await;

        let (session, mut state) = match prepared {
            Ok(Some(candidate)) => candidate,
            result => {
                // Release outside the caller-held persistence guard. The
                // reservation itself provides cancellation-safe cleanup.
                drop(reservation);
                return result.map(|_| true);
            }
        };
        let now = now.max(Utc::now());
        if !matches!(state.status(self.host_instance, now), LoopStatus::Scheduled) {
            drop(reservation);
            return Ok(false);
        }
        let previous = state.last_run_status(&session).unwrap_or("none");
        state.advance(now);
        state.last_run_id = Some(reservation.run_id().to_owned());
        state.dispatch_failed = false;
        let saved = self.commit(&session, &state).await?;
        self.schedule(id, &state, now);
        let guidance = format!(
            "{SYSTEM_GUIDANCE}\n\nScheduled iteration {}. Previous scheduled turn status: {previous}.",
            state.iteration
        );
        if let Err(error) = self
            .coordinator
            .dispatch_paced_loop_turn(
                reservation,
                saved.clone(),
                state.prompt.clone(),
                state.id,
                state.iteration,
                Some(guidance),
            )
            .await
        {
            tracing::warn!(session_id = id, %error, "session loop dispatch failed; future iterations stay scheduled");
            state.dispatch_failed = true;
            self.commit(&saved, &state).await?;
        }
        Ok(false)
    }
}

impl super::AppState {
    pub(crate) async fn apply_paced_loop(
        &self,
        session_id: &str,
        command: PacedLoopCommand,
    ) -> Result<Option<PacedLoopRecord>, PacedLoopError> {
        if matches!(command, PacedLoopCommand::Status) {
            return self.paced_loops.apply(session_id, command).await;
        }
        // As with native chat admission, a dropped HTTP request must not
        // cancel the canonical write or leave its scheduler handoff stranded.
        let loops = self.paced_loops.clone();
        let session_id = session_id.to_owned();
        tokio::spawn(async move { loops.apply(&session_id, command).await })
            .await
            .map_err(|error| {
                tracing::error!(%error, "session loop command task failed");
                PacedLoopError::Internal("Session loop command did not finish".into())
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overdue_ticks_advance_once_and_keep_the_original_cadence() {
        let due = DateTime::from_timestamp(1_000, 0).unwrap();
        let mut state = PacedLoopState {
            version: 1,
            id: Uuid::new_v4(),
            host_instance: Uuid::new_v4(),
            interval_seconds: 60,
            prompt: "Check the build".to_owned(),
            next_fire_at: due,
            expires_at: due + LIFETIME,
            stopped: false,
            iteration: 0,
            last_run_id: None,
            dispatch_failed: false,
        };
        state.advance(due + TimeDelta::seconds(145));
        assert_eq!(state.iteration, 1);
        assert_eq!(state.next_fire_at, due + TimeDelta::seconds(180));
        assert!(matches!(
            state.status(Uuid::new_v4(), due),
            LoopStatus::Paused
        ));
        assert!(matches!(
            state.status(state.host_instance, state.expires_at),
            LoopStatus::Expired
        ));
    }
}

#[cfg(test)]
mod host_tests;
