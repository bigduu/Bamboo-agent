use std::sync::{Arc, Mutex};
use std::time::Duration;

use actix_web::web;
use tokio::sync::broadcast;

use crate::app_state::{AgentStatus, AppState};
use bamboo_agent_core::agent::events::TokenUsage;
use bamboo_agent_core::agent::Role;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::AgentStatusState;
use bamboo_engine::session_app::execute::{
    clear_startup_handoff, consume_pending_clarification_resume, has_pending_clarification_resume,
    has_pending_retry_resume, mark_startup_handoff, startup_handoff_at,
};

const PENDING_TURN_ID_KEY: &str = "execute.pending_turn_message_id";
const PENDING_TURN_GRACE_SECS: i64 = 60;
const STARTUP_FAILURE_PREFIX: &str = "Agent startup failed: ";
const EXPIRED_STARTUP_RECHECK: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const IDLE_STARTUP_PROBE: Duration = Duration::from_secs(15);
#[cfg(test)]
const IDLE_STARTUP_PROBE: Duration = Duration::from_millis(25);

/// Owns one in-process `/execute` preparation for a session. The registry is
/// AppState-scoped (not process-global), and reference counting keeps an
/// overlapping request protected until its last handler returns.
pub(crate) struct ExecuteStartupGuard {
    startups: Arc<Mutex<std::collections::HashMap<String, usize>>>,
    session_id: String,
    active: bool,
}

impl ExecuteStartupGuard {
    /// Release this request's preparation ownership and return the number of
    /// other startup owners still active for the session.
    pub(crate) fn release(&mut self) -> usize {
        if !self.active {
            return self
                .startups
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&self.session_id)
                .copied()
                .unwrap_or(0);
        }
        self.active = false;
        let mut startups = self.startups.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = startups.get_mut(&self.session_id) {
            *count -= 1;
            if *count == 0 {
                startups.remove(&self.session_id);
                return 0;
            }
            return *count;
        }
        0
    }
}

impl Drop for ExecuteStartupGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) fn begin_execute_startup(state: &AppState, session_id: &str) -> ExecuteStartupGuard {
    let mut startups = state
        .execute_startups
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *startups.entry(session_id.to_string()).or_default() += 1;
    drop(startups);
    ExecuteStartupGuard {
        startups: state.execute_startups.clone(),
        session_id: session_id.to_string(),
        active: true,
    }
}

pub(crate) fn execute_startup_is_in_flight(state: &AppState, session_id: &str) -> bool {
    state
        .execute_startups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(session_id)
}

pub(crate) fn mark_pending_turn(session: &mut Session) {
    if let Some(message) = session.messages.last() {
        session
            .metadata
            .insert(PENDING_TURN_ID_KEY.to_string(), message.id.clone());
    }
    session.set_last_run_status("pending");
    session.clear_last_run_error();
    mark_startup_handoff(session);
}

pub(crate) fn clear_pending_turn(session: &mut Session) {
    session.metadata.remove(PENDING_TURN_ID_KEY);
    clear_startup_handoff(session);
}

pub(crate) fn pending_turn_id(session: &Session) -> Option<String> {
    if !matches!(session.last_run_status().as_deref(), Some("pending")) {
        return None;
    }
    let message = session
        .messages
        .last()
        .filter(|message| matches!(message.role, Role::User))?;
    match session.metadata.get(PENDING_TURN_ID_KEY) {
        Some(owner) if owner == &message.id => Some(owner.clone()),
        Some(_) => None,
        // Backward compatibility for a pending turn persisted before the
        // ownership marker shipped: last-User id is still an exact CAS token.
        None => Some(message.id.clone()),
    }
}

/// Exact durable message token for any work accepted by `/execute`: a normal
/// pending User turn or a clarification/retry resume whose last message is a
/// tool result. Used as a compare-and-set owner across async preparation.
pub(crate) fn startup_work_id(session: &Session) -> Option<String> {
    pending_turn_id(session).or_else(|| {
        (has_pending_clarification_resume(session) || has_pending_retry_resume(session))
            .then(|| session.messages.last().map(|message| message.id.clone()))
            .flatten()
    })
}

pub(crate) fn mark_startup_failed_if_owned(
    session: &mut Session,
    expected_turn_id: &str,
    detail: &str,
) -> bool {
    if startup_work_id(session).as_deref() != Some(expected_turn_id) {
        return false;
    }
    session.set_last_run_status("error");
    session.set_last_run_error(format!("{STARTUP_FAILURE_PREFIX}{detail}"));
    clear_pending_turn(session);
    consume_pending_clarification_resume(session);
    true
}

pub(crate) enum StartupFailureTarget<'a> {
    WorkId {
        work_id: &'a str,
        startup_guard: &'a mut ExecuteStartupGuard,
    },
    Abandoned(&'a broadcast::Receiver<AgentEvent>),
}

/// Atomically reject startup work if the latest durable session still belongs
/// to `target`.
///
/// The per-session persistence lock covers the latest load, ownership CAS, raw
/// save, cache replacement, and broadcast. A stale preparation/rollback can
/// therefore never overwrite or notify after a newer chat turn is persisted.
pub(crate) async fn transition_startup_failure_if_owned(
    state: &web::Data<AppState>,
    session_id: &str,
    target: StartupFailureTarget<'_>,
    detail: &str,
) -> bool {
    let (mut expected_work_id, abandoned_receiver) = match target {
        StartupFailureTarget::WorkId {
            work_id,
            startup_guard,
        } => {
            // Consume this failing request while holding the persistence lock.
            // Only the last preparation failure may own the durable rollback;
            // this prevents both premature failure and the dual-failure gap.
            (Some((work_id, startup_guard)), None)
        }
        StartupFailureTarget::Abandoned(receiver) => (None, Some(receiver)),
    };
    if abandoned_receiver.is_some_and(|receiver| !receiver.is_empty()) {
        return false;
    }

    let _session_guard = state.persistence.acquire_lock(session_id).await;
    if let Some((_, startup_guard)) = expected_work_id.as_mut() {
        if (*startup_guard).release() != 0 {
            return false;
        }
    }
    // An overlapping execute may already own this same durable work id but not
    // yet have persisted the marker clear. Its live runner is authoritative.
    if matches!(
        state
            .agent_runners
            .read()
            .await
            .get(session_id)
            .map(|runner| &runner.status),
        Some(AgentStatus::Pending | AgentStatus::Running)
    ) {
        return false;
    }
    if let Some(receiver) = abandoned_receiver {
        if !receiver.is_empty() || execute_startup_is_in_flight(state.get_ref(), session_id) {
            return false;
        }
    }

    let Ok(Some(mut session)) = state.storage.load_session(session_id).await else {
        return false;
    };
    let expected_work_id = match expected_work_id {
        Some((work_id, _)) => work_id.to_string(),
        None => {
            let Some(work_id) = startup_work_id(&session) else {
                return false;
            };
            if startup_work_is_active(&session) {
                return false;
            }
            work_id
        }
    };

    // Execute registers its startup guard while holding this same persistence
    // lock. Re-read all live ownership sources immediately before the CAS.
    if matches!(
        state
            .agent_runners
            .read()
            .await
            .get(session_id)
            .map(|runner| &runner.status),
        Some(AgentStatus::Pending | AgentStatus::Running)
    ) {
        return false;
    }
    if let Some(receiver) = abandoned_receiver {
        if !receiver.is_empty() || execute_startup_is_in_flight(state.get_ref(), session_id) {
            return false;
        }
    }

    if !mark_startup_failed_if_owned(&mut session, &expected_work_id, detail) {
        return false;
    }
    session.updated_at = chrono::Utc::now();
    let message = session
        .last_run_error()
        .unwrap_or_else(|| format!("{STARTUP_FAILURE_PREFIX}{detail}"));
    if let Err(error) = state.storage.save_session(&session).await {
        tracing::error!("[{session_id}] failed to persist startup failure transition: {error}");
        // A concrete execute request has already failed and released its last
        // startup owner. Even if durable storage is unavailable, unblock its
        // currently-connected client with the exact owned failure. An
        // abandoned-startup probe must instead stay silent and retry: emitting
        // without clearing its durable marker would create duplicate terminals.
        if abandoned_receiver.is_none() {
            let sender = state.get_session_event_sender(session_id).await;
            let _ = sender.send(AgentEvent::Error { message });
        }
        return false;
    }
    state.sessions.insert(
        session_id.to_string(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(session)),
    );
    let sender = state.get_session_event_sender(session_id).await;
    let _ = sender.send(AgentEvent::Error { message });
    true
}

fn startup_work_is_active(session: &Session) -> bool {
    let Some(started_at) = startup_work_started_at(session) else {
        return false;
    };
    chrono::Utc::now().signed_duration_since(started_at)
        <= chrono::Duration::seconds(PENDING_TURN_GRACE_SECS)
}

fn startup_work_started_at(session: &Session) -> Option<chrono::DateTime<chrono::Utc>> {
    startup_work_id(session)?;
    startup_handoff_at(session)
        .or_else(|| session.messages.last().map(|message| message.created_at))
}

fn startup_failure_message(session: &Session) -> Option<String> {
    matches!(session.last_run_status().as_deref(), Some("error"))
        .then(|| session.last_run_error())
        .flatten()
        .filter(|message| message.starts_with(STARTUP_FAILURE_PREFIX))
}

/// Delay until the next live-transport startup probe. Active work sleeps until
/// its exact durable grace expires; idle transports probe infrequently so a
/// later chat/retry written after subscription still arms reconciliation.
pub(crate) async fn startup_reconcile_delay(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Duration {
    let Some(session) = state.storage.load_session(session_id).await.ok().flatten() else {
        return IDLE_STARTUP_PROBE;
    };
    let Some(started_at) = startup_work_started_at(&session) else {
        return IDLE_STARTUP_PROBE;
    };
    let remaining = chrono::Duration::seconds(PENDING_TURN_GRACE_SECS)
        - chrono::Utc::now().signed_duration_since(started_at);
    if remaining <= chrono::Duration::zero() {
        return EXPIRED_STARTUP_RECHECK;
    }
    let millis = remaining.num_milliseconds().max(1) as u64;
    Duration::from_millis(millis + 25)
}

pub(crate) async fn terminal_event_if_ready(
    state: &web::Data<AppState>,
    session_id: &str,
    runner_status: Option<AgentStatus>,
) -> Option<AgentEvent> {
    // A slow but legitimate execute may outlive the durable handoff grace
    // before it can reserve a runner. Its RAII owner wins until every
    // overlapping handler returns; a crash naturally drops the whole AppState.
    if execute_startup_is_in_flight(state.get_ref(), session_id) {
        return None;
    }
    let session = match state.storage.load_session(session_id).await {
        Ok(Some(session)) => Some(session),
        _ => None,
    };

    // Diagnostic snapshot of the inputs that decide terminal-vs-live. This is the
    // key signal for "subscribed repeatedly but never executes": if the last
    // message is not a User message (and nothing is suspended/pending), the
    // session looks "finished" so we emit a one-shot terminal and the client
    // reconnects — even though the user expected a run to start.
    if let Some(session) = session.as_ref() {
        let last_role = session.messages.last().map(|m| format!("{:?}", m.role));
        let runtime_status = session
            .agent_runtime_state
            .as_ref()
            .map(|rt| format!("{:?}", rt.status));
        tracing::debug!(
            "[{}] terminal_event_if_ready: messages={}, last_role={:?}, has_pending_question={}, runtime_status={:?}, runner_status={:?}",
            session_id,
            session.messages.len(),
            last_role,
            session.has_pending_question(),
            runtime_status,
            runner_status,
        );
    } else {
        tracing::debug!(
            "[{}] terminal_event_if_ready: session could not be loaded from storage (runner_status={:?})",
            session_id,
            runner_status,
        );
    }

    // `Pending` is the reservation window immediately before a runner becomes
    // `Running`. Treat it as live just like `Running`: a reconnect in this gap
    // must wait for the new run instead of closing on the previous history.
    if matches!(
        runner_status,
        Some(AgentStatus::Pending | AgentStatus::Running)
    ) {
        tracing::debug!(
            "[{}] terminal_event_if_ready -> None (runner is pending/running) -> LIVE stream",
            session_id,
        );
        return None;
    }

    // Admission must never synthesize an abandoned-startup terminal directly.
    // The startup guard/runner/durable owner can change after the initial
    // snapshots above, so a one-shot response here could close a subscriber
    // while a concurrent `/execute` is legitimately starting the same work.
    // Keep the transport live and let its periodic reconciler perform the
    // per-session-lock exact-work CAS before broadcasting any Error.
    if session.as_ref().and_then(startup_work_id).is_some() {
        tracing::debug!(
            "[{}] terminal_event_if_ready -> None (startup work requires locked reconcile) -> LIVE stream",
            session_id,
        );
        return None;
    }

    // Absence of an in-memory runner is not itself proof that a run finished:
    // a newly-created session is persisted before its runner is reserved. In
    // that window a subscriber must stay live for the first token instead of
    // receiving a synthetic Complete. Require positive durable/in-memory
    // evidence of a prior run before considering the one-shot terminal path.
    if !session_has_terminal_evidence(session.as_ref(), runner_status.as_ref()) {
        tracing::debug!(
            "[{}] terminal_event_if_ready -> None (session has not started) -> LIVE stream",
            session_id,
        );
        return None;
    }

    if session_prevents_terminal_event(session.as_ref(), runner_status.as_ref()) {
        tracing::debug!(
            "[{}] terminal_event_if_ready -> None (pending user message / pending question / suspended) -> LIVE stream",
            session_id,
        );
        return None;
    }
    if has_running_child(state, session_id).await {
        tracing::debug!(
            "[{}] terminal_event_if_ready -> None (running child session) -> LIVE stream",
            session_id,
        );
        return None;
    }
    tracing::debug!(
        "[{}] terminal_event_if_ready -> Some(terminal): no pending user message, not suspended, no running child",
        session_id,
    );
    Some(terminal_event_for_sources(session.as_ref(), runner_status))
}

/// Atomically consume an abandoned startup and enqueue its terminal error on
/// the session broadcast. Each transport consumes that broadcast through its
/// normal ordered receive path.
pub(crate) async fn reconcile_abandoned_startup(
    state: &web::Data<AppState>,
    session_id: &str,
    receiver: &broadcast::Receiver<AgentEvent>,
) -> bool {
    transition_startup_failure_if_owned(
        state,
        session_id,
        StartupFailureTarget::Abandoned(receiver),
        "Agent execution was not started; please retry this turn",
    )
    .await
}

pub(super) fn session_has_terminal_evidence(
    session: Option<&Session>,
    runner_status: Option<&AgentStatus>,
) -> bool {
    if matches!(
        runner_status,
        Some(AgentStatus::Completed | AgentStatus::Cancelled | AgentStatus::Error(_))
    ) {
        return true;
    }
    let Some(session) = session else {
        return false;
    };
    if !session.messages.is_empty() {
        return true;
    }
    session.agent_runtime_state.as_ref().is_some_and(|runtime| {
        matches!(
            runtime.status,
            AgentStatusState::Completed | AgentStatusState::Cancelled | AgentStatusState::Failed
        )
    })
}

pub(super) fn terminal_event_for_status(runner_status: Option<AgentStatus>) -> AgentEvent {
    match runner_status {
        Some(AgentStatus::Error(message)) => AgentEvent::Error { message },
        Some(AgentStatus::Cancelled) => AgentEvent::Cancelled {
            message: Some("Agent execution cancelled by user".to_string()),
        },
        _ => AgentEvent::Complete {
            // We don't persist TokenUsage today; clients can fetch history for results.
            usage: TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        },
    }
}

pub(super) fn terminal_event_for_sources(
    session: Option<&Session>,
    runner_status: Option<AgentStatus>,
) -> AgentEvent {
    // A startup rejection belongs to the newest durable handoff and must beat
    // an old runner/runtime snapshot from the preceding run.
    if let Some(message) = session.and_then(startup_failure_message) {
        return AgentEvent::Error { message };
    }
    if runner_status.is_some() {
        return terminal_event_for_status(runner_status);
    }

    match session
        .and_then(|session| session.agent_runtime_state.as_ref())
        .map(|runtime| runtime.status)
    {
        Some(AgentStatusState::Cancelled) => {
            terminal_event_for_status(Some(AgentStatus::Cancelled))
        }
        Some(AgentStatusState::Failed) => AgentEvent::Error {
            // The durable runtime state records the failure class but does not
            // currently persist the runner's error string.
            message: "Agent execution failed".to_string(),
        },
        _ => terminal_event_for_status(None),
    }
}

pub(super) fn session_prevents_terminal_event(
    session: Option<&Session>,
    runner_status: Option<&AgentStatus>,
) -> bool {
    let Some(session) = session else {
        return false;
    };

    // A newly queued run is newer than a terminal runner/runtime snapshot. Chat
    // persistence writes this marker together with the new User message, so a
    // reconnect between POST /chat and POST /execute remains live even if the
    // previous run was cancelled or failed.
    if startup_work_id(session).is_some() {
        // Admission never turns startup ownership into an unlocked synthetic
        // terminal, even after the grace expires. The live transport's
        // reconciler revalidates the exact durable work id under the canonical
        // persistence lock and broadcasts the authoritative Error.
        return true;
    }

    // Durable startup rejection is terminal even when the previous run left a
    // pending-question or Suspended runtime snapshot behind.
    if startup_failure_message(session).is_some() {
        return false;
    }

    if session.has_pending_question() {
        return true;
    }

    // A resume is pending. After the user answers a clarification /
    // `conclusion_with_options` question, `submit_pending_response` records the
    // answer as a tool-result message (so the last message is NOT `User`),
    // clears the pending question, removes `runtime.suspend_reason`, and sets the
    // resume markers. The resumed runner is reserved and marked `Running` only a
    // moment later in `resume_session_execution`. An SSE subscription that opens
    // in that gap would otherwise look "finished" here and receive a one-shot
    // terminal `Complete` — closing the stream so the resumed agent streams into
    // a broadcast with no live subscriber, leaving the UI stuck on "thinking".
    // Mirror the resume decision, which uses this same predicate, so the window
    // [markers set] and [runner Running] tile with no gap.
    if has_pending_clarification_resume(session) || has_pending_retry_resume(session) {
        return true;
    }

    if session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|runtime| matches!(runtime.status, AgentStatusState::Suspended))
    {
        return true;
    }

    let last_message_is_user = session
        .messages
        .last()
        .is_some_and(|message| matches!(message.role, Role::User));
    if !last_message_is_user {
        return false;
    }

    // Cancellation/failure can happen before the model appends an Assistant
    // message, leaving User as the last role. Those terminal sources are more
    // authoritative than the role heuristic and must be replayed to a late
    // subscriber. A genuinely newer User turn is protected by the `pending`
    // marker above.
    !terminal_failure_is_authoritative(session, runner_status)
}

fn terminal_failure_is_authoritative(
    session: &Session,
    runner_status: Option<&AgentStatus>,
) -> bool {
    match runner_status {
        Some(AgentStatus::Cancelled | AgentStatus::Error(_)) => true,
        // Any extant non-failure runner is the current source of truth; do not
        // fall back to a stale persisted failure from an older run.
        Some(_) => false,
        None => {
            startup_failure_message(session).is_some()
                || session.agent_runtime_state.as_ref().is_some_and(|runtime| {
                    matches!(
                        runtime.status,
                        AgentStatusState::Cancelled | AgentStatusState::Failed
                    )
                })
        }
    }
}

/// Whether the watched session still has any running **descendant** (transitive,
/// not just direct children). The root's SSE stream stays open until every
/// descendant in its tree finishes — required once sub-agents nest, since an
/// intermediate child can suspend (its runner gone) while a grandchild still
/// runs. For a flat tree (every child directly under the root) this is identical
/// to the old direct-child check, because a direct child's `root_session_id`
/// equals the root's.
pub(crate) async fn has_running_child(state: &web::Data<AppState>, session_id: &str) -> bool {
    // The tree root the watched session belongs to (a Root is its own root).
    let watched_root = state
        .session_store
        .get_index_entry(session_id)
        .await
        .map(|entry| entry.root_session_id)
        .unwrap_or_else(|| session_id.to_string());

    let running_session_ids: Vec<String> = {
        let runners = state.agent_runners.read().await;
        runners
            .iter()
            .filter(|(running_id, runner)| {
                matches!(runner.status, AgentStatus::Running) && running_id.as_str() != session_id
            })
            .map(|(running_id, _)| running_id.clone())
            .collect()
    };

    for running_session_id in running_session_ids {
        let Some(entry) = state
            .session_store
            .get_index_entry(&running_session_id)
            .await
        else {
            continue;
        };
        if entry.kind == SessionKind::Child && entry.root_session_id == watched_root {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;
    use bamboo_domain::AgentRuntimeState;
    use tempfile::tempdir;

    #[test]
    fn no_session_does_not_prevent_terminal() {
        assert!(!session_prevents_terminal_event(None, None));
    }

    #[test]
    fn last_user_message_prevents_terminal() {
        let mut session = Session::new("s-1", "test-model");
        session.add_message(Message::user("hello"));
        assert!(session_prevents_terminal_event(Some(&session), None));
    }

    #[test]
    fn finished_session_allows_terminal() {
        let mut session = Session::new("s-1", "test-model");
        session.add_message(Message::user("hello"));
        session.add_message(Message::assistant("done", None));
        assert!(!session_prevents_terminal_event(Some(&session), None));
    }

    /// Regression: after answering a `conclusion_with_options` question the answer
    /// is recorded as a tool result (last message is NOT `User`) and the pending
    /// question is cleared, but the resume marker is set and the resumed runner is
    /// not yet `Running`. The SSE handler must keep the stream LIVE in this window
    /// rather than emitting a one-shot terminal that strands the resumed run.
    #[test]
    fn pending_conclusion_with_options_resume_prevents_terminal() {
        let mut session = Session::new("s-1", "test-model");
        session.add_message(Message::assistant("tool question", None));
        session.add_message(Message::tool_result("ask-1", "Selected response: A"));
        // No pending question, last message is a tool result — looks "finished"...
        assert!(!session.has_pending_question());
        assert!(!session_prevents_terminal_event(Some(&session), None));

        // ...until the resume marker set by `submit_pending_response` is present.
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "true".to_string(),
        );
        assert!(session_prevents_terminal_event(Some(&session), None));
    }

    async fn state_with_session(mut session: Session) -> (tempfile::TempDir, web::Data<AppState>) {
        let dir = tempdir().expect("temporary app data");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        state.save_session(&mut session).await;
        (dir, state)
    }

    fn expire_startup_handoff(session: &mut Session) {
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            (chrono::Utc::now() - chrono::Duration::seconds(PENDING_TURN_GRACE_SECS + 1))
                .to_rfc3339(),
        );
    }

    #[actix_web::test]
    async fn terminal_helper_replays_in_memory_cancelled_after_last_user() {
        let mut session = Session::new("cancelled-user", "test-model");
        session.add_message(Message::user("cancel before first token"));
        // The in-memory terminal is authoritative even if persistence still
        // reflects the run's prior phase.
        session.set_last_run_status("running");
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(&state, "cancelled-user", Some(AgentStatus::Cancelled))
            .await
            .expect("cancelled run is terminal");
        assert!(matches!(event, AgentEvent::Cancelled { .. }));
    }

    #[actix_web::test]
    async fn terminal_helper_replays_in_memory_error_after_last_user() {
        let mut session = Session::new("failed-user", "test-model");
        session.add_message(Message::user("fail before first token"));
        session.set_last_run_status("running");
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(
            &state,
            "failed-user",
            Some(AgentStatus::Error("provider failed".to_string())),
        )
        .await
        .expect("failed run is terminal");
        assert!(matches!(event, AgentEvent::Error { message } if message == "provider failed"));
    }

    #[actix_web::test]
    async fn terminal_helper_replays_persisted_cancelled_after_last_user() {
        let mut session = Session::new("persisted-cancelled-user", "test-model");
        session.add_message(Message::user("cancel before first token"));
        session.set_last_run_status("cancelled");
        let mut runtime = AgentRuntimeState::new("cancelled-run");
        runtime.status = AgentStatusState::Cancelled;
        session.agent_runtime_state = Some(runtime);
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(&state, "persisted-cancelled-user", None)
            .await
            .expect("persisted cancellation is terminal");
        assert!(matches!(event, AgentEvent::Cancelled { .. }));
    }

    #[actix_web::test]
    async fn terminal_helper_replays_persisted_failure_after_last_user() {
        let mut session = Session::new("persisted-failed-user", "test-model");
        session.add_message(Message::user("fail before first token"));
        session.set_last_run_status("error");
        let mut runtime = AgentRuntimeState::new("failed-run");
        runtime.status = AgentStatusState::Failed;
        session.agent_runtime_state = Some(runtime);
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(&state, "persisted-failed-user", None)
            .await
            .expect("persisted failure is terminal");
        assert!(matches!(event, AgentEvent::Error { .. }));
    }

    #[actix_web::test]
    async fn terminal_helper_keeps_new_user_after_old_failure_live() {
        let mut session = Session::new("new-user-after-failure", "test-model");
        session.add_message(Message::user("new request"));
        mark_pending_turn(&mut session);
        let mut runtime = AgentRuntimeState::new("old-failed-run");
        runtime.status = AgentStatusState::Failed;
        session.agent_runtime_state = Some(runtime);
        let (_dir, state) = state_with_session(session).await;

        assert!(
            terminal_event_if_ready(&state, "new-user-after-failure", None)
                .await
                .is_none(),
            "new pending work must win over the old persisted failure"
        );
    }

    #[actix_web::test]
    async fn terminal_helper_defers_abandoned_pending_turn_to_locked_reconcile() {
        let mut session = Session::new("abandoned-turn", "test-model");
        session.add_message(Message::user("never executed"));
        mark_pending_turn(&mut session);
        expire_startup_handoff(&mut session);
        let mut runtime = AgentRuntimeState::new("old-failed-run");
        runtime.status = AgentStatusState::Failed;
        session.agent_runtime_state = Some(runtime);
        let (_dir, state) = state_with_session(session).await;

        assert!(
            terminal_event_if_ready(
                &state,
                "abandoned-turn",
                Some(AgentStatus::Error("old run failed".to_string())),
            )
            .await
            .is_none(),
            "admission must stay live until the durable CAS"
        );

        let sender = state.get_session_event_sender("abandoned-turn").await;
        let mut receiver = sender.subscribe();
        assert!(reconcile_abandoned_startup(&state, "abandoned-turn", &receiver,).await);
        assert!(matches!(
            receiver.try_recv(),
            Ok(AgentEvent::Error { message }) if message.contains("was not started")
        ));
    }

    #[actix_web::test]
    async fn abandoned_first_turn_waits_for_locked_reconcile() {
        let mut session = Session::new("abandoned-first-turn", "test-model");
        session.add_message(Message::user("never executed"));
        mark_pending_turn(&mut session);
        expire_startup_handoff(&mut session);
        let (_dir, state) = state_with_session(session).await;

        assert!(
            terminal_event_if_ready(&state, "abandoned-first-turn", None)
                .await
                .is_none(),
            "admission must not emit an unlocked synthetic terminal"
        );
        let sender = state.get_session_event_sender("abandoned-first-turn").await;
        let mut receiver = sender.subscribe();
        assert!(reconcile_abandoned_startup(&state, "abandoned-first-turn", &receiver,).await);
        assert!(matches!(
            receiver.try_recv(),
            Ok(AgentEvent::Error { message }) if message.contains("was not started")
        ));
    }

    #[actix_web::test]
    async fn app_state_scoped_guard_refcounts_slow_overlapping_startups() {
        let mut session = Session::new("slow-startup", "test-model");
        session.add_message(Message::user("slow image preparation"));
        mark_pending_turn(&mut session);
        expire_startup_handoff(&mut session);
        let (_dir, state) = state_with_session(session).await;

        let first = begin_execute_startup(state.get_ref(), "slow-startup");
        let second = begin_execute_startup(state.get_ref(), "slow-startup");
        assert!(terminal_event_if_ready(&state, "slow-startup", None)
            .await
            .is_none());
        drop(first);
        assert!(terminal_event_if_ready(&state, "slow-startup", None)
            .await
            .is_none());
        drop(second);
        assert!(terminal_event_if_ready(&state, "slow-startup", None)
            .await
            .is_none());
        let sender = state.get_session_event_sender("slow-startup").await;
        let receiver = sender.subscribe();
        assert!(reconcile_abandoned_startup(&state, "slow-startup", &receiver,).await);
    }

    #[actix_web::test]
    async fn startup_failure_is_durable_terminal_without_runtime_state() {
        let mut session = Session::new("startup-rejected", "test-model");
        session.add_message(Message::user("start me"));
        mark_pending_turn(&mut session);
        let turn_id = pending_turn_id(&session).expect("owned turn");
        assert!(mark_startup_failed_if_owned(
            &mut session,
            &turn_id,
            "provider rejected"
        ));
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(&state, "startup-rejected", None)
            .await
            .expect("startup rejection is terminal");
        assert!(matches!(
            event,
            AgentEvent::Error { message } if message.contains("provider rejected")
        ));
    }

    #[actix_web::test]
    async fn startup_failure_after_suspended_resume_is_authoritative_terminal() {
        let mut session = Session::new("suspended-resume-failed", "test-model");
        session.add_message(Message::assistant("question", None));
        session.add_message(Message::tool_result("ask-1", "answer"));
        let mut runtime = AgentRuntimeState::new("old-suspended-run");
        runtime.status = AgentStatusState::Suspended;
        session.agent_runtime_state = Some(runtime);
        session.metadata.insert(
            "clarification_resume_pending".to_string(),
            "true".to_string(),
        );
        mark_startup_handoff(&mut session);
        let work_id = startup_work_id(&session).expect("resume work id");
        assert!(mark_startup_failed_if_owned(
            &mut session,
            &work_id,
            "resume provider rejected"
        ));
        let (_dir, state) = state_with_session(session).await;

        let event = terminal_event_if_ready(&state, "suspended-resume-failed", None)
            .await
            .expect("durable startup rejection beats old Suspended state");
        assert!(matches!(
            event,
            AgentEvent::Error { message } if message.contains("resume provider rejected")
        ));
    }

    #[test]
    fn newly_marked_retry_on_old_history_gets_a_fresh_grace_window() {
        let mut session = Session::new("old-history-retry", "test-model");
        session.add_message(Message::assistant("old failure", None));
        session.messages.last_mut().unwrap().created_at =
            chrono::Utc::now() - chrono::Duration::hours(6);
        session
            .metadata
            .insert("retry_resume_pending".to_string(), "true".to_string());
        mark_startup_handoff(&mut session);

        assert!(startup_work_id(&session).is_some());
        assert!(startup_work_is_active(&session));
        assert!(session_prevents_terminal_event(Some(&session), None));
    }

    #[actix_web::test]
    async fn concurrent_live_reconcilers_broadcast_abandoned_startup_once() {
        let mut session = Session::new("multi-reconcile", "test-model");
        session.add_message(Message::user("never executed"));
        mark_pending_turn(&mut session);
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        );
        let (_dir, state) = state_with_session(session).await;
        let sender = state.get_session_event_sender("multi-reconcile").await;
        let mut first_rx = sender.subscribe();
        let mut second_rx = sender.subscribe();

        let (first, second) = tokio::join!(
            reconcile_abandoned_startup(&state, "multi-reconcile", &first_rx),
            reconcile_abandoned_startup(&state, "multi-reconcile", &second_rx),
        );
        if first == second {
            let debug = state
                .storage
                .load_session("multi-reconcile")
                .await
                .expect("debug load")
                .expect("debug session");
            panic!(
                "only one durable CAS may broadcast: first={first}, second={second}, status={:?}, work={:?}, handoff={:?}, error={:?}",
                debug.last_run_status(),
                startup_work_id(&debug),
                startup_handoff_at(&debug),
                debug.last_run_error(),
            );
        }
        assert!(matches!(first_rx.try_recv(), Ok(AgentEvent::Error { .. })));
        assert!(matches!(second_rx.try_recv(), Ok(AgentEvent::Error { .. })));
        assert!(first_rx.try_recv().is_err());
        assert!(second_rx.try_recv().is_err());

        let stored = state
            .storage
            .load_session("multi-reconcile")
            .await
            .expect("load session")
            .expect("stored session");
        assert!(startup_work_id(&stored).is_none());
        assert!(startup_failure_message(&stored).is_some());
    }

    #[actix_web::test]
    async fn newer_chat_waiting_on_session_lock_wins_before_reconcile() {
        let mut session = Session::new("chat-reconcile-race", "test-model");
        session.add_message(Message::user("abandoned first turn"));
        mark_pending_turn(&mut session);
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        );
        let mut newer = session.clone();
        newer.add_message(Message::user("newer turn"));
        mark_pending_turn(&mut newer);
        let newer_id = startup_work_id(&newer).expect("newer work id");
        let (_dir, state) = state_with_session(session).await;
        let sender = state.get_session_event_sender("chat-reconcile-race").await;
        let reconcile_receiver = sender.subscribe();
        let mut observer = sender.subscribe();

        // Queue the chat save first, then reconciliation, behind the same
        // per-session guard. FIFO lock acquisition makes the new turn durable
        // before reconciliation re-evaluates it.
        let blocker = state.persistence.acquire_lock("chat-reconcile-race").await;
        let persistence = state.persistence.clone();
        let save = tokio::spawn(async move {
            persistence
                .merge_save_runtime(&mut newer)
                .await
                .expect("save newer turn");
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let reconcile_state = state.clone();
        let reconcile = tokio::spawn(async move {
            reconcile_abandoned_startup(
                &reconcile_state,
                "chat-reconcile-race",
                &reconcile_receiver,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(blocker);

        save.await.expect("save task");
        assert!(!reconcile.await.expect("reconcile task"));
        let stored = state
            .storage
            .load_session("chat-reconcile-race")
            .await
            .expect("load session")
            .expect("stored session");
        assert_eq!(startup_work_id(&stored).as_deref(), Some(newer_id.as_str()));
        assert_eq!(stored.last_run_status().as_deref(), Some("pending"));
        assert!(observer.try_recv().is_err(), "new turn must not be closed");
    }

    #[test]
    fn startup_failure_only_rolls_back_the_owned_turn() {
        let mut owned = Session::new("owned", "test-model");
        owned.add_message(Message::user("start me"));
        mark_pending_turn(&mut owned);
        let owned_id = pending_turn_id(&owned).expect("owned turn id");
        assert!(mark_startup_failed_if_owned(
            &mut owned,
            &owned_id,
            "provider rejected"
        ));
        assert_eq!(owned.last_run_status().as_deref(), Some("error"));
        assert!(owned
            .last_run_error()
            .is_some_and(|error| error.contains("provider rejected")));

        let mut stale = Session::new("stale", "test-model");
        stale.add_message(Message::user("first"));
        mark_pending_turn(&mut stale);
        let stale_id = pending_turn_id(&stale).expect("first turn id");
        stale.add_message(Message::user("newer turn"));
        mark_pending_turn(&mut stale);
        assert!(!mark_startup_failed_if_owned(
            &mut stale,
            &stale_id,
            "late rejection"
        ));
        assert_eq!(stale.last_run_status().as_deref(), Some("pending"));

        let mut legacy = Session::new("legacy", "test-model");
        legacy.add_message(Message::user("persisted before owner marker"));
        legacy.set_last_run_status("pending");
        let legacy_id = pending_turn_id(&legacy).expect("legacy last-User ownership");
        assert!(mark_startup_failed_if_owned(
            &mut legacy,
            &legacy_id,
            "legacy startup rejected"
        ));

        let mut resume = Session::new("resume", "test-model");
        resume.add_message(Message::assistant("question", None));
        resume.add_message(Message::tool_result("ask-1", "answer"));
        resume.metadata.insert(
            "clarification_resume_pending".to_string(),
            "true".to_string(),
        );
        let resume_id = startup_work_id(&resume).expect("resume work id");
        assert!(mark_startup_failed_if_owned(
            &mut resume,
            &resume_id,
            "resume persistence failed"
        ));
        assert!(!has_pending_clarification_resume(&resume));
        assert_eq!(resume.last_run_status().as_deref(), Some("error"));
    }
}
