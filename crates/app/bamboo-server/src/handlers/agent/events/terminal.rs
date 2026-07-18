use actix_web::web;

use crate::app_state::{AgentStatus, AppState};
use bamboo_agent_core::agent::events::TokenUsage;
use bamboo_agent_core::agent::Role;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::AgentStatusState;
use bamboo_engine::session_app::execute::{
    has_pending_clarification_resume, has_pending_retry_resume,
};

pub(crate) async fn terminal_event_if_ready(
    state: &web::Data<AppState>,
    session_id: &str,
    runner_status: Option<AgentStatus>,
) -> Option<AgentEvent> {
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
    if matches!(session.last_run_status().as_deref(), Some("pending")) {
        return true;
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
        None => session.agent_runtime_state.as_ref().is_some_and(|runtime| {
            matches!(
                runtime.status,
                AgentStatusState::Cancelled | AgentStatusState::Failed
            )
        }),
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
        session.set_last_run_status("pending");
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
}
