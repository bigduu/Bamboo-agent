use actix_web::web;

use crate::app_state::{AgentStatus, AppState};
use bamboo_agent_core::agent::events::TokenUsage;
use bamboo_agent_core::agent::Role;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::AgentStatusState;
use bamboo_engine::session_app::execute::has_pending_user_message;

pub(super) async fn terminal_event_if_ready(
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

    if session_prevents_terminal_event(session.as_ref()) {
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
    Some(terminal_event_for_status(runner_status))
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

pub(super) fn session_prevents_terminal_event(session: Option<&Session>) -> bool {
    let Some(session) = session else {
        return false;
    };

    if session
        .messages
        .last()
        .is_some_and(|message| matches!(message.role, Role::User))
    {
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
    if has_pending_user_message(session) {
        return true;
    }

    session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|runtime| matches!(runtime.status, AgentStatusState::Suspended))
}

/// Whether the watched session still has any running **descendant** (transitive,
/// not just direct children). The root's SSE stream stays open until every
/// descendant in its tree finishes — required once sub-agents nest, since an
/// intermediate child can suspend (its runner gone) while a grandchild still
/// runs. For a flat tree (every child directly under the root) this is identical
/// to the old direct-child check, because a direct child's `root_session_id`
/// equals the root's.
pub(super) async fn has_running_child(state: &web::Data<AppState>, session_id: &str) -> bool {
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
            .filter_map(|(running_id, runner)| {
                (matches!(runner.status, AgentStatus::Running) && running_id.as_str() != session_id)
                    .then(|| running_id.clone())
            })
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

    #[test]
    fn no_session_does_not_prevent_terminal() {
        assert!(!session_prevents_terminal_event(None));
    }

    #[test]
    fn last_user_message_prevents_terminal() {
        let mut session = Session::new("s-1", "test-model");
        session.add_message(Message::user("hello"));
        assert!(session_prevents_terminal_event(Some(&session)));
    }

    #[test]
    fn finished_session_allows_terminal() {
        let mut session = Session::new("s-1", "test-model");
        session.add_message(Message::user("hello"));
        session.add_message(Message::assistant("done", None));
        assert!(!session_prevents_terminal_event(Some(&session)));
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
        assert!(!session_prevents_terminal_event(Some(&session)));

        // ...until the resume marker set by `submit_pending_response` is present.
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "true".to_string(),
        );
        assert!(session_prevents_terminal_event(Some(&session)));
    }
}
