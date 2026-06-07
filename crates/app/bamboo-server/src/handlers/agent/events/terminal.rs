use actix_web::web;

use crate::app_state::{AgentStatus, AppState};
use bamboo_agent_core::agent::events::TokenUsage;
use bamboo_agent_core::agent::Role;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::AgentStatusState;

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

    session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|runtime| matches!(runtime.status, AgentStatusState::Suspended))
}

async fn has_running_child(state: &web::Data<AppState>, session_id: &str) -> bool {
    let running_session_ids: Vec<String> = {
        let runners = state.agent_runners.read().await;
        runners
            .iter()
            .filter_map(|(session_id, runner)| {
                if matches!(runner.status, AgentStatus::Running) {
                    Some(session_id.clone())
                } else {
                    None
                }
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
        if entry.kind == SessionKind::Child
            && entry.parent_session_id.as_deref() == Some(session_id)
        {
            return true;
        }
    }

    false
}
