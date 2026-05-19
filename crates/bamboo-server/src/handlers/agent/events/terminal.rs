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

    if session_prevents_terminal_event(session.as_ref()) {
        return None;
    }
    if has_running_child(state, session_id).await {
        return None;
    }
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
