use actix_web::web;

use crate::agent::core::agent::events::TokenUsage;
use crate::agent::core::agent::Role;
use crate::agent::core::{AgentEvent, SessionKind};
use crate::server::app_state::{AgentStatus, AppState};

pub(super) async fn terminal_event_if_ready(
    state: &web::Data<AppState>,
    session_id: &str,
    runner_status: Option<AgentStatus>,
) -> Option<AgentEvent> {
    if last_message_is_user(state, session_id).await {
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
        Some(AgentStatus::Cancelled) => AgentEvent::Error {
            message: "Agent execution cancelled by user".to_string(),
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

async fn last_message_is_user(state: &web::Data<AppState>, session_id: &str) -> bool {
    match state.storage.load_session(session_id).await {
        Ok(Some(session)) => session
            .messages
            .last()
            .map(|message| matches!(message.role, Role::User))
            .unwrap_or(false),
        _ => false,
    }
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
