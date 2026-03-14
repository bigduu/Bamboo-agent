use crate::agent::core::{agent::Role, Session};

pub(in crate::server::handlers::agent::execute) fn has_pending_user_message(
    session: &Session,
) -> bool {
    session
        .messages
        .last()
        .map(|message| matches!(message.role, Role::User))
        .unwrap_or(false)
}

pub(super) fn system_prompt_for_session(session: &Session) -> Option<String> {
    session
        .messages
        .iter()
        .find(|message| matches!(message.role, Role::System))
        .map(|message| message.content.clone())
}

pub(super) fn initial_user_message_for_session(session: &Session) -> String {
    session
        .messages
        .last()
        .filter(|message| matches!(message.role, Role::User))
        .map(|message| message.content.clone())
        .unwrap_or_default()
}
