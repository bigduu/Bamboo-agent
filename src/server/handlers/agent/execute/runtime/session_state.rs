use crate::agent::core::{agent::Role, Session};

const ASK_USER_RESUME_PENDING_KEY: &str = "ask_user_resume_pending";
const RETRY_RESUME_PENDING_KEY: &str = "retry_resume_pending";
const RETRY_RESUME_REASON_KEY: &str = "retry_resume_reason";

pub(in crate::server::handlers::agent::execute) fn has_pending_user_message(
    session: &Session,
) -> bool {
    if has_pending_ask_user_resume(session) || has_pending_retry_resume(session) {
        return true;
    }

    session
        .messages
        .last()
        .map(|message| matches!(message.role, Role::User))
        .unwrap_or(false)
}

pub(in crate::server::handlers::agent::execute) fn consume_pending_ask_user_resume(
    session: &mut Session,
) {
    session.metadata.remove(ASK_USER_RESUME_PENDING_KEY);
    session.metadata.remove(RETRY_RESUME_PENDING_KEY);
    session.metadata.remove(RETRY_RESUME_REASON_KEY);
}

fn has_pending_ask_user_resume(session: &Session) -> bool {
    session
        .metadata
        .get(ASK_USER_RESUME_PENDING_KEY)
        .is_some_and(|value| value == "true")
}

fn has_pending_retry_resume(session: &Session) -> bool {
    session
        .metadata
        .get(RETRY_RESUME_PENDING_KEY)
        .is_some_and(|value| value == "true")
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

pub(super) fn selected_skill_ids_for_session(session: &Session) -> Option<Vec<String>> {
    session
        .metadata
        .get("selected_skill_ids")
        .and_then(|raw| crate::agent::skill::selection::parse_selected_skill_ids_metadata(raw))
}

pub(super) fn selected_skill_mode_for_session(session: &Session) -> Option<String> {
    let value = session
        .metadata
        .get("skill_mode")
        .or_else(|| session.metadata.get("mode"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
