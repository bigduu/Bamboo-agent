//! Pending user-work / resume-marker helpers.

use bamboo_agent_core::Role;
use bamboo_domain::Session;

pub(crate) const CLARIFICATION_RESUME_PENDING_KEY: &str = "clarification_resume_pending";
pub(crate) const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str =
    "conclusion_with_options_resume_pending";

/// Whether the session has resumable user work (pending tool response, retry, or last message is from user).
pub fn has_pending_user_message(session: &Session) -> bool {
    if has_pending_clarification_resume(session) || has_pending_retry_resume(session) {
        return true;
    }
    session
        .messages
        .last()
        .map(|message| matches!(message.role, Role::User))
        .unwrap_or(false)
}

pub fn consume_pending_clarification_resume(session: &mut Session) {
    session.metadata.remove(CLARIFICATION_RESUME_PENDING_KEY);
    session
        .metadata
        .remove(CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY);
    session.metadata.remove("retry_resume_pending");
    session.metadata.remove("retry_resume_reason");
}

pub fn has_pending_clarification_resume(session: &Session) -> bool {
    session
        .metadata
        .get(CLARIFICATION_RESUME_PENDING_KEY)
        .or_else(|| {
            session
                .metadata
                .get(CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY)
        })
        .is_some_and(|value| value == "true")
}

pub fn has_pending_conclusion_with_options_resume(session: &Session) -> bool {
    has_pending_clarification_resume(session)
}

pub fn has_pending_retry_resume(session: &Session) -> bool {
    session
        .metadata
        .get("retry_resume_pending")
        .is_some_and(|value| value == "true")
}

pub fn consume_pending_conclusion_with_options_resume(session: &mut Session) {
    consume_pending_clarification_resume(session)
}
