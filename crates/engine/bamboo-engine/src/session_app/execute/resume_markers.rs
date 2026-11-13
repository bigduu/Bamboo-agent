//! Pending user-work / resume-marker helpers.

use bamboo_agent_core::Role;
use bamboo_domain::Session;
use chrono::{DateTime, SecondsFormat, Utc};

pub(crate) const CLARIFICATION_RESUME_PENDING_KEY: &str = "clarification_resume_pending";
pub(crate) const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str =
    "conclusion_with_options_resume_pending";
const STARTUP_HANDOFF_AT_KEY: &str = "execute.startup_handoff_at";

/// Persist the start of a chat-to-execute or resume-to-run handoff.
///
/// Resume operations can reuse an old transcript message, so message creation
/// time is not a safe grace-period origin. This marker records when the user
/// actually requested the new handoff.
pub fn mark_startup_handoff(session: &mut Session) {
    session.metadata.insert(
        STARTUP_HANDOFF_AT_KEY.to_string(),
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    );
}

pub fn clear_startup_handoff(session: &mut Session) {
    session.metadata.remove(STARTUP_HANDOFF_AT_KEY);
}

pub fn startup_handoff_at(session: &Session) -> Option<DateTime<Utc>> {
    session
        .metadata
        .get(STARTUP_HANDOFF_AT_KEY)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

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
    clear_startup_handoff(session);
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
