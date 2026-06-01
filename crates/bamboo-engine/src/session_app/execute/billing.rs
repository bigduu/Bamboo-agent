//! Billing helpers.
//!
//! Some upstream products bill on "user-initiated message turns" rather than on
//! LLM request count. With the runtime-level suspend/resume model used by the
//! SubAgent tool, the engine can append `Role::User` messages that are NOT
//! caused by the human user (e.g. child-completion resume, retry resume,
//! conclusion_with_options resume). These helpers let a billing layer count
//! only genuine user turns and skip system-injected resume messages.

use bamboo_agent_core::{Message, Role};
use bamboo_domain::Session;

/// Returns true if the message was synthesized by the runtime to resume a
/// suspended root session (child completion, retry, conclusion-with-options,
/// gold auto-continue).
///
/// Such messages have one or both of the following stable markers:
/// - `metadata.hidden_from_ui == true`
/// - `metadata.runtime_kind` set to a known resume kind
pub fn is_system_resume_message(message: &Message) -> bool {
    if !matches!(message.role, Role::User) {
        return false;
    }
    let Some(metadata) = message.metadata.as_ref() else {
        return false;
    };

    if metadata
        .get("hidden_from_ui")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return true;
    }

    matches!(
        metadata
            .get("runtime_kind")
            .and_then(|value| value.as_str()),
        Some("child_completion_resume")
            | Some("retry_resume")
            | Some("conclusion_with_options_resume")
            | Some("clarification_resume")
            | Some("gold_continue_resume")
            | Some("gold_goal_resume")
    )
}

/// Returns true when the message represents a billable end-user turn.
///
/// Use this in any per-message billing accounting (e.g. "1 user message =
/// 1 quota unit"). LLM request / token billing should still be done at the
/// provider call layer; this helper is only relevant when the product itself
/// counts user-initiated messages.
pub fn is_billable_user_turn(message: &Message) -> bool {
    matches!(message.role, Role::User) && !is_system_resume_message(message)
}

/// Count the number of billable user turns in a session — i.e. user messages
/// that were actually initiated by the human, excluding runtime-injected
/// resume messages from child completion, retry, or conclusion-with-options.
pub fn billable_user_turn_count(session: &Session) -> usize {
    session
        .messages
        .iter()
        .filter(|message| is_billable_user_turn(message))
        .count()
}
