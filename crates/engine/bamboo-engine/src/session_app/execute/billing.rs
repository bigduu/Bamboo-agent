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

pub use bamboo_domain::is_system_resume_message;

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
