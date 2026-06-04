//! Single authoritative pre-execution session mutation point.
//!
//! Historically three execution entry points each duplicated — with subtly
//! different logic — the work of (a) placing the authoritative leading System
//! message and (b) setting `session.model` before handing the session to the
//! agent loop. This module consolidates that into [`prepare_session_for_execution`]
//! so there is exactly one place that defines the pre-execution mutation.
//!
//! The three callers are:
//! - the SDK facade (`bamboo_sdk::agent::Agent::execute_internal`), which owns a
//!   configured instruction and model and passes both;
//! - the server spawn path (`runtime::execution::agent_spawn::spawn_session_execution`),
//!   whose caller has already placed the system prompt, so it passes `None` for
//!   `system_prompt` and only the resolved model;
//! - the child spawn path (`sdk::spawn::run_child_spawn`), likewise `None` for the
//!   system prompt and only the child model.

use bamboo_agent_core::{Message, Role, Session};

/// Apply the authoritative pre-execution mutations to `session`.
///
/// This encodes the single, authoritative behavior that every execution entry
/// point must share:
///
/// - If `system_prompt` is `Some`, it is applied as the session's **leading**
///   System message. The supplied prompt is authoritative: if the first message
///   is already a [`Role::System`] message it is *replaced* (never duplicated),
///   otherwise a System message is *inserted* at index 0. This guarantees a
///   caller-supplied session can't silently shadow the configured instruction.
/// - If `model` is `Some`, `session.model` is set to it.
///
/// Call sites that don't supply one of these inputs (e.g. the spawn paths, whose
/// caller already placed the system prompt) pass `None` for that parameter, so
/// behavior is identical to the previous inline logic.
pub fn prepare_session_for_execution(
    session: &mut Session,
    system_prompt: Option<&str>,
    model: Option<&str>,
) {
    if let Some(prompt) = system_prompt {
        match session.messages.first() {
            Some(first) if matches!(first.role, Role::System) => {
                session.messages[0] = Message::system(prompt.to_string());
            }
            _ => session
                .messages
                .insert(0, Message::system(prompt.to_string())),
        }
    }

    if let Some(model) = model {
        session.model = model.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;

    fn session_with(messages: Vec<Message>) -> Session {
        let mut s = Session::new("test-session", "old-model");
        s.messages = messages;
        s
    }

    #[test]
    fn empty_session_with_prompt_inserts_at_index_zero() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, Some("you are helpful"), None);

        assert_eq!(session.messages.len(), 2);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "you are helpful");
        assert!(matches!(session.messages[1].role, Role::User));
    }

    #[test]
    fn leading_system_is_replaced_not_duplicated() {
        let mut session = session_with(vec![
            Message::system("stale prompt"),
            Message::user("hello"),
        ]);

        prepare_session_for_execution(&mut session, Some("authoritative prompt"), None);

        // Replaced in place, no duplicate System message.
        assert_eq!(session.messages.len(), 2);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "authoritative prompt");
        assert!(matches!(session.messages[1].role, Role::User));
    }

    #[test]
    fn leading_non_system_gets_prompt_inserted_at_zero() {
        let mut session =
            session_with(vec![Message::user("hello"), Message::assistant("hi", None)]);

        prepare_session_for_execution(&mut session, Some("you are helpful"), None);

        assert_eq!(session.messages.len(), 3);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "you are helpful");
        assert!(matches!(session.messages[1].role, Role::User));
        assert!(matches!(session.messages[2].role, Role::Assistant));
    }

    #[test]
    fn model_is_set_when_some() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, None, Some("new-model"));

        assert_eq!(session.model, "new-model");
        // No system prompt supplied → message list untouched.
        assert_eq!(session.messages.len(), 1);
        assert!(matches!(session.messages[0].role, Role::User));
    }

    #[test]
    fn none_inputs_leave_session_untouched() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, None, None);

        assert_eq!(session.model, "old-model");
        assert_eq!(session.messages.len(), 1);
    }
}
