use crate::agent::core::agent::types::PendingQuestion;
use crate::agent::core::{Message, Session};

use super::submit::{update_or_append_tool_result_message, validate_pending_response};

fn pending_question(allow_custom: bool) -> PendingQuestion {
    PendingQuestion {
        tool_call_id: "tool-1".to_string(),
        question: "Pick one".to_string(),
        options: vec!["A".to_string(), "B".to_string()],
        allow_custom,
    }
}

#[test]
fn validate_pending_response_accepts_custom_input_when_allowed() {
    let pending = pending_question(true);
    let result = validate_pending_response(&pending, "anything");
    assert!(result.is_ok());
}

#[test]
fn validate_pending_response_rejects_option_not_in_list_when_custom_is_disabled() {
    let pending = pending_question(false);
    let error = validate_pending_response(&pending, "C").expect_err("response should be rejected");
    assert_eq!(error, "Response must be one of: A, B");
}

#[test]
fn update_or_append_tool_result_message_updates_existing_message() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(Message::tool_result("tool-1", "placeholder"));

    let updated = update_or_append_tool_result_message(&mut session, "tool-1", "A");

    assert!(updated);
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].content, "User selected: A");
}

#[test]
fn update_or_append_tool_result_message_appends_when_placeholder_is_missing() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(Message::assistant("no tool result", None));

    let updated = update_or_append_tool_result_message(&mut session, "tool-1", "B");

    assert!(!updated);
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[1].tool_call_id.as_deref(), Some("tool-1"));
    assert_eq!(session.messages[1].content, "User selected: B");
}
