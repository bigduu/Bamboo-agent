use super::terminal::{session_prevents_terminal_event, terminal_event_for_status};
use crate::app_state::AgentStatus;
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};

#[test]
fn terminal_event_for_cancelled_maps_to_cancelled_event() {
    let event = terminal_event_for_status(Some(AgentStatus::Cancelled));
    match event {
        AgentEvent::Cancelled { message } => {
            assert_eq!(
                message.as_deref(),
                Some("Agent execution cancelled by user")
            );
        }
        other => panic!("expected cancelled event, got {other:?}"),
    }
}

#[test]
fn terminal_event_for_error_status_preserves_message() {
    let event = terminal_event_for_status(Some(AgentStatus::Error("boom".to_string())));
    match event {
        AgentEvent::Error { message } => assert_eq!(message, "boom"),
        other => panic!("expected error event, got {other:?}"),
    }
}

#[test]
fn terminal_event_for_non_error_status_defaults_to_complete() {
    let event = terminal_event_for_status(None);
    match event {
        AgentEvent::Complete { usage } => {
            assert_eq!(usage.prompt_tokens, 0);
            assert_eq!(usage.completion_tokens, 0);
            assert_eq!(usage.total_tokens, 0);
        }
        other => panic!("expected complete event, got {other:?}"),
    }
}

#[test]
fn session_prevents_terminal_when_last_message_is_user() {
    let mut session = Session::new("sess-1", "test-model");
    session.add_message(Message::user("Hi"));

    assert!(session_prevents_terminal_event(Some(&session)));
}

#[test]
fn session_prevents_terminal_when_pending_question_exists() {
    let mut session = Session::new("sess-2", "test-model");
    session.set_pending_question(
        "call-1".to_string(),
        "QuestionTool".to_string(),
        "Need more info?".to_string(),
        vec!["Yes".to_string(), "No".to_string()],
        true,
    );

    assert!(session_prevents_terminal_event(Some(&session)));
}

#[test]
fn session_prevents_terminal_when_runtime_is_suspended() {
    let mut session = Session::new("sess-3", "test-model");
    let mut runtime = AgentRuntimeState::new("run-1");
    runtime.status = AgentStatusState::Suspended;
    session.agent_runtime_state = Some(runtime);

    assert!(session_prevents_terminal_event(Some(&session)));
}

#[test]
fn session_allows_terminal_when_not_waiting_for_user() {
    let session = Session::new("sess-4", "test-model");

    assert!(!session_prevents_terminal_event(Some(&session)));
    assert!(!session_prevents_terminal_event(None));
}
