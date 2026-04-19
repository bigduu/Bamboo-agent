use super::terminal::terminal_event_for_status;
use crate::app_state::AgentStatus;
use bamboo_agent_core::AgentEvent;

#[test]
fn terminal_event_for_cancelled_maps_to_error_event() {
    let event = terminal_event_for_status(Some(AgentStatus::Cancelled));
    match event {
        AgentEvent::Error { message } => {
            assert_eq!(message, "Agent execution cancelled by user");
        }
        other => panic!("expected error event, got {other:?}"),
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
