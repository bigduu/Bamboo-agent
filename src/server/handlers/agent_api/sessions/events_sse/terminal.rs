use crate::agent::core::{AgentEvent, TokenUsage};
use crate::server::app_state::AgentStatus;

const CANCELLED_MESSAGE: &str = "Claude Code execution cancelled";

fn empty_usage() -> TokenUsage {
    TokenUsage {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
    }
}

pub(super) fn terminal_event_from_status(status: &AgentStatus) -> Option<AgentEvent> {
    match status {
        AgentStatus::Completed => Some(AgentEvent::Complete {
            usage: empty_usage(),
        }),
        AgentStatus::Cancelled => Some(AgentEvent::Error {
            message: CANCELLED_MESSAGE.to_string(),
        }),
        AgentStatus::Error(message) => Some(AgentEvent::Error {
            message: message.clone(),
        }),
        AgentStatus::Pending | AgentStatus::Running => None,
    }
}

pub(super) fn is_terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::Complete { .. } | AgentEvent::Error { .. }
    )
}

#[cfg(test)]
mod tests {
    use crate::agent::core::{AgentEvent, TokenUsage};
    use crate::server::app_state::AgentStatus;

    use super::{is_terminal_event, terminal_event_from_status, CANCELLED_MESSAGE};

    #[test]
    fn terminal_event_from_status_maps_completed_to_complete_event() {
        let event = terminal_event_from_status(&AgentStatus::Completed).expect("event");
        assert!(matches!(
            event,
            AgentEvent::Complete {
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0
                }
            }
        ));
    }

    #[test]
    fn terminal_event_from_status_maps_error_to_error_event() {
        let event = terminal_event_from_status(&AgentStatus::Error("boom".to_string()))
            .expect("error event");
        assert!(matches!(event, AgentEvent::Error { message } if message == "boom"));
    }

    #[test]
    fn terminal_event_from_status_maps_cancelled_to_error_event() {
        let event = terminal_event_from_status(&AgentStatus::Cancelled).expect("cancelled event");
        assert!(matches!(event, AgentEvent::Error { message } if message == CANCELLED_MESSAGE));
    }

    #[test]
    fn terminal_event_from_status_ignores_active_statuses() {
        assert!(terminal_event_from_status(&AgentStatus::Pending).is_none());
        assert!(terminal_event_from_status(&AgentStatus::Running).is_none());
    }

    #[test]
    fn is_terminal_event_detects_complete_and_error() {
        let complete = AgentEvent::Complete {
            usage: TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        };
        let error = AgentEvent::Error {
            message: "x".to_string(),
        };
        let token = AgentEvent::Token {
            content: "hello".to_string(),
        };

        assert!(is_terminal_event(&complete));
        assert!(is_terminal_event(&error));
        assert!(!is_terminal_event(&token));
    }
}
