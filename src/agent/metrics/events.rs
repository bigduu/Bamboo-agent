use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{ForwardStatus, RoundStatus, SessionStatus, TokenUsage};

/// Metadata attached to every metrics event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    /// Unique event ID (UUID v4)
    pub event_id: String,
    /// When the event occurred
    pub occurred_at: DateTime<Utc>,
    /// Optional trace ID for correlating request chains
    pub trace_id: Option<String>,
}

impl EventMeta {
    pub fn new() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            occurred_at: Utc::now(),
            trace_id: None,
        }
    }

    pub fn with_trace_id(trace_id: impl Into<String>) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            occurred_at: Utc::now(),
            trace_id: Some(trace_id.into()),
        }
    }
}

impl Default for EventMeta {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified metrics event enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricsEvent {
    Chat(ChatEvent),
    Forward(ForwardEvent),
    System(SystemEvent),
}

// ============================================================================
// Chat Events (Agent-internal usage)
// ============================================================================

/// Events emitted by the agent loop during chat sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatEvent {
    SessionStarted {
        meta: EventMeta,
        session_id: String,
        model: String,
    },
    SessionCompleted {
        meta: EventMeta,
        session_id: String,
        status: SessionStatus,
    },
    RoundStarted {
        meta: EventMeta,
        round_id: String,
        session_id: String,
        model: String,
    },
    RoundCompleted {
        meta: EventMeta,
        round_id: String,
        session_id: String,
        status: RoundStatus,
        usage: TokenUsage,
        latency_ms: u64,
        error: Option<String>,
    },
    ToolCalled {
        meta: EventMeta,
        tool_call_id: String,
        round_id: String,
        session_id: String,
        tool_name: String,
        latency_ms: u64,
        success: bool,
    },
    MessageCountUpdated {
        meta: EventMeta,
        session_id: String,
        message_count: u32,
    },
}

// ============================================================================
// Forward Events (HTTP proxy)
// ============================================================================

/// Events emitted when forwarding requests to upstream APIs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForwardEvent {
    RequestStarted {
        meta: EventMeta,
        request_id: String,
        /// Endpoint identifier (e.g., "openai.chat_completions" or "anthropic.messages")
        endpoint: String,
        model: String,
        is_stream: bool,
    },
    RequestCompleted {
        meta: EventMeta,
        request_id: String,
        status_code: u16,
        status: ForwardStatus,
        usage: Option<TokenUsage>,
        latency_ms: u64,
        error: Option<String>,
    },
}

// ============================================================================
// System Events
// ============================================================================

/// System-level events for operational metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemEvent {
    MetricsDropped {
        count: u64,
        reason: String,
    },
    StorageError {
        error: String,
        event_type: String,
    },
    WorkerStarted,
    WorkerStopped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_meta_new() {
        let meta = EventMeta::new();
        assert!(!meta.event_id.is_empty());
        assert!(meta.trace_id.is_none());
    }

    #[test]
    fn test_event_meta_with_trace_id() {
        let meta = EventMeta::with_trace_id("trace-123");
        assert!(!meta.event_id.is_empty());
        assert_eq!(meta.trace_id, Some("trace-123".to_string()));
    }

    #[test]
    fn test_chat_event_serialization() {
        let event = MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: EventMeta::new(),
            session_id: "session-123".to_string(),
            model: "gpt-4".to_string(),
        });

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: MetricsEvent = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            MetricsEvent::Chat(ChatEvent::SessionStarted { session_id, model, .. }) => {
                assert_eq!(session_id, "session-123");
                assert_eq!(model, "gpt-4");
            }
            _ => panic!("Expected SessionStarted event"),
        }
    }

    #[test]
    fn test_forward_event_serialization() {
        let event = MetricsEvent::Forward(ForwardEvent::RequestStarted {
            meta: EventMeta::new(),
            request_id: "req-456".to_string(),
            endpoint: "openai.chat_completions".to_string(),
            model: "gpt-5-mini".to_string(),
            is_stream: true,
        });

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: MetricsEvent = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            MetricsEvent::Forward(ForwardEvent::RequestStarted {
                request_id,
                endpoint,
                model,
                is_stream,
                ..
            }) => {
                assert_eq!(request_id, "req-456");
                assert_eq!(endpoint, "openai.chat_completions");
                assert_eq!(model, "gpt-5-mini");
                assert!(is_stream);
            }
            _ => panic!("Expected RequestStarted event"),
        }
    }
}
