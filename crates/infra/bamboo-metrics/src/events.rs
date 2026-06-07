//! Metrics Event System
//!
//! This module defines the event types used for tracking agent performance and behavior.
//! The event system follows a unified architecture that supports both internal agent operations
//! and HTTP proxy forwarding.
//!
//! # Event Categories
//!
//! The system organizes metrics into three distinct categories:
//!
//! ## Chat Events (`ChatEvent`)
//! Events emitted by the agent's internal chat loop during conversation sessions.
//! These track the lifecycle of sessions, rounds, and tool invocations.
//!
//! ## Forward Events (`ForwardEvent`)
//! Events emitted when forwarding requests to upstream API providers (e.g., OpenAI, Anthropic).
//! These track HTTP proxy operations for external API calls.
//!
//! ## System Events (`SystemEvent`)
//! Operational events for monitoring the metrics system itself, including error tracking
//! and worker lifecycle management.
//!
//! # Architecture
//!
//! All events share common metadata (`EventMeta`) that includes:
//! - Unique event identifier (UUID v4)
//! - Precise timestamp of occurrence
//! - Optional trace ID for distributed request tracing
//!
//! Events are designed to be:
//! - **Serializable**: All events implement `Serialize` and `Deserialize` for persistence
//! - **Immutable**: Once created, events should not be modified
//! - **Traceable**: Events can be linked via trace IDs across system boundaries

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{ForwardStatus, RoundStatus, SessionStatus, TokenUsage};

/// Metadata attached to every metrics event.
///
/// This structure provides common identification and timing information
/// that is shared across all event types in the metrics system.
/// Metadata attached to every metrics event.
///
/// This structure provides common identification and timing information
/// that is shared across all event types in the metrics system.
///
/// # Fields
///
/// - `event_id`: Unique identifier for this event (UUID v4 format)
/// - `occurred_at`: UTC timestamp when the event occurred
/// - `trace_id`: Optional identifier for correlating related events across services
///
/// # Example
///
/// ```rust,ignore
///
/// // Create event metadata without trace ID
/// let meta = EventMeta::new();
/// assert!(!meta.event_id.is_empty());
/// assert!(meta.trace_id.is_none());
///
/// // Create event metadata with trace ID for distributed tracing
/// let traced_meta = EventMeta::with_trace_id("req-abc-123");
/// assert_eq!(traced_meta.trace_id, Some("req-abc-123".to_string()));
/// ```
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
    /// Creates new event metadata with a fresh UUID and current timestamp.
    ///
    /// The trace ID is set to `None`. Use this constructor when you don't need
    /// to correlate this event with other events across service boundaries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::events::EventMeta;
    ///
    /// let meta = EventMeta::new();
    /// assert!(!meta.event_id.is_empty());
    /// assert!(meta.trace_id.is_none());
    /// ```
    pub fn new() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            occurred_at: Utc::now(),
            trace_id: None,
        }
    }

    /// Creates new event metadata with a trace ID for distributed request tracing.
    ///
    /// Use this constructor when you need to correlate this event with other
    /// events that are part of the same logical request or operation flow.
    ///
    /// # Arguments
    ///
    /// * `trace_id` - A unique identifier for the request chain (will be converted to String)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::events::EventMeta;
    ///
    /// let meta = EventMeta::with_trace_id("req-12345");
    /// assert_eq!(meta.trace_id, Some("req-12345".to_string()));
    /// ```
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

/// Unified metrics event enum encompassing all event categories.
///
/// This enum serves as the top-level container for all metrics events
/// in the system. It provides a single type that can be serialized,
/// deserialized, and processed uniformly.
///
/// # Variants
///
/// - `Chat`: Events from the internal agent chat loop
/// - `Forward`: Events from HTTP proxy operations to upstream APIs
/// - `System`: Operational events for system monitoring
///
/// # Serialization
///
/// All variants are JSON-serializable via serde, making them suitable
/// for storage, transmission over networks, and logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricsEvent {
    Chat(ChatEvent),
    Forward(ForwardEvent),
    System(SystemEvent),
}

// ============================================================================
// Chat Events (Agent-internal usage)
// ============================================================================

/// Events emitted by the agent loop during chat sessions.
///
/// These events track the complete lifecycle of chat interactions within
/// the agent, from session initialization to completion, including all
/// intermediate operations like rounds and tool calls.
///
/// # Event Lifecycle
///
/// A typical chat session produces events in this order:
/// 1. `SessionStarted` - When a new conversation begins
/// 2. `RoundStarted` - When the agent begins processing a message
/// 3. `ToolCalled` (multiple) - For each tool invocation during the round
/// 4. `RoundCompleted` - When the agent finishes processing the message
/// 5. `MessageCountUpdated` - When the message count changes
/// 6. `SessionCompleted` - When the conversation ends
///
/// # Usage
///
/// These events are primarily used for:
/// - Performance monitoring (latency tracking)
/// - Resource usage analysis (token consumption)
/// - Behavior analytics (tool usage patterns)
/// - Error tracking and debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatEvent {
    /// Emitted when a new chat session is initialized.
    ///
    /// This marks the beginning of a conversation between the user and agent.
    SessionStarted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for this chat session
        session_id: String,
        /// The AI model being used for this session (e.g., "gpt-4", "claude-3")
        model: String,
    },

    /// Emitted when a chat session completes or terminates.
    ///
    /// This marks the end of a conversation, whether successful or due to an error.
    SessionCompleted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for the chat session that completed
        session_id: String,
        /// Final status of the session (completed, failed, or cancelled)
        status: SessionStatus,
    },

    /// Emitted when the agent starts processing a new message round.
    ///
    /// A round represents a single request-response cycle within a session.
    /// Each user message typically triggers one round.
    RoundStarted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for this round
        round_id: String,
        /// Session this round belongs to
        session_id: String,
        /// The AI model being used for this round
        model: String,
    },

    /// Emitted when the agent completes processing a message round.
    ///
    /// Contains comprehensive metrics about the round including token usage,
    /// latency, and any errors that occurred.
    RoundCompleted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for the round that completed
        round_id: String,
        /// Session this round belongs to
        session_id: String,
        /// Final status of the round (success or failed)
        status: RoundStatus,
        /// Token consumption during this round
        usage: TokenUsage,
        /// Total time to process the round in milliseconds
        latency_ms: u64,
        /// Error message if the round failed, None on success
        error: Option<String>,
    },

    /// Emitted when the agent invokes a tool during a round.
    ///
    /// Tracks tool execution for understanding agent behavior and
    /// measuring tool performance.
    ToolCalled {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for this tool invocation
        tool_call_id: String,
        /// Round this tool call belongs to
        round_id: String,
        /// Session this tool call belongs to
        session_id: String,
        /// Name of the tool being invoked (e.g., "read_file", "execute_command")
        tool_name: String,
        /// Time taken to execute the tool in milliseconds
        latency_ms: u64,
        /// Whether the tool execution succeeded
        success: bool,
    },

    /// Emitted when the message count for a session is updated.
    ///
    /// This tracks the total number of messages exchanged in the session,
    /// including both user and assistant messages.
    MessageCountUpdated {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Session whose message count was updated
        session_id: String,
        /// New total message count for the session
        message_count: u32,
    },
}

// ============================================================================
// Forward Events (HTTP proxy)
// ============================================================================

/// Events emitted when forwarding requests to upstream APIs.
///
/// These events track HTTP proxy operations when the system forwards
/// requests to external API providers like OpenAI, Anthropic, or other
/// LLM services. They enable monitoring of API usage, costs, and performance.
///
/// # Event Lifecycle
///
/// A typical forward operation produces events in this order:
/// 1. `RequestStarted` - When a request is initiated to the upstream API
/// 2. `RequestCompleted` - When the response is received (or an error occurs)
///
/// # Use Cases
///
/// These events support:
/// - API cost tracking via token consumption
/// - Performance monitoring (latency, success rates)
/// - Error rate analysis per endpoint
/// - Load balancing and capacity planning
/// - Compliance and audit logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForwardEvent {
    /// Emitted when a request is initiated to an upstream API.
    ///
    /// This marks the beginning of a forwarded HTTP request to an
    /// external service provider.
    RequestStarted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for this forwarded request
        request_id: String,
        /// Endpoint identifier (e.g., "openai.chat_completions" or "anthropic.messages")
        endpoint: String,
        /// The AI model being requested (e.g., "gpt-4", "claude-3-opus")
        model: String,
        /// Whether this is a streaming request (SSE) or regular request
        is_stream: bool,
    },

    /// Emitted when a forwarded request completes or fails.
    ///
    /// Contains comprehensive information about the request outcome,
    /// including HTTP status, token usage, latency, and any errors.
    RequestCompleted {
        /// Event metadata including unique ID and timestamp
        meta: EventMeta,
        /// Unique identifier for the request that completed
        request_id: String,
        /// HTTP status code returned by the upstream API
        status_code: u16,
        /// Status classification (success, error, or timeout)
        status: ForwardStatus,
        /// Token usage if available from the API response
        usage: Option<TokenUsage>,
        /// Total time for the request in milliseconds
        latency_ms: u64,
        /// Error message if the request failed, None on success
        error: Option<String>,
    },
}

// ============================================================================
// System Events
// ============================================================================

/// System-level events for operational metrics and monitoring.
///
/// These events track the health and operation of the metrics system itself,
/// providing visibility into system behavior, errors, and resource management.
/// They are primarily used for operational monitoring and debugging.
///
/// # Use Cases
///
/// - Monitoring system health and stability
/// - Tracking metrics collection reliability
/// - Identifying storage or processing issues
/// - Understanding worker lifecycle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemEvent {
    /// Emitted when metrics events are dropped due to errors or resource constraints.
    ///
    /// This typically indicates system stress or configuration issues
    /// that should be investigated.
    ///
    /// # Fields
    ///
    /// - `count`: Number of events that were dropped
    /// - `reason`: Human-readable explanation of why events were dropped
    MetricsDropped {
        /// Number of events that were dropped
        count: u64,
        /// Explanation of why the events were dropped
        reason: String,
    },

    /// Emitted when a storage operation fails.
    ///
    /// Tracks errors in persisting metrics to the storage backend,
    /// helping identify database issues or data quality problems.
    ///
    /// # Fields
    ///
    /// - `error`: Description of the storage error
    /// - `event_type`: Type of event that failed to be stored
    StorageError {
        /// Description of the storage error that occurred
        error: String,
        /// Type of metrics event that failed to be stored
        event_type: String,
    },

    /// Emitted when a metrics processing worker starts.
    ///
    /// Indicates that a new worker thread/task has begun processing
    /// metrics events from the event queue.
    WorkerStarted,

    /// Emitted when a metrics processing worker stops.
    ///
    /// Indicates that a worker has shut down, either gracefully or
    /// due to an error. Used to track worker lifecycle and system capacity.
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
            MetricsEvent::Chat(ChatEvent::SessionStarted {
                session_id, model, ..
            }) => {
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
