//! Agent error types
//!
//! This module defines the error types used throughout the agent system.

use std::fmt;
use std::time::Duration;

use thiserror::Error;

/// Watchdog phase that expired while establishing or consuming an LLM stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTimeoutPhase {
    /// The provider call did not establish a response stream before the
    /// transport-idle deadline.
    Bootstrap,
    /// No valid provider transport frame arrived before the transport-idle
    /// deadline.
    TransportIdle,
    /// The stream was transport-live but produced no semantic output before
    /// the first-semantic deadline.
    FirstSemantic,
    /// Semantic output had started, then stopped before the semantic-idle
    /// deadline.
    SemanticIdle,
}

impl fmt::Display for StreamTimeoutPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Bootstrap => "bootstrap",
            Self::TransportIdle => "transport_idle",
            Self::FirstSemantic => "first_semantic",
            Self::SemanticIdle => "semantic_idle",
        })
    }
}

/// Structured, secret-free diagnostics for one LLM stream watchdog expiry.
///
/// Retry safety is derived from structured request provenance and observed
/// semantic output instead of parsed from the formatted diagnostic string. A
/// primary response timeout before any text, reasoning, or tool-call delta can
/// be replayed by the turn-level retry policy; auxiliary calls and timeouts
/// after semantic output starts are terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTimeoutError {
    phase: StreamTimeoutPhase,
    deadline: Duration,
    provider: Option<String>,
    model: Option<String>,
    last_transport: Duration,
    last_semantic: Option<Duration>,
    turn_retry_eligible: bool,
}

impl StreamTimeoutError {
    /// Build a timeout diagnostic from already-sanitized provider/model
    /// identifiers, monotonic elapsed durations, and whether the caller is the
    /// primary turn response eligible for replay.
    pub fn new(
        phase: StreamTimeoutPhase,
        deadline: Duration,
        provider: Option<String>,
        model: Option<String>,
        last_transport: Duration,
        last_semantic: Option<Duration>,
        turn_retry_eligible: bool,
    ) -> Self {
        Self {
            phase,
            deadline,
            provider,
            model,
            last_transport,
            last_semantic,
            turn_retry_eligible,
        }
    }

    /// Watchdog phase that expired.
    pub fn phase(&self) -> StreamTimeoutPhase {
        self.phase
    }

    /// Whether replaying the turn cannot duplicate already-visible semantic
    /// output or a partially emitted tool call.
    pub fn retry_safe(&self) -> bool {
        self.turn_retry_eligible && self.last_semantic.is_none()
    }

    /// Whether any text, reasoning, or tool-call delta was observed before the
    /// timeout.
    pub fn semantic_output_started(&self) -> bool {
        self.last_semantic.is_some()
    }
}

impl fmt::Display for StreamTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let last_semantic = self
            .last_semantic
            .map(|duration| format!("{}ms", duration.as_millis()))
            .unwrap_or_else(|| "never".to_string());
        write!(
            formatter,
            "phase={}, deadline_ms={}, provider={}, model={}, last_transport_ms_ago={}, \
             last_semantic_ms_ago={}, semantic_output_started={}, retry_safe={}",
            self.phase,
            self.deadline.as_millis(),
            self.provider.as_deref().unwrap_or("unknown"),
            self.model.as_deref().unwrap_or("unknown"),
            self.last_transport.as_millis(),
            last_semantic,
            self.semantic_output_started(),
            self.retry_safe(),
        )
    }
}

/// Errors that can occur during agent operations
#[derive(Error, Debug)]
pub enum AgentError {
    /// Session with the specified ID was not found
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    /// Error from LLM provider (API error, network error, etc.)
    #[error("LLM error: {0}")]
    LLM(String),

    /// The provider completed an assistant response without visible content or
    /// tool calls. Kept distinct from transient provider failures so turn-level
    /// retry logic cannot replay the same billable empty response.
    #[error("Empty assistant response from LLM (response_id={response_id:?})")]
    EmptyAssistantResponse {
        /// Provider response identifier when one was emitted. Response IDs are
        /// diagnostic correlation handles; no request content or credentials
        /// are retained here.
        response_id: Option<String>,
    },

    /// LLM request exceeded provider context/input limits and requires
    /// host-side overflow recovery before retry.
    #[error("LLM overflow: {0}")]
    LLMOverflow(String),

    /// An LLM stream watchdog expired. The attached diagnostic identifies
    /// whether transport liveness, first semantic output, or midstream semantic
    /// progress stalled. Kept distinct from `LLM` so the turn retry policy can
    /// replay only timeouts that occurred before semantic output, never partial
    /// text or tool-call state (#618).
    #[error("Stream timed out: {0}")]
    StreamTimeout(StreamTimeoutError),

    /// Error during tool execution
    #[error("Tool error: {0}")]
    Tool(String),

    /// Stable Project identity, availability, or workspace ownership failed
    /// validation before an execution round.
    #[error("Project context error: {0}")]
    ProjectContext(String),

    /// A lifecycle hook deliberately suspended this activation. The outer
    /// runner converts this control signal into a normal persisted suspension.
    #[error("Hook suspended: {0}")]
    HookSuspended(String),

    /// Token budget exceeded error
    #[error("Budget error: {0}")]
    Budget(String),

    /// Agent execution was cancelled by user
    #[error("Cancelled")]
    Cancelled,

    /// An actor child worker produced no first frame within the deadline — it is
    /// presumed dead (e.g. a pooled worker that exited after its liveness check
    /// but before handling the Run). Signals the runner to reap it and retry on a
    /// fresh worker, rather than waiting forever on a queued Run nobody serves.
    #[error("Worker unresponsive: {0}")]
    WorkerUnresponsive(String),
}

impl AgentError {
    /// Returns `true` if this error represents a user-initiated cancellation.
    ///
    /// Prefer this over substring-matching the error message: a reworded or
    /// localized message must not silently break cancellation/terminal-status
    /// logic.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, AgentError::Cancelled)
    }

    /// Returns `true` when a lifecycle hook intentionally suspended the run.
    pub fn is_hook_suspended(&self) -> bool {
        matches!(self, AgentError::HookSuspended(_))
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentError, StreamTimeoutError, StreamTimeoutPhase};
    use std::time::Duration;

    #[test]
    fn empty_assistant_response_is_typed_and_has_secret_free_diagnostics() {
        let with_id = AgentError::EmptyAssistantResponse {
            response_id: Some("resp_740".to_string()),
        };
        assert!(matches!(
            &with_id,
            AgentError::EmptyAssistantResponse {
                response_id: Some(response_id)
            } if response_id == "resp_740"
        ));
        assert_eq!(
            with_id.to_string(),
            "Empty assistant response from LLM (response_id=Some(\"resp_740\"))"
        );

        let without_id = AgentError::EmptyAssistantResponse { response_id: None };
        assert_eq!(
            without_id.to_string(),
            "Empty assistant response from LLM (response_id=None)"
        );
    }

    #[test]
    fn stream_timeout_retry_safety_is_structured_not_message_parsed() {
        let timeout = StreamTimeoutError::new(
            StreamTimeoutPhase::Bootstrap,
            Duration::from_secs(120),
            Some("provider-id".to_string()),
            Some("model-id".to_string()),
            Duration::from_secs(120),
            None,
            true,
        );

        assert_eq!(timeout.phase(), StreamTimeoutPhase::Bootstrap);
        assert!(timeout.retry_safe());
        assert!(!timeout.semantic_output_started());
        assert_eq!(
            AgentError::StreamTimeout(timeout).to_string(),
            "Stream timed out: phase=bootstrap, deadline_ms=120000, provider=provider-id, \
             model=model-id, last_transport_ms_ago=120000, last_semantic_ms_ago=never, \
             semantic_output_started=false, retry_safe=true"
        );
    }
}
