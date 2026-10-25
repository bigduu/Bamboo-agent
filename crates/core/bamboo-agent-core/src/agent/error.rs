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
    identity: Box<StreamTimeoutIdentity>,
    phase: StreamTimeoutPhase,
    deadline: Duration,
    provider: Option<String>,
    model: Option<String>,
    last_transport: Duration,
    last_semantic: Option<Duration>,
    turn_retry_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamTimeoutIdentity {
    diagnostic_id: uuid::Uuid,
    observed_at: chrono::DateTime<chrono::Utc>,
    last_http_retry: Option<(u16, Duration)>,
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
            identity: Box::new(StreamTimeoutIdentity {
                diagnostic_id: uuid::Uuid::new_v4(),
                observed_at: chrono::Utc::now(),
                last_http_retry: None,
            }),
            phase,
            deadline,
            provider: provider
                .filter(|value| bamboo_domain::StreamTimeoutDiagnostic::valid_identifier(value)),
            model: model
                .filter(|value| bamboo_domain::StreamTimeoutDiagnostic::valid_identifier(value)),
            last_transport,
            last_semantic,
            turn_retry_eligible,
        }
    }

    /// Attach a status and bounded delay observed while the initial provider
    /// request was being retried. No response body, URL, or credential is kept.
    pub fn with_last_http_retry(mut self, status: u16, delay: Duration) -> Self {
        self.identity.last_http_retry = Some((status, delay));
        self
    }

    pub fn last_http_retry(&self) -> Option<(u16, Duration)> {
        self.identity.last_http_retry
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

    /// Project the typed watchdog failure without parsing its display text.
    /// This observation carries no authority to replay a request or a tool.
    pub fn diagnostic(
        &self,
        session: &bamboo_domain::Session,
    ) -> Option<bamboo_domain::LlmRequestDiagnostic> {
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).ok();
        let last_semantic_ms_ago = match self.last_semantic {
            Some(elapsed) => Some(millis(elapsed)?),
            None => None,
        };
        let (last_http_status, retry_delay_ms) = match self.last_http_retry() {
            Some((status, delay)) => (Some(status), Some(millis(delay)?)),
            None => (None, None),
        };
        bamboo_domain::LlmRequestDiagnostic::from_stream_timeout(
            bamboo_domain::StreamTimeoutDiagnostic {
                schema_version: 1,
                kind: "stream_timeout".into(),
                diagnostic_id: self.identity.diagnostic_id,
                error_id: self.identity.diagnostic_id,
                observed_at: self.identity.observed_at,
                provider: self.provider.clone(),
                model: self.model.clone(),
                session_hash: bamboo_domain::llm_diagnostic_session_hash(&session.id),
                run_id: session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| uuid::Uuid::parse_str(&state.run_id).ok()),
                phase: self.phase.to_string(),
                deadline_ms: millis(self.deadline)?,
                last_transport_ms_ago: millis(self.last_transport)?,
                last_semantic_ms_ago,
                semantic_output_started: self.semantic_output_started(),
                retry_safe: self.retry_safe(),
                last_http_status,
                retry_delay_ms,
            },
        )
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
        )?;
        if let Some((status, delay)) = self.last_http_retry() {
            write!(
                formatter,
                ", last_http_status={status}, retry_delay_ms={}",
                delay.as_millis()
            )?;
        }
        write!(
            formatter,
            ". Diagnostic ID: {}.",
            self.identity.diagnostic_id
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

    /// A provider exhausted its request budget, or a stream failed after
    /// semantic output. Replaying the turn would reset that budget or output.
    #[error("LLM error: {0}")]
    LLMNonRetryable(String),

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
        let expected = format!(
            "Stream timed out: phase=bootstrap, deadline_ms=120000, provider=provider-id, \
             model=model-id, last_transport_ms_ago=120000, last_semantic_ms_ago=never, \
             semantic_output_started=false, retry_safe=true. Diagnostic ID: {}.",
            timeout.identity.diagnostic_id,
        );
        assert_eq!(AgentError::StreamTimeout(timeout).to_string(), expected);
    }

    #[test]
    fn bootstrap_timeout_can_include_secret_free_http_retry_evidence() {
        let timeout = StreamTimeoutError::new(
            StreamTimeoutPhase::Bootstrap,
            Duration::from_secs(120),
            None,
            None,
            Duration::from_secs(120),
            None,
            true,
        );
        let identity_before_retry = (timeout.identity.diagnostic_id, timeout.identity.observed_at);
        let timeout = timeout.with_last_http_retry(429, Duration::from_secs(60));
        assert_eq!(
            (timeout.identity.diagnostic_id, timeout.identity.observed_at),
            identity_before_retry
        );

        assert!(timeout.retry_safe());
        assert_eq!(
            timeout.last_http_retry(),
            Some((429, Duration::from_secs(60)))
        );
        assert!(timeout
            .to_string()
            .contains("last_http_status=429, retry_delay_ms=60000"));

        let cloned = timeout.clone();
        assert_eq!(cloned, timeout);
        assert_eq!(cloned.to_string(), timeout.to_string());
        assert_eq!(cloned.last_http_retry(), timeout.last_http_retry());
        let session = bamboo_domain::Session::new("private-session", "model");
        assert_eq!(
            serde_json::to_value(cloned.diagnostic(&session).unwrap()).unwrap(),
            serde_json::to_value(timeout.diagnostic(&session).unwrap()).unwrap()
        );
    }

    #[test]
    fn every_watchdog_phase_exports_the_exact_unique_terminal_error() {
        let session = bamboo_domain::Session::new("private-session", "model");
        for (phase, semantic) in [
            (StreamTimeoutPhase::Bootstrap, None),
            (StreamTimeoutPhase::FirstSemantic, None),
            (
                StreamTimeoutPhase::TransportIdle,
                Some(Duration::from_secs(120)),
            ),
            (
                StreamTimeoutPhase::SemanticIdle,
                Some(Duration::from_secs(600)),
            ),
        ] {
            let timeout = StreamTimeoutError::new(
                phase,
                Duration::from_secs(if phase == StreamTimeoutPhase::SemanticIdle {
                    600
                } else {
                    120
                }),
                Some("provider-id".into()),
                Some("gpt-6-astra".into()),
                Duration::from_secs(120),
                semantic,
                true,
            );
            let timeout = if phase == StreamTimeoutPhase::Bootstrap {
                timeout.with_last_http_retry(429, Duration::from_secs(5))
            } else {
                timeout
            };
            let receipt = timeout.diagnostic(&session).unwrap();
            let error = AgentError::StreamTimeout(timeout);
            assert_eq!(receipt.error_message(), error.to_string());
            assert!(receipt.matches_session(&session.id));
            assert!(!receipt.matches_session("foreign-session"));
            assert!(!receipt.matches_error_message(&format!("LLM error: {error}")));
            let wire = serde_json::to_value(&receipt).unwrap();
            assert_eq!(wire["kind"], "stream_timeout");
            assert_eq!(wire["semantic_output_started"], semantic.is_some());
            assert_eq!(wire["retry_safe"], semantic.is_none());
            assert_eq!(wire["diagnostic_id"], wire["error_id"]);
            assert!(wire.get("request_id").is_none());
            assert!(!wire.to_string().contains("private-session"));

            let other = StreamTimeoutError::new(
                phase,
                Duration::from_secs(120),
                None,
                None,
                Duration::from_secs(120),
                semantic,
                true,
            );
            assert_ne!(other.identity.diagnostic_id, receipt.diagnostic_id());
            assert!(!receipt.matches_error_message(&AgentError::StreamTimeout(other).to_string()));
        }
        // StreamHandlingFailure adds a boxed partial-output snapshot to this
        // error; the complete error must stay below Clippy's 128-byte limit.
        assert!(std::mem::size_of::<(AgentError, Box<()>)>() < 128);
    }

    #[test]
    fn timeout_identifiers_are_redacted_before_both_display_and_export() {
        let session = bamboo_domain::Session::new("session", "model");
        for secret in [
            "https://private.invalid/token",
            "sk-private-credential",
            "Bearer_private",
            "raw private input",
        ] {
            let timeout = StreamTimeoutError::new(
                StreamTimeoutPhase::TransportIdle,
                Duration::from_secs(120),
                Some(secret.into()),
                Some(secret.into()),
                Duration::from_secs(120),
                None,
                true,
            );
            let receipt = timeout.diagnostic(&session).unwrap();
            assert!(!timeout.to_string().contains(secret));
            let wire = serde_json::to_value(&receipt).unwrap();
            assert!(wire["provider"].is_null());
            assert!(wire["model"].is_null());
            assert_eq!(
                receipt.error_message(),
                AgentError::StreamTimeout(timeout).to_string()
            );
        }
    }
}
