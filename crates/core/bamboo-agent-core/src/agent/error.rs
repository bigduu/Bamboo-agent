//! Agent error types
//!
//! This module defines the error types used throughout the agent system.

use thiserror::Error;

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
    /// progress stalled. Kept distinct from `LLM` so generic retry logic cannot
    /// replay partial output or tool-call state (#618).
    #[error("Stream timed out: {0}")]
    StreamTimeout(String),

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
