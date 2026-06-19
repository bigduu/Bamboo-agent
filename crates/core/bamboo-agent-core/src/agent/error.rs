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

    /// LLM request exceeded provider context/input limits and requires
    /// host-side overflow recovery before retry.
    #[error("LLM overflow: {0}")]
    LLMOverflow(String),

    /// The provider's stream stalled mid-response: no chunk was received within
    /// the inter-chunk idle deadline (issue #28). This indicates a hung
    /// connection rather than a slow-but-healthy stream — the consume loop
    /// resets the deadline on every received chunk, so a legitimately long
    /// stream that keeps producing data never trips it.
    #[error("Stream timed out: {0}")]
    StreamTimeout(String),

    /// Error during tool execution
    #[error("Tool error: {0}")]
    Tool(String),

    /// Token budget exceeded error
    #[error("Budget error: {0}")]
    Budget(String),

    /// Agent execution was cancelled by user
    #[error("Cancelled")]
    Cancelled,
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
}
