//! Agent loop runner implementation.
//!
//! This module provides the core agent execution loop that orchestrates LLM interactions,
//! tool execution, and event streaming for conversational AI agents.

use bamboo_agent_core::TokenUsage;
use bamboo_agent_core::{AgentError, Session};

pub mod image_fallback;
mod logging;
mod loop_execution;
mod metrics_lifecycle;
pub(crate) mod prompt_context;
pub(crate) mod round_frame;
pub(crate) mod round_lifecycle;
pub(crate) mod round_prelude;
pub(crate) mod session_finalize;
pub(crate) mod session_setup;
pub(crate) mod state_bridge;
mod task_lifecycle;
pub(crate) mod tool_execution;
mod workspace_context;

pub use bamboo_agent_core::PromptSnapshot;
pub(crate) use loop_execution::run_agent_loop_with_config;

pub fn read_prompt_snapshot(session: &Session) -> Option<PromptSnapshot> {
    session_setup::read_prompt_snapshot(session)
}

pub fn refresh_prompt_snapshot(session: &mut Session) {
    session_setup::refresh_prompt_snapshot(session)
}

pub(super) fn to_event_token_usage(prompt_tokens: u64, completion_tokens: u64) -> TokenUsage {
    let mut usage = TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: 0,
    };
    usage.recompute_total();
    usage
}

/// Result type for agent loop operations.
pub type Result<T> = std::result::Result<T, AgentError>;

#[cfg(test)]
mod tests;
