//! Agent loop runner implementation.
//!
//! This module provides the core agent execution loop that orchestrates LLM interactions,
//! tool execution, and event streaming for conversational AI agents.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::agent::events::TokenUsage;
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;

use crate::agent::loop_module::config::AgentLoopConfig;

mod image_fallback;
mod logging;
mod loop_execution;
mod metrics_lifecycle;
mod prompt_context;
mod round_flow;
mod round_lifecycle;
mod round_prelude;
mod session_finalize;
mod session_setup;
mod task_lifecycle;
mod tool_execution;
mod workspace_context;

pub use loop_execution::run_agent_loop_with_config;

pub(super) fn to_event_token_usage(prompt_tokens: u64, completion_tokens: u64) -> TokenUsage {
    let prompt_tokens_u32 = u32::try_from(prompt_tokens).unwrap_or(u32::MAX);
    let completion_tokens_u32 = u32::try_from(completion_tokens).unwrap_or(u32::MAX);
    let total_tokens_u64 = prompt_tokens.saturating_add(completion_tokens);
    let total_tokens_u32 = u32::try_from(total_tokens_u64).unwrap_or(u32::MAX);

    TokenUsage {
        prompt_tokens: prompt_tokens_u32,
        completion_tokens: completion_tokens_u32,
        total_tokens: total_tokens_u32,
    }
}

/// Result type for agent loop operations.
pub type Result<T> = std::result::Result<T, AgentError>;

#[allow(dead_code)]
pub async fn run_agent_loop(
    session: &mut Session,
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: CancellationToken,
    max_rounds: usize,
) -> Result<()> {
    run_agent_loop_with_config(
        session,
        initial_message,
        event_tx,
        llm,
        tools,
        cancel_token,
        AgentLoopConfig {
            max_rounds,
            skip_initial_user_message: false,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
mod tests;
