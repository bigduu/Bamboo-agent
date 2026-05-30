//! Agent loop runner implementation.
//!
//! This module provides the core agent execution loop that orchestrates LLM interactions,
//! tool execution, and event streaming for conversational AI agents.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::TokenUsage;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_infrastructure::LLMProvider;

use crate::runtime::config::AgentLoopConfig;

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
pub use loop_execution::run_agent_loop_with_config;

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
