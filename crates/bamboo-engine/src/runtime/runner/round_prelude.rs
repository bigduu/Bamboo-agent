//! Per-round prelude helpers for the agent loop runner.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, Session};
use bamboo_infrastructure::LLMProvider;

mod cancellation;
mod prompt_updates;
mod round_state;

use cancellation::ensure_not_cancelled;
use prompt_updates::refresh_round_prompt_context;
use round_state::{build_round_id, log_round_start, update_task_round_state};

use super::prompt_context::PromptMemoryRuntimeContext;

pub(crate) async fn prepare_round(
    session: &mut Session,
    task_context: &mut Option<TaskLoopContext>,
    round: usize,
    max_rounds: usize,
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    model_name: &str,
    debug_enabled: bool,
    config: &AgentLoopConfig,
    llm: Arc<dyn LLMProvider>,
    _tools: &dyn ToolExecutor,
) -> Result<String, AgentError> {
    let _ = session_id;
    let runtime_context = PromptMemoryRuntimeContext {
        llm,
        background_model_name: config.background_model_name.clone(),
    };
    refresh_round_prompt_context(session, config.prompt_memory_flags, Some(&runtime_context)).await;
    update_task_round_state(task_context, round, max_rounds);

    let round_id = build_round_id(session_id, round);

    log_round_start(
        debug_enabled,
        session_id,
        round,
        max_rounds,
        session.messages.len(),
    );

    ensure_not_cancelled(
        cancel_token,
        metrics_collector,
        session_id,
        session.messages.len(),
    )?;

    super::metrics_lifecycle::record_round_started(
        metrics_collector,
        &round_id,
        session_id,
        model_name,
    );

    Ok(round_id)
}

#[cfg(test)]
mod tests;
