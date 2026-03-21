//! Task lifecycle helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::metrics::TokenUsage as MetricsTokenUsage;
use crate::core::ReasoningEffort;

mod evaluation;
mod finalize;

pub(super) async fn evaluate_round_task_progress(
    task_context: &mut Option<TaskLoopContext>,
    session: &mut Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<MetricsTokenUsage, AgentError> {
    evaluation::evaluate_round_task_progress(
        task_context,
        session,
        llm,
        event_tx,
        session_id,
        round_number,
        model_name,
        reasoning_effort,
    )
    .await
}

pub(super) async fn finalize_task_context(
    task_context: Option<TaskLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
) {
    finalize::finalize_task_context(task_context, session, event_tx, session_id, config).await;
}
