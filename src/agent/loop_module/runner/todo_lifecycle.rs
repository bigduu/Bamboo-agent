//! Todo lifecycle helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::TokenUsage as MetricsTokenUsage;

mod evaluation;
mod finalize;

pub(super) async fn evaluate_round_todo_progress(
    todo_context: &mut Option<TodoLoopContext>,
    session: &mut Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
) -> Result<MetricsTokenUsage, AgentError> {
    evaluation::evaluate_round_todo_progress(
        todo_context,
        session,
        llm,
        event_tx,
        session_id,
        round_number,
        model_name,
    )
    .await
}

pub(super) async fn finalize_todo_context(
    todo_context: Option<TodoLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
) {
    finalize::finalize_todo_context(todo_context, session, event_tx, session_id, config).await;
}
