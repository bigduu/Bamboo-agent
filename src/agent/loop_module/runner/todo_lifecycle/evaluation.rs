use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::loop_module::todo_evaluation::evaluate_todo_progress;
use crate::agent::metrics::TokenUsage as MetricsTokenUsage;
use crate::core::ReasoningEffort;

pub(super) async fn evaluate_round_todo_progress(
    todo_context: &mut Option<TodoLoopContext>,
    session: &mut Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<MetricsTokenUsage, AgentError> {
    let Some(ctx_snapshot) = todo_context.as_ref() else {
        return Ok(MetricsTokenUsage::default());
    };

    log::debug!(
        "[{}] Evaluating todo list progress at end of round {}",
        session_id,
        round_number
    );

    let model = model_name
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let mut usage = MetricsTokenUsage::default();
    match evaluate_todo_progress(
        ctx_snapshot,
        session,
        llm,
        event_tx,
        session_id,
        model,
        reasoning_effort,
    )
    .await
    {
        Ok(evaluation_result) => {
            usage.prompt_tokens = evaluation_result.prompt_tokens;
            usage.completion_tokens = evaluation_result.completion_tokens;
            usage.total_tokens = usage.prompt_tokens.saturating_add(usage.completion_tokens);

            if evaluation_result.needs_evaluation && !evaluation_result.updates.is_empty() {
                log::info!(
                    "[{}] LLM evaluated {} todo item updates",
                    session_id,
                    evaluation_result.updates.len()
                );

                for update in evaluation_result.updates {
                    let status = update.status.clone();
                    if let Some(ctx) = todo_context.as_mut() {
                        ctx.update_item_status(&update.item_id, status.clone());
                    }
                    let _ =
                        session.update_todo_item(&update.item_id, status, update.notes.as_deref());
                }
            }
        }
        Err(error) => {
            log::warn!("[{}] Todo evaluation failed: {}", session_id, error);
        }
    }

    Ok(usage)
}
