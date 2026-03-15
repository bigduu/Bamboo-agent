use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::{AgentError, AgentEvent, Session, TodoItemStatus};
use crate::agent::llm::{LLMProvider, LLMRequestOptions};
use crate::core::ReasoningEffort;

use super::super::todo_context::TodoLoopContext;
use super::message_builder::build_todo_evaluation_messages;
use super::schema::get_todo_evaluation_tools;
use super::token_estimation::estimate_prompt_tokens;
use super::TodoEvaluationResult;

mod outcomes;

fn has_tool_activity(ctx: &TodoLoopContext) -> bool {
    ctx.items.iter().any(|item| !item.tool_calls.is_empty())
}

fn skipped_evaluation(reasoning: &str) -> TodoEvaluationResult {
    TodoEvaluationResult {
        needs_evaluation: false,
        updates: Vec::new(),
        reasoning: reasoning.to_string(),
        prompt_tokens: 0,
        completion_tokens: 0,
    }
}

/// 执行 TodoList 评估
pub async fn evaluate_todo_progress(
    ctx: &TodoLoopContext,
    session: &Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model: &str,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<TodoEvaluationResult, AgentError> {
    use crate::agent::loop_module::stream::handler::consume_llm_stream_silent;

    let in_progress_count = ctx
        .items
        .iter()
        .filter(|item| matches!(item.status, TodoItemStatus::InProgress))
        .count();

    if in_progress_count == 0 {
        return Ok(skipped_evaluation("No in-progress tasks to evaluate"));
    }

    if !has_tool_activity(ctx) {
        return Ok(skipped_evaluation(
            "No tool executions yet; skipping todo evaluation.",
        ));
    }

    log::info!(
        "[{}] Evaluating {} in-progress todo items",
        session_id,
        in_progress_count
    );

    let _ = event_tx
        .send(AgentEvent::TodoEvaluationStarted {
            session_id: session_id.to_string(),
            items_count: in_progress_count,
        })
        .await;

    let messages = build_todo_evaluation_messages(ctx, session);
    let prompt_tokens = estimate_prompt_tokens(&messages);
    let tools = get_todo_evaluation_tools();

    // Use model from parameter (passed from config), not from session.
    log::debug!("[{}] Todo evaluation using model: {}", session_id, model);

    let request_options = LLMRequestOptions { reasoning_effort };
    match llm
        .chat_stream_with_options(&messages, &tools, Some(500), model, Some(&request_options))
        .await
    {
        Ok(stream) => {
            let stream_output = consume_llm_stream_silent(
                stream,
                &tokio_util::sync::CancellationToken::new(),
                session_id,
            )
            .await
            .map_err(|error| AgentError::LLM(error.to_string()))?;

            Ok(
                outcomes::build_success_result(stream_output, event_tx, session_id, prompt_tokens)
                    .await,
            )
        }
        Err(error) => {
            log::warn!("[{}] Todo evaluation failed: {}", session_id, error);
            Ok(skipped_evaluation(&format!("Evaluation failed: {}", error)))
        }
    }
}
