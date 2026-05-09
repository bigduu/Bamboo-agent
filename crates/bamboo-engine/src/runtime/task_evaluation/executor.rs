use std::sync::Arc;

use tokio::sync::mpsc;

use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_domain::TaskItemStatus;
use bamboo_infrastructure::{LLMProvider, LLMRequestOptions};

use super::super::task_context::TaskLoopContext;
use super::message_builder::build_task_evaluation_messages;
use super::schema::get_task_evaluation_tools;
use super::token_estimation::estimate_prompt_tokens;
use super::TaskEvaluationResult;

mod outcomes;

fn has_tool_activity(ctx: &TaskLoopContext) -> bool {
    ctx.items.iter().any(|item| !item.tool_calls.is_empty())
}

fn skipped_evaluation(reasoning: &str) -> TaskEvaluationResult {
    TaskEvaluationResult {
        needs_evaluation: false,
        updates: Vec::new(),
        reasoning: reasoning.to_string(),
        prompt_tokens: 0,
        completion_tokens: 0,
    }
}

fn normalize_lightweight_reasoning_effort(
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<ReasoningEffort> {
    reasoning_effort.map(|effort| match effort {
        ReasoningEffort::Xhigh | ReasoningEffort::Max => ReasoningEffort::High,
        other => other,
    })
}

/// 执行 TaskList 评估
pub async fn evaluate_task_progress(
    ctx: &TaskLoopContext,
    session: &Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model: &str,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<TaskEvaluationResult, AgentError> {
    use crate::runtime::stream::handler::consume_llm_stream_silent;

    let in_progress_count = ctx
        .items
        .iter()
        .filter(|item| matches!(item.status, TaskItemStatus::InProgress))
        .count();

    if in_progress_count == 0 {
        return Ok(skipped_evaluation("No in-progress tasks to evaluate"));
    }

    let is_plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|s| s.plan_mode.as_ref())
        .is_some();

    if !is_plan_mode && !has_tool_activity(ctx) {
        return Ok(skipped_evaluation(
            "No tool executions yet; skipping task evaluation.",
        ));
    }

    tracing::info!(
        "[{}] Evaluating {} in-progress task items",
        session_id,
        in_progress_count
    );

    let _ = event_tx
        .send(AgentEvent::TaskEvaluationStarted {
            session_id: session_id.to_string(),
            items_count: in_progress_count,
        })
        .await;

    let messages = build_task_evaluation_messages(ctx, session);
    let prompt_tokens = estimate_prompt_tokens(&messages);
    let tools = get_task_evaluation_tools();

    // Use model from parameter (passed from config), not from session.
    tracing::debug!("[{}] Task evaluation using model: {}", session_id, model);

    let request_reasoning_effort = normalize_lightweight_reasoning_effort(reasoning_effort);
    if request_reasoning_effort != reasoning_effort {
        tracing::debug!(
            "[{}] Task evaluation downgraded reasoning effort from {:?} to {:?} for lightweight request",
            session_id,
            reasoning_effort,
            request_reasoning_effort
        );
    }

    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort: request_reasoning_effort,
        parallel_tool_calls: None,
        responses: None,
        request_purpose: Some("task_evaluation".to_string()),
    };
    match llm
        .chat_stream_with_options(&messages, &tools, Some(8192), model, Some(&request_options))
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
            tracing::warn!("[{}] Task evaluation failed: {}", session_id, error);
            Ok(skipped_evaluation(&format!("Evaluation failed: {}", error)))
        }
    }
}
