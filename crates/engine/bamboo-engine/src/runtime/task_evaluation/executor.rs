use std::sync::Arc;

use tokio::sync::mpsc;

use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_domain::TaskItemStatus;
use bamboo_llm::{LLMProvider, LLMRequestOptions};

use super::super::task_context::TaskLoopContext;
use super::message_builder::build_task_evaluation_messages;
use super::schema::get_task_evaluation_tools;
use super::token_estimation::estimate_prompt_tokens;
use super::TaskEvaluationResult;

mod outcomes;

pub struct TaskEvaluationFrame<'a> {
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub session_id: &'a str,
    pub model: &'a str,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub timeout_context: &'a crate::runtime::stream::handler::StreamTimeoutContext,
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
    frame: &TaskEvaluationFrame<'_>,
) -> Result<TaskEvaluationResult, AgentError> {
    evaluate_task_progress_with_dispatch(ctx, session, llm, frame, std::future::ready(()), || {})
        .await
}

/// Internal evaluator entry point that observes the exact provider-dispatch
/// boundary. Queueing and prompt preparation must not inflate evaluator
/// duration metrics, so the callback runs immediately before the provider
/// future is first polled.
pub(crate) async fn evaluate_task_progress_with_dispatch<G, Fut, F>(
    ctx: &TaskLoopContext,
    session: &Session,
    llm: Arc<dyn LLMProvider>,
    frame: &TaskEvaluationFrame<'_>,
    acquire_dispatch_guard: Fut,
    on_dispatch: F,
) -> Result<TaskEvaluationResult, AgentError>
where
    Fut: std::future::Future<Output = G>,
    F: FnOnce(),
{
    use crate::runtime::stream::handler::{
        await_stream_bootstrap, consume_llm_stream_silent_with_context,
    };

    let event_tx = frame.event_tx;
    let session_id = frame.session_id;
    let model = frame.model;
    let reasoning_effort = frame.reasoning_effort;
    let timeout_context = frame.timeout_context.clone().begin_request();

    let in_progress_count = ctx
        .items
        .iter()
        .filter(|item| matches!(item.status, TaskItemStatus::InProgress))
        .count();

    if in_progress_count == 0 {
        return Ok(skipped_evaluation("No in-progress tasks to evaluate"));
    }

    // When to evaluate is owned entirely by the caller (the loop spawns this only
    // on a Task-tool write); this function just decides how. The single remaining
    // guard above skips when there is nothing in progress to assess.
    tracing::info!(
        "[{}] Evaluating {} in-progress task items",
        session_id,
        in_progress_count
    );

    let _ = event_tx
        .send(AgentEvent::TaskEvaluationStarted {
            session_id: session_id.to_string(),
            items_count: in_progress_count,
            generation: Some(ctx.version),
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
        required_tool: None,
        responses: None,
        request_purpose: Some("task_evaluation".to_string()),
        cache: None,
    };
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let _dispatch_guard = acquire_dispatch_guard.await;
    on_dispatch();
    let stream = match await_stream_bootstrap(
        llm.chat_stream_with_options(&messages, &tools, Some(8192), model, Some(&request_options)),
        &cancel_token,
        session_id,
        &timeout_context,
    )
    .await?
    {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!("[{}] Task evaluation failed: {}", session_id, error);
            return Ok(skipped_evaluation(&format!("Evaluation failed: {error}")));
        }
    };
    let stream_output =
        consume_llm_stream_silent_with_context(stream, &cancel_token, session_id, &timeout_context)
            .await?;

    Ok(outcomes::build_success_result(
        stream_output,
        event_tx,
        session_id,
        prompt_tokens,
        ctx.version,
    )
    .await)
}
