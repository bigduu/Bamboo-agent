use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::agent::events::TokenBudgetUsage;
use crate::agent::core::budget::PreparedContext;
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::{LLMProvider, LLMRequestOptions};
use crate::core::ReasoningEffort;

pub(super) async fn execute_llm_stream(
    session: &mut Session,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    prepared_context: &PreparedContext,
    tool_schemas: &[ToolSchema],
    max_output_tokens: u32,
    model: &str,
    reasoning_effort: Option<ReasoningEffort>,
    session_id: &str,
) -> Result<
    (
        crate::agent::loop_module::stream::handler::StreamHandlingOutput,
        u128,
    ),
    AgentError,
> {
    let llm_started_at = std::time::Instant::now();
    let request_options = LLMRequestOptions {
        reasoning_effort,
        responses: None,
    };
    let stream = llm
        .chat_stream_with_options(
            &prepared_context.messages,
            tool_schemas,
            Some(max_output_tokens),
            model,
            Some(&request_options),
        )
        .await
        .map_err(|error| AgentError::LLM(error.to_string()))?;

    // Send token budget update AFTER LLM call succeeds.
    // This timing gives frontend time to subscribe to /events endpoint.
    let usage = TokenBudgetUsage {
        system_tokens: prepared_context.token_usage.system_tokens,
        summary_tokens: prepared_context.token_usage.summary_tokens,
        window_tokens: prepared_context.token_usage.window_tokens,
        total_tokens: prepared_context.token_usage.total_tokens,
        budget_limit: prepared_context.token_usage.budget_limit,
        truncation_occurred: prepared_context.truncation_occurred,
        segments_removed: prepared_context.segments_removed,
    };

    session.token_usage = Some(usage.clone());

    let budget_event = AgentEvent::TokenBudgetUpdated { usage };
    if let Err(error) = event_tx.send(budget_event).await {
        log::warn!(
            "[{}] Failed to send token budget event: {}",
            session_id,
            error
        );
    }

    let stream_output = crate::agent::loop_module::stream::handler::consume_llm_stream(
        stream,
        event_tx,
        cancel_token,
        session_id,
    )
    .await?;

    let llm_duration = llm_started_at.elapsed().as_millis();

    Ok((stream_output, llm_duration))
}

#[cfg(test)]
mod tests;
