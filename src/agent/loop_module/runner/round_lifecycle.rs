//! LLM round lifecycle helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::stream::handler::StreamHandlingOutput;
use crate::agent::metrics::TokenUsage as MetricsTokenUsage;

use token_estimation::{estimate_completion_tokens, estimate_prompt_tokens};

mod context_preparation;
mod stream_execution;
mod token_budget;
mod token_estimation;

pub(super) use context_preparation::force_overflow_context_recovery;

pub(super) struct RoundLlmExecutionOutput {
    pub stream_output: StreamHandlingOutput,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub round_usage: MetricsTokenUsage,
}

pub(super) async fn execute_llm_round(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
    model_name: &str,
    tool_schemas: &[ToolSchema],
) -> Result<RoundLlmExecutionOutput, AgentError> {
    let prepared = context_preparation::prepare_round_context(
        session,
        config,
        model_name,
        session_id,
        tool_schemas,
        llm,
        Some(event_tx),
    )
    .await?;

    // Use model from config (provided by execute request), not from session.
    let model = config
        .model_name
        .as_deref()
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let (stream_output, llm_duration) = stream_execution::execute_llm_stream(
        session,
        llm,
        event_tx,
        cancel_token,
        &prepared.prepared_context,
        prepared.budget.max_context_tokens,
        tool_schemas,
        prepared.budget.max_output_tokens,
        model,
        config.provider_name.as_deref(),
        config.reasoning_effort,
        session_id,
    )
    .await?;

    if stream_output.tool_calls.is_empty() && stream_output.content.trim().is_empty() {
        return Err(AgentError::LLM(
            "empty assistant response from LLM (retryable)".to_string(),
        ));
    }

    let prompt_tokens = estimate_prompt_tokens(&prepared.prepared_context.messages);
    let completion_tokens =
        estimate_completion_tokens(&stream_output.content, &stream_output.tool_calls);
    let round_usage = MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    };

    tracing::debug!(
        "[{}] LLM response completed in {}ms, answer_chars={}, reasoning_chars={}, {} estimated tokens",
        session_id,
        llm_duration,
        stream_output.token_count,
        stream_output.reasoning_content.len(),
        round_usage.total_tokens
    );

    Ok(RoundLlmExecutionOutput {
        stream_output,
        prompt_tokens,
        completion_tokens,
        round_usage,
    })
}

pub(super) async fn maybe_apply_mid_turn_context_compression(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model_name: &str,
    tool_schemas: &[ToolSchema],
) -> Result<bool, AgentError> {
    context_preparation::maybe_apply_host_context_compression(
        session,
        config,
        model_name,
        session_id,
        tool_schemas,
        llm,
        Some(event_tx),
        "mid-turn",
    )
    .await
}
