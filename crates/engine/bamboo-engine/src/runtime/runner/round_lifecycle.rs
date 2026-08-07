//! LLM round lifecycle helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::stream::handler::StreamHandlingOutput;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_llm::LLMProvider;
use bamboo_metrics::TokenUsage as MetricsTokenUsage;

use token_estimation::{estimate_completion_tokens, estimate_prompt_tokens};

mod context_preparation;
mod prefix_drift;
mod stream_execution;
mod token_budget;
mod token_estimation;

pub(crate) use context_preparation::force_overflow_context_recovery;
pub(crate) use stream_execution::discard_latest_interrupted_assistant_output;

pub(crate) struct RoundLlmExecutionOutput {
    pub stream_output: StreamHandlingOutput,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Canonical usage for this single billed provider attempt.
    pub attempt_usage: MetricsTokenUsage,
    /// Validation that can only run after a provider stream has completed.
    ///
    /// The pipeline must absorb `attempt_usage` before surfacing this error so
    /// a billed response is not turned into a zero-usage terminal round.
    pub terminal_validation_error: Option<AgentError>,
}

/// Resolve one billed attempt's canonical token usage.
///
/// Availability is decided independently for prompt and completion tokens:
/// an authoritative provider snapshot wins (including an explicit zero), a
/// non-zero legacy provider counter is the compatibility fallback, and the
/// local tokenizer estimate is used only when that component was not reported.
/// The total is always recomputed from the selected components so runtime
/// budgets and durable metrics cannot disagree with an inconsistent provider
/// `total_tokens` field.
pub(crate) fn canonical_attempt_usage(
    stream_output: &StreamHandlingOutput,
    estimated_prompt_tokens: u64,
    estimated_completion_tokens: u64,
) -> MetricsTokenUsage {
    let prompt_tokens = stream_output
        .provider_usage
        .and_then(|usage| usage.input_tokens)
        .or_else(|| (stream_output.input_tokens > 0).then_some(stream_output.input_tokens))
        .unwrap_or(estimated_prompt_tokens);
    let completion_tokens = stream_output
        .provider_usage
        .and_then(|usage| usage.output_tokens)
        .or_else(|| (stream_output.output_tokens > 0).then_some(stream_output.output_tokens))
        .unwrap_or(estimated_completion_tokens);

    MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    }
    .clamped_for_durable_metrics()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_llm_round(
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

    let frame = stream_execution::LlmStreamFrame {
        event_tx,
        cancel_token,
        session_id,
        model,
        provider_name: config.provider_name.as_deref(),
        provider_type: config.provider_type.as_deref(),
        reasoning_effort: config.reasoning_effort,
        max_context_tokens: prepared.budget.max_context_tokens,
        max_output_tokens: prepared.budget.max_output_tokens,
    };

    let (stream_output, llm_duration) = stream_execution::execute_llm_stream(
        session,
        config,
        llm,
        &prepared.prepared_context,
        tool_schemas,
        &frame,
    )
    .await?;

    // This is a terminal validation error, but the completed stream was still
    // billed. Return it alongside the canonical attempt usage so the runner can
    // account for the attempt before ending the round.
    let terminal_validation_error = (stream_output.tool_calls.is_empty()
        && stream_output.content.trim().is_empty())
    .then(|| AgentError::EmptyAssistantResponse {
        response_id: stream_output.response_id.clone(),
    });

    let prompt_tokens = estimate_prompt_tokens(&prepared.prepared_context.messages);
    let completion_tokens =
        estimate_completion_tokens(&stream_output.content, &stream_output.tool_calls);
    let attempt_usage = canonical_attempt_usage(&stream_output, prompt_tokens, completion_tokens);

    tracing::debug!(
        "[{}] LLM response completed in {}ms, answer_chars={}, reasoning_chars={}, estimated_tokens={}, canonical_tokens={}",
        session_id,
        llm_duration,
        stream_output.token_count,
        stream_output.reasoning_content.len(),
        prompt_tokens.saturating_add(completion_tokens),
        attempt_usage.total_tokens,
    );

    Ok(RoundLlmExecutionOutput {
        stream_output,
        prompt_tokens,
        completion_tokens,
        attempt_usage,
        terminal_validation_error,
    })
}

pub(crate) async fn maybe_apply_mid_turn_context_compression(
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
