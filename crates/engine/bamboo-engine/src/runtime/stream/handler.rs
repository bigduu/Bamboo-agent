use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_config::StreamTimeoutConfig;
use bamboo_llm::LLMStream;

mod chunk_handling;
mod consume;
mod stream_state;

/// Resolved identifiers and deadlines for one stream. Identifiers are sanitized
/// before they reach timeout diagnostics; prompts and provider payloads are
/// never retained here.
#[derive(Debug, Clone)]
pub struct StreamTimeoutContext {
    pub(crate) policy: StreamTimeoutConfig,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
}

impl StreamTimeoutContext {
    pub fn new(policy: StreamTimeoutConfig, provider: Option<&str>, model: Option<&str>) -> Self {
        let policy = match policy.validate() {
            Ok(()) => policy,
            Err(error) => {
                tracing::warn!(
                    "invalid programmatic stream timeout policy ({error}); using safe defaults"
                );
                StreamTimeoutConfig::default()
            }
        };
        Self {
            policy,
            provider: provider.and_then(sanitize_identifier),
            model: model.and_then(sanitize_identifier),
        }
    }
}

impl Default for StreamTimeoutContext {
    fn default() -> Self {
        Self::new(StreamTimeoutConfig::default(), None, None)
    }
}

fn sanitize_identifier(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let sanitized: String = value
        .chars()
        .take(120)
        .map(|character| match character {
            character if character.is_ascii_alphanumeric() => character,
            '-' | '_' | '.' | ':' | '/' | '@' => character,
            _ => '_',
        })
        .collect();
    (!sanitized.is_empty()).then_some(sanitized)
}

/// Raw authoritative usage fields preserved from provider terminal events.
///
/// Each field remains optional so callers can distinguish an explicit
/// provider-reported zero from an omitted value. Flat counters on
/// [`StreamHandlingOutput`] remain the normalized compatibility view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderUsageSnapshot {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    /// OpenAI Responses cache-write volume. This is raw provider metadata and
    /// is not folded into the disjoint legacy prompt-cache counters.
    pub cache_write_input_tokens: Option<u64>,
}

pub struct StreamHandlingOutput {
    pub response_id: Option<String>,
    pub content: String,
    pub reasoning_content: String,
    /// Provider-minted signature covering `reasoning_content`, present only
    /// when the turn's thinking arrived as exactly one signed Anthropic
    /// `thinking` block — see [`bamboo_llm::LLMChunk::ReasoningSignature`] (#520).
    pub reasoning_signature: Option<String>,
    pub token_count: usize,
    pub tool_calls: Vec<ToolCall>,
    pub output_tokens: u64,
    pub thinking_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    /// Merged authoritative provider snapshot, when at least one provider-usage
    /// chunk was observed. Repeated cumulative snapshots are idempotent, absent
    /// fields do not erase known values, and explicit zeros remain `Some(0)`.
    pub provider_usage: Option<ProviderUsageSnapshot>,
    /// Normalized non-cached ("fresh") input, disjoint from the adjacent cache
    /// counters. When a provider total is available this is derived from that
    /// total with a saturating, cache-subset policy; the raw total remains in
    /// [`Self::provider_usage`].
    pub input_tokens: u64,
}

impl StreamHandlingOutput {
    /// Prompt usage for runtime budget enforcement.
    ///
    /// Provider-reported totals are authoritative when present. Legacy streams
    /// retain their historical fallback to the flat fresh-input counter.
    pub(crate) fn prompt_tokens_for_runtime_budget(&self) -> u64 {
        self.provider_usage
            .and_then(|usage| usage.input_tokens)
            .unwrap_or(self.input_tokens)
    }

    /// Completion usage for runtime budget enforcement.
    ///
    /// A provider-reported output total (including explicit zero) wins over
    /// legacy summaries. Reasoning is a subset breakdown and is never added.
    pub(crate) fn completion_tokens_for_runtime_budget(&self) -> u64 {
        self.provider_usage
            .and_then(|usage| usage.output_tokens)
            .unwrap_or(self.output_tokens)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct PartialToolCallSnapshot {
    pub id: String,
    pub tool_type: String,
    pub name: String,
    pub arguments: String,
    pub index: Option<u32>,
}

/// Crate-private interrupted-stream payload.  Keeping this separate from the
/// public successful [`StreamHandlingOutput`] avoids a source-breaking public
/// field while retaining fragments that finalization intentionally drops or
/// normalizes.
pub(crate) struct InterruptedStreamOutput {
    pub content: String,
    pub reasoning_content: String,
    pub partial_tool_calls: Vec<PartialToolCallSnapshot>,
}

impl From<&bamboo_agent_core::tools::PartialToolCall> for PartialToolCallSnapshot {
    fn from(value: &bamboo_agent_core::tools::PartialToolCall) -> Self {
        Self {
            id: value.id.clone(),
            tool_type: value.tool_type.clone(),
            name: value.name.clone(),
            arguments: value.arguments.clone(),
            index: value.index,
        }
    }
}

/// A stream failure together with every semantic fragment accumulated before
/// the failure.  The agent round uses this to create a durable, explicitly
/// interrupted assistant record instead of losing already-visible output.
pub(crate) struct StreamHandlingFailure {
    pub error: AgentError,
    pub partial_output: InterruptedStreamOutput,
}

pub async fn consume_llm_stream(
    stream: LLMStream,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    consume_llm_stream_with_context(
        stream,
        event_tx,
        cancel_token,
        session_id,
        &StreamTimeoutContext::default(),
    )
    .await
}

pub async fn consume_llm_stream_with_context(
    stream: LLMStream,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, AgentError> {
    consume::consume_llm_stream_internal(
        stream,
        Some(event_tx),
        cancel_token,
        session_id,
        timeout_context,
    )
    .await
}

pub(crate) async fn consume_llm_stream_with_context_and_partial(
    stream: LLMStream,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, StreamHandlingFailure> {
    consume::consume_llm_stream_internal_with_partial(
        stream,
        Some(event_tx),
        cancel_token,
        session_id,
        timeout_context,
    )
    .await
}

pub async fn consume_llm_stream_silent(
    stream: LLMStream,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    consume_llm_stream_silent_with_context(
        stream,
        cancel_token,
        session_id,
        &StreamTimeoutContext::default(),
    )
    .await
}

pub async fn consume_llm_stream_silent_with_context(
    stream: LLMStream,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, AgentError> {
    consume::consume_llm_stream_internal(stream, None, cancel_token, session_id, timeout_context)
        .await
}

pub(crate) async fn consume_llm_stream_silent_with_context_and_partial(
    stream: LLMStream,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, StreamHandlingFailure> {
    consume::consume_llm_stream_internal_with_partial(
        stream,
        None,
        cancel_token,
        session_id,
        timeout_context,
    )
    .await
}

#[cfg(test)]
mod tests;
