use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, AgentEvent, StreamTimeoutError, StreamTimeoutPhase};
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
    request_started_at: Option<Instant>,
    turn_retry_eligible: bool,
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
            request_started_at: None,
            turn_retry_eligible: false,
        }
    }

    /// Mark this as the primary turn response. Only that stream can safely ask
    /// the outer agent loop to replay a timeout that occurs before semantic
    /// output; auxiliary streams may run after durable turn side effects.
    pub(crate) fn allow_turn_retry_before_semantic_output(mut self) -> Self {
        self.turn_retry_eligible = true;
        self
    }

    /// Bind a fresh request-dispatch timestamp so the first-semantic deadline
    /// includes provider bootstrap time as documented.
    pub(crate) fn begin_request(mut self) -> Self {
        self.request_started_at = Some(Instant::now());
        self
    }

    fn timeout_error(
        &self,
        session_id: &str,
        phase: StreamTimeoutPhase,
        deadline: Duration,
        last_transport: Duration,
        last_semantic: Option<Duration>,
    ) -> AgentError {
        let timeout = StreamTimeoutError::new(
            phase,
            deadline,
            self.provider.clone(),
            self.model.clone(),
            last_transport,
            last_semantic,
            self.turn_retry_eligible,
        );
        tracing::warn!("[{}] LLM stream watchdog expired: {}", session_id, timeout,);
        AgentError::StreamTimeout(timeout)
    }
}

impl Default for StreamTimeoutContext {
    fn default() -> Self {
        Self::new(StreamTimeoutConfig::default(), None, None)
    }
}

/// Wait for a provider call to establish its response stream while preserving
/// cancellation and the existing transport-idle policy.
///
/// Provider implementations return [`LLMStream`] only after the initial HTTP
/// response has been established. Without this outer watchdog, a proxy that
/// accepts the request but never returns response headers can hold the agent
/// forever before the normal per-frame stream watchdog starts.
pub(crate) async fn await_stream_bootstrap<F, T>(
    future: F,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<T, AgentError>
where
    F: Future<Output = T>,
{
    let started_at = timeout_context
        .request_started_at
        .unwrap_or_else(Instant::now);
    let deadline = Duration::from_secs(timeout_context.policy.transport_idle_timeout_secs);
    let expires_at = started_at + deadline;

    tokio::select! {
        biased;
        _ = cancel_token.cancelled() => Err(AgentError::Cancelled),
        result = future => Ok(result),
        _ = tokio::time::sleep_until(expires_at) => {
            let now = Instant::now();
            Err(timeout_context.timeout_error(
                session_id,
                StreamTimeoutPhase::Bootstrap,
                deadline,
                now.saturating_duration_since(started_at),
                None,
            ))
        }
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
