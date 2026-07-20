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
    pub input_tokens: u64,
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

#[cfg(test)]
mod tests;
