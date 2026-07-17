use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::LLMStream;

mod chunk_handling;
mod consume;
mod stream_state;

/// Default *inter-chunk* idle timeout for LLM streams (issue #28).
///
/// A provider connection that stops sending chunks for longer than this is
/// treated as a hung stream. Because the deadline resets on every received
/// chunk (see [`consume::consume_llm_stream_internal`]), a legitimately long
/// stream that keeps producing data — e.g. a thinking model that streams for
/// minutes — is never affected; only a true stall (no chunk at all) trips it.
///
/// This is a well-named const default rather than a threaded config field: the
/// consume function is shared across the main stream path and several
/// auxiliary (silent) callers, and wiring a new field through `AgentLoopConfig`
/// plus every call site is invasive for a streaming hot path. The internal
/// function still accepts an `idle_timeout` override so it remains configurable
/// and testable; full config wiring is deferred.
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

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
    consume::consume_llm_stream_internal(
        stream,
        Some(event_tx),
        cancel_token,
        session_id,
        DEFAULT_STREAM_IDLE_TIMEOUT,
    )
    .await
}

pub async fn consume_llm_stream_silent(
    stream: LLMStream,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    consume::consume_llm_stream_internal(
        stream,
        None,
        cancel_token,
        session_id,
        DEFAULT_STREAM_IDLE_TIMEOUT,
    )
    .await
}

#[cfg(test)]
mod tests;
