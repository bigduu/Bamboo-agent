use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_application_agent::tools::ToolCall;
use bamboo_application_agent::{AgentError, AgentEvent};
use bamboo_infrastructure_llm::LLMStream;

mod chunk_handling;
mod consume;
mod stream_state;

pub struct StreamHandlingOutput {
    pub response_id: Option<String>,
    pub content: String,
    pub reasoning_content: String,
    pub token_count: usize,
    pub tool_calls: Vec<ToolCall>,
}

pub async fn consume_llm_stream(
    stream: LLMStream,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    consume::consume_llm_stream_internal(stream, Some(event_tx), cancel_token, session_id).await
}

pub async fn consume_llm_stream_silent(
    stream: LLMStream,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    consume::consume_llm_stream_internal(stream, None, cancel_token, session_id).await
}

#[cfg(test)]
mod tests;
