use futures::{Stream, StreamExt};

use bamboo_agent_core::ToolCall;
use bamboo_infrastructure::LLMChunk;

#[derive(Debug, Default, PartialEq)]
pub(super) struct CollectedResponse {
    pub(super) full_content: String,
    pub(super) tool_calls: Option<Vec<ToolCall>>,
}

pub(super) async fn collect_response_chunks<S, E>(stream: &mut S) -> Result<CollectedResponse, E>
where
    S: Stream<Item = Result<LLMChunk, E>> + Unpin,
{
    let mut collected = CollectedResponse::default();

    while let Some(chunk) = stream.next().await {
        match chunk? {
            LLMChunk::ResponseId(_) => {}
            LLMChunk::Token(token) => collected.full_content.push_str(&token),
            LLMChunk::ReasoningToken(_) => {}
            LLMChunk::Done => break,
            // Keep the last tool call batch, matching the original behavior.
            LLMChunk::ToolCalls(calls) => collected.tool_calls = Some(calls),
        }
    }

    Ok(collected)
}
