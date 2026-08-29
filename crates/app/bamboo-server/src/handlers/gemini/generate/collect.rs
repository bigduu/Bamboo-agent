use futures::{Stream, StreamExt};

use bamboo_agent_core::ToolCall;
use bamboo_llm::LLMChunk;

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
            LLMChunk::ResponseId(_)
            | LLMChunk::ResponsesEvent { .. }
            | LLMChunk::ProviderTranscriptItem(_) => {}
            LLMChunk::Token(token) => collected.full_content.push_str(&token),
            LLMChunk::ReasoningToken(_) => {}
            LLMChunk::Done => break,
            // Keep the last tool call batch, matching the original behavior.
            LLMChunk::ToolCalls(calls) => collected.tool_calls = Some(calls),
            // Indexed variant: drop indices, same behavior. #236.
            LLMChunk::ToolCallsIndexed(calls) => {
                collected.tool_calls = Some(calls.into_iter().map(|(_, call)| call).collect())
            }
            LLMChunk::TransportActivity
            | LLMChunk::CacheUsage { .. }
            | LLMChunk::ProviderUsage { .. }
            | LLMChunk::UsageSummary { .. }
            | LLMChunk::ReasoningSignature(_) => {}
        }
    }

    Ok(collected)
}
