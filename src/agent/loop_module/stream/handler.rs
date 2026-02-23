use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use agent_core::tools::{ToolCall, ToolCallAccumulator};
use agent_core::{AgentError, AgentEvent};
use agent_llm::{LLMChunk, LLMStream};

pub struct StreamHandlingOutput {
    pub content: String,
    pub token_count: usize,
    pub tool_calls: Vec<ToolCall>,
}

pub async fn consume_llm_stream(
    mut stream: LLMStream,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
) -> Result<StreamHandlingOutput, AgentError> {
    let mut content = String::new();
    let mut token_count = 0usize;
    let mut tool_calls = ToolCallAccumulator::new();

    while let Some(chunk_result) = stream.next().await {
        if cancel_token.is_cancelled() {
            return Err(AgentError::Cancelled);
        }

        match chunk_result {
            Ok(LLMChunk::Token(token)) => {
                token_count += token.len();
                content.push_str(&token);

                let _ = event_tx
                    .send(AgentEvent::Token {
                        content: token.clone(),
                    })
                    .await;
            }
            Ok(LLMChunk::ToolCalls(partial_calls)) => {
                log::debug!(
                    "[{}] Received {} tool call parts",
                    session_id,
                    partial_calls.len()
                );
                tool_calls.extend(partial_calls);
            }
            Ok(LLMChunk::Done) => {
                log::debug!("[{}] LLM stream completed", session_id);
            }
            Err(error) => {
                let message = format!("Stream error: {error}");
                let _ = event_tx
                    .send(AgentEvent::Error {
                        message: message.clone(),
                    })
                    .await;
                return Err(AgentError::LLM(error.to_string()));
            }
        }
    }

    Ok(StreamHandlingOutput {
        content,
        token_count,
        tool_calls: tool_calls.finalize(),
    })
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use agent_core::tools::{FunctionCall, ToolCall};
    use agent_core::AgentEvent;
    use agent_llm::LLMStream;

    use super::*;

    fn build_stream(items: Vec<agent_llm::provider::Result<LLMChunk>>) -> LLMStream {
        Box::pin(stream::iter(items))
    }

    #[tokio::test]
    async fn consume_llm_stream_accumulates_tokens_and_tool_calls() {
        let stream = build_stream(vec![
            Ok(LLMChunk::Token("hi".to_string())),
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: "call_1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "test_tool".to_string(),
                    arguments: "{".to_string(),
                },
            }])),
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: "call_1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: String::new(),
                    arguments: "}".to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ]);

        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
        let output = consume_llm_stream(stream, &event_tx, &CancellationToken::new(), "session-1")
            .await
            .expect("stream should succeed");

        assert_eq!(output.content, "hi");
        assert_eq!(output.token_count, 2);
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].function.name, "test_tool");
        assert_eq!(output.tool_calls[0].function.arguments, "{}");

        let token_event = event_rx.recv().await.expect("missing token event");
        assert!(matches!(token_event, AgentEvent::Token { .. }));
    }
}
