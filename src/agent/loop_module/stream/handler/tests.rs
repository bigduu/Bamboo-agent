use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::tools::{FunctionCall, ToolCall};
use crate::agent::core::AgentEvent;
use crate::agent::llm::{LLMChunk, LLMStream};

use super::{consume_llm_stream, consume_llm_stream_silent};

fn build_stream(items: Vec<crate::agent::llm::provider::Result<LLMChunk>>) -> LLMStream {
    Box::pin(stream::iter(items))
}

#[tokio::test]
async fn consume_llm_stream_accumulates_tokens_and_tool_calls() {
    let stream = build_stream(vec![
        Ok(LLMChunk::ResponseId("resp_123".to_string())),
        Ok(LLMChunk::ReasoningToken("thinking".to_string())),
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

    assert_eq!(output.response_id.as_deref(), Some("resp_123"));
    assert_eq!(output.content, "hi");
    assert_eq!(output.reasoning_content, "thinking");
    assert_eq!(output.token_count, 2);
    assert_eq!(output.tool_calls.len(), 1);
    assert_eq!(output.tool_calls[0].function.name, "test_tool");
    assert_eq!(output.tool_calls[0].function.arguments, "{}");

    let reasoning_event = event_rx.recv().await.expect("missing reasoning event");
    assert!(matches!(reasoning_event, AgentEvent::ReasoningToken { .. }));

    let token_event = event_rx.recv().await.expect("missing token event");
    assert!(matches!(token_event, AgentEvent::Token { .. }));
}

#[tokio::test]
async fn consume_llm_stream_silent_does_not_emit_events() {
    let stream = build_stream(vec![
        Ok(LLMChunk::Token("hello".to_string())),
        Ok(LLMChunk::Done),
    ]);

    let output = consume_llm_stream_silent(stream, &CancellationToken::new(), "session-2")
        .await
        .expect("silent stream should succeed");

    assert!(output.response_id.is_none());
    assert_eq!(output.content, "hello");
    assert!(output.reasoning_content.is_empty());
    assert_eq!(output.token_count, 5);
    assert!(output.tool_calls.is_empty());
}
