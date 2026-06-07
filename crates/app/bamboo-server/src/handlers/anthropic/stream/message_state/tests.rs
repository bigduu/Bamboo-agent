use bamboo_llm::api::models::{
    ChatCompletionStreamChunk, StreamChoice, StreamDelta, StreamFunctionCall, StreamToolCall,
};

use super::AnthropicStreamState;

#[test]
fn message_start_uses_chunk_model_when_state_model_is_empty() {
    let mut state = AnthropicStreamState::new(String::new());
    let chunk = stream_chunk(
        "chunk-1",
        Some("model-from-chunk"),
        StreamDelta {
            role: None,
            content: Some("hello".to_string()),
            tool_calls: None,
        },
        None,
    );

    let payload = state.handle_chunk(&chunk);
    assert!(payload.contains("\"model\":\"model-from-chunk\""));
}

#[test]
fn tool_block_start_waits_for_id_and_name() {
    let mut state = AnthropicStreamState::new("claude".to_string());
    let first = stream_chunk(
        "chunk-1",
        Some("gpt-4.1"),
        StreamDelta {
            role: None,
            content: None,
            tool_calls: Some(vec![StreamToolCall {
                index: 0,
                id: Some("tool-1".to_string()),
                tool_type: Some("function".to_string()),
                function: Some(StreamFunctionCall {
                    name: None,
                    arguments: Some("{".to_string()),
                }),
            }]),
        },
        None,
    );

    let first_payload = state.handle_chunk(&first);
    assert!(!first_payload.contains("\"type\":\"tool_use\""));

    let second = stream_chunk(
        "chunk-2",
        Some("gpt-4.1"),
        StreamDelta {
            role: None,
            content: None,
            tool_calls: Some(vec![StreamToolCall {
                index: 0,
                id: None,
                tool_type: None,
                function: Some(StreamFunctionCall {
                    name: Some("search".to_string()),
                    arguments: Some("\"query\":\"rust\"}".to_string()),
                }),
            }]),
        },
        None,
    );

    let second_payload = state.handle_chunk(&second);
    assert!(second_payload.contains("\"type\":\"tool_use\""));
    assert!(second_payload.contains("\"id\":\"tool-1\""));
    assert!(second_payload.contains("\"name\":\"search\""));
}

#[test]
fn finish_stops_text_and_tool_blocks() {
    let mut state = AnthropicStreamState::new("claude".to_string());
    let chunk = stream_chunk(
        "chunk-3",
        Some("gpt-4.1"),
        StreamDelta {
            role: None,
            content: Some("hello".to_string()),
            tool_calls: Some(vec![StreamToolCall {
                index: 0,
                id: Some("tool-2".to_string()),
                tool_type: Some("function".to_string()),
                function: Some(StreamFunctionCall {
                    name: Some("weather".to_string()),
                    arguments: Some("{\"city\":\"Shanghai\"}".to_string()),
                }),
            }]),
        },
        None,
    );
    state.handle_chunk(&chunk);

    let finish_payload = state.finish(Some("stop"));
    assert!(finish_payload.contains("message_stop"));
    assert_eq!(
        finish_payload.matches("event: content_block_stop").count(),
        2
    );
}

fn stream_chunk(
    id: &str,
    model: Option<&str>,
    delta: StreamDelta,
    finish_reason: Option<&str>,
) -> ChatCompletionStreamChunk {
    ChatCompletionStreamChunk {
        id: id.to_string(),
        object: Some("chat.completion.chunk".to_string()),
        created: 1,
        model: model.map(ToString::to_string),
        choices: vec![StreamChoice {
            index: 0,
            delta,
            finish_reason: finish_reason.map(ToString::to_string),
        }],
        usage: None,
    }
}
