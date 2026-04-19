use bamboo_infrastructure::api::models::{ChatCompletionStreamChunk, StreamChoice, StreamDelta};

use super::{map_completion_stream_chunk, AnthropicStreamState};

#[test]
fn map_completion_stream_chunk_includes_stop_reason_mapping() {
    let chunk = ChatCompletionStreamChunk {
        id: "chunk-1".to_string(),
        object: Some("chat.completion.chunk".to_string()),
        created: 1,
        model: Some("gpt-4.1".to_string()),
        choices: vec![StreamChoice {
            index: 0,
            delta: StreamDelta {
                role: None,
                content: None,
                tool_calls: None,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: None,
    };

    let payload = map_completion_stream_chunk(&chunk, "claude-3-5-sonnet");
    assert!(payload.contains("data:"));
    assert!(payload.contains("\"stop_reason\":\"stop_sequence\""));
}

#[test]
fn anthropic_stream_state_finish_is_idempotent() {
    let mut state = AnthropicStreamState::new("claude".to_string());

    let first = state.finish(Some("stop"));
    assert!(first.contains("message_stop"));

    let second = state.finish(Some("stop"));
    assert!(second.is_empty());
}

#[test]
fn anthropic_stream_state_starts_message_and_text_block() {
    let mut state = AnthropicStreamState::new("claude".to_string());
    let chunk = ChatCompletionStreamChunk {
        id: "chunk-2".to_string(),
        object: Some("chat.completion.chunk".to_string()),
        created: 1,
        model: Some("gpt-4.1".to_string()),
        choices: vec![StreamChoice {
            index: 0,
            delta: StreamDelta {
                role: None,
                content: Some("hello".to_string()),
                tool_calls: None,
            },
            finish_reason: None,
        }],
        usage: None,
    };

    let payload = state.handle_chunk(&chunk);
    assert!(payload.contains("message_start"));
    assert!(payload.contains("content_block_start"));
    assert!(payload.contains("text_delta"));
}
