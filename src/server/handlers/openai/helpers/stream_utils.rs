use bytes::Bytes;
use std::time::SystemTime;

use crate::agent::llm::api::models::{
    ChatCompletionStreamChunk, StreamChoice, StreamDelta, StreamFunctionCall, StreamToolCall,
};

pub(super) fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub(super) fn sse_data(json: &str) -> Bytes {
    Bytes::from(format!("data: {}\n\n", json))
}

pub(super) fn convert_chunk_to_openai(
    chunk: crate::agent::llm::types::LLMChunk,
    model: &str,
) -> Option<ChatCompletionStreamChunk> {
    match chunk {
        crate::agent::llm::types::LLMChunk::Token(text) => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some(text),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        }),
        crate::agent::llm::types::LLMChunk::ToolCalls(tool_calls) => {
            let stream_tool_calls: Vec<StreamToolCall> = tool_calls
                .into_iter()
                .enumerate()
                .map(|(index, tool_call)| StreamToolCall {
                    index: index as u32,
                    id: Some(tool_call.id),
                    tool_type: Some(tool_call.tool_type),
                    function: Some(StreamFunctionCall {
                        name: Some(tool_call.function.name),
                        arguments: Some(tool_call.function.arguments),
                    }),
                })
                .collect();

            Some(ChatCompletionStreamChunk {
                id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
                object: Some("chat.completion.chunk".to_string()),
                created: chrono::Utc::now().timestamp() as u64,
                model: Some(model.to_string()),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: StreamDelta {
                        role: None,
                        content: None,
                        tool_calls: Some(stream_tool_calls),
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
        }
        crate::agent::llm::types::LLMChunk::Done => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
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
        }),
    }
}
