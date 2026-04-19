use std::time::SystemTime;

use bamboo_infrastructure::api::models::{
    ChatCompletionStreamChunk, StreamChoice, StreamDelta, StreamFunctionCall, StreamToolCall,
};

pub(super) fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub(super) fn convert_chunk_to_openai(
    chunk: bamboo_infrastructure::types::LLMChunk,
    model: &str,
) -> Option<ChatCompletionStreamChunk> {
    match chunk {
        bamboo_infrastructure::types::LLMChunk::ResponseId(_) => None,
        bamboo_infrastructure::types::LLMChunk::Token(text) => Some(ChatCompletionStreamChunk {
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
        bamboo_infrastructure::types::LLMChunk::ToolCalls(tool_calls) => {
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
        bamboo_infrastructure::types::LLMChunk::ReasoningToken(_) => None,
        bamboo_infrastructure::types::LLMChunk::Done => Some(ChatCompletionStreamChunk {
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

#[cfg(test)]
mod tests {
    use super::{convert_chunk_to_openai, now_unix_ts};
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_infrastructure::types::LLMChunk;

    fn tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn returns_current_unix_timestamp() {
        assert!(now_unix_ts() > 0);
    }

    #[test]
    fn converts_token_chunk_to_openai_stream_chunk() {
        let chunk = convert_chunk_to_openai(LLMChunk::Token("hello".to_string()), "gpt-5")
            .expect("token chunk should convert");

        assert!(chunk.id.starts_with("chatcmpl-"));
        assert_eq!(chunk.object.as_deref(), Some("chat.completion.chunk"));
        assert!(chunk.created > 0);
        assert_eq!(chunk.model.as_deref(), Some("gpt-5"));
        assert_eq!(chunk.choices.len(), 1);
        assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("hello"));
        assert!(chunk.choices[0].delta.tool_calls.is_none());
        assert!(chunk.choices[0].finish_reason.is_none());
        assert!(chunk.usage.is_none());
    }

    #[test]
    fn converts_tool_calls_chunk_to_openai_stream_chunk() {
        let tool_calls = vec![
            tool_call("call_1", "read_file", r#"{"path":"README.md"}"#),
            tool_call(
                "call_2",
                "write_file",
                r#"{"path":"out.txt","content":"ok"}"#,
            ),
        ];
        let chunk = convert_chunk_to_openai(LLMChunk::ToolCalls(tool_calls), "gpt-5")
            .expect("tool-calls chunk should convert");

        let stream_calls = chunk.choices[0]
            .delta
            .tool_calls
            .as_ref()
            .expect("tool calls should be present");
        assert_eq!(stream_calls.len(), 2);
        assert_eq!(stream_calls[0].index, 0);
        assert_eq!(stream_calls[0].id.as_deref(), Some("call_1"));
        assert_eq!(stream_calls[0].tool_type.as_deref(), Some("function"));
        assert_eq!(
            stream_calls[0]
                .function
                .as_ref()
                .and_then(|function| function.name.as_deref()),
            Some("read_file")
        );
        assert_eq!(
            stream_calls[1]
                .function
                .as_ref()
                .and_then(|function| function.arguments.as_deref()),
            Some(r#"{"path":"out.txt","content":"ok"}"#)
        );
    }

    #[test]
    fn returns_none_for_reasoning_token_chunk() {
        let chunk = convert_chunk_to_openai(LLMChunk::ReasoningToken("think".to_string()), "gpt-5");
        assert!(chunk.is_none());
    }

    #[test]
    fn converts_done_chunk_with_stop_finish_reason() {
        let chunk =
            convert_chunk_to_openai(LLMChunk::Done, "gpt-5").expect("done chunk should convert");

        assert_eq!(chunk.choices.len(), 1);
        assert!(chunk.choices[0].delta.content.is_none());
        assert!(chunk.choices[0].delta.tool_calls.is_none());
        assert_eq!(chunk.choices[0].finish_reason.as_deref(), Some("stop"));
    }
}
