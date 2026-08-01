use std::time::SystemTime;

use bamboo_llm::api::models::{
    ChatCompletionStreamChunk, StreamChoice, StreamDelta, StreamFunctionCall, StreamToolCall,
};

pub(super) fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Build a chat-completion tool-call chunk carrying deltas at the GIVEN indices.
///
/// The `index` must be the provider's own tool-call index (from
/// `LLMChunk::ToolCallsIndexed`), or the positional index for providers that
/// don't carry one. A downstream OpenAI-compatible client keys argument-fragment
/// reassembly on this index, so re-deriving it per chunk via `.enumerate()`
/// yields 0 for a single-fragment chunk and corrupts interleaved multi-tool-call
/// streams. #379.
pub(crate) fn tool_call_stream_chunk(
    indexed: Vec<(u32, bamboo_agent_core::tools::ToolCall)>,
    model: &str,
) -> ChatCompletionStreamChunk {
    let stream_tool_calls: Vec<StreamToolCall> = indexed
        .into_iter()
        .map(|(index, tool_call)| StreamToolCall {
            index,
            id: Some(tool_call.id),
            tool_type: Some(tool_call.tool_type),
            function: Some(StreamFunctionCall {
                name: Some(tool_call.function.name),
                arguments: Some(tool_call.function.arguments),
            }),
        })
        .collect();
    ChatCompletionStreamChunk {
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
    }
}

pub(super) fn convert_chunk_to_openai(
    chunk: bamboo_llm::types::LLMChunk,
    model: &str,
) -> Option<ChatCompletionStreamChunk> {
    match chunk {
        bamboo_llm::types::LLMChunk::ResponseId(_) => None,
        bamboo_llm::types::LLMChunk::Token(text) => Some(ChatCompletionStreamChunk {
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
        // Preserve the provider's REAL tool-call index so a downstream client can
        // route interleaved argument fragments to the right call. Re-deriving it
        // via `.enumerate()` over a single-fragment chunk always yields 0 and
        // silently corrupts multi-tool-call streams. #379.
        bamboo_llm::types::LLMChunk::ToolCallsIndexed(tool_calls) => {
            Some(tool_call_stream_chunk(tool_calls, model))
        }
        bamboo_llm::types::LLMChunk::ToolCalls(tool_calls) => {
            // No per-fragment index carried (Gemini / Responses API) → the
            // positional index within the chunk is the correct value.
            let indexed = tool_calls
                .into_iter()
                .enumerate()
                .map(|(i, call)| (i as u32, call))
                .collect();
            Some(tool_call_stream_chunk(indexed, model))
        }
        bamboo_llm::types::LLMChunk::ReasoningToken(_) => None,
        bamboo_llm::types::LLMChunk::Done => Some(ChatCompletionStreamChunk {
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
        bamboo_llm::types::LLMChunk::TransportActivity
        | bamboo_llm::types::LLMChunk::ResponsesEvent { .. }
        | bamboo_llm::types::LLMChunk::CacheUsage { .. }
        | bamboo_llm::types::LLMChunk::ProviderUsage { .. }
        | bamboo_llm::types::LLMChunk::UsageSummary { .. }
        | bamboo_llm::types::LLMChunk::ReasoningSignature(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{convert_chunk_to_openai, now_unix_ts};
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_llm::types::LLMChunk;

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
    fn indexed_tool_calls_preserve_provider_index() {
        // #379: a `ToolCallsIndexed` chunk must re-serialize with the PROVIDER's
        // real index, not a per-chunk positional `.enumerate()` (which yields 0
        // for a single-fragment chunk and corrupts multi-call streams downstream).
        let chunk = convert_chunk_to_openai(
            LLMChunk::ToolCallsIndexed(vec![
                (3, tool_call("call_a", "a", "{}")),
                (7, tool_call("call_b", "b", "{}")),
            ]),
            "gpt-5",
        )
        .expect("indexed tool-calls chunk should convert");

        let stream_calls = chunk.choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(stream_calls[0].index, 3, "real provider index preserved");
        assert_eq!(stream_calls[0].id.as_deref(), Some("call_a"));
        assert_eq!(stream_calls[1].index, 7, "second index preserved, not 1");
        assert_eq!(stream_calls[1].id.as_deref(), Some("call_b"));

        // A single-fragment indexed chunk for call #1 must NOT collapse to 0.
        let single = convert_chunk_to_openai(
            LLMChunk::ToolCallsIndexed(vec![(1, tool_call("c", "c", ""))]),
            "gpt-5",
        )
        .unwrap();
        assert_eq!(
            single.choices[0].delta.tool_calls.as_ref().unwrap()[0].index,
            1,
            "single-fragment index must stay 1, not enumerate to 0"
        );
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
