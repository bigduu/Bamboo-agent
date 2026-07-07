use bamboo_domain::ToolCall;

#[derive(Debug, Clone)]
pub enum LLMChunk {
    ResponseId(String),
    Token(String),
    ReasoningToken(String),
    ToolCalls(Vec<ToolCall>),
    /// Tool-call deltas that carry the provider's `index` field, so the engine
    /// accumulator can route argument-only continuation fragments to the correct
    /// call even when an upstream/aggregator interleaves fragments across indices.
    ///
    /// The chat-completions path (`parse_openai_compat_chunk`) emits this instead
    /// of [`LLMChunk::ToolCalls`] because every OpenAI-compatible tool-call delta
    /// carries an `index`. Providers whose wire format has no per-fragment index
    /// (Gemini, the Responses API, etc.) keep using [`LLMChunk::ToolCalls`] and its
    /// positional accumulation. `u32` is the tool-call index; the paired
    /// [`ToolCall`] is the (possibly partial) delta. #236.
    ToolCallsIndexed(Vec<(u32, ToolCall)>),
    /// Anthropic prompt cache token usage from `message_start` or `message_delta`.
    CacheUsage {
        cache_creation_input_tokens: u64,
        cache_read_input_tokens: u64,
        /// Non-cached "fresh" input tokens billed at the base rate — disjoint
        /// from the cache read/creation counts. With all three, the precise
        /// prompt size is `input + cache_read + cache_creation` and the exact
        /// cache-hit ratio is `cache_read / that_sum`. `0` when the provider
        /// does not report it on this event.
        input_tokens: u64,
    },
    /// Token usage summary at the end of an Anthropic response.
    UsageSummary {
        output_tokens: u64,
        thinking_tokens: u64,
    },
    Done,
}

impl LLMChunk {
    /// Collapse [`LLMChunk::ToolCallsIndexed`] into [`LLMChunk::ToolCalls`],
    /// dropping the per-call indices.
    ///
    /// The indexed variant exists so the engine's streaming accumulator can route
    /// argument fragments by index (#236). Consumers that only need the flattened
    /// tool calls — the bamboo-server proxy handlers that re-serialize chunks to a
    /// downstream client, and any collector that does its own or no accumulation —
    /// call this to treat both variants uniformly. The engine accumulator handles
    /// the indexed variant directly and does NOT use this.
    pub fn normalize_tool_calls(self) -> Self {
        match self {
            LLMChunk::ToolCallsIndexed(calls) => {
                LLMChunk::ToolCalls(calls.into_iter().map(|(_, call)| call).collect())
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_llm_chunk_token() {
        let chunk = LLMChunk::Token("Hello".to_string());
        match chunk {
            LLMChunk::Token(s) => assert_eq!(s, "Hello"),
            _ => panic!("Expected Token variant"),
        }
    }

    #[test]
    fn test_llm_chunk_reasoning_token() {
        let chunk = LLMChunk::ReasoningToken("Thinking...".to_string());
        match chunk {
            LLMChunk::ReasoningToken(s) => assert_eq!(s, "Thinking..."),
            _ => panic!("Expected ReasoningToken variant"),
        }
    }

    #[test]
    fn normalize_tool_calls_flattens_indexed_variant() {
        let tc = |id: &str| ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: bamboo_domain::FunctionCall {
                name: "f".to_string(),
                arguments: String::new(),
            },
        };
        let chunk = LLMChunk::ToolCallsIndexed(vec![(1, tc("b")), (0, tc("a"))]);
        match chunk.normalize_tool_calls() {
            // Order is preserved (indices only dropped), not re-sorted.
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].id, "b");
                assert_eq!(calls[1].id, "a");
            }
            other => panic!("expected ToolCalls, got {other:?}"),
        }

        // Non-tool-call chunks pass through untouched.
        assert!(matches!(
            LLMChunk::Token("x".to_string()).normalize_tool_calls(),
            LLMChunk::Token(_)
        ));
    }

    #[test]
    fn test_llm_chunk_response_id() {
        let chunk = LLMChunk::ResponseId("resp_123".to_string());
        match chunk {
            LLMChunk::ResponseId(id) => assert_eq!(id, "resp_123"),
            _ => panic!("Expected ResponseId variant"),
        }
    }

    #[test]
    fn test_llm_chunk_tool_calls() {
        let chunk = LLMChunk::ToolCalls(vec![]);
        match chunk {
            LLMChunk::ToolCalls(calls) => assert!(calls.is_empty()),
            _ => panic!("Expected ToolCalls variant"),
        }
    }

    #[test]
    fn test_llm_chunk_done() {
        let chunk = LLMChunk::Done;
        match chunk {
            LLMChunk::Done => (),
            _ => panic!("Expected Done variant"),
        }
    }

    #[test]
    fn test_llm_chunk_clone() {
        let chunk1 = LLMChunk::Token("test".to_string());
        let chunk2 = chunk1.clone();
        match (chunk1, chunk2) {
            (LLMChunk::Token(s1), LLMChunk::Token(s2)) => assert_eq!(s1, s2),
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_llm_chunk_debug() {
        let chunk = LLMChunk::Token("test".to_string());
        let debug_str = format!("{:?}", chunk);
        assert!(debug_str.contains("Token"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_llm_chunk_debug_response_id() {
        let chunk = LLMChunk::ResponseId("resp_123".to_string());
        let debug_str = format!("{:?}", chunk);
        assert!(debug_str.contains("ResponseId"));
        assert!(debug_str.contains("resp_123"));
    }

    #[test]
    fn test_llm_chunk_debug_reasoning() {
        let chunk = LLMChunk::ReasoningToken("thinking".to_string());
        let debug_str = format!("{:?}", chunk);
        assert!(debug_str.contains("ReasoningToken"));
    }

    #[test]
    fn test_llm_chunk_debug_tool_calls() {
        let chunk = LLMChunk::ToolCalls(vec![]);
        let debug_str = format!("{:?}", chunk);
        assert!(debug_str.contains("ToolCalls"));
    }

    #[test]
    fn test_llm_chunk_debug_done() {
        let chunk = LLMChunk::Done;
        let debug_str = format!("{:?}", chunk);
        assert!(debug_str.contains("Done"));
    }

    #[test]
    fn test_llm_chunk_with_empty_string() {
        let chunk = LLMChunk::Token("".to_string());
        match chunk {
            LLMChunk::Token(s) => assert_eq!(s, ""),
            _ => panic!("Expected Token variant"),
        }
    }

    #[test]
    fn test_llm_chunk_with_multiline_string() {
        let chunk = LLMChunk::Token("Line1\nLine2\nLine3".to_string());
        match chunk {
            LLMChunk::Token(s) => assert!(s.contains("\n")),
            _ => panic!("Expected Token variant"),
        }
    }
}
