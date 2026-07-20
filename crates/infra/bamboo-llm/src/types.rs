use bamboo_domain::ToolCall;

#[derive(Debug, Clone)]
pub enum LLMChunk {
    /// A valid provider transport frame that intentionally carried no semantic
    /// model output (for example an SSE ping or lifecycle event).
    ///
    /// Consumers must not expose or persist this marker. It exists so stream
    /// watchdogs can distinguish a live connection from a silent socket even
    /// when provider parsers filter the frame's payload (#618).
    TransportActivity,
    ResponseId(String),
    Token(String),
    ReasoningToken(String),
    /// Provider-minted cryptographic signature covering the turn's accumulated
    /// reasoning text (Anthropic `signature_delta`). Emitted once, after the
    /// turn's single `thinking` block closes; an EMPTY string is an
    /// invalidation marker (the turn produced multiple thinking blocks or a
    /// `redacted_thinking` block, so no single signature covers the
    /// accumulated reasoning and any previously captured one must be
    /// discarded). Consumers that don't replay thinking ignore this. (#520)
    ReasoningSignature(String),
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
    /// Whether this chunk advances model-authored content or tool state.
    /// Protocol metadata, usage, completion, and transport-only markers are
    /// deliberately excluded from semantic-progress deadlines.
    pub fn is_semantic_progress(&self) -> bool {
        match self {
            Self::Token(value) | Self::ReasoningToken(value) => !value.is_empty(),
            Self::ToolCalls(calls) => !calls.is_empty(),
            Self::ToolCallsIndexed(calls) => !calls.is_empty(),
            Self::TransportActivity
            | Self::ResponseId(_)
            | Self::ReasoningSignature(_)
            | Self::CacheUsage { .. }
            | Self::UsageSummary { .. }
            | Self::Done => false,
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
    fn transport_activity_is_not_semantic_progress() {
        assert!(!LLMChunk::TransportActivity.is_semantic_progress());
        assert!(!LLMChunk::ResponseId("resp_123".to_string()).is_semantic_progress());
        assert!(LLMChunk::ReasoningToken("thinking".to_string()).is_semantic_progress());
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
