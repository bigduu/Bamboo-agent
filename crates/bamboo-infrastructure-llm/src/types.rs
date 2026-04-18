use bamboo_domain_session::ToolCall;

#[derive(Debug, Clone)]
pub enum LLMChunk {
    ResponseId(String),
    Token(String),
    ReasoningToken(String),
    ToolCalls(Vec<ToolCall>),
    Done,
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
