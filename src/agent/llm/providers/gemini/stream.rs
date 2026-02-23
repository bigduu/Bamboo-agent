//! Gemini SSE stream parser.
//!
//! Gemini uses a simple SSE format where each event is a JSON object:
//! ```text
//! data: {"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"}}]}
//!
//! data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"search","args":{"q":"test"}}}],"role":"model"}}]}
//!
//! data: [DONE]
//! ```

use crate::provider::{LLMError, Result};
use crate::types::LLMChunk;
use agent_core::tools::{FunctionCall, ToolCall};
use serde_json::Value;

/// Stateful parser for Gemini SSE streaming events.
///
/// Tracks partial tool calls by index so we can accumulate arguments across chunks.
#[derive(Default)]
pub struct GeminiStreamState {
    /// Counter for generating unique tool call IDs
    next_tool_id: usize,
}

impl GeminiStreamState {
    /// Generate a unique tool call ID.
    fn generate_tool_id(&mut self) -> String {
        let id = format!("gemini_{}", self.next_tool_id);
        self.next_tool_id += 1;
        id
    }
}

/// Parse a single Gemini SSE event into an optional [`LLMChunk`].
///
/// Gemini sends JSON objects as data, not named events. The `event_type` parameter
/// is typically empty or "message" for Gemini streams.
///
/// Returns:
/// - `Ok(Some(chunk))` for content-bearing events (text, tool calls)
/// - `Ok(None)` for non-content events (empty data, metadata)
/// - `Err(_)` for malformed JSON or unexpected shapes
///
/// # Example
///
/// ```
/// use agent_llm::providers::gemini::{GeminiStreamState, parse_gemini_sse_event};
///
/// let mut state = GeminiStreamState::default();
/// let data = r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"}}]}"#;
///
/// let chunk = parse_gemini_sse_event(&mut state, "", data).unwrap();
/// ```
pub fn parse_gemini_sse_event(
    state: &mut GeminiStreamState,
    _event_type: &str,
    data: &str,
) -> Result<Option<LLMChunk>> {
    // Trim whitespace
    let data = data.trim();

    // Empty data or [DONE] signal
    if data.is_empty() {
        return Ok(None);
    }

    if data == "[DONE]" {
        return Ok(Some(LLMChunk::Done));
    }

    // Parse the JSON response
    let value: Value = serde_json::from_str(data)
        .map_err(|e| LLMError::Stream(format!("Failed to parse Gemini SSE data: {}: {}", e, data)))?;

    // Check for error in the response
    if let Some(error) = value.get("error") {
        let error_msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown Gemini API error");
        return Err(LLMError::Api(error_msg.to_string()));
    }

    // Extract candidates array
    let candidates = value
        .get("candidates")
        .and_then(|c| c.as_array())
        .ok_or_else(|| LLMError::Stream(format!("Missing candidates in Gemini response: {}", data)))?;

    if candidates.is_empty() {
        return Ok(None);
    }

    // Get the first candidate (Gemini typically returns one)
    let candidate = &candidates[0];

    // Check for finish reason
    if let Some(finish_reason) = candidate.get("finishReason").and_then(|f| f.as_str()) {
        if finish_reason == "STOP" || finish_reason == "MAX_TOKENS" {
            // Still need to process any content, but this might be the last chunk
        }
    }

    // Extract content
    let content = match candidate.get("content") {
        Some(c) => c,
        None => return Ok(None),
    };

    // Extract parts array
    let parts = match content.get("parts").and_then(|p| p.as_array()) {
        Some(p) => p,
        None => return Ok(None),
    };

    if parts.is_empty() {
        return Ok(None);
    }

    // Process the first part (Gemini typically sends one part per chunk)
    let part = &parts[0];

    // Check for text content
    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
        if !text.is_empty() {
            return Ok(Some(LLMChunk::Token(text.to_string())));
        }
        return Ok(None);
    }

    // Check for function call (tool call)
    if let Some(function_call) = part.get("functionCall") {
        let name = function_call
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| LLMError::Stream(format!("Missing function name in Gemini response: {}", data)))?;

        let args = function_call
            .get("args")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

        let args_str = serde_json::to_string(&args)
            .map_err(|e| LLMError::Stream(format!("Failed to serialize function args: {}", e)))?;

        let tool_id = state.generate_tool_id();

        return Ok(Some(LLMChunk::ToolCalls(vec![ToolCall {
            id: tool_id,
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args_str,
            },
        }])));
    }

    // Unknown part type, skip it
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_chunk() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"}}]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Token(text) => assert_eq!(text, "Hello"),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }
    }

    #[test]
    fn parse_empty_data_returns_none() {
        let mut state = GeminiStreamState::default();
        let chunk = parse_gemini_sse_event(&mut state, "", "").unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn parse_done_signal() {
        let mut state = GeminiStreamState::default();
        let chunk = parse_gemini_sse_event(&mut state, "", "[DONE]")
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Done => {}
            other => panic!("expected LLMChunk::Done, got {:?}", other),
        }
    }

    #[test]
    fn parse_function_call() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"search","args":{"q":"test"}}}],"role":"model"}}]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
                assert!(calls[0].id.starts_with("gemini_"));
            }
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        }
    }

    #[test]
    fn parse_empty_candidates_returns_none() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn parse_missing_content_returns_none() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"finishReason":"STOP"}]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn parse_error_response() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"error":{"message":"API key invalid","code":401}}"#;

        let result = parse_gemini_sse_event(&mut state, "", data);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("API key invalid"));
    }

    #[test]
    fn parse_invalid_json() {
        let mut state = GeminiStreamState::default();
        let data = "{invalid json}";

        let result = parse_gemini_sse_event(&mut state, "", data);
        assert!(result.is_err());
    }

    #[test]
    fn parse_multipart_text_accumulates() {
        let mut state = GeminiStreamState::default();

        // First chunk
        let data1 = r#"{"candidates":[{"content":{"parts":[{"text":"Hello "}],"role":"model"}}]}"#;
        let chunk1 = parse_gemini_sse_event(&mut state, "", data1)
            .unwrap()
            .expect("chunk1");

        match chunk1 {
            LLMChunk::Token(text) => assert_eq!(text, "Hello "),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }

        // Second chunk
        let data2 = r#"{"candidates":[{"content":{"parts":[{"text":"world!"}],"role":"model"}}]}"#;
        let chunk2 = parse_gemini_sse_event(&mut state, "", data2)
            .unwrap()
            .expect("chunk2");

        match chunk2 {
            LLMChunk::Token(text) => assert_eq!(text, "world!"),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }
    }

    #[test]
    fn parse_function_call_with_empty_args() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_time","args":{}}}],"role":"model"}}]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].function.name, "get_time");
                assert_eq!(calls[0].function.arguments, "{}");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        }
    }

    #[test]
    fn parse_whitespace_data_is_trimmed() {
        let mut state = GeminiStreamState::default();
        let data = "   [DONE]   ";

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Done => {}
            other => panic!("expected LLMChunk::Done, got {:?}", other),
        }
    }

    #[test]
    fn state_generates_unique_tool_ids() {
        let mut state = GeminiStreamState::default();

        let id1 = state.generate_tool_id();
        let id2 = state.generate_tool_id();
        let id3 = state.generate_tool_id();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert!(id1.starts_with("gemini_"));
        assert!(id2.starts_with("gemini_"));
        assert!(id3.starts_with("gemini_"));
    }

    #[test]
    fn multiple_function_calls_get_unique_ids() {
        let mut state = GeminiStreamState::default();

        let data1 = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"search","args":{}}}],"role":"model"}}]}"#;
        let chunk1 = parse_gemini_sse_event(&mut state, "", data1)
            .unwrap()
            .expect("chunk1");

        let data2 = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","args":{}}}],"role":"model"}}]}"#;
        let chunk2 = parse_gemini_sse_event(&mut state, "", data2)
            .unwrap()
            .expect("chunk2");

        let id1 = match chunk1 {
            LLMChunk::ToolCalls(calls) => calls[0].id.clone(),
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        };

        let id2 = match chunk2 {
            LLMChunk::ToolCalls(calls) => calls[0].id.clone(),
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        };

        assert_ne!(id1, id2);
    }
}
