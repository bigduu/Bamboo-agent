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
use bamboo_domain::{FunctionCall, ToolCall};
use serde_json::Value;

/// Stateful parser for Gemini SSE streaming events.
///
/// Tracks partial tool calls by index so we can accumulate arguments across chunks.
#[derive(Default)]
pub struct GeminiStreamState {
    /// Counter for generating unique tool call IDs
    next_tool_id: usize,
    /// Whether stream contained explicit thinking/thought signal.
    pub observed_thinking_signal: bool,
    /// Count of thought parts observed.
    pub thinking_parts_count: usize,
    /// Approximate characters contained in thought text chunks.
    pub thinking_text_chars: usize,
    /// Whether prompt-cache usage has already been emitted for this stream.
    /// Gemini reports `usageMetadata` cumulatively; emitting once (on the final,
    /// content-free chunk) keeps the downstream accumulator from over-counting.
    cache_usage_emitted: bool,
    /// Whether an [`LLMChunk::UsageSummary`] has already been emitted for this
    /// stream. Like cache usage, emitted once from the final `usageMetadata`.
    usage_summary_emitted: bool,
    /// A usage chunk (e.g. [`LLMChunk::UsageSummary`]) deferred from an earlier
    /// content-free chunk, awaiting the next parse call. Gemini folds cache and
    /// output/thinking usage into a single final `usageMetadata`, but a parse
    /// call yields at most one [`LLMChunk`]; the secondary chunk is buffered
    /// here and drained at the top of the next call.
    pending_usage: Option<LLMChunk>,
}

impl GeminiStreamState {
    /// Generate a unique tool call ID.
    fn generate_tool_id(&mut self) -> String {
        let id = format!("gemini_{}", self.next_tool_id);
        self.next_tool_id += 1;
        id
    }
}

/// Emit a [`LLMChunk::CacheUsage`] once, from a Gemini chunk's `usageMetadata`
/// (`cachedContentTokenCount`). Used at content-free return points so cache
/// reporting never displaces actual content tokens.
fn take_gemini_cache_usage(state: &mut GeminiStreamState, value: &Value) -> Option<LLMChunk> {
    if state.cache_usage_emitted {
        return None;
    }
    let chunk = value
        .get("usageMetadata")
        .and_then(crate::cache::cache_usage_from_gemini_usage)?;
    state.cache_usage_emitted = true;
    Some(chunk)
}

/// Emit an [`LLMChunk::UsageSummary`] once, from a Gemini chunk's
/// `usageMetadata`: `candidatesTokenCount` maps to `output_tokens` and
/// `thoughtsTokenCount` maps to `thinking_tokens`. `thoughtsTokenCount` is
/// absent for non-thinking models (or when no thinking occurred), so it
/// defaults to `0`. Returns `None` when no output token count is reported.
///
/// This is the Gemini analogue of the usage emission Anthropic
/// (`message_delta` `usage`) and OpenAI Responses (`response.completed`
/// `usage`) already perform, so downstream cost accounting / budget
/// enforcement works for Gemini too (issue #27).
fn take_gemini_usage_summary(state: &mut GeminiStreamState, value: &Value) -> Option<LLMChunk> {
    if state.usage_summary_emitted {
        return None;
    }
    let usage = value.get("usageMetadata")?;
    let output_tokens = usage.get("candidatesTokenCount").and_then(Value::as_u64)?;
    let thinking_tokens = usage
        .get("thoughtsTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    state.usage_summary_emitted = true;
    Some(LLMChunk::UsageSummary {
        output_tokens,
        thinking_tokens,
    })
}

/// Emit the final usage from a content-free Gemini chunk's `usageMetadata`:
/// the existing [`LLMChunk::CacheUsage`] (cache hit) plus the new
/// [`LLMChunk::UsageSummary`] (output/thinking). Gemini folds both into a
/// single final `usageMetadata`, but a parse call returns at most one chunk, so
/// when both are present the cache chunk is emitted first (unchanged path) and
/// the usage summary is buffered in [`GeminiStreamState::pending_usage`] for
/// the next parse call. Either may be `None`; returns `None` only when neither
/// is reported.
fn take_gemini_final_usage(state: &mut GeminiStreamState, value: &Value) -> Option<LLMChunk> {
    let cache = take_gemini_cache_usage(state, value);
    let summary = take_gemini_usage_summary(state, value);
    match (cache, summary) {
        (Some(cache), Some(summary)) => {
            state.pending_usage = Some(summary);
            Some(cache)
        }
        (Some(chunk), None) | (None, Some(chunk)) => Some(chunk),
        (None, None) => None,
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
/// ```ignore
/// use crate::providers::gemini::{GeminiStreamState, parse_gemini_sse_event};
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
    // Drain any usage chunk deferred from a previous content-free chunk (see
    // `take_gemini_final_usage`) before handling the current event, so a final
    // `usageMetadata` that carries both cache and output/thinking usage still
    // surfaces both chunks.
    if let Some(pending) = state.pending_usage.take() {
        return Ok(Some(pending));
    }

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
    let value: Value = serde_json::from_str(data).map_err(|e| {
        LLMError::Stream(format!("Failed to parse Gemini SSE data: {}: {}", e, data))
    })?;

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
        .ok_or_else(|| {
            LLMError::Stream(format!("Missing candidates in Gemini response: {}", data))
        })?;

    if candidates.is_empty() {
        return Ok(take_gemini_final_usage(state, &value));
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
        None => return Ok(take_gemini_final_usage(state, &value)),
    };

    // Extract parts array
    let parts = match content.get("parts").and_then(|p| p.as_array()) {
        Some(p) => p,
        None => return Ok(None),
    };

    if parts.is_empty() {
        return Ok(take_gemini_final_usage(state, &value));
    }

    // Process the first part (Gemini typically sends one part per chunk)
    let part = &parts[0];

    // Best-effort thinking signal detection.
    let is_thinking_part = part
        .get("thought")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        || part.get("thoughtSignature").is_some()
        || part.get("thinking").is_some();

    if is_thinking_part {
        state.observed_thinking_signal = true;
        state.thinking_parts_count = state.thinking_parts_count.saturating_add(1);
        let text_len = part
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::len)
            .unwrap_or(0);
        state.thinking_text_chars = state.thinking_text_chars.saturating_add(text_len);
    }

    // Check for text content
    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
        if !text.is_empty() {
            if is_thinking_part {
                return Ok(Some(LLMChunk::ReasoningToken(text.to_string())));
            }
            return Ok(Some(LLMChunk::Token(text.to_string())));
        }
        return Ok(None);
    }

    // Check for function call (tool call)
    if let Some(function_call) = part.get("functionCall") {
        let name = function_call
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| {
                LLMError::Stream(format!(
                    "Missing function name in Gemini response: {}",
                    data
                ))
            })?;

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
    fn parse_thought_text_chunk_emits_reasoning_token() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"thought":true,"text":"Thinking..."}],"role":"model"}}]}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ReasoningToken(text) => assert_eq!(text, "Thinking..."),
            other => panic!("expected LLMChunk::ReasoningToken, got {:?}", other),
        }
        assert!(state.observed_thinking_signal);
        assert_eq!(state.thinking_parts_count, 1);
    }

    #[test]
    fn parse_usage_metadata_emits_usage_summary() {
        // Final content-free chunk: Gemini reports cumulative usage here.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(output_tokens, 42);
                assert_eq!(thinking_tokens, 7);
            }
            other => panic!("expected LLMChunk::UsageSummary, got {:?}", other),
        }
    }

    #[test]
    fn parse_usage_metadata_without_thinking_defaults_to_zero() {
        // Non-thinking models omit thoughtsTokenCount entirely.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"totalTokenCount":52}}"#;

        let chunk = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(output_tokens, 42);
                assert_eq!(thinking_tokens, 0);
            }
            other => panic!("expected LLMChunk::UsageSummary, got {:?}", other),
        }
    }

    #[test]
    fn parse_usage_metadata_emitted_once() {
        // Gemini may echo usageMetadata on more than one chunk; only the first
        // must produce a UsageSummary (the downstream accumulator would otherwise
        // double-count output/thinking tokens).
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;

        let first = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(
            matches!(first, Some(LLMChunk::UsageSummary { .. })),
            "expected UsageSummary on first usageMetadata chunk, got {:?}",
            first
        );

        // A second, identical cumulative usageMetadata chunk must not re-emit.
        let second = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(
            second.is_none(),
            "UsageSummary must be emitted only once; got {:?}",
            second
        );
    }

    #[test]
    fn parse_usage_metadata_preserves_cache_usage() {
        // A final chunk can carry BOTH a prompt-cache hit and output/thinking
        // usage. Cache reporting (issue #12) must survive the new usage
        // emission: CacheUsage is emitted first, the UsageSummary is buffered
        // and drained on the next parse call.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1042}}"#;

        let cache = parse_gemini_sse_event(&mut state, "", data)
            .unwrap()
            .expect("cache chunk");
        match cache {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(cache_read_input_tokens, 555),
            other => panic!("expected LLMChunk::CacheUsage first, got {:?}", other),
        }

        // The UsageSummary was buffered; it surfaces on the next parse call
        // (here, the trailing [DONE] event).
        let summary = parse_gemini_sse_event(&mut state, "", "[DONE]")
            .unwrap()
            .expect("buffered usage summary");
        match summary {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(output_tokens, 42);
                assert_eq!(thinking_tokens, 7);
            }
            other => panic!("expected buffered LLMChunk::UsageSummary, got {:?}", other),
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
