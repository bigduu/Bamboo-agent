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
//!
//! Gemini's `streamGenerateContent?alt=sse` sends **no** `[DONE]` sentinel — the
//! connection simply closes after the final chunk. A single Gemini event can
//! carry several logical outputs (the final `usageMetadata` folds a
//! prompt-cache hit AND output/thinking token usage into one JSON object), so
//! [`parse_gemini_sse_event`] returns a [`Vec<LLMChunk>`]: it emits every chunk
//! an event carries in order, and the caller flattens them. Nothing is deferred
//! to a later parse call — there is no later call (issue #27).

use crate::provider::{LLMError, Result};
use crate::providers::common::sse::sse_error_is_present;
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
    /// Non-terminal content events may carry partial cumulative metadata, so
    /// only content-free or terminal events consume it; this flag prevents
    /// repeated final metadata from being double-counted.
    cache_usage_emitted: bool,
    /// Whether an [`LLMChunk::UsageSummary`] has already been emitted for this
    /// stream. It follows the same terminal/content-free and emit-once rules.
    usage_summary_emitted: bool,
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
/// (`cachedContentTokenCount`).
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

/// Take usage carried by a Gemini event's `usageMetadata` as an *ordered*
/// sequence of chunks: [`LLMChunk::CacheUsage`] (cache hit) first, then
/// [`LLMChunk::UsageSummary`] (output/thinking).
///
/// Gemini folds both pieces into a single final `usageMetadata`, but
/// `streamGenerateContent?alt=sse` sends no `[DONE]` sentinel — the connection
/// closes right after that final event. Returning both chunks from this one
/// call (rather than deferring the second to a later parse call that never
/// comes) guarantees usage is delivered for cached requests too. Either piece
/// may be absent (or already emitted); only the pieces present are included.
fn take_gemini_event_usage(state: &mut GeminiStreamState, value: &Value) -> Vec<LLMChunk> {
    let cache = take_gemini_cache_usage(state, value);
    let summary = take_gemini_usage_summary(state, value);
    let mut out = Vec::with_capacity(2);
    if let Some(cache) = cache {
        out.push(cache);
    }
    if let Some(summary) = summary {
        out.push(summary);
    }
    out
}

/// Parse a single Gemini SSE event into zero or more [`LLMChunk`]s.
///
/// Gemini sends JSON objects as data, not named events. The `event_type` parameter
/// is typically empty or "message" for Gemini streams.
///
/// Returns:
/// - `Ok(vec![chunk, ..])` for content-bearing events (text, tool calls) and
///   usage events. Only terminal content-bearing events consume cumulative
///   `usageMetadata`; a final event may yield both a `CacheUsage` and a
///   `UsageSummary`. Chunks have deterministic order: non-empty text/reasoning
///   tokens in part order, one `ToolCalls` chunk containing every function call
///   in part order, then `CacheUsage`, then `UsageSummary`
/// - `Ok(vec![])` for non-content events (empty data, metadata already emitted)
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
/// let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
/// ```
pub fn parse_gemini_sse_event(
    state: &mut GeminiStreamState,
    _event_type: &str,
    data: &str,
) -> Result<Vec<LLMChunk>> {
    // Trim whitespace
    let data = data.trim();

    // Empty data or [DONE] signal
    if data.is_empty() {
        return Ok(Vec::new());
    }

    if data == "[DONE]" {
        return Ok(vec![LLMChunk::Done]);
    }

    // Parse the JSON response
    let value: Value = serde_json::from_str(data).map_err(|e| {
        LLMError::Stream(format!("Failed to parse Gemini SSE data: {}: {}", e, data))
    })?;

    // Check for a REAL error in the response. Guard against a benign no-error
    // marker (`{"error": null}` / `""` / `{}`) that some Gemini gateways emit on an
    // otherwise-normal chunk — `get("error")` returns `Some(Null)` for an explicit
    // null, which would otherwise abort a valid stream (#99, mirroring #26).
    let error_field = value.get("error");
    if let Some(error) = error_field.filter(|e| sse_error_is_present(e)) {
        let error_msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown Gemini API error");
        return Err(LLMError::Api(error_msg.to_string()));
    }

    // Extract candidates array
    let candidates = match value.get("candidates").and_then(|c| c.as_array()) {
        Some(candidates) => candidates,
        // No candidates is normally malformed — EXCEPT a standalone benign no-error
        // marker (`{"error": null}`), which carries an `error` key but no content;
        // treat that as a no-op instead of aborting the stream (#99). Route through
        // take_gemini_event_usage so any `usageMetadata` riding on the marker is
        // still emitted (empty when none is present).
        None if error_field.is_some() => return Ok(take_gemini_event_usage(state, &value)),
        None => {
            return Err(LLMError::Stream(format!(
                "Missing candidates in Gemini response: {}",
                data
            )))
        }
    };

    if candidates.is_empty() {
        return Ok(take_gemini_event_usage(state, &value));
    }

    // Get the first candidate (Gemini typically returns one)
    let candidate = &candidates[0];

    // Gemini defines an empty finishReason as "generation has not stopped".
    // Any non-empty reason is terminal (including safety/filter/tool errors, not
    // only STOP/MAX_TOKENS), so only such content-bearing events may consume the
    // cumulative usageMetadata. Content-free usage events remain handled by the
    // early-return paths below.
    let is_terminal_content_event = candidate
        .get("finishReason")
        .and_then(Value::as_str)
        .map(|finish_reason| !finish_reason.is_empty())
        .unwrap_or(false);

    // Extract content
    let content = match candidate.get("content") {
        Some(c) => c,
        None => return Ok(take_gemini_event_usage(state, &value)),
    };

    // Extract parts array
    let parts = match content.get("parts").and_then(|p| p.as_array()) {
        Some(p) => p,
        None => return Ok(take_gemini_event_usage(state, &value)),
    };

    if parts.is_empty() {
        return Ok(take_gemini_event_usage(state, &value));
    }

    // Process EVERY part. A single Gemini chunk can carry multiple parts —
    // notably parallel `functionCall`s, or a thought part alongside text — so
    // reading only `parts[0]` silently drops the rest (e.g. all-but-one of a
    // set of parallel tool calls). Text parts emit tokens in order; all function
    // calls in the chunk are collected into a single `ToolCalls` chunk.
    let mut chunks = Vec::new();
    let mut tool_calls = Vec::new();

    for part in parts {
        // Best-effort thinking signal detection (per part).
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

        // Text content.
        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
            if !text.is_empty() {
                chunks.push(if is_thinking_part {
                    LLMChunk::ReasoningToken(text.to_string())
                } else {
                    LLMChunk::Token(text.to_string())
                });
            }
            continue;
        }

        // Function call (tool call).
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

            let args_str = serde_json::to_string(&args).map_err(|e| {
                LLMError::Stream(format!("Failed to serialize function args: {}", e))
            })?;

            tool_calls.push(ToolCall {
                id: state.generate_tool_id(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: name.to_string(),
                    arguments: args_str,
                },
            });
            continue;
        }

        // Unknown part type, skip it.
    }

    if !tool_calls.is_empty() {
        chunks.push(LLMChunk::ToolCalls(tool_calls));
    }

    if is_terminal_content_event {
        // A final Gemini event can carry both content and cumulative
        // usageMetadata. Preserve every content/tool chunk above, then append
        // emit-once usage in the documented cache-before-summary order before
        // the stream closes. Intermediate cumulative totals are intentionally
        // ignored so they cannot suppress the final totals.
        chunks.extend(take_gemini_event_usage(state, &value));
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect_sse_body(sse_body: String) -> Vec<LLMChunk> {
        use crate::providers::common::sse::llm_stream_from_sse_multi;
        use futures::StreamExt;

        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(sse_body)
                .expect("http response"),
        );

        let mut state = GeminiStreamState::default();
        let mut stream = llm_stream_from_sse_multi(response, move |event, data| {
            parse_gemini_sse_event(&mut state, event, data)
        });

        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("chunk"));
        }
        chunks
    }

    async fn collect_single_sse_event(payload: &str) -> Vec<LLMChunk> {
        collect_sse_body(format!("data: {payload}\n\n")).await
    }

    #[test]
    fn parse_text_chunk() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
            LLMChunk::Token(text) => assert_eq!(text, "Hello"),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }
    }

    #[test]
    fn parse_thought_text_chunk_emits_reasoning_token() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"thought":true,"text":"Thinking..."}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
            LLMChunk::ReasoningToken(text) => assert_eq!(text, "Thinking..."),
            other => panic!("expected LLMChunk::ReasoningToken, got {:?}", other),
        }
        assert!(state.observed_thinking_signal);
        assert_eq!(state.thinking_parts_count, 1);
    }

    #[test]
    fn parse_multiple_function_calls_in_one_chunk() {
        // Parallel tool calls: a single chunk with two functionCall parts must
        // yield BOTH, not just parts[0]. Regression for the parts[0]-only bug.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_weather","args":{"city":"SF"}}},{"functionCall":{"name":"get_time","args":{"tz":"UTC"}}}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        let tool_calls: Vec<_> = chunks
            .iter()
            .flat_map(|c| match c {
                LLMChunk::ToolCalls(tcs) => tcs.clone(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(tool_calls.len(), 2, "both parallel tool calls must survive");
        assert_eq!(tool_calls[0].function.name, "get_weather");
        assert_eq!(tool_calls[1].function.name, "get_time");
        assert_ne!(
            tool_calls[0].id, tool_calls[1].id,
            "tool ids must be distinct"
        );
    }

    #[test]
    fn parse_multipart_text_and_function_call() {
        // A chunk carrying both a text part and a functionCall part must emit
        // both, not silently drop the trailing part.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"Let me check"},{"functionCall":{"name":"search","args":{"q":"rust"}}}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        let has_token = chunks
            .iter()
            .any(|c| matches!(c, LLMChunk::Token(t) if t.as_str() == "Let me check"));
        let tool_calls: Vec<_> = chunks
            .iter()
            .flat_map(|c| match c {
                LLMChunk::ToolCalls(tcs) => tcs.clone(),
                _ => Vec::new(),
            })
            .collect();
        assert!(has_token, "the text part must survive");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "search");
    }

    #[test]
    fn parse_final_text_with_usage_preserves_all_chunks() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"Hello "},{"text":"world"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 3, "text + usage chunks: {chunks:?}");
        assert!(
            matches!(&chunks[0], LLMChunk::Token(text) if text == "Hello "),
            "first text part must be preserved: {chunks:?}"
        );
        assert!(
            matches!(&chunks[1], LLMChunk::Token(text) if text == "world"),
            "second text part must be preserved: {chunks:?}"
        );
        match &chunks[2] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected UsageSummary after text, got {other:?}"),
        }
    }

    #[test]
    fn parse_final_tool_calls_with_usage_preserves_all_chunks() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_weather","args":{"city":"SF"}}},{"functionCall":{"name":"get_time","args":{"tz":"UTC"}}}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"thoughtsTokenCount":1,"totalTokenCount":15}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 2, "tools + usage chunks: {chunks:?}");
        match &chunks[0] {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].function.name, "get_weather");
                assert_eq!(calls[1].function.name, "get_time");
            }
            other => panic!("expected ToolCalls before usage, got {other:?}"),
        }
        match &chunks[1] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 4);
                assert_eq!(*thinking_tokens, 1);
            }
            other => panic!("expected UsageSummary after tools, got {other:?}"),
        }
    }

    #[test]
    fn parse_final_content_cache_and_usage_has_deterministic_emit_once_order() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"text":"done"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1049}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(
            chunks.len(),
            3,
            "content + cache + usage chunks: {chunks:?}"
        );
        assert!(
            matches!(&chunks[0], LLMChunk::Token(text) if text == "done"),
            "content must be first: {chunks:?}"
        );
        match &chunks[1] {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(*cache_read_input_tokens, 555),
            other => panic!("expected CacheUsage after content, got {other:?}"),
        }
        match &chunks[2] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected UsageSummary after cache usage, got {other:?}"),
        }

        // Repeated cumulative metadata must not double count, while new content
        // from the repeated event must still be delivered.
        let repeated = r#"{"candidates":[{"content":{"parts":[{"text":"epilogue"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1049}}"#;
        let repeated_chunks = parse_gemini_sse_event(&mut state, "", repeated).unwrap();
        assert_eq!(
            repeated_chunks.len(),
            1,
            "cumulative metadata must be emit-once: {repeated_chunks:?}"
        );
        assert!(
            matches!(&repeated_chunks[0], LLMChunk::Token(text) if text == "epilogue"),
            "repeated usage metadata must not displace content: {repeated_chunks:?}"
        );
    }

    #[test]
    fn parse_usage_metadata_emits_usage_summary() {
        // Final content-free chunk: Gemini reports cumulative usage here.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected LLMChunk::UsageSummary, got {:?}", other),
        }
    }

    #[test]
    fn parse_usage_metadata_without_thinking_defaults_to_zero() {
        // Non-thinking models omit thoughtsTokenCount entirely.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"totalTokenCount":52}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 0);
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
        assert_eq!(first.len(), 1);
        assert!(
            matches!(first[0], LLMChunk::UsageSummary { .. }),
            "expected UsageSummary on first usageMetadata chunk, got {:?}",
            first
        );

        // A second, identical cumulative usageMetadata chunk must not re-emit.
        let second = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(
            second.is_empty(),
            "UsageSummary must be emitted only once; got {:?}",
            second
        );
    }

    #[test]
    fn parse_usage_metadata_preserves_cache_usage() {
        // A final chunk can carry BOTH a prompt-cache hit and output/thinking
        // usage. Gemini folds them into one `usageMetadata`, so BOTH must be
        // delivered from this single parse call — Gemini's stream sends no
        // [DONE], so there is no later call on which a deferred chunk could
        // surface (issue #27). CacheUsage comes first, UsageSummary second.
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1042}}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(
            chunks.len(),
            2,
            "expected cache + usage chunks, got {:?}",
            chunks
        );

        match &chunks[0] {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(*cache_read_input_tokens, 555),
            other => panic!("expected LLMChunk::CacheUsage first, got {:?}", other),
        }
        match &chunks[1] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected LLMChunk::UsageSummary second, got {:?}", other),
        }

        // A second identical chunk must not re-emit either piece.
        let second = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(
            second.is_empty(),
            "cache + usage must each be emitted only once; got {:?}",
            second
        );
    }

    /// Real-world Gemini termination: the final chunk carries BOTH a
    /// prompt-cache hit and output/thinking usage, then the connection CLOSES
    /// — `streamGenerateContent?alt=sse` sends no `[DONE]` sentinel and no
    /// further event. This drives the SSE stream the way `consume.rs` does
    /// (pull every chunk until the stream is exhausted, with NO synthetic
    /// trailing event) and asserts that BOTH cache reporting AND the usage
    /// summary actually reach the consumer (issue #27 regression guard).
    #[tokio::test]
    async fn final_cache_usage_delivered_when_stream_ends_without_done() {
        use crate::providers::common::sse::llm_stream_from_sse_multi;
        use futures::StreamExt;

        let payload = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1042}}"#;
        // One final event, then the body ends — exactly how Gemini closes the
        // connection, with no trailing `data: [DONE]`.
        let sse_body = format!("data: {payload}\n\n");

        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(sse_body)
                .expect("http response"),
        );

        let mut state = GeminiStreamState::default();
        let mut stream = llm_stream_from_sse_multi(response, move |event, data| {
            parse_gemini_sse_event(&mut state, event, data)
        });

        // Pull until exhausted — no extra trailing event injected.
        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("chunk"));
        }

        assert_eq!(
            chunks.len(),
            2,
            "cache + usage must both be delivered on stream close; got {:?}",
            chunks
        );
        match &chunks[0] {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(*cache_read_input_tokens, 555),
            other => panic!("expected CacheUsage first, got {:?}", other),
        }
        match &chunks[1] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected UsageSummary second, got {:?}", other),
        }
    }

    /// Non-cached Gemini termination: the final chunk carries only output/
    /// thinking usage (no `cachedContentTokenCount`), then the connection
    /// closes. A single UsageSummary must still be delivered (issue #27).
    #[tokio::test]
    async fn final_usage_delivered_when_stream_ends_without_cache() {
        use crate::providers::common::sse::llm_stream_from_sse_multi;
        use futures::StreamExt;

        let payload = r#"{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;
        let sse_body = format!("data: {payload}\n\n");

        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(sse_body)
                .expect("http response"),
        );

        let mut state = GeminiStreamState::default();
        let mut stream = llm_stream_from_sse_multi(response, move |event, data| {
            parse_gemini_sse_event(&mut state, event, data)
        });

        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("chunk"));
        }

        assert_eq!(chunks.len(), 1, "got {:?}", chunks);
        match &chunks[0] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(*output_tokens, 42);
                assert_eq!(*thinking_tokens, 7);
            }
            other => panic!("expected UsageSummary, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn final_text_and_usage_delivered_when_stream_ends_without_done() {
        let payload = r#"{"candidates":[{"content":{"parts":[{"text":"Hello "},{"text":"world"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":42,"thoughtsTokenCount":7,"totalTokenCount":59}}"#;

        let chunks = collect_single_sse_event(payload).await;
        assert_eq!(chunks.len(), 3, "text + usage on stream close: {chunks:?}");
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "Hello "));
        assert!(matches!(&chunks[1], LLMChunk::Token(text) if text == "world"));
        assert!(matches!(
            chunks[2],
            LLMChunk::UsageSummary {
                output_tokens: 42,
                thinking_tokens: 7
            }
        ));
    }

    #[tokio::test]
    async fn final_tool_calls_and_usage_delivered_when_stream_ends_without_done() {
        let payload = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_weather","args":{"city":"SF"}}},{"functionCall":{"name":"get_time","args":{"tz":"UTC"}}}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"thoughtsTokenCount":1,"totalTokenCount":15}}"#;

        let chunks = collect_single_sse_event(payload).await;
        assert_eq!(chunks.len(), 2, "tools + usage on stream close: {chunks:?}");
        match &chunks[0] {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].function.name, "get_weather");
                assert_eq!(calls[1].function.name, "get_time");
            }
            other => panic!("expected ToolCalls before usage, got {other:?}"),
        }
        assert!(matches!(
            chunks[1],
            LLMChunk::UsageSummary {
                output_tokens: 4,
                thinking_tokens: 1
            }
        ));
    }

    #[tokio::test]
    async fn final_content_cache_and_usage_delivered_when_stream_ends_without_done() {
        let payload = r#"{"candidates":[{"content":{"parts":[{"text":"done"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":42,"thoughtsTokenCount":7,"cachedContentTokenCount":555,"totalTokenCount":1049}}"#;

        let chunks = collect_single_sse_event(payload).await;
        assert_eq!(
            chunks.len(),
            3,
            "content + cache + usage on stream close: {chunks:?}"
        );
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "done"));
        assert!(matches!(
            chunks[1],
            LLMChunk::CacheUsage {
                cache_read_input_tokens: 555,
                ..
            }
        ));
        assert!(matches!(
            chunks[2],
            LLMChunk::UsageSummary {
                output_tokens: 42,
                thinking_tokens: 7
            }
        ));
    }

    #[tokio::test]
    async fn cumulative_usage_waits_for_terminal_content_event_without_done() {
        // Gemini may attach monotonically cumulative usageMetadata to every
        // content event. The first event has no finishReason and the second has
        // an explicitly empty one: both are non-terminal and must preserve
        // semantic chunks without consuming their partial 50/100-token totals.
        // The final OTHER reason proves every non-empty finishReason is terminal,
        // not just STOP/MAX_TOKENS. The body then closes without [DONE].
        let sse_body = concat!(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"alpha\"}],\"role\":\"model\"}}],\"usageMetadata\":{\"promptTokenCount\":1000,\"candidatesTokenCount\":50,\"thoughtsTokenCount\":3,\"cachedContentTokenCount\":10,\"totalTokenCount\":1053}}\n",
            "\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"plan\"},{\"functionCall\":{\"name\":\"search\",\"args\":{\"q\":\"rust\"}}}],\"role\":\"model\"},\"finishReason\":\"\"}],\"usageMetadata\":{\"promptTokenCount\":1000,\"candidatesTokenCount\":100,\"thoughtsTokenCount\":8,\"cachedContentTokenCount\":20,\"totalTokenCount\":1108}}\n",
            "\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"omega\"}],\"role\":\"model\"},\"finishReason\":\"OTHER\"}],\"usageMetadata\":{\"promptTokenCount\":1000,\"candidatesTokenCount\":253,\"thoughtsTokenCount\":17,\"cachedContentTokenCount\":55,\"totalTokenCount\":1270}}\n",
            "\n",
        );

        let chunks = collect_sse_body(sse_body.to_string()).await;
        assert_eq!(
            chunks.len(),
            6,
            "only final cumulative usage should be emitted: {chunks:?}"
        );
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "alpha"));
        assert!(matches!(
            &chunks[1],
            LLMChunk::ReasoningToken(text) if text == "plan"
        ));
        match &chunks[2] {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].function.name, "search");
            }
            other => panic!("expected intermediate ToolCalls, got {other:?}"),
        }
        assert!(matches!(&chunks[3], LLMChunk::Token(text) if text == "omega"));
        match &chunks[4] {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(
                *cache_read_input_tokens, 55,
                "partial cache totals must not suppress the final total"
            ),
            other => panic!("expected final CacheUsage after all content, got {other:?}"),
        }
        match &chunks[5] {
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                assert_eq!(
                    *output_tokens, 253,
                    "partial output totals must not suppress the final total"
                );
                assert_eq!(*thinking_tokens, 17);
            }
            other => panic!("expected final UsageSummary after cache usage, got {other:?}"),
        }
    }

    #[test]
    fn parse_empty_data_returns_none() {
        let mut state = GeminiStreamState::default();
        let chunks = parse_gemini_sse_event(&mut state, "", "").unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn parse_done_signal() {
        let mut state = GeminiStreamState::default();
        let chunks = parse_gemini_sse_event(&mut state, "", "[DONE]").unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
            LLMChunk::Done => {}
            other => panic!("expected LLMChunk::Done, got {:?}", other),
        }
    }

    #[test]
    fn parse_function_call() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"search","args":{"q":"test"}}}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
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

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn parse_missing_content_returns_none() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"finishReason":"STOP"}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(chunks.is_empty());
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
    fn benign_null_error_marker_does_not_abort_stream() {
        // Some gateways emit `{"error": null}` (or ""/{}) as a NO-error marker.
        // It must not surface as an API error or abort the stream (#99).
        for data in [
            r#"{"error":null}"#,
            r#"{"error":""}"#,
            r#"{"error":{}}"#,
            r#"{"error":[]}"#,
        ] {
            let mut state = GeminiStreamState::default();
            let result = parse_gemini_sse_event(&mut state, "", data);
            assert!(
                result.is_ok(),
                "benign no-error marker {data:?} must not error, got {result:?}"
            );
            assert!(
                result.unwrap().is_empty(),
                "benign marker {data:?} yields no chunks"
            );
        }
    }

    #[test]
    fn null_error_marker_alongside_content_still_parses_content() {
        // The marker rides on an otherwise-normal chunk: the content is parsed,
        // the null error is ignored.
        let mut state = GeminiStreamState::default();
        let data = r#"{"error":null,"candidates":[{"content":{"parts":[{"text":"hello"}]}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert!(
            chunks
                .iter()
                .any(|c| matches!(c, LLMChunk::Token(t) if t == "hello")),
            "content alongside a null error marker is still parsed: {chunks:?}"
        );
    }

    #[test]
    fn missing_candidates_without_error_key_still_errors() {
        // A genuinely malformed chunk (no candidates AND no error key) must still
        // surface an error — the #99 no-op path is only for benign markers.
        let mut state = GeminiStreamState::default();
        let data = r#"{"usageMetadata":{"totalTokenCount":5}}"#;

        let result = parse_gemini_sse_event(&mut state, "", data);
        assert!(
            result.is_err(),
            "missing candidates with no error key is malformed, got {result:?}"
        );
    }

    #[test]
    fn parse_multipart_text_accumulates() {
        let mut state = GeminiStreamState::default();

        // First chunk
        let data1 = r#"{"candidates":[{"content":{"parts":[{"text":"Hello "}],"role":"model"}}]}"#;
        let chunks1 = parse_gemini_sse_event(&mut state, "", data1).unwrap();
        assert_eq!(chunks1.len(), 1);
        match &chunks1[0] {
            LLMChunk::Token(text) => assert_eq!(text, "Hello "),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }

        // Second chunk
        let data2 = r#"{"candidates":[{"content":{"parts":[{"text":"world!"}],"role":"model"}}]}"#;
        let chunks2 = parse_gemini_sse_event(&mut state, "", data2).unwrap();
        assert_eq!(chunks2.len(), 1);
        match &chunks2[0] {
            LLMChunk::Token(text) => assert_eq!(text, "world!"),
            other => panic!("expected LLMChunk::Token, got {:?}", other),
        }
    }

    #[test]
    fn parse_function_call_with_empty_args() {
        let mut state = GeminiStreamState::default();
        let data = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_time","args":{}}}],"role":"model"}}]}"#;

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
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

        let chunks = parse_gemini_sse_event(&mut state, "", data).unwrap();
        assert_eq!(chunks.len(), 1);

        match &chunks[0] {
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
        let chunks1 = parse_gemini_sse_event(&mut state, "", data1).unwrap();
        assert_eq!(chunks1.len(), 1);

        let data2 = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","args":{}}}],"role":"model"}}]}"#;
        let chunks2 = parse_gemini_sse_event(&mut state, "", data2).unwrap();
        assert_eq!(chunks2.len(), 1);

        let id1 = match &chunks1[0] {
            LLMChunk::ToolCalls(calls) => calls[0].id.clone(),
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        };

        let id2 = match &chunks2[0] {
            LLMChunk::ToolCalls(calls) => calls[0].id.clone(),
            other => panic!("expected LLMChunk::ToolCalls, got {:?}", other),
        };

        assert_ne!(id1, id2);
    }
}
