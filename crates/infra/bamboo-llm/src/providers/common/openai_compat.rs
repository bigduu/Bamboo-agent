//! OpenAI-compatible request serialization helpers.
//!
//! Many providers (OpenAI, GitHub Copilot, etc.) accept a request/stream shape that is compatible
//! with OpenAI's chat completions API. These helpers build a "compat" JSON body without leaking
//! internal `bamboo_domain::Message` fields (like `id` / `created_at`).

use bamboo_domain::ToolSchema;
use bamboo_domain::{Message, MessagePart, Role};
use futures::{stream, StreamExt};
use reqwest::Response;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::sse::{llm_stream_from_sse_multi, sse_error_is_present};
use super::tool_schema::sanitize_openai_function_parameters_schema;
use crate::provider::{LLMError, LLMStream, Result};
use crate::types::LLMChunk;
use bamboo_domain::ReasoningEffort;

/// Convert internal [`Message`] values to an OpenAI-compatible JSON array.
///
/// This intentionally omits internal fields like `id` and `created_at`.
///
/// A `role:tool` message can only carry string content on the Chat Completions
/// API — there is no place for an image (e.g. an MCP screenshot) the way the
/// Responses / Anthropic paths have. Rather than silently drop it (#412, sibling
/// of #237 finding 6), we relay any tool-result images as a synthetic `role:user`
/// message. The API also requires that all tool replies for a turn stay
/// CONSECUTIVE before any other role, so the images are buffered and flushed as a
/// single user message once the tool block ends (before the next non-tool
/// message, or at the end) — never interleaved between two tool messages.
pub fn messages_to_openai_compat_json(messages: &[Message]) -> Vec<Value> {
    fn flush_relayed_images(out: &mut Vec<Value>, pending: &mut Vec<Value>) {
        if pending.is_empty() {
            return;
        }
        let mut content = vec![json!({
            "type": "text",
            "text": "Image output from the preceding tool result(s).",
        })];
        content.append(pending);
        out.push(json!({ "role": "user", "content": Value::Array(content) }));
    }

    let mut out: Vec<Value> = Vec::with_capacity(messages.len());
    let mut pending_tool_images: Vec<Value> = Vec::new();

    for m in messages {
        // A non-tool message closes any open tool block: relay buffered images
        // first so they land AFTER the whole block, keeping tool replies contiguous.
        if !matches!(m.role, Role::Tool) {
            flush_relayed_images(&mut out, &mut pending_tool_images);
        }

        let role = match m.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        };

        // OpenAI-compatible APIs accept either a string content, or an array of
        // typed parts for multimodal messages. A `role:tool` message stays a
        // string (its images are relayed separately, below).
        let content_value = if matches!(m.role, Role::Tool) {
            json!(m.content)
        } else if let Some(parts) = m.content_parts.as_ref() {
            json!(parts)
        } else {
            json!(m.content)
        };

        let mut msg = json!({
            "role": role,
            "content": content_value,
        });

        if let Some(tool_call_id) = &m.tool_call_id {
            msg["tool_call_id"] = json!(tool_call_id);
        }

        if let Some(tool_calls) = &m.tool_calls {
            msg["tool_calls"] = json!(tool_calls);
        }

        out.push(msg);

        // Buffer this tool message's images (if any) to relay after the block.
        if matches!(m.role, Role::Tool) {
            if let Some(parts) = m.content_parts.as_ref() {
                for part in parts {
                    if matches!(part, MessagePart::ImageUrl { .. }) {
                        // Reuse MessagePart's serde shape — identical to how user
                        // multimodal images are emitted on this path.
                        pending_tool_images.push(json!(part));
                    }
                }
            }
        }
    }

    // Relay any images from a trailing tool block.
    flush_relayed_images(&mut out, &mut pending_tool_images);
    out
}

/// Convert internal [`ToolSchema`] values to the OpenAI `tools` array JSON.
pub fn tools_to_openai_compat_json(tools: &[ToolSchema]) -> Vec<Value> {
    tools
        .iter()
        .map(|t| {
            json!({
                "type": t.schema_type,
                "function": {
                    "name": t.function.name,
                    "description": t.function.description,
                    "parameters": sanitize_openai_function_parameters_schema(&t.function.parameters),
                }
            })
        })
        .collect()
}

pub(crate) fn set_openai_compat_token_limit(
    body: &mut Value,
    max_output_tokens: Option<u32>,
    reasoning_enabled: bool,
) {
    let Some(max_tokens) = max_output_tokens else {
        return;
    };

    // Reasoning models count hidden reasoning and visible answer tokens in the
    // Chat Completions `max_completion_tokens` limit. The legacy `max_tokens`
    // field is incompatible with o-series models, but remains the
    // broadest-compatible field for ordinary chat requests.
    let field = if reasoning_enabled {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };
    body[field] = json!(max_tokens);
}

/// Build a standard OpenAI-compatible streaming chat request body.
pub fn build_openai_compat_body(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
    tool_choice: Option<Value>,
    max_output_tokens: Option<u32>,
    reasoning_effort: Option<ReasoningEffort>,
    parallel_tool_calls: Option<bool>,
) -> Value {
    let mut body = json!({
        "model": model,
        "messages": messages_to_openai_compat_json(messages),
        "stream": true,
        // Ask for a final usage chunk so we can report prompt-cache hits
        // (`prompt_tokens_details.cached_tokens`). Standard OpenAI-compatible APIs
        // ignore options they do not support.
        "stream_options": { "include_usage": true },
        "tools": tools_to_openai_compat_json(tools),
    });

    if let Some(tool_choice) = tool_choice {
        body["tool_choice"] = tool_choice;
    }

    set_openai_compat_token_limit(&mut body, max_output_tokens, reasoning_effort.is_some());

    if let Some(reasoning_effort) = reasoning_effort {
        body["reasoning_effort"] = json!(reasoning_effort.to_wire_format(model));
    }

    if let Some(parallel_tool_calls) = parallel_tool_calls {
        body["parallel_tool_calls"] = json!(parallel_tool_calls);
    }

    body
}

// --- OpenAI-compatible streaming chunk parsing ---

#[derive(Debug, Deserialize)]
pub struct OpenAICompatStreamChunk {
    #[allow(dead_code)]
    id: Option<String>,
    #[serde(default)]
    choices: Vec<OpenAICompatChoice>,
    /// Present only when `stream_options.include_usage` is requested; OpenAI-
    /// compatible APIs send it in a final chunk that has empty `choices`.
    #[serde(default)]
    usage: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAICompatChoice {
    delta: OpenAICompatDelta,
    #[allow(dead_code)]
    #[serde(rename = "finish_reason")]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct OpenAICompatDelta {
    content: Option<String>,
    #[allow(dead_code)]
    role: Option<String>,
    #[serde(rename = "tool_calls")]
    tool_calls: Option<Vec<OpenAICompatToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct OpenAICompatToolCallDelta {
    /// The tool-call index, propagated into [`LLMChunk::ToolCallsIndexed`] so the
    /// engine accumulator routes argument fragments to the right call. #236.
    index: u32,
    id: Option<String>,
    #[serde(rename = "type")]
    tool_type: Option<String>,
    function: Option<OpenAICompatFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct OpenAICompatFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

/// Convert one OpenAI-compatible stream frame into every logical chunk it
/// carries.
///
/// Some compatible gateways attach `usage` to the same frame as content, tool
/// deltas, or `finish_reason`. Business output is emitted first, the cumulative
/// provider snapshot follows it, and [`LLMChunk::Done`] is always last so no
/// semantic chunk is stranded behind a terminal marker.
pub fn parse_openai_compat_chunk_multi(chunk: OpenAICompatStreamChunk) -> Vec<LLMChunk> {
    let mut chunks = Vec::new();
    let mut is_terminal = false;

    if let Some(choice) = chunk.choices.first() {
        if let Some(tool_calls) = &choice.delta.tool_calls {
            // Carry each delta's `index` so the engine accumulator can route
            // argument-only continuation fragments (which arrive with empty id/name)
            // to the matching call. A positional heuristic corrupts arguments when an
            // aggregator interleaves fragments across indices. #236.
            let calls: Vec<(u32, bamboo_domain::ToolCall)> = tool_calls
                .iter()
                .map(|tc| {
                    (
                        tc.index,
                        bamboo_domain::ToolCall {
                            id: tc.id.clone().unwrap_or_default(),
                            tool_type: tc
                                .tool_type
                                .clone()
                                .unwrap_or_else(|| "function".to_string()),
                            function: bamboo_domain::FunctionCall {
                                name: tc
                                    .function
                                    .as_ref()
                                    .and_then(|f| f.name.clone())
                                    .unwrap_or_default(),
                                arguments: tc
                                    .function
                                    .as_ref()
                                    .and_then(|f| f.arguments.clone())
                                    .unwrap_or_default(),
                            },
                        },
                    )
                })
                .collect();

            if !calls.is_empty() {
                chunks.push(LLMChunk::ToolCallsIndexed(calls));
            }
        }

        if let Some(content) = &choice.delta.content {
            chunks.push(LLMChunk::Token(content.clone()));
        }

        is_terminal = choice.finish_reason.is_some();
    }

    if let Some(usage) = &chunk.usage {
        if let Some(usage_chunk) = crate::cache::provider_usage_from_openai_usage(usage) {
            chunks.push(usage_chunk);
        }
    }

    if is_terminal {
        chunks.push(LLMChunk::Done);
    } else if chunks.is_empty() {
        chunks.push(LLMChunk::Token(String::new()));
    }

    chunks
}

fn require_single_openai_compat_chunk(mut chunks: Vec<LLMChunk>) -> Result<LLMChunk> {
    if chunks.len() != 1 {
        return Err(LLMError::Stream(format!(
            "OpenAI-compatible SSE frame produced {} logical chunks; use the multi-output parser",
            chunks.len()
        )));
    }
    Ok(chunks.remove(0))
}

/// Legacy single-output conversion.
///
/// Valid multi-semantic frames fail explicitly instead of silently discarding
/// content or usage. Production Chat adapters use
/// [`parse_openai_compat_chunk_multi`].
pub fn parse_openai_compat_chunk(chunk: OpenAICompatStreamChunk) -> Result<LLMChunk> {
    require_single_openai_compat_chunk(parse_openai_compat_chunk_multi(chunk))
}

/// Surface a mid-stream OpenAI-compatible `"error"` event as an [`LLMError::Api`].
///
/// OpenAI-compatible providers (OpenAI, GitHub Copilot, Bodhi) deliver
/// rate-limit / server / content-filter errors as ordinary SSE `data:` JSON
/// carrying a top-level `"error"` field, e.g.
/// `{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}`.
/// If the error object has a human-readable `"message"` string we surface it
/// directly; otherwise we fall back to the JSON form of the whole error
/// object so no detail is lost. Mirrors the error-event handling already used
/// by the Gemini (`gemini/stream.rs`) and Anthropic (`anthropic/mod.rs`)
/// parsers.
fn openai_compat_error_to_llm_error(error: &Value) -> LLMError {
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| error.to_string());
    LLMError::Api(message)
}

/// Parse an SSE `data:` payload in strict mode (OpenAI behavior).
///
/// - `"[DONE]"` -> `LLMChunk::Done`
/// - A JSON object with a top-level `"error"` field -> `Err(LLMError::Api(..))`
///   (rate limits, server errors, content-filter triggers, etc. — these arrive
///   as regular `data:` JSON and would otherwise be swallowed as an empty
///   token). See issue #26.
/// - Invalid JSON -> error
pub fn parse_openai_compat_sse_data_strict(data: &str) -> Result<LLMChunk> {
    require_single_openai_compat_chunk(parse_openai_compat_sse_data_strict_multi(data)?)
}

/// Strict OpenAI-compatible SSE parsing that preserves every logical chunk in
/// one frame.
pub fn parse_openai_compat_sse_data_strict_multi(data: &str) -> Result<Vec<LLMChunk>> {
    if data.trim() == "[DONE]" {
        return Ok(vec![LLMChunk::Done]);
    }

    // Parse into a generic JSON value first so we can detect mid-stream error
    // events (which otherwise deserialize as a chunk with empty `choices` and
    // silently swallow the provider's error) before deserializing the stream
    // chunk type. See issue #26.
    let value: Value = serde_json::from_str(data)?;
    if let Some(error) = value.get("error").filter(|e| sse_error_is_present(e)) {
        return Err(openai_compat_error_to_llm_error(error));
    }
    let chunk: OpenAICompatStreamChunk = serde_json::from_value(value)?;
    Ok(parse_openai_compat_chunk_multi(chunk))
}

/// Parse an SSE `data:` payload in lenient mode (Copilot behavior).
///
/// - `"[DONE]"` -> `LLMChunk::Done`
/// - A JSON object with a top-level `"error"` field -> `Err(LLMError::Api(..))`
///   (surfaced even in lenient mode so provider errors are never silently
///   swallowed as empty tokens). See issue #26.
/// - Invalid JSON -> `LLMChunk::Token("")`
pub fn parse_openai_compat_sse_data_lenient(data: &str) -> Result<LLMChunk> {
    require_single_openai_compat_chunk(parse_openai_compat_sse_data_lenient_multi(data)?)
}

/// Lenient Copilot-compatible SSE parsing that preserves every logical chunk
/// in one valid frame while retaining the historical empty-token fallback for
/// malformed payloads.
pub fn parse_openai_compat_sse_data_lenient_multi(data: &str) -> Result<Vec<LLMChunk>> {
    if data.trim() == "[DONE]" {
        return Ok(vec![LLMChunk::Done]);
    }

    match serde_json::from_str::<Value>(data) {
        Ok(value) => {
            // Surface mid-stream API errors even in lenient mode; only
            // structurally-unexpected (or unparseable) payloads degrade to an
            // empty token.
            if let Some(error) = value.get("error").filter(|e| sse_error_is_present(e)) {
                return Err(openai_compat_error_to_llm_error(error));
            }
            match serde_json::from_value::<OpenAICompatStreamChunk>(value) {
                Ok(chunk) => Ok(parse_openai_compat_chunk_multi(chunk)),
                Err(_) => Ok(vec![LLMChunk::Token(String::new())]),
            }
        }
        Err(_) => Ok(vec![LLMChunk::Token(String::new())]),
    }
}

/// Build an OpenAI-compatible **Chat Completions** stream with one globally
/// terminal [`LLMChunk::Done`].
///
/// Chat providers may signal completion twice: first with a choice
/// `finish_reason`, then with the `[DONE]` SSE sentinel. When
/// `stream_options.include_usage` is enabled, the cumulative usage trailer
/// arrives between those two signals. Stateless frame parsing therefore
/// produces `Done -> ProviderUsage -> Done`.
///
/// This Chat-only boundary defers `finish_reason` markers while usage may still
/// follow. An explicit `[DONE]` emits the one terminal marker immediately and
/// stops polling the HTTP body; when a provider omits `[DONE]`, a pending
/// `finish_reason` is finalized at normal EOF. If parsing or transport fails,
/// the error terminates the stream and no synthetic successful `Done` is
/// emitted. Responses API streams deliberately continue to use the generic SSE
/// adapter and retain their existing terminal parser semantics.
pub fn openai_compat_chat_stream_from_sse<H>(response: Response, mut handler: H) -> LLMStream
where
    H: FnMut(&str, &str) -> Result<Vec<LLMChunk>> + Send + 'static,
{
    let sentinel_seen = Arc::new(AtomicBool::new(false));
    let sentinel_seen_by_handler = Arc::clone(&sentinel_seen);
    let upstream = llm_stream_from_sse_multi(response, move |event, data| {
        if data.trim() == "[DONE]" {
            sentinel_seen_by_handler.store(true, Ordering::Release);
        }
        handler(event, data)
    });

    coordinate_openai_compat_chat_terminal(upstream, sentinel_seen)
}

fn coordinate_openai_compat_chat_terminal(
    upstream: LLMStream,
    sentinel_seen: Arc<AtomicBool>,
) -> LLMStream {
    struct TerminalState {
        upstream: LLMStream,
        sentinel_seen: Arc<AtomicBool>,
        pending_finish: bool,
        finished: bool,
    }

    let state = TerminalState {
        upstream,
        sentinel_seen,
        pending_finish: false,
        finished: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.finished {
            return None;
        }

        loop {
            match state.upstream.next().await {
                Some(Ok(LLMChunk::Done)) => {
                    if state.sentinel_seen.swap(false, Ordering::AcqRel) {
                        state.finished = true;
                        return Some((Ok(LLMChunk::Done), state));
                    }
                    state.pending_finish = true;
                    cooperative_yield_once().await;
                }
                Some(Ok(chunk)) => return Some((Ok(chunk), state)),
                Some(Err(error)) => {
                    state.finished = true;
                    return Some((Err(error), state));
                }
                None if state.pending_finish => {
                    state.finished = true;
                    return Some((Ok(LLMChunk::Done), state));
                }
                None => return None,
            }
        }
    }))
}

async fn cooperative_yield_once() {
    // Suppressed finish markers are intentionally invisible to consumers, but
    // an always-ready upstream can otherwise keep this `unfold` future inside
    // one poll forever. Force exactly one self-waking Pending boundary so
    // cancellation, timeouts, and sibling tasks remain observable.
    let mut yielded = false;
    futures::future::poll_fn(move |context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::provider::Result;
    use crate::types::LLMChunk;
    use bamboo_domain::Message;
    use bamboo_domain::{FunctionCall, ToolCall};
    use bamboo_domain::{FunctionSchema, ToolSchema};
    use futures::{Future, StreamExt};

    type MultiParser = fn(&str) -> Result<Vec<LLMChunk>>;

    async fn collect_chat_stream(body: &str, parse: MultiParser) -> Vec<Result<LLMChunk>> {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(body.to_string())
                .expect("http response"),
        );
        let mut stream =
            super::openai_compat_chat_stream_from_sse(response, move |_event, data| parse(data));
        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item);
        }
        chunks
    }

    fn tool_call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn build_openai_compat_body_preserves_max_reasoning_effort() {
        let body = super::build_openai_compat_body(
            "gpt-5.6-sol",
            &[],
            &[],
            None,
            Some(32_000),
            Some(bamboo_domain::ReasoningEffort::Max),
            None,
        );

        assert_eq!(body["reasoning_effort"], "max");
        assert_eq!(body["max_completion_tokens"], 32_000);
        assert!(body.get("max_tokens").is_none());
    }

    #[test]
    fn build_openai_compat_body_keeps_legacy_models_on_xhigh() {
        let body = super::build_openai_compat_body(
            "gpt-4o",
            &[],
            &[],
            None,
            Some(16_384),
            Some(bamboo_domain::ReasoningEffort::Max),
            None,
        );

        assert_eq!(body["reasoning_effort"], "xhigh");
        assert_eq!(body["max_completion_tokens"], 16_384);
        assert!(body.get("max_tokens").is_none());
    }

    #[test]
    fn messages_to_openai_compat_json_omits_internal_fields() {
        let messages = vec![Message::user("Hello")];

        let out = super::messages_to_openai_compat_json(&messages);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["role"], "user");
        assert_eq!(out[0]["content"], "Hello");
        assert!(out[0].get("id").is_none());
        assert!(out[0].get("created_at").is_none());
    }

    #[test]
    fn messages_to_openai_compat_json_includes_tool_fields() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let messages = vec![
            Message::assistant("", Some(vec![tool_call])),
            Message::tool_result("call_1", "ok"),
        ];

        let out = super::messages_to_openai_compat_json(&messages);

        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["role"], "assistant");
        assert!(out[0].get("tool_calls").is_some());
        assert_eq!(out[0]["tool_calls"][0]["id"], "call_1");
        assert_eq!(out[0]["tool_calls"][0]["type"], "function");
        assert_eq!(out[0]["tool_calls"][0]["function"]["name"], "search");
        assert_eq!(
            out[0]["tool_calls"][0]["function"]["arguments"],
            r#"{"q":"test"}"#
        );

        assert_eq!(out[1]["role"], "tool");
        assert_eq!(out[1]["tool_call_id"], "call_1");
        // A text-only tool result adds no synthetic relay message.
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn tool_result_image_relayed_as_trailing_user_message() {
        let messages = vec![
            Message::assistant("", Some(vec![tool_call("call_1", "screenshot")])),
            Message::tool_result_with_images(
                "call_1",
                "captured",
                true,
                vec![bamboo_domain::ToolResultImage {
                    mime_type: "image/png".to_string(),
                    data: "AAAA".to_string(),
                }],
            ),
        ];

        let out = super::messages_to_openai_compat_json(&messages);

        // assistant, tool (string content), then a synthetic user message carrying
        // the image the tool role can't hold.
        assert_eq!(out.len(), 3);
        assert_eq!(out[1]["role"], "tool");
        assert_eq!(out[1]["content"], "captured");
        assert_eq!(out[2]["role"], "user");
        let content = out[2]["content"].as_array().expect("array content");
        assert!(content
            .iter()
            .any(|p| p["type"] == "image_url"
                && p["image_url"]["url"] == "data:image/png;base64,AAAA"));
    }

    #[test]
    fn parallel_tool_result_images_flush_after_block_keeping_tools_consecutive() {
        // Two parallel tool results, each with an image. The Chat Completions API
        // requires the tool replies to stay CONSECUTIVE; the relayed images must
        // therefore land in ONE user message AFTER the whole tool block, never
        // interleaved between the two tool messages (which would be a 400).
        let messages = vec![
            Message::assistant(
                "",
                Some(vec![
                    tool_call("call_1", "shotA"),
                    tool_call("call_2", "shotB"),
                ]),
            ),
            Message::tool_result_with_images(
                "call_1",
                "a",
                true,
                vec![bamboo_domain::ToolResultImage {
                    mime_type: "image/png".to_string(),
                    data: "AAAA".to_string(),
                }],
            ),
            Message::tool_result_with_images(
                "call_2",
                "b",
                true,
                vec![bamboo_domain::ToolResultImage {
                    mime_type: "image/png".to_string(),
                    data: "BBBB".to_string(),
                }],
            ),
        ];

        let out = super::messages_to_openai_compat_json(&messages);

        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[1]["role"], "tool", "tool replies must stay consecutive");
        assert_eq!(out[2]["role"], "tool", "no user message between the tools");
        assert_eq!(out[3]["role"], "user");
        let content = out[3]["content"].as_array().expect("array content");
        assert!(content
            .iter()
            .any(|p| p["image_url"]["url"] == "data:image/png;base64,AAAA"));
        assert!(content
            .iter()
            .any(|p| p["image_url"]["url"] == "data:image/png;base64,BBBB"));
    }

    #[test]
    fn tool_result_images_flush_before_a_following_user_message() {
        let messages = vec![
            Message::assistant("", Some(vec![tool_call("call_1", "shot")])),
            Message::tool_result_with_images(
                "call_1",
                "a",
                true,
                vec![bamboo_domain::ToolResultImage {
                    mime_type: "image/png".to_string(),
                    data: "AAAA".to_string(),
                }],
            ),
            Message::user("thanks"),
        ];

        let out = super::messages_to_openai_compat_json(&messages);

        // The relayed-image user message must be flushed BEFORE the real user turn.
        assert_eq!(out.len(), 4);
        assert_eq!(out[1]["role"], "tool");
        assert_eq!(out[2]["role"], "user");
        assert!(out[2]["content"].is_array(), "relayed image message");
        assert_eq!(out[3]["role"], "user");
        assert_eq!(out[3]["content"], "thanks");
    }

    #[test]
    fn separate_tool_blocks_each_flush_independently() {
        // Two distinct tool blocks separated by an assistant turn: each block's
        // image must be relayed by its OWN flush, at the right position.
        let img = |data: &str| bamboo_domain::ToolResultImage {
            mime_type: "image/png".to_string(),
            data: data.to_string(),
        };
        let messages = vec![
            Message::assistant("", Some(vec![tool_call("call_1", "shotA")])),
            Message::tool_result_with_images("call_1", "a", true, vec![img("AAAA")]),
            Message::assistant("here you go", None), // closes block 1
            Message::assistant("", Some(vec![tool_call("call_2", "shotB")])),
            Message::tool_result_with_images("call_2", "b", true, vec![img("BBBB")]),
        ];

        let out = super::messages_to_openai_compat_json(&messages);

        // assistant, tool(A), user(relay A), assistant("here"), assistant(tool),
        // tool(B), user(relay B).
        assert_eq!(out.len(), 7);
        assert_eq!(out[1]["role"], "tool");
        assert_eq!(out[2]["role"], "user");
        assert!(out[2]["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["image_url"]["url"] == "data:image/png;base64,AAAA"));
        assert_eq!(out[3]["role"], "assistant");
        assert_eq!(out[3]["content"], "here you go");
        assert_eq!(out[5]["role"], "tool");
        assert_eq!(out[6]["role"], "user");
        let last = out[6]["content"].as_array().unwrap();
        assert!(last
            .iter()
            .any(|p| p["image_url"]["url"] == "data:image/png;base64,BBBB"));
        // The second block's relay must NOT carry the first block's image.
        assert!(!last
            .iter()
            .any(|p| p["image_url"]["url"] == "data:image/png;base64,AAAA"));
    }

    #[test]
    fn tools_to_openai_compat_json_serializes_shape() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search".to_string(),
                description: "Search the web".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "q": { "type": "string" }
                    },
                }),
            },
        }];

        let out = super::tools_to_openai_compat_json(&tools);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "function");
        assert!(out[0].get("schema_type").is_none());
        assert_eq!(out[0]["function"]["name"], "search");
        assert_eq!(out[0]["function"]["description"], "Search the web");
        assert_eq!(out[0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn tools_to_openai_compat_json_sanitizes_top_level_combinators() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "edit".to_string(),
                description: "Edit file".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "file_path": { "type": "string" },
                        "patch": { "type": "string" }
                    },
                    "oneOf": [
                        { "required": ["patch"] },
                        { "required": ["old_string", "new_string"] }
                    ]
                }),
            },
        }];

        let out = super::tools_to_openai_compat_json(&tools);
        assert!(out[0]["function"]["parameters"]["oneOf"].is_null());
        assert_eq!(out[0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn build_openai_compat_body_includes_required_fields() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body(
            "gpt-4o-mini",
            &messages,
            &tools,
            None,
            None,
            None,
            None,
        );

        assert_eq!(body["model"], "gpt-4o-mini");
        assert_eq!(body["stream"], true);
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"].as_array().unwrap().len(), 0);
        assert!(body.get("tool_choice").is_none());
        assert!(body.get("max_tokens").is_none());
    }

    #[test]
    fn build_openai_compat_body_keeps_system_messages_in_messages_array() {
        let messages = vec![
            Message::system("Stable instructions"),
            Message::user("Hello"),
        ];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body(
            "gpt-4o-mini",
            &messages,
            &tools,
            None,
            None,
            None,
            None,
        );

        let serialized_messages = body["messages"].as_array().expect("messages array");
        assert_eq!(serialized_messages.len(), 2);
        assert_eq!(serialized_messages[0]["role"], "system");
        assert_eq!(serialized_messages[0]["content"], "Stable instructions");
        assert_eq!(serialized_messages[1]["role"], "user");
        assert_eq!(serialized_messages[1]["content"], "Hello");
        assert!(body.get("instructions").is_none());
        assert!(body.get("input").is_none());
        assert!(body.get("previous_response_id").is_none());
        assert!(body.get("store").is_none());
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_content_delta_yields_token() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"content":"Hello"}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "Hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_tool_calls_delta_yields_indexed_tool_calls() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":\"test\"}"}}]}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        // The chat-completions path emits ToolCallsIndexed so the engine can route
        // continuation fragments by index. #236.
        match chunk {
            LLMChunk::ToolCallsIndexed(calls) => {
                assert_eq!(calls.len(), 1);
                let (index, call) = &calls[0];
                assert_eq!(*index, 0);
                assert_eq!(call.id, "call_1");
                assert_eq!(call.tool_type, "function");
                assert_eq!(call.function.name, "search");
                assert_eq!(call.function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected LLMChunk::ToolCallsIndexed, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_propagates_second_tool_call_index() {
        // Two tool calls in one delta: indices must be preserved distinctly so the
        // engine routes each call's later argument fragments correctly. #236.
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"a","arguments":""}},{"index":1,"id":"call_b","type":"function","function":{"name":"b","arguments":""}}]}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::ToolCallsIndexed(calls) => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].0, 0);
                assert_eq!(calls[0].1.id, "call_a");
                assert_eq!(calls[1].0, 1);
                assert_eq!(calls[1].1.id, "call_b");
            }
            other => panic!("expected LLMChunk::ToolCallsIndexed, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_usage_chunk_preserves_totals_reasoning_and_cache() {
        let data = r#"{"id":"chatcmpl_1","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":120,"prompt_tokens_details":{"cached_tokens":768},"completion_tokens_details":{"reasoning_tokens":20}}}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        assert!(matches!(
            chunk,
            LLMChunk::ProviderUsage {
                input_tokens: Some(1000),
                output_tokens: Some(120),
                reasoning_tokens: Some(20),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(768),
                ..
            }
        ));
    }

    #[test]
    fn parse_openai_compat_usage_chunk_preserves_totals_with_zero_cache() {
        let data = r#"{"id":"chatcmpl_1","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":120,"prompt_tokens_details":{"cached_tokens":0}}}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        assert!(matches!(
            chunk,
            LLMChunk::ProviderUsage {
                input_tokens: Some(1000),
                output_tokens: Some(120),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn parse_openai_compat_usage_chunk_does_not_invent_missing_totals() {
        let data = r#"{"id":"chatcmpl_1","choices":[],"usage":{"total_tokens":1120}}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        assert!(matches!(
            chunk,
            LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: None,
                total_tokens: Some(1120),
                ..
            }
        ));
    }

    #[test]
    fn multi_parsers_preserve_text_tool_and_finish_frames_with_usage() {
        type MultiParser = fn(&str) -> crate::provider::Result<Vec<LLMChunk>>;
        let parsers: [MultiParser; 2] = [
            super::parse_openai_compat_sse_data_strict_multi,
            super::parse_openai_compat_sse_data_lenient_multi,
        ];
        let usage = r#""usage":{"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}"#;

        for parse in parsers {
            let text = format!(r#"{{"choices":[{{"delta":{{"content":"answer"}}}}],{usage}}}"#);
            let chunks = parse(&text).expect("text plus usage");
            assert_eq!(chunks.len(), 2);
            assert!(matches!(&chunks[0], LLMChunk::Token(token) if token == "answer"));
            assert!(matches!(chunks[1], LLMChunk::ProviderUsage { .. }));

            let tool = format!(
                r#"{{"choices":[{{"delta":{{"tool_calls":[{{"index":0,"id":"call_1","type":"function","function":{{"name":"search","arguments":"{{}}"}}}}]}}}}],{usage}}}"#
            );
            let chunks = parse(&tool).expect("tool plus usage");
            assert_eq!(chunks.len(), 2);
            assert!(matches!(
                &chunks[0],
                LLMChunk::ToolCallsIndexed(calls)
                    if calls.len() == 1 && calls[0].1.function.name == "search"
            ));
            assert!(matches!(chunks[1], LLMChunk::ProviderUsage { .. }));

            let finish =
                format!(r#"{{"choices":[{{"delta":{{}},"finish_reason":"stop"}}],{usage}}}"#);
            let chunks = parse(&finish).expect("finish plus usage");
            assert_eq!(chunks.len(), 2);
            assert!(matches!(chunks[0], LLMChunk::ProviderUsage { .. }));
            assert!(
                matches!(chunks[1], LLMChunk::Done),
                "Done must remain the final logical chunk"
            );
        }
    }

    #[test]
    fn multi_parsers_preserve_independent_empty_choices_usage_frame() {
        let data = r#"{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":0}}}"#;

        for chunks in [
            super::parse_openai_compat_sse_data_strict_multi(data).unwrap(),
            super::parse_openai_compat_sse_data_lenient_multi(data).unwrap(),
        ] {
            assert_eq!(chunks.len(), 1);
            assert!(matches!(
                chunks[0],
                LLMChunk::ProviderUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(4),
                    cache_read_input_tokens: Some(0),
                    ..
                }
            ));
        }
    }

    #[test]
    fn legacy_single_parsers_reject_multi_semantic_frames_explicitly() {
        let data = r#"{"choices":[{"delta":{"content":"answer"}}],"usage":{"prompt_tokens":10,"completion_tokens":4}}"#;

        for error in [
            super::parse_openai_compat_sse_data_strict(data).unwrap_err(),
            super::parse_openai_compat_sse_data_lenient(data).unwrap_err(),
        ] {
            assert!(error.to_string().contains("use the multi-output parser"));
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_empty_delta_yields_empty_token() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    // --- Edge case tests ---

    #[test]
    fn messages_to_openai_compat_json_handles_empty_list() {
        let messages: Vec<Message> = vec![];
        let out = super::messages_to_openai_compat_json(&messages);
        assert!(out.is_empty());
    }

    #[test]
    fn messages_to_openai_compat_json_handles_all_roles() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
            Message::assistant("Hi there", None),
            Message::tool_result("call_1", "Result"),
        ];

        let out = super::messages_to_openai_compat_json(&messages);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["role"], "system");
        assert_eq!(out[1]["role"], "user");
        assert_eq!(out[2]["role"], "assistant");
        assert_eq!(out[3]["role"], "tool");
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_done_yields_done() {
        let chunk = super::parse_openai_compat_sse_data_strict("[DONE]").unwrap();
        assert!(matches!(chunk, LLMChunk::Done));
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_done_with_whitespace() {
        let chunk = super::parse_openai_compat_sse_data_strict("  [DONE]  ").unwrap();
        assert!(matches!(chunk, LLMChunk::Done));
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_finish_reason_without_done_yields_done() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{},"finish_reason":"stop"}]}"#;
        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();
        assert!(matches!(chunk, LLMChunk::Done));
    }

    #[test]
    fn parse_openai_compat_sse_data_lenient_finish_reason_without_done_yields_done() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{},"finish_reason":"stop"}]}"#;
        let chunk = super::parse_openai_compat_sse_data_lenient(data).unwrap();
        assert!(matches!(chunk, LLMChunk::Done));
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_invalid_json_errors() {
        let data = "{invalid json}";
        let result = super::parse_openai_compat_sse_data_strict(data);
        assert!(result.is_err());
    }

    #[test]
    fn parse_openai_compat_sse_data_lenient_invalid_json_yields_empty_token() {
        let data = "{invalid json}";
        let chunk = super::parse_openai_compat_sse_data_lenient(data).unwrap();
        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_lenient_valid_json_works() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"content":"Hello"}}]}"#;
        let chunk = super::parse_openai_compat_sse_data_lenient(data).unwrap();
        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "Hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_error_event_yields_api_error() {
        // Issue #26: a mid-stream error event arrives as ordinary `data:` JSON
        // with a top-level "error" field and must NOT be swallowed as an empty
        // token.
        let data = r#"{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}"#;

        let result = super::parse_openai_compat_sse_data_strict(data);
        assert!(result.is_err(), "expected an error for an error event");
        match result.unwrap_err() {
            crate::provider::LLMError::Api(msg) => {
                assert!(
                    msg.contains("rate limit exceeded"),
                    "error message should surface the provider message, got: {msg}"
                );
            }
            other => panic!("expected LLMError::Api, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_lenient_error_event_yields_api_error() {
        // Even lenient mode (Copilot) must surface mid-stream API errors
        // instead of silently swallowing them as empty tokens. See issue #26.
        let data = r#"{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}"#;

        let result = super::parse_openai_compat_sse_data_lenient(data);
        assert!(result.is_err(), "expected an error for an error event");
        match result.unwrap_err() {
            crate::provider::LLMError::Api(msg) => {
                assert!(
                    msg.contains("rate limit exceeded"),
                    "error message should surface the provider message, got: {msg}"
                );
            }
            other => panic!("expected LLMError::Api, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_error_event_without_message_falls_back_to_object() {
        // No "message" field -> fall back to stringifying the whole error object.
        let data = r#"{"error":{"type":"server_error","code":503}}"#;

        let result = super::parse_openai_compat_sse_data_strict(data);
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::provider::LLMError::Api(msg) => {
                assert!(
                    msg.contains("server_error"),
                    "fallback should carry the error object, got: {msg}"
                );
            }
            other => panic!("expected LLMError::Api, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_string_error_surfaces_value() {
        // Some providers send "error" as a plain string rather than an object.
        let data = r#"{"error":"quota exceeded"}"#;

        let result = super::parse_openai_compat_sse_data_strict(data);
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::provider::LLMError::Api(msg) => {
                assert!(msg.contains("quota exceeded"), "got: {msg}");
            }
            other => panic!("expected LLMError::Api, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_sse_data_strict_normal_chunk_unchanged_after_error_check() {
        // Happy path regression: a normal content delta still yields a Token,
        // unaffected by the new error-field check.
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"content":"Hello"}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();
        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "Hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn null_or_empty_error_field_does_not_abort() {
        // Several OpenAI-compatible gateways emit `{"error": null}` (or empty
        // string/object/array) as a *no-error* marker. `serde_json::get` returns
        // Some(Null) for an explicit null, so these must NOT be treated as errors
        // (issue #26): they must continue the stream, not abort it.
        for data in [
            r#"{"error":null}"#,
            r#"{"error":""}"#,
            r#"{"error":{}}"#,
            r#"{"error":[]}"#,
            r#"{"error":null,"choices":[{"delta":{"content":"Hi"}}]}"#,
        ] {
            // strict: must not return Err(LLMError::Api)
            let strict = super::parse_openai_compat_sse_data_strict(data);
            assert!(
                !matches!(strict, Err(crate::provider::LLMError::Api(_))),
                "strict wrongly aborted on benign error marker: {data} -> {strict:?}"
            );
            // lenient: contract is "never abort"
            let lenient = super::parse_openai_compat_sse_data_lenient(data);
            assert!(
                !matches!(lenient, Err(crate::provider::LLMError::Api(_))),
                "lenient wrongly aborted on benign error marker: {data} -> {lenient:?}"
            );
        }
        // And the content alongside a null error still surfaces.
        match super::parse_openai_compat_sse_data_strict(
            r#"{"error":null,"choices":[{"delta":{"content":"Hi"}}]}"#,
        )
        .unwrap()
        {
            LLMChunk::Token(token) => assert_eq!(token, "Hi"),
            other => panic!("expected Token(\"Hi\"), got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_chunk_multiple_choices_uses_first() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"content":"First"}},{"delta":{"content":"Second"}}]}"#;
        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();
        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "First"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_chunk_no_choices_yields_empty_token() {
        let data = r#"{"id":"chatcmpl_1","choices":[]}"#;
        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();
        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn build_openai_compat_body_with_tool_choice() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();
        let tool_choice = serde_json::json!("auto");

        let body = super::build_openai_compat_body(
            "gpt-4",
            &messages,
            &tools,
            Some(tool_choice),
            None,
            None,
            None,
        );

        assert_eq!(body["tool_choice"], "auto");
    }

    #[test]
    fn build_openai_compat_body_with_max_tokens() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body(
            "gpt-4",
            &messages,
            &tools,
            None,
            Some(4096),
            None,
            None,
        );

        assert_eq!(body["max_tokens"], 4096);
    }

    #[test]
    fn build_openai_compat_body_with_parallel_tool_calls() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body(
            "gpt-4",
            &messages,
            &tools,
            None,
            None,
            None,
            Some(true),
        );

        assert_eq!(body["parallel_tool_calls"], true);
    }

    #[test]
    fn messages_with_empty_content_serializes_correctly() {
        let messages = vec![Message::assistant("", None)];
        let out = super::messages_to_openai_compat_json(&messages);
        assert_eq!(out[0]["content"], "");
    }

    #[test]
    fn tool_calls_with_empty_arguments() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: String::new(),
            },
        };

        let messages = vec![Message::assistant("", Some(vec![tool_call]))];
        let out = super::messages_to_openai_compat_json(&messages);

        assert_eq!(out[0]["tool_calls"][0]["function"]["arguments"], "");
    }

    #[tokio::test]
    async fn official_usage_sequence_has_one_terminal_done_at_end() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":null}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\n",
            "data: [DONE]\n\n",
        );

        for parse in [
            super::parse_openai_compat_sse_data_strict_multi as MultiParser,
            super::parse_openai_compat_sse_data_lenient_multi as MultiParser,
        ] {
            let chunks = collect_chat_stream(body, parse)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .expect("successful stream");

            assert_eq!(chunks.len(), 3);
            assert!(matches!(&chunks[0], LLMChunk::Token(token) if token == "answer"));
            assert!(matches!(chunks[1], LLMChunk::ProviderUsage { .. }));
            assert!(matches!(chunks[2], LLMChunk::Done));
            assert_eq!(
                chunks
                    .iter()
                    .filter(|chunk| matches!(chunk, LLMChunk::Done))
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn colocated_business_usage_and_finish_preserve_all_chunks_before_done() {
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"answer\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\n";

        for parse in [
            super::parse_openai_compat_sse_data_strict_multi as MultiParser,
            super::parse_openai_compat_sse_data_lenient_multi as MultiParser,
        ] {
            let chunks = collect_chat_stream(body, parse)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .expect("successful stream");

            assert_eq!(chunks.len(), 4);
            assert!(matches!(
                &chunks[0],
                LLMChunk::ToolCallsIndexed(calls)
                    if calls.len() == 1 && calls[0].1.function.name == "search"
            ));
            assert!(matches!(&chunks[1], LLMChunk::Token(token) if token == "answer"));
            assert!(matches!(chunks[2], LLMChunk::ProviderUsage { .. }));
            assert!(matches!(chunks[3], LLMChunk::Done));
        }
    }

    #[tokio::test]
    async fn finish_reason_without_usage_or_done_sentinel_completes_at_eof() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );

        for parse in [
            super::parse_openai_compat_sse_data_strict_multi as MultiParser,
            super::parse_openai_compat_sse_data_lenient_multi as MultiParser,
        ] {
            let chunks = collect_chat_stream(body, parse)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .expect("successful stream");

            assert_eq!(chunks.len(), 2);
            assert!(matches!(&chunks[0], LLMChunk::Token(token) if token == "answer"));
            assert!(matches!(chunks[1], LLMChunk::Done));
        }
    }

    #[tokio::test]
    async fn done_sentinel_terminates_without_waiting_for_http_eof() {
        let body_stream = futures::stream::once(async {
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"data: [DONE]\n\n"))
        })
        .chain(futures::stream::pending::<
            std::result::Result<bytes::Bytes, std::io::Error>,
        >());
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(reqwest::Body::wrap_stream(body_stream))
                .expect("streaming http response"),
        );
        let mut stream = super::openai_compat_chat_stream_from_sse(response, |_event, data| {
            super::parse_openai_compat_sse_data_strict_multi(data)
        });

        let item = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("[DONE] must not wait for physical EOF")
            .expect("terminal chunk")
            .expect("successful terminal chunk");
        assert!(matches!(item, LLMChunk::Done));

        let end = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("stream must stop polling the never-ending HTTP tail");
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn duplicate_finish_and_done_signals_emit_one_done() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );

        for parse in [
            super::parse_openai_compat_sse_data_strict_multi as MultiParser,
            super::parse_openai_compat_sse_data_lenient_multi as MultiParser,
        ] {
            let chunks = collect_chat_stream(body, parse)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .expect("successful stream");

            assert_eq!(chunks.len(), 1);
            assert!(matches!(chunks[0], LLMChunk::Done));
        }
    }

    #[test]
    fn always_ready_duplicate_finish_source_has_bounded_poll_work() {
        let upstream_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_in_stream = std::sync::Arc::clone(&upstream_polls);
        let upstream: crate::provider::LLMStream =
            Box::pin(futures::stream::poll_fn(move |_context| {
                polls_in_stream.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::task::Poll::Ready(Some(Ok(LLMChunk::Done)))
            }));
        let sentinel_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = super::coordinate_openai_compat_chat_terminal(upstream, sentinel_seen);
        let mut next = Box::pin(stream.next());
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);

        assert!(matches!(
            next.as_mut().poll(&mut context),
            std::task::Poll::Pending
        ));
        assert_eq!(
            upstream_polls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one downstream poll must not spin through duplicate finish markers"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn infinite_duplicate_finish_source_is_cooperative_and_cancellable() {
        let upstream_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_in_stream = std::sync::Arc::clone(&upstream_polls);
        let upstream: crate::provider::LLMStream =
            Box::pin(futures::stream::poll_fn(move |_context| {
                polls_in_stream.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::task::Poll::Ready(Some(Ok(LLMChunk::Done)))
            }));
        let sentinel_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = super::coordinate_openai_compat_chat_terminal(upstream, sentinel_seen);

        let reader = tokio::spawn(async move { stream.next().await });
        let sibling_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sibling_flag = std::sync::Arc::clone(&sibling_ran);
        let sibling = tokio::spawn(async move {
            tokio::task::yield_now().await;
            sibling_flag.store(true, std::sync::atomic::Ordering::Release);
        });

        sibling.await.expect("sibling task");
        assert!(sibling_ran.load(std::sync::atomic::Ordering::Acquire));
        assert!(
            upstream_polls.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "the infinite reader must have started before cancellation"
        );
        assert!(
            !reader.is_finished(),
            "duplicate finish markers must stay deferred"
        );

        reader.abort();
        assert!(
            reader
                .await
                .expect_err("reader must be aborted")
                .is_cancelled(),
            "a cooperative read must remain cancellable"
        );
    }

    #[tokio::test]
    async fn stream_error_after_finish_reason_has_no_synthetic_done() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"error\":{\"message\":\"connection failed\"}}\n\n",
            "data: [DONE]\n\n",
        );

        for parse in [
            super::parse_openai_compat_sse_data_strict_multi as MultiParser,
            super::parse_openai_compat_sse_data_lenient_multi as MultiParser,
        ] {
            let chunks = collect_chat_stream(body, parse).await;

            assert_eq!(chunks.len(), 2);
            assert!(matches!(
                &chunks[0],
                Ok(LLMChunk::Token(token)) if token == "partial"
            ));
            assert!(
                matches!(&chunks[1], Err(crate::provider::LLMError::Stream(message))
                if message.contains("connection failed"))
            );
            assert!(!chunks
                .iter()
                .any(|chunk| matches!(chunk, Ok(LLMChunk::Done))));
        }
    }
}
