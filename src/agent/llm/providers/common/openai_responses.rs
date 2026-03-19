//! OpenAI Responses API request serialization + streaming parsing helpers.
//!
//! Some upstreams (notably newer "agent"/"codex" style models) only support the
//! OpenAI Responses API instead of Chat Completions. We normalize Responses SSE
//! events into [`LLMChunk`] so the rest of Bamboo can stay provider-agnostic.

use super::tool_schema::sanitize_openai_function_parameters_schema;
use crate::agent::core::{agent::Role, tools::ToolSchema, Message};
use crate::agent::llm::models::ContentPart;
use crate::agent::llm::provider::ResponsesRequestOptions;
use crate::agent::llm::provider::Result;
use crate::agent::llm::types::LLMChunk;
use crate::core::ReasoningEffort;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Convert internal [`Message`] values to a Responses API `input` array.
///
/// The Responses API uses a heterogeneous input array containing:
/// - `{"type": "message", "role": "...", "content": "..."}` for regular messages
/// - `{"type": "function_call", "call_id": "...", "name": "...", "arguments": "..."}` for tool invocations
/// - `{"type": "function_call_output", "call_id": "...", "output": "..."}` for tool results
///
/// This function properly serializes the full tool-call chain so the model
/// maintains structured context across rounds, instead of degrading tool
/// interactions into plain-text user messages.
///
/// For assistant messages that contain `tool_calls`, we:
/// 1. Emit a `message` item for any text content the assistant produced.
/// 2. Emit a `function_call` item for each tool call.
///
/// For tool-result messages (`Role::Tool`), we emit a `function_call_output` item.
pub fn messages_to_responses_input_json(messages: &[Message]) -> Vec<Value> {
    // If any message contains image parts, emit a "typed" content array shape so
    // multimodal inputs have a chance to reach upstream Responses implementations.
    let has_images = messages.iter().any(|m| {
        m.content_parts.as_ref().is_some_and(|parts| {
            parts
                .iter()
                .any(|p| matches!(p, ContentPart::ImageUrl { .. }))
        })
    });

    let mut out: Vec<Value> = Vec::with_capacity(messages.len());

    for m in messages {
        match m.role {
            Role::Assistant => {
                // Emit assistant text content as a message item (even if empty, for completeness).
                let has_text = !m.content.trim().is_empty();
                if has_text || m.tool_calls.is_none() {
                    let content = build_content_value(m, has_images, None);
                    out.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": content,
                    }));
                }

                // Emit each tool call as a structured function_call item.
                if let Some(calls) = m.tool_calls.as_ref() {
                    for c in calls {
                        out.push(json!({
                            "type": "function_call",
                            "call_id": c.id,
                            "name": c.function.name,
                            "arguments": c.function.arguments,
                        }));
                    }
                }
            }

            Role::Tool => {
                // Emit tool result as a structured function_call_output item.
                let call_id = m.tool_call_id.as_deref().unwrap_or("");
                if !call_id.is_empty() {
                    out.push(json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": m.content,
                    }));
                } else {
                    // Fallback: no call_id available — degrade to user message with prefix.
                    let content = json!(format!("[tool_result]\n{}", m.content));
                    out.push(json!({
                        "type": "message",
                        "role": "user",
                        "content": content,
                    }));
                }
            }

            Role::System | Role::User => {
                let role = if m.role == Role::System {
                    "system"
                } else {
                    "user"
                };
                let content = build_content_value(m, has_images, None);
                out.push(json!({
                    "type": "message",
                    "role": role,
                    "content": content,
                }));
            }
        }
    }

    out
}

/// Build the `content` value for a message item.
///
/// If `has_images` is true, uses typed content array (`input_text` / `input_image`).
/// Otherwise, uses a plain string value.
fn build_content_value(m: &Message, has_images: bool, text_override: Option<&str>) -> Value {
    if has_images {
        let mut parts = Vec::new();
        if let Some(content_parts) = m.content_parts.as_ref() {
            for part in content_parts {
                match part {
                    ContentPart::Text { text } => {
                        parts.push(json!({"type": "input_text", "text": text}));
                    }
                    ContentPart::ImageUrl { image_url } => {
                        parts.push(json!({"type": "input_image", "image_url": image_url.url}));
                    }
                }
            }
        } else {
            let text = text_override.unwrap_or(&m.content);
            parts.push(json!({"type": "input_text", "text": text}));
        }
        json!(parts)
    } else {
        let text = text_override.unwrap_or(&m.content);
        json!(text)
    }
}

/// Convert internal tool schemas to a Responses API `tools` array JSON.
pub fn tools_to_responses_json(tools: &[ToolSchema]) -> Vec<Value> {
    // OpenAI Responses API expects tools in a flattened shape:
    // { "type": "function", "name": "...", "description": "...", "parameters": {..} }
    //
    // Our internal ToolSchema matches the Chat Completions shape:
    // { "type": "function", "function": { name, description, parameters } }
    tools
        .iter()
        .map(|t| {
            json!({
                "type": t.schema_type,
                "name": t.function.name,
                "description": t.function.description,
                "parameters": sanitize_openai_function_parameters_schema(&t.function.parameters),
            })
        })
        .collect()
}

/// Build a standard Responses API streaming request body.
pub fn build_responses_body(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
    max_output_tokens: Option<u32>,
    reasoning_effort: Option<ReasoningEffort>,
    responses_options: Option<&ResponsesRequestOptions>,
) -> Value {
    let mut body = json!({
        "model": model,
        "input": messages_to_responses_input_json(messages),
        "stream": true,
    });

    if let Some(previous_response_id) = responses_options
        .and_then(|opts| opts.previous_response_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body["previous_response_id"] = json!(previous_response_id);
    }

    if !tools.is_empty() {
        body["tools"] = json!(tools_to_responses_json(tools));
        // Best-effort default; upstreams may ignore/override.
        body["tool_choice"] = json!("auto");
    }

    if let Some(max_tokens) = max_output_tokens {
        body["max_output_tokens"] = json!(max_tokens);
    }

    let reasoning_summary = responses_options
        .and_then(|opts| opts.reasoning_summary.as_deref())
        .map(str::trim)
        .filter(|summary| !summary.is_empty());
    if reasoning_effort.is_some() || reasoning_summary.is_some() {
        let mut reasoning = serde_json::Map::new();
        if let Some(effort) = reasoning_effort {
            reasoning.insert("effort".to_string(), json!(effort.to_wire_format(model)));
        }
        if let Some(summary) = reasoning_summary {
            reasoning.insert("summary".to_string(), json!(summary));
        }
        if !reasoning.is_empty() {
            body["reasoning"] = Value::Object(reasoning);
        }
    }

    if let Some(include) = responses_options
        .and_then(|opts| opts.include.as_ref())
        .filter(|values| !values.is_empty())
    {
        body["include"] = json!(include);
    }

    if let Some(truncation) = responses_options
        .and_then(|opts| opts.truncation.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body["truncation"] = json!(truncation);
    }

    let store = responses_options
        .and_then(|opts| opts.store)
        .unwrap_or(false);
    body["store"] = json!(store);

    body
}

#[derive(Debug, Default)]
struct AccFnCall {
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

/// Parser that converts Responses SSE events into [`LLMChunk`]s.
///
/// We aim for "works with common upstreams" rather than strict spec compliance.
/// Event shape varies; we primarily look at:
/// - SSE `event:` name (e.g. "response.output_text.delta")
/// - JSON `type` field (same as SSE event name in OpenAI)
///
/// Supported:
/// - `response.output_text.delta` -> `LLMChunk::Token(delta)`
/// - `response.output_item.*` + `response.function_call_arguments.delta` -> `LLMChunk::ToolCalls`
/// - `response.completed` -> `LLMChunk::Done`
pub struct ResponsesSseParser {
    // item_id -> accumulated function call
    fn_calls: HashMap<String, AccFnCall>,
    provider_label: String,
    model: String,
    requested_reasoning_effort: Option<ReasoningEffort>,
    request_reasoning_enabled: bool,
    observed_reasoning_signal: bool,
    reasoning_event_count: usize,
    reasoning_text_chars: usize,
    logged_summary: bool,
    emitted_response_id: Option<String>,
}

impl ResponsesSseParser {
    #[allow(dead_code)] // Used in tests and retained for backward compatibility.
    pub fn new() -> Self {
        Self::new_with_context("Responses", "", None)
    }

    pub fn new_with_context(
        provider_label: &str,
        model: &str,
        requested_reasoning_effort: Option<ReasoningEffort>,
    ) -> Self {
        Self {
            fn_calls: HashMap::new(),
            provider_label: provider_label.to_string(),
            model: model.to_string(),
            requested_reasoning_effort,
            request_reasoning_enabled: requested_reasoning_effort.is_some(),
            observed_reasoning_signal: false,
            reasoning_event_count: 0,
            reasoning_text_chars: 0,
            logged_summary: false,
            emitted_response_id: None,
        }
    }

    fn event_type<'a>(&self, event: &'a str, v: &'a Value) -> &'a str {
        v.get("type").and_then(|t| t.as_str()).unwrap_or(event)
    }

    fn ensure_fn_call(&mut self, item_id: &str) -> &mut AccFnCall {
        self.fn_calls.entry(item_id.to_string()).or_default()
    }

    fn parse_fn_call_item(&mut self, item_id: &str, item: &Value) {
        let entry = self.ensure_fn_call(item_id);

        // Different upstreams use either `call_id` or overload `id`.
        if entry.call_id.is_none() {
            entry.call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        if entry.name.is_none() {
            entry.name = item
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        if let Some(args) = item.get("arguments").and_then(|v| v.as_str()) {
            if !args.is_empty() {
                entry.arguments.push_str(args);
            }
        }
    }

    fn finalize_tool_call(&mut self, item_id: &str) -> Option<crate::agent::core::tools::ToolCall> {
        let acc = self.fn_calls.remove(item_id)?;
        Some(crate::agent::core::tools::ToolCall {
            id: acc.call_id?,
            tool_type: "function".to_string(),
            function: crate::agent::core::tools::FunctionCall {
                name: acc.name?,
                arguments: acc.arguments,
            },
        })
    }

    fn log_reasoning_summary_if_needed(&mut self, usage: Option<&Value>) {
        if self.logged_summary {
            return;
        }

        if !(self.request_reasoning_enabled || self.observed_reasoning_signal) {
            return;
        }

        let reasoning_tokens = usage
            .and_then(|value| value.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(|tokens| tokens.as_u64())
            .or_else(|| {
                usage
                    .and_then(|value| value.get("reasoning_tokens"))
                    .and_then(|tokens| tokens.as_u64())
            });

        tracing::info!(
            "{} responses reasoning summary: model='{}' requested_effort={} request_reasoning_enabled={} observed_reasoning_signal={} reasoning_event_count={} reasoning_text_chars={} reasoning_tokens={}",
            self.provider_label,
            if self.model.is_empty() { "<unknown>" } else { self.model.as_str() },
            self.requested_reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            self.request_reasoning_enabled,
            self.observed_reasoning_signal,
            self.reasoning_event_count,
            self.reasoning_text_chars,
            reasoning_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        self.logged_summary = true;
    }

    fn response_id_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        value
            .get("response")
            .and_then(|response| response.get("id"))
            .and_then(|id| id.as_str())
            .or_else(|| value.get("response_id").and_then(|id| id.as_str()))
    }

    fn maybe_emit_response_id(&mut self, event_type: &str, value: &Value) -> Option<LLMChunk> {
        if !matches!(
            event_type,
            "response.created" | "response.in_progress" | "response.completed"
        ) {
            return None;
        }
        let response_id = self.response_id_from_value(value)?;
        if self.emitted_response_id.as_deref() == Some(response_id) {
            return None;
        }
        self.emitted_response_id = Some(response_id.to_string());
        Some(LLMChunk::ResponseId(response_id.to_string()))
    }

    pub fn handle_event(&mut self, event: &str, data: &str) -> Result<Option<LLMChunk>> {
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            // Be lenient: some upstreams occasionally send non-JSON keepalives.
            return Ok(None);
        };

        let event_type = self.event_type(event, &v);

        if let Some(chunk) = self.maybe_emit_response_id(event_type, &v) {
            return Ok(Some(chunk));
        }

        if event_type.contains("reasoning") {
            self.observed_reasoning_signal = true;
            self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
            let reasoning_chunk = v
                .get("delta")
                .and_then(|value| value.as_str())
                .or_else(|| v.get("text").and_then(|value| value.as_str()))
                .or_else(|| v.get("summary").and_then(|value| value.as_str()))
                .unwrap_or("");
            self.reasoning_text_chars = self
                .reasoning_text_chars
                .saturating_add(reasoning_chunk.len());
            if reasoning_chunk.is_empty() {
                return Ok(None);
            }
            return Ok(Some(LLMChunk::ReasoningToken(reasoning_chunk.to_string())));
        }

        match event_type {
            "response.output_text.delta" => {
                let delta = v.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                if delta.is_empty() {
                    return Ok(None);
                }
                Ok(Some(LLMChunk::Token(delta.to_string())))
            }

            "response.output_item.added" => {
                // Best-effort: pre-register function_call metadata.
                if let Some(item) = v.get("item") {
                    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    if item_type == "function_call" {
                        let item_id = item
                            .get("id")
                            .and_then(|id| id.as_str())
                            .or_else(|| v.get("item_id").and_then(|id| id.as_str()))
                            .unwrap_or("");
                        if !item_id.is_empty() {
                            self.parse_fn_call_item(item_id, item);
                        }
                    }
                }
                Ok(None)
            }

            "response.function_call_arguments.delta" => {
                let item_id = v.get("item_id").and_then(|id| id.as_str()).unwrap_or("");
                let delta = v.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                if item_id.is_empty() || delta.is_empty() {
                    return Ok(None);
                }
                let entry = self.ensure_fn_call(item_id);
                entry.arguments.push_str(delta);
                Ok(None)
            }

            "response.output_item.done" => {
                // Emit tool call when the function_call item is done.
                let item_id = v
                    .get("item_id")
                    .and_then(|id| id.as_str())
                    .or_else(|| {
                        v.get("item")
                            .and_then(|it| it.get("id"))
                            .and_then(|id| id.as_str())
                    })
                    .unwrap_or("");

                if let Some(item) = v.get("item") {
                    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    if item_type == "function_call" && !item_id.is_empty() {
                        self.parse_fn_call_item(item_id, item);
                    }
                }

                if item_id.is_empty() {
                    return Ok(None);
                }

                let Some(call) = self.finalize_tool_call(item_id) else {
                    return Ok(None);
                };

                Ok(Some(LLMChunk::ToolCalls(vec![call])))
            }

            "response.completed" => {
                let usage = v
                    .get("response")
                    .and_then(|response| response.get("usage"))
                    .or_else(|| v.get("usage"));
                self.log_reasoning_summary_if_needed(usage);
                Ok(Some(LLMChunk::Done))
            }

            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::{FunctionCall, ToolCall};
    use crate::agent::core::tools::{FunctionSchema, ToolSchema};

    #[test]
    fn build_responses_body_includes_input_and_stream() {
        let body = build_responses_body("gpt-5.3-codex", &[], &[], Some(123), None, None);
        assert_eq!(body["model"], "gpt-5.3-codex");
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_output_tokens"], 123);
        assert_eq!(body["store"], false);
        assert!(body.get("input").is_some());
    }

    #[test]
    fn build_responses_body_applies_responses_options() {
        let body = build_responses_body(
            "gpt-5.4",
            &[],
            &[],
            None,
            Some(ReasoningEffort::High),
            Some(&ResponsesRequestOptions {
                reasoning_summary: Some("detailed".to_string()),
                include: Some(vec!["reasoning.encrypted_content".to_string()]),
                store: Some(true),
                previous_response_id: Some("resp_123".to_string()),
                truncation: Some("auto".to_string()),
            }),
        );
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["reasoning"]["summary"], "detailed");
        assert_eq!(
            body["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );
        assert_eq!(body["store"], true);
        assert_eq!(body["previous_response_id"], "resp_123");
        assert_eq!(body["truncation"], "auto");
    }

    #[test]
    fn tools_to_responses_json_flattens_function_schema() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search".to_string(),
                description: "Search things".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } },
                    "required": ["q"]
                }),
            },
        }];

        let out = tools_to_responses_json(&tools);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "function");
        assert_eq!(out[0]["name"], "search");
        assert_eq!(out[0]["description"], "Search things");
        assert!(out[0].get("function").is_none());
        assert!(out[0].get("parameters").is_some());
    }

    #[test]
    fn tools_to_responses_json_sanitizes_top_level_combinators() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "edit".to_string(),
                description: "Edit file".to_string(),
                parameters: json!({
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

        let out = tools_to_responses_json(&tools);
        assert!(out[0]["parameters"]["oneOf"].is_null());
        assert_eq!(out[0]["parameters"]["type"], "object");
    }

    #[test]
    fn messages_to_responses_input_json_serializes_tool_calls_structurally() {
        let messages = vec![
            Message::assistant(
                "Calling a tool...",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "search".to_string(),
                        arguments: r#"{"q":"x"}"#.to_string(),
                    },
                }]),
            ),
            Message::tool_result("call_1", "result payload"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // Should produce 3 items: assistant message, function_call, function_call_output
        assert_eq!(out.len(), 3);

        // Item 0: assistant message with text content
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["content"], "Calling a tool...");

        // Item 1: structured function_call
        assert_eq!(out[1]["type"], "function_call");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["name"], "search");
        assert_eq!(out[1]["arguments"], r#"{"q":"x"}"#);

        // Item 2: structured function_call_output
        assert_eq!(out[2]["type"], "function_call_output");
        assert_eq!(out[2]["call_id"], "call_1");
        assert_eq!(out[2]["output"], "result payload");
    }

    #[test]
    fn messages_to_responses_input_json_assistant_with_only_tool_calls_no_text() {
        // When assistant has empty content and tool_calls, skip the message item
        let messages = vec![
            Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "read_file".to_string(),
                        arguments: r#"{"path":"/tmp/test"}"#.to_string(),
                    },
                }]),
            ),
            Message::tool_result("call_1", "file contents"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // Should produce 2 items: function_call, function_call_output (no empty assistant message)
        assert_eq!(out.len(), 2);

        assert_eq!(out[0]["type"], "function_call");
        assert_eq!(out[0]["call_id"], "call_1");
        assert_eq!(out[0]["name"], "read_file");

        assert_eq!(out[1]["type"], "function_call_output");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["output"], "file contents");
    }

    #[test]
    fn messages_to_responses_input_json_multiple_tool_calls_in_one_round() {
        let messages = vec![
            Message::user("Search and read"),
            Message::assistant(
                "",
                Some(vec![
                    ToolCall {
                        id: "call_1".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "search".to_string(),
                            arguments: r#"{"q":"test"}"#.to_string(),
                        },
                    },
                    ToolCall {
                        id: "call_2".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "read_file".to_string(),
                            arguments: r#"{"path":"/tmp"}"#.to_string(),
                        },
                    },
                ]),
            ),
            Message::tool_result("call_1", "search results"),
            Message::tool_result("call_2", "file contents"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // user_msg + 2x function_call + 2x function_call_output = 5 items
        assert_eq!(out.len(), 5);

        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "user");

        assert_eq!(out[1]["type"], "function_call");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["name"], "search");

        assert_eq!(out[2]["type"], "function_call");
        assert_eq!(out[2]["call_id"], "call_2");
        assert_eq!(out[2]["name"], "read_file");

        assert_eq!(out[3]["type"], "function_call_output");
        assert_eq!(out[3]["call_id"], "call_1");

        assert_eq!(out[4]["type"], "function_call_output");
        assert_eq!(out[4]["call_id"], "call_2");
    }

    #[test]
    fn messages_to_responses_input_json_tool_result_without_call_id_falls_back() {
        let messages = vec![Message::tool_result("", "orphan result")];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        // Fallback: degrade to user message with prefix
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "user");
        assert!(out[0]["content"]
            .as_str()
            .unwrap_or("")
            .contains("[tool_result]"));
    }

    #[test]
    fn messages_to_responses_input_json_system_and_user_messages() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
        ];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "system");
        assert_eq!(out[0]["content"], "You are helpful");
        assert_eq!(out[1]["type"], "message");
        assert_eq!(out[1]["role"], "user");
        assert_eq!(out[1]["content"], "Hello");
    }

    #[test]
    fn messages_to_responses_input_json_assistant_without_tool_calls() {
        let messages = vec![Message::assistant("Just a text reply", None)];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["content"], "Just a text reply");
    }

    #[test]
    fn parser_emits_token_on_output_text_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","delta":"hi"}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "hi"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_response_id_on_created_event() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.created",
                r#"{"type":"response.created","response":{"id":"resp_123","status":"in_progress"}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ResponseId(response_id)) => assert_eq!(response_id, "resp_123"),
            other => panic!("expected response id, got {other:?}"),
        }
    }

    #[test]
    fn parser_does_not_confuse_item_id_with_response_id() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search","arguments":"{}"}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_emits_reasoning_token_on_reasoning_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.reasoning.delta",
                r#"{"type":"response.reasoning.delta","delta":"think"}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ReasoningToken(t)) => assert_eq!(t, "think"),
            other => panic!("expected reasoning token, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_tool_call_on_output_item_done() {
        let mut p = ResponsesSseParser::new();

        let _ = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search","arguments":"{\"q\":\""}}"#,
            )
            .unwrap();

        let _ = p
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"item_1","delta":"test\"}"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item_id":"item_1","item":{"id":"item_1","type":"function_call"}}"#,
            )
            .unwrap();

        match out {
            Some(LLMChunk::ToolCalls(calls)) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected tool_calls, got {other:?}"),
        }
    }
}
