//! OpenAI Responses API request serialization + streaming parsing helpers.
//!
//! Some upstreams (notably newer "agent"/"codex" style models) only support the
//! OpenAI Responses API instead of Chat Completions. We normalize Responses SSE
//! events into [`LLMChunk`] so the rest of Bamboo can stay provider-agnostic.

use crate::agent::core::{agent::Role, tools::ToolSchema, Message};
use crate::agent::llm::models::ContentPart;
use crate::agent::llm::provider::Result;
use crate::agent::llm::types::LLMChunk;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Convert internal [`Message`] values to a Responses API `input` array.
///
/// We intentionally keep this conservative:
/// - `role`
/// - `content`
///
/// NOTE: Many upstreams implementing the Responses API *do not* accept a `tool`
/// role in the input (they only allow: system/developer/user/assistant). Bamboo
/// internally represents tool results as `Role::Tool`; when serializing for
/// Responses, we convert these tool-result messages into a regular `user`
/// message with a clear prefix so the model can incorporate the observation.
///
/// We also intentionally avoid sending assistant `tool_calls` back in the input
/// since the Responses API input format differs across providers and versions.
pub fn messages_to_responses_input_json(messages: &[Message]) -> Vec<Value> {
    // If any message contains image parts, emit a "typed" content array shape so
    // multimodal inputs have a chance to reach upstream Responses implementations.
    //
    // For text-only requests, we keep the conservative string content shape that
    // has proven to work across multiple upstreams.
    let has_images = messages.iter().any(|m| {
        m.content_parts.as_ref().is_some_and(|parts| {
            parts
                .iter()
                .any(|p| matches!(p, ContentPart::ImageUrl { .. }))
        })
    });

    // Best-effort index so we can add a tool name in the serialized observation.
    let mut call_id_to_name: HashMap<&str, &str> = HashMap::new();
    for m in messages {
        if m.role != Role::Assistant {
            continue;
        }
        let Some(calls) = m.tool_calls.as_ref() else {
            continue;
        };
        for c in calls {
            if !c.id.is_empty() && !c.function.name.is_empty() {
                call_id_to_name.insert(c.id.as_str(), c.function.name.as_str());
            }
        }
    }

    messages
        .iter()
        .map(|m| {
            let role = match m.role {
                Role::System => "system",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "user",
            };

            let tool_observation_text: Option<String> = if m.role == Role::Tool {
                // Preserve the call id as plain text; some upstreams reject `tool_call_id`.
                let call_id = m.tool_call_id.as_deref().unwrap_or("");
                let tool_name = if !call_id.is_empty() {
                    call_id_to_name.get(call_id).copied().unwrap_or("")
                } else {
                    ""
                };

                if !tool_name.is_empty() && !call_id.is_empty() {
                    Some(format!(
                        "[tool_result name={tool_name} call_id={call_id}]\n{}",
                        m.content
                    ))
                } else if !call_id.is_empty() {
                    Some(format!("[tool_result call_id={call_id}]\n{}", m.content))
                } else {
                    Some(format!("[tool_result]\n{}", m.content))
                }
            } else {
                None
            };

            let content: Value = if has_images {
                // Typed content array (best-effort for multimodal responses).
                let mut out = Vec::new();

                if let Some(parts) = m.content_parts.as_ref() {
                    for part in parts {
                        match part {
                            ContentPart::Text { text } => {
                                out.push(json!({"type": "input_text", "text": text}));
                            }
                            ContentPart::ImageUrl { image_url } => {
                                out.push(
                                    json!({"type": "input_image", "image_url": image_url.url}),
                                );
                            }
                        }
                    }
                } else {
                    // No parts: degrade to a typed text entry (or tool observation text).
                    let text = tool_observation_text
                        .clone()
                        .unwrap_or_else(|| m.content.clone());
                    out.push(json!({"type": "input_text", "text": text}));
                }

                json!(out)
            } else {
                // Conservative string content (widely compatible).
                if let Some(text) = tool_observation_text {
                    json!(text)
                } else {
                    json!(m.content)
                }
            };

            let msg = json!({
                "role": role,
                "content": content,
            });

            msg
        })
        .collect()
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
                "parameters": t.function.parameters,
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
) -> Value {
    let mut body = json!({
        "model": model,
        "input": messages_to_responses_input_json(messages),
        "stream": true,
    });

    if !tools.is_empty() {
        body["tools"] = json!(tools_to_responses_json(tools));
        // Best-effort default; upstreams may ignore/override.
        body["tool_choice"] = json!("auto");
    }

    if let Some(max_tokens) = max_output_tokens {
        body["max_output_tokens"] = json!(max_tokens);
    }

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
}

impl ResponsesSseParser {
    pub fn new() -> Self {
        Self {
            fn_calls: HashMap::new(),
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

    pub fn handle_event(&mut self, event: &str, data: &str) -> Result<Option<LLMChunk>> {
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            // Be lenient: some upstreams occasionally send non-JSON keepalives.
            return Ok(None);
        };

        let event_type = self.event_type(event, &v);

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

            "response.completed" => Ok(Some(LLMChunk::Done)),

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
        let body = build_responses_body("gpt-5.3-codex", &[], &[], Some(123));
        assert_eq!(body["model"], "gpt-5.3-codex");
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_output_tokens"], 123);
        assert!(body.get("input").is_some());
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
    fn messages_to_responses_input_json_converts_tool_role_to_user_observation() {
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
        assert_eq!(out.len(), 2);
        assert_eq!(out[1]["role"], "user");
        assert!(out[1]["content"]
            .as_str()
            .unwrap_or("")
            .contains("tool_result"));
        assert!(out[1]["content"].as_str().unwrap_or("").contains("call_1"));
        assert!(out[1]["content"].as_str().unwrap_or("").contains("search"));
        assert!(out[1].get("tool_call_id").is_none());
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
