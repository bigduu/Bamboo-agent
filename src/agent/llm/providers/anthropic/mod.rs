//! Anthropic provider and request-building helpers.

pub mod api_types;
pub mod conversion;
pub mod stream;

// Re-export commonly used types
pub use api_types::*;
pub use conversion::{convert_complete_request, convert_complete_response, convert_messages_request, convert_messages_response, format_model_display_name};
pub use stream::{AnthropicStreamAdapter, map_completion_stream_chunk, format_sse_event, format_sse_data};

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::{Client, header::HeaderMap};
use agent_core::{agent::Role, tools::ToolSchema, Message};
use serde_json::{json, Value};

use crate::provider::{LLMError, LLMProvider, LLMStream, Result};
use crate::types::LLMChunk;

/// Anthropic Messages API provider.
pub struct AnthropicProvider {
    client: Client,
    api_key: String,
    base_url: String,
    max_tokens: u32,
}

impl AnthropicProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "https://api.anthropic.com/v1".to_string(),
            max_tokens: 1024,
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    fn build_headers(&self) -> Result<HeaderMap> {
        use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(&self.api_key)
                .map_err(|e| LLMError::Auth(format!("Invalid API key: {}", e)))?,
        );
        headers.insert(
            "anthropic-version",
            HeaderValue::from_static("2023-06-01"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(headers)
    }
}

#[async_trait]
impl LLMProvider for AnthropicProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        let max_tokens = max_output_tokens.unwrap_or(self.max_tokens);

        log::debug!(
            "Anthropic provider using model: {}",
            model
        );

        let body = build_anthropic_request(messages, tools, model, max_tokens, true);
        let headers = self.build_headers()?;

        let response = self
            .client
            .post(format!("{}/messages", self.base_url))
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(LLMError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.map_err(LLMError::Http)?;

            if status == 401 || status == 403 {
                return Err(LLMError::Auth(format!(
                    "Anthropic authentication failed: {}. Please check your API key.",
                    text
                )));
            }

            return Err(LLMError::Api(format!(
                "Anthropic API error: HTTP {}: {}",
                status, text
            )));
        }

        // Use shared SSE adapter with Anthropic-specific parser
        let mut state = AnthropicStreamState::default();

        let stream = crate::providers::common::sse::llm_stream_from_sse(response, move |event, data| {
            parse_anthropic_sse_event(&mut state, event, data)
        });

        Ok(stream)
    }
}

/// Build an Anthropic Messages API request body from internal message/tool types.
///
/// This is a pure conversion helper: it does no I/O and intentionally omits internal fields
/// like message `id`/`created_at`.
pub fn build_anthropic_request(
    messages: &[Message],
    tools: &[ToolSchema],
    model: &str,
    max_tokens: u32,
    stream: bool,
) -> Value {
    let (system, anthropic_messages) = messages_to_anthropic_json(messages);

    let mut body = json!({
        "model": model,
        "max_tokens": max_tokens,
        "stream": stream,
        "messages": anthropic_messages,
        "tools": tools_to_anthropic_json(tools),
    });

    if let Some(system) = system {
        body["system"] = json!(system);
    }

    body
}

fn messages_to_anthropic_json(messages: &[Message]) -> (Option<String>, Vec<Value>) {
    let mut system_parts: Vec<&str> = Vec::new();
    let mut out: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            Role::System => system_parts.push(m.content.as_str()),
            Role::User | Role::Assistant | Role::Tool => out.push(message_to_anthropic_json(m)),
        }
    }

    let system = if system_parts.is_empty() {
        None
    } else {
        Some(system_parts.join("\n\n"))
    };

    (system, out)
}

fn message_to_anthropic_json(message: &Message) -> Value {
    match message.role {
        Role::System => unreachable!("system messages should be extracted into top-level `system`"),
        Role::User => json!({
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": message.content,
                }
            ],
        }),
        Role::Assistant => {
            let mut blocks: Vec<Value> = Vec::new();

            if !message.content.is_empty() {
                blocks.push(json!({
                    "type": "text",
                    "text": message.content,
                }));
            }

            if let Some(tool_calls) = &message.tool_calls {
                for tc in tool_calls {
                    blocks.push(tool_call_to_tool_use_block(tc));
                }
            }

            json!({
                "role": "assistant",
                "content": blocks,
            })
        }
        Role::Tool => {
            let tool_use_id = message
                .tool_call_id
                .as_deref()
                .expect("tool messages must include tool_call_id");

            json!({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": message.content,
                    }
                ],
            })
        }
    }
}

fn tool_call_to_tool_use_block(tool_call: &agent_core::tools::ToolCall) -> Value {
    let input: Value = serde_json::from_str(&tool_call.function.arguments)
        .unwrap_or_else(|_| Value::String(tool_call.function.arguments.clone()));

    json!({
        "type": "tool_use",
        "id": tool_call.id,
        "name": tool_call.function.name,
        "input": input,
    })
}

fn tools_to_anthropic_json(tools: &[ToolSchema]) -> Vec<Value> {
    tools
        .iter()
        .map(|t| {
            json!({
                "name": t.function.name,
                "description": t.function.description,
                "input_schema": t.function.parameters,
            })
        })
        .collect()
}

/// Stateful parser for Anthropic SSE streaming events.
///
/// Tracks tool_use blocks by index so we can emit partial ToolCall chunks with correct id/name.
#[derive(Default)]
pub struct AnthropicStreamState {
    tool_uses_by_index: HashMap<usize, (String, String)>, // (id, name)
}

/// Parse a single Anthropic SSE event into an optional [`LLMChunk`].
///
/// Returns:
/// - `Ok(Some(chunk))` for content-bearing events (text deltas, tool calls, message_stop)
/// - `Ok(None)` for non-content events (message_start, pings, etc.)
/// - `Err(_)` for malformed JSON or unexpected shapes
pub fn parse_anthropic_sse_event(
    state: &mut AnthropicStreamState,
    event_type: &str,
    data: &str,
) -> Result<Option<LLMChunk>> {
    match event_type {
        "ping" | "message_start" | "message_delta" => Ok(None),
        "message_stop" => Ok(Some(LLMChunk::Done)),
        "error" => Err(LLMError::Api(format!("Anthropic error event: {data}"))),
        "content_block_start" => {
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic content_block_start missing index: {data}"
                )));
            };
            let Some(content_block) = v.get("content_block") else {
                return Err(LLMError::Stream(format!(
                    "Anthropic content_block_start missing content_block: {data}"
                )));
            };

            let block_type = content_block
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            if block_type != "tool_use" {
                return Ok(None);
            }

            let Some(id) = content_block.get("id").and_then(|s| s.as_str()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic tool_use content_block missing id: {data}"
                )));
            };
            let Some(name) = content_block.get("name").and_then(|s| s.as_str()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic tool_use content_block missing name: {data}"
                )));
            };

            let index = index as usize;
            state
                .tool_uses_by_index
                .insert(index, (id.to_string(), name.to_string()));

            Ok(Some(LLMChunk::ToolCalls(vec![agent_core::tools::ToolCall {
                id: id.to_string(),
                tool_type: "function".to_string(),
                function: agent_core::tools::FunctionCall {
                    name: name.to_string(),
                    arguments: String::new(),
                },
            }])))
        }
        "content_block_delta" => {
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            let Some(delta) = v.get("delta") else {
                return Ok(None);
            };

            let delta_type = delta
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            match delta_type {
                "text_delta" => {
                    let text = delta
                        .get("text")
                        .and_then(|t| t.as_str())
                        .unwrap_or_default();
                    Ok(Some(LLMChunk::Token(text.to_string())))
                }
                "input_json_delta" => {
                    let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                        return Err(LLMError::Stream(format!(
                            "Anthropic input_json_delta missing index: {data}"
                        )));
                    };
                    let partial = delta
                        .get("partial_json")
                        .and_then(|p| p.as_str())
                        .unwrap_or_default();

                    let index = index as usize;
                    let Some((id, name)) = state.tool_uses_by_index.get(&index) else {
                        return Err(LLMError::Stream(format!(
                            "Anthropic input_json_delta for unknown tool_use index {index}: {data}"
                        )));
                    };

                    Ok(Some(LLMChunk::ToolCalls(vec![agent_core::tools::ToolCall {
                        id: id.clone(),
                        tool_type: "function".to_string(),
                        function: agent_core::tools::FunctionCall {
                            name: name.clone(),
                            arguments: partial.to_string(),
                        },
                    }])))
                }
                _ => Ok(None),
            }
        }
        "content_block_stop" => {
            // Keep memory bounded: once a content block is complete, we don't need its id/name.
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            if let Some(index) = v.get("index").and_then(|i| i.as_u64()) {
                state.tool_uses_by_index.remove(&(index as usize));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod anthropic_request_building {
    use agent_core::tools::{FunctionCall, ToolCall};
    use agent_core::Message;

    #[test]
    fn system_messages_are_extracted_into_top_level_system_field() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("Hi"),
            Message::system("Be concise."),
            Message::assistant("Hello!", None),
        ];

        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["system"], "You are helpful.\n\nBe concise.");
        assert_eq!(out["messages"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn tool_messages_become_tool_result_blocks() {
        let messages = vec![Message::tool_result("call_1", "OK")];

        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
        assert_eq!(out["messages"][0]["role"], "user");
        assert_eq!(out["messages"][0]["content"][0]["type"], "tool_result");
        assert_eq!(out["messages"][0]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(out["messages"][0]["content"][0]["content"], "OK");
    }

    #[test]
    fn assistant_tool_calls_become_tool_use_blocks_with_parsed_json_input() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let messages = vec![Message::assistant("", Some(vec![tool_call]))];

        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
        assert_eq!(out["messages"][0]["role"], "assistant");
        assert_eq!(out["messages"][0]["content"][0]["type"], "tool_use");
        assert_eq!(out["messages"][0]["content"][0]["id"], "call_1");
        assert_eq!(out["messages"][0]["content"][0]["name"], "search");
        assert_eq!(out["messages"][0]["content"][0]["input"]["q"], "test");
    }
}

#[cfg(test)]
mod anthropic_stream_parse {
    use crate::types::LLMChunk;

    #[test]
    fn message_start_is_ignored() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[]}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "message_start", data).unwrap();

        assert!(chunk.is_none());
    }

    #[test]
    fn message_stop_yields_done() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"message_stop"}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "message_stop", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Done => {}
            other => panic!("expected LLMChunk::Done, got {other:?}"),
        }
    }

    #[test]
    fn text_delta_yields_token() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "Hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn tool_use_start_and_input_json_delta_yield_tool_call_parts() {
        let mut state = super::AnthropicStreamState::default();

        let start = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_start", start)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert!(calls[0].function.arguments.is_empty());
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        let delta1 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"te"}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta1)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"te"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        let delta2 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"st\"}"}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta2)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, "st\"}");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }
    }

    #[test]
    fn empty_data_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let chunk = super::parse_anthropic_sse_event(&mut state, "", "").unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn invalid_json_returns_error() {
        let mut state = super::AnthropicStreamState::default();
        let result = super::parse_anthropic_sse_event(&mut state, "content_block_delta", "{invalid}");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_event_type_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"unknown_event"}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "unknown_event", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn text_delta_with_empty_text_returns_empty_token() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":""}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn multiple_tool_uses_tracked_independently() {
        let mut state = super::AnthropicStreamState::default();

        // First tool
        let start1 = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}"#;
        let chunk1 = super::parse_anthropic_sse_event(&mut state, "content_block_start", start1)
            .unwrap()
            .expect("chunk1");

        match chunk1 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Second tool
        let start2 = r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_2","name":"read","input":{}}}"#;
        let chunk2 = super::parse_anthropic_sse_event(&mut state, "content_block_start", start2)
            .unwrap()
            .expect("chunk2");

        match chunk2 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_2");
                assert_eq!(calls[0].function.name, "read");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Delta for first tool
        let delta1 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"test\"}"}}"#;
        let chunk3 = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta1)
            .unwrap()
            .expect("chunk3");

        match chunk3 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Delta for second tool
        let delta2 = r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file\":\"test.txt\"}"}}"#;
        let chunk4 = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta2)
            .unwrap()
            .expect("chunk4");

        match chunk4 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_2");
                assert_eq!(calls[0].function.name, "read");
                assert_eq!(calls[0].function.arguments, r#"{"file":"test.txt"}"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }
    }

    #[test]
    fn content_block_start_without_tool_use_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"Hello"}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_start", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn input_json_delta_without_prior_tool_start_returns_error() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"test\"}"}}"#;

        let result = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data);
        // Should return an error because there's no prior tool_use start for index 0
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod anthropic_request_building_edge_cases {
    use agent_core::Message;

    #[test]
    fn empty_messages_list() {
        let messages: Vec<Message> = vec![];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert!(out["system"].is_null());
        assert_eq!(out["messages"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn only_system_messages() {
        let messages = vec![Message::system("Be helpful")];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["system"], "Be helpful");
        assert_eq!(out["messages"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn multiple_system_messages_joined() {
        let messages = vec![
            Message::system("Be helpful"),
            Message::system("Be concise"),
            Message::system("Be safe"),
        ];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["system"], "Be helpful\n\nBe concise\n\nBe safe");
    }

    #[test]
    fn assistant_message_with_both_content_and_tool_calls() {
        use agent_core::tools::{FunctionCall, ToolCall};

        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let messages = vec![Message::assistant("Let me search for that.", Some(vec![tool_call]))];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        assert_eq!(out["messages"][0]["role"], "assistant");
        assert_eq!(out["messages"][0]["content"].as_array().unwrap().len(), 2);
        assert_eq!(out["messages"][0]["content"][0]["type"], "text");
        assert_eq!(out["messages"][0]["content"][0]["text"], "Let me search for that.");
        assert_eq!(out["messages"][0]["content"][1]["type"], "tool_use");
    }

    #[test]
    fn tool_call_with_invalid_json_arguments_falls_back_to_string() {
        use agent_core::tools::{FunctionCall, ToolCall};

        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: "not valid json".to_string(),
            },
        };

        let messages = vec![Message::assistant("", Some(vec![tool_call]))];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);

        // Invalid JSON should be kept as a string
        assert_eq!(out["messages"][0]["content"][0]["input"], "not valid json");
    }

    #[test]
    fn stream_parameter_set_correctly() {
        let messages = vec![Message::user("Hello")];

        let out_stream_true = super::build_anthropic_request(&messages, &[], "claude-test", 64, true);
        assert_eq!(out_stream_true["stream"], true);

        let out_stream_false = super::build_anthropic_request(&messages, &[], "claude-test", 64, false);
        assert_eq!(out_stream_false["stream"], false);
    }

    #[test]
    fn max_tokens_included_in_request() {
        let messages = vec![Message::user("Hello")];
        let out = super::build_anthropic_request(&messages, &[], "claude-test", 2048, false);

        assert_eq!(out["max_tokens"], 2048);
    }

    #[test]
    fn model_included_in_request() {
        let messages = vec![Message::user("Hello")];
        let out = super::build_anthropic_request(&messages, &[], "claude-3-opus-20240229", 64, false);

        assert_eq!(out["model"], "claude-3-opus-20240229");
    }
}

#[cfg(test)]
mod anthropic_provider_tests {
    use super::*;

    #[test]
    fn test_new_provider() {
        let provider = AnthropicProvider::new("test_api_key");
        assert_eq!(provider.api_key, "test_api_key");
        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
    }

    #[test]
    fn test_with_base_url() {
        let provider = AnthropicProvider::new("test_key")
            .with_base_url("https://custom.anthropic.com");
        assert_eq!(provider.base_url, "https://custom.anthropic.com");
    }

    #[test]
    fn test_with_max_tokens() {
        let provider = AnthropicProvider::new("test_key")
            .with_max_tokens(2048);
        assert_eq!(provider.max_tokens, 2048);
    }

    #[test]
    fn test_chained_builders() {
        let provider = AnthropicProvider::new("test_key")
            .with_base_url("https://custom.api.com")
            .with_max_tokens(4096);

        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://custom.api.com");
        assert_eq!(provider.max_tokens, 4096);
    }

    #[test]
    fn test_request_headers() {
        let provider = AnthropicProvider::new("test_key");
        let headers = provider.build_headers().unwrap();

        assert!(headers.contains_key("x-api-key"));
        assert_eq!(headers.get("x-api-key").unwrap().to_str().unwrap(), "test_key");

        assert!(headers.contains_key("anthropic-version"));
        assert_eq!(headers.get("anthropic-version").unwrap().to_str().unwrap(), "2023-06-01");

        assert!(headers.contains_key("content-type"));
        assert_eq!(headers.get("content-type").unwrap().to_str().unwrap(), "application/json");
    }

    #[test]
    fn test_headers_with_invalid_api_key() {
        // Test that headers with non-ASCII characters in API key fail
        let provider = AnthropicProvider::new("test\u{0000}key"); // null byte
        let result = provider.build_headers();
        assert!(result.is_err());
    }

    #[test]
    fn test_default_values() {
        let provider = AnthropicProvider::new("key");

        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
    }

    #[test]
    fn test_error_response_handling() {
        // Test error event parsing
        let mut state = AnthropicStreamState::default();
        let error_data = r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;

        let result = parse_anthropic_sse_event(&mut state, "error", error_data);
        assert!(result.is_err());

        match result {
            Err(LLMError::Api(msg)) => {
                assert!(msg.contains("Anthropic error event"));
            }
            _ => panic!("Expected LLMError::Api"),
        }
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: AnthropicProvider does NOT have a model field
    #[test]
    fn anthropic_provider_has_no_model_field() {
        // This test documents the provider structure:
        // pub struct AnthropicProvider {
        //     client: Client,
        //     api_key: String,
        //     base_url: String,
        //     max_tokens: u32,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = AnthropicProvider::new("test_key");
        // Verify we can access known fields
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
        // There is NO provider.model field to access
    }

    /// Test: AnthropicProvider does NOT have with_model() method
    #[test]
    fn anthropic_provider_has_no_with_model_method() {
        let provider = AnthropicProvider::new("test_key");

        // Available builder methods:
        let provider = provider
            .with_base_url("https://custom.api.com")
            .with_max_tokens(2048);

        // There is NO .with_model("gpt-4") method
        // Model is passed to chat_stream() as a parameter

        assert_eq!(provider.base_url, "https://custom.api.com");
        assert_eq!(provider.max_tokens, 2048);
    }
}
