//! OpenAI-compatible request serialization helpers.
//!
//! Many providers (OpenAI, GitHub Copilot, etc.) accept a request/stream shape that is compatible
//! with OpenAI's chat completions API. These helpers build a "compat" JSON body without leaking
//! internal `bamboo_domain::Message` fields (like `id` / `created_at`).

use bamboo_domain::ToolSchema;
use bamboo_domain::{Message, Role};
use serde::Deserialize;
use serde_json::{json, Value};

use super::tool_schema::sanitize_openai_function_parameters_schema;
use crate::provider::Result;
use crate::types::LLMChunk;
use bamboo_domain::ReasoningEffort;

/// Convert internal [`Message`] values to an OpenAI-compatible JSON array.
///
/// This intentionally omits internal fields like `id` and `created_at`.
pub fn messages_to_openai_compat_json(messages: &[Message]) -> Vec<Value> {
    messages
        .iter()
        .map(|m| {
            let role = match m.role {
                Role::System => "system",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
            };

            // OpenAI-compatible APIs accept either a string content, or an array of
            // typed parts for multimodal messages. We only emit parts when present
            // and when the role supports user/assistant content.
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

            msg
        })
        .collect()
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

    if let Some(max_tokens) = max_output_tokens {
        body["max_tokens"] = json!(max_tokens);
    }

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
    #[allow(dead_code)]
    index: usize,
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

/// Convert a single OpenAI-compatible stream chunk into an [`LLMChunk`].
pub fn parse_openai_compat_chunk(chunk: OpenAICompatStreamChunk) -> LLMChunk {
    // Final usage chunk (empty choices): surface provider-side prompt cache hits
    // so the cache badge works for OpenAI-compatible providers too.
    if let Some(usage) = &chunk.usage {
        if let Some(cache_chunk) = crate::cache::cache_usage_from_openai_usage(usage) {
            return cache_chunk;
        }
    }

    let Some(choice) = chunk.choices.first() else {
        return LLMChunk::Token(String::new());
    };

    if let Some(tool_calls) = &choice.delta.tool_calls {
        let calls: Vec<bamboo_domain::ToolCall> = tool_calls
            .iter()
            .map(|tc| bamboo_domain::ToolCall {
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
            })
            .collect();

        if !calls.is_empty() {
            return LLMChunk::ToolCalls(calls);
        }

        return LLMChunk::Token(String::new());
    }

    if let Some(content) = &choice.delta.content {
        return LLMChunk::Token(content.clone());
    }

    // Some OpenAI-compatible providers terminate with an empty delta plus
    // finish_reason (without emitting a separate [DONE] marker).
    if choice.finish_reason.is_some() {
        return LLMChunk::Done;
    }

    LLMChunk::Token(String::new())
}

/// Parse an SSE `data:` payload in strict mode (OpenAI behavior).
///
/// - `"[DONE]"` -> `LLMChunk::Done`
/// - Invalid JSON -> error
pub fn parse_openai_compat_sse_data_strict(data: &str) -> Result<LLMChunk> {
    if data.trim() == "[DONE]" {
        return Ok(LLMChunk::Done);
    }

    let chunk: OpenAICompatStreamChunk = serde_json::from_str(data)?;
    Ok(parse_openai_compat_chunk(chunk))
}

/// Parse an SSE `data:` payload in lenient mode (Copilot behavior).
///
/// - `"[DONE]"` -> `LLMChunk::Done`
/// - Invalid JSON -> `LLMChunk::Token(\"\")`
pub fn parse_openai_compat_sse_data_lenient(data: &str) -> Result<LLMChunk> {
    if data.trim() == "[DONE]" {
        return Ok(LLMChunk::Done);
    }

    match serde_json::from_str::<OpenAICompatStreamChunk>(data) {
        Ok(chunk) => Ok(parse_openai_compat_chunk(chunk)),
        Err(_) => Ok(LLMChunk::Token(String::new())),
    }
}

#[cfg(test)]
mod tests {
    use crate::types::LLMChunk;
    use bamboo_domain::Message;
    use bamboo_domain::{FunctionCall, ToolCall};
    use bamboo_domain::{FunctionSchema, ToolSchema};

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
    fn parse_openai_compat_sse_data_strict_tool_calls_delta_yields_tool_calls() {
        let data = r#"{"id":"chatcmpl_1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":\"test\"}"}}]}}]}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_1");
                assert_eq!(calls[0].tool_type, "function");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_usage_chunk_yields_cache_usage() {
        // Final usage chunk with empty choices and cached prompt tokens.
        let data = r#"{"id":"chatcmpl_1","choices":[],"usage":{"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":768}}}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            } => assert_eq!(cache_read_input_tokens, 768),
            other => panic!("expected LLMChunk::CacheUsage, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_compat_usage_chunk_without_cache_yields_empty_token() {
        let data = r#"{"id":"chatcmpl_1","choices":[],"usage":{"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":0}}}"#;

        let chunk = super::parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected empty LLMChunk::Token, got {other:?}"),
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
}
