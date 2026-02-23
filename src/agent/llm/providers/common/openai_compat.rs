//! OpenAI-compatible request serialization helpers.
//!
//! Many providers (OpenAI, GitHub Copilot, etc.) accept a request/stream shape that is compatible
//! with OpenAI's chat completions API. These helpers build a "compat" JSON body without leaking
//! internal `agent_core::Message` fields (like `id` / `created_at`).

use agent_core::{agent::Role, tools::ToolSchema, Message};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::provider::Result;
use crate::types::LLMChunk;

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

            let mut msg = json!({
                "role": role,
                "content": m.content,
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
    tools.iter().map(|t| json!(t)).collect()
}

/// Build a standard OpenAI-compatible streaming chat request body.
pub fn build_openai_compat_body(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
    tool_choice: Option<Value>,
    max_output_tokens: Option<u32>,
) -> Value {
    let mut body = json!({
        "model": model,
        "messages": messages_to_openai_compat_json(messages),
        "stream": true,
        "tools": tools_to_openai_compat_json(tools),
    });

    if let Some(tool_choice) = tool_choice {
        body["tool_choice"] = tool_choice;
    }

    if let Some(max_tokens) = max_output_tokens {
        body["max_tokens"] = json!(max_tokens);
    }

    body
}

// --- OpenAI-compatible streaming chunk parsing ---

#[derive(Debug, Deserialize)]
pub struct OpenAICompatStreamChunk {
    #[allow(dead_code)]
    id: Option<String>,
    choices: Vec<OpenAICompatChoice>,
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
    let Some(choice) = chunk.choices.first() else {
        return LLMChunk::Token(String::new());
    };

    if let Some(tool_calls) = &choice.delta.tool_calls {
        let calls: Vec<agent_core::tools::ToolCall> = tool_calls
            .iter()
            .map(|tc| agent_core::tools::ToolCall {
                id: tc.id.clone().unwrap_or_default(),
                tool_type: tc
                    .tool_type
                    .clone()
                    .unwrap_or_else(|| "function".to_string()),
                function: agent_core::tools::FunctionCall {
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
    use agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
    use agent_core::Message;

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
        assert_eq!(out[0]["tool_calls"][0]["function"]["arguments"], r#"{"q":"test"}"#);

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
    fn build_openai_compat_body_includes_required_fields() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body("gpt-4o-mini", &messages, &tools, None, None);

        assert_eq!(body["model"], "gpt-4o-mini");
        assert_eq!(body["stream"], true);
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"].as_array().unwrap().len(), 0);
        assert!(body.get("tool_choice").is_none());
        assert!(body.get("max_tokens").is_none());
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

        let body = super::build_openai_compat_body("gpt-4", &messages, &tools, Some(tool_choice), None);

        assert_eq!(body["tool_choice"], "auto");
    }

    #[test]
    fn build_openai_compat_body_with_max_tokens() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = Vec::new();

        let body = super::build_openai_compat_body("gpt-4", &messages, &tools, None, Some(4096));

        assert_eq!(body["max_tokens"], 4096);
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
