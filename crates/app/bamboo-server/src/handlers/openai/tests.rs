use actix_web::http::StatusCode;
use actix_web::{web, App};

use bamboo_llm::api::models::{Content, ContentPart, FunctionCall, Role, ToolCall};

use super::helpers::{build_completion_response, responses_input_to_chat_messages};

#[test]
fn responses_input_string_becomes_single_user_message() {
    let msgs = responses_input_to_chat_messages(serde_json::json!("hi")).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::User);
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "hi"),
        _ => panic!("expected text content"),
    }
}

#[test]
fn responses_input_array_parses_role_and_content_string() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        { "role": "system", "content": "s" },
        { "role": "user", "content": "u" },
        { "role": "assistant", "content": "a" }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].role, Role::System);
    assert_eq!(msgs[1].role, Role::User);
    assert_eq!(msgs[2].role, Role::Assistant);
}

#[test]
fn responses_input_parts_support_input_text() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
          "role": "user",
          "content": [{ "type": "input_text", "text": "hello" }]
        }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 1);
    match &msgs[0].content {
        Content::Parts(parts) => {
            assert_eq!(parts.len(), 1);
            match &parts[0] {
                ContentPart::Text { text } => assert_eq!(text, "hello"),
                _ => panic!("expected text part"),
            }
        }
        _ => panic!("expected parts content"),
    }
}

#[test]
fn responses_input_parts_support_output_text() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
          "role": "assistant",
          "content": [{ "type": "output_text", "text": "hello from assistant" }]
        }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Assistant);
    match &msgs[0].content {
        Content::Parts(parts) => {
            assert_eq!(parts.len(), 1);
            match &parts[0] {
                ContentPart::Text { text } => assert_eq!(text, "hello from assistant"),
                _ => panic!("expected text part"),
            }
        }
        _ => panic!("expected parts content"),
    }
}

#[test]
fn responses_input_parts_support_refusal() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
          "role": "assistant",
          "content": [{ "type": "refusal", "refusal": "I can't help with that." }]
        }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Assistant);
    match &msgs[0].content {
        Content::Parts(parts) => {
            assert_eq!(parts.len(), 1);
            match &parts[0] {
                ContentPart::Text { text } => assert_eq!(text, "I can't help with that."),
                _ => panic!("expected text part"),
            }
        }
        _ => panic!("expected parts content"),
    }
}

#[test]
fn build_completion_response_populates_core_openai_fields() {
    let tool_call = ToolCall {
        id: "call_1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "read_file".to_string(),
            arguments: r#"{"path":"README.md"}"#.to_string(),
        },
    };

    let response = build_completion_response(
        "hello from assistant".to_string(),
        Some(vec![tool_call.clone()]),
        "gpt-test",
    );

    assert!(response.id.starts_with("chatcmpl-"));
    assert_eq!(response.object.as_deref(), Some("chat.completion"));
    assert_eq!(response.model.as_deref(), Some("gpt-test"));
    assert_eq!(response.choices.len(), 1);
    assert_eq!(response.choices[0].message.role, Role::Assistant);
    assert_eq!(
        response.choices[0].message.tool_calls,
        Some(vec![tool_call])
    );
    assert_eq!(response.choices[0].finish_reason.as_deref(), Some("stop"));

    match &response.choices[0].message.content {
        Content::Text(text) => assert_eq!(text, "hello from assistant"),
        _ => panic!("expected text content"),
    }

    let usage = response.usage.expect("usage should always be present");
    assert_eq!(usage.prompt_tokens, 0);
    assert_eq!(usage.completion_tokens, 0);
    assert_eq!(usage.total_tokens, 0);
}

#[test]
fn responses_input_function_call_item_becomes_assistant_with_tool_calls() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
            "type": "function_call",
            "call_id": "call_abc",
            "name": "search",
            "arguments": "{\"q\":\"test\"}"
        }
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Assistant);

    // content should be empty text
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, ""),
        _ => panic!("expected empty text content"),
    }

    // tool_calls should have exactly one entry
    let tool_calls = msgs[0].tool_calls.as_ref().expect("expected tool_calls");
    assert_eq!(tool_calls.len(), 1);
    assert_eq!(tool_calls[0].id, "call_abc");
    assert_eq!(tool_calls[0].tool_type, "function");
    assert_eq!(tool_calls[0].function.name, "search");
    assert_eq!(tool_calls[0].function.arguments, "{\"q\":\"test\"}");

    assert!(msgs[0].tool_call_id.is_none());
}

#[test]
fn responses_input_function_call_output_item_becomes_tool_result() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
            "type": "function_call_output",
            "call_id": "call_abc",
            "output": "search results here"
        }
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Tool);

    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "search results here"),
        _ => panic!("expected text content with output"),
    }

    assert_eq!(msgs[0].tool_call_id.as_deref(), Some("call_abc"));
    assert!(msgs[0].tool_calls.is_none());
}

#[test]
fn responses_input_mixed_messages_and_function_calls() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        { "role": "system", "content": "You are a helpful assistant." },
        { "role": "user", "content": "Find info about Rust." },
        {
            "type": "function_call",
            "call_id": "call_1",
            "name": "web_search",
            "arguments": "{\"query\":\"Rust programming\"}"
        },
        {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": "Rust is a systems programming language."
        },
        { "role": "assistant", "content": "Here is what I found about Rust." }
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 5);

    // 0: system message
    assert_eq!(msgs[0].role, Role::System);
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "You are a helpful assistant."),
        _ => panic!("expected text"),
    }

    // 1: user message
    assert_eq!(msgs[1].role, Role::User);

    // 2: function_call -> assistant with tool_calls
    assert_eq!(msgs[2].role, Role::Assistant);
    let tc = msgs[2].tool_calls.as_ref().expect("expected tool_calls");
    assert_eq!(tc.len(), 1);
    assert_eq!(tc[0].id, "call_1");
    assert_eq!(tc[0].function.name, "web_search");
    assert_eq!(tc[0].function.arguments, "{\"query\":\"Rust programming\"}");

    // 3: function_call_output -> tool message
    assert_eq!(msgs[3].role, Role::Tool);
    assert_eq!(msgs[3].tool_call_id.as_deref(), Some("call_1"));
    match &msgs[3].content {
        Content::Text(t) => assert_eq!(t, "Rust is a systems programming language."),
        _ => panic!("expected text"),
    }

    // 4: assistant message
    assert_eq!(msgs[4].role, Role::Assistant);
    match &msgs[4].content {
        Content::Text(t) => assert_eq!(t, "Here is what I found about Rust."),
        _ => panic!("expected text"),
    }
}

#[actix_web::test]
async fn openai_config_registers_models_and_completion_routes() {
    let app = actix_web::test::init_service(
        App::new().service(web::scope("/openai/v1").configure(super::config)),
    )
    .await;

    for (method, uri) in [
        ("GET", "/openai/v1/models"),
        ("POST", "/openai/v1/chat/completions"),
        ("POST", "/openai/v1/responses"),
    ] {
        let req = match method {
            "GET" => actix_web::test::TestRequest::get().uri(uri).to_request(),
            "POST" => actix_web::test::TestRequest::post().uri(uri).to_request(),
            _ => unreachable!("unexpected HTTP method"),
        };
        let resp = actix_web::test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected route to be registered: {uri}"
        );
    }
}

// ── #525: Responses-API tool declarations + input hardening ────────────

#[test]
fn convert_responses_tools_maps_flat_function_tool() {
    let tools: Vec<super::types::ResponsesToolParam> = serde_json::from_value(serde_json::json!([
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get weather",
            "strict": false,
            "parameters": {"type": "object", "properties": {"location": {"type": "string"}}}
        }
    ]))
    .unwrap();

    let schemas = super::helpers::convert_responses_tools(Some(tools)).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].schema_type, "function");
    assert_eq!(schemas[0].function.name, "get_weather");
    assert_eq!(schemas[0].function.description, "Get weather");
    assert!(schemas[0].function.parameters.get("properties").is_some());
}

#[test]
fn convert_responses_tools_accepts_legacy_nested_shape() {
    let tools: Vec<super::types::ResponsesToolParam> = serde_json::from_value(serde_json::json!([
        {
            "type": "function",
            "function": {"name": "search", "parameters": {"type": "object"}}
        }
    ]))
    .unwrap();

    let schemas = super::helpers::convert_responses_tools(Some(tools)).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].function.name, "search");
}

#[test]
fn convert_responses_tools_skips_non_function_tool_types() {
    let tools: Vec<super::types::ResponsesToolParam> = serde_json::from_value(serde_json::json!([
        {"type": "web_search"},
        {"type": "function", "name": "search", "parameters": {"type": "object"}}
    ]))
    .unwrap();

    let schemas = super::helpers::convert_responses_tools(Some(tools)).unwrap();
    assert_eq!(schemas.len(), 1, "web_search skipped, function kept");
    assert_eq!(schemas[0].function.name, "search");
}

#[test]
fn convert_responses_tools_rejects_malformed_function_tool() {
    // A function-typed entry matching neither the flat nor the nested shape
    // (no name, no function object) must fail with a targeted message.
    let tools: Vec<super::types::ResponsesToolParam> =
        serde_json::from_value(serde_json::json!([{"type": "function"}])).unwrap();

    let err = super::helpers::convert_responses_tools(Some(tools))
        .expect_err("malformed function tool must be rejected");
    assert!(matches!(err, crate::error::AppError::BadRequest(_)));
}

#[test]
fn responses_input_function_call_output_accepts_content_part_array() {
    // The spec allows `output` to be an array of content parts, which used to
    // silently degrade to "" (#525).
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
            "type": "function_call_output",
            "call_id": "call_abc",
            "output": [
                {"type": "output_text", "text": "72"},
                {"type": "output_text", "text": "°F"}
            ]
        }
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Tool);
    assert_eq!(msgs[0].tool_call_id.as_deref(), Some("call_abc"));
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "72°F"),
        _ => panic!("expected text content"),
    }
}

#[test]
fn responses_input_skips_unknown_item_types() {
    // Codex round-trips `reasoning` items in `input`; they must be skipped,
    // not turned into empty user messages that pollute the prompt (#525).
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {"type": "reasoning", "id": "rs_1", "summary": []},
        {"role": "user", "content": "hello"}
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 1, "reasoning item skipped");
    assert_eq!(msgs[0].role, Role::User);
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "hello"),
        _ => panic!("expected text content"),
    }
}

#[test]
fn convert_responses_tools_normalizes_missing_parameters_to_empty_schema() {
    // An omitted `parameters` must not forward `parameters: null` upstream.
    let tools: Vec<super::types::ResponsesToolParam> = serde_json::from_value(serde_json::json!([
        {"type": "function", "name": "ping"}
    ]))
    .unwrap();

    let schemas = super::helpers::convert_responses_tools(Some(tools)).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(
        schemas[0].function.parameters,
        serde_json::json!({"type": "object", "properties": {}})
    );
}

#[test]
fn convert_responses_tools_rejects_entry_without_type() {
    // A tools[] entry with no `type` at all is garbage, not an unsupported
    // feature — fail fast like the old strict parsing did.
    let tools: Vec<super::types::ResponsesToolParam> =
        serde_json::from_value(serde_json::json!([{"name": "my_tool", "parameters": {}}])).unwrap();

    let err = super::helpers::convert_responses_tools(Some(tools))
        .expect_err("type-less tools entry must be rejected");
    assert!(matches!(err, crate::error::AppError::BadRequest(_)));
}

// #525 review: parallel tool calls round-tripped as consecutive function_call
// items must merge into ONE assistant message — one single-call assistant
// message per item is rejected by Chat-Completions upstreams ("assistant
// message with tool_calls must be followed by tool messages responding to
// each tool_call_id").
#[test]
fn responses_input_merges_parallel_function_calls_into_one_assistant_turn() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {"type": "function_call", "call_id": "c1", "name": "read", "arguments": "{\"p\":\"a\"}"},
        {"type": "function_call", "call_id": "c2", "name": "read", "arguments": "{\"p\":\"b\"}"},
        {"type": "function_call_output", "call_id": "c1", "output": "A"},
        {"type": "function_call_output", "call_id": "c2", "output": "B"}
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 3, "one assistant turn + two tool results");
    assert_eq!(msgs[0].role, Role::Assistant);
    let calls = msgs[0].tool_calls.as_ref().expect("tool_calls");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id, "c1");
    assert_eq!(calls[1].id, "c2");
    assert_eq!(msgs[1].role, Role::Tool);
    assert_eq!(msgs[1].tool_call_id.as_deref(), Some("c1"));
    assert_eq!(msgs[2].role, Role::Tool);
    assert_eq!(msgs[2].tool_call_id.as_deref(), Some("c2"));
}

// A function_call following the assistant's text message from the same turn
// attaches to that message (Chat-Completions puts tool_calls ON the assistant
// message that carries the turn's content).
#[test]
fn responses_input_attaches_function_call_to_preceding_assistant_message() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {"role": "assistant", "content": "Let me check."},
        {"type": "function_call", "call_id": "c1", "name": "search", "arguments": "{}"}
    ]))
    .unwrap();

    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::Assistant);
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "Let me check."),
        _ => panic!("expected text content"),
    }
    let calls = msgs[0].tool_calls.as_ref().expect("tool_calls attached");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, "c1");
}
