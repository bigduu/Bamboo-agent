use actix_web::http::StatusCode;
use actix_web::{web, App};

use crate::agent::llm::api::models::{Content, ContentPart, FunctionCall, Role, ToolCall};

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

    assert_eq!(
        msgs[0].tool_call_id.as_deref(),
        Some("call_abc")
    );
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
