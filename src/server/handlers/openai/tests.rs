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
