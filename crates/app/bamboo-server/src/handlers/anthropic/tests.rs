use actix_web::http::StatusCode;
use serde_json::json;
use std::collections::HashMap;

use bamboo_llm::api::models::{
    ChatCompletionResponse, ChatMessage, Content, ResponseChoice, Role, Usage,
};
use bamboo_llm::providers::anthropic::api_types::{
    AnthropicContent, AnthropicMessage, AnthropicMessagesRequest, AnthropicRole,
    AnthropicToolChoice,
};
use bamboo_llm::providers::anthropic::conversion::AnthropicConversionError;

use super::conversion::{convert_messages_request, convert_messages_response};
use super::errors::map_conversion_error;

#[test]
fn convert_messages_request_should_convert_basic_user_text() {
    let request = AnthropicMessagesRequest {
        model: "claude-3-5-sonnet-latest".to_string(),
        messages: vec![AnthropicMessage {
            role: AnthropicRole::User,
            content: AnthropicContent::Text("hello".to_string()),
        }],
        system: None,
        max_tokens: Some(64),
        temperature: None,
        top_p: None,
        top_k: None,
        stop_sequences: None,
        stream: Some(false),
        tools: None,
        tool_choice: None,
        extra: HashMap::new(),
    };

    let converted = convert_messages_request(request).expect("request conversion should work");

    assert_eq!(converted.messages.len(), 1);
    assert_eq!(converted.parameters.get("max_tokens"), Some(&json!(64)));
    assert_eq!(
        converted.messages[0].content,
        Content::Text("hello".to_string())
    );
}

#[test]
fn convert_messages_response_should_map_tool_calls_to_tool_use_stop_reason() {
    let response = ChatCompletionResponse {
        id: "chatcmpl-1".to_string(),
        object: Some("chat.completion".to_string()),
        created: Some(1),
        model: Some("gpt-4o".to_string()),
        choices: vec![ResponseChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content: Content::Text("hi".to_string()),
                phase: None,
                tool_calls: None,
                tool_call_id: None,
            },
            finish_reason: Some("tool_calls".to_string()),
        }],
        usage: Some(Usage {
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
        }),
        system_fingerprint: None,
    };

    let converted = convert_messages_response(response, "claude-3-5-sonnet-latest")
        .expect("response conversion should work");

    assert_eq!(converted.stop_reason, "tool_use");
}

#[test]
fn convert_messages_request_should_preserve_provider_error_status_and_type() {
    let request = AnthropicMessagesRequest {
        model: "claude-3-5-sonnet-latest".to_string(),
        messages: vec![AnthropicMessage {
            role: AnthropicRole::User,
            content: AnthropicContent::Text("hello".to_string()),
        }],
        system: None,
        max_tokens: None,
        temperature: None,
        top_p: None,
        top_k: None,
        stop_sequences: None,
        stream: Some(false),
        tools: None,
        tool_choice: Some(AnthropicToolChoice::String("unsupported".to_string())),
        extra: HashMap::new(),
    };

    let error =
        convert_messages_request(request).expect_err("conversion should reject bad tool_choice");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.error_type, "invalid_request_error");
    assert!(error.message.contains("Unsupported tool_choice value"));
}

#[test]
fn map_conversion_error_should_fallback_to_bad_gateway_for_invalid_status_code() {
    let mapped = map_conversion_error(AnthropicConversionError::new(
        1000,
        "api_error",
        "invalid upstream status".to_string(),
    ));

    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
}
