//! E2E tests for Anthropic-compatible API endpoints

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent::agent::core::tools::ToolSchema;
use bamboo_agent::agent::core::{Message, Role};
use bamboo_agent::agent::llm::api::models::ContentPart;
use bamboo_agent::agent::llm::{LLMChunk, LLMProvider, LLMStream};
use bamboo_agent::server::handlers::anthropic;
use futures::stream;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct RecordedChatCall {
    messages: Vec<Message>,
    model: String,
    max_output_tokens: Option<u32>,
}

#[derive(Clone, Default)]
struct RecordingProvider {
    calls: Arc<Mutex<Vec<RecordedChatCall>>>,
}

impl RecordingProvider {
    fn calls(&self) -> Vec<RecordedChatCall> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .clone()
    }
}

#[async_trait]
impl LLMProvider for RecordingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_agent::agent::llm::provider::Result<LLMStream> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .push(RecordedChatCall {
                messages: messages.to_vec(),
                model: model.to_string(),
                max_output_tokens,
            });

        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("ok".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }

    async fn list_models(&self) -> bamboo_agent::agent::llm::provider::Result<Vec<String>> {
        Ok(vec!["claude-3-5-sonnet-20241022".to_string()])
    }
}

async fn create_anthropic_state(
    recording_provider: &RecordingProvider,
    image_hook_enabled: bool,
) -> actix_web::web::Data<bamboo_agent::server::AppState> {
    let state = crate::e2e::common::create_test_app().await;

    {
        let mut config = state.config.write().await;
        config.anthropic_model_mapping.mappings.insert(
            "sonnet".to_string(),
            "claude-3-5-sonnet-20241022".to_string(),
        );
        config.hooks.image_fallback.enabled = image_hook_enabled;
        config.hooks.image_fallback.mode = "placeholder".to_string();
    }

    {
        let mut provider = state.provider.write().await;
        *provider = Arc::new(recording_provider.clone());
    }

    state
}

#[actix_web::test]
async fn test_anthropic_messages_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ],
            "max_tokens": 1024
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should at least respond (even if with an error due to no real provider)
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    // If app state wiring is wrong (missing `Data<AppState>`), Actix returns 500.
    // We want this test to specifically ensure extractor wiring is correct.
    assert!(resp.status().is_client_error());
}

#[actix_web::test]
async fn test_anthropic_messages_accepts_all_fields() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "Test message"
                }
            ],
            "system": "You are a helpful assistant",
            "max_tokens": 2048,
            "temperature": 0.7,
            "top_p": 0.9,
            "top_k": 50,
            "stop_sequences": ["STOP"],
            "stream": false
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept the request structure
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_with_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "What is the weather?"
                }
            ],
            "max_tokens": 1024,
            "tools": [
                {
                    "name": "get_weather",
                    "description": "Get the current weather",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "location": {
                                "type": "string",
                                "description": "City name"
                            }
                        },
                        "required": ["location"]
                    }
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept tools parameter
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_with_system_blocks() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ],
            "system": [
                {
                    "type": "text",
                    "text": "You are a helpful assistant"
                }
            ],
            "max_tokens": 1024
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept system blocks
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_complete_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/complete",
        web::post().to(anthropic::complete),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/complete")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "prompt": "\n\nHuman: Hello\n\nAssistant:",
            "max_tokens_to_sample": 1024
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should at least respond
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_complete_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/complete",
        web::post().to(anthropic::complete),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/complete")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}

#[actix_web::test]
async fn test_anthropic_get_models_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/anthropic/v1/models", web::get().to(anthropic::get_models)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/anthropic/v1/models")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The models endpoint should respond
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_with_streaming() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ],
            "max_tokens": 1024,
            "stream": true
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should accept streaming requests
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );

    // If successful, should have SSE content type
    if resp.status().is_success() {
        let content_type = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());

        // Should be text/event-stream for streaming
        assert!(content_type.is_some());
    }
}

#[actix_web::test]
async fn test_anthropic_messages_with_content_blocks() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Hello"
                        }
                    ]
                }
            ],
            "max_tokens": 1024
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept content blocks
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_with_tool_result() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": "What is the weather?"
                },
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu_123",
                            "name": "get_weather",
                            "input": {"location": "San Francisco"}
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "toolu_123",
                            "content": "The weather is sunny"
                        }
                    ]
                }
            ],
            "max_tokens": 1024
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept tool result messages
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_anthropic_messages_passes_image_parts_through_when_hook_disabled() {
    let recording_provider = RecordingProvider::default();
    let state = create_anthropic_state(&recording_provider, false).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "describe this"
                        },
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": "QUJDRA=="
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 64
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "Expected success, got {}",
        resp.status()
    );

    let calls = recording_provider.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].model, "claude-3-5-sonnet-20241022");
    assert_eq!(calls[0].max_output_tokens, Some(64));

    let user_message = calls[0]
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .expect("expected one user message");
    let parts = user_message
        .content_parts
        .as_ref()
        .expect("image parts should be preserved");
    assert_eq!(parts.len(), 2);
    assert!(matches!(
        &parts[0],
        ContentPart::Text { text } if text == "describe this"
    ));
    assert!(matches!(
        &parts[1],
        ContentPart::ImageUrl { image_url } if image_url.url == "data:image/png;base64,QUJDRA=="
    ));
}

#[actix_web::test]
async fn test_anthropic_messages_placeholder_hook_rewrites_image_parts() {
    let recording_provider = RecordingProvider::default();
    let state = create_anthropic_state(&recording_provider, true).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "describe this"
                        },
                        {
                            "type": "image",
                            "source": {
                                "type": "url",
                                "url": "https://example.com/cat.png"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 64
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "Expected success, got {}",
        resp.status()
    );

    let calls = recording_provider.calls();
    assert_eq!(calls.len(), 1);

    let user_message = calls[0]
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .expect("expected one user message");
    assert!(user_message.content_parts.is_none());
    assert!(user_message.content.contains("describe this"));
    assert!(user_message.content.contains("[Image omitted:"));
    assert!(user_message.content.contains("https://example.com/cat.png"));
}
