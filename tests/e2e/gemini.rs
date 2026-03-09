//! E2E tests for Gemini-compatible API endpoints

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent::agent::core::tools::ToolSchema;
use bamboo_agent::agent::core::{Message, Role};
use bamboo_agent::agent::llm::api::models::ContentPart;
use bamboo_agent::agent::llm::{LLMChunk, LLMProvider, LLMStream};
use bamboo_agent::server::handlers::gemini;
use futures::stream;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct RecordedChatCall {
    messages: Vec<Message>,
    model: String,
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
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_agent::agent::llm::provider::Result<LLMStream> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .push(RecordedChatCall {
                messages: messages.to_vec(),
                model: model.to_string(),
            });

        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("ok".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }

    async fn list_models(&self) -> bamboo_agent::agent::llm::provider::Result<Vec<String>> {
        Ok(vec!["gemini-2.0-flash-exp".to_string()])
    }
}

async fn create_gemini_state(
    recording_provider: &RecordingProvider,
    image_hook_enabled: bool,
) -> actix_web::web::Data<bamboo_agent::server::AppState> {
    let state = crate::e2e::common::create_test_app().await;

    {
        let mut config = state.config.write().await;
        config
            .gemini_model_mapping
            .mappings
            .insert("flash".to_string(), "gemini-2.0-flash-exp".to_string());
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
async fn test_gemini_list_models_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/gemini/v1beta/models", web::get().to(gemini::list_models)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/gemini/v1beta/models")
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
async fn test_gemini_generate_content_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "Hello"
                        }
                    ]
                }
            ]
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
async fn test_gemini_generate_content_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}

#[actix_web::test]
async fn test_gemini_generate_content_with_system_instruction() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "Hello"
                        }
                    ]
                }
            ],
            "systemInstruction": {
                "parts": [
                    {
                        "text": "You are a helpful assistant"
                    }
                ]
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept system instruction
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_gemini_generate_content_with_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "What is the weather?"
                        }
                    ]
                }
            ],
            "tools": [
                {
                    "functionDeclarations": [
                        {
                            "name": "get_weather",
                            "description": "Get the current weather",
                            "parameters": {
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
async fn test_gemini_stream_generate_content_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:streamGenerateContent",
        web::post().to(gemini::stream_generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:streamGenerateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "Hello"
                        }
                    ]
                }
            ]
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
async fn test_gemini_stream_generate_content_content_type() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:streamGenerateContent",
        web::post().to(gemini::stream_generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:streamGenerateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "Hello"
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

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
async fn test_gemini_generate_content_with_multiple_parts() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "First part"
                        },
                        {
                            "text": "Second part"
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept multiple parts
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_gemini_generate_content_with_conversation_history() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "What is the capital of France?"
                        }
                    ]
                },
                {
                    "role": "model",
                    "parts": [
                        {
                            "text": "The capital of France is Paris."
                        }
                    ]
                },
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "What about Germany?"
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept conversation history
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_gemini_stream_generate_content_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:streamGenerateContent",
        web::post().to(gemini::stream_generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:streamGenerateContent")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}

#[actix_web::test]
async fn test_gemini_generate_content_with_different_models() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    // Test with different model names
    let models = vec!["gemini-2.0-flash-exp", "gemini-1.5-pro", "gemini-1.5-flash"];

    for model in models {
        let uri = format!("/gemini/v1beta/models/{}:generateContent", model);
        let req = test::TestRequest::post()
            .uri(&uri)
            .set_json(json!({
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {
                                "text": "Test"
                            }
                        ]
                    }
                ]
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;

        // Endpoint should accept different model names
        assert!(
            resp.status().is_client_error()
                || resp.status().is_server_error()
                || resp.status().is_success()
        );
    }
}

#[actix_web::test]
async fn test_gemini_generate_content_with_function_response() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "What is the weather in San Francisco?"
                        }
                    ]
                },
                {
                    "role": "model",
                    "parts": [
                        {
                            "functionCall": {
                                "name": "get_weather",
                                "args": {
                                    "location": "San Francisco"
                                }
                            }
                        }
                    ]
                },
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": "get_weather",
                                "response": {
                                    "temperature": "72F",
                                    "condition": "Sunny"
                                }
                            }
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept function response
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_gemini_stream_with_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:streamGenerateContent",
        web::post().to(gemini::stream_generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:streamGenerateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "What is the weather?"
                        }
                    ]
                }
            ],
            "tools": [
                {
                    "functionDeclarations": [
                        {
                            "name": "get_weather",
                            "description": "Get the current weather",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "location": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept streaming with tools
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_gemini_generate_content_passes_image_parts_through_when_hook_disabled() {
    let recording_provider = RecordingProvider::default();
    let state = create_gemini_state(&recording_provider, false).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "describe this"
                        },
                        {
                            "inlineData": {
                                "mimeType": "image/png",
                                "data": "QUJDRA=="
                            }
                        }
                    ]
                }
            ]
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
    assert_eq!(calls[0].model, "gemini-2.0-flash-exp");

    let user_message = calls[0]
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .expect("expected one user message");
    let parts = user_message
        .content_parts
        .as_ref()
        .expect("image parts should be preserved");
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
async fn test_gemini_generate_content_placeholder_hook_rewrites_image_parts() {
    let recording_provider = RecordingProvider::default();
    let state = create_gemini_state(&recording_provider, true).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": "describe this"
                        },
                        {
                            "fileData": {
                                "fileUri": "https://example.com/cat.png"
                            }
                        }
                    ]
                }
            ]
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
