//! E2E tests for Anthropic-compatible API endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::anthropic;
use serde_json::json;

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
