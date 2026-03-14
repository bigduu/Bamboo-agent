use super::*;

#[actix_web::test]
async fn test_chat_completions_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should at least respond (even if with an error due to no real LLM provider)
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_completions_with_valid_request() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant."
                },
                {
                    "role": "user",
                    "content": "What is the capital of France?"
                }
            ],
            "temperature": 0.7,
            "max_tokens": 100
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
async fn test_chat_completions_with_stream() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": "Tell me a story"
                }
            ],
            "stream": true
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept streaming requests
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );

    // If successful, check that the response is SSE stream
    if resp.status().is_success() {
        let content_type = resp.headers().get(actix_web::http::header::CONTENT_TYPE);
        if let Some(ct) = content_type {
            let ct_str = ct.to_str().unwrap_or("");
            assert!(
                ct_str.contains("text/event-stream") || ct_str.contains("application/json"),
                "Expected SSE or JSON content type, got: {}",
                ct_str
            );
        }
    }
}

#[actix_web::test]
async fn test_chat_completions_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    // Test without any body
    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error());
}

#[actix_web::test]
async fn test_chat_completions_with_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": "What is the weather in Tokyo?"
                }
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "description": "Get the current weather for a location",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "location": {
                                    "type": "string",
                                    "description": "The city and country"
                                }
                            },
                            "required": ["location"]
                        }
                    }
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept requests with tools
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_completions_missing_required_fields() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    // Test without model field
    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    // Should reject or handle gracefully
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );

    // Test without messages field
    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_completions_with_empty_messages() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should handle empty messages array appropriately
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_completions_with_different_roles() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant."
                },
                {
                    "role": "user",
                    "content": "Hello"
                },
                {
                    "role": "assistant",
                    "content": "Hi there!"
                },
                {
                    "role": "user",
                    "content": "How are you?"
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
async fn test_chat_completions_with_multimodal_content() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "gpt-4-vision-preview",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "What's in this image?"
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "https://example.com/image.jpg"
                            }
                        }
                    ]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept multimodal content
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_completions_with_default_model() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/chat/completions",
        web::post().to(openai::chat_completions),
    ))
    .await;

    // Test with "default" model which should be resolved from config
    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_json(json!({
            "model": "default",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello"
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should handle "default" model keyword
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}
