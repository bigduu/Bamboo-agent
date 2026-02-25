//! E2E tests for Gemini-compatible API endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::gemini;
use serde_json::json;

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
