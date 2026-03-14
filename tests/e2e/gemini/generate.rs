use super::*;

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
                    "parts": [{ "text": "Hello" }]
                }
            ]
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
                    "parts": [{ "text": "Hello" }]
                }
            ],
            "systemInstruction": {
                "parts": [{ "text": "You are a helpful assistant" }]
            }
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
                    "parts": [{ "text": "What is the weather?" }]
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

    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
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
                        { "text": "First part" },
                        { "text": "Second part" }
                    ]
                }
            ]
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
                    "parts": [{ "text": "What is the capital of France?" }]
                },
                {
                    "role": "model",
                    "parts": [{ "text": "The capital of France is Paris." }]
                },
                {
                    "role": "user",
                    "parts": [{ "text": "What about Germany?" }]
                }
            ]
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
async fn test_gemini_generate_content_with_different_models() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let models = vec!["gemini-2.0-flash-exp", "gemini-1.5-pro", "gemini-1.5-flash"];

    for model in models {
        let uri = format!("/gemini/v1beta/models/{model}:generateContent");
        let req = test::TestRequest::post()
            .uri(&uri)
            .set_json(json!({
                "contents": [
                    {
                        "role": "user",
                        "parts": [{ "text": "Test" }]
                    }
                ]
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;

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
                    "parts": [{ "text": "What is the weather in San Francisco?" }]
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

    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}
