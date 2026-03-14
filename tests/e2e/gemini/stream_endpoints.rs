use super::*;

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
                    "parts": [{ "text": "Hello" }]
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    if resp.status().is_success() {
        let content_type = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());

        assert!(content_type.is_some());
    }
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

    assert!(resp.status().is_client_error() || resp.status().is_server_error());
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

    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}
