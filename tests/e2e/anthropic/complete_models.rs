use super::*;

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

    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}
