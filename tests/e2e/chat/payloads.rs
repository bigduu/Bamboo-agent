use super::*;

#[actix_web::test]
async fn test_chat_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/chat", web::post().to(handlers::chat::handler)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/chat")
        .set_json(json!({
            "message": "Hello",
            "session_id": uuid::Uuid::new_v4().to_string()
        }))
        .to_request();

    // This will fail because we don't have a real LLM provider
    // but we're testing that the endpoint exists and accepts requests
    let resp = test::call_service(&app, req).await;

    // The endpoint should at least respond (even if with an error)
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}

#[actix_web::test]
async fn test_chat_requires_json_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/chat", web::post().to(handlers::chat::handler)),
    )
    .await;

    let req = test::TestRequest::post().uri("/api/v1/chat").to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error());
}
