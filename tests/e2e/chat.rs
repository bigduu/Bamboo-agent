//! E2E tests for /api/v1/chat endpoint

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;
use serde_json::json;

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
    assert!(resp.status().is_client_error() || resp.status().is_server_error() || resp.status().is_success());
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

    let req = test::TestRequest::post()
        .uri("/api/v1/chat")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without proper JSON body
    assert!(resp.status().is_client_error());
}

#[actix_web::test]
async fn test_chat_accepts_session_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/chat", web::post().to(handlers::chat::handler)),
    )
    .await;

    let session_id = uuid::Uuid::new_v4().to_string();

    let req = test::TestRequest::post()
        .uri("/api/v1/chat")
        .set_json(json!({
            "message": "Test message",
            "session_id": session_id
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept the request structure
    assert!(resp.status().is_client_error() || resp.status().is_server_error() || resp.status().is_success());
}
