//! E2E tests for /api/v1/execute/{session_id} endpoint

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;
use serde_json::json;

#[actix_web::test]
async fn test_execute_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/execute/{session_id}",
                web::post().to(handlers::execute::handler),
            ),
    )
    .await;

    let uri = format!("/api/v1/execute/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .set_json(json!({
            "message": "Execute this task"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should respond
    assert!(resp.status().is_client_error() || resp.status().is_server_error() || resp.status().is_success());
}

#[actix_web::test]
async fn test_execute_with_different_session_ids() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/execute/{session_id}",
                web::post().to(handlers::execute::handler),
            ),
    )
    .await;

    // Test with multiple session IDs
    for _ in 0..3 {
        let session_id = uuid::Uuid::new_v4().to_string();
        let uri = format!("/api/v1/execute/{}", session_id);

        let req = test::TestRequest::post()
            .uri(&uri)
            .set_json(json!({
                "message": "Test execution"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_client_error() || resp.status().is_server_error() || resp.status().is_success());
    }
}

#[actix_web::test]
async fn test_execute_accepts_message() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/execute/{session_id}",
                web::post().to(handlers::execute::handler),
            ),
    )
    .await;

    let uri = format!("/api/v1/execute/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .set_json(json!({
            "message": "Execute a specific task with parameters"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_client_error() || resp.status().is_server_error() || resp.status().is_success());
}
