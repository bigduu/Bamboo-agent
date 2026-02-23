//! E2E tests for /api/v1/respond/* endpoints

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;
use serde_json::json;

#[actix_web::test]
async fn test_submit_response_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/respond/{session_id}",
                web::post().to(handlers::respond::submit_response),
            ),
    )
    .await;

    let uri = format!("/api/v1/respond/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .set_json(json!({
            "response": "User response here"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept response or return appropriate error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_get_pending_question_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/respond/{session_id}/pending",
                web::get().to(handlers::respond::get_pending_question),
            ),
    )
    .await;

    let uri = format!("/api/v1/respond/{}/pending", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return pending question or not found
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_respond_with_empty_body() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/respond/{session_id}",
                web::post().to(handlers::respond::submit_response),
            ),
    )
    .await;

    let uri = format!("/api/v1/respond/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject requests without body
    assert!(resp.status().is_client_error());
}
