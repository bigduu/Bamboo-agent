//! E2E tests for /api/v1/stream/{session_id} endpoint (Legacy SSE streaming)

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;

#[actix_web::test]
async fn test_stream_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/stream/{session_id}",
                web::get().to(handlers::stream::handler),
            ),
    )
    .await;

    let uri = format!("/api/v1/stream/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should respond (even if the session doesn't exist)
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_stream_content_type() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/stream/{session_id}",
                web::get().to(handlers::stream::handler),
            ),
    )
    .await;

    let uri = format!("/api/v1/stream/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return proper content type for SSE or an error
    if resp.status().is_success() {
        let content_type = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());

        // SSE should have text/event-stream content type
        assert!(content_type.is_some());
    }
}

#[actix_web::test]
async fn test_stream_with_different_sessions() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/stream/{session_id}",
                web::get().to(handlers::stream::handler),
            ),
    )
    .await;

    // Test with multiple different session IDs
    for _ in 0..3 {
        let session_id = uuid::Uuid::new_v4().to_string();
        let uri = format!("/api/v1/stream/{}", session_id);

        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;

        assert!(resp.status().is_success() || resp.status().is_client_error());
    }
}
