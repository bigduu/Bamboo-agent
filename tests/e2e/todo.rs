//! E2E tests for /api/v1/todo/* endpoints

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;

#[actix_web::test]
async fn test_get_todo_list_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/todo/{session_id}",
                web::get().to(handlers::todo::get_todo_list),
            ),
    )
    .await;

    let uri = format!("/api/v1/todo/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return todo list or appropriate error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_has_todo_list_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/todo/{session_id}/exists",
                web::get().to(handlers::todo::has_todo_list),
            ),
    )
    .await;

    let uri = format!("/api/v1/todo/{}/exists", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return boolean or appropriate status
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_todo_endpoints_with_different_sessions() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/todo/{session_id}",
                web::get().to(handlers::todo::get_todo_list),
            )
            .route(
                "/api/v1/todo/{session_id}/exists",
                web::get().to(handlers::todo::has_todo_list),
            ),
    )
    .await;

    // Test multiple sessions
    for _ in 0..3 {
        let session_id = uuid::Uuid::new_v4().to_string();

        // Test get todo list
        let uri = format!("/api/v1/todo/{}", session_id);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success() || resp.status().is_client_error());

        // Test has todo list
        let uri = format!("/api/v1/todo/{}/exists", session_id);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success() || resp.status().is_client_error());
    }
}
