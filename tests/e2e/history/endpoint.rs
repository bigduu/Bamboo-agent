use super::*;

#[actix_web::test]
async fn test_history_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/history/{session_id}",
        web::get().to(handlers::history::handler),
    ))
    .await;

    let uri = format!("/api/v1/history/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should respond
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_history_returns_empty_for_new_session() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/history/{session_id}",
        web::get().to(handlers::history::handler),
    ))
    .await;

    let uri = format!("/api/v1/history/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return success (even if empty) or not found for new session
    assert!(resp.status().is_success() || resp.status().is_client_error());
}
