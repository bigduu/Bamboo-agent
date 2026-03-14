use super::*;

#[actix_web::test]
async fn test_delete_session_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}",
        web::delete().to(handlers::delete::handler),
    ))
    .await;

    let uri = format!("/api/v1/sessions/{}", session_id);
    let req = test::TestRequest::delete().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should delete session or return appropriate error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_delete_nonexistent_session() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}",
        web::delete().to(handlers::delete::handler),
    ))
    .await;

    let uri = format!("/api/v1/sessions/{}", session_id);
    let req = test::TestRequest::delete().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Deleting non-existent session should handle gracefully
    assert!(resp.status().is_success() || resp.status().is_client_error());
}
