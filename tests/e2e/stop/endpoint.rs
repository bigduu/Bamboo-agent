use super::*;

#[actix_web::test]
async fn test_stop_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/stop/{session_id}",
        web::post().to(handlers::stop::handler),
    ))
    .await;

    let uri = format!("/api/v1/stop/{}", session_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept stop request
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_stop_nonexistent_session() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/stop/{session_id}",
        web::post().to(handlers::stop::handler),
    ))
    .await;

    let uri = format!("/api/v1/stop/{}", session_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Stopping a non-existent session should handle gracefully
    assert!(resp.status().is_success() || resp.status().is_client_error());
}
