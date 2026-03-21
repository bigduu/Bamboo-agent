use super::*;

#[actix_web::test]
async fn test_task_endpoints_with_different_sessions() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/task/{session_id}",
                web::get().to(handlers::task::get_task_list),
            )
            .route(
                "/api/v1/task/{session_id}/exists",
                web::get().to(handlers::task::has_task_list),
            ),
    )
    .await;

    // Test multiple sessions
    for _ in 0..3 {
        let session_id = uuid::Uuid::new_v4().to_string();

        // Test get task list
        let uri = format!("/api/v1/task/{}", session_id);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success() || resp.status().is_client_error());

        // Test has task list
        let uri = format!("/api/v1/task/{}/exists", session_id);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success() || resp.status().is_client_error());
    }
}
