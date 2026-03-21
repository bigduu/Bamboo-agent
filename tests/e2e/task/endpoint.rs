use super::*;

#[actix_web::test]
async fn test_get_task_list_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/task/{session_id}",
        web::get().to(handlers::task::get_task_list),
    ))
    .await;

    let uri = format!("/api/v1/task/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return task list or appropriate error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_has_task_list_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/task/{session_id}/exists",
        web::get().to(handlers::task::has_task_list),
    ))
    .await;

    let uri = format!("/api/v1/task/{}/exists", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return boolean or appropriate status
    assert!(resp.status().is_success() || resp.status().is_client_error());
}
