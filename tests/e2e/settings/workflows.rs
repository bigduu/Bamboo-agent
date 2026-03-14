use super::*;

#[actix_web::test]
async fn test_list_workflows_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::get().to(settings::list_workflows),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_list_workflows_returns_json_array() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::get().to(settings::list_workflows),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_array());
}

#[actix_web::test]
async fn test_create_and_get_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/workflows",
                web::post().to(settings::save_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::get().to(settings::get_workflow),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(&json!({
            "name": "test-workflow",
            "content": "# Test Workflow\n\nThis is a test workflow."
        }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/test-workflow")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["name"], "test-workflow");
    assert!(result["content"]
        .as_str()
        .expect("content should be string")
        .contains("# Test Workflow"));
}

#[actix_web::test]
async fn test_delete_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/workflows",
                web::post().to(settings::save_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::delete().to(settings::delete_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::get().to(settings::get_workflow),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(&json!({
            "name": "workflow-to-delete",
            "content": "# Workflow to Delete"
        }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    let delete_req = test::TestRequest::delete()
        .uri("/v1/bamboo/workflows/workflow-to-delete")
        .to_request();

    let delete_resp = test::call_service(&app, delete_req).await;
    assert!(delete_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/workflow-to-delete")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert_eq!(get_resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_get_nonexistent_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows/{name}",
        web::get().to(settings::get_workflow),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/nonexistent-workflow")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}
