use super::*;

#[actix_web::test]
async fn test_validate_workspace_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/validate",
        web::post().to(workspace::validate_workspace),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": temp_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_object());
    assert!(result.get("path").is_some());
    assert!(result.get("is_valid").is_some());
}

#[actix_web::test]
async fn test_validate_workspace_with_valid_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/validate",
        web::post().to(workspace::validate_workspace),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": temp_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["is_valid"], true);
    assert!(result["workspace_name"].is_string());
}

#[actix_web::test]
async fn test_validate_workspace_with_invalid_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/validate",
        web::post().to(workspace::validate_workspace),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": "/nonexistent/path/that/does/not/exist" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["is_valid"], false);
    assert!(result["error_message"].is_string());
}

#[actix_web::test]
async fn test_validate_workspace_with_empty_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/validate",
        web::post().to(workspace::validate_workspace),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": "" }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}
