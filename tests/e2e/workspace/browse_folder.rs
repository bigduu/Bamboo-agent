use super::*;

#[actix_web::test]
async fn test_browse_folder_endpoint_default() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/browse-folder",
        web::post().to(workspace::browse_folder),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({}))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_object());
    assert!(result.get("current_path").is_some());
    assert!(result.get("folders").is_some());
    assert!(result["folders"].is_array());
}

#[actix_web::test]
async fn test_browse_folder_endpoint_with_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/browse-folder",
        web::post().to(workspace::browse_folder),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    std::fs::create_dir_all(temp_dir.path().join("folder1")).expect("Failed to create folder1");
    std::fs::create_dir_all(temp_dir.path().join("folder2")).expect("Failed to create folder2");

    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({ "path": temp_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result["current_path"].is_string());
    let current_path = result["current_path"]
        .as_str()
        .expect("current_path should be string");
    assert!(
        current_path.contains("tmp") || current_path.contains("TMP"),
        "Should be in temp directory"
    );

    let folders = result["folders"]
        .as_array()
        .expect("folders should be array");
    assert!(
        folders.len() >= 2,
        "Should have at least the two created folders"
    );
}

#[actix_web::test]
async fn test_browse_folder_endpoint_invalid_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/browse-folder",
        web::post().to(workspace::browse_folder),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({ "path": "/nonexistent/path" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_browse_folder_rejects_traversal() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/browse-folder",
        web::post().to(workspace::browse_folder),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({ "path": "/tmp/../etc" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}
