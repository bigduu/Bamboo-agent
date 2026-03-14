use super::*;

#[actix_web::test]
async fn test_workspace_endpoints_integration() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/workspace/validate",
                web::post().to(workspace::validate_workspace),
            )
            .route(
                "/v1/workspace/recent",
                web::get().to(workspace::get_recent_workspaces),
            )
            .route(
                "/v1/workspace/recent",
                web::post().to(workspace::add_recent_workspace),
            )
            .route(
                "/v1/workspace/suggestions",
                web::get().to(workspace::get_workspace_suggestions),
            )
            .route(
                "/v1/workspace/browse-folder",
                web::post().to(workspace::browse_folder),
            )
            .route(
                "/v1/workspace/files",
                web::post().to(workspace::list_workspace_files),
            ),
    )
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    std::fs::write(temp_dir.path().join("test.txt"), "content").expect("Failed to write test.txt");

    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let req = test::TestRequest::post()
        .uri("/v1/workspace/recent")
        .set_json(json!({
            "path": temp_path,
            "metadata": {
                "workspace_name": "Integration Test"
            }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NO_CONTENT);

    let req = test::TestRequest::get()
        .uri("/v1/workspace/recent")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");
    let workspaces = result.as_array().expect("Result should be array");
    assert!(!workspaces.is_empty());

    let req = test::TestRequest::get()
        .uri("/v1/workspace/suggestions")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}
