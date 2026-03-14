use super::*;

#[actix_web::test]
async fn test_get_recent_workspaces_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/recent",
        web::get().to(workspace::get_recent_workspaces),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/workspace/recent")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert!(result.is_array());
}

#[actix_web::test]
async fn test_add_recent_workspace_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/workspace/recent",
                web::post().to(workspace::add_recent_workspace),
            )
            .route(
                "/v1/workspace/recent",
                web::get().to(workspace::get_recent_workspaces),
            ),
    )
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let req = test::TestRequest::post()
        .uri("/v1/workspace/recent")
        .set_json(json!({
            "path": temp_path,
            "metadata": {
                "workspace_name": "Test Workspace",
                "description": "A test workspace",
                "tags": ["test", "e2e"]
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NO_CONTENT);

    let req = test::TestRequest::get()
        .uri("/v1/workspace/recent")
        .to_request();
    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_array());
    let workspaces = result.as_array().expect("Result should be array");
    assert!(!workspaces.is_empty());

    let found = workspaces
        .iter()
        .any(|workspace| workspace["path"] == temp_path);
    assert!(found, "Added workspace should be in recent list");
}

#[actix_web::test]
async fn test_add_recent_workspace_updates_existing() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/recent",
        web::post().to(workspace::add_recent_workspace),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let req = test::TestRequest::post()
        .uri("/v1/workspace/recent")
        .set_json(json!({
            "path": temp_path,
            "metadata": {
                "workspace_name": "First Name"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NO_CONTENT);

    let req = test::TestRequest::post()
        .uri("/v1/workspace/recent")
        .set_json(json!({
            "path": temp_path,
            "metadata": {
                "workspace_name": "Updated Name"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NO_CONTENT);
}
