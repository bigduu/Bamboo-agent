use super::*;

#[actix_web::test]
async fn test_list_workspace_files_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/files",
        web::post().to(workspace::list_workspace_files),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    std::fs::write(temp_dir.path().join("file1.txt"), "content1").expect("Failed to write file1");
    std::fs::write(temp_dir.path().join("file2.rs"), "fn main() {}")
        .expect("Failed to write file2");

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({ "path": temp_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_array());
    let files = result.as_array().expect("Result should be array");
    assert!(
        files.len() >= 2,
        "Should have at least the two created files"
    );
}

#[actix_web::test]
async fn test_list_workspace_files_with_options() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/files",
        web::post().to(workspace::list_workspace_files),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    std::fs::create_dir_all(temp_dir.path().join("subdir1")).expect("Failed to create subdir1");
    std::fs::write(temp_dir.path().join("file1.txt"), "content").expect("Failed to write file1");
    std::fs::write(temp_dir.path().join("subdir1").join("file2.txt"), "content")
        .expect("Failed to write file2");

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({
            "path": temp_path,
            "max_depth": 2,
            "max_entries": 100,
            "include_hidden": false
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_array());
}

#[actix_web::test]
async fn test_list_workspace_files_invalid_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/files",
        web::post().to(workspace::list_workspace_files),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({ "path": "/nonexistent/path" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_list_workspace_files_respects_max_entries() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/files",
        web::post().to(workspace::list_workspace_files),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    for i in 0..20 {
        std::fs::write(temp_dir.path().join(format!("file{i}.txt")), "content")
            .expect("Failed to write file");
    }

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({
            "path": temp_path,
            "max_entries": 5
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    let files = result.as_array().expect("Result should be array");
    assert!(files.len() <= 5, "Should respect max_entries limit");
}

#[actix_web::test]
async fn test_list_workspace_files_skips_ignored_dirs() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/files",
        web::post().to(workspace::list_workspace_files),
    ))
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    std::fs::create_dir_all(temp_dir.path().join("node_modules"))
        .expect("Failed to create node_modules");
    std::fs::create_dir_all(temp_dir.path().join(".git")).expect("Failed to create .git");
    std::fs::write(
        temp_dir.path().join("node_modules").join("package.js"),
        "content",
    )
    .expect("Failed to write package.js");
    std::fs::write(temp_dir.path().join(".git").join("config"), "content")
        .expect("Failed to write config");

    std::fs::create_dir_all(temp_dir.path().join("src")).expect("Failed to create src");
    std::fs::write(temp_dir.path().join("src").join("main.rs"), "fn main() {}")
        .expect("Failed to write main.rs");

    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({ "path": temp_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    let files = result.as_array().expect("Result should be array");

    let has_node_modules = files
        .iter()
        .any(|file| file["path"].as_str().unwrap_or("").contains("node_modules"));
    let has_git = files
        .iter()
        .any(|file| file["path"].as_str().unwrap_or("").contains(".git"));

    assert!(
        !has_node_modules,
        "Should not include files from node_modules"
    );
    assert!(!has_git, "Should not include files from .git");
}
