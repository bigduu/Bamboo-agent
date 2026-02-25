//! E2E tests for /v1/workspace/* endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::app_state::AppState;
use bamboo_agent::server::handlers::workspace;
use serde_json::json;
use tempfile::TempDir;

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

    // Add a workspace
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

    // Verify it was added
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

    let found = workspaces.iter().any(|w| w["path"] == temp_path);
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

    // Add workspace first time
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

    // Add same workspace again with different metadata
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

#[actix_web::test]
async fn test_get_workspace_suggestions_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/suggestions",
        web::get().to(workspace::get_workspace_suggestions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/workspace/suggestions")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_object());
    assert!(result.get("suggestions").is_some());
    assert!(result["suggestions"].is_array());

    let suggestions = result["suggestions"]
        .as_array()
        .expect("suggestions should be array");
    assert!(
        !suggestions.is_empty(),
        "Should have at least home suggestion"
    );

    // Home should always be present
    let has_home = suggestions.iter().any(|s| s["suggestion_type"] == "home");
    assert!(has_home, "Should include home suggestion");
}

#[actix_web::test]
async fn test_get_workspace_suggestions_includes_recent() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/workspace/recent",
                web::post().to(workspace::add_recent_workspace),
            )
            .route(
                "/v1/workspace/suggestions",
                web::get().to(workspace::get_workspace_suggestions),
            ),
    )
    .await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    // Add a recent workspace
    let req = test::TestRequest::post()
        .uri("/v1/workspace/recent")
        .set_json(json!({
            "path": temp_path,
            "metadata": {
                "workspace_name": "Recent Test Workspace"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NO_CONTENT);

    // Get suggestions
    let req = test::TestRequest::get()
        .uri("/v1/workspace/suggestions")
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    let suggestions = result["suggestions"]
        .as_array()
        .expect("suggestions should be array");
    let has_recent = suggestions
        .iter()
        .any(|s| s["path"] == temp_path && s["suggestion_type"] == "recent");

    assert!(has_recent, "Should include recent workspace in suggestions");
}

#[actix_web::test]
async fn test_browse_folder_endpoint_default() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/workspace/browse-folder",
        web::post().to(workspace::browse_folder),
    ))
    .await;

    // Browse with no path - should default to home
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

    // Create some subdirectories
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

    // Paths may be canonicalized (macOS temp dirs are symlinks)
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

    // Create some files
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

    // Create nested structure
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

    // Create many files
    for i in 0..20 {
        std::fs::write(temp_dir.path().join(format!("file{}.txt", i)), "content")
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

    // Create ignored directories
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

    // Create a regular directory
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

    // Should not include files from ignored directories
    let has_node_modules = files
        .iter()
        .any(|f| f["path"].as_str().unwrap_or("").contains("node_modules"));
    let has_git = files
        .iter()
        .any(|f| f["path"].as_str().unwrap_or("").contains(".git"));

    assert!(
        !has_node_modules,
        "Should not include files from node_modules"
    );
    assert!(!has_git, "Should not include files from .git");
}

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

    // Create files
    std::fs::write(temp_dir.path().join("test.txt"), "content").expect("Failed to write test.txt");

    // 1. Validate workspace
    let req = test::TestRequest::post()
        .uri("/v1/workspace/validate")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 2. Add to recent
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

    // 3. Get recent workspaces
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

    // 4. Get suggestions (should include our recent workspace)
    let req = test::TestRequest::get()
        .uri("/v1/workspace/suggestions")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 5. Browse folder
    let req = test::TestRequest::post()
        .uri("/v1/workspace/browse-folder")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 6. List files
    let req = test::TestRequest::post()
        .uri("/v1/workspace/files")
        .set_json(json!({ "path": temp_path }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}
