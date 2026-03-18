use super::path::{should_skip_entry, to_display_name};
use actix_web::{body::to_bytes, http::StatusCode, web};
use std::path::Path;
use tempfile::tempdir;

#[test]
fn should_skip_entry_respects_hidden_flag() {
    assert!(should_skip_entry(".env", false, false));
    assert!(!should_skip_entry(".env", false, true));
}

#[test]
fn should_skip_entry_filters_known_build_dirs() {
    assert!(should_skip_entry("node_modules", true, true));
    assert!(should_skip_entry(".git", true, true));
    assert!(!should_skip_entry("src", true, true));
}

#[test]
fn to_display_name_returns_relative_path_when_possible() {
    let root = Path::new("/tmp/project");
    let nested = Path::new("/tmp/project/src/main.rs");
    assert_eq!(to_display_name(root, nested), "src/main.rs");
}

#[actix_web::test]
async fn workspace_config_registers_expected_routes() {
    let app = actix_web::test::init_service(actix_web::App::new().configure(super::config)).await;

    let requests = [
        actix_web::test::TestRequest::post()
            .uri("/workspace/validate")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/workspace/recent")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/workspace/recent")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/workspace/suggestions")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/workspace/browse-folder")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/workspace/files")
            .to_request(),
    ];

    for request in requests {
        let response = actix_web::test::call_service(&app, request).await;
        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "expected workspace route to be registered"
        );
    }
}

#[actix_web::test]
async fn browse_folder_returns_not_found_when_path_is_a_file() {
    let temp = tempdir().expect("create temp dir");
    let file_path = temp.path().join("README.md");
    std::fs::write(&file_path, "hello").expect("write test file");

    let app_state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/workspace/browse-folder")
        .set_json(serde_json::json!({ "path": file_path.to_string_lossy().to_string() }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn browse_folder_returns_only_directories_sorted_case_insensitively() {
    let temp = tempdir().expect("create temp dir");
    let workspace_root = temp.path().join("ws");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    let alpha = workspace_root.join("Alpha");
    let zeta = workspace_root.join("zeta");
    std::fs::create_dir_all(&alpha).expect("create alpha dir");
    std::fs::create_dir_all(&zeta).expect("create zeta dir");
    std::fs::write(workspace_root.join("notes.txt"), "ignore file").expect("write plain file");

    let app_state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/workspace/browse-folder")
        .set_json(serde_json::json!({ "path": workspace_root.to_string_lossy().to_string() }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("parse browse payload");
    let folder_names = payload["folders"]
        .as_array()
        .expect("folders should be an array")
        .iter()
        .map(|item| item["name"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();

    assert_eq!(folder_names, vec!["Alpha".to_string(), "zeta".to_string()]);
}

#[actix_web::test]
async fn list_workspace_files_respects_depth_and_hidden_flag() {
    let temp = tempdir().expect("create temp dir");
    let workspace_root = temp.path().join("ws");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::write(workspace_root.join("visible.txt"), "v").expect("write visible file");
    std::fs::write(workspace_root.join(".hidden.txt"), "h").expect("write hidden file");
    let nested = workspace_root.join("nested");
    std::fs::create_dir_all(&nested).expect("create nested dir");
    std::fs::write(nested.join("deep.txt"), "d").expect("write deep file");

    let app_state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/workspace/files")
        .set_json(serde_json::json!({
            "path": workspace_root.to_string_lossy().to_string(),
            "max_depth": 0,
            "include_hidden": false
        }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("parse workspace files payload");
    let names = payload
        .as_array()
        .expect("workspace files should be an array")
        .iter()
        .map(|entry| entry["name"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["visible.txt".to_string()]);
}

#[actix_web::test]
async fn list_workspace_files_returns_not_found_for_missing_workspace() {
    let temp = tempdir().expect("create temp dir");
    let missing_path = temp.path().join("does-not-exist");

    let app_state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/workspace/files")
        .set_json(serde_json::json!({ "path": missing_path.to_string_lossy().to_string() }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
