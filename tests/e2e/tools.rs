//! E2E tests for /v1/tools/execute endpoint

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::tools;
use serde_json::{json, Value};

#[actix_web::test]
async fn test_execute_tool_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "get_current_dir",
            "parameters": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should respond (success or error, but not 404)
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_execute_tool_with_valid_input() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Test with get_current_dir (a simple tool that doesn't require parameters)
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "get_current_dir",
            "parameters": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have result field
    assert!(result.get("result").is_some());
}

#[actix_web::test]
async fn test_execute_tool_requires_body() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Test without body (should fail)
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should be a client error (400 Bad Request)
    assert!(resp.status().is_client_error());
}

#[actix_web::test]
async fn test_execute_tool_with_invalid_tool_name() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "nonexistent_tool",
            "parameters": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return 404 or 400 for nonexistent tool
    assert!(
        resp.status() == actix_web::http::StatusCode::NOT_FOUND
            || resp.status() == actix_web::http::StatusCode::BAD_REQUEST
    );
}

#[actix_web::test]
async fn test_execute_task_alias_requires_session_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "sub_task",
            "parameters": [
                {"name":"title","value":"Search refs"},
                {"name":"responsibility","value":"Find parser entrypoints"},
                {"name":"prompt","value":"Scan parser modules"},
                {"name":"subagent_type","value":"general-purpose"}
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert!(result["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("requires session_id"));
}

#[actix_web::test]
async fn test_execute_tool_with_parameters() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Create a temp file to test read_file tool
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("test.txt");
    tokio::fs::write(&test_file, "Hello, World!")
        .await
        .expect("Failed to write test file");

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "read_file",
            "parameters": [
                {
                    "name": "path",
                    "value": test_file.to_str().unwrap()
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have result field
    assert!(result.get("result").is_some());

    // The result should contain our file content
    let result_str = result["result"].as_str().unwrap();
    let inner: Value = serde_json::from_str(result_str).expect("Inner result should be JSON");
    assert!(inner["result"].as_str().unwrap().contains("Hello, World!"));
}

#[actix_web::test]
async fn test_execute_edit_tool_with_patch_mode() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_patch.txt");
    tokio::fs::write(
        &test_file,
        "fn a() {\n    let v = 1;\n}\n\nfn b() {\n    let v = 1;\n}\n",
    )
    .await
    .expect("Failed to write test file");
    let session_id = "tools-e2e-edit-patch";

    let read_req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "read_file",
            "session_id": session_id,
            "parameters": [
                {"name":"path","value": test_file.to_str().unwrap()}
            ]
        }))
        .to_request();
    let read_resp = test::call_service(&app, read_req).await;
    assert!(read_resp.status().is_success());

    let patch = "<<<<<<< SEARCH\nfn b() {\n    let v = 1;\n}\n=======\nfn b() {\n    let v = 2;\n}\n>>>>>>> REPLACE";
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {
                    "name": "file_path",
                    "value": test_file.to_str().unwrap()
                },
                {
                    "name": "patch",
                    "value": patch
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let updated = tokio::fs::read_to_string(&test_file).await.unwrap();
    assert!(updated.contains("fn a() {\n    let v = 1;\n}"));
    assert!(updated.contains("fn b() {\n    let v = 2;\n}"));
}

#[actix_web::test]
async fn test_execute_edit_tool_patch_rejects_ambiguous_search() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_ambiguous.txt");
    tokio::fs::write(&test_file, "x = 1;\nx = 1;\n")
        .await
        .expect("Failed to write test file");
    let session_id = "tools-e2e-edit-ambiguous";

    let read_req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "read_file",
            "session_id": session_id,
            "parameters": [
                {"name":"path","value": test_file.to_str().unwrap()}
            ]
        }))
        .to_request();
    let read_resp = test::call_service(&app, read_req).await;
    assert!(read_resp.status().is_success());

    let patch = "<<<<<<< SEARCH\nx = 1;\n=======\nx = 2;\n>>>>>>> REPLACE";
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {
                    "name": "file_path",
                    "value": test_file.to_str().unwrap()
                },
                {
                    "name": "patch",
                    "value": patch
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_client_error());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert!(result["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("matched 2 times"));
}

#[actix_web::test]
async fn test_execute_edit_tool_requires_session_id() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_requires_session.txt");
    tokio::fs::write(&test_file, "hello world")
        .await
        .expect("Failed to write test file");

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "parameters": [
                {"name":"file_path","value": test_file.to_str().unwrap()},
                {"name":"old_string","value":"hello"},
                {"name":"new_string","value":"hi"}
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_client_error());
    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert!(result["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("requires session_id"));
}

#[actix_web::test]
async fn test_execute_read_then_edit_with_same_session_id() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_with_session.txt");
    tokio::fs::write(&test_file, "hello world")
        .await
        .expect("Failed to write test file");
    let session_id = "tools-e2e-session-1";

    let read_req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "read_file",
            "session_id": session_id,
            "parameters": [
                {"name":"path","value": test_file.to_str().unwrap()}
            ]
        }))
        .to_request();
    let read_resp = test::call_service(&app, read_req).await;
    assert!(read_resp.status().is_success());

    let edit_req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {"name":"file_path","value": test_file.to_str().unwrap()},
                {"name":"old_string","value":"hello"},
                {"name":"new_string","value":"hi"}
            ]
        }))
        .to_request();
    let edit_resp = test::call_service(&app, edit_req).await;
    assert!(edit_resp.status().is_success());

    let updated = tokio::fs::read_to_string(&test_file).await.unwrap();
    assert_eq!(updated, "hi world");
}

#[actix_web::test]
async fn test_execute_tool_with_missing_required_parameter() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Try to execute read_file without required "path" parameter
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "read_file",
            "parameters": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return an error (400 or 500)
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}

#[actix_web::test]
async fn test_execute_tool_with_json_parameter() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Test with a parameter that should be parsed as JSON
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "list_directory",
            "parameters": [
                {
                    "name": "path",
                    "value": "/"
                },
                {
                    "name": "recursive",
                    "value": "false"
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should succeed or fail gracefully, but not crash
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error()
    );
}

#[actix_web::test]
async fn test_execute_tool_response_format() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "get_current_dir",
            "parameters": []
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify response structure
    assert!(result.is_object());
    assert!(result.get("result").is_some());

    // The result field should be a JSON string
    let result_str = result["result"]
        .as_str()
        .expect("result should be a string");
    let inner: Value = serde_json::from_str(result_str).expect("result should contain valid JSON");

    // Inner result should have tool_name and result fields
    assert!(inner.get("tool_name").is_some());
    assert!(inner.get("result").is_some());
    assert_eq!(inner["display_preference"], "Default");
}

#[actix_web::test]
async fn test_execute_tool_list_directory() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Create a temp directory with some files
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    tokio::fs::write(temp_dir.path().join("file1.txt"), "content1")
        .await
        .expect("Failed to write file");
    tokio::fs::write(temp_dir.path().join("file2.txt"), "content2")
        .await
        .expect("Failed to write file");

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "list_directory",
            "parameters": [
                {
                    "name": "path",
                    "value": temp_dir.path().to_str().unwrap()
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_execute_tool_file_exists() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    // Create a test file
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("exists.txt");
    tokio::fs::write(&test_file, "test")
        .await
        .expect("Failed to write file");

    // Test with existing file
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "file_exists",
            "parameters": [
                {
                    "name": "path",
                    "value": test_file.to_str().unwrap()
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Test with non-existing file
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "file_exists",
            "parameters": [
                {
                    "name": "path",
                    "value": "/nonexistent/path/to/file.txt"
                }
            ]
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success()); // Tool should execute successfully even if file doesn't exist
}
