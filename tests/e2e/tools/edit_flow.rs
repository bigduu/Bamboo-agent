use super::*;

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

    let updated = tokio::fs::read_to_string(&test_file)
        .await
        .expect("read updated file");
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

    let updated = tokio::fs::read_to_string(&test_file)
        .await
        .expect("read updated file");
    assert_eq!(updated, "hi world");
}

#[actix_web::test]
async fn test_execute_edit_tool_rejects_excessive_replace_all() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_replace_all_guard.txt");
    let content = (0..9).map(|_| "foo").collect::<Vec<_>>().join("\n") + "\n";
    tokio::fs::write(&test_file, content)
        .await
        .expect("Failed to write test file");
    let session_id = "tools-e2e-edit-replace-all-guard";

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

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {"name":"file_path","value": test_file.to_str().unwrap()},
                {"name":"old_string","value":"foo"},
                {"name":"new_string","value":"bar"},
                {"name":"replace_all","value":"true"}
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
        .contains("replace_all would modify"));
}

#[actix_web::test]
async fn test_execute_edit_tool_rejects_replace_all_for_short_old_string() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir
        .path()
        .join("edit_replace_all_short_old_string.txt");
    tokio::fs::write(&test_file, "a\na\n")
        .await
        .expect("Failed to write test file");
    let session_id = "tools-e2e-edit-replace-all-short-old-string";

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

    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {"name":"file_path","value": test_file.to_str().unwrap()},
                {"name":"old_string","value":"a"},
                {"name":"new_string","value":"b"},
                {"name":"replace_all","value":"true"}
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
        .contains("non-whitespace characters"));
}

#[actix_web::test]
async fn test_execute_edit_tool_patch_rejects_large_scope() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("edit_large_scope_patch.txt");
    let old_block = (0..70)
        .map(|idx| format!("line {idx}"))
        .collect::<Vec<_>>()
        .join("\n");
    let new_block = (0..70)
        .map(|idx| format!("updated {idx}"))
        .collect::<Vec<_>>()
        .join("\n");
    tokio::fs::write(&test_file, format!("{old_block}\n"))
        .await
        .expect("Failed to write test file");
    let session_id = "tools-e2e-edit-large-scope-guard";

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

    let patch = format!("<<<<<<< SEARCH\n{old_block}\n=======\n{new_block}\n>>>>>>> REPLACE");
    let req = test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "Edit",
            "session_id": session_id,
            "parameters": [
                {"name": "file_path", "value": test_file.to_str().unwrap()},
                {"name": "patch", "value": patch}
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
        .contains("exceeding the safe limit"));
}
