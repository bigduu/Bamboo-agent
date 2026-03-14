use super::*;

#[actix_web::test]
async fn test_execute_tool_with_parameters() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/tools/execute", web::post().to(tools::execute_tool)),
    )
    .await;

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

    assert!(result.get("result").is_some());

    let result_str = result["result"].as_str().expect("result should be string");
    let inner: Value = serde_json::from_str(result_str).expect("Inner result should be JSON");
    assert!(inner["result"]
        .as_str()
        .unwrap_or_default()
        .contains("Hello, World!"));
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

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = temp_dir.path().join("exists.txt");
    tokio::fs::write(&test_file, "test")
        .await
        .expect("Failed to write file");

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
    assert!(resp.status().is_success());
}
