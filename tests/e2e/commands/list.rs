use super::*;

#[actix_web::test]
async fn test_list_commands_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/commands", web::get().to(command::list_commands)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/commands").to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_list_commands_returns_json() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/commands", web::get().to(command::list_commands)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/commands").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    // Should be valid JSON
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have commands array
    assert!(result.is_object());
    assert!(result.get("commands").is_some());
    assert!(result.get("total").is_some());
    assert!(result["commands"].is_array());
}

#[actix_web::test]
async fn test_list_commands_includes_workflows_and_skills() {
    let state = crate::e2e::common::create_test_app_with_workflows().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/commands", web::get().to(command::list_commands)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/commands").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let result: Value = serde_json::from_slice(&test::read_body(resp).await)
        .expect("Response should be valid JSON");
    assert!(result["commands"]
        .as_array()
        .expect("commands should be an array")
        .iter()
        .all(|command| command["name"] != "example"));

    // Publish a new source after watcher startup; it cannot come from the initial snapshot.
    let workflow_path = state.app_data_dir.join("workflows/example.md");
    tokio::fs::write(&workflow_path, "# Example Workflow")
        .await
        .expect("Failed to write workflow");

    let command = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let req = test::TestRequest::get().uri("/v1/commands").to_request();
            let resp = test::call_service(&app, req).await;
            let body = test::read_body(resp).await;
            let result: Value =
                serde_json::from_slice(&body).expect("Response should be valid JSON");
            if let Some(command) = result["commands"]
                .as_array()
                .expect("commands should be an array")
                .iter()
                .find(|command| command["name"] == "example")
                .cloned()
            {
                break command;
            }
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("watcher should publish the imported workflow command");

    assert_eq!(command["id"], "workflow-example");
    assert_eq!(command["type"], "workflow");
    assert!(command["description"]
        .as_str()
        .is_some_and(|description| description.contains("Legacy workflow 'example'")));
}
