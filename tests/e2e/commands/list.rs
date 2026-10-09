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
    let state = crate::e2e::common::create_test_app().await;

    // Create a test workflow
    let workflows_dir = state.app_data_dir.join("workflows");
    let root_existed_before_create = workflows_dir.is_dir();
    eprintln!("WORKFLOW_LIST_PHASE root_before_create={root_existed_before_create}");
    tokio::fs::create_dir_all(&workflows_dir)
        .await
        .expect("Failed to create workflows dir");

    let workflow_path = workflows_dir.join("example.md");
    tokio::fs::write(&workflow_path, "# Example Workflow")
        .await
        .expect("Failed to write workflow");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/commands", web::get().to(command::list_commands)),
    )
    .await;

    let mut polls = 0usize;
    let mut last_status = None;
    let mut last_command_count = None;
    let command = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let req = test::TestRequest::get().uri("/v1/commands").to_request();
            let resp = test::call_service(&app, req).await;
            polls += 1;
            last_status = Some(resp.status());
            let body = test::read_body(resp).await;
            let result: Value =
                serde_json::from_slice(&body).expect("Response should be valid JSON");
            last_command_count = result["commands"].as_array().map(Vec::len);
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
    .unwrap_or_else(|error| {
        panic!("watcher should publish the imported workflow command: {error:?}; root_before_create={root_existed_before_create}; polls={polls}; last_status={last_status:?}; last_command_count={last_command_count:?}")
    });
    eprintln!("WORKFLOW_LIST_PHASE published polls={polls} last_status={last_status:?} last_command_count={last_command_count:?}");

    assert_eq!(command["id"], "workflow-example");
    assert_eq!(command["type"], "workflow");
    assert!(command["description"]
        .as_str()
        .is_some_and(|description| description.contains("Legacy workflow 'example'")));
}
