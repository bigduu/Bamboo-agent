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

    let req = test::TestRequest::get().uri("/v1/commands").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have at least one command (the workflow we created)
    let commands = result["commands"]
        .as_array()
        .expect("commands should be an array");
    assert!(!commands.is_empty());

    // Should include our workflow
    let has_workflow = commands
        .iter()
        .any(|cmd| cmd["type"] == "workflow" && cmd["name"] == "example");
    assert!(has_workflow, "Should include the example workflow");
}
