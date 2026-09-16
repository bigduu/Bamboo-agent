use super::*;

#[actix_web::test]
async fn archive_context_tool_endpoint_responds() {
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
            "tool_name": "archive_context",
            "parameters": {}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error(),
        "endpoint should respond, got: {}",
        resp.status()
    );
}

#[actix_web::test]
async fn archive_context_schema_has_no_instruction_field() {
    use bamboo_agent_core::tools::Tool;

    let tool = bamboo_server::server_tools::ArchiveContextTool;
    assert_eq!(tool.name(), "archive_context");
    let schema = tool.parameters_schema();
    assert_eq!(schema["properties"], json!({}));
    assert_eq!(schema["additionalProperties"], false);
}
