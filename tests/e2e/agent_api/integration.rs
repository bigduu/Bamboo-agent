use super::*;

#[actix_web::test]
async fn test_full_project_workflow() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;
    let temp_project = create_temp_project();
    let project_path = temp_project.to_string_lossy().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/projects",
                web::post().to(agent_api::create_project),
            )
            .route(
                "/v1/agent/projects",
                web::get().to(agent_api::list_projects),
            )
            .route(
                "/v1/agent/projects/{project_id}/sessions",
                web::get().to(agent_api::get_project_sessions),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({ "path": project_path }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    let create_body = test::read_body(create_resp).await;
    let project: Value = serde_json::from_slice(&create_body).expect("Failed to parse response");
    let project_id = project["id"].as_str().expect("Project ID should be string");

    let list_req = test::TestRequest::get()
        .uri("/v1/agent/projects")
        .to_request();
    let list_resp = test::call_service(&app, list_req).await;
    assert!(list_resp.status().is_success());

    let sessions_uri = format!("/v1/agent/projects/{project_id}/sessions");
    let sessions_req = test::TestRequest::get().uri(&sessions_uri).to_request();
    let sessions_resp = test::call_service(&app, sessions_req).await;
    assert!(sessions_resp.status().is_success());

    let _ = std::fs::remove_dir_all(&temp_project);
}

#[actix_web::test]
async fn test_settings_and_prompt_integration() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/settings",
                web::post().to(agent_api::save_claude_settings),
            )
            .route(
                "/v1/agent/system-prompt",
                web::post().to(agent_api::save_system_prompt),
            )
            .route(
                "/v1/agent/sessions/execute",
                web::post().to(agent_api::execute_claude_code),
            ),
    )
    .await;

    let settings_req = test::TestRequest::post()
        .uri("/v1/agent/settings")
        .set_json(json!({
            "settings": {
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 4096
            }
        }))
        .to_request();
    let settings_resp = test::call_service(&app, settings_req).await;
    assert!(settings_resp.status().is_success());

    let prompt_req = test::TestRequest::post()
        .uri("/v1/agent/system-prompt")
        .set_json(json!({
            "content": "# Integration Test Prompt\n\nBe helpful and concise."
        }))
        .to_request();
    let prompt_resp = test::call_service(&app, prompt_req).await;
    assert!(prompt_resp.status().is_success());

    let execute_req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Test execution with new settings"
        }))
        .to_request();
    let execute_resp = test::call_service(&app, execute_req).await;
    assert!(execute_resp.status().is_success());
}
