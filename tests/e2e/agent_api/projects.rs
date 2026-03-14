use super::*;

#[actix_web::test]
async fn test_list_projects_empty() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::get().to(agent_api::list_projects),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/projects")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let projects: Vec<Value> = serde_json::from_slice(&body).expect("Failed to parse response");
    drop(projects);
}

#[actix_web::test]
async fn test_create_project_success() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;
    let temp_project = create_temp_project();
    let project_path = temp_project.to_string_lossy().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::post().to(agent_api::create_project),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({ "path": project_path }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let project: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(project["id"].is_string());
    assert!(project["path"].is_string());
    assert!(project["sessions"].is_array());
    assert!(project["created_at"].is_number());

    let _ = std::fs::remove_dir_all(&temp_project);
}

#[actix_web::test]
async fn test_create_project_invalid_path() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::post().to(agent_api::create_project),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({ "path": "/nonexistent/path/12345" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_server_error());
}

#[actix_web::test]
async fn test_get_project_sessions_nonexistent() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects/{project_id}/sessions",
        web::get().to(agent_api::get_project_sessions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/projects/nonexistent-project-12345/sessions")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_server_error());
}
