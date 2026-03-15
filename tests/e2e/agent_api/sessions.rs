use super::*;

#[actix_web::test]
async fn test_list_running_sessions() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/running",
        web::get().to(agent_api::list_running_claude_sessions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/running")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let sessions: Vec<Value> = serde_json::from_slice(&body).expect("Failed to parse response");
    drop(sessions);
}

#[actix_web::test]
async fn test_claude_events_returns_not_found_for_missing_runner() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/events",
        web::get().to(agent_api::claude_events),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/missing-session/events")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

    let body = test::read_body(resp).await;
    let payload: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert_eq!(payload["error"], "Claude session not running");
    assert_eq!(payload["session_id"], "missing-session");
}

#[actix_web::test]
async fn test_claude_events_returns_one_shot_complete_for_completed_runner() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;
    let session_id = "completed-session";

    {
        let mut runners = state.claude_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Completed;
        runners.insert(session_id.to_string(), runner);
    }

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/events",
        web::get().to(agent_api::claude_events),
    ))
    .await;

    let uri = format!("/v1/agent/sessions/{session_id}/events");
    let req = test::TestRequest::get().uri(&uri).to_request();
    let body = test::call_and_read_body(&app, req).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"type\":\"complete\""));
}

#[actix_web::test]
async fn test_claude_events_returns_one_shot_error_for_errored_runner() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;
    let session_id = "errored-session";

    {
        let mut runners = state.claude_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Error("boom".to_string());
        runners.insert(session_id.to_string(), runner);
    }

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/events",
        web::get().to(agent_api::claude_events),
    ))
    .await;

    let uri = format!("/v1/agent/sessions/{session_id}/events");
    let req = test::TestRequest::get().uri(&uri).to_request();
    let body = test::call_and_read_body(&app, req).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"type\":\"error\""));
    assert!(text.contains("boom"));
}

#[actix_web::test]
async fn test_execute_claude_code() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/execute",
        web::post().to(agent_api::execute_claude_code),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Hello, Claude!",
            "session_id": null
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(result["success"].is_boolean());
}

#[actix_web::test]
async fn test_execute_with_session_id() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/execute",
        web::post().to(agent_api::execute_claude_code),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Continue conversation",
            "session_id": "test-session-123"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_cancel_execution() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/cancel",
        web::post().to(agent_api::cancel_claude_execution),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/cancel")
        .set_json(json!({
            "session_id": "test-session-to-cancel"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(result["success"].is_boolean());
    assert_eq!(result["success"], true);
}

#[actix_web::test]
async fn test_get_session_jsonl_missing_project_id() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/jsonl",
        web::get().to(agent_api::get_session_jsonl),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/test-session/jsonl")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_server_error());
}

#[actix_web::test]
async fn test_get_session_jsonl_nonexistent() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/jsonl",
        web::get().to(agent_api::get_session_jsonl),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/nonexistent-session/jsonl?project_id=nonexistent-project")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_server_error());
}
