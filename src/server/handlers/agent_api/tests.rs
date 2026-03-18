use actix_web::{body::to_bytes, http::StatusCode, web};
use tempfile::tempdir;
use uuid::Uuid;

#[actix_web::test]
async fn agent_api_config_registers_expected_routes() {
    let app = actix_web::test::init_service(actix_web::App::new().configure(super::config)).await;

    let requests = [
        actix_web::test::TestRequest::get()
            .uri("/agent/projects")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/agent/projects")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/agent/projects/project-1/sessions")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/agent/settings")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/agent/settings")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/agent/system-prompt")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/agent/system-prompt")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/agent/sessions/running")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/agent/sessions/execute")
            .to_request(),
        actix_web::test::TestRequest::post()
            .uri("/agent/sessions/cancel")
            .to_request(),
        actix_web::test::TestRequest::get()
            .uri("/agent/sessions/session-1/jsonl")
            .to_request(),
    ];

    for request in requests {
        let response = actix_web::test::call_service(&app, request).await;
        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "expected agent API route to be registered"
        );
    }
}

#[actix_web::test]
async fn cancel_route_rejects_empty_session_id() {
    let temp = tempdir().expect("create temp dir");
    let app_state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/agent/sessions/cancel")
        .set_json(serde_json::json!({ "session_id": "   " }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("parse error payload");
    assert!(payload["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("session_id is required"));
}

#[actix_web::test]
async fn cancel_route_resolves_alias_when_session_not_running() {
    let temp = tempdir().expect("create temp dir");
    let state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    let alias = "friendly-session";
    let target_session_id = Uuid::new_v4().to_string();
    state
        .claude_session_aliases
        .write()
        .await
        .insert(alias.to_string(), target_session_id.clone());

    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/agent/sessions/cancel")
        .set_json(serde_json::json!({ "session_id": alias }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("parse response payload");
    assert_eq!(payload["success"], serde_json::json!(true));
    assert_eq!(
        payload["message"],
        serde_json::json!("Session not found or not running")
    );
    assert_eq!(payload["session_id"], serde_json::json!(alias));
    assert_eq!(
        payload["claude_session_id"],
        serde_json::json!(target_session_id)
    );
}

#[actix_web::test]
async fn execute_route_returns_bad_request_for_non_directory_project_path() {
    let temp = tempdir().expect("create temp dir");
    let state =
        web::Data::new(crate::server::app_state::AppState::new(temp.path().to_path_buf()).await);
    *state.claude_cli_path.write().await = Some("claude".to_string());

    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .configure(super::config),
    )
    .await;

    let req = actix_web::test::TestRequest::post()
        .uri("/agent/sessions/execute")
        .set_json(serde_json::json!({
            "project_path": temp.path().join("missing-project").to_string_lossy().to_string(),
            "prompt": "hello"
        }))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(resp.into_body())
        .await
        .expect("read response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("parse error payload");
    assert!(payload["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("project_path is not a directory"));
}
