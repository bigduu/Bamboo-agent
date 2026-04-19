use actix_web::{test, web, App};
use bamboo_agent::server::handlers::agent::sessions;
use bamboo_domain::reasoning::ReasoningEffort;
use serde_json::json;

#[actix_web::test]
async fn test_create_session_inherits_provider_default_model_and_reasoning() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;
    super::configure_openai_defaults(&state, "gpt-session-default", Some(ReasoningEffort::High))
        .await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route("/api/v1/sessions", web::get().to(sessions::list_sessions))
            .route(
                "/api/v1/sessions/{session_id}",
                web::get().to(sessions::get_session),
            )
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({
            "title": "Session default inheritance"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    let session_id = body["session"]["id"]
        .as_str()
        .expect("session id should be returned")
        .to_string();

    assert_eq!(body["session"]["model"], "gpt-session-default");
    assert_eq!(body["session"]["reasoning_effort"], "high");

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("session should load from storage")
        .expect("session should exist");
    assert_eq!(stored.model, "gpt-session-default");
    assert_eq!(stored.reasoning_effort, Some(ReasoningEffort::High));

    let entry = state
        .session_store
        .get_index_entry(&session_id)
        .await
        .expect("index entry should exist");
    assert_eq!(entry.model, "gpt-session-default");
    assert_eq!(entry.reasoning_effort, Some(ReasoningEffort::High));

    let session_file = state.app_data_dir.join(entry.rel_path).join("session.json");
    let raw = tokio::fs::read_to_string(&session_file)
        .await
        .expect("session file should be readable");
    let saved: serde_json::Value = serde_json::from_str(&raw).expect("session file should be json");
    assert_eq!(saved["model"], "gpt-session-default");
    assert_eq!(saved["reasoning_effort"], "high");
}

#[actix_web::test]
async fn test_patch_session_persists_model_and_reasoning_to_storage_and_file() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;
    super::configure_openai_defaults(&state, "gpt-session-default", Some(ReasoningEffort::Medium))
        .await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route("/api/v1/sessions", web::get().to(sessions::list_sessions))
            .route(
                "/api/v1/sessions/{session_id}",
                web::get().to(sessions::get_session),
            )
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({
            "title": "Patch persistence"
        }))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id should be returned")
        .to_string();

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({
            "model": "gpt-session-patched",
            "reasoning_effort": "xhigh"
        }))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .to_request();
    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());
    let get_body: serde_json::Value = test::read_body_json(get_resp).await;
    assert_eq!(get_body["session"]["model"], "gpt-session-patched");
    assert_eq!(get_body["session"]["reasoning_effort"], "xhigh");

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("session should load from storage")
        .expect("session should exist");
    assert_eq!(stored.model, "gpt-session-patched");
    assert_eq!(stored.reasoning_effort, Some(ReasoningEffort::Xhigh));

    let entry = state
        .session_store
        .get_index_entry(&session_id)
        .await
        .expect("index entry should exist");
    assert_eq!(entry.model, "gpt-session-patched");
    assert_eq!(entry.reasoning_effort, Some(ReasoningEffort::Xhigh));

    let session_file = state.app_data_dir.join(entry.rel_path).join("session.json");
    let raw = tokio::fs::read_to_string(&session_file)
        .await
        .expect("session file should be readable");
    let saved: serde_json::Value = serde_json::from_str(&raw).expect("session file should be json");
    assert_eq!(saved["model"], "gpt-session-patched");
    assert_eq!(saved["reasoning_effort"], "xhigh");
}

#[actix_web::test]
async fn test_patch_session_can_clear_reasoning_effort_without_changing_model() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;
    super::configure_openai_defaults(&state, "gpt-session-default", Some(ReasoningEffort::High))
        .await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route("/api/v1/sessions", web::get().to(sessions::list_sessions))
            .route(
                "/api/v1/sessions/{session_id}",
                web::get().to(sessions::get_session),
            )
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({
            "title": "Clear reasoning",
            "model": "gpt-clear-test",
            "reasoning_effort": "high"
        }))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id should be returned")
        .to_string();

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({
            "clear_reasoning_effort": true
        }))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success());
    let patch_body: serde_json::Value = test::read_body_json(patch_resp).await;
    assert_eq!(patch_body["session"]["model"], "gpt-clear-test");
    assert!(patch_body["session"].get("reasoning_effort").is_none());

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("session should load from storage")
        .expect("session should exist");
    assert_eq!(stored.model, "gpt-clear-test");
    assert_eq!(stored.reasoning_effort, None);

    let entry = state
        .session_store
        .get_index_entry(&session_id)
        .await
        .expect("index entry should exist");
    assert_eq!(entry.model, "gpt-clear-test");
    assert_eq!(entry.reasoning_effort, None);

    let session_file = state.app_data_dir.join(entry.rel_path).join("session.json");
    let raw = tokio::fs::read_to_string(&session_file)
        .await
        .expect("session file should be readable");
    let saved: serde_json::Value = serde_json::from_str(&raw).expect("session file should be json");
    assert_eq!(saved["model"], "gpt-clear-test");
    assert!(saved.get("reasoning_effort").is_none() || saved["reasoning_effort"].is_null());
}
