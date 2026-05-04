use actix_web::{test, web, App};
use bamboo_agent::server::app_state::{AgentRunner, AgentStatus};
use bamboo_agent::server::handlers::agent::sessions;
use bamboo_agent_core::{AgentEvent, TitleSource};
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

/// B1: PATCH title persists, bumps `title_version`, and emits
/// `SessionTitleUpdated` through the replayable publisher.
#[actix_web::test]
async fn test_patch_session_title_persists_and_emits_event() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({"title": "New Session"}))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id")
        .to_string();

    // Subscribe to broadcast BEFORE the PATCH so we can observe the event.
    let sender = state.get_session_event_sender(&session_id).await;
    let mut subscriber = sender.subscribe();

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"title": "  Renamed Session  "}))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success());

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("session exists");
    assert_eq!(stored.title, "Renamed Session");
    assert_eq!(stored.title_version, 1);

    let event = tokio::time::timeout(std::time::Duration::from_millis(200), subscriber.recv())
        .await
        .expect("event before timeout")
        .expect("event received");
    match event {
        AgentEvent::SessionTitleUpdated {
            session_id: emitted_id,
            title,
            title_version,
            source,
            ..
        } => {
            assert_eq!(emitted_id, session_id);
            assert_eq!(title, "Renamed Session");
            assert_eq!(title_version, 1);
            assert_eq!(source, TitleSource::Manual);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

/// B2: PATCH pinned persists and emits `SessionPinnedUpdated`.
#[actix_web::test]
async fn test_patch_session_pinned_persists_and_emits_event() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({"title": "Pinning Test"}))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id")
        .to_string();

    let sender = state.get_session_event_sender(&session_id).await;
    let mut subscriber = sender.subscribe();

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"pinned": true}))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success());

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("session exists");
    assert!(stored.pinned, "pinned should be persisted as true");

    let event = tokio::time::timeout(std::time::Duration::from_millis(200), subscriber.recv())
        .await
        .expect("event before timeout")
        .expect("event received");
    match event {
        AgentEvent::SessionPinnedUpdated {
            session_id: emitted_id,
            pinned,
            ..
        } => {
            assert_eq!(emitted_id, session_id);
            assert!(pinned);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

/// B4: After a metadata PATCH, `running_sessions_snapshot` returns the
/// cached event in `last_critical_events` so a late/reconnecting subscriber
/// can replay it.
#[actix_web::test]
async fn test_running_snapshot_returns_cached_metadata_events() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            )
            .route(
                "/api/v1/runs/active",
                web::get().to(sessions::running_sessions_snapshot),
            ),
    )
    .await;

    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({"title": "Replayable"}))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id")
        .to_string();

    // Install a Running runner so the session shows up in the active snapshot
    // and the publisher caches events on it.
    {
        let mut runners = state.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        runners.insert(session_id.clone(), runner);
    }

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"title": "Cached Title", "pinned": true}))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success());

    let snapshot_req = test::TestRequest::get()
        .uri("/api/v1/runs/active")
        .to_request();
    let snapshot_resp = test::call_service(&app, snapshot_req).await;
    assert!(snapshot_resp.status().is_success());
    let snapshot_body: serde_json::Value = test::read_body_json(snapshot_resp).await;

    let entry = snapshot_body["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .find(|e| e["session_id"] == session_id)
        .expect("our session in active snapshot");

    let cached = entry["last_critical_events"]
        .as_array()
        .expect("last_critical_events array");
    assert_eq!(cached.len(), 2, "title + pinned events should be cached");

    let has_title = cached
        .iter()
        .any(|e| e["type"] == "session_title_updated" && e["title"] == "Cached Title");
    let has_pinned = cached
        .iter()
        .any(|e| e["type"] == "session_pinned_updated" && e["pinned"] == true);
    assert!(has_title, "title event should be in the cache");
    assert!(has_pinned, "pinned event should be in the cache");
}

/// E1 (cross-stack): the full PATCH-while-running → disconnect → reconnect →
/// resume cycle. This is the integration test that proves the whole metadata
/// architecture: authoritative write through the service, replayable event
/// cached via `publish_replayable_session_event`, and a late subscriber that
/// dropped its SSE recovers the latest state from the running snapshot —
/// then continues to receive new live events.
#[actix_web::test]
async fn test_title_replay_e2e_patch_disconnect_reconnect() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = super::sessions_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/sessions", web::post().to(sessions::create_session))
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            )
            .route(
                "/api/v1/runs/active",
                web::get().to(sessions::running_sessions_snapshot),
            ),
    )
    .await;

    // Create a session that we'll then "run".
    let create_req = test::TestRequest::post()
        .uri("/api/v1/sessions")
        .set_json(json!({"title": "Initial"}))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());
    let create_body: serde_json::Value = test::read_body_json(create_resp).await;
    let session_id = create_body["session"]["id"]
        .as_str()
        .expect("session id")
        .to_string();

    // Mark the session as actively running so the publisher caches events on
    // its runner and the snapshot endpoint exposes it.
    {
        let mut runners = state.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        runners.insert(session_id.clone(), runner);
    }

    // ---- Phase 1: connected client receives the live event ----
    let sender = state.get_session_event_sender(&session_id).await;
    let mut live = sender.subscribe();

    let patch1_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"title": "Title v1"}))
        .to_request();
    let patch1_resp = test::call_service(&app, patch1_req).await;
    assert!(patch1_resp.status().is_success());

    let live_event = tokio::time::timeout(std::time::Duration::from_millis(200), live.recv())
        .await
        .expect("live event before timeout")
        .expect("live event received");
    match live_event {
        AgentEvent::SessionTitleUpdated {
            title,
            title_version,
            source,
            ..
        } => {
            assert_eq!(title, "Title v1");
            assert_eq!(title_version, 1);
            assert_eq!(source, TitleSource::Manual);
        }
        other => panic!("unexpected live event: {other:?}"),
    }

    // ---- Phase 2: client disconnects mid-flight ----
    drop(live);

    // ---- Phase 3: PATCH arrives while no subscriber is connected ----
    let patch2_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"title": "Title v2"}))
        .to_request();
    let patch2_resp = test::call_service(&app, patch2_req).await;
    assert!(patch2_resp.status().is_success());

    // ---- Phase 4: client reconnects ----
    // The snapshot is the recovery point — it should expose the latest title
    // even though the live event was missed.
    let snapshot_req = test::TestRequest::get()
        .uri("/api/v1/runs/active")
        .to_request();
    let snapshot_resp = test::call_service(&app, snapshot_req).await;
    assert!(snapshot_resp.status().is_success());
    let snapshot_body: serde_json::Value = test::read_body_json(snapshot_resp).await;

    let entry = snapshot_body["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .find(|e| e["session_id"] == session_id)
        .expect("our session in active snapshot");
    let cached = entry["last_critical_events"]
        .as_array()
        .expect("last_critical_events array");

    // Both PATCHes should be cached, and the latest one should reflect v2.
    let title_events: Vec<&serde_json::Value> = cached
        .iter()
        .filter(|e| e["type"] == "session_title_updated")
        .collect();
    assert_eq!(
        title_events.len(),
        2,
        "both v1 and v2 title events should be cached"
    );

    let latest_title_event = title_events
        .iter()
        .max_by_key(|e| e["title_version"].as_u64().unwrap_or(0))
        .expect("at least one title event");
    assert_eq!(latest_title_event["title"], "Title v2");
    assert_eq!(latest_title_event["title_version"], 2);

    // On-disk state must agree with the cached snapshot — there is no other
    // authority. If these diverge, the architecture is broken.
    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("session exists");
    assert_eq!(stored.title, "Title v2");
    assert_eq!(stored.title_version, 2);

    // ---- Phase 5: new live subscription continues to receive events ----
    let mut live2 = sender.subscribe();
    let patch3_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({"title": "Title v3"}))
        .to_request();
    let patch3_resp = test::call_service(&app, patch3_req).await;
    assert!(patch3_resp.status().is_success());

    let post_reconnect = tokio::time::timeout(std::time::Duration::from_millis(200), live2.recv())
        .await
        .expect("post-reconnect event before timeout")
        .expect("post-reconnect event received");
    match post_reconnect {
        AgentEvent::SessionTitleUpdated {
            title,
            title_version,
            ..
        } => {
            assert_eq!(title, "Title v3");
            assert_eq!(title_version, 3);
        }
        other => panic!("unexpected post-reconnect event: {other:?}"),
    }
}
