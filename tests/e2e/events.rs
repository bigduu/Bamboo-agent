//! E2E tests for /api/v1/events/{session_id} endpoint (SSE streaming)

use actix_web::{test, web, App};
use bamboo_agent::agent::core::{Message, Session};
use bamboo_agent::server::app_state::{AgentRunner, AgentStatus};
use bamboo_agent::server::handlers;

#[actix_web::test]
async fn test_events_endpoint_exists() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/events/{session_id}",
        web::get().to(handlers::events::handler),
    ))
    .await;

    let uri = format!("/api/v1/events/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should respond (even if the session doesn't exist)
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_events_content_type() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/events/{session_id}",
        web::get().to(handlers::events::handler),
    ))
    .await;

    let uri = format!("/api/v1/events/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return proper content type for SSE or an error
    if resp.status().is_success() {
        let content_type = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());

        // SSE should have text/event-stream content type
        assert!(content_type.is_some());
    }
}

#[actix_web::test]
async fn test_events_with_different_sessions() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/events/{session_id}",
        web::get().to(handlers::events::handler),
    ))
    .await;

    // Test with multiple different session IDs
    for _ in 0..3 {
        let session_id = uuid::Uuid::new_v4().to_string();
        let uri = format!("/api/v1/events/{}", session_id);

        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;

        assert!(resp.status().is_success() || resp.status().is_client_error());
    }
}

#[actix_web::test]
async fn test_events_returns_one_shot_complete_for_completed_session_without_runner() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    // Persist a session whose last message is NOT a user message.
    let mut session = Session::new(session_id.clone(), "test-model".to_string());
    session.add_message(Message::user("hi".to_string()));
    session.add_message(Message::assistant("hello".to_string(), None));
    state.storage.save_session(&session).await.unwrap();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/events/{session_id}",
        web::get().to(handlers::events::handler),
    ))
    .await;

    let uri = format!("/api/v1/events/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let body = test::call_and_read_body(&app, req).await;
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("\"type\":\"complete\""));
}

#[actix_web::test]
async fn test_events_returns_one_shot_error_for_errored_runner() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    // Persist a session whose last message is NOT a user message.
    let mut session = Session::new(session_id.clone(), "test-model".to_string());
    session.add_message(Message::user("hi".to_string()));
    session.add_message(Message::assistant("hello".to_string(), None));
    state.storage.save_session(&session).await.unwrap();

    // Seed an errored runner.
    {
        let mut runners = state.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Error("boom".to_string());
        runners.insert(session_id.clone(), runner);
    }

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/events/{session_id}",
        web::get().to(handlers::events::handler),
    ))
    .await;

    let uri = format!("/api/v1/events/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let body = test::call_and_read_body(&app, req).await;
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("\"type\":\"error\""));
    assert!(text.contains("boom"));
}
