use super::*;

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
