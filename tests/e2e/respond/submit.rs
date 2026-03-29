use super::*;
use bamboo_agent::agent::core::{Message, Session};

#[actix_web::test]
async fn test_submit_response_rejects_invalid_option_and_keeps_pending_question() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = "respond-invalid-option".to_string();

    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::tool_result("tool-call-1", "placeholder"));
    session.set_pending_question(
        "tool-call-1".to_string(),
        "Pick one".to_string(),
        vec!["A".to_string(), "B".to_string()],
        false,
    );
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/respond/{session_id}",
        web::post().to(handlers::respond::submit_response),
    ))
    .await;

    let uri = format!("/api/v1/respond/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .set_json(json!({
            "response": "C"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "Invalid response");
    assert_eq!(body["message"], "Response must be one of: A, B");

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert!(loaded.pending_question.is_some());
}

#[actix_web::test]
async fn test_submit_response_updates_tool_result_and_clears_pending_question() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = "respond-valid-option".to_string();

    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::tool_result("tool-call-2", "placeholder"));
    session.set_pending_question(
        "tool-call-2".to_string(),
        "Pick one".to_string(),
        vec!["A".to_string(), "B".to_string()],
        false,
    );
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/respond/{session_id}",
        web::post().to(handlers::respond::submit_response),
    ))
    .await;

    let uri = format!("/api/v1/respond/{}", session_id);
    let req = test::TestRequest::post()
        .uri(&uri)
        .set_json(json!({
            "response": "A"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["response"], "A");
    assert_eq!(body["auto_resume_status"], "not_requested");

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert!(loaded.pending_question.is_none());
    assert!(loaded
        .messages
        .iter()
        .any(|message| message.content == "User selected: A"));
    assert!(!loaded.messages.iter().any(|message| message
        .content
        .contains("I chose 'A' in response to: Pick one")));
    assert_eq!(
        loaded
            .metadata
            .get("conclusion_with_options_resume_pending")
            .map(String::as_str),
        Some("true")
    );
}

#[actix_web::test]
async fn test_respond_with_empty_body() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/respond/{session_id}",
        web::post().to(handlers::respond::submit_response),
    ))
    .await;

    let uri = format!("/api/v1/respond/{}", session_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_client_error());
}
