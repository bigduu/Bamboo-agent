//! E2E tests for message mutation endpoints (delete/truncate).

use actix_web::{test, web, App};
use bamboo_agent::agent::core::{Message, Session};
use bamboo_agent::server::handlers::agent::messages;
use serde_json::json;

#[actix_web::test]
async fn test_truncate_after_last_user() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-truncate-1".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    session.add_message(Message::user("u1"));
    session.add_message(Message::assistant("a1", None));
    session.add_message(Message::user("u2"));
    session.add_message(Message::assistant("a2", None));

    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/api/v1/sessions/{session_id}/messages/truncate",
                web::post().to(messages::truncate_messages),
            ),
    )
    .await;

    let req = test::TestRequest::post()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/truncate",
            session_id
        ))
        .set_json(json!({ "mode": "after_last_user" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["session_id"], session_id);
    assert_eq!(body["messages_removed"], 1);
    assert_eq!(body["message_count"], 4);

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(loaded.messages.len(), 4);
    assert_eq!(loaded.messages.last().unwrap().role, bamboo_agent::agent::core::Role::User);
    assert_eq!(loaded.messages.last().unwrap().content, "u2");
}

#[actix_web::test]
async fn test_delete_message_persists() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-delete-1".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    session.add_message(Message::user("u1"));
    let assistant = Message::assistant("a1", None);
    let assistant_id = assistant.id.clone();
    session.add_message(assistant);

    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/api/v1/sessions/{session_id}/messages/{message_id}",
                web::delete().to(messages::delete_message),
            ),
    )
    .await;

    let req = test::TestRequest::delete()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, assistant_id
        ))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(loaded.messages.len(), 2);
    assert!(loaded.messages.iter().all(|m| m.id != assistant_id));
}
