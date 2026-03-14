use super::*;

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

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::delete().to(messages::delete_message),
    ))
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
