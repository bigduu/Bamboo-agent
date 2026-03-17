use super::*;
use bamboo_agent::agent::core::agent::events::TokenBudgetUsage;
use bamboo_agent::agent::core::tools::{FunctionCall, ToolCall};
use bamboo_agent::agent::core::ConversationSummary;

fn create_tool_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "read_file".to_string(),
            arguments: "{\"path\":\"/tmp/file.txt\"}".to_string(),
        },
    }
}

#[actix_web::test]
async fn test_patch_message_persists_and_clears_derived_context() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-patch-1".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let assistant = Message::assistant("```mermaid\ngraph TD\nA -->\n```", None);
    let assistant_id = assistant.id.clone();
    session.add_message(assistant);
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 10,
        summary_tokens: 5,
        window_tokens: 15,
        total_tokens: 30,
        budget_limit: 100,
        truncation_occurred: false,
        segments_removed: 0,
    });
    session.conversation_summary = Some(ConversationSummary::new("summary", 2, 5));

    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let updated_content = "```mermaid\ngraph TD\nA --> B\n```";
    let req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, assistant_id
        ))
        .set_json(json!({ "content": updated_content }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["session_id"], session_id);
    assert_eq!(body["message_id"], assistant_id);
    assert_eq!(body["message_count"], 2);

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");

    let persisted = loaded
        .messages
        .iter()
        .find(|m| m.id == assistant_id)
        .expect("assistant message exists");
    assert_eq!(persisted.content, updated_content);
    assert!(loaded.token_usage.is_none());
    assert!(loaded.conversation_summary.is_none());
}

#[actix_web::test]
async fn test_patch_message_rejects_empty_content() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-patch-2".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let assistant = Message::assistant("original", None);
    let assistant_id = assistant.id.clone();
    session.add_message(assistant);
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, assistant_id
        ))
        .set_json(json!({ "content": "   " }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_patch_message_rejects_assistant_tool_call_messages() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-patch-3".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let assistant_with_tool =
        Message::assistant("calling tool", Some(vec![create_tool_call("tool-call-1")]));
    let message_id = assistant_with_tool.id.clone();
    session.add_message(assistant_with_tool);
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, message_id
        ))
        .set_json(json!({ "content": "updated" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_patch_message_rejects_non_assistant_messages() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-patch-4".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let user_message = Message::user("hello");
    let message_id = user_message.id.clone();
    session.add_message(user_message);
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, message_id
        ))
        .set_json(json!({ "content": "updated" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_patch_message_returns_not_found_for_missing_session_or_message() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-patch-5".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    let assistant = Message::assistant("original", None);
    session.add_message(assistant);
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let missing_session_req = test::TestRequest::patch()
        .uri("/api/v1/sessions/not-exist/messages/msg-1")
        .set_json(json!({ "content": "updated" }))
        .to_request();
    let missing_session_resp = test::call_service(&app, missing_session_req).await;
    assert_eq!(
        missing_session_resp.status(),
        actix_web::http::StatusCode::NOT_FOUND
    );

    let missing_message_req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/not-exist",
            session_id
        ))
        .set_json(json!({ "content": "updated" }))
        .to_request();
    let missing_message_resp = test::call_service(&app, missing_message_req).await;
    assert_eq!(
        missing_message_resp.status(),
        actix_web::http::StatusCode::NOT_FOUND
    );
}

#[actix_web::test]
async fn test_patch_message_returns_conflict_when_session_is_running() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = "session-patch-6".to_string();

    let mut session = Session::new(session_id.clone(), "test-model");
    let assistant = Message::assistant("original", None);
    let message_id = assistant.id.clone();
    session.add_message(assistant);
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    {
        let mut runners = state.agent_runners.write().await;
        let mut runner = bamboo_agent::server::app_state::AgentRunner::new();
        runner.status = bamboo_agent::server::app_state::AgentStatus::Running;
        runners.insert(session_id.clone(), runner);
    }

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/sessions/{session_id}/messages/{message_id}",
        web::patch().to(messages::patch_message),
    ))
    .await;

    let req = test::TestRequest::patch()
        .uri(&format!(
            "/api/v1/sessions/{}/messages/{}",
            session_id, message_id
        ))
        .set_json(json!({ "content": "updated" }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::CONFLICT);
}
