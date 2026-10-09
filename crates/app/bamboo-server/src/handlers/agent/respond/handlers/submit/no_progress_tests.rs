use super::*;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use actix_web::{test, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, PendingQuestionSource, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};

#[derive(Default)]
struct ProgressProvider {
    calls: AtomicUsize,
    user_directions: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl LLMProvider for ProgressProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.user_directions.lock().unwrap().extend(
            messages
                .iter()
                .filter(|message| message.role == bamboo_agent_core::Role::User)
                .map(|message| message.content.clone()),
        );
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::Token(
                "Finished using the requested approach.".into(),
            )),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn state_with_provider(
    path: &std::path::Path,
    provider: Arc<ProgressProvider>,
) -> web::Data<AppState> {
    let mut config = bamboo_llm::Config::from_data_dir(Some(path.to_path_buf()));
    config.provider = "progress-test".into();
    config.features.provider_model_ref = true;
    let provider: Arc<dyn LLMProvider> = provider;
    let mut state = AppState::new_with_provider(path.to_path_buf(), config, provider.clone())
        .await
        .unwrap();
    state.provider_registry = Arc::new(bamboo_llm::ProviderRegistry::new(
        HashMap::from([("progress-test".into(), provider)]),
        "progress-test".into(),
    ));
    state.provider_router = Arc::new(bamboo_llm::ProviderModelRouter::new(
        state.provider_registry.clone(),
    ));
    web::Data::new(state)
}

async fn save_paused(state: &AppState, id: &str) -> Session {
    let mut session = Session::new(id, "model");
    session.set_workspace_path_meta(state.app_data_dir.to_string_lossy());
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "read-before-pause".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: r#"{"path":"file"}"#.into(),
            },
        }]),
    ));
    session.add_message(Message::tool_result_with_status(
        "read-before-pause",
        "same evidence",
        true,
    ));
    let mut question = Message::assistant(
        "The run is paused. Continue, stop, or provide a different approach.",
        None,
    );
    question.metadata = Some(serde_json::json!({"runtime_kind": "observation_progress_pause"}));
    let question_id = question.id.clone();
    session.add_message(question);
    session.set_pending_question_with_source(
        question_id,
        "observation_progress".into(),
        "The run is paused.".into(),
        vec!["Continue".into(), "Stop".into()],
        true,
        PendingQuestionSource::AgenticClarification,
    );
    session.metadata.insert(
        "runtime.suspend_reason".into(),
        "awaiting_clarification".into(),
    );
    session.agent_runtime_state = Some(AgentRuntimeState {
        status: AgentStatusState::Suspended,
        ..Default::default()
    });
    session.set_last_run_status("suspended");
    state.save_and_cache_session(&mut session).await;
    session
}

async fn wait_completed(state: &AppState, id: &str) -> Session {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let running = state
                .agent_runners
                .read()
                .await
                .get(id)
                .is_some_and(|runner| {
                    matches!(
                        runner.status,
                        bamboo_engine::AgentStatus::Pending | bamboo_engine::AgentStatus::Running
                    )
                });
            let durable = state.storage.load_session(id).await.unwrap().unwrap();
            if !running && durable.last_run_status().as_deref() == Some("completed") {
                break durable;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the resumed Root completes")
}

#[actix_web::test]
async fn no_progress_registered_http_stop_is_consumed_once_with_zero_provider_calls() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(ProgressProvider::default());
    let state = state_with_provider(dir.path(), provider.clone()).await;
    let paused = save_paused(&state, "http-progress-stop").await;
    let evidence = serde_json::to_value(&paused.messages[..2]).unwrap();
    let question_id = paused.pending_question.unwrap().tool_call_id;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    // Subscribe through the registered SSE route while the Root is paused.
    // The Stop response must reach this live stream before releasing ownership.
    let events = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions/http-progress-stop/events")
            .to_request(),
    )
    .await;
    assert_eq!(events.status(), actix_web::http::StatusCode::OK);
    let request = || {
        test::TestRequest::post()
            .uri("/api/v1/sessions/http-progress-stop/respond")
            .set_json(serde_json::json!({"response": "Stop", "expected_tool_call_id": question_id}))
            .to_request()
    };
    let response = test::call_service(&app, request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["stopped"], true);
    assert_eq!(body["auto_resume_status"], "completed");
    let stream = tokio::time::timeout(std::time::Duration::from_secs(5), test::read_body(events))
        .await
        .expect("Stop closes the existing HTTP stream");
    let stream = String::from_utf8(stream.to_vec()).unwrap();
    assert!(
        stream.contains("Stopped this run. Send another message to continue."),
        "{stream}"
    );
    assert!(stream.contains("\"type\":\"cancelled\""), "{stream}");
    assert_eq!(stream.matches("[DONE]").count(), 1);
    let duplicate = test::call_service(&app, request()).await;
    assert_eq!(duplicate.status(), actix_web::http::StatusCode::BAD_REQUEST);
    let durable = state
        .storage
        .load_session("http-progress-stop")
        .await
        .unwrap()
        .unwrap();
    assert!(durable.pending_question.is_none());
    assert_eq!(durable.last_run_status().as_deref(), Some("cancelled"));
    assert_eq!(
        serde_json::to_value(&durable.messages[..2]).unwrap(),
        evidence
    );
    assert!(!bamboo_engine::session_app::execute::has_pending_user_message(&durable));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(!state
        .agent_runners
        .read()
        .await
        .get(&durable.id)
        .is_some_and(|runner| matches!(
            runner.status,
            bamboo_engine::AgentStatus::Pending | bamboo_engine::AgentStatus::Running
        )));
}

#[actix_web::test]
async fn no_progress_preflight_binds_question_for_clients_without_expected_id() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(ProgressProvider::default());
    let state = state_with_provider(dir.path(), provider).await;
    let paused = save_paused(&state, "progress-default-cas-id").await;
    let pending = paused.pending_question.as_ref().unwrap();
    assert_eq!(
        response_expected_tool_call_id(&paused, pending, None),
        Some(pending.tool_call_id.clone())
    );
    assert_eq!(
        response_expected_tool_call_id(&paused, pending, Some("client-expected")),
        Some("client-expected".into())
    );
    let mut other = paused.clone();
    other.set_pending_question(
        "other-call".into(),
        "conclusion_with_options".into(),
        "Choose".into(),
        vec![],
        true,
    );
    assert_eq!(
        response_expected_tool_call_id(&other, other.pending_question.as_ref().unwrap(), None),
        None
    );
}

#[actix_web::test]
async fn no_progress_registered_http_continue_and_custom_input_resume_once() {
    for (id, direction) in [
        ("http-progress-continue", "Continue"),
        ("http-progress-custom", "Inspect the other file and finish"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(ProgressProvider::default());
        let state = state_with_provider(dir.path(), provider.clone()).await;
        let paused = save_paused(&state, id).await;
        let evidence = serde_json::to_value(&paused.messages[..2]).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post().uri(&format!("/api/v1/sessions/{id}/respond")).set_json(serde_json::json!({"response": direction, "expected_tool_call_id": paused.pending_question.unwrap().tool_call_id})).to_request()).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let durable = wait_completed(&state, id).await;
        assert!(durable.pending_question.is_none());
        assert_eq!(
            serde_json::to_value(&durable.messages[..2]).unwrap(),
            evidence
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(provider
            .user_directions
            .lock()
            .unwrap()
            .iter()
            .any(|text| text.contains(direction)));
    }
}

#[actix_web::test]
async fn no_progress_registered_http_ordinary_chat_message_resumes_runtime_question() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(ProgressProvider::default());
    let state = state_with_provider(dir.path(), provider.clone()).await;
    let paused = save_paused(&state, "http-progress-chat").await;
    let evidence_messages = paused.messages[..2].to_vec();
    let evidence = serde_json::to_value(&evidence_messages).unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let chat = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            // Stable Human input IDs use the authenticated Inbox route.
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(serde_json::json!({
                "session_id": paused.id,
                "message_id": "progress-new-human-message",
                "message": "Use the other file and finish",
                "model": "model",
                "model_ref": {"provider": "progress-test", "model": "model"},
                "workspace_path": dir.path()
            }))
            .to_request(),
    )
    .await;
    let chat_status = chat.status();
    let chat_body = test::read_body(chat).await;
    assert_eq!(
        chat_status,
        actix_web::http::StatusCode::CREATED,
        "chat status={chat_status}, body={}",
        String::from_utf8_lossy(&chat_body)
    );
    let execute = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/sessions/http-progress-chat/execute")
            .set_json(serde_json::json!({}))
            .to_request(),
    )
    .await;
    let execute_status = execute.status();
    let execute_body = test::read_body(execute).await;
    assert!(
        execute_status.is_success(),
        "execute status={execute_status}, body={}",
        String::from_utf8_lossy(&execute_body)
    );
    let durable = wait_completed(&state, "http-progress-chat").await;
    assert!(durable.pending_question.is_none());
    assert!(!durable.metadata.contains_key("runtime.suspend_reason"));
    // Chat may prepend its system prompt; the original records keep their IDs
    // and full contents regardless of their new positions in the history.
    let retained: Vec<_> = durable
        .messages
        .iter()
        .filter(|message| {
            evidence_messages
                .iter()
                .any(|original| original.id == message.id)
        })
        .collect();
    assert_eq!(serde_json::to_value(retained).unwrap(), evidence);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(provider
        .user_directions
        .lock()
        .unwrap()
        .iter()
        .any(|text| text.contains("Use the other file and finish")));
}
