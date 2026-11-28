use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use actix_web::{http::header, test as http_test, App};
use bamboo_agent_core::ToolSchema;
use bamboo_config::AccessControlConfig;
use bamboo_domain::{
    Message, ProjectId, ProjectManifest, ProviderFamily, ProviderProtocol,
    ProviderTranscriptAuthor, ProviderTranscriptItem, ProviderTranscriptOrigin, ReasoningEffort,
    Role, Session,
};
use bamboo_engine::events::journal;
use bamboo_llm::providers::BodhiProvider;
use bamboo_llm::{
    AnthropicProvider, Config, GeminiProvider, LLMChunk, LLMError, LLMProvider, LLMRequestOptions,
    LLMStream, OpenAIProvider, ProviderRegistry,
};
use fs2::FileExt;
use serde_json::{json, Value};
use tempfile::tempdir;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use super::{web, AppState, StatusCode};
use crate::routes::configure_routes;

const PROVIDER: &str = "category-model-fixture";
const FAST_MODEL: &str = "category-fast-model";

#[derive(Clone, Copy)]
enum Reply {
    Text(&'static str),
    Tools,
    NativeTool,
    Incomplete,
    Failed,
    Pending,
}

struct StubProvider {
    replies: Mutex<VecDeque<Reply>>,
    inputs: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl LLMProvider for StubProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("expected a request-scoped suggestion")
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, FAST_MODEL);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, Role::System);
        assert_eq!(messages[1].role, Role::User);
        assert!(tools.is_empty());
        assert_eq!(max_tokens, Some(32));
        let options = options.unwrap();
        assert_eq!(
            options.request_purpose.as_deref(),
            Some("session_category_suggestion")
        );
        assert_eq!(options.reasoning_effort, Some(ReasoningEffort::Disabled));
        assert!(options.session_id.is_none());
        self.inputs
            .lock()
            .unwrap()
            .push(serde_json::from_str(&messages[1].content).unwrap());
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("extra model call");
        let chunks = match reply {
            Reply::Text(text) => vec![LLMChunk::Token(text.into()), LLMChunk::Done],
            Reply::Tools => vec![
                LLMChunk::Token("1".into()),
                LLMChunk::ToolCalls(vec![serde_json::from_value(json!({
                    "id": "unexpected-fixture",
                    "type": "function",
                    "function": {"name": "ordinary_fixture", "arguments": "{}"},
                }))
                .unwrap()]),
                LLMChunk::Done,
            ],
            Reply::NativeTool => vec![
                LLMChunk::Token("1".into()),
                LLMChunk::ProviderTranscriptItem(
                    ProviderTranscriptItem::try_from_payload(
                        ProviderFamily::OpenAi,
                        ProviderProtocol::OpenAiResponsesV1,
                        ProviderTranscriptOrigin::Provider,
                        ProviderTranscriptAuthor::Model,
                        json!({
                            "type": "function_call",
                            "name": "ordinary_fixture",
                            "call_id": "transcript-fixture",
                            "arguments": "{}"
                        }),
                    )
                    .unwrap(),
                ),
                LLMChunk::Done,
            ],
            Reply::Incomplete => vec![LLMChunk::Token("1".into())],
            Reply::Failed => return Err(LLMError::Api("fixture upstream detail".into())),
            Reply::Pending => return Ok(Box::pin(futures::stream::pending())),
        };
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}

async fn fixture(home: &Path, replies: &[Reply]) -> (web::Data<AppState>, Arc<StubProvider>) {
    let provider = Arc::new(StubProvider {
        replies: Mutex::new(replies.iter().copied().collect()),
        inputs: Mutex::new(Vec::new()),
    });
    let mut config = Config::from_data_dir(Some(home.to_path_buf()));
    config.provider = PROVIDER.into();
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat": {"provider": PROVIDER, "model": "category-chat-model"},
            "fast": {"provider": PROVIDER, "model": FAST_MODEL},
        }))
        .unwrap(),
    );
    let state = Box::pin(AppState::new_with_provider(
        home.to_path_buf(),
        config,
        provider.clone(),
    ))
    .await
    .unwrap();
    set_provider(&state, provider.clone());
    (web::Data::new(state), provider)
}

fn set_provider(state: &AppState, provider: Arc<dyn LLMProvider>) {
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), provider)]),
        PROVIDER.into(),
    ));
}

fn project(state: &AppState, home: &Path, name: &str) -> ProjectManifest {
    let workspace = home.join(name);
    std::fs::create_dir(&workspace).unwrap();
    state
        .project_store
        .create_with_project_path(
            name,
            Some("Fixture Project description".into()),
            workspace.to_string_lossy(),
            Vec::new(),
        )
        .unwrap()
}

async fn seed_category(state: &AppState, project: &ProjectId, id: &str, name: &str) {
    let mut session = Session::new(id, "category-chat-model");
    session.set_project_id_meta(project.to_string());
    session.metadata.insert(
        bamboo_domain::SESSION_CATEGORY_METADATA_KEY.into(),
        name.into(),
    );
    session.add_message(Message::user("Existing session conversation"));
    state.storage.save_session(&session).await.unwrap();
}

async fn session_ids(state: &AppState) -> BTreeSet<String> {
    state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .map(|entry| entry.id)
        .collect()
}

fn suggestion(project: &ProjectId, text: &str) -> http_test::TestRequest {
    http_test::TestRequest::post()
        .uri(&format!(
            "/api/v1/projects/{project}/session-category-suggestion"
        ))
        .set_json(json!({"task_text": text}))
}

#[actix_web::test]
async fn suggestions_use_exact_project_inventory_and_keep_creation_separate() {
    let home = tempdir().unwrap();
    let (state, provider) = fixture(home.path(), &[Reply::Text("2"), Reply::Text("2")]).await;
    let populated = project(&state, home.path(), "populated");
    let empty = project(&state, home.path(), "empty");
    let other = project(&state, home.path(), "other");
    std::fs::write(
        home.path().join("populated/README.md"),
        "Fixture workspace file",
    )
    .unwrap();
    seed_category(&state, &populated.id, "categorized-review", "Review").await;
    seed_category(&state, &populated.id, "categorized-analysis", "Analysis").await;
    seed_category(&state, &other.id, "categorized-other", "Other project").await;
    let before_sessions = session_ids(&state).await;
    let before = journal::read_since(state.account_sink.events_dir(), 0)
        .unwrap()
        .len();
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let selected = http_test::call_service(
        &app,
        suggestion(&populated.id, "  Review the draft  ").to_request(),
    )
    .await;
    assert_eq!(selected.status(), StatusCode::OK);
    assert_eq!(
        selected.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let selected: Value = http_test::read_body_json(selected).await;
    assert_eq!(
        selected,
        json!({"project_id": populated.id, "category": "Review", "outcome": "selected"})
    );
    let starter: Value = http_test::call_and_read_body_json(
        &app,
        suggestion(&empty.id, "Help a customer").to_request(),
    )
    .await;
    assert_eq!(
        starter,
        json!({"project_id": empty.id, "category": "Support", "outcome": "selected"})
    );
    assert_eq!(
        provider.inputs.lock().unwrap().as_slice(),
        &[
            json!({"task_text": "Review the draft", "candidates": ["Analysis", "Review"]}),
            json!({"task_text": "Help a customer", "candidates": ["开发", "Support", "调查"]}),
        ]
    );
    assert_eq!(session_ids(&state).await, before_sessions);
    assert!(state
        .session_store
        .project_session_category_counts(&empty.id)
        .await
        .is_empty());
    assert_eq!(
        journal::read_since(state.account_sink.events_dir(), 0)
            .unwrap()
            .len(),
        before
    );
    let operation_dir = home.path().join("session-create-operations/v1");
    match std::fs::read_dir(&operation_dir) {
        Ok(mut entries) => assert!(entries.all(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_none_or(|extension| extension != "json")
        })),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
    }

    let payload = json!({"project_id": empty.id, "category": starter["category"]});
    let create = || {
        http_test::TestRequest::post()
            .uri("/api/v1/sessions")
            .insert_header(("Idempotency-Key", "selected-category-fixture"))
            .set_json(&payload)
            .to_request()
    };
    let response = http_test::call_service(&app, create()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = http_test::read_body_json(response).await;
    assert_eq!(created["session"]["category"], "Support");
    let replay = http_test::call_service(&app, create()).await;
    assert_eq!(replay.status(), StatusCode::OK);
    let replay: Value = http_test::read_body_json(replay).await;
    assert_eq!(replay["session"]["id"], created["session"]["id"]);
    assert_eq!(replay["session"]["project_id"], empty.id.as_str());
    assert_eq!(replay["session"]["category"], "Support");
    assert_eq!(session_ids(&state).await.len(), before_sessions.len() + 1);
    assert_eq!(provider.inputs.lock().unwrap().len(), 2);
    state.shutdown().await;
}

#[actix_web::test]
async fn invalid_outputs_and_provider_failures_abstain_with_bounded_outcomes() {
    let home = tempdir().unwrap();
    let cases = [
        (Reply::Text("9"), "uncertain"),
        (Reply::Text("[1,2]"), "uncertain"),
        (Reply::Text("\"Review\""), "uncertain"),
        (Reply::Text("null"), "uncertain"),
        (Reply::Tools, "uncertain"),
        (Reply::NativeTool, "uncertain"),
        (Reply::Incomplete, "unavailable"),
        (Reply::Failed, "unavailable"),
        (Reply::Pending, "timeout"),
    ];
    let replies: Vec<_> = cases.iter().map(|(reply, _)| *reply).collect();
    let (state, provider) = fixture(home.path(), &replies).await;
    let target = project(&state, home.path(), "outcomes");
    let before_sessions = session_ids(&state).await;
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    for (_, outcome) in cases {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            http_test::call_service(
                &app,
                suggestion(&target.id, "Review a short note").to_request(),
            ),
        )
        .await
        .expect("bounded response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = http_test::read_body_json(response).await;
        assert_eq!(
            body,
            json!({"project_id": target.id, "category": null, "outcome": outcome})
        );
    }
    assert_eq!(provider.inputs.lock().unwrap().len(), cases.len());
    assert_eq!(session_ids(&state).await, before_sessions);
    state.shutdown().await;
}

#[actix_web::test]
async fn project_and_input_validation_precedes_the_model_call() {
    let home = tempdir().unwrap();
    let (state, provider) = fixture(home.path(), &[]).await;
    let target = project(&state, home.path(), "many-categories");
    let archived = project(&state, home.path(), "archived");
    state
        .project_store
        .archive(&archived.id, archived.revision)
        .unwrap();
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    for (project, text, status) in [
        (target.id.clone(), "  ".to_string(), StatusCode::BAD_REQUEST),
        (
            target.id.clone(),
            "文".repeat(2_001),
            StatusCode::BAD_REQUEST,
        ),
        (
            ProjectId::new(),
            "Review a draft".to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            archived.id,
            "Review a draft".to_string(),
            StatusCode::CONFLICT,
        ),
    ] {
        let response =
            http_test::call_service(&app, suggestion(&project, &text).to_request()).await;
        assert_eq!(response.status(), status);
    }
    for index in 0..33 {
        seed_category(
            &state,
            &target.id,
            &format!("candidate-{index}"),
            &format!("Category {index}"),
        )
        .await;
    }
    let body: Value = http_test::call_and_read_body_json(
        &app,
        suggestion(&target.id, "Review a draft").to_request(),
    )
    .await;
    assert_eq!(
        body,
        json!({"project_id": target.id, "category": null, "outcome": "uncertain"})
    );
    assert!(provider.inputs.lock().unwrap().is_empty());
    state.shutdown().await;
}

#[actix_web::test]
async fn unavailable_project_returns_an_error_without_repairs() {
    let home = tempdir().unwrap();
    let (state, provider) = fixture(home.path(), &[Reply::Text("1")]).await;
    let target = project(&state, home.path(), "busy-project");
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(
            state
                .project_store
                .paths()
                .project_home(&target.id)
                .join(".project.lock"),
        )
        .unwrap();
    FileExt::lock_exclusive(&lock).unwrap();
    let busy =
        http_test::call_service(&app, suggestion(&target.id, "Review a draft").to_request()).await;
    FileExt::unlock(&lock).unwrap();
    assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = http_test::read_body_json(busy).await;
    assert_eq!(error["error"]["code"], "project_unavailable");

    let paths = state.project_store.paths();
    let primary = paths.manifest_path(&target.id);
    let before = std::fs::read(&primary).unwrap();
    let floor = paths
        .project_home(&target.id)
        .join("state/manifest-revision");
    std::fs::write(&floor, b"2\n").unwrap();
    let inconsistent =
        http_test::call_service(&app, suggestion(&target.id, "Review a draft").to_request()).await;
    assert_eq!(inconsistent.status(), StatusCode::CONFLICT);
    assert_eq!(std::fs::read(&primary).unwrap(), before);
    assert_eq!(std::fs::read(&floor).unwrap(), b"2\n");
    std::fs::remove_file(&floor).unwrap();
    let missing_floor =
        http_test::call_service(&app, suggestion(&target.id, "Review a draft").to_request()).await;
    assert_eq!(missing_floor.status(), StatusCode::CONFLICT);
    assert_eq!(std::fs::read(primary).unwrap(), before);
    assert!(!floor.exists());
    assert!(provider.inputs.lock().unwrap().is_empty());
    state.shutdown().await;
}

#[actix_web::test]
async fn managed_transport_accepts_one_answer_and_rejects_ambiguous_wire_output() {
    let home = tempdir().unwrap();
    let (state, _) = fixture(home.path(), &[]).await;
    let target = project(&state, home.path(), "managed-wire");
    for ordinal in 1..=12 {
        seed_category(
            &state,
            &target.id,
            &format!("wire-category-{ordinal}"),
            &format!("Category {ordinal:02}"),
        )
        .await;
    }
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;

    let message =
        |id, text| json!({"type":"message","id":id,"content":[{"type":"output_text","text":text}]});
    let event = |kind, data| format!("event: {kind}\ndata: {data}\n\n");
    let completed = |output| {
        let data = json!({"type":"response.completed","response":{"output":output}});
        event("response.completed", data)
    };
    let item_done = |index, item| {
        let kind = "response.output_item.done";
        event(kind, json!({"type":kind,"output_index":index,"item":item}))
    };
    let part_done = |index, id, part_index, text| {
        let kind = "response.content_part.done";
        event(
            kind,
            json!({
                "type":kind,"output_index":index,"item_id":id,"content_index":part_index,
                "part":{"type":"output_text","text":text}
            }),
        )
    };
    let chat = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"1\"},",
        "\"finish_reason\":\"stop\"},{\"index\":1,\"delta\":{\"content\":\"2\"},",
        "\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    let anthropic = |left, right, second_index| {
        let fragment = |index, text| {
            let data = json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":text}});
            format!("event: content_block_delta\ndata: {data}\n\n")
        };
        format!(
            "{}{}{}",
            fragment(0, left),
            fragment(second_index, right),
            concat!(
                "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
                "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            )
        )
    };
    let gemini = |parts| {
        let data =
            json!({"candidates":[{"index":0,"finishReason":"STOP","content":{"parts":parts}}]});
        format!("data: {data}\n\n")
    };
    let gemini_path = format!("/models/{FAST_MODEL}:streamGenerateContent");
    let bodhi_gemini_path = format!("/proxy/gemini/v1beta{gemini_path}");
    let finished = completed(Value::Null);
    let first_part = part_done(0, "one", 0, "1");
    let cases = [
        (
            "/responses",
            200,
            completed(json!([message("one", "1")])),
            "selected",
        ),
        (
            "/responses",
            200,
            completed(json!([message("one", "1"), {"type":"web_search_call","id":"fixture"}])),
            "uncertain",
        ),
        (
            "/responses",
            200,
            completed(json!([message("one", "1"), message("two", "2")])),
            "uncertain",
        ),
        (
            "/responses",
            200,
            format!(
                "{first_part}{}{}",
                item_done(0, message("one", "1")),
                completed(json!([message("one", "1")]))
            ),
            "selected",
        ),
        (
            "/responses",
            200,
            format!(
                "{first_part}{}{finished}",
                item_done(1, message("two", "2"))
            ),
            "uncertain",
        ),
        (
            "/responses",
            200,
            format!("{first_part}{}{finished}", part_done(0, "one", 1, "2")),
            "uncertain",
        ),
        (
            "/responses",
            200,
            format!("{first_part}{}", completed(json!([message("two", "1")]))),
            "uncertain",
        ),
        ("/chat/completions", 200, chat.into(), "uncertain"),
        ("/messages", 200, anthropic(" ", "1", 0), "selected"),
        ("/messages", 200, anthropic("1", "2", 1), "uncertain"),
        (
            &gemini_path,
            200,
            gemini(json!([{"thought":true,"text":"Thinking"},{"text":"1"}])),
            "selected",
        ),
        (
            &gemini_path,
            200,
            gemini(json!([{"text":"1"},{"text":"2"}])),
            "uncertain",
        ),
        (
            &gemini_path,
            200,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"1\"}]}}]}\n\n".into(),
            "unavailable",
        ),
        (
            &bodhi_gemini_path,
            200,
            gemini(json!([{"text":"1"}])),
            "selected",
        ),
        (
            &bodhi_gemini_path,
            200,
            gemini(json!([{"text":"1"},{"text":"2"}])),
            "uncertain",
        ),
        (
            "/chat/completions",
            503,
            "fixture unavailable".into(),
            "unavailable",
        ),
    ];
    for (case, (endpoint, status, stream, outcome)) in cases.into_iter().enumerate() {
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(status).set_body_raw(stream, "text/event-stream"))
            .expect(1)
            .mount(&upstream)
            .await;
        let url = upstream.uri();
        let provider: Arc<dyn LLMProvider> = match endpoint {
            "/messages" => Arc::new(AnthropicProvider::new("controlled-key").with_base_url(url)),
            path if path == gemini_path => {
                Arc::new(GeminiProvider::new("controlled-key").with_base_url(url))
            }
            path if path == bodhi_gemini_path => Arc::new(
                BodhiProvider::new("controlled-key")
                    .with_base_url(url)
                    .with_target_provider("gemini"),
            ),
            "/responses" => Arc::new(
                OpenAIProvider::new("controlled-key")
                    .with_base_url(url)
                    .with_responses_only_models(vec![FAST_MODEL.into()]),
            ),
            _ => Arc::new(OpenAIProvider::new("controlled-key").with_base_url(url)),
        };
        set_provider(&state, provider);
        let body: Value = http_test::call_and_read_body_json(
            &app,
            suggestion(&target.id, "Review a draft").to_request(),
        )
        .await;
        let requests = upstream.received_requests().await.unwrap();
        let request_paths: Vec<_> = requests.iter().map(|request| request.url.path()).collect();
        let category = (outcome == "selected").then_some("Category 01");
        assert_eq!(
            body,
            json!({"project_id": target.id, "category": category, "outcome": outcome}),
            "wire case {case} for {endpoint}, requests: {request_paths:?}"
        );
        assert_eq!(requests.len(), 1);
        let wire: Value = requests[0].body_json().unwrap();
        if !endpoint.ends_with(":streamGenerateContent") {
            assert_eq!(wire["model"], FAST_MODEL);
        }
        assert!(wire.get("tools").is_none_or(|tools| tools == &json!([])));
        if endpoint == "/responses" {
            assert_eq!(wire["max_output_tokens"], 32);
            assert_eq!(wire["store"], false);
            assert!(wire.get("previous_response_id").is_none());
        }
    }
    state.shutdown().await;
}

#[actix_web::test]
async fn suggestions_keep_existing_api_authentication() {
    let home = tempdir().unwrap();
    let (state, provider) = fixture(home.path(), &[]).await;
    let target = project(&state, home.path(), "protected");
    state.config.write().await.access_control = Some(AccessControlConfig {
        password_enabled: true,
        repair_required: false,
        password_hash: Some(
            "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".into(),
        ),
        password_salt: Some("01010101010101010101010101010101".into()),
        password_credential_ref: None,
        password_configured: false,
        updated_at: None,
        devices: Vec::new(),
    });
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let request = suggestion(&target.id, "Review a draft")
        .peer_addr("203.0.113.7:5700".parse().unwrap())
        .insert_header((header::HOST, "bamboo.example.com"))
        .to_request();
    let response = http_test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(provider.inputs.lock().unwrap().is_empty());
    state.shutdown().await;
}
