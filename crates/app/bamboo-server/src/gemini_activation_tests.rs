//! Actual Gemini receipt activation using the unchanged neutral Native port.
use super::*;

const GEMINI: &str = "/gemini/v1beta/models/bamboo-agent:generateContent";
fn gemini_body() -> Value {
    json!({"contents":[]})
}
async fn gemini_state(home: &Path, provider: Arc<dyn LLMProvider>) -> web::Data<AppState> {
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.into()));
    config.provider = "anthropic".into();
    config.providers_mut().anthropic = Some(bamboo_config::AnthropicConfig {
        model: Some("test-model".into()),
        ..Default::default()
    });
    let mut state = AppState::new_with_provider(home.into(), config, provider.clone())
        .await
        .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        std::collections::HashMap::from([("anthropic".into(), provider)]),
        "anthropic".into(),
    ));
    state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
    assert!(state.provider_registry.get("openai").is_none());
    assert_eq!(state.provider_registry.default_provider_name(), "anthropic");
    web::Data::new(state)
}

#[actix_web::test]
async fn gemini_activation_actual_selected_receipt_runs_root_and_reads_only_its_output() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = gemini_state(home.path(), provider.clone()).await;
    let id = "gemini-activation-real";
    seed(&state, id).await;
    let sibling = chat(&state, id, "GEMINI_SIBLING_PENDING").await;
    let selected = chat(&state, id, "GEMINI_SELECTED_INPUT").await;
    provider.release.notify_one();
    let (status, receipt, headers) = request_headers(
        &state,
        GEMINI,
        gemini_body(),
        &activation_headers(&selected, "gemini-real", id),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "Gemini explicit receipt must reach the actual Native activation port: {receipt}"
    );
    assert_eq!(receipt["object"], "bamboo.activation");
    assert_eq!(receipt["input"]["message_id"], selected["message_id"]);
    assert_eq!(receipt["input"]["ingress_seq"], selected["ingress_seq"]);
    assert!(
        receipt.get("output").is_none()
            && receipt.get("usage").is_none()
            && receipt.get("choices").is_none()
            && receipt.get("candidates").is_none()
            && receipt.get("usageMetadata").is_none()
    );
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let run = receipt["run_id"].as_str().unwrap();
    assert_eq!(state.agent_runners.read().await[id].run_id, run);
    assert_eq!(
        state
            .session_store
            .inspect_actor(id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .run_id,
        run
    );
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let output = committed_output(
        &state,
        id,
        run,
        selected["session_created_at"].as_str().unwrap(),
    )
    .await;
    assert_eq!(output["input"]["message_id"], selected["message_id"]);
    assert!(output.to_string().contains("GEMINI_SELECTED_INPUT"));
    assert!(
        !output.to_string().contains("GEMINI_SIBLING_PENDING")
            && !output.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT")
    );
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(state.provider_registry.get("openai").is_none());
}

async fn visible_gemini(
    state: &web::Data<AppState>,
    id: &str,
    input: &Value,
    key: &str,
    text: &str,
    provider: &OutputProvider,
) -> (Value, Value) {
    let (status, receipt, _) = request_headers(
        state,
        GEMINI,
        gemini_body(),
        &activation_headers(input, key, id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{receipt}");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let run = receipt["run_id"].as_str().unwrap();
    let published = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let (status, body) = output(state, id, run, input["session_created_at"].as_str()).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            if body["output"]["messages"]
                .as_array()
                .is_some_and(|messages| {
                    messages
                        .iter()
                        .any(|m| m["content"] == format!("visible answer for {text}"))
                })
            {
                break body;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (receipt, published)
}

#[actix_web::test]
async fn gemini_activation_real_skills_eof_then_action_uses_server_owned_provider() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let source = write_skill(home.path(), "native-proof", false);
    let provider = Arc::new(ReceiptGate {
        inner: provider(home.path(), 4096, true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
    });
    let state = gemini_state(home.path(), provider.clone()).await;
    let id = "gemini-skills";
    seed(&state, id).await;
    let sibling = chat(&state, id, "GEMINI_SKILL_SIBLING").await;
    let (status,mut input)=http(&state,"/api/v1/chat",json!({"session_id":id,"message":"Read the proof and perform the action","workspace_path":home.path(),"workflow_selection":selection(&state,"native-proof").await})).await;
    assert_eq!(status, StatusCode::CREATED, "{input}");
    input["session_created_at"] = json!(
        state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .created_at
    );
    let (status, receipt, _) = request_headers(
        &state,
        GEMINI,
        gemini_body(),
        &activation_headers(&input, "gemini-skills", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{receipt}");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let run = receipt["run_id"].as_str().unwrap();
    assert_eq!(state.agent_runners.read().await[id].run_id, run);
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    {
        let trace = provider.inner.trace.lock().unwrap();
        assert!(trace.failures.is_empty(), "{:?}", trace.failures);
        assert!(trace.main_pages > 1 && trace.task_completed && trace.requests.len() > 3);
        assert_eq!(
            trace.contents[&("native-proof".into(), "SKILL.md".into())],
            source
        );
        assert_eq!(
            trace.contents[&("native-proof".into(), "references/proof.txt".into())],
            "REFERENCE_EOF_native-proof\n"
        );
        for messages in &trace.requests {
            assert!(messages.iter().any(|m| m.id == input["message_id"]));
            assert!(!messages.iter().any(|m| m.id == sibling["message_id"]));
        }
    }
    let visible = committed_output(
        &state,
        id,
        run,
        input["session_created_at"].as_str().unwrap(),
    )
    .await;
    assert!(visible
        .to_string()
        .contains("Normal task completed after complete Skill EOF"));
    assert!(!visible.to_string().contains("GEMINI_SKILL_SIBLING"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert!(state.provider_registry.get("openai").is_none());
}

#[actix_web::test]
async fn gemini_activation_five_entry_idempotency_cancel_prefix_410_and_proxy_cannot_replay() {
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = gemini_state(home.path(), provider.clone()).await;
    let id = "gemini-five-entry";
    seed(&state, id).await;
    let sibling = chat(&state, id, "GEMINI_CANCEL_SIBLING").await;
    let selected = chat(&state, id, "GEMINI_CANCEL_PREFIX").await;
    let (receipt, live) = visible_gemini(
        &state,
        id,
        &selected,
        "five-entry-operation",
        "GEMINI_CANCEL_PREFIX",
        &provider,
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    let headers = activation_headers(&selected, "five-entry-operation", id);
    let (status, native, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&selected, "five-entry-operation"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{native}");
    assert_eq!(native["run_id"], run);
    for (path, body) in [
        ("/openai/v1/chat/completions", activation_body()),
        (
            "/anthropic/v1/messages",
            json!({"model":"bamboo-agent","messages":[]}),
        ),
        (
            "/openai/v1/responses",
            json!({"model":"bamboo-agent","input":[],"stream":false,"tools":[]}),
        ),
        (GEMINI, json!({"contents":[],"tools":[]})),
    ] {
        let (status, replay, _) = request_headers(&state, path, body, &headers).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{path}: {replay}");
        assert_eq!(replay, receipt);
    }
    // Gemini retains its narrower HTTP path scope: even a valid Responses
    // model-proxy credential cannot replay a Gemini activation cache entry.
    let issued = state.codex_run_tokens.issue("gemini-worker-cache").unwrap();
    let mut scoped = headers.clone();
    scoped.push(("Authorization", format!("Bearer {}", issued.token)));
    let (status, denied, _) = request_headers(&state, GEMINI, gemini_body(), &scoped).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{denied}");
    // Also keep a verified Worker context intact through the actual handler.
    // This synthetic auth fixture checks the neutral User gate if a future
    // route admits such a context; actual run-token path denial is above.
    use actix_web::{dev::Service as _, HttpMessage as _};
    let worker_app = test::init_service(
        App::new()
            .app_data(state.clone())
            .wrap_fn(|req, service| {
                req.extensions_mut()
                    .insert(crate::codex_run_tokens::CodexRunAuthContext {
                        session_id: "gemini-verified-worker".into(),
                    });
                service.call(req)
            })
            .configure(crate::configure_routes),
    )
    .await;
    let mut request = test::TestRequest::post()
        .uri(GEMINI)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .set_json(gemini_body());
    for (name, value) in &headers {
        request = request.insert_header((*name, value.as_str()));
    }
    assert_eq!(
        test::call_service(&worker_app, request.to_request())
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    let (status, conflict, _) = request_headers(
        &state,
        GEMINI,
        gemini_body(),
        &activation_headers(&sibling, "five-entry-operation", id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let (status, stopped, _) = request_headers(
        &state,
        receipt["cancel_url"].as_str().unwrap(),
        json!({}),
        &[("Bamboo-Run-Id", run.to_owned())],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{stopped}");
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Cancelled));
    let (status, cancelled) =
        output(&state, id, run, selected["session_created_at"].as_str()).await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["runner_status"], "cancelled");
    assert_eq!(cancelled["output"]["messages"], live["output"]["messages"]);
    assert!(
        !cancelled.to_string().contains("GEMINI_CANCEL_SIBLING")
            && !cancelled
                .to_string()
                .contains("PRIVATE_REASONING_NOT_OUTPUT")
    );
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    state.agent_runners.write().await.remove(id);
    let (status, replay, _) = request_headers(&state, GEMINI, gemini_body(), &headers).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(replay, receipt);
    assert_eq!(
        output(&state, id, run, selected["session_created_at"].as_str())
            .await
            .0,
        StatusCode::GONE
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[actix_web::test]
async fn gemini_activation_real_coordinator_409_background_410_and_stale_cancel_isolated() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = gemini_state(home.path(), provider.clone()).await;
    let id = "gemini-coordinator";
    seed(&state, id).await;
    let first = chat(&state, id, "GEMINI_BOOTSTRAP").await;
    let (receipt, _) = visible_gemini(
        &state,
        id,
        &first,
        "gemini-bootstrap",
        "GEMINI_BOOTSTRAP",
        &provider,
    )
    .await;
    let old = receipt["run_id"].as_str().unwrap();
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = first["session_created_at"].as_str().unwrap();
    committed_output(&state, id, old, birth).await;
    let automatic = chat(&state, id, "GEMINI_AUTOMATIC_OWNER").await;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    assert!(state
        .session_inbox
        .was_admitted(id, &durable_receipt(&automatic).id)
        .await
        .unwrap());
    let actual = state.agent_runners.read().await[id].run_id.clone();
    assert_ne!(actual, old);
    let (status, rejected, _) = request_headers(
        &state,
        GEMINI,
        gemini_body(),
        &activation_headers(&automatic, "gemini-no-adoption", id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(state.agent_runners.read().await[id].run_id, actual);
    assert_eq!(
        output(&state, id, &actual, Some(birth)).await.0,
        StatusCode::GONE
    );
    assert_eq!(
        output(&state, id, old, Some(birth)).await.0,
        StatusCode::GONE
    );
    let (status, replay, _) = request_headers(
        &state,
        GEMINI,
        gemini_body(),
        &activation_headers(&first, "gemini-bootstrap", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(replay, receipt);
    let (status, stale, _) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/stop"),
        json!({}),
        &[("Bamboo-Run-Id", old.into())],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert!(!state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
    assert_eq!(
        request_headers(
            &state,
            &format!("/api/v1/sessions/{id}/stop"),
            json!({}),
            &[("Bamboo-Run-Id", actual)]
        )
        .await
        .0,
        StatusCode::OK
    );
    provider.release.notify_one();
    assert!(!matches!(
        done(&state, id).await,
        AgentStatus::Pending | AgentStatus::Running
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}

#[actix_web::test]
async fn gemini_activation_strict_presence_null_headers_foreign_and_owner_reject_without_admission()
{
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = gemini_state(home.path(), provider.clone()).await;
    let id = "gemini-validation";
    seed(&state, id).await;
    let input = chat(&state, id, "GEMINI_KEEP_PENDING").await;
    let headers = activation_headers(&input, "gemini-validation", id);
    let variants = [
        json!({"contents":null}),
        json!({"contents":""}),
        json!({"contents":"NEW_TRANSCRIPT_IS_NOT_INPUT"}),
        json!({"contents":[{"role":"user","content":"not canonical"}]}),
        json!({"tools":null}),
        json!({"tools":[null]}),
        json!({"tools":[{"functionDeclarations":[]}]}),
        json!({"tools":[{"type":"function","name":"FakeTool","parameters":{"type":"object"}}]}),
    ];
    for fields in variants {
        let mut body = gemini_body();
        body.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let (status, rejected, _) = request_headers(&state, GEMINI, body, &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
        assert_eq!(
            rejected["error"]["code"],
            "unsupported_bamboo_activation_body"
        );
    }
    // Unknown fields are rejected by presence, including explicit null.
    for field in [
        "model",
        "stream",
        "systemInstruction",
        "system_instruction",
        "generationConfig",
        "generation_config",
        "safetySettings",
        "toolConfig",
        "cachedContent",
        "input",
        "messages",
        "instructions",
        "max_output_tokens",
        "previous_response_id",
        "conversation",
        "tool_choice",
        "parallel_tool_calls",
        "reasoning",
        "metadata",
        "store",
        "include",
        "truncation",
        "text",
        "no_human_approver",
        "run_budget",
        "model_ref",
        "client_sync",
    ] {
        let mut body = gemini_body();
        body[field] = Value::Null;
        let (status, rejected, _) = request_headers(&state, GEMINI, body, &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {rejected}");
        assert_eq!(
            rejected["error"]["code"],
            "unsupported_bamboo_activation_body"
        );
    }
    let mut missing = gemini_body();
    missing.as_object_mut().unwrap().remove("contents");
    assert_eq!(
        request_headers(&state, GEMINI, missing, &headers).await.0,
        StatusCode::BAD_REQUEST
    );
    for missing in [
        "Bamboo-Activation",
        "Bamboo-Session-Id",
        "Bamboo-Input-Id",
        "Bamboo-Ingress-Seq",
        "Bamboo-Session-Created-At",
        "Idempotency-Key",
    ] {
        let partial = headers
            .iter()
            .filter(|(name, _)| *name != missing)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            request_headers(&state, GEMINI, gemini_body(), &partial)
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "missing{missing}"
        );
    }
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    for duplicate in [
        "Bamboo-Activation",
        "Bamboo-Session-Id",
        "Bamboo-Input-Id",
        "Bamboo-Ingress-Seq",
        "Bamboo-Session-Created-At",
        "Idempotency-Key",
    ] {
        let mut req = test::TestRequest::post()
            .uri(GEMINI)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(gemini_body());
        for (name, value) in &headers {
            req = req.insert_header((*name, value.as_str()));
        }
        req = req.append_header((
            duplicate,
            headers
                .iter()
                .find(|(name, _)| *name == duplicate)
                .unwrap()
                .1
                .as_str(),
        ));
        assert_eq!(
            test::call_service(&app, req.to_request()).await.status(),
            StatusCode::BAD_REQUEST,
            "duplicate{duplicate}"
        );
    }
    for path in [
        "/gemini/v1beta/models/gemini-pro:generateContent",
        "/gemini/v1beta/models/Bamboo-agent:generateContent",
    ] {
        let (status, rejected, _) = request_headers(&state, path, gemini_body(), &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {rejected}");
    }
    let duplicate = test::TestRequest::post()
        .uri(GEMINI)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .insert_header(("Content-Type", "application/json"))
        .set_payload(r#"{"contents":[],"contents":[]}"#);
    let mut duplicate = duplicate;
    for (name, value) in &headers {
        duplicate = duplicate.insert_header((*name, value.as_str()));
    }
    assert_eq!(
        test::call_service(&app, duplicate.to_request())
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let (status, unsupported, _) = request_headers(
        &state,
        "/gemini/v1beta/models/bamboo-agent:streamGenerateContent",
        gemini_body(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{unsupported}");
    assert_eq!(
        unsupported["error"]["code"],
        "bamboo_activation_not_supported"
    );
    let mut stale = input.clone();
    stale["session_created_at"] = json!(chrono::Utc::now());
    assert_eq!(
        request_headers(
            &state,
            GEMINI,
            gemini_body(),
            &activation_headers(&stale, "gemini-stale-birth", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let foreign = "gemini-foreign";
    seed(&state, foreign).await;
    let foreign_input = chat(&state, foreign, "FOREIGN_RECEIPT").await;
    let mut wrong = input.clone();
    wrong["message_id"] = foreign_input["message_id"].clone();
    assert_eq!(
        request_headers(
            &state,
            GEMINI,
            gemini_body(),
            &activation_headers(&wrong, "gemini-foreign-receipt", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let mut req = test::TestRequest::post()
        .uri(GEMINI)
        .peer_addr("203.0.113.4:5700".parse().unwrap())
        .insert_header(("Host", "remote.example"))
        .set_json(gemini_body());
    for (name, value) in &headers {
        req = req.insert_header((*name, value.as_str()));
    }
    assert_eq!(
        test::call_service(&app, req.to_request()).await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert_eq!(
        state.session_inbox.inspect(foreign).await.unwrap().pending,
        1
    );
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    no_running(&state, id).await;
}

#[derive(Clone)]
struct GeminiRouteCall {
    model: String,
    tools: Vec<String>,
    user: String,
    max_output: Option<u32>,
}
struct GeminiRouteProvider {
    inner: Arc<OutputProvider>,
    calls: std::sync::Mutex<Vec<GeminiRouteCall>>,
}
impl GeminiRouteProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: OutputProvider::new(false),
            calls: std::sync::Mutex::new(Vec::new()),
        })
    }
}
#[async_trait]
impl LLMProvider for GeminiRouteProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.lock().unwrap().push(GeminiRouteCall {
            model: model.into(),
            tools: tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
            user: messages
                .iter()
                .rev()
                .find(|m| m.role == Role::User)
                .map(|m| m.content.clone())
                .unwrap_or_default(),
            max_output,
        });
        self.inner
            .chat_stream(messages, tools, max_output, model)
            .await
    }
}

#[actix_web::test]
async fn gemini_activation_preserves_ordinary_native_dto_model_provider_routes_sse_and_token_scope()
{
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let default = GeminiRouteProvider::new();
    let endpoint = GeminiRouteProvider::new();
    let state = gemini_state(home.path(), default.clone()).await;
    state.provider_registry.replace_with(ProviderRegistry::new(
        std::collections::HashMap::from([
            ("anthropic".into(), default.clone() as Arc<dyn LLMProvider>),
            ("gemini".into(), endpoint.clone() as Arc<dyn LLMProvider>),
        ]),
        "anthropic".into(),
    ));
    state
        .config
        .write()
        .await
        .gemini_model_mapping
        .mappings
        .insert("pro".into(), "mapped-native-model".into());
    let id = "gemini-ordinary-ceiling";
    seed(&state, id).await;
    let input = chat(&state, id, "ORDINARY_GEMINI_MUST_NOT_CONSUME_ROOT").await;
    let ordinary = "/gemini/v1beta/models/gemini-pro:generateContent";
    let streaming = "/gemini/v1beta/models/gemini-pro:streamGenerateContent";
    let key = [("Idempotency-Key", "ordinary-gemini-provider-key".into())];
    default.inner.release.notify_one();
    let camel = json!({"contents":[{"role":"user","parts":[{"text":"ordinary Gemini fallback"}]}],"systemInstruction":null,"generationConfig":null,"tools":[{"functionDeclarations":[{"name":"native_tool","parameters":{"type":"object"}}]}]});
    let (status, reply, _) = request_headers(&state, ordinary, camel, &key).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert!(reply["candidates"][0]["content"]["parts"][0]["text"]
        .as_str()
        .unwrap()
        .contains("ordinary Gemini fallback"));
    assert!(reply.get("run_id").is_none() && reply.get("object").is_none());
    assert_eq!(default.calls.lock().unwrap().len(), 1);
    assert!(endpoint.calls.lock().unwrap().is_empty());

    // Keep the existing flag-controlled endpoint route for nonstream traffic.
    state.config.write().await.features.provider_model_ref = true;
    endpoint.inner.release.notify_one();
    let snake = json!({"contents":[{"role":"user","parts":[{"text":"ordinary Gemini endpoint"}]}],"system_instruction":null,"generation_config":null,"tools":[{"function_declarations":[{"name":"native_tool","parameters_json_schema":{"type":"object"}}]}]});
    let (status, reply, _) = request_headers(&state, ordinary, snake, &key).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert!(reply.to_string().contains("ordinary Gemini endpoint"));
    assert_eq!(endpoint.calls.lock().unwrap().len(), 1);
    assert_eq!(default.calls.lock().unwrap().len(), 1);

    // Existing Gemini streaming uses the default provider even with that flag.
    // This slice preserves that original routing behavior.
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    default.inner.release.notify_one();
    let request = test::TestRequest::post().uri(streaming)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .set_json(json!({"contents":[{"role":"user","parts":[{"text":"ordinary Gemini stream"}]}],"tools":[{"functionDeclarations":[{"name":"native_tool","parameters":{"type":"object"}}]}]})).to_request();
    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let bytes = test::read_body(response).await;
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.contains("candidates") && text.contains("ordinary Gemini stream"),
        "{text}"
    );
    assert!(
        !text.contains("bamboo.activation")
            && !text.contains("ORDINARY_GEMINI_MUST_NOT_CONSUME_ROOT")
    );
    endpoint.inner.release.notify_one();
    let (status, reply, _) = request_headers(&state, ordinary,
        json!({"contents":[{"role":"user","parts":[{"text":"ordinary Gemini null"}]}],"tools":null,"systemInstruction":null,"generationConfig":null,"unrecognized_ordinary_option":null}), &key).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert!(reply.to_string().contains("ordinary Gemini null"));
    {
        let calls = default.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].user, "ordinary Gemini fallback");
        assert_eq!(calls[1].user, "ordinary Gemini stream");
        for call in &*calls {
            assert_eq!(call.model, "mapped-native-model");
            assert_eq!(call.tools, vec!["native_tool"]);
            assert_eq!(call.max_output, None);
        }
    }
    {
        let calls = endpoint.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].user, "ordinary Gemini endpoint");
        assert_eq!(calls[1].user, "ordinary Gemini null");
        assert_eq!(calls[0].tools, vec!["native_tool"]);
        assert!(calls[1].tools.is_empty());
        for call in &*calls {
            assert_eq!(call.model, "mapped-native-model");
            assert_eq!(call.max_output, None);
        }
    }
    let (status, _, _) = request_headers(
        &state,
        "/gemini/v1beta/models/unmapped:generateContent",
        json!({"contents":[{"role":"user","parts":[{"text":"no model route"}]}]}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for raw in [
        r#"{"contents":[],"contents":[]}"#,
        r#"{"contents":[],"generationConfig":null,"generation_config":null}"#,
    ] {
        let request = test::TestRequest::post()
            .uri(ordinary)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header(("Content-Type", "application/json"))
            .set_payload(raw)
            .to_request();
        assert_eq!(
            test::call_service(&app, request).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    // Valid and revoked Responses run-tokens retain their existing Gemini path
    // denial, including ordinary forwarding, streaming and cached activation.
    let issued = state
        .codex_run_tokens
        .issue("gemini-scoped-worker")
        .unwrap();
    let auth = format!("Bearer {}", issued.token);
    for path in [ordinary, streaming] {
        let request = test::TestRequest::post()
            .uri(path)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header(("Authorization", auth.as_str()))
            .set_json(
                json!({"contents":[{"role":"user","parts":[{"text":"proxy must not forward"}]}]}),
            )
            .to_request();
        assert_eq!(
            test::call_service(&app, request).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let mut headers = activation_headers(&input, "worker-never-activates-gemini", id);
    headers.push(("Authorization", auth.clone()));
    assert_eq!(
        request_headers(&state, GEMINI, gemini_body(), &headers)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    state.codex_run_tokens.revoke(&issued.token_id);
    assert_eq!(
        request_headers(&state, GEMINI, gemini_body(), &headers)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(default.inner.calls.load(Ordering::SeqCst), 2);
    assert_eq!(endpoint.inner.calls.load(Ordering::SeqCst), 2);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    assert!(state.provider_registry.get("openai").is_none());
    no_running(&state, id).await;
}
