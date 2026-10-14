//! Actual Responses receipt activation using the unchanged neutral Native port.
use super::*;

const RESPONSES: &str = "/openai/v1/responses";
fn responses_body() -> Value {
    json!({"model":"bamboo-agent","input":[]})
}
async fn responses_state(home: &Path, provider: Arc<dyn LLMProvider>) -> web::Data<AppState> {
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
async fn responses_activation_actual_selected_receipt_runs_root_and_reads_only_its_output() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-activation-real";
    seed(&state, id).await;
    let sibling = chat(&state, id, "RESPONSES_SIBLING_PENDING").await;
    let selected = chat(&state, id, "RESPONSES_SELECTED_INPUT").await;
    provider.release.notify_one();
    let (status, receipt, headers) = request_headers(
        &state,
        RESPONSES,
        responses_body(),
        &activation_headers(&selected, "responses-real", id),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "Responses explicit receipt must reach the actual Native activation port: {receipt}"
    );
    assert_eq!(receipt["object"], "bamboo.activation");
    assert_eq!(receipt["input"]["message_id"], selected["message_id"]);
    assert_eq!(receipt["input"]["ingress_seq"], selected["ingress_seq"]);
    assert!(
        receipt.get("output").is_none()
            && receipt.get("usage").is_none()
            && receipt.get("choices").is_none()
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
    assert!(output.to_string().contains("RESPONSES_SELECTED_INPUT"));
    assert!(
        !output.to_string().contains("RESPONSES_SIBLING_PENDING")
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

async fn visible_responses(
    state: &web::Data<AppState>,
    id: &str,
    input: &Value,
    key: &str,
    text: &str,
    provider: &OutputProvider,
) -> (Value, Value) {
    let (status, receipt, _) = request_headers(
        state,
        RESPONSES,
        responses_body(),
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
async fn responses_activation_real_skills_eof_then_action_uses_server_owned_provider() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let source = write_skill(home.path(), "native-proof", false);
    let provider = Arc::new(ReceiptGate {
        inner: provider(home.path(), 4096, true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
    });
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-skills";
    seed(&state, id).await;
    let sibling = chat(&state, id, "RESPONSES_SKILL_SIBLING").await;
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
        RESPONSES,
        responses_body(),
        &activation_headers(&input, "responses-skills", id),
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
    assert!(!visible.to_string().contains("RESPONSES_SKILL_SIBLING"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert!(state.provider_registry.get("openai").is_none());
}

#[actix_web::test]
async fn responses_activation_four_entry_idempotency_cancel_prefix_410_and_proxy_cannot_replay() {
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-four-entry";
    seed(&state, id).await;
    let sibling = chat(&state, id, "RESPONSES_CANCEL_SIBLING").await;
    let selected = chat(&state, id, "RESPONSES_CANCEL_PREFIX").await;
    let (receipt, live) = visible_responses(
        &state,
        id,
        &selected,
        "four-entry-operation",
        "RESPONSES_CANCEL_PREFIX",
        &provider,
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    let headers = activation_headers(&selected, "four-entry-operation", id);
    let (status, native, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&selected, "four-entry-operation"),
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
            RESPONSES,
            json!({"model":"bamboo-agent","input":[],"stream":false,"tools":[]}),
        ),
    ] {
        let (status, replay, _) = request_headers(&state, path, body, &headers).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{path}: {replay}");
        assert_eq!(replay, receipt);
    }
    // Responses accepts this credential for ordinary model proxying. Even a
    // cache hit must pass the neutral verified User gate before Native replay.
    let issued = state
        .codex_run_tokens
        .issue("responses-worker-cache")
        .unwrap();
    let mut scoped = headers.clone();
    scoped.push(("Authorization", format!("Bearer {}", issued.token)));
    let (status, denied, _) = request_headers(&state, RESPONSES, responses_body(), &scoped).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    let (status, conflict, _) = request_headers(
        &state,
        RESPONSES,
        responses_body(),
        &activation_headers(&sibling, "four-entry-operation", id),
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
        !cancelled.to_string().contains("RESPONSES_CANCEL_SIBLING")
            && !cancelled
                .to_string()
                .contains("PRIVATE_REASONING_NOT_OUTPUT")
    );
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    state.agent_runners.write().await.remove(id);
    let (status, replay, _) = request_headers(&state, RESPONSES, responses_body(), &headers).await;
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
async fn responses_activation_real_coordinator_409_background_410_and_stale_cancel_isolated() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-coordinator";
    seed(&state, id).await;
    let first = chat(&state, id, "RESPONSES_BOOTSTRAP").await;
    let (receipt, _) = visible_responses(
        &state,
        id,
        &first,
        "responses-bootstrap",
        "RESPONSES_BOOTSTRAP",
        &provider,
    )
    .await;
    let old = receipt["run_id"].as_str().unwrap();
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = first["session_created_at"].as_str().unwrap();
    committed_output(&state, id, old, birth).await;
    let automatic = chat(&state, id, "RESPONSES_AUTOMATIC_OWNER").await;
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
        RESPONSES,
        responses_body(),
        &activation_headers(&automatic, "responses-no-adoption", id),
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
        RESPONSES,
        responses_body(),
        &activation_headers(&first, "responses-bootstrap", id),
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
async fn responses_activation_strict_presence_null_headers_foreign_and_owner_reject_without_admission(
) {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-validation";
    seed(&state, id).await;
    let input = chat(&state, id, "RESPONSES_KEEP_PENDING").await;
    let headers = activation_headers(&input, "responses-validation", id);
    let variants = [
        json!({"model":null}),
        json!({"model":"test-model"}),
        json!({"input":null}),
        json!({"input":""}),
        json!({"input":"NEW_TRANSCRIPT_IS_NOT_INPUT"}),
        json!({"input":[{"role":"user","content":"not canonical"}]}),
        json!({"stream":true}),
        json!({"stream":null}),
        json!({"tools":null}),
        json!({"tools":[null]}),
        json!({"tools":[{"type":"function","name":"FakeTool","parameters":{"type":"object"}}]}),
    ];
    for fields in variants {
        let mut body = responses_body();
        body.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let (status, rejected, _) = request_headers(&state, RESPONSES, body, &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
        assert_eq!(
            rejected["error"]["code"],
            "unsupported_bamboo_activation_body"
        );
    }
    // Unknown fields are rejected by presence, including explicit null.
    for field in [
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
        let mut body = responses_body();
        body[field] = Value::Null;
        let (status, rejected, _) = request_headers(&state, RESPONSES, body, &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {rejected}");
        assert_eq!(
            rejected["error"]["code"],
            "unsupported_bamboo_activation_body"
        );
    }
    let mut missing = responses_body();
    missing.as_object_mut().unwrap().remove("input");
    assert_eq!(
        request_headers(&state, RESPONSES, missing, &headers)
            .await
            .0,
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
            request_headers(&state, RESPONSES, responses_body(), &partial)
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
            .uri(RESPONSES)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(responses_body());
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
    let mut stale = input.clone();
    stale["session_created_at"] = json!(chrono::Utc::now());
    assert_eq!(
        request_headers(
            &state,
            RESPONSES,
            responses_body(),
            &activation_headers(&stale, "responses-stale-birth", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let foreign = "responses-foreign";
    seed(&state, foreign).await;
    let foreign_input = chat(&state, foreign, "FOREIGN_RECEIPT").await;
    let mut wrong = input.clone();
    wrong["message_id"] = foreign_input["message_id"].clone();
    assert_eq!(
        request_headers(
            &state,
            RESPONSES,
            responses_body(),
            &activation_headers(&wrong, "responses-foreign-receipt", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let mut req = test::TestRequest::post()
        .uri(RESPONSES)
        .peer_addr("203.0.113.4:5700".parse().unwrap())
        .insert_header(("Host", "remote.example"))
        .set_json(responses_body());
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
struct ProxyCall {
    options: bamboo_llm::LLMRequestOptions,
    max_output: Option<u32>,
    tools: Vec<String>,
}
struct ProxyProvider {
    inner: Arc<OutputProvider>,
    calls: std::sync::Mutex<Vec<ProxyCall>>,
}
#[async_trait]
impl LLMProvider for ProxyProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.inner
            .chat_stream(messages, tools, max_output, model)
            .await
    }
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.calls.lock().unwrap().push(ProxyCall {
            options: options.cloned().unwrap_or_default(),
            max_output,
            tools: tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
        });
        self.inner
            .chat_stream(messages, tools, max_output, model)
            .await
    }
}

#[actix_web::test]
async fn responses_activation_preserves_ordinary_proxy_session_dto_sse_and_never_promotes_worker() {
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = Arc::new(ProxyProvider {
        inner: OutputProvider::new(false),
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let state = responses_state(home.path(), provider.clone()).await;
    let id = "responses-proxy-ceiling";
    seed(&state, id).await;
    let input = chat(&state, id, "PROXY_MUST_NOT_CONSUME_ROOT_INPUT").await;
    let worker = "responses-scoped-worker";
    let issued = state.codex_run_tokens.issue(worker).unwrap();
    let auth = format!("Bearer {}", issued.token);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    // Flat Responses tools and its own continuation/instructions stay on the
    // original ordinary DTO path, including the genuine middleware context.
    for streaming in [false, true] {
        provider.inner.release.notify_one();
        let body = json!({"model":"test-model","input":"ordinary scoped Responses","instructions":"scope instructions","previous_response_id":"resp_before","store":false,"max_output_tokens":16,"stream":streaming,"tools":[{"type":"function","name":"proxy_scope_tool","parameters":{"type":"object","properties":{}}}]});
        let req = test::TestRequest::post()
            .uri(RESPONSES)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .insert_header(("Authorization", auth.as_str()))
            .insert_header(("Idempotency-Key", "ordinary-response-proxy-key"))
            .set_json(body)
            .to_request();
        let response = test::call_service(&app, req).await;
        assert_eq!(response.status(), StatusCode::OK);
        if streaming {
            assert_eq!(
                response.headers().get("content-type").unwrap(),
                "text/event-stream"
            );
            let bytes = test::read_body(response).await;
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(
                text.contains("response.completed") && text.contains("ordinary scoped Responses"),
                "{text}"
            );
            assert!(
                !text.contains("bamboo.activation")
                    && !text.contains("PROXY_MUST_NOT_CONSUME_ROOT_INPUT")
            );
        } else {
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["object"], "response");
            assert!(body.to_string().contains("ordinary scoped Responses"));
            assert!(body.get("run_id").is_none());
        }
    }
    provider.inner.release.notify_one();
    let (status,ordinary,_)=request_headers(&state,RESPONSES,json!({"model":"test-model","input":"ordinary null Responses","instructions":null,"tools":null,"stream":null,"max_output_tokens":null}),&[("Authorization",auth.clone())]).await;
    assert_eq!(status, StatusCode::OK, "{ordinary}");
    assert_eq!(ordinary["object"], "response");
    {
        let calls = provider.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        for call in &*calls {
            assert_eq!(call.options.session_id.as_deref(), Some(worker));
            assert_eq!(
                call.options.request_purpose.as_deref(),
                Some("openai_compat")
            );
        }
        for call in &calls[..2] {
            let options = call.options.responses.as_ref().unwrap();
            assert_eq!(options.instructions.as_deref(), Some("scope instructions"));
            assert_eq!(options.previous_response_id.as_deref(), Some("resp_before"));
            assert_eq!(options.store, Some(false));
            assert_eq!(call.max_output, Some(16));
            assert_eq!(call.tools, vec!["proxy_scope_tool"]);
        }
        assert_eq!(calls[2].max_output, None);
        assert!(calls[2].tools.is_empty());
        assert!(calls[2]
            .options
            .responses
            .as_ref()
            .unwrap()
            .instructions
            .is_none());
    }
    let duplicate = test::TestRequest::post()
        .uri(RESPONSES)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .insert_header(("Authorization", auth.as_str()))
        .insert_header(("Content-Type", "application/json"))
        .set_payload(
            r#"{"model":"test-model","model":"test-model","input":"duplicate must not forward"}"#,
        )
        .to_request();
    assert_eq!(
        test::call_service(&app, duplicate).await.status(),
        StatusCode::BAD_REQUEST
    );
    let mut headers = activation_headers(&input, "worker-never-activates", id);
    headers.push(("Authorization", auth.clone()));
    let (status, denied, _) = request_headers(&state, RESPONSES, responses_body(), &headers).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "valid proxy credential must not gain Root authority: {denied}"
    );
    assert_eq!(provider.inner.calls.load(Ordering::SeqCst), 3);
    assert_eq!(provider.calls.lock().unwrap().len(), 3);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    no_running(&state, id).await;
    assert!(state.storage.load_session(worker).await.unwrap().is_none());
    state.codex_run_tokens.revoke(&issued.token_id);
    let (status, revoked, _) = request_headers(
        &state,
        RESPONSES,
        json!({"model":"test-model","input":"revoked must not forward"}),
        &[("Authorization", auth)],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{revoked}");
    assert_eq!(provider.calls.lock().unwrap().len(), 3);
}
