//! Actual Anthropic Messages receipt activation; no OpenAI request conversion.
use super::*;

const MESSAGES: &str = "/anthropic/v1/messages";
fn anthropic_body() -> Value {
    json!({"model":"bamboo-agent","messages":[]})
}
async fn anthropic_state(home: &Path, provider: Arc<dyn LLMProvider>) -> web::Data<AppState> {
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.into()));
    config.provider = "anthropic".into();
    config.providers_mut().anthropic = Some(bamboo_config::AnthropicConfig {
        model: Some("test-model".into()),
        ..Default::default()
    });
    config
        .anthropic_model_mapping
        .mappings
        .insert("sonnet".into(), "test-model".into());
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
async fn anthropic_activation_actual_messages_receipt_starts_anthropic_root_and_exact_output() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-activation-real";
    seed(&state, id).await;
    let sibling = chat(&state, id, "ANTHROPIC_SIBLING_PENDING").await;
    let selected = chat(&state, id, "ANTHROPIC_SELECTED_INPUT").await;
    let headers = activation_headers(&selected, "anthropic-real", id);
    provider.release.notify_one();
    let (status, receipt, response_headers) =
        request_headers(&state, MESSAGES, anthropic_body(), &headers).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "Anthropic receipt activation must reach the real neutral Native port: {receipt}"
    );
    assert_eq!(receipt["object"], "bamboo.activation");
    assert_eq!(receipt["input"]["message_id"], selected["message_id"]);
    assert_eq!(receipt["input"]["ingress_seq"], selected["ingress_seq"]);
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(receipt["session_created_at"].as_str().unwrap())
            .unwrap(),
        chrono::DateTime::parse_from_rfc3339(selected["session_created_at"].as_str().unwrap())
            .unwrap()
    );
    assert_eq!(receipt["session_id"], id);
    assert!(
        receipt.get("content").is_none()
            && receipt.get("usage").is_none()
            && receipt.get("stop_reason").is_none()
    );
    assert_eq!(response_headers.get("cache-control").unwrap(), "no-store");
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
    let visible = committed_output(
        &state,
        id,
        run,
        selected["session_created_at"].as_str().unwrap(),
    )
    .await;
    assert_eq!(visible["input"]["message_id"], selected["message_id"]);
    assert!(visible.to_string().contains("ANTHROPIC_SELECTED_INPUT"));
    assert!(!visible.to_string().contains("ANTHROPIC_SIBLING_PENDING"));
    assert!(!visible.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(state.provider_registry.get("openai").is_none());
}

async fn visible_anthropic(
    state: &web::Data<AppState>,
    id: &str,
    input: &Value,
    key: &str,
    text: &str,
    provider: &OutputProvider,
) -> (Value, Value) {
    let (status, receipt, _) = request_headers(
        state,
        MESSAGES,
        anthropic_body(),
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
async fn anthropic_activation_real_skills_eof_then_action_without_openai_provider() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let source = write_skill(home.path(), "native-proof", false);
    let provider = Arc::new(ReceiptGate {
        inner: provider(home.path(), 4096, true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
    });
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-activation-skills";
    seed(&state, id).await;
    let sibling = chat(&state, id, "ANTHROPIC_SKILL_SIBLING").await;
    let (status,mut selected)=http(&state,"/api/v1/chat",json!({"session_id":id,"message":"Read the proof and perform the action","workspace_path":home.path(),"workflow_selection":selection(&state,"native-proof").await})).await;
    assert_eq!(status, StatusCode::CREATED, "{selected}");
    selected["session_created_at"] = json!(
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
        MESSAGES,
        anthropic_body(),
        &activation_headers(&selected, "anthropic-skills", id),
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
            assert!(messages.iter().any(|m| m.id == selected["message_id"]));
            assert!(!messages.iter().any(|m| m.id == sibling["message_id"]));
        }
    }
    let visible = committed_output(
        &state,
        id,
        run,
        selected["session_created_at"].as_str().unwrap(),
    )
    .await;
    assert!(visible
        .to_string()
        .contains("Normal task completed after complete Skill EOF"));
    assert!(!visible.to_string().contains("ANTHROPIC_SKILL_SIBLING"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert!(state.provider_registry.get("openai").is_none());
}

#[actix_web::test]
async fn anthropic_activation_shares_native_chat_idempotency_exact_cancel_and_output_lifetime() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-shared-operation";
    seed(&state, id).await;
    let sibling = chat(&state, id, "ANTHROPIC_CANCEL_SIBLING").await;
    let selected = chat(&state, id, "ANTHROPIC_CANCEL_PREFIX").await;
    let (receipt, live) = visible_anthropic(
        &state,
        id,
        &selected,
        "three-entry-operation",
        "ANTHROPIC_CANCEL_PREFIX",
        &provider,
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    // This is the original Native key/request identity, not a vendor cache.
    let (status, native, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&selected, "three-entry-operation"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{native}");
    assert_eq!(native["run_id"], run);
    let (status, chat_replay, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&selected, "three-entry-operation", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{chat_replay}");
    assert_eq!(chat_replay, receipt);
    let (status, replay, _) = request_headers(
        &state,
        MESSAGES,
        json!({"model":"bamboo-agent","messages":[],"stream":false,"tools":[]}),
        &activation_headers(&selected, "three-entry-operation", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
    assert_eq!(replay, receipt);
    let (status, conflict, _) = request_headers(
        &state,
        MESSAGES,
        anthropic_body(),
        &activation_headers(&sibling, "three-entry-operation", id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(!state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
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
    assert!(!cancelled.to_string().contains("ANTHROPIC_CANCEL_SIBLING"));
    assert!(!cancelled
        .to_string()
        .contains("PRIVATE_REASONING_NOT_OUTPUT"));
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    state.agent_runners.write().await.remove(id);
    let (status, replay, _) = request_headers(
        &state,
        MESSAGES,
        anthropic_body(),
        &activation_headers(&selected, "three-entry-operation", id),
    )
    .await;
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
async fn anthropic_activation_real_coordinator_409_output_410_and_stale_cancel_never_adopt() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-coordinator";
    seed(&state, id).await;
    let first = chat(&state, id, "ANTHROPIC_BOOTSTRAP").await;
    let (receipt, _) = visible_anthropic(
        &state,
        id,
        &first,
        "anthropic-bootstrap",
        "ANTHROPIC_BOOTSTRAP",
        &provider,
    )
    .await;
    let old = receipt["run_id"].as_str().unwrap();
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = first["session_created_at"].as_str().unwrap();
    committed_output(&state, id, old, birth).await;
    // No activation spawner replacement: the actual default coordinator wins.
    let automatic = chat(&state, id, "ANTHROPIC_AUTOMATIC_OWNER").await;
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
        MESSAGES,
        anthropic_body(),
        &activation_headers(&automatic, "no-anthropic-adoption", id),
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
    let (status, cached, _) = request_headers(
        &state,
        MESSAGES,
        anthropic_body(),
        &activation_headers(&first, "anthropic-bootstrap", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(cached, receipt);
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
async fn anthropic_activation_native_request_body_headers_and_owner_ceiling_fail_closed() {
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-validation";
    seed(&state, id).await;
    let input = chat(&state, id, "ANTHROPIC_KEEP_PENDING").await;
    let headers = activation_headers(&input, "anthropic-validation", id);
    let variants = [
        json!({"model":"claude-sonnet"}),
        json!({"messages":[{"role":"user","content":"NEW_TRANSCRIPT_IS_NOT_INPUT"}]}),
        json!({"system":"SYSTEM_IS_NOT_AUTHORITY"}),
        json!({"system":[{"type":"text","text":"BLOCKS_ARE_NOT_AUTHORITY"}]}),
        json!({"max_tokens":1}),
        json!({"max_tokens":0}),
        json!({"temperature":0}),
        json!({"top_p":0}),
        json!({"top_k":0}),
        json!({"stop_sequences":[]}),
        json!({"stream":true}),
        json!({"tools":[{"name":"FakeTool","input_schema":{"type":"object"}}]}),
        json!({"tool_choice":"auto"}),
        json!({"tool_choice":{"type":"tool","name":"FakeTool"}}),
        json!({"thinking":{"type":"enabled","budget_tokens":1}}),
        json!({"stream_options":{"include_usage":true}}),
        json!({"no_human_approver":true}),
        json!({"run_budget":{"max_rounds":999999}}),
    ];
    for fields in variants {
        let mut body = anthropic_body();
        body.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let (status, rejected, _) = request_headers(&state, MESSAGES, body, &headers).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
        assert_eq!(
            rejected["error"]["code"],
            "unsupported_bamboo_activation_body"
        );
    }
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
            request_headers(&state, MESSAGES, anthropic_body(), &partial)
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
            .uri(MESSAGES)
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(anthropic_body());
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
            MESSAGES,
            anthropic_body(),
            &activation_headers(&stale, "anthropic-stale-birth", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let issued = state
        .codex_run_tokens
        .issue("anthropic-worker-only")
        .unwrap();
    for (peer, token, expected) in [
        (
            "127.0.0.1:5700",
            Some(issued.token),
            StatusCode::UNAUTHORIZED,
        ),
        ("203.0.113.4:5700", None, StatusCode::FORBIDDEN),
    ] {
        let mut req = test::TestRequest::post()
            .uri(MESSAGES)
            .peer_addr(peer.parse().unwrap())
            .insert_header(("Host", "remote.example"))
            .set_json(anthropic_body());
        for (name, value) in &headers {
            req = req.insert_header((*name, value.as_str()));
        }
        if let Some(token) = token {
            req = req.insert_header(("Authorization", format!("Bearer {token}")));
        }
        assert_eq!(
            test::call_service(&app, req.to_request()).await.status(),
            expected
        );
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    no_running(&state, id).await;
}

#[actix_web::test]
async fn anthropic_activation_plain_messages_sse_remains_stateless_and_activation_stream_rejects() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = anthropic_state(home.path(), provider.clone()).await;
    let id = "anthropic-original-sse";
    seed(&state, id).await;
    let input = chat(&state, id, "SSE_MUST_NOT_CONSUME_PENDING").await;
    let (status, rejected, _) = request_headers(
        &state,
        MESSAGES,
        json!({"model":"bamboo-agent","messages":[],"stream":true}),
        &activation_headers(&input, "anthropic-stream-unsupported", id),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    provider.release.notify_one();
    let req=test::TestRequest::post().uri(MESSAGES).peer_addr("127.0.0.1:5700".parse().unwrap()).insert_header(("Idempotency-Key","ordinary-anthropic-only")).set_json(json!({"model":"claude-sonnet","messages":[{"role":"user","content":"ordinary Anthropic SSE"}],"max_tokens":16,"stream":true})).to_request();
    let response = test::call_service(&app, req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let bytes = test::read_body(response).await;
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.contains("message_start")
            && text.contains("message_stop")
            && text.contains("visible answer for ordinary Anthropic SSE"),
        "{text}"
    );
    assert!(!text.contains("bamboo.activation") && !text.contains("SSE_MUST_NOT_CONSUME_PENDING"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    no_running(&state, id).await;
}
