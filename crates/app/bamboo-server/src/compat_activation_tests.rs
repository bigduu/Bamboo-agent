//! Actual compatibility receipt activation over the existing Native contracts.
use super::*;

fn activation_headers(input: &Value, key: &str, id: &str) -> Vec<(&'static str, String)> {
    let mut headers = receipt_headers(input, key);
    headers.push(("Bamboo-Activation", "receipt".into()));
    headers.push(("Bamboo-Session-Id", id.into()));
    headers
}
fn activation_body() -> Value {
    json!({"model":"bamboo-agent","messages":[]})
}

#[actix_web::test]
async fn compat_activation_real_chat_receipt_runs_exact_input_and_reads_isolated_output() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-activation-real";
    seed(&state, id).await;
    let sibling = chat(&state, id, "UNREQUESTED_PENDING").await;
    let selected = chat(&state, id, "COMPAT_SELECTED").await;
    let headers = activation_headers(&selected, "compat-real", id);
    // The pre-existing stateless forwarding path must finish for the red proof.
    // A real Native activation can also complete immediately with this permit.
    provider.release.notify_one();
    let (status, receipt, response_headers) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &headers,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "explicit activation must not be stateless completion: {receipt}"
    );
    assert_eq!(receipt["object"], "bamboo.activation");
    assert_eq!(receipt["input"]["message_id"], selected["message_id"]);
    assert_eq!(receipt["input"]["ingress_seq"], selected["ingress_seq"]);
    assert_eq!(receipt["session_id"], id);
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
    assert!(state
        .session_inbox
        .was_admitted(id, &durable_receipt(&selected).id)
        .await
        .unwrap());
    assert!(receipt.get("choices").is_none() && receipt.get("usage").is_none());
    assert_eq!(
        response_headers.get("Bamboo-Output-Scope").unwrap(),
        "activation-receipt"
    );
    let exposed = response_headers
        .get("access-control-expose-headers")
        .unwrap()
        .to_str()
        .unwrap();
    for name in [
        "bamboo-input-id",
        "bamboo-ingress-seq",
        "bamboo-session-created-at",
        "bamboo-output-scope",
    ] {
        assert!(response_headers.contains_key(name));
        assert!(
            exposed
                .split(',')
                .any(|v| v.trim().eq_ignore_ascii_case(name)),
            "{name}: {exposed}"
        );
    }
    assert_eq!(
        receipt["output_url"],
        format!("/api/v1/sessions/{id}/runs/{run}/output")
    );
    assert_eq!(receipt["cancel_url"], format!("/api/v1/sessions/{id}/stop"));
    assert_eq!(receipt["cancel_header"]["Bamboo-Run-Id"], run);
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let visible = committed_output(
        &state,
        id,
        run,
        selected["session_created_at"].as_str().unwrap(),
    )
    .await;
    assert_eq!(visible["input"]["message_id"], selected["message_id"]);
    assert!(visible.to_string().contains("COMPAT_SELECTED"));
    assert!(!visible.to_string().contains("UNREQUESTED_PENDING"));
    assert!(!visible.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let (status, retry, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry, receipt);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

async fn visible_activation(
    state: &web::Data<AppState>,
    id: &str,
    input: &Value,
    key: &str,
    text: &str,
    provider: &OutputProvider,
) -> (Value, Value) {
    let (status, receipt, _) = request_headers(
        state,
        "/openai/v1/chat/completions",
        activation_body(),
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
    let birth = input["session_created_at"].as_str().unwrap();
    let published = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let (status, body) = output(state, id, run, Some(birth)).await;
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
async fn compat_activation_real_skills_reader_pages_then_action_preserves_current_input() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let source = write_skill(home.path(), "native-proof", false);
    let provider = Arc::new(ReceiptGate {
        inner: provider(home.path(), 4096, true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
    });
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-skills-reader";
    seed(&state, id).await;
    let sibling = chat(&state, id, "NEVER_READ_THIS_SIBLING").await;
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
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&selected, "compat-skill", id),
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
    assert!(!visible.to_string().contains("NEVER_READ_THIS_SIBLING"));
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
}

#[actix_web::test]
async fn compat_activation_native_shared_idempotency_cancel_and_retained_prefix() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-cancel";
    seed(&state, id).await;
    let sibling = chat(&state, id, "UNSELECTED_CANCEL_SIBLING").await;
    let selected = chat(&state, id, "CANCEL_PREFIX").await;
    let (receipt, live) = visible_activation(
        &state,
        id,
        &selected,
        "compat-native-key",
        "CANCEL_PREFIX",
        &provider,
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    let headers = receipt_headers(&selected, "compat-native-key");
    let (status, native, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{native}");
    assert_eq!(native["run_id"], run);
    let (status, retry, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&selected, "compat-native-key", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry, receipt);
    let (status, conflict, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&sibling, "compat-native-key", id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
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
    assert!(state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Cancelled));
    let (status, visible) = output(&state, id, run, selected["session_created_at"].as_str()).await;
    assert_eq!(status, StatusCode::OK, "{visible}");
    assert_eq!(visible["runner_status"], "cancelled");
    assert_eq!(visible["input"], live["input"]);
    assert_eq!(visible["output"]["messages"], live["output"]["messages"]);
    assert!(!visible.to_string().contains("UNSELECTED_CANCEL_SIBLING"));
    assert!(!visible.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    // The only cached operation is the original Native execute. A cached202
    // does not retain its finite output or create a fresh run after eviction.
    state.agent_runners.write().await.remove(id);
    let (status, retry, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&selected, "compat-native-key", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry, receipt);
    assert_eq!(
        output(&state, id, run, selected["session_created_at"].as_str())
            .await
            .0,
        StatusCode::GONE
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[actix_web::test]
async fn compat_activation_real_coordinator_wins_receipt_race_without_adoption_or_stale_cancel() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-coordinator";
    seed(&state, id).await;
    let first = chat(&state, id, "COMPAT_BOOTSTRAP").await;
    let (receipt, _) = visible_activation(
        &state,
        id,
        &first,
        "compat-bootstrap",
        "COMPAT_BOOTSTRAP",
        &provider,
    )
    .await;
    let old_run = receipt["run_id"].as_str().unwrap();
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = first["session_created_at"].as_str().unwrap();
    committed_output(&state, id, old_run, birth).await;
    // Actual default ChildCompletionCoordinator remains installed. Chat may
    // wake and admit its new input before any explicit compatibility request.
    let automatic = chat(&state, id, "COORDINATOR_OWNS_THIS_INPUT").await;
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
    assert_ne!(actual, old_run);
    let (status, rejected, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&automatic, "never-adopt-coordinator", id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(state.agent_runners.read().await[id].run_id, actual);
    assert_eq!(
        output(&state, id, &actual, Some(birth)).await.0,
        StatusCode::GONE
    );
    assert_eq!(
        output(&state, id, old_run, Some(birth)).await.0,
        StatusCode::GONE
    );
    let (status, cached, _) = request_headers(
        &state,
        "/openai/v1/chat/completions",
        activation_body(),
        &activation_headers(&first, "compat-bootstrap", id),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(cached, receipt);
    let (status, stale, _) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/stop"),
        json!({}),
        &[("Bamboo-Run-Id", old_run.into())],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert!(!state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
    let (status, stopped, _) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/stop"),
        json!({}),
        &[("Bamboo-Run-Id", actual.clone())],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{stopped}");
    provider.release.notify_one();
    assert!(!matches!(
        done(&state, id).await,
        AgentStatus::Running | AgentStatus::Pending
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
}

#[actix_web::test]
async fn compat_activation_invalid_body_headers_and_foreign_receipts_never_fallback() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-reject";
    seed(&state, id).await;
    let selected = chat(&state, id, "KEEP_SELECTED_PENDING").await;
    let valid = activation_headers(&selected, "compat-reject", id);
    for body in [
        json!({"model":"test-model","messages":[]}),
        json!({"model":"bamboo-agent","messages":[{"role":"user","content":"UNTRUSTED_NEW_INPUT"}]}),
        json!({"model":"bamboo-agent","messages":[],"stream":true}),
        json!({"model":"bamboo-agent","messages":[],"stream_options":{"include_usage":true}}),
        json!({"model":"bamboo-agent","messages":[],"tools":[{"type":"function","function":{"name":"FakeAuthority","parameters":{}}}]}),
        json!({"model":"bamboo-agent","messages":[],"tool_choice":"none"}),
        json!({"model":"bamboo-agent","messages":[],"temperature":0}),
        json!({"model":"bamboo-agent","messages":[],"no_human_approver":true}),
        json!({"model":"bamboo-agent","messages":[],"run_budget":{"max_rounds":999999}}),
    ] {
        let (status, rejected, _) =
            request_headers(&state, "/openai/v1/chat/completions", body, &valid).await;
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
        let headers = valid
            .iter()
            .filter(|(name, _)| *name != missing)
            .cloned()
            .collect::<Vec<_>>();
        let (status, rejected, _) = request_headers(
            &state,
            "/openai/v1/chat/completions",
            activation_body(),
            &headers,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "missing{name}: {rejected}",
            name = missing
        );
    }
    for (name, value) in [
        ("Bamboo-Activation", "unknown"),
        ("Bamboo-Session-Id", "bad/session"),
        ("Bamboo-Ingress-Seq", "0"),
        ("Idempotency-Key", ""),
    ] {
        let mut headers = valid.clone();
        headers.iter_mut().find(|(key, _)| *key == name).unwrap().1 = value.into();
        let (status, rejected, _) = request_headers(
            &state,
            "/openai/v1/chat/completions",
            activation_body(),
            &headers,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {rejected}");
    }
    let mut stale = selected.clone();
    stale["session_created_at"] = json!(chrono::Utc::now());
    assert_eq!(
        request_headers(
            &state,
            "/openai/v1/chat/completions",
            activation_body(),
            &activation_headers(&stale, "wrong-birth", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let foreign = "compat-foreign";
    seed(&state, foreign).await;
    let foreign_input = chat(&state, foreign, "FOREIGN_INPUT").await;
    let mut wrong = selected.clone();
    wrong["message_id"] = foreign_input["message_id"].clone();
    assert_eq!(
        request_headers(
            &state,
            "/openai/v1/chat/completions",
            activation_body(),
            &activation_headers(&wrong, "foreign-input", id)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert_eq!(
        state.session_inbox.inspect(foreign).await.unwrap().pending,
        1
    );
    no_running(&state, id).await;
    no_running(&state, foreign).await;
}

fn other_protocol_requests() -> [(&'static str, Value); 5] {
    [
        (
            "/openai/v1/responses",
            json!({"model":"test-model","input":"ordinary input"}),
        ),
        (
            "/anthropic/v1/messages",
            json!({"model":"claude-sonnet","max_tokens":16,"messages":[{"role":"user","content":"ordinary input"}]}),
        ),
        (
            "/anthropic/v1/complete",
            json!({"model":"claude-sonnet","max_tokens_to_sample":16,"prompt":"\n\nHuman: ordinary input\n\nAssistant:"}),
        ),
        (
            "/gemini/v1beta/models/gemini-pro:generateContent",
            json!({"contents":[{"role":"user","parts":[{"text":"ordinary input"}]}]}),
        ),
        (
            "/gemini/v1beta/models/gemini-pro:streamGenerateContent",
            json!({"contents":[{"role":"user","parts":[{"text":"ordinary input"}]}]}),
        ),
    ]
}

#[actix_web::test]
async fn compat_activation_other_protocol_entries_explicitly_unsupported_without_consumption() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-other-protocols";
    seed(&state, id).await;
    let input = chat(&state, id, "DO_NOT_CONSUME_FROM_OTHER_PROTOCOLS").await;
    for (path, body) in other_protocol_requests() {
        let (status, rejected, headers) = request_headers(
            &state,
            path,
            body.clone(),
            &activation_headers(&input, "other-protocol", id),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{path}: {rejected}");
        assert_eq!(rejected["error"]["code"], "bamboo_activation_not_supported");
        assert_eq!(headers.get("cache-control").unwrap(), "no-store");
        // A partial activation selector must not quietly forward either.
        assert_eq!(
            request_headers(
                &state,
                path,
                body,
                &[(
                    "Bamboo-Input-Id",
                    input["message_id"].as_str().unwrap().into()
                )]
            )
            .await
            .0,
            StatusCode::NOT_IMPLEMENTED
        );
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    no_running(&state, id).await;
}

#[actix_web::test]
async fn compat_activation_unselected_four_protocol_forwarding_remains_stateless() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    state
        .config
        .write()
        .await
        .gemini_model_mapping
        .mappings
        .insert("pro".into(), "test-model".into());
    state
        .config
        .write()
        .await
        .anthropic_model_mapping
        .mappings
        .insert("sonnet".into(), "test-model".into());
    let id = "compat-ordinary-forwarding";
    seed(&state, id).await;
    let input = chat(&state, id, "PENDING_NOT_AUTHORITY_FOR_FORWARDING").await;
    let mut cases = vec![(
        "/openai/v1/chat/completions",
        json!({"model":"test-model","messages":[{"role":"user","content":"ordinary input"}]}),
    )];
    cases.extend(
        other_protocol_requests()
            .into_iter()
            .filter(|(path, _)| !path.ends_with("streamGenerateContent")),
    );
    for (path, body) in cases {
        provider.release.notify_one();
        // Idempotency-Key alone is an ordinary vendor header, not activation.
        let (status, reply, _) = request_headers(
            &state,
            path,
            body,
            &[("Idempotency-Key", "ordinary-provider-request".into())],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {reply}");
        assert!(!reply.to_string().contains("bamboo.activation"));
        assert!(!reply
            .to_string()
            .contains("PENDING_NOT_AUTHORITY_FOR_FORWARDING"));
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    no_running(&state, id).await;
}

#[actix_web::test]
async fn compat_activation_authenticated_owner_ceiling_and_duplicate_headers_fail_closed() {
    use actix_web::HttpMessage as _;
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority as _;
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "compat-owner-ceiling";
    seed(&state, id).await;
    let input = chat(&state, id, "AUTHORIZATION_CANNOT_BE_PROMOTED").await;
    let headers = activation_headers(&input, "owner-ceiling", id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    let issued = state.codex_run_tokens.issue("worker-only").unwrap();
    let mut req = test::TestRequest::post()
        .uri("/openai/v1/chat/completions")
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .insert_header(("Authorization", format!("Bearer {}", issued.token)))
        .set_json(activation_body());
    for (name, value) in &headers {
        req = req.insert_header((*name, value.as_str()));
    }
    assert_eq!(
        test::call_service(&app, req.to_request()).await.status(),
        StatusCode::UNAUTHORIZED
    );
    // Also prove the neutral glue itself rejects an existing verified Worker
    // context if a future provider route admits that narrower credential.
    let mut req = test::TestRequest::post().peer_addr("127.0.0.1:5700".parse().unwrap());
    for (name, value) in &headers {
        req = req.insert_header((*name, value.as_str()));
    }
    let req = req.to_http_request();
    req.extensions_mut()
        .insert(crate::codex_run_tokens::CodexRunAuthContext {
            session_id: "worker-only".into(),
        });
    assert_eq!(
        crate::handlers::llm_compat::activation::activate(state.clone(), req)
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    let mut req = test::TestRequest::post()
        .uri("/openai/v1/chat/completions")
        .peer_addr("203.0.113.4:5700".parse().unwrap())
        .insert_header(("Host", "remote.example"))
        .set_json(activation_body());
    for (name, value) in &headers {
        req = req.insert_header((*name, value.as_str()));
    }
    assert_eq!(
        test::call_service(&app, req.to_request()).await.status(),
        StatusCode::FORBIDDEN
    );
    for duplicate in [
        "Bamboo-Activation",
        "Bamboo-Session-Id",
        "Bamboo-Input-Id",
        "Bamboo-Ingress-Seq",
        "Bamboo-Session-Created-At",
        "Idempotency-Key",
    ] {
        let mut req = test::TestRequest::post()
            .uri("/openai/v1/chat/completions")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(activation_body());
        for (name, value) in &headers {
            req = req.insert_header((*name, value.as_str()));
        }
        let value = headers
            .iter()
            .find(|(name, _)| *name == duplicate)
            .unwrap()
            .1
            .as_str();
        req = req.append_header((duplicate, value));
        assert_eq!(
            test::call_service(&app, req.to_request()).await.status(),
            StatusCode::BAD_REQUEST,
            "duplicate {duplicate}"
        );
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    no_running(&state, id).await;
}

#[actix_web::test]
async fn compat_activation_browser_preflight_keeps_origin_policy_and_receipt_headers() {
    let app = test::init_service(
        App::new()
            .wrap(crate::config::build_cors("127.0.0.1", 9562))
            .configure(crate::configure_routes),
    )
    .await;
    for (origin, expected) in [
        ("http://127.0.0.1:1420", StatusCode::OK),
        ("https://untrusted.example", StatusCode::BAD_REQUEST),
    ] {
        let req=test::TestRequest::default().method(actix_web::http::Method::OPTIONS).uri("/openai/v1/chat/completions").insert_header(("Origin",origin)).insert_header(("Access-Control-Request-Method","POST")).insert_header(("Access-Control-Request-Headers","bamboo-activation,bamboo-session-id,bamboo-input-id,bamboo-ingress-seq,bamboo-session-created-at,idempotency-key")).to_request();
        let response = test::call_service(&app, req).await;
        assert_eq!(response.status(), expected, "{origin}");
        if expected == StatusCode::OK {
            assert_eq!(
                response
                    .headers()
                    .get("access-control-allow-origin")
                    .unwrap(),
                origin
            );
            let allowed = response
                .headers()
                .get("access-control-allow-headers")
                .unwrap()
                .to_str()
                .unwrap()
                .to_lowercase();
            for name in [
                "bamboo-activation",
                "bamboo-session-id",
                "bamboo-input-id",
                "bamboo-ingress-seq",
                "bamboo-session-created-at",
                "idempotency-key",
            ] {
                assert!(allowed.split(',').any(|v| v.trim() == name), "{allowed}");
            }
        }
    }
}
