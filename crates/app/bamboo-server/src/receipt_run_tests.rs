//! Actual HTTP -> reservation -> ACK -> current-input -> provider/Skills proofs.
use super::*;
use bamboo_domain::{
    ActorDirectoryPort, SessionActivationPolicy, SessionInboxReceipt, SessionMessageEnvelope,
    SessionMessageId,
};

async fn request_headers(
    state: &web::Data<AppState>,
    path: &str,
    body: Value,
    headers: &[(&str, String)],
) -> (StatusCode, Value, actix_web::http::header::HeaderMap) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::configure_routes),
    )
    .await;
    let mut request = test::TestRequest::post()
        .uri(path)
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .set_json(body);
    for (name, value) in headers {
        request = request.insert_header((*name, value.as_str()));
    }
    let response = test::call_service(&app, request.to_request()).await;
    let status = response.status();
    let headers = response.headers().clone();
    (status, test::read_body_json(response).await, headers)
}
fn receipt_headers(receipt: &Value, key: &str) -> Vec<(&'static str, String)> {
    vec![
        (
            "Bamboo-Input-Id",
            receipt["message_id"].as_str().unwrap().into(),
        ),
        (
            "Bamboo-Ingress-Seq",
            receipt["ingress_seq"].as_u64().unwrap().to_string(),
        ),
        (
            "Bamboo-Session-Created-At",
            receipt["session_created_at"].as_str().unwrap().into(),
        ),
        ("Idempotency-Key", key.into()),
    ]
}
async fn chat(state: &web::Data<AppState>, id: &str, message: &str) -> Value {
    let (status, body) = http(
        state,
        "/api/v1/chat",
        json!({"session_id":id,"message":message}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let mut body = body;
    body["session_created_at"] = json!(
        state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .created_at
    );
    body
}
fn durable_receipt(value: &Value) -> SessionInboxReceipt {
    SessionInboxReceipt {
        id: SessionMessageId::parse(value["message_id"].as_str().unwrap()).unwrap(),
        generation: value["ingress_seq"].as_u64().unwrap(),
    }
}
async fn no_running(state: &web::Data<AppState>, id: &str) {
    assert!(!state
        .agent_runners
        .read()
        .await
        .get(id)
        .is_some_and(|r| matches!(r.status, AgentStatus::Pending | AgentStatus::Running)));
    if let Some(active) = state
        .session_store
        .inspect_actor(id)
        .await
        .unwrap()
        .activation
    {
        assert!(
            !active.status.is_live(),
            "abandoned reservation kept a live Actor"
        );
    }
}
struct ReceiptGate {
    inner: Arc<SkillsProvider>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    first: AtomicBool,
}
#[async_trait]
impl LLMProvider for ReceiptGate {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        if !self.first.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(Box::pin(futures::stream::iter(
            self.inner.next(messages, tools).into_iter().map(Ok),
        )))
    }
}
fn gated(home: &Path) -> Arc<ReceiptGate> {
    Arc::new(ReceiptGate {
        inner: provider(home, 4096, false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
    })
}

#[actix_web::test]
async fn receipt_run_actual_selected_user_skills_multiple_rounds_and_ack_leave_siblings_pending() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let source = write_skill(home.path(), "native-proof", false);
    let provider = gated(home.path());
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-real";
    seed(&state, id).await;
    let staged = state
        .session_inbox
        .deliver(&SessionMessageEnvelope::user_input(id, "STAGED_SIBLING"))
        .await
        .unwrap();
    let earlier = chat(&state, id, "EARLIER_ELIGIBLE_SIBLING").await;
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
    let headers = receipt_headers(&selected, "real-selected");
    let body = json!({"skill_mode":"code"});
    let (status, started, response_headers) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/execute"),
        body.clone(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{started}");
    assert_eq!(
        response_headers
            .get("Bamboo-Input-Id")
            .unwrap()
            .to_str()
            .unwrap(),
        selected["message_id"]
    );
    assert_eq!(
        response_headers.get("Bamboo-Output-Scope").unwrap(),
        "activation-receipt"
    );
    let run = started["run_id"].as_str().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let activation = state
        .session_store
        .inspect_actor(id)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_eq!(activation.run_id, run);
    assert_eq!(state.agent_runners.read().await[id].run_id, run);
    assert!(state
        .session_inbox
        .was_admitted(id, &durable_receipt(&selected).id)
        .await
        .unwrap());
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&earlier).id)
        .await
        .unwrap());
    let (status, duplicate, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        body.clone(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate, started);
    let (status, conflict, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        body.clone(),
        &receipt_headers(&earlier, "real-selected"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
    let (status, joined, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        body.clone(),
        &receipt_headers(&earlier, "must-not-join"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{joined}");
    assert!(!state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
    let later = state
        .session_inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input(id, "DURING_RUN_SIBLING"),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    {
        let trace = provider.inner.trace.lock().unwrap();
        assert!(trace.failures.is_empty(), "{:?}", trace.failures);
        assert!(trace.requests.len() > 3 && trace.task_completed);
        assert_eq!(
            trace.contents[&("native-proof".into(), "SKILL.md".into())],
            source
        );
        for messages in &trace.requests {
            assert!(messages.iter().any(|m| m.id == selected["message_id"]));
            assert!(!messages.iter().any(|m| m.id == earlier["message_id"]
                || m.id == staged.id.as_str()
                || m.id == later.id.as_str()));
        }
    }
    let backlog = state.session_inbox.inspect(id).await.unwrap();
    assert_eq!(
        (
            backlog.pending,
            backlog.claimed,
            backlog.coordinator_generation
        ),
        (3, 0, 0)
    );
    let (status, replay, replay_headers) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        body.clone(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(replay, started);
    for name in [
        "Bamboo-Input-Id",
        "Bamboo-Ingress-Seq",
        "Bamboo-Session-Created-At",
        "Bamboo-Output-Scope",
    ] {
        assert_eq!(replay_headers.get(name), response_headers.get(name));
    }
    let calls = provider.inner.trace.lock().unwrap().requests.len();
    let (status, conflict, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({"skill_mode":"ask"}),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    let (status, consumed, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        body,
        &receipt_headers(&selected, "expired-or-new-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{consumed}");
    no_running(&state, id).await;
    assert_eq!(provider.inner.trace.lock().unwrap().requests.len(), calls);
}

#[actix_web::test]
async fn receipt_run_header_and_idempotency_validation_happen_before_admission() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-headers";
    seed(&state, id).await;
    let receipt = chat(&state, id, "pending input").await;
    let mut cases = vec![
        vec![(
            "Bamboo-Input-Id",
            receipt["message_id"].as_str().unwrap().into(),
        )],
        vec![("Bamboo-Ingress-Seq", "1".into())],
    ];
    let mut missing_key = receipt_headers(&receipt, "unused");
    missing_key.pop();
    cases.push(missing_key);
    let mut zero = receipt_headers(&receipt, "zero");
    zero[1].1 = "0".into();
    cases.push(zero);
    let mut bad_key = receipt_headers(&receipt, "bad");
    bad_key[3].1 = "contains space".into();
    cases.push(bad_key);
    for headers in cases {
        let (status, body, _) = request_headers(
            &state,
            &format!("/api/v1/execute/{id}"),
            json!({}),
            &headers,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    assert!(provider.trace.lock().unwrap().requests.is_empty());
    let backlog = state.session_inbox.inspect(id).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (1, 0));
    assert!(!state.agent_runners.read().await.contains_key(id));
}

#[actix_web::test]
async fn receipt_run_wrong_generation_staged_and_foreign_selector_never_consume_sibling() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-wrong";
    seed(&state, id).await;
    let staged = state
        .session_inbox
        .deliver(&SessionMessageEnvelope::user_input(id, "staged"))
        .await
        .unwrap();
    let input = chat(&state, id, "eligible sibling").await;
    let staged = json!({"message_id":staged.id.as_str(),"ingress_seq":staged.generation,"session_created_at":input["session_created_at"]});
    let wrong = json!({"message_id":input["message_id"],"ingress_seq":input["ingress_seq"].as_u64().unwrap()+1,"session_created_at":input["session_created_at"]});
    let foreign = json!({"message_id":"not-an-input","ingress_seq":input["ingress_seq"],"session_created_at":input["session_created_at"]});
    for (n, receipt) in [staged, wrong, foreign].into_iter().enumerate() {
        let (status, body, _) = request_headers(
            &state,
            &format!("/api/v1/execute/{id}"),
            json!({}),
            &receipt_headers(&receipt, &format!("wrong-{n}")),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        no_running(&state, id).await;
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(
            (
                backlog.pending,
                backlog.claimed,
                backlog.coordinator_generation
            ),
            (2, 0, 0)
        );
    }
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}

#[actix_web::test]
async fn receipt_run_startup_rejection_cleans_exact_owner_and_does_not_recreate_fresh_input() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-reject";
    seed(&state, id).await;
    let sibling = chat(&state, id, "unrelated pending").await;
    let input = chat(&state, id, "selected").await;
    let (status, body, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({"provider":"does-not-exist"}),
        &receipt_headers(&input, "startup-failure"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    no_running(&state, id).await;
    assert!(state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    let (status, body, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, "retry-consumed"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    no_running(&state, id).await;
    assert!(provider.trace.lock().unwrap().requests.is_empty());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
}

#[actix_web::test]
async fn receipt_run_exact_cancel_revokes_real_execution_and_delayed_cancel_cannot_hit_successor() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let provider = gated(home.path());
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-cancel";
    seed(&state, id).await;
    bindings()
        .lock()
        .unwrap()
        .insert(id.into(), std::sync::Weak::new());
    let input = chat(&state, id, "selected current input").await;
    let (status, started, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, "cancellable"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{started}");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let executor = bindings()
        .lock()
        .unwrap()
        .remove(id)
        .unwrap()
        .upgrade()
        .unwrap();
    assert_eq!(executor.run.reservation_id, started["run_id"]);
    let (status, _, _) = request_headers(
        &state,
        &format!("/api/v1/stop/{id}"),
        json!({}),
        &[("Bamboo-Run-Id", "wrong-run".into())],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(!executor.run.cancel.is_cancelled());
    let exact = vec![("Bamboo-Run-Id", started["run_id"].as_str().unwrap().into())];
    let legacy = state.cancel_tokens.write().await;
    let (status, body, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        request_headers(
            &state,
            &format!("/api/v1/sessions/{id}/stop"),
            json!({}),
            &exact,
        ),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(executor.run.cancel.is_cancelled());
    drop(legacy);
    assert!(matches!(done(&state, id).await, AgentStatus::Cancelled));
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while executor.run.live.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(executor.run.check_live().is_err());
    assert!(!provider.inner.trace.lock().unwrap().task_started);
    let mut successor = crate::app_state::AgentRunner::new();
    successor.status = AgentStatus::Running;
    let token = successor.cancel_token.clone();
    let successor_id = successor.run_id.clone();
    state
        .agent_runners
        .write()
        .await
        .insert(id.into(), successor);
    let (status, _, _) =
        request_headers(&state, &format!("/api/v1/stop/{id}"), json!({}), &exact).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(!token.is_cancelled());
    assert_eq!(state.agent_runners.read().await[id].run_id, successor_id);
}

#[derive(Default)]
struct RestartProbe {
    purposes: Mutex<Vec<String>>,
}
#[async_trait]
impl LLMProvider for RestartProbe {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.purposes.lock().unwrap().push("unattributed".into());
        Ok(Box::pin(futures::stream::iter([Ok(LLMChunk::Done)])))
    }
    async fn chat_stream_with_options(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
        options: Option<&bamboo_llm::LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let purpose = options
            .and_then(|o| o.request_purpose.clone())
            .unwrap_or_else(|| "unattributed".into());
        self.purposes.lock().unwrap().push(purpose);
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("synthetic auxiliary result".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn receipt_run_acked_input_after_real_restart_does_not_start_or_join_another_run() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let first_provider = provider(home.path(), 4096, false);
    let state_a = state(home.path(), first_provider.clone()).await;
    let id = "receipt-run-restart";
    seed(&state_a, id).await;
    let input = chat(&state_a, id, "selected").await;
    let (status, _, _) = request_headers(
        &state_a,
        &format!("/api/v1/execute/{id}"),
        json!({"provider":"does-not-exist"}),
        &receipt_headers(&input, "lost-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    no_running(&state_a, id).await;
    drop(state_a);
    let second_provider = Arc::new(RestartProbe::default());
    let state_b = state(home.path(), second_provider.clone()).await;
    let (status, body, _) = request_headers(
        &state_b,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, "lost-receipt"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    no_running(&state_b, id).await;
    let purposes = second_provider.purposes.lock().unwrap();
    assert!(
        purposes
            .iter()
            .all(|purpose| purpose != "agent_loop" && purpose != "unattributed"),
        "restart primary work: {purposes:?}"
    );
    eprintln!("actual restart auxiliary purposes: {purposes:?}");
}

#[actix_web::test]
async fn receipt_run_specific_wait_and_legacy_ingress_are_rejected_without_admission() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    for mode in ["wait", "legacy"] {
        let id = format!("receipt-run-excluded-{mode}");
        seed(&state, &id).await;
        let input = chat(&state, &id, "pending").await;
        let mut session = state.storage.load_session(&id).await.unwrap().unwrap();
        if mode == "wait" {
            let runtime = session
                .agent_runtime_state
                .get_or_insert_with(Default::default);
            runtime.waiting_for_children =
                Some(bamboo_domain::WaitingForChildrenState::for_children(
                    vec!["child".into()],
                    Default::default(),
                    chrono::Utc::now(),
                ));
        } else {
            session
                .metadata
                .insert("chat.queued_ingress.v1".into(), "legacy-input".into());
        }
        state.save_and_cache_session(&mut session).await;
        let (status, body, _) = request_headers(
            &state,
            &format!("/api/v1/execute/{id}"),
            json!({}),
            &receipt_headers(&input, &id),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(!state
            .session_inbox
            .was_admitted(&id, &durable_receipt(&input).id)
            .await
            .unwrap());
        assert!(!state.agent_runners.read().await.contains_key(&id));
    }
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}

#[actix_web::test]
async fn receipt_run_old_lifetime_cannot_select_recreated_roots_reused_id_and_generation() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = provider(home.path(), 4096, false);
    let state = state(home.path(), provider.clone()).await;
    let id = "receipt-run-recreated";
    seed(&state, id).await;
    let old = chat(&state, id, "old accepted input").await;
    state
        .session_store
        .delete_session_recursive(id, true)
        .await
        .unwrap();
    state
        .storage
        .recreate_root_session(id, "test-model")
        .await
        .unwrap();
    let mut input = SessionMessageEnvelope::user_input(id, "new lifetime input");
    input.id = durable_receipt(&old).id;
    let current = state
        .session_inbox
        .deliver_with_activation_intent(&input, SessionActivationPolicy::RespectSpecificWait, None)
        .await
        .unwrap();
    assert_eq!(current.generation, old["ingress_seq"].as_u64().unwrap());
    assert_ne!(
        state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        old["session_created_at"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    );
    let (status, body, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&old, "stale-birth"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(!state.agent_runners.read().await.contains_key(id));
    assert!(provider.trace.lock().unwrap().requests.is_empty());
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert!(!state
        .session_inbox
        .was_admitted(id, &current.id)
        .await
        .unwrap());
}
