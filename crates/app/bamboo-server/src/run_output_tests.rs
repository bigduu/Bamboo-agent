//! Actual selected HTTP runs and the exact-run output adapter.
use super::*;
use futures::StreamExt;

struct OutputProvider {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: AtomicU64,
    fail: bool,
}
impl OutputProvider {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            calls: AtomicU64::new(0),
            fail,
        })
    }
}
#[async_trait]
impl LLMProvider for OutputProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(|m| m.content.clone())
            .unwrap_or_default();
        let entered = self.entered.clone();
        let release = self.release.clone();
        let fail = self.fail;
        Ok(Box::pin(
            futures::stream::iter([
                Ok(LLMChunk::ReasoningToken(
                    "PRIVATE_REASONING_NOT_OUTPUT".into(),
                )),
                Ok(LLMChunk::Token(format!("visible answer for {text}"))),
            ])
            .chain(futures::stream::once(async move {
                entered.notify_one();
                release.notified().await;
                if fail {
                    Err(LLMError::Auth(
                        "synthetic nonretryable provider failure".into(),
                    ))
                } else {
                    Ok(LLMChunk::Done)
                }
            })),
        ))
    }
}

// Explicit execute can select only an input that a background consumer has
// not already admitted. Exercise that legitimate availability condition by
// injecting failure at the existing optional activation port; all HTTP input,
// real Root reservation, Inbox claim/ACK and output publication stay intact.
#[derive(Default)]
struct DeferredRootActivation {
    requests: std::sync::Mutex<Vec<(String, u64)>>,
}
#[async_trait]
impl bamboo_engine::session_activation::SessionActivationSpawner for DeferredRootActivation {
    async fn reserve_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> Result<
        bamboo_engine::session_activation::SessionActivationReserveOutcome,
        bamboo_domain::SessionActivationError,
    > {
        self.requests
            .lock()
            .unwrap()
            .push((target.to_owned(), generation));
        Err(bamboo_domain::SessionActivationError::Internal(
            "synthetic optional activation unavailable; explicit execute remains available".into(),
        ))
    }
}
async fn defer_root_activation(state: &web::Data<AppState>) -> Arc<DeferredRootActivation> {
    let deferred = Arc::new(DeferredRootActivation::default());
    state
        .session_activation_router
        .set_spawner(deferred.clone())
        .await;
    deferred
}

async fn output(
    state: &web::Data<AppState>,
    id: &str,
    run: &str,
    birth: Option<&str>,
) -> (StatusCode, Value) {
    output_from_peer(state, id, run, birth, "127.0.0.1:5700").await
}
async fn output_from_peer(
    state: &web::Data<AppState>,
    id: &str,
    run: &str,
    birth: Option<&str>,
    peer: &str,
) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .wrap(crate::config::build_cors("127.0.0.1", 9562))
            .configure(crate::configure_routes),
    )
    .await;
    let mut req = test::TestRequest::get()
        .uri(&format!("/api/v1/sessions/{id}/runs/{run}/output"))
        .peer_addr(peer.parse().unwrap())
        .insert_header(("Origin", "http://127.0.0.1:1420"));
    if let Some(birth) = birth {
        req = req.insert_header(("Bamboo-Session-Created-At", birth));
    }
    let response = test::call_service(&app, req.to_request()).await;
    let status = response.status();
    assert_eq!(response.headers().get("Cache-Control").unwrap(), "no-store");
    (status, test::read_body_json(response).await)
}
async fn start_output(
    state: &web::Data<AppState>,
    id: &str,
    key: &str,
    provider: &OutputProvider,
) -> (Value, String) {
    let input = chat(state, id, key).await;
    start_output_receipt(state, id, key, provider, input).await
}
async fn start_output_receipt(
    state: &web::Data<AppState>,
    id: &str,
    key: &str,
    provider: &OutputProvider,
    input: Value,
) -> (Value, String) {
    assert!(
        input["message_id"].is_string(),
        "actual accepted receipt required: {input}"
    );
    let (status, body, _) = request_headers(
        state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, key),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let run = body["run_id"].as_str().unwrap().to_owned();
    // Provider production precedes the asynchronous fenced forwarder. Wait
    // for real HTTP publication, rather than treating provider entry as ACK.
    let birth = input["session_created_at"].as_str().unwrap();
    let expected = format!("visible answer for {key}");
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let (status, published) = output(state, id, &run, Some(birth)).await;
            assert_eq!(status, StatusCode::OK, "{published}");
            if published["output"]["messages"]
                .as_array()
                .is_some_and(|messages| messages.iter().any(|m| m["content"] == expected))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("selected visible text was not published through the real HTTP output boundary");
    (input, run)
}
async fn committed_output(state: &web::Data<AppState>, id: &str, run: &str, birth: &str) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let (status, body) = output(state, id, run, Some(birth)).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            if body["output"]["history_committed"] == true {
                return body;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[actix_web::test]
async fn run_output_real_selected_execution_replays_after_history_and_never_follows_successor() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "run-output-real";
    seed(&state, id).await;
    let sibling = chat(&state, id, "SIBLING_MUST_STAY_PENDING").await;
    let (input, run) = start_output(&state, id, "OUTPUT_ONE", &provider).await;
    let birth = input["session_created_at"].as_str().unwrap();
    let (status, live) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::OK, "{live}");
    assert_eq!(live["input"]["message_id"], input["message_id"]);
    assert_eq!(live["input"]["ingress_seq"], input["ingress_seq"]);
    assert_eq!(live["run_id"], run);
    assert_eq!(live["output_scope"], "visible_messages");
    assert_eq!(live["output"]["history_committed"], false);
    assert!(live.to_string().contains("OUTPUT_ONE"));
    assert!(!live.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT"));
    assert!(!live.to_string().contains("SIBLING_MUST_STAY_PENDING"));
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let replay = committed_output(&state, id, &run, birth).await;
    assert_eq!(replay["output"]["terminal"], "complete");
    assert_eq!(replay["output"]["messages"], live["output"]["messages"]);
    let (status, again) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, replay);
    let (_, legacy) = state.agent_runners.read().await[id]
        .visible_messages
        .subscribe_with_snapshot();
    assert!(legacy.messages.is_empty());
    assert!(legacy.history_committed);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    let root = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(state
        .session_store
        .root_actor_input_required(&root)
        .await
        .unwrap());
    let deferred = defer_root_activation(&state).await;
    // The successor must use the actual Actor-mode Root Chat response;
    // direct Inbox acceptance cannot prove receipt propagation at this entry.
    let next_input = chat(&state, id, "OUTPUT_TWO").await;
    assert!(deferred
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|(target, generation)| target == id
            && Some(*generation) == next_input["ingress_seq"].as_u64()));
    let (next_input, next_run) =
        start_output_receipt(&state, id, "OUTPUT_TWO", &provider, next_input).await;
    assert_ne!(run, next_run);
    let (status, old) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::GONE, "{old}");
    assert!(!old.to_string().contains("OUTPUT_TWO"));
    let (status, next) = output(&state, id, &next_run, Some(birth)).await;
    assert_eq!(status, StatusCode::OK, "{next}");
    assert_eq!(next["input"]["message_id"], next_input["message_id"]);
    assert!(next.to_string().contains("OUTPUT_TWO"));
    assert!(!next.to_string().contains("OUTPUT_ONE"));
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let final_output = committed_output(&state, id, &next_run, birth).await;
    assert_eq!(final_output["output"]["terminal"], "complete");
    // Original runner reclamation removes replay; no durable/history fallback.
    state.agent_runners.write().await.remove(id);
    assert_eq!(
        output(&state, id, &next_run, Some(birth)).await.0,
        StatusCode::GONE
    );
    drop(state);
    let restarted = state_for_restart(home.path()).await;
    assert_eq!(
        output(&restarted, id, &next_run, Some(birth)).await.0,
        StatusCode::GONE
    );
}
async fn state_for_restart(home: &Path) -> web::Data<AppState> {
    state(home, Arc::new(RestartProbe::default())).await
}

#[actix_web::test]
async fn run_output_real_cancel_and_error_keep_exact_visible_prefix() {
    let _proof = HTTP_PROOF.lock().await;
    for fail in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let provider = OutputProvider::new(fail);
        let state = state(home.path(), provider.clone()).await;
        let id = if fail {
            "run-output-error"
        } else {
            "run-output-cancel"
        };
        seed(&state, id).await;
        let (input, run) = start_output(&state, id, "VISIBLE_PREFIX", &provider).await;
        let birth = input["session_created_at"].as_str().unwrap();
        if fail {
            provider.release.notify_one();
        } else {
            let (status, body, _) = request_headers(
                &state,
                &format!("/api/v1/sessions/{id}/stop"),
                json!({}),
                &[("Bamboo-Run-Id", run.clone())],
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert!(state.agent_runners.read().await[id]
                .cancel_token
                .is_cancelled());
            provider.release.notify_one();
        }
        let final_status = done(&state, id).await;
        assert!(
            if fail {
                matches!(final_status, AgentStatus::Error(_))
            } else {
                matches!(final_status, AgentStatus::Cancelled)
            },
            "{final_status:?}"
        );
        let body = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let (status, body) = output(&state, id, &run, Some(birth)).await;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(
                    body["runner_status"],
                    if fail { "error" } else { "cancelled" }
                );
                // Preserve Native's existing Error terminal for cancellation;
                // the exact existing runner status disambiguates it.
                if body["output"]["terminal"] == "error" {
                    break body;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(body.to_string().contains("VISIBLE_PREFIX"));
        assert!(!body.to_string().contains("PRIVATE_REASONING_NOT_OUTPUT"));
        assert_eq!(body["run_id"], run);
    }
}

#[actix_web::test]
async fn run_output_headers_nonselected_run_and_recreated_root_fail_closed() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "run-output-lifetime";
    seed(&state, id).await;
    let birth = state
        .storage
        .load_session(id)
        .await
        .unwrap()
        .unwrap()
        .created_at
        .to_rfc3339();
    assert_eq!(
        output(&state, id, "unknown", None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        output(&state, id, "unknown", Some("bad")).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        output(&state, id, "unknown", Some(&birth)).await.0,
        StatusCode::GONE
    );
    let mut normal = crate::AgentRunner::new();
    normal
        .visible_messages
        .append("UNATTRIBUTED_SESSION_TEXT".into());
    let normal_id = normal.run_id.clone();
    normal.status = AgentStatus::Completed;
    state.agent_runners.write().await.insert(id.into(), normal);
    let (status, denied) = output(&state, id, &normal_id, Some(&birth)).await;
    assert_eq!(status, StatusCode::GONE);
    assert!(!denied.to_string().contains("UNATTRIBUTED_SESSION_TEXT"));
    let (input, run) = start_output(&state, id, "OLD_ROOT_OUTPUT", &provider).await;
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = input["session_created_at"].as_str().unwrap();
    committed_output(&state, id, &run, birth).await;
    // Deliberately retain stale runner bytes while the real trusted storage
    // port deletes/recreates the Root, as a stronger read-side ABA regression.
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
    let new_birth = state
        .storage
        .load_session(id)
        .await
        .unwrap()
        .unwrap()
        .created_at
        .to_rfc3339();
    assert_ne!(birth, new_birth);
    let (status, old) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(!old.to_string().contains("OLD_ROOT_OUTPUT"));
    let (status, new) = output(&state, id, &run, Some(&new_birth)).await;
    assert_eq!(status, StatusCode::GONE);
    assert!(!new.to_string().contains("OLD_ROOT_OUTPUT"));
}

#[actix_web::test]
async fn run_output_cancelled_before_binding_never_starts_title_or_primary() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = Arc::new(RestartProbe::default());
    let state = state(home.path(), provider.clone()).await;
    {
        let mut config = state.config.write().await;
        config.features.provider_model_ref = false;
        config.providers_mut().openai.as_mut().unwrap().fast_model = Some("test-model".into());
        assert!(bamboo_engine::model_config_helper::resolve_fast_model(
            &config,
            "openai",
            &state.provider_registry
        )
        .is_some());
    }
    let id = "run-output-aborted-before-title";
    // Unlike other fixtures this deliberately leaves title generation pending.
    state
        .storage
        .save_session(&Session::new(id, "test-model"))
        .await
        .unwrap();
    bindings()
        .lock()
        .unwrap()
        .insert(id.into(), std::sync::Weak::new());
    let input = chat(&state, id, "cancel this startup before output binding").await;
    let cancel_at_real_binding = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let executor = {
                    bindings()
                        .lock()
                        .unwrap()
                        .get(id)
                        .and_then(std::sync::Weak::upgrade)
                };
                if let Some(executor) = executor {
                    executor.run.cancel.cancel();
                    return executor;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    });
    let (status, body, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, "cancel-before-output"),
    )
    .await;
    let executor = cancel_at_real_binding.await.unwrap();
    assert!(executor.run.cancel.is_cancelled());
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if !provider.purposes.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let purposes = provider.purposes.lock().unwrap().clone();
    eprintln!("aborted Ready provider purposes: {purposes:?}");
    assert!(
        purposes.is_empty(),
        "aborted Ready started provider work: {purposes:?}"
    );
    assert!(state
        .agent_runners
        .read()
        .await
        .get(id)
        .is_none_or(|runner| runner.visible_messages.selected_output_snapshot().is_none()));
}

#[actix_web::test]
async fn run_output_waits_for_root_recreation_and_rechecks_supervisor_owner() {
    let _proof = HTTP_PROOF.lock().await;
    for supervisor in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let provider = OutputProvider::new(false);
        let state = state(home.path(), provider.clone()).await;
        let id = if supervisor {
            bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID
        } else {
            "run-output-concurrent-recreate"
        };
        seed(&state, id).await;
        let (input, run) = start_output(&state, id, "CONCURRENT_ROOT_OUTPUT", &provider).await;
        let birth = input["session_created_at"].as_str().unwrap();
        provider.release.notify_one();
        assert!(matches!(done(&state, id).await, AgentStatus::Completed));
        committed_output(&state, id, &run, birth).await;

        // Hold the same Host lock as delete/create while deliberately leaving
        // the old exact-run buffer alive: the read must serialize, then
        // validate the replacement's actual birth and owner authority.
        let guard = state.persistence.acquire_lock(id).await;
        let peer = if supervisor {
            "203.0.113.17:5700"
        } else {
            "127.0.0.1:5700"
        };
        let mut read = Box::pin(output_from_peer(&state, id, &run, Some(birth), peer));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut read)
                .await
                .is_err(),
            "output read escaped the canonical Root persistence lock"
        );
        state
            .session_store
            .delete_session_recursive(id, true)
            .await
            .unwrap();
        if supervisor {
            state
                .storage
                .get_or_create_default_supervisor("test-model")
                .await
                .unwrap();
        } else {
            state
                .storage
                .recreate_root_session(id, "test-model")
                .await
                .unwrap();
        }
        let replacement = state
            .storage
            .load_root_authority(id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement.created_at.to_rfc3339(), birth);
        assert_eq!(replacement.authority_identity.is_ordinary(), !supervisor);
        drop(guard);
        let (status, body) = tokio::time::timeout(std::time::Duration::from_secs(15), read)
            .await
            .unwrap();
        assert_eq!(
            status,
            if supervisor {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::CONFLICT
            },
            "{body}"
        );
        assert!(!body.to_string().contains("CONCURRENT_ROOT_OUTPUT"));
        eprintln!("serialized Root replacement supervisor={supervisor}: {status}");
    }
}

#[actix_web::test]
async fn root_chat_receipt_actual_http_retry_conflict_selected_cancel_and_sibling_isolation() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "root-chat-receipt-real";
    seed(&state, id).await;
    let (bootstrap, _) = start_output(&state, id, "ROOT_BOOTSTRAP", &provider).await;
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let root = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(state
        .session_store
        .root_actor_input_required(&root)
        .await
        .unwrap());
    let birth = bootstrap["session_created_at"].as_str().unwrap();
    let deferred = defer_root_activation(&state).await;
    let sibling = chat(&state, id, "ROOT_CHAT_SIBLING").await;
    assert!(sibling["message_id"].is_string(), "{sibling}");
    assert!(sibling["ingress_seq"].as_u64().unwrap() > 0);

    let chat_body = json!({"session_id":id,"message":"ROOT_CHAT_SELECTED"});
    let chat_headers = [("Idempotency-Key", "root-chat-returned-receipt".to_owned())];
    let (status, raw, _) =
        request_headers(&state, "/api/v1/chat", chat_body.clone(), &chat_headers).await;
    assert_eq!(status, StatusCode::CREATED, "{raw}");
    assert!(
        raw["message_id"].is_string(),
        "real Root Chat must return its receipt: {raw}"
    );
    assert!(raw["ingress_seq"].as_u64().unwrap() > sibling["ingress_seq"].as_u64().unwrap());
    assert_ne!(raw["message_id"], sibling["message_id"]);
    assert!(deferred.requests.lock().unwrap().iter().any(
        |(target, generation)| target == id && Some(*generation) == raw["ingress_seq"].as_u64()
    ));
    let (status, retry, _) =
        request_headers(&state, "/api/v1/chat", chat_body.clone(), &chat_headers).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(retry, raw);
    let (status, _, _) = request_headers(
        &state,
        "/api/v1/chat",
        json!({"session_id":id,"message":"CONFLICT_MUST_NOT_QUEUE"}),
        &chat_headers,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 2);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let mut selected = raw.clone();
    selected["session_created_at"] = json!(birth);
    let (selected, run) =
        start_output_receipt(&state, id, "ROOT_CHAT_SELECTED", &provider, selected).await;
    let (status, visible) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::OK, "{visible}");
    assert_eq!(visible["input"]["message_id"], raw["message_id"]);
    assert_eq!(visible["input"]["ingress_seq"], raw["ingress_seq"]);
    assert!(visible.to_string().contains("ROOT_CHAT_SELECTED"));
    assert!(!visible.to_string().contains("ROOT_CHAT_SIBLING"));
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
    let (status, stopped, _) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/stop"),
        json!({}),
        &[("Bamboo-Run-Id", run.clone())],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{stopped}");
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Cancelled));
    let (status, cancelled) = output(&state, id, &run, Some(birth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["runner_status"], "cancelled");
    assert_eq!(cancelled["input"], visible["input"]);
    assert_eq!(
        cancelled["output"]["messages"],
        visible["output"]["messages"]
    );
    let (status, replay, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&selected, "ROOT_CHAT_SELECTED"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(replay["run_id"], run);
    let (status, retry, _) =
        request_headers(&state, "/api/v1/chat", chat_body, &chat_headers).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(retry, raw);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    assert!(!state
        .session_inbox
        .was_admitted(id, &durable_receipt(&sibling).id)
        .await
        .unwrap());
}

#[actix_web::test]
async fn root_chat_receipt_real_coordinator_can_consume_before_execute_without_owner_adoption() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let provider = OutputProvider::new(false);
    let state = state(home.path(), provider.clone()).await;
    let id = "root-chat-real-activation-race";
    seed(&state, id).await;
    let (bootstrap, bootstrap_run) = start_output(&state, id, "AUTO_BOOTSTRAP", &provider).await;
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Completed));
    let birth = bootstrap["session_created_at"].as_str().unwrap();
    committed_output(&state, id, &bootstrap_run, birth).await;
    let root = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(state
        .session_store
        .root_actor_input_required(&root)
        .await
        .unwrap());
    // Keep the actual AppState coordinator here; no scheduling port is replaced.
    let body = json!({"session_id":id,"message":"ROOT_CHAT_AUTO_CONSUMED"});
    let headers = [("Idempotency-Key", "root-chat-real-coordinator".to_owned())];
    let (status, raw, _) = request_headers(&state, "/api/v1/chat", body.clone(), &headers).await;
    assert_eq!(status, StatusCode::CREATED, "{raw}");
    assert!(raw["message_id"].is_string(), "{raw}");
    assert!(raw["ingress_seq"].as_u64().unwrap() > 0);
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let mut input = raw.clone();
    input["session_created_at"] = json!(birth);
    assert!(state
        .session_inbox
        .was_admitted(id, &durable_receipt(&input).id)
        .await
        .unwrap());
    let actual_run = state.agent_runners.read().await[id].run_id.clone();
    assert_ne!(actual_run, bootstrap_run);
    let (status, retry, _) = request_headers(&state, "/api/v1/chat", body, &headers).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(retry, raw);
    let (status, rejected, _) = request_headers(
        &state,
        &format!("/api/v1/execute/{id}"),
        json!({}),
        &receipt_headers(&input, "never-adopt-auto-owner"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(state.agent_runners.read().await[id].run_id, actual_run);
    assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    // A returned input receipt cannot opt the background run into selected output.
    assert_eq!(
        output(&state, id, &actual_run, Some(birth)).await.0,
        StatusCode::GONE
    );
    let (status, stopped, _) = request_headers(
        &state,
        &format!("/api/v1/sessions/{id}/stop"),
        json!({}),
        &[("Bamboo-Run-Id", actual_run.clone())],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{stopped}");
    assert!(state.agent_runners.read().await[id]
        .cancel_token
        .is_cancelled());
    provider.release.notify_one();
    let terminal = done(&state, id).await;
    assert!(!matches!(
        terminal,
        AgentStatus::Pending | AgentStatus::Running
    ));
    assert_eq!(state.agent_runners.read().await[id].run_id, actual_run);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    eprintln!("actual coordinator accepted receipt{}, explicit execute409, background selected-output410, terminal={terminal:?}",raw["message_id"]);
}
