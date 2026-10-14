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

async fn output(
    state: &web::Data<AppState>,
    id: &str,
    run: &str,
    birth: Option<&str>,
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
        .peer_addr("127.0.0.1:5700".parse().unwrap())
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
    (input, body["run_id"].as_str().unwrap().to_owned())
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
    // Existing Actor-mode Root Chat does not return its queued receipt. Use
    // the existing canonical accepted-Inbox port for this independent producer,
    // then exercise the same real selected HTTP activation and actual successor.
    let next = state
        .session_inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input(id, "OUTPUT_TWO"),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let next_input = json!({"message_id": next.id.as_str(), "ingress_seq": next.generation, "session_created_at": birth});
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
