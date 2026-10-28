// Real HTTP/Host/Root checkpoint + actual Anthropic loopback producer.
// Only the first model response and the tool are synthetic; no real provider is contacted.
struct VerticalCountingTool(std::sync::atomic::AtomicUsize);

#[async_trait]
impl bamboo_agent_core::tools::Tool for VerticalCountingTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "synthetic read"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }

    fn classify(&self, _: &serde_json::Value) -> bamboo_agent_core::tools::ToolClass {
        bamboo_agent_core::tools::ToolClass::READONLY_PARALLEL
    }

    async fn invoke(
        &self,
        _: serde_json::Value,
        ctx: bamboo_agent_core::tools::ToolCtx,
    ) -> Result<bamboo_agent_core::tools::ToolOutcome, bamboo_agent_core::tools::ToolError> {
        assert_eq!(ctx.tool_call_id.as_ref(), "vertical-tool-call");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(bamboo_agent_core::tools::ToolOutcome::Completed(
            bamboo_agent_core::tools::ToolResult::text(true, "synthetic tool result"),
        ))
    }
}

struct VerticalToolThenAnthropic {
    calls: std::sync::atomic::AtomicUsize,
    anthropic: bamboo_llm::providers::anthropic::AnthropicProvider,
}

#[async_trait]
impl LLMProvider for VerticalToolThenAnthropic {
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[bamboo_agent_core::tools::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("the real structured request path must be used")
    }

    async fn chat_stream_ir(
        &self,
        ir: &bamboo_llm::prompt_ir::PromptIR,
        tools: &[bamboo_agent_core::tools::ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&bamboo_llm::provider::LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::ToolCalls(vec![
                    bamboo_agent_core::tools::ToolCall {
                        id: "vertical-tool-call".into(),
                        tool_type: "function".into(),
                        function: bamboo_agent_core::tools::FunctionCall {
                            name: "Read".into(),
                            arguments: "{}".into(),
                        },
                    },
                ])),
                Ok(LLMChunk::Done),
            ]))),
            1 => {
                assert!(ir
                    .body_chat()
                    .iter()
                    .any(|message| message.tool_call_id.as_deref() == Some("vertical-tool-call")));
                self.anthropic
                    .chat_stream_ir(ir, tools, max, model, options)
                    .await
            }
            count => panic!("a resumed provider future was rebuilt: {count}"),
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum LiveVerticalCase {
    Continue,
    ConcurrentConfirm,
    StopWaiting,
    StopAfterContinue,
    StopDuringConfirm,
    GateCancelledBeforeResult,
    EarlyKeyBeforeOffer,
    RejectConfirmation,
    HeadersTimeout,
}

#[cfg(unix)]
async fn run_live_retry_vertical(case: LiveVerticalCase) {
    use std::time::Duration;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let continue_allowed = Arc::new(AtomicBool::new(false));
    let ready = continue_allowed.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            if !ready.load(Ordering::SeqCst) {
                if case == LiveVerticalCase::HeadersTimeout {
                    // The driver sets a 100 ms attempt timeout. This delay is
                    // before any response headers from the synthetic upstream.
                    ResponseTemplate::new(200).set_delay(Duration::from_millis(500))
                } else {
                    ResponseTemplate::new(503).insert_header(
                        "Retry-After",
                        match case {
                            LiveVerticalCase::StopAfterContinue => "600",
                            LiveVerticalCase::ConcurrentConfirm => "600",
                            _ => "0",
                        },
                    )
                }
            } else {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(concat!(
                        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"fixture\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
                        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"vertical success\"}}\n\n",
                        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
                    ))
            }
        })
        .mount(&server)
        .await;
    let provider = Arc::new(VerticalToolThenAnthropic {
        calls: std::sync::atomic::AtomicUsize::new(0),
        anthropic: bamboo_llm::providers::anthropic::AnthropicProvider::new("fixture-key")
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .with_base_url(server.uri()),
    });
    let tools = Arc::new(VerticalCountingTool(std::sync::atomic::AtomicUsize::new(0)));
    let home = tempfile::tempdir().unwrap();
    let hook_file = home.path().join("vertical-session-start");
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
    config.provider = "anthropic".into();
    config.providers_mut().anthropic = Some(bamboo_config::AnthropicConfig {
        api_key: "fixture-key".into(),
        model: Some("test-model".into()),
        base_url: Some(server.uri()),
        ..Default::default()
    });
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
        enabled: true,
        session_start: vec![bamboo_config::LifecycleHookGroup {
            enabled: true,
            matcher: None,
            hooks: vec![bamboo_config::LifecycleHookHandler::command(
                format!("printf s >> '{}'", hook_file.display()),
                bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
            )],
        }],
        ..Default::default()
    };
    let mut state =
        crate::AppState::new_with_provider(home.path().into(), config, provider.clone())
            .await
            .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        std::collections::HashMap::from([(
            "anthropic".into(),
            provider.clone() as Arc<dyn LLMProvider>,
        )]),
        "anthropic".into(),
    ));
    state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
    // Retain the genuine Root tool/Skill surface, replacing only this one
    // read tool with the counted synthetic implementation.
    let root = Arc::new(crate::tools::OverlayToolExecutor::new(
        state.tools_for(crate::tools::ToolSurface::Root),
        tools.clone(),
    ));
    state
        .child_completion_coordinator
        .set_root_tools(root.clone())
        .await;
    state.tool_factory = crate::tools::ToolSurfaceFactory::new(
        state.tools_for(crate::tools::ToolSurface::Base),
        state.tools_for(crate::tools::ToolSurface::WithTask),
        root,
    );
    let state = web::Data::new(state);
    let registration_gate = (case == LiveVerticalCase::EarlyKeyBeforeOffer)
        .then(|| state.live_retry.pause_registration_for_test());
    state
        .session_activation_router
        .set_spawner(Arc::new(ManualHttpOnly))
        .await;
    let id = "live-retry-vertical-http";
    state
        .storage
        .save_session(&owned_http_session(&state, id))
        .await
        .unwrap();
    queue_owned_root(&state, id).await;
    let app = test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let mut events = state.get_session_event_sender(id).await.subscribe();
    if case == LiveVerticalCase::HeadersTimeout {
        assert_eq!(
            bamboo_llm::retry::initial_request_limits().attempt_timeout,
            Duration::from_millis(100),
            "the timeout case must run with its controlled attempt limit"
        );
    }
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(
                serde_json::json!({"model":"test-model", "live_bootstrap_resume":true,
            "run_budget":{"max_tool_calls":10,"max_rounds":10}}),
            )
            .to_request(),
    )
    .await;
    let first_status = response.status();
    let first: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        first_status,
        StatusCode::ACCEPTED,
        "initial execute: {first}"
    );
    let run_id = first["run_id"].as_str().unwrap().to_owned();
    if let Some(gate) = &registration_gate {
        tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
            .await
            .unwrap();
        let early = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .insert_header(("Idempotency-Key", "early-continuation-key"))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        assert_eq!(early.status(), StatusCode::CONFLICT);
        let early: serde_json::Value = test::read_body_json(early).await;
        assert_eq!(early["error"]["code"], "retry_resume_outcome_uncertain");
        gate.release.notify_one();
    }
    let waiting = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/api/v1/execute/{id}/retry"))
                    .to_request(),
            )
            .await;
            let body: serde_json::Value = test::read_body_json(response).await;
            if body["status"] == "retry_waiting" {
                break Some(body);
            }
            if state
                .agent_runners
                .read()
                .await
                .get(id)
                .is_some_and(|runner| {
                    matches!(
                        runner.status,
                        AgentStatus::Completed | AgentStatus::Error(_) | AgentStatus::Cancelled
                    )
                })
            {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(None);
    let before = state.storage.load_session(id).await.unwrap().unwrap();
    let before_http = server.received_requests().await.unwrap();
    let before_provider = provider.calls.load(Ordering::SeqCst);
    let before_tool = tools.0.load(Ordering::SeqCst);
    let before_hook = std::fs::read_to_string(&hook_file).unwrap_or_default();
    let before_user = before
        .messages
        .iter()
        .filter(|message| message.content == "one owned Root input")
        .count();
    let before_result = before
        .messages
        .iter()
        .filter(|message| message.tool_call_id.as_deref() == Some("vertical-tool-call"))
        .count();
    let before_budget = before
        .agent_runtime_state
        .as_ref()
        .map(|runtime| runtime.round.total_tool_calls);
    eprintln!("vertical before: waiting={}, provider={before_provider}, tool={before_tool}, hook={before_hook:?}, user={before_user}, result={before_result}, budget={before_budget:?}, http={}, last_status={:?}", waiting.is_some(), before_http.len(), before.last_run_status());
    if waiting.is_none() {
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        panic!("real Host did not expose a checkpointed live offer");
    }
    let waiting = waiting.unwrap();
    assert_eq!(waiting["run_id"], run_id);
    let actor = bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
        .await
        .unwrap();
    let activation = actor.activation.expect("waiting Root has a real actor");
    assert_eq!(activation.run_id, run_id);
    assert_eq!(
        activation.status,
        bamboo_domain::ActorActivationStatus::Running
    );
    assert_eq!(
        (before_provider, before_tool, before_user, before_result),
        (2, 1, 1, 1)
    );
    assert_eq!(before_hook, "s");
    assert_eq!(before_budget, Some(1));
    assert_eq!(
        before_http.len(),
        1,
        "exactly one initial request before decision"
    );

    if case == LiveVerticalCase::GateCancelledBeforeResult {
        let gate = state.live_retry.pause_confirmations_for_test();
        let request = || {
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()
        };
        let winner = test::call_service(&app, request());
        let verify = async {
            tokio::time::timeout(Duration::from_secs(5), gate.claimed.notified())
                .await
                .unwrap();
            assert!(state.live_retry.cancel_claimed_gate_for_test(id, &run_id));
            let follower = test::call_service(&app, request()).await;
            assert_eq!(follower.status(), StatusCode::CONFLICT);
            let body: serde_json::Value = test::read_body_json(follower).await;
            assert_eq!(body["error"]["code"], "retry_resume_outcome_uncertain");
            assert_eq!(body["run_id"], run_id);
            let stop = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/stop/{id}"))
                    .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(stop.status(), StatusCode::OK);
        };
        let (winner, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            futures::join!(winner, verify)
        })
        .await
        .expect("cancelled gate must not strand confirmation");
        assert_eq!(winner.status(), StatusCode::CONFLICT);
        let body: serde_json::Value = test::read_body_json(winner).await;
        assert_eq!(body["error"]["code"], "retry_resume_outcome_uncertain");
        assert_eq!(state.live_retry.active_payloads_for_test(), 0);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        return;
    }

    if case == LiveVerticalCase::StopDuringConfirm {
        let gate = state.live_retry.pause_confirmations_for_test();
        let request = || {
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()
        };
        let requests = async {
            futures::join!(
                test::call_service(&app, request()),
                test::call_service(&app, request())
            )
        };
        let stop = async {
            tokio::time::timeout(Duration::from_secs(5), gate.claimed.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), gate.follower.notified())
                .await
                .unwrap();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/stop/{id}"))
                    .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
        };
        let ((first, second), ()) = tokio::time::timeout(Duration::from_secs(10), async {
            futures::join!(requests, stop)
        })
        .await
        .expect("Stop must release both confirmation requests");
        for response in [first, second] {
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body: serde_json::Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], "retry_resume_outcome_uncertain");
            assert_eq!(body["run_id"], run_id);
        }
        assert_eq!(state.live_retry.active_payloads_for_test(), 0);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        return;
    }
    if case == LiveVerticalCase::RejectConfirmation {
        state.live_retry.reject_next_callback_for_test();
        let confirmation = tokio::time::timeout(
            Duration::from_secs(5),
            test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/execute/{id}"))
                    .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                    .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                    .to_request(),
            ),
        )
        .await
        .expect("a refused gate callback must not deadlock");
        assert_eq!(confirmation.status(), StatusCode::CONFLICT);
        let body: serde_json::Value = test::read_body_json(confirmation).await;
        assert_eq!(body["error"]["code"], "retry_resume_not_prepared");
        assert_eq!(state.live_retry.active_payloads_for_test(), 0);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        return;
    }
    if case == LiveVerticalCase::StopAfterContinue {
        let confirmation = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        assert_eq!(confirmation.status(), StatusCode::OK);
    }
    if matches!(
        case,
        LiveVerticalCase::StopWaiting | LiveVerticalCase::StopAfterContinue
    ) {
        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/stop/{id}"))
                .insert_header(("Bamboo-Run-Id", "earlier-run"))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(!state
            .agent_runners
            .read()
            .await
            .get(id)
            .unwrap()
            .cancel_token
            .is_cancelled());
        let stop = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/stop/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(stop.status(), StatusCode::OK);
        let stopped: serde_json::Value = test::read_body_json(stop).await;
        assert_eq!(stopped["run_id"], run_id);
        wait_for_owned_terminal_with_cancellation(&state, id, &run_id, &mut events, true).await;
        let diagnostics = bamboo_llm::retry::recent_initial_request_diagnostics(id);
        let cancelled = diagnostics
            .iter()
            .rev()
            .map(|value| serde_json::to_value(value).unwrap())
            .find(|value| value["stop_reason"] == "cancelled_or_dropped")
            .expect("cancelled request diagnostic");
        if case == LiveVerticalCase::StopAfterContinue {
            assert_eq!(cancelled["retry_mode"], "until_cancelled");
            assert!(cancelled["attempt_limit"].is_null());
            assert!(cancelled["total_budget_ms"].is_null());
            assert!(cancelled["remaining_budget_ms"].is_null());
        } else {
            assert!(cancelled["retry_mode"].is_null());
            assert!(cancelled["attempt_limit"].is_number());
        }
        let waiting_after = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/execute/{id}/retry"))
                .to_request(),
        )
        .await;
        let waiting_after: serde_json::Value = test::read_body_json(waiting_after).await;
        assert_eq!(waiting_after["status"], "unavailable");
        let late = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        assert_eq!(late.status(), StatusCode::CONFLICT);
        let late: serde_json::Value = test::read_body_json(late).await;
        let expected = if case == LiveVerticalCase::StopAfterContinue {
            "retry_resume_outcome_uncertain"
        } else {
            "retry_resume_not_prepared"
        };
        assert_eq!(late["error"]["code"], expected);
        assert_eq!(state.live_retry.active_payloads_for_test(), 0);
        // Once the lightweight receipt is cleaned up, no history is inferred.
        state.live_retry.clear_receipt_for_test(id, &run_id);
        let after_cleanup = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        assert_eq!(after_cleanup.status(), StatusCode::CONFLICT);
        let after_cleanup: serde_json::Value = test::read_body_json(after_cleanup).await;
        assert_eq!(
            after_cleanup["error"]["code"],
            "retry_resume_outcome_uncertain"
        );
        assert_eq!(after_cleanup["run_id"], run_id);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), before_provider);
        assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
        assert_eq!(
            std::fs::read_to_string(&hook_file).unwrap_or_default(),
            before_hook
        );
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.content == "one owned Root input")
                .count(),
            1
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("vertical-tool-call"))
                .count(),
            1
        );
        assert_eq!(
            after
                .agent_runtime_state
                .as_ref()
                .map(|r| r.round.total_tool_calls),
            before_budget
        );
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        return;
    }

    if case == LiveVerticalCase::ConcurrentConfirm {
        // Exercise the actual trait object, not an inherent-method call. Its
        // proof has a different gate from the real waiting producer offer.
        let owner = bamboo_domain::RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: before.created_at,
            conversation_generation: before.conversation_generation_observation(),
        };
        let bound = state
            .persistence
            .bind_root_actor_writer(owner, Arc::new(|_| {}))
            .unwrap();
        let port: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = Arc::new(bound);
        let proof = port
            .checkpoint_live_bootstrap(
                &before,
                state.session_inbox.clone(),
                bamboo_domain::AdmissionGate::default(),
            )
            .await
            .expect("bound runtime trait forwards checkpoint");
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback = |count: Arc<std::sync::atomic::AtomicUsize>| -> bamboo_domain::RootActorRuntimeEventPublisher {
            Box::new(move |check| {
                check()?;
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        port.confirm_live_bootstrap(&proof, state.session_inbox.clone(), callback(count.clone()))
            .await
            .expect("bound runtime trait forwards confirmation");
        assert!(port
            .confirm_live_bootstrap(&proof, state.session_inbox.clone(), callback(count.clone()))
            .await
            .is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let unbound: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = state.persistence.clone();
        assert!(unbound
            .checkpoint_live_bootstrap(
                &before,
                state.session_inbox.clone(),
                bamboo_domain::AdmissionGate::default(),
            )
            .await
            .is_err());
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
    }

    if case == LiveVerticalCase::EarlyKeyBeforeOffer {
        let replay = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .insert_header(("Idempotency-Key", "early-continuation-key"))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        let replay: serde_json::Value = test::read_body_json(replay).await;
        assert_eq!(replay["error"]["code"], "retry_resume_outcome_uncertain");
        assert!(state
            .live_retry
            .waiting(&state.agent_runners, id)
            .await
            .is_some());
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
    }
    continue_allowed.store(true, Ordering::SeqCst);
    let request = || {
        let request = test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .insert_header(("Bamboo-Run-Id", run_id.as_str()));
        let request = if case == LiveVerticalCase::EarlyKeyBeforeOffer {
            request.insert_header(("Idempotency-Key", "new-explicit-confirmation"))
        } else {
            request
        };
        request
            .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()
    };
    let response = if case == LiveVerticalCase::ConcurrentConfirm {
        let gate = state.live_retry.pause_confirmations_for_test();
        let requests = async {
            futures::join!(
                test::call_service(&app, request()),
                test::call_service(&app, request())
            )
        };
        let release = async {
            tokio::time::timeout(Duration::from_secs(10), gate.claimed.notified())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), gate.follower.notified())
                .await
                .unwrap();
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                before_http.len()
            );
            gate.release.notify_one();
        };
        let ((first, second), ()) = futures::join!(requests, release);
        assert_eq!(
            first.status(),
            StatusCode::OK,
            "first client must receive the committed continuation"
        );
        assert_eq!(
            second.status(),
            StatusCode::OK,
            "second client must recognize the same committed continuation"
        );
        let repeated: serde_json::Value = test::read_body_json(second).await;
        assert_eq!(repeated["status"], "retry_continuing");
        assert_eq!(repeated["run_id"], run_id);
        assert_eq!(state.live_retry.active_payloads_for_test(), 0);
        let duplicate = test::call_service(&app, request()).await;
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate: serde_json::Value = test::read_body_json(duplicate).await;
        assert_eq!(duplicate["run_id"], run_id);
        assert_eq!(duplicate["status"], "retry_continuing");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        let first: serde_json::Value = test::read_body_json(first).await;
        assert_eq!(first["run_id"], run_id);
        assert_eq!(first["status"], "retry_continuing");
        let keyed_request = |run: &str| {
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .insert_header(("Bamboo-Run-Id", run))
                .insert_header(("Idempotency-Key", "live-retry-confirm-key"))
                .set_json(serde_json::json!({"upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()
        };
        let keyed = test::call_service(&app, keyed_request(&run_id)).await;
        assert_eq!(keyed.status(), StatusCode::OK);
        let stop = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/stop/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(stop.status(), StatusCode::OK);
        wait_for_owned_terminal_with_cancellation(&state, id, &run_id, &mut events, true).await;
        let replay = test::call_service(&app, keyed_request(&run_id)).await;
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        let replay: serde_json::Value = test::read_body_json(replay).await;
        assert_eq!(replay["error"]["code"], "retry_resume_outcome_uncertain");
        assert_eq!(replay["run_id"], run_id);
        let mismatch = test::call_service(&app, keyed_request("different-run")).await;
        assert_eq!(mismatch.status(), StatusCode::CONFLICT);
        let mismatch: serde_json::Value = test::read_body_json(mismatch).await;
        assert_eq!(mismatch["error"]["code"], "idempotency_key_conflict");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before_http.len()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), before_provider);
        assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.content == "one owned Root input")
                .count(),
            1
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("vertical-tool-call"))
                .count(),
            1
        );
        tokio::time::timeout(Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        return;
    } else {
        test::call_service(&app, request()).await
    };
    let status = response.status();
    let continued: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(status, StatusCode::OK, "explicit continue: {continued}");
    assert_eq!(continued["status"], "retry_continuing");
    assert_eq!(continued["run_id"], run_id);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if state
                .agent_runners
                .read()
                .await
                .get(id)
                .is_some_and(|runner| {
                    runner.run_id == run_id && matches!(runner.status, AgentStatus::Completed)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("same run must complete");
    let after = state.storage.load_session(id).await.unwrap().unwrap();
    let after_http = server.received_requests().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), before_provider);
    assert_eq!(tools.0.load(Ordering::SeqCst), before_tool);
    assert_eq!(
        after
            .agent_runtime_state
            .as_ref()
            .map(|runtime| runtime.round.total_tool_calls),
        before_budget
    );
    assert_eq!(
        std::fs::read_to_string(&hook_file).unwrap_or_default(),
        before_hook
    );
    assert_eq!(
        after
            .messages
            .iter()
            .filter(|message| message.content == "one owned Root input")
            .count(),
        1
    );
    assert_eq!(
        after
            .messages
            .iter()
            .filter(|message| message.tool_call_id.as_deref() == Some("vertical-tool-call"))
            .count(),
        1
    );
    assert!(after
        .messages
        .iter()
        .any(|message| message.content.contains("vertical success")));
    assert_eq!(after_http.len(), before_http.len() + 1);
    assert!(after_http
        .iter()
        .all(|request| request.body == before_http[0].body));
    tokio::time::timeout(Duration::from_secs(15), state.shutdown())
        .await
        .unwrap();
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_real_actor_tool_then_anthropic_once() {
    run_live_retry_vertical(LiveVerticalCase::Continue).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_concurrent_confirm_and_trait_port_are_one_shot() {
    run_live_retry_vertical(LiveVerticalCase::ConcurrentConfirm).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_stop_after_confirmation_reports_continuous_mode() {
    run_live_retry_vertical(LiveVerticalCase::StopAfterContinue).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_rejected_callback_cannot_deadlock() {
    run_live_retry_vertical(LiveVerticalCase::RejectConfirmation).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_early_idempotency_key_cannot_consume_later_offer() {
    run_live_retry_vertical(LiveVerticalCase::EarlyKeyBeforeOffer).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_cancelled_claim_is_uncertain_before_result_publication() {
    run_live_retry_vertical(LiveVerticalCase::GateCancelledBeforeResult).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_stop_releases_competing_confirmations() {
    run_live_retry_vertical(LiveVerticalCase::StopDuringConfirm).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_stop_waiting_cancels_exact_run_without_replay() {
    run_live_retry_vertical(LiveVerticalCase::StopWaiting).await;
}

#[cfg(unix)]
#[actix_web::test]
async fn live_retry_http_response_headers_timeout_continues_same_run() {
    run_live_retry_vertical(LiveVerticalCase::HeadersTimeout).await;
}
