// Actual Gold auto-answer consumers with deliberately separate router and
// AppState registries. All providers and session content are synthetic.

struct GoldRouterCapture {
    requests: Mutex<Vec<RecordedRequest>>,
}

impl GoldRouterCapture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
        })
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl LLMProvider for GoldRouterCapture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("Gold consumers must supply request options")
    }

    async fn chat_stream_with_options(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let purpose = options
            .and_then(|options| options.request_purpose.as_deref())
            .unwrap_or("missing-purpose");
        self.requests.lock().unwrap().push(RecordedRequest {
            purpose: purpose.to_string(),
            model: model.to_string(),
        });
        let call = match purpose {
            "gold_evaluation" => gold_evaluation_call(json!({
                "decision":"continue", "confidence":"high", "reasoning":"synthetic"
            })),
            "gold_auto_answer" => auto_answer_call(json!({
                "apply":false, "confidence":"high", "reasoning":"synthetic decline"
            })),
            _ => panic!("unexpected synthetic purpose {purpose}"),
        };
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![call])),
            Ok(LLMChunk::Done),
        ])))
    }
}

fn differing_router_pending_session(id: &str) -> Session {
    let mut session = build_pending_session(
        id,
        "synthetic-call",
        "conclusion_with_options",
        "Continue the synthetic task?",
        &["OK", "Need changes"],
        true,
        json!({"summary":"synthetic fixture"}),
    );
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new("relay", "test-model"));
    session
}

fn disable_gold_router_fixture_background(config: &mut Config) {
    let memory = config.memory_mut().get_or_insert_with(Default::default);
    memory.auto_dream_enabled = false;
    memory.gardener_enabled = false;
    memory.dedup_gardener_enabled = false;
    memory.ledger_gardener_enabled = false;
    memory.ledger_distillation_enabled = false;
    memory.granularity_freshness_gardener_enabled = false;
}

#[tokio::test]
async fn host_gold_auto_answer_preserves_self_managed_router_override() {
    let home = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.provider = "relay".into();
    disable_gold_router_fixture_background(&mut config);
    let wrong = GoldRouterCapture::new();
    let selected = GoldRouterCapture::new();
    let wrong_handle: Arc<dyn LLMProvider> = wrong.clone();
    let selected_handle: Arc<dyn LLMProvider> = selected.clone();
    let mut state = AppState::new_with_provider(home.path().into(), config, wrong_handle.clone())
        .await
        .unwrap();
    state.stop_config_watcher_for_test();
    state.provider_registry = Arc::new(ProviderRegistry::new(
        HashMap::from([("relay".to_string(), wrong_handle)]),
        "relay".into(),
    ));
    let router_registry = Arc::new(ProviderRegistry::new(
        HashMap::from([("relay".to_string(), selected_handle)]),
        "relay".into(),
    ));
    state.provider_router = Arc::new(ProviderModelRouter::new(router_registry));
    let state = Data::new(state);
    let id = "gold-different-unmanaged-router";
    let mut session = differing_router_pending_session(id);
    state.save_and_cache_session(&mut session).await;
    let resume = AppStateResumeRef(state.clone());

    for (iteration, flag) in [None, Some(false), Some(true)].into_iter().enumerate() {
        state.config.write().await.features = serde_json::from_value(match flag {
            None => json!({}),
            Some(flag) => json!({"provider_model_ref":flag}),
        })
        .unwrap();
        let result = timeout(
            Duration::from_secs(5),
            maybe_auto_answer_pending_question(
                state.get_ref(),
                &resume,
                id,
                Some(test_gold_config()),
            ),
        )
        .await
        .expect("synthetic Gold must finish");
        assert!(
            matches!(result, GoldAutoAnswerOutcome::Skipped { ref reason }
            if reason.starts_with("question_decision_declined:")),
            "{result:?}"
        );
        assert!(
            wrong.requests().is_empty(),
            "the separate state registry must not receive Gold requests"
        );
        let observed = selected.requests();
        assert_eq!(observed.len(), (iteration + 1) * 2);
        let start = iteration * 2;
        assert_eq!(
            observed[start..],
            [
                RecordedRequest {
                    purpose: "gold_evaluation".into(),
                    model: "test-model".into()
                },
                RecordedRequest {
                    purpose: "gold_auto_answer".into(),
                    model: "test-model".into()
                },
            ]
        );
    }
}

#[tokio::test]
async fn host_gold_auto_answer_checks_router_managed_config_before_both_consumers() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(|request: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let tool = body["tools"][0]["function"]["name"].as_str().unwrap();
            let args = match tool {
                "report_gold_evaluation" => json!({
                    "decision":"continue", "confidence":"high", "reasoning":"synthetic"
                }),
                "report_gold_auto_answer" => json!({
                    "apply":false, "confidence":"high", "reasoning":"synthetic decline"
                }),
                _ => panic!("unexpected synthetic tool {tool}"),
            };
            let frame = json!({"id":"synthetic-router","object":"chat.completion.chunk",
                "created":0,"model":"test-model","choices":[{"index":0,"delta":{
                    "tool_calls":[{"index":0,"id":"synthetic-tool","type":"function",
                        "function":{"name":tool,"arguments":args.to_string()}}]
                },"finish_reason":null}]});
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {frame}\n\ndata: [DONE]\n\n"))
        })
        .mount(&mock)
        .await;

    let home = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.provider = "relay".into();
    config.default_provider_instance = Some("relay".into());
    config.http_proxy = "http://127.0.0.1:9".into();
    config.https_proxy.clear();
    config.proxy_auth = None;
    disable_gold_router_fixture_background(&mut config);
    config.provider_instances.insert("relay".into(), serde_json::from_value(json!({
        "provider_type":"openai", "enabled":true, "api_key":"synthetic-only",
        "base_url":mock.uri(), "model":"main-model", "runtime_models":["main-model", "test-model"]
    })).unwrap());
    let wrong = GoldRouterCapture::new();
    let wrong_handle: Arc<dyn LLMProvider> = wrong.clone();
    let mut state = AppState::new_with_provider(home.path().into(), config, wrong_handle.clone())
        .await
        .unwrap();
    state.stop_config_watcher_for_test();
    let router_registry = state.provider_registry.clone();
    assert!(state
        .provider_router
        .route(&bamboo_domain::ProviderModelRef::new("relay", "test-model"))
        .is_ok());
    state.provider_registry = Arc::new(ProviderRegistry::new(
        HashMap::from([("relay".to_string(), wrong_handle)]),
        "relay".into(),
    ));
    assert!(!Arc::ptr_eq(&state.provider_registry, &router_registry));
    let state = Data::new(state);
    let id = "gold-different-managed-router";
    let mut session = differing_router_pending_session(id);
    state.save_and_cache_session(&mut session).await;
    let resume = AppStateResumeRef(state.clone());
    let valid = timeout(
        Duration::from_secs(5),
        maybe_auto_answer_pending_question(state.get_ref(), &resume, id, Some(test_gold_config())),
    )
    .await
    .expect("managed Gold must finish");
    assert!(
        matches!(valid, GoldAutoAnswerOutcome::Skipped { ref reason }
        if reason.starts_with("question_decision_declined:")),
        "{valid:?}"
    );
    let requests = mock.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    for (request, tool) in requests
        .iter()
        .zip(["report_gold_evaluation", "report_gold_auto_answer"])
    {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["model"], "test-model");
        assert_eq!(body["tools"][0]["function"]["name"], tool);
    }
    assert!(wrong.requests().is_empty());

    state
        .config
        .write()
        .await
        .provider_instances
        .get_mut("relay")
        .unwrap()
        .extra
        .insert("runtime_models".into(), json!(["main-model"]));
    let stale = timeout(
        Duration::from_secs(5),
        maybe_auto_answer_pending_question(state.get_ref(), &resume, id, Some(test_gold_config())),
    )
    .await
    .expect("rejected Gold must finish");
    assert!(
        matches!(stale, GoldAutoAnswerOutcome::Skipped { ref reason }
        if reason.starts_with("state_evaluation_failed:") && reason.contains("admitted")),
        "{stale:?}"
    );
    assert!(wrong.requests().is_empty());
    assert_eq!(mock.received_requests().await.unwrap().len(), 2);
}
