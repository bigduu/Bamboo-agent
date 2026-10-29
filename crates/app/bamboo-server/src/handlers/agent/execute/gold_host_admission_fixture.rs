// Synthetic Host integration test using a local provider endpoint.

#[actix_web::test]
async fn host_execute_explicit_gold_rechecks_managed_session_target() {
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    let account = MockServer::start().await;
    let foreground = Arc::new(AtomicUsize::new(0));
    let calls = foreground.clone();
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let model = body["model"].as_str().expect("synthetic model");
            let (delta, delay) = match model {
                "test-model" if calls.fetch_add(1, AtomicOrdering::SeqCst) % 2 == 0 => (
                    json!({"tool_calls": [{
                        "index": 0, "id": "fixture-goal", "type": "function",
                        "function": {"name": "update_goal", "arguments": "{\"status\":\"complete\"}"}
                    }]}),
                    false,
                ),
                "test-model" => (json!({"content": "synthetic final answer"}), true),
                "explicit-gold" => (
                    json!({"tool_calls": [{
                        "index": 0, "id": "fixture-verdict", "type": "function",
                        "function": {"name": "report_gold_evaluation", "arguments":
                            "{\"decision\":\"achieved\",\"confidence\":\"high\",\"reasoning\":\"synthetic\",\"missing_information\":[]}"}
                    }]}),
                    false,
                ),
                other => panic!("unexpected synthetic model {other}"),
            };
            let frame = json!({"id":"fixture","object":"chat.completion.chunk","created":0,
                "model":model,"choices":[{"index":0,"delta":delta,"finish_reason":null}]});
            let response = ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {frame}\n\ndata: [DONE]\n\n"));
            if delay { response.set_delay(Duration::from_secs(1)) } else { response }
        })
        .mount(&account)
        .await;

    let home = tempfile::tempdir().unwrap();
    let mut config = bamboo_llm::Config::default();
    config.provider = "chat-a".into();
    config.default_provider_instance = Some("chat-a".into());
    config.http_proxy = "http://127.0.0.1:9".into();
    config.https_proxy.clear();
    config.proxy_auth = None;
    let memory = config.memory_mut().get_or_insert_with(Default::default);
    memory.auto_dream_enabled = false;
    memory.gardener_enabled = false;
    memory.dedup_gardener_enabled = false;
    memory.ledger_gardener_enabled = false;
    memory.ledger_distillation_enabled = false;
    memory.granularity_freshness_gardener_enabled = false;
    config.provider_instances.insert(
        "chat-a".into(),
        serde_json::from_value(json!({
            "provider_type":"openai", "enabled":true, "api_key":"synthetic-local-only",
            "base_url": account.uri(), "model":"test-model",
            "runtime_models":["test-model", "explicit-gold"]
        }))
        .unwrap(),
    );
    let mut app_state = crate::AppState::new_with_provider(
        home.path().to_path_buf(),
        config,
        Arc::new(LocalProvider),
    )
    .await
    .unwrap();
    app_state.stop_config_watcher_for_test();
    let state = web::Data::new(app_state);
    state
        .session_activation_router
        .set_spawner(Arc::new(ManualHttpOnly))
        .await;
    let initial = state.config.read().await.clone();
    let old = state
        .provider_registry
        .get("chat-a")
        .expect("managed instance initialized");
    let app = test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let mut offset = 0usize;
    for revoked in [false, true] {
        if revoked {
            state
                .config
                .write()
                .await
                .provider_instances
                .get_mut("chat-a")
                .unwrap()
                .extra
                .insert("runtime_models".into(), json!(["test-model"]));
        }
        let live = state.config.read().await;
        let main_ref = bamboo_domain::ProviderModelRef::new("chat-a", "test-model");
        let gold_ref = bamboo_domain::ProviderModelRef::new("chat-a", "explicit-gold");
        assert!(state
            .provider_registry
            .provider_for_configured_model(&live, &main_ref)
            .is_ok());
        assert_eq!(
            state
                .provider_registry
                .provider_for_configured_model(&live, &gold_ref)
                .is_err(),
            revoked
        );
        assert!(Arc::ptr_eq(
            &old,
            &state.provider_registry.get("chat-a").unwrap()
        ));
        drop(live);
        let id = if revoked {
            "host-gold-revoked"
        } else {
            "host-gold-admitted"
        };
        let mut session = owned_http_session(&state, id);
        session.model_ref = Some(main_ref);
        session.metadata.insert(
            "gold_config".into(),
            json!({
                "enabled":true,"auto_continue_enabled":true,"goal":"synthetic goal",
                "model_name":"explicit-gold"
            })
            .to_string(),
        );
        state.storage.save_session(&session).await.unwrap();
        queue_owned_root(&state, id).await;
        let sender = state.get_session_event_sender(id).await;
        let mut events = sender.subscribe();
        let before = foreground.load(AtomicOrdering::SeqCst);
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        wait_for_owned_terminal(&state, id, body["run_id"].as_str().unwrap(), &mut events).await;
        assert_eq!(
            foreground.load(AtomicOrdering::SeqCst) - before,
            2,
            "foreground must run"
        );
        let requests = account.received_requests().await.unwrap();
        let gold: Vec<_> = requests[offset..]
            .iter()
            .filter(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["model"]
                    == "explicit-gold"
            })
            .collect();
        offset = requests.len();
        if revoked {
            assert!(
                gold.is_empty(),
                "revoked Gold dispatched {} requests",
                gold.len()
            );
        } else {
            for checkpoint in ["checkpoint=post_round", "checkpoint=terminal"] {
                assert!(
                    gold.iter()
                        .any(|request| String::from_utf8_lossy(&request.body).contains(checkpoint)),
                    "admitted Gold missed {checkpoint}"
                );
            }
        }
    }
    *state.config.write().await = initial;
}
