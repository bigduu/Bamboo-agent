// Exact shared Gold guard wrapped around the real Work budget and FileStore.
// All provider traffic is to an isolated loopback mock.
#[tokio::test]
async fn gold_guard_before_work_budget_preserves_local_refusal_and_network_accounting() {
    use bamboo_domain::ProviderModelRef;
    use bamboo_llm::ProviderRegistry;
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    for scenario in ["network", "revoked", "cancelled"] {
        let (fixture, _registration) = admitted().await;
        configure(&fixture, 140);
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&mock)
            .await;
        let mut config = bamboo_llm::Config::default();
        config.provider = "chat-a".into();
        config.default_provider_instance = Some("chat-a".into());
        config.http_proxy = "http://127.0.0.1:9".into();
        config.https_proxy.clear();
        config.proxy_auth = None;
        config.provider_instances.insert(
            "chat-a".into(),
            serde_json::from_value(json!({
                "provider_type":"openai", "enabled":true, "api_key":"synthetic-only",
                "base_url":mock.uri(), "model":"fixture-model", "runtime_models":["fixture-model"]
            }))
            .unwrap(),
        );
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, fixture.dir.path().into())
                .await
                .unwrap(),
        );
        let target = ProviderModelRef::new("chat-a", "fixture-model");
        let expected = registry
            .provider_for_configured_model(&config, &target)
            .unwrap();
        let config = Arc::new(tokio::sync::RwLock::new(config));
        let budget = NativeWorkTokenBudget::capture(
            Some(fixture.service.clone()),
            fixture.storage.clone(),
            &fixture.child,
        )
        .await
        .unwrap()
        .unwrap();
        let delegate = budget.wrap(
            expected.clone(),
            &AgentLoopConfig {
                app_data_dir: Some(fixture.dir.path().to_path_buf()),
                legacy_model_limits: Some(json!([{
                    "model_pattern":"fixture-model", "max_context_tokens":100,
                    "max_output_tokens":40, "safety_margin":0
                }])),
                ..Default::default()
            },
        );
        let guard = bamboo_llm::gold_admission::guarded_gold_target(
            config.clone(),
            registry,
            target,
            expected,
            delegate,
        );
        assert!(
            guard.known_pre_dispatch_error().is_none(),
            "initial preflight is valid"
        );
        if scenario == "revoked" {
            config
                .write()
                .await
                .provider_instances
                .get_mut("chat-a")
                .unwrap()
                .extra
                .insert("runtime_models".into(), json!([]));
        }
        let cancellation = tokio_util::sync::CancellationToken::new();
        if scenario == "cancelled" {
            cancellation.cancel();
        }
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                result = guard.chat_stream(&[], &[], None, "fixture-model") => Some(result),
            }
        })
        .await
        .expect("local test must finish");
        let calls = mock.received_requests().await.unwrap().len();
        let ledger = ledger(&fixture);
        if scenario == "network" {
            assert!(result.unwrap().is_err());
            assert!(
                !guard.local_dispatch_refused(),
                "upstream errors are not local refusals"
            );
            assert_eq!(calls, 1);
            assert_eq!(ledger.reservations.len(), 1);
            assert_eq!(ledger.total_charged, 140);
            assert_eq!(ledger.reservations.values().next().unwrap().usage, None);
        } else {
            if scenario == "revoked" {
                assert!(result.unwrap().is_err());
                assert!(guard.local_dispatch_refused());
            } else {
                assert!(result.is_none());
            }
            assert_eq!(calls, 0);
            assert_eq!(
                ledger.reservations.len(),
                0,
                "nothing was admitted to Work accounting"
            );
            assert_eq!(ledger.total_charged, 0);
        }
    }
}
