// Synthetic providers only receive test calls. The managed provider factory is
// constructed with synthetic settings solely to obtain a valid origin; no
// request is ever sent through that factory-created provider.
#[derive(Default)]
struct ChildGoldSyntheticProvider {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl bamboo_llm::LLMProvider for ChildGoldSyntheticProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[bamboo_domain::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::pin(futures::stream::empty()))
    }
}

fn child_gold_test_config() -> Config {
    let mut config = Config::default();
    config.provider = "relay".into();
    config.default_provider_instance = Some("relay".into());
    config.http_proxy = "http://127.0.0.1:9".into();
    config.https_proxy.clear();
    config.proxy_auth = None;
    config.provider_instances.insert(
        "relay".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type": "openai", "enabled": true,
            "api_key": "synthetic-only", "base_url": "http://127.0.0.1:9",
            "model": "main-a", "runtime_models": ["main-a", "gold"]
        })).unwrap(),
    );
    config
}

#[tokio::test]
async fn child_gold_origin_captured_for_both_selection_branches_stays_fail_closed() {
    for via_router in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let config = child_gold_test_config();
        let live = Arc::new(RwLock::new(config.clone()));
        let registry = Arc::new(ProviderRegistry::from_config(&config, dir.path().into()).await.unwrap());
        let router = ProviderModelRouter::new(registry.clone());
        let main = bamboo_domain::ProviderModelRef::new("relay", "main-a");
        let (selected, origin) = if via_router {
            router.route_with_origin(&main).unwrap()
        } else {
            registry.get_with_origin("relay").unwrap()
        };
        assert!(origin.is_some());
        let resolve = make_child_resume_gold_resolver(
            live.clone(), "relay".into(), selected.clone(), origin,
        ).unwrap();
        let capture = Arc::new(ChildGoldSyntheticProvider::default());
        let synthetic_delegate: Arc<dyn bamboo_llm::LLMProvider> = capture.clone();
        let admitted = resolve("gold", synthetic_delegate.clone());
        assert!(admitted.known_pre_dispatch_error().is_none());
        // Do not dispatch this guard: the fake delegate is only a tripwire for
        // refusals; real resume binds the budgeted selected provider instead.
        let denied = resolve("not-admitted", synthetic_delegate.clone());
        assert!(denied.known_pre_dispatch_error().is_some());
        assert!(denied.chat_stream(&[], &[], None, "not-admitted").await.is_err());
        {
            let _writer = live.write().await;
            let busy = admitted.known_pre_dispatch_error().expect("busy config fails closed");
            assert!(busy.to_string().contains("busy"));
            assert!(admitted.chat_stream(&[], &[], None, "gold").await.is_err());
        }
        assert_eq!(capture.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        // The original managed origin remains captured even if the registry is
        // replaced by an unmanaged map containing the same selected Arc.
        registry.replace_with(ProviderRegistry::new(
            HashMap::from([("relay".into(), selected)]), "relay".into(),
        ));
        assert!(admitted.known_pre_dispatch_error().is_some());
        assert!(admitted.chat_stream(&[], &[], None, "gold").await.is_err());
        assert_eq!(capture.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn child_gold_missing_managed_binding_is_not_an_unmanaged_bypass() {
    let dir = tempfile::tempdir().unwrap();
    let config = child_gold_test_config();
    let live = Arc::new(RwLock::new(config.clone()));
    let registry = Arc::new(ProviderRegistry::from_config(&config, dir.path().into()).await.unwrap());
    let capture = Arc::new(ChildGoldSyntheticProvider::default());
    let selected: Arc<dyn bamboo_llm::LLMProvider> = capture.clone();
    // insert invalidates the managed binding while retaining managed origin.
    registry.insert("relay".into(), selected.clone());
    let (same, origin) = registry.get_with_origin("relay").unwrap();
    assert!(Arc::ptr_eq(&same, &selected));
    assert!(origin.is_some());
    let resolve = make_child_resume_gold_resolver(live.clone(), "relay".into(), same.clone(), origin).unwrap();
    let guard = resolve("gold", same.clone());
    assert!(guard.known_pre_dispatch_error().is_some());
    assert!(guard.chat_stream(&[], &[], None, "gold").await.is_err());
    assert_eq!(capture.calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    let unmanaged = Arc::new(ProviderRegistry::new(
        HashMap::from([("relay".into(), selected)]), "relay".into(),
    ));
    let (injected, origin) = unmanaged.get_with_origin("relay").unwrap();
    assert!(origin.is_none());
    assert!(make_child_resume_gold_resolver(live, "relay".into(), injected, origin).is_none());
}

#[test]
fn child_gold_delegate_binding_rejects_mismatch_and_leaves_sdk_unchanged() {
    use crate::runtime::config::{AgentLoopConfig, GoldTargetResolver};
    use crate::runtime::runtime::bind_child_gold_delegate;
    let selected: Arc<dyn bamboo_llm::LLMProvider> = Arc::new(ChildGoldSyntheticProvider::default());
    let other: Arc<dyn bamboo_llm::LLMProvider> = Arc::new(ChildGoldSyntheticProvider::default());
    let identity: GoldTargetResolver = Arc::new(|_, delegate| delegate);
    let mut config = AgentLoopConfig { gold_target_resolver: Some(identity.clone()), ..Default::default() };
    bind_child_gold_delegate(&mut config, Some(&selected), &selected, &selected).unwrap();
    let result = config.gold_target_resolver.as_ref().unwrap()("gold", other.clone());
    assert!(Arc::ptr_eq(&result, &selected));
    let mut mismatch = AgentLoopConfig { gold_target_resolver: Some(identity.clone()), ..Default::default() };
    assert!(bind_child_gold_delegate(&mut mismatch, Some(&selected), &other, &other).is_err());
    let mut missing = AgentLoopConfig::default();
    assert!(bind_child_gold_delegate(&mut missing, Some(&selected), &selected, &selected).is_err());
    let mut sdk = AgentLoopConfig { gold_target_resolver: Some(identity), ..Default::default() };
    bind_child_gold_delegate(&mut sdk, None, &selected, &selected).unwrap();
    let result = sdk.gold_target_resolver.as_ref().unwrap()("gold", other.clone());
    assert!(Arc::ptr_eq(&result, &other));
}
