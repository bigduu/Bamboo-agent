//! Synthetic managed registry: no real provider, config, network, or model.
//! Exercises the same atomic selection methods used by child resume and the
//! underlying origin guard; it is not a full coordinator execution.
use super::*;
use bamboo_domain::{Message, ProviderModelRef, ToolSchema};
use std::sync::Mutex;

#[derive(Default)]
struct Capture(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl LLMProvider for Capture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<crate::LLMStream, LLMError> {
        self.0.lock().unwrap().push(model.to_string());
        Ok(Box::pin(futures::stream::empty()))
    }
}

fn config() -> Config {
    let mut config = Config::default();
    config.provider = "relay".into();
    config.default_provider_instance = Some("relay".into());
    config.http_proxy.clear();
    config.https_proxy.clear();
    config.proxy_auth = None;
    config.provider_instances.insert(
        "relay".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type":"openai", "enabled":true,
            "api_key":"synthetic-a", "base_url":"http://127.0.0.1:9",
            "model":"main-a", "runtime_models":["main-a", "gold"]
        }))
        .unwrap(),
    );
    config
}

fn make_registry(config: &Config, provider: Arc<dyn LLMProvider>) -> ProviderRegistry {
    let key = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
        .unwrap();
    let binding = configured_binding(config, "relay", &key).unwrap().unwrap();
    ProviderRegistry {
        state: RwLock::new(RegistryState {
            providers: HashMap::from([("relay".into(), provider)]),
            metadata: HashMap::new(),
            default_provider: "relay".into(),
            runtime_models: Some(HashMap::from([(
                "relay".into(),
                vec!["main-a".into(), "gold".into()],
            )])),
            runtime_models_explicit: false,
            managed_direct_pristine: false,
            origin: RegistryOrigin::Managed {
                key: Box::new(key),
                bindings: HashMap::from([("relay".into(), binding)]),
                legacy_memory_implicit: HashSet::new(),
            },
        }),
    }
}

#[tokio::test]
async fn child_resume_selection_and_origin_guard_never_redirect_to_new_account() {
    for via_router in [true, false] {
        let config_a = config();
        let live = Arc::new(tokio::sync::RwLock::new(config_a.clone()));
        let a = Arc::new(Capture::default());
        let b = Arc::new(Capture::default());
        let selected_a: Arc<dyn LLMProvider> = a.clone();
        let registry = Arc::new(make_registry(&config_a, selected_a.clone()));
        let router = crate::router::ProviderModelRouter::new(registry.clone());
        let main = ProviderModelRef::new("relay", "main-a");
        let (selected, origin) = if via_router {
            router.route_with_origin(&main).unwrap()
        } else {
            registry.get_with_origin("relay").unwrap()
        };
        assert!(Arc::ptr_eq(&selected, &selected_a));
        let origin = origin.expect("managed origin");
        let make = |model: &str| {
            crate::gold_admission::guarded_gold_target_with_origin(
                live.clone(),
                origin.clone(),
                ProviderModelRef::new("relay", model),
                selected.clone(),
                selected.clone(),
            )
        };
        let admitted = make("gold");
        assert!(admitted.known_pre_dispatch_error().is_none());
        admitted.chat_stream(&[], &[], None, "gold").await.unwrap();
        assert_eq!(*a.0.lock().unwrap(), vec!["gold".to_string()]);
        assert!(b.0.lock().unwrap().is_empty());
        let denied = make("not-admitted");
        assert!(denied
            .chat_stream(&[], &[], None, "not-admitted")
            .await
            .is_err());
        {
            let _writer = live.write().await;
            assert!(admitted.chat_stream(&[], &[], None, "gold").await.is_err());
        }
        assert_eq!(a.0.lock().unwrap().len(), 1);

        let mut config_b = config_a.clone();
        config_b
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .api_key = "synthetic-b".into();
        let selected_b: Arc<dyn LLMProvider> = b.clone();
        let replacement = make_registry(&config_b, selected_b);
        {
            let mut writer = live.write().await;
            *writer = config_b;
            registry.replace_with(replacement);
        }
        assert!(admitted.chat_stream(&[], &[], None, "gold").await.is_err());
        assert_eq!(a.0.lock().unwrap().len(), 1);
        assert!(b.0.lock().unwrap().is_empty());
        // A managed-to-unmanaged replacement cannot erase the captured origin.
        registry.replace_with(ProviderRegistry::new(
            HashMap::from([("relay".into(), selected_a)]),
            "relay".into(),
        ));
        assert!(admitted.chat_stream(&[], &[], None, "gold").await.is_err());
        assert_eq!(a.0.lock().unwrap().len(), 1);
        assert!(b.0.lock().unwrap().is_empty());
    }
}
