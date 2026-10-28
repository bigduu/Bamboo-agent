//! Synthetic registry dispatch-origin proofs. These fixtures manufacture
//! private managed bindings for unit coverage; they do not prove a real Host
//! factory, ReloadableProvider, or Engine integration.
use super::*;
use bamboo_domain::{Message, ProviderModelRef, ToolSchema};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Capture(AtomicUsize);

#[async_trait::async_trait]
impl LLMProvider for Capture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<crate::LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
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
    for id in ["relay", "mirror"] {
        config.provider_instances.insert(
            id.into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": true, "api_key": "synthetic",
                "base_url": "http://127.0.0.1:9", "model": "main",
                "runtime_models": ["main", "gold"]
            }))
            .unwrap(),
        );
    }
    config
}

fn managed(config: &Config, entries: Vec<(&str, Arc<dyn LLMProvider>)>) -> ProviderRegistry {
    let key = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
        .unwrap();
    let mut providers = HashMap::new();
    let mut bindings = HashMap::new();
    let mut runtime_models = HashMap::new();
    for (name, provider) in entries {
        providers.insert(name.to_owned(), provider);
        bindings.insert(
            name.to_owned(),
            configured_binding(config, name, &key).unwrap().unwrap(),
        );
        runtime_models.insert(name.to_owned(), vec!["main".into(), "gold".into()]);
    }
    ProviderRegistry {
        state: RwLock::new(RegistryState {
            providers,
            metadata: HashMap::new(),
            default_provider: "relay".into(),
            runtime_models: Some(runtime_models),
            origin: RegistryOrigin::Managed {
                key: Box::new(key),
                bindings,
                legacy_memory_implicit: HashSet::new(),
            },
        }),
    }
}

#[test]
fn reverse_lookup_requires_a_unique_bound_managed_arc() {
    let config = config();
    let selected: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let unrelated: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let registry = Arc::new(managed(&config, vec![("relay", selected.clone())]));
    let (key, origin) = registry
        .managed_origin_for_dispatched_provider(&selected)
        .unwrap();
    assert_eq!(key, "relay");
    assert!(Arc::ptr_eq(&origin.selected, &selected));
    assert!(origin
        .validate_gold_target(&config, &ProviderModelRef::new("relay", "gold"), &selected)
        .is_ok());
    assert!(registry
        .managed_origin_for_dispatched_provider(&unrelated)
        .is_err());

    // A manually inserted Arc is still in a managed registry, but cannot
    // inherit the factory binding of the handle it replaced.
    registry.insert("relay".into(), selected.clone());
    assert!(registry
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());

    let unmanaged = Arc::new(ProviderRegistry::new(
        HashMap::from([("relay".into(), selected.clone())]),
        "relay".into(),
    ));
    assert!(unmanaged
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());

    // Even if each key has a valid independent tag, shared allocation alone
    // cannot prove which key the original dispatcher meant.
    let ambiguous = Arc::new(managed(
        &config,
        vec![("relay", selected.clone()), ("mirror", selected.clone())],
    ));
    assert!(ambiguous
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());
}

#[test]
fn locked_publication_and_managed_to_unmanaged_never_prove_a_new_route() {
    let config = config();
    let selected: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let registry = Arc::new(managed(&config, vec![("relay", selected.clone())]));
    let (_, origin) = registry
        .managed_origin_for_dispatched_provider(&selected)
        .unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer_registry = registry.clone();
    let writer_selected = selected.clone();
    let writer = std::thread::spawn(move || {
        let guard = writer_registry.state.write().unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(guard);
        writer_registry.replace_with(ProviderRegistry::new(
            HashMap::from([("relay".into(), writer_selected)]),
            "relay".into(),
        ));
    });
    locked_rx.recv().unwrap();
    assert!(registry
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    assert!(registry
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());
    assert!(origin
        .validate_gold_target(&config, &ProviderModelRef::new("relay", "gold"), &selected)
        .is_err());
}

#[test]
fn poisoned_registry_does_not_prove_dispatch_origin() {
    let config = config();
    let selected: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let registry = Arc::new(managed(&config, vec![("relay", selected.clone())]));
    let writer_registry = registry.clone();
    let writer = std::thread::spawn(move || {
        let _guard = writer_registry.state.write().unwrap();
        panic!("synthetic registry poison");
    });
    assert!(writer.join().is_err());
    assert!(registry
        .managed_origin_for_dispatched_provider(&selected)
        .is_err());
}
