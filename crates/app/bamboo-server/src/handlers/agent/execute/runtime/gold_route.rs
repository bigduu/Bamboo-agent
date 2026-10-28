//! Bind explicit Gold to the registry handle already selected for this run.
use std::sync::Arc;

use bamboo_domain::ProviderModelRef;
use bamboo_engine::config::GoldTargetResolver;
use bamboo_llm::{LLMProvider, ProviderRegistry};

use crate::app_state::AppState;

pub(super) fn make_gold_target_resolver(
    state: &AppState,
    provider_name: &str,
    provider_override: Option<&Arc<dyn LLMProvider>>,
    provider_override_managed_registry: Option<&Arc<ProviderRegistry>>,
) -> Option<GoldTargetResolver> {
    // A session route selected by the Host has the actual registry handle.
    // Leave SDK/injected registries and unbound default providers unchanged.
    let expected = provider_override?.clone();
    let registry = provider_override_managed_registry?.clone();
    let config = state.config.clone();
    let provider_name = provider_name.to_string();
    Some(Arc::new(move |model, delegate| {
        bamboo_llm::gold_admission::guarded_gold_target(
            config.clone(),
            registry.clone(),
            ProviderModelRef::new(provider_name.as_str(), model),
            expected.clone(),
            delegate,
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{LLMError, LLMStream};
    use std::collections::HashMap;

    struct UnusedInjectedProvider;
    #[async_trait::async_trait]
    impl LLMProvider for UnusedInjectedProvider {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("the injected test provider must not be called")
        }
    }

    #[tokio::test]
    async fn managed_gold_selection_remains_guarded_after_registry_becomes_unmanaged() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = bamboo_llm::Config::default();
        config.provider = "chat-a".into();
        config.default_provider_instance = Some("chat-a".into());
        config.http_proxy = "http://127.0.0.1:9".into();
        config.https_proxy.clear();
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        config.provider_instances.insert(
            "chat-a".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "enabled":true, "api_key":"synthetic-only",
                "base_url":"http://127.0.0.1:9", "model":"main-a",
                "runtime_models":["main-a", "gold"]
            }))
            .unwrap(),
        );
        let mut state = AppState::new_with_provider(
            dir.path().into(),
            config,
            Arc::new(UnusedInjectedProvider),
        )
        .await
        .unwrap();
        state.stop_config_watcher_for_test();

        // The router and AppState can be independently injected. Selection
        // and the guard must use the router's original registry, not the
        // separate AppState registry or an account from it.
        let actual_registry = state.provider_registry.clone();
        state.provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), "other".into()));
        assert!(!Arc::ptr_eq(&state.provider_registry, &actual_registry));
        let main = ProviderModelRef::new("chat-a", "main-a");
        let (selected, managed_source) = state.provider_router.route_with_origin(&main).unwrap();
        let managed_source = managed_source.expect("selected route is managed");
        assert!(Arc::ptr_eq(&managed_source, &actual_registry));
        let resolve =
            make_gold_target_resolver(&state, "chat-a", Some(&selected), Some(&managed_source))
                .expect("a previously managed run must keep its guard");
        let guarded = resolve("gold", selected.clone());
        assert!(guarded.known_pre_dispatch_error().is_none());

        // Replacing the actual managed registry with an unmanaged one holding
        // the SAME Arc must not downgrade the already-bound run into bypass.
        actual_registry.replace_with(ProviderRegistry::new(
            HashMap::from([("chat-a".into(), selected.clone())]),
            "chat-a".into(),
        ));
        let (same, now_source) = state.provider_router.route_with_origin(&main).unwrap();
        assert!(now_source.is_none() && Arc::ptr_eq(&selected, &same));
        assert!(guarded.known_pre_dispatch_error().is_some());
        assert!(guarded.chat_stream(&[], &[], None, "gold").await.is_err());
        assert!(
            guarded.local_dispatch_refused(),
            "no underlying provider was entered"
        );
        assert!(
            make_gold_target_resolver(&state, "chat-a", Some(&same), now_source.as_ref()).is_none(),
            "an originally unmanaged SDK selection retains its existing semantics"
        );
        assert!(make_gold_target_resolver(&state, "chat-a", None, None).is_none());
    }
}
