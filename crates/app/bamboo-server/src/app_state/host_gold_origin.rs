//! Resolve the provenance of the concrete provider selected by a main call.
//!
//! Injection is recognized only by the exact Arc supplied to the Host's
//! injection constructor. A failed managed lookup is never called injection.

use std::sync::Arc;

use bamboo_engine::runtime::host_gold::{HostGoldOrigin, HostGoldOriginResolver};
use bamboo_llm::{LLMProvider, ProviderRegistry};

pub(crate) fn origin_resolver(
    registry: Arc<ProviderRegistry>,
    injected: Option<Arc<dyn LLMProvider>>,
) -> HostGoldOriginResolver {
    Arc::new(move |selected| {
        if injected
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, selected))
        {
            return Ok(HostGoldOrigin::Injected);
        }
        registry
            .managed_origin_for_dispatched_provider(selected)
            .map(|(provider_name, origin)| HostGoldOrigin::Managed {
                provider_name,
                origin,
            })
            .map_err(|error| error.to_string())
    })
}

/// Bind a pre-admitted managed main provider to Gold without replacing the
/// runtime's decorated delegate (which can own Work accounting).
pub(crate) fn managed_selection_resolver(
    config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    provider_name: &str,
    selected: Option<&Arc<dyn LLMProvider>>,
    origin: Option<&Arc<bamboo_llm::provider_registry::ManagedProviderOrigin>>,
) -> Option<bamboo_engine::config::GoldTargetResolver> {
    let selected = selected?.clone();
    let origin = origin?.clone();
    let provider_name = provider_name.to_owned();
    Some(Arc::new(move |model, delegate| {
        bamboo_llm::gold_admission::guarded_gold_target_with_origin(
            config.clone(),
            origin.clone(),
            bamboo_domain::ProviderModelRef::new(provider_name.as_str(), model),
            selected.clone(),
            delegate,
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Injected;
    #[async_trait::async_trait]
    impl LLMProvider for Injected {
        async fn chat_stream(
            &self,
            _: &[bamboo_agent_core::Message],
            _: &[bamboo_agent_core::ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[tokio::test]
    async fn selected_managed_gold_keeps_delegate_and_rejects_revoked_origin() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = bamboo_llm::Config::default();
        config.provider = "account-a".into();
        config.default_provider_instance = Some("account-a".into());
        config.provider_instances.insert(
            "account-a".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "enabled":true, "api_key":"synthetic-only",
                "base_url":"http://127.0.0.1:9", "model":"main-a",
                "runtime_models":["main-a", "gold"]
            }))
            .unwrap(),
        );
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, dir.path().into())
                .await
                .unwrap(),
        );
        let requested = bamboo_domain::ProviderModelRef::new("account-a", "main-a")
            .with_reasoning_effort(bamboo_domain::ReasoningEffort::High);
        let selected = bamboo_engine::model_config_helper::try_resolve_primary_model(
            &config, &registry, &requested,
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &selected.provider,
            &registry.get("account-a").unwrap()
        ));
        let live = Arc::new(tokio::sync::RwLock::new(config));
        let policy = managed_selection_resolver(
            live,
            "account-a",
            Some(&selected.provider),
            selected.managed_origin.as_ref(),
        )
        .unwrap();
        let delegate = selected.provider.clone();
        let allowed = policy("gold", delegate.clone());
        assert!(allowed.known_pre_dispatch_error().is_none());
        assert!(Arc::ptr_eq(
            &allowed.fixed_gold_policy_delegate().unwrap(),
            &delegate
        ));
        let denied = policy("not-admitted", delegate.clone());
        assert!(denied.known_pre_dispatch_error().is_some());

        registry.replace_with(ProviderRegistry::new(
            std::collections::HashMap::from([("account-a".into(), delegate.clone())]),
            "account-a".into(),
        ));
        let stale = policy("gold", delegate);
        assert!(stale.known_pre_dispatch_error().is_some());
    }

    #[tokio::test]
    async fn injected_source_is_explicit_and_unknown_is_not_injected() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = bamboo_llm::Config::default();
        config.provider = "account-a".into();
        config.default_provider_instance = Some("account-a".into());
        config.provider_instances.insert(
            "account-a".into(),
            serde_json::from_value(serde_json::json!({"provider_type":"openai", "enabled":true,
                "api_key":"synthetic-only", "base_url":"http://127.0.0.1:9",
                "model":"main-a", "runtime_models":["main-a", "gold"]}))
            .unwrap(),
        );
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, dir.path().into())
                .await
                .unwrap(),
        );
        let selected = registry.get("account-a").unwrap();
        let injected: Arc<dyn LLMProvider> = Arc::new(Injected);
        let unknown: Arc<dyn LLMProvider> = Arc::new(Injected);
        let resolve = origin_resolver(registry, Some(injected.clone()));
        assert!(matches!(resolve(&injected), Ok(HostGoldOrigin::Injected)));
        assert!(matches!(
            resolve(&selected),
            Ok(HostGoldOrigin::Managed { .. })
        ));
        assert!(resolve(&unknown).is_err());
    }
}
