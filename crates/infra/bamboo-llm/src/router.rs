//! Routes a [`ProviderModelRef`] to the correct [`LLMProvider`] from the registry.

use std::sync::Arc;

use bamboo_domain::ProviderModelRef;

use crate::provider::{LLMError, LLMProvider};
use crate::provider_registry::ProviderRegistry;

/// Given a [`ProviderModelRef`], resolve the correct [`LLMProvider`].
pub struct ProviderModelRouter {
    registry: Arc<ProviderRegistry>,
}

impl ProviderModelRouter {
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self { registry }
    }

    /// Resolve the provider for a given model reference.
    pub fn route(&self, target: &ProviderModelRef) -> Result<Arc<dyn LLMProvider>, LLMError> {
        self.registry.get(&target.provider).ok_or_else(|| {
            LLMError::Auth(format!(
                "Provider '{}' not available. Available: {}",
                target.provider,
                self.registry.provider_names().join(", ")
            ))
        })
    }

    /// Resolve the canonical provider type for a given model reference.
    ///
    /// In multi-instance mode `target.provider` may be an instance id, so this
    /// consults registry metadata first and falls back to the raw provider key
    /// for backward compatibility.
    pub fn provider_type_for(&self, target: &ProviderModelRef) -> Option<String> {
        let provider = target.provider.trim();
        if provider.is_empty() {
            return None;
        }

        self.registry
            .get_metadata(provider)
            .map(|meta| meta.provider_type)
            .or_else(|| Some(provider.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::Config;

    #[test]
    fn test_route_missing_provider() {
        let config = Config::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let registry = rt
            .block_on(ProviderRegistry::from_config(
                &config,
                std::path::PathBuf::new(),
            ))
            .unwrap();
        let router = ProviderModelRouter::new(Arc::new(registry));

        let target = ProviderModelRef::new("nonexistent", "some-model");
        let result = router.route(&target);
        assert!(result.is_err());
    }
}
