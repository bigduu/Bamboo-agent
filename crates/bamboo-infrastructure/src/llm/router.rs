//! Routes a [`ProviderModelRef`] to the correct [`LLMProvider`] from the registry.

use std::sync::Arc;

use bamboo_domain::ProviderModelRef;

use crate::llm::provider::{LLMError, LLMProvider};
use crate::llm::provider_registry::ProviderRegistry;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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
