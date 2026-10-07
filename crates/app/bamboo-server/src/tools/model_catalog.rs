//! Server-side `ModelCatalogPort`: lists models per configured provider so the
//! parent agent can pin a child session to an explicit model
//! (`SubAgent` tool `action=list_models` / `create.model`).

use std::sync::Arc;

use async_trait::async_trait;

use bamboo_engine::session_app::child_session::{ModelCatalogPort, ProviderModelList};
use bamboo_llm::ProviderRegistry;

/// Model catalog backed by the live provider registry.
pub struct RegistryModelCatalog {
    registry: Arc<ProviderRegistry>,
}

impl RegistryModelCatalog {
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl ModelCatalogPort for RegistryModelCatalog {
    async fn list_models(&self) -> Vec<ProviderModelList> {
        let mut names = self.registry.provider_names();
        names.sort();

        names
            .into_iter()
            .map(|name| ProviderModelList {
                models: self
                    .registry
                    .runtime_models_for_provider(&name)
                    .unwrap_or_default(),
                provider: name,
                error: None,
            })
            .collect()
    }

    fn default_provider(&self) -> String {
        self.registry.default_provider_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::{LLMProvider, LLMStream};
    use std::collections::HashMap;

    struct DiscoveryMustNotRun;

    #[async_trait]
    impl LLMProvider for DiscoveryMustNotRun {
        async fn chat_stream(
            &self,
            _messages: &[bamboo_domain::Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            unreachable!("listing must not execute a model")
        }

        async fn list_models(&self) -> bamboo_llm::provider::Result<Vec<String>> {
            panic!("SubAgent runtime listing must not discover upstream candidates")
        }
    }

    #[tokio::test]
    async fn runtime_model_listing_uses_only_admission_without_upstream_discovery() {
        let registry = Arc::new(ProviderRegistry::new(
            HashMap::from([
                (
                    "relay".into(),
                    Arc::new(DiscoveryMustNotRun) as Arc<dyn LLMProvider>,
                ),
                (
                    "empty".into(),
                    Arc::new(DiscoveryMustNotRun) as Arc<dyn LLMProvider>,
                ),
            ]),
            "relay".into(),
        ));
        registry.set_runtime_models(HashMap::from([
            ("relay".into(), vec!["private/custom-id".into()]),
            ("empty".into(), Vec::new()),
        ]));
        let catalog = RegistryModelCatalog::new(registry);
        let providers = catalog.list_models().await;
        assert_eq!(providers[0].provider, "empty");
        assert!(providers[0].models.is_empty());
        assert_eq!(providers[1].provider, "relay");
        assert_eq!(providers[1].models, ["private/custom-id"]);
        assert!(providers.iter().all(|provider| provider.error.is_none()));
    }
}
