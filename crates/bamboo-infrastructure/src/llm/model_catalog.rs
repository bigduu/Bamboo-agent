//! Aggregates model lists from all configured providers into a unified [`ProviderCatalog`].

use std::sync::Arc;

use bamboo_domain::provider_catalog::{
    ModelCapabilities, ModelSource, ProviderCatalog, ProviderDescriptor, ProviderModelDescriptor,
};
use bamboo_domain::ProviderModelRef;

use crate::llm::provider_registry::ProviderRegistry;

/// Service that aggregates model metadata across all registered providers.
pub struct ModelCatalogService {
    registry: Arc<ProviderRegistry>,
}

impl ModelCatalogService {
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self { registry }
    }

    /// Build a full catalog by querying every provider for its model list.
    pub async fn get_catalog(&self) -> ProviderCatalog {
        let mut providers = Vec::new();
        let mut models = Vec::new();

        for name in self.registry.provider_names() {
            providers.push(ProviderDescriptor {
                id: name.clone(),
                display_name: display_name_for_provider(&name),
                enabled: true,
                authenticated: self.registry.get(&name).is_some(),
            });

            if let Some(provider) = self.registry.get(&name) {
                match provider.list_model_info().await {
                    Ok(info_list) => {
                        for info in info_list {
                            models.push(ProviderModelDescriptor {
                                reference: ProviderModelRef::new(&name, &info.id),
                                display_name: info.id.clone(),
                                provider_display_name: display_name_for_provider(&name),
                                capabilities: ModelCapabilities::default(),
                                source: Some(ModelSource::Upstream),
                                discovered_at: None,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!(provider = &name, error = %e, "Failed to list models");
                    }
                }
            }
        }

        ProviderCatalog {
            providers,
            models,
            updated_at: Some(chrono::Utc::now().to_rfc3339()),
        }
    }

    /// List models for a single provider.
    pub async fn list_models_for_provider(
        &self,
        provider_name: &str,
    ) -> Result<Vec<ProviderModelDescriptor>, String> {
        let provider = self
            .registry
            .get(provider_name)
            .ok_or_else(|| format!("Provider '{}' not found", provider_name))?;

        let info_list = provider
            .list_model_info()
            .await
            .map_err(|e| e.to_string())?;

        Ok(info_list
            .into_iter()
            .map(|info| ProviderModelDescriptor {
                reference: ProviderModelRef::new(provider_name, &info.id),
                display_name: info.id.clone(),
                provider_display_name: display_name_for_provider(provider_name),
                capabilities: ModelCapabilities::default(),
                source: Some(ModelSource::Upstream),
                discovered_at: None,
            })
            .collect())
    }

    /// Fetch model lists from all registered providers.
    ///
    /// Returns a result per provider, preserving individual success/failure status
    /// so callers can see partial results.
    pub async fn fetch_models_for_all_providers(
        &self,
    ) -> Vec<ProviderFetchResult> {
        let names = self.registry.provider_names();
        let mut results = Vec::with_capacity(names.len());

        for name in names {
            let result = self.list_models_for_provider(&name).await;
            results.push(match result {
                Ok(models) => ProviderFetchResult {
                    provider: name,
                    models: Some(models),
                    error: None,
                },
                Err(e) => ProviderFetchResult {
                    provider: name,
                    models: None,
                    error: Some(e),
                },
            });
        }

        results
    }
}

/// Result of fetching models from a single provider during a bulk fetch.
#[derive(Debug, serde::Serialize)]
pub struct ProviderFetchResult {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<ProviderModelDescriptor>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn display_name_for_provider(id: &str) -> String {
    match id {
        "openai" => "OpenAI".to_string(),
        "anthropic" => "Anthropic".to_string(),
        "gemini" => "Gemini".to_string(),
        "copilot" => "GitHub Copilot".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::provider::{LLMError, LLMProvider, LLMStream, ProviderModelInfo, Result};
    use async_trait::async_trait;
    use bamboo_domain::Message;
    use bamboo_domain::ToolSchema;
    use futures::stream;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct MockProvider {
        models: Vec<ProviderModelInfo>,
    }

    #[async_trait]
    impl LLMProvider for MockProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream> {
            Ok(Box::pin(stream::empty()))
        }

        async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(self.models.clone())
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(self.models.iter().map(|m| m.id.clone()).collect())
        }
    }

    struct FailingProvider;

    #[async_trait]
    impl LLMProvider for FailingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream> {
            Err(LLMError::Api("fail".to_string()))
        }

        async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
            Err(LLMError::Api("list failed".to_string()))
        }
    }

    fn make_registry(
        providers: Vec<(&str, Vec<ProviderModelInfo>)>,
        default: &str,
    ) -> Arc<ProviderRegistry> {
        let mut map = HashMap::new();
        for (name, models) in providers {
            map.insert(
                name.to_string(),
                Arc::new(MockProvider { models }) as Arc<dyn LLMProvider>,
            );
        }
        Arc::new(ProviderRegistry::new(map, default.to_string()))
    }

    // ---- display_name_for_provider ----

    #[test]
    fn display_name_openai() {
        assert_eq!(display_name_for_provider("openai"), "OpenAI");
    }

    #[test]
    fn display_name_anthropic() {
        assert_eq!(display_name_for_provider("anthropic"), "Anthropic");
    }

    #[test]
    fn display_name_gemini() {
        assert_eq!(display_name_for_provider("gemini"), "Gemini");
    }

    #[test]
    fn display_name_copilot() {
        assert_eq!(display_name_for_provider("copilot"), "GitHub Copilot");
    }

    #[test]
    fn display_name_unknown_passthrough() {
        assert_eq!(display_name_for_provider("custom-llm"), "custom-llm");
    }

    // ---- get_catalog ----

    #[tokio::test]
    async fn catalog_includes_all_providers() {
        let registry = make_registry(
            vec![
                ("openai", vec![ProviderModelInfo::from_id("gpt-4o")]),
                ("anthropic", vec![ProviderModelInfo::from_id("claude-3")]),
            ],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        assert_eq!(catalog.providers.len(), 2);
        let provider_ids: Vec<&str> = catalog.providers.iter().map(|p| p.id.as_str()).collect();
        assert!(provider_ids.contains(&"openai"));
        assert!(provider_ids.contains(&"anthropic"));
    }

    #[tokio::test]
    async fn catalog_aggregates_models_from_all_providers() {
        let registry = make_registry(
            vec![
                (
                    "openai",
                    vec![
                        ProviderModelInfo::from_id("gpt-4o"),
                        ProviderModelInfo::from_id("gpt-4-turbo"),
                    ],
                ),
                ("anthropic", vec![ProviderModelInfo::from_id("claude-3")]),
            ],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        assert_eq!(catalog.models.len(), 3);
    }

    #[tokio::test]
    async fn catalog_model_refs_include_provider() {
        let registry = make_registry(
            vec![("openai", vec![ProviderModelInfo::from_id("gpt-4o")])],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        let model = &catalog.models[0];
        assert_eq!(model.reference.provider, "openai");
        assert_eq!(model.reference.model, "gpt-4o");
        assert_eq!(model.provider_display_name, "OpenAI");
    }

    #[tokio::test]
    async fn catalog_providers_marked_authenticated() {
        let registry = make_registry(
            vec![("openai", vec![ProviderModelInfo::from_id("gpt-4o")])],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        assert!(catalog.providers[0].authenticated);
    }

    #[tokio::test]
    async fn catalog_empty_registry() {
        let registry = make_registry(vec![], "openai");
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        assert!(catalog.providers.is_empty());
        assert!(catalog.models.is_empty());
        assert!(catalog.updated_at.is_some());
    }

    #[tokio::test]
    async fn catalog_has_updated_at_timestamp() {
        let registry = make_registry(vec![], "openai");
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        assert!(catalog.updated_at.is_some());
        assert!(!catalog.updated_at.unwrap().is_empty());
    }

    #[tokio::test]
    async fn catalog_skips_provider_on_list_error() {
        let mut map = HashMap::new();
        map.insert(
            "openai".to_string(),
            Arc::new(MockProvider {
                models: vec![ProviderModelInfo::from_id("gpt-4o")],
            }) as Arc<dyn LLMProvider>,
        );
        map.insert(
            "broken".to_string(),
            Arc::new(FailingProvider) as Arc<dyn LLMProvider>,
        );
        let registry = Arc::new(ProviderRegistry::new(map, "openai".to_string()));
        let service = ModelCatalogService::new(registry);
        let catalog = service.get_catalog().await;

        // Both providers listed, but broken one has no models
        assert_eq!(catalog.providers.len(), 2);
        assert_eq!(catalog.models.len(), 1);
        assert_eq!(catalog.models[0].reference.model, "gpt-4o");
    }

    // ---- list_models_for_provider ----

    #[tokio::test]
    async fn list_models_for_single_provider() {
        let registry = make_registry(
            vec![
                ("openai", vec![ProviderModelInfo::from_id("gpt-4o")]),
                ("anthropic", vec![ProviderModelInfo::from_id("claude-3")]),
            ],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let models = service.list_models_for_provider("openai").await.unwrap();

        assert_eq!(models.len(), 1);
        assert_eq!(models[0].reference.provider, "openai");
        assert_eq!(models[0].reference.model, "gpt-4o");
    }

    #[tokio::test]
    async fn list_models_for_unknown_provider_returns_error() {
        let registry = make_registry(vec![], "openai");
        let service = ModelCatalogService::new(registry);
        let result = service.list_models_for_provider("nonexistent").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    // ---- fetch_models_for_all_providers ----

    #[tokio::test]
    async fn fetch_all_returns_result_for_each_provider() {
        let registry = make_registry(
            vec![
                ("openai", vec![ProviderModelInfo::from_id("gpt-4o")]),
                ("anthropic", vec![ProviderModelInfo::from_id("claude-3")]),
            ],
            "openai",
        );
        let service = ModelCatalogService::new(registry);
        let results = service.fetch_models_for_all_providers().await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.models.is_some() && r.error.is_none()));
    }

    #[tokio::test]
    async fn fetch_all_preserves_individual_failures() {
        let mut map = HashMap::new();
        map.insert(
            "openai".to_string(),
            Arc::new(MockProvider {
                models: vec![ProviderModelInfo::from_id("gpt-4o")],
            }) as Arc<dyn LLMProvider>,
        );
        map.insert(
            "broken".to_string(),
            Arc::new(FailingProvider) as Arc<dyn LLMProvider>,
        );
        let registry = Arc::new(ProviderRegistry::new(map, "openai".to_string()));
        let service = ModelCatalogService::new(registry);
        let results = service.fetch_models_for_all_providers().await;

        assert_eq!(results.len(), 2);
        let ok = results.iter().find(|r| r.provider == "openai").unwrap();
        assert!(ok.models.is_some());
        assert!(ok.error.is_none());

        let fail = results.iter().find(|r| r.provider == "broken").unwrap();
        assert!(fail.models.is_none());
        assert!(fail.error.is_some());
    }

    #[tokio::test]
    async fn fetch_all_empty_registry() {
        let registry = make_registry(vec![], "openai");
        let service = ModelCatalogService::new(registry);
        let results = service.fetch_models_for_all_providers().await;

        assert!(results.is_empty());
    }
}
