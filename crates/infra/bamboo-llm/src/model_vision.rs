//! Provider-instance Vision policy carried by the same routed provider handle.
use crate::provider::{ProviderModelInfo, Result};
use crate::{LLMProvider, LLMRequestOptions, LLMStream, PromptIR, ProviderVisibleToolFootprint};
use async_trait::async_trait;
use bamboo_domain::{CapabilityLoadingMode, Message, ToolSchema};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct ModelVisionProvider {
    pub(crate) inner: Arc<dyn LLMProvider>,
    pub(crate) overrides: BTreeMap<String, bool>,
}

#[async_trait]
impl LLMProvider for ModelVisionProvider {
    async fn supports_vision(&self, model: &str) -> Option<bool> {
        match self.overrides.get(model) {
            Some(value) => Some(*value),
            None => self.inner.supports_vision(model).await,
        }
    }
    async fn capability_loading_mode(
        &self,
        model: &str,
        required: Option<&str>,
    ) -> CapabilityLoadingMode {
        self.inner.capability_loading_mode(model, required).await
    }
    async fn provider_visible_tool_footprint(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        model: &str,
        required: Option<&str>,
    ) -> Result<ProviderVisibleToolFootprint> {
        self.inner
            .provider_visible_tool_footprint(ir, tools, model, required)
            .await
    }
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        self.inner.chat_stream(messages, tools, max, model).await
    }
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        self.inner
            .chat_stream_with_options(messages, tools, max, model, options)
            .await
    }
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        self.inner
            .chat_stream_ir(ir, tools, max, model, options)
            .await
    }
    async fn list_models(&self) -> Result<Vec<String>> {
        self.inner.list_models().await
    }
    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
        self.inner.list_model_info().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModelCatalogService, ProviderRegistry, ResolvedModel};
    use bamboo_domain::{ProviderModelRef, ReasoningEffort};
    struct MetadataProvider;
    #[async_trait]
    impl LLMProvider for MetadataProvider {
        async fn supports_vision(&self, model: &str) -> Option<bool> {
            (model == "metadata").then_some(true)
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            panic!("capability lookup must not dispatch or bill a model request")
        }
    }
    fn provider(overrides: BTreeMap<String, bool>) -> Arc<dyn LLMProvider> {
        Arc::new(ModelVisionProvider {
            inner: Arc::new(MetadataProvider),
            overrides,
        })
    }
    #[tokio::test]
    async fn manual_vision_wins_over_metadata_and_unknown_preserves_legacy() {
        let p = provider(BTreeMap::from([
            ("metadata".into(), false),
            ("image".into(), true),
        ]));
        assert_eq!(p.supports_vision("metadata").await, Some(false));
        assert_eq!(p.supports_vision("image").await, Some(true));
        assert_eq!(p.supports_vision("gpt-name-is-not-evidence").await, None);
        assert_eq!(
            provider(BTreeMap::new()).supports_vision("metadata").await,
            Some(true)
        );
        let mut reference = ProviderModelRef::new("work", "metadata");
        reference.reasoning_effort = Some(ReasoningEffort::Low);
        let role = ResolvedModel::from_ref(p, &reference);
        assert_eq!(
            role.provider.supports_vision(&role.model_name).await,
            Some(false)
        );
    }
    #[tokio::test]
    async fn routing_catalog_and_reload_keep_model_and_instance_vision_independent() {
        use std::collections::HashMap;
        let make = |work_support| {
            let registry = ProviderRegistry::new(
                HashMap::from([
                    (
                        "work".into(),
                        provider(BTreeMap::from([
                            ("shared".into(), work_support),
                            ("text".into(), false),
                        ])),
                    ),
                    (
                        "personal".into(),
                        provider(BTreeMap::from([("shared".into(), false)])),
                    ),
                ]),
                "work".into(),
            );
            registry.set_runtime_models(HashMap::from([
                ("work".into(), vec!["shared".into(), "text".into()]),
                ("personal".into(), vec!["shared".into()]),
            ]));
            registry
        };
        let registry = Arc::new(make(true));
        for (instance, model, expected) in [
            ("work", "shared", true),
            ("work", "text", false),
            ("personal", "shared", false),
            ("work", "shared", true),
        ] {
            let target = ProviderModelRef::new(instance, model);
            assert_eq!(
                registry
                    .provider_for_model(&target)
                    .unwrap()
                    .supports_vision(model)
                    .await,
                Some(expected)
            );
        }
        let catalog = ModelCatalogService::new(registry.clone());
        let view = catalog.get_catalog().await;
        assert!(view.models.iter().any(|m| m.reference.provider == "work"
            && m.reference.model == "shared"
            && m.capabilities.supports_vision));
        assert!(view
            .models
            .iter()
            .any(|m| m.reference.provider == "personal"
                && m.reference.model == "shared"
                && !m.capabilities.supports_vision));
        registry.replace_with(make(false));
        assert!(!catalog
            .get_catalog()
            .await
            .models
            .iter()
            .any(|m| m.capabilities.supports_vision));
    }
}
