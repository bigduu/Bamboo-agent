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
    fn queued_route_identity(&self) -> crate::provider::QueuedRouteIdentity {
        crate::provider::queued_identity_for(&self.inner)
    }
    fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
        let Some(inner) = self.inner.queued_provider_snapshot()? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(Self {
            inner,
            overrides: self.overrides.clone(),
        })))
    }
    fn known_pre_dispatch_error(&self) -> Option<crate::provider::LLMError> {
        self.inner.known_pre_dispatch_error()
    }

    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        self.overrides.get(model).copied()
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

    struct LocalRefusal;
    #[async_trait]
    impl LLMProvider for LocalRefusal {
        fn known_pre_dispatch_error(&self) -> Option<crate::provider::LLMError> {
            Some(crate::provider::LLMError::Auth("synthetic refusal".into()))
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            panic!("a known local refusal must not dispatch")
        }
    }

    #[tokio::test]
    async fn vision_and_role_decorators_forward_known_pre_dispatch_failure() {
        let vision: Arc<dyn LLMProvider> = Arc::new(ModelVisionProvider {
            inner: Arc::new(LocalRefusal),
            overrides: BTreeMap::new(),
        });
        let mut reference = ProviderModelRef::new("fixture", "model");
        reference.reasoning_effort = Some(ReasoningEffort::Low);
        let selected = ResolvedModel::from_ref(vision, &reference);
        assert!(matches!(selected.provider.known_pre_dispatch_error(),
            Some(crate::provider::LLMError::Auth(message)) if message == "synthetic refusal"));
    }
    struct MetadataProvider;
    #[async_trait]
    impl LLMProvider for MetadataProvider {
        async fn vision_support_override(&self, model: &str) -> Option<bool> {
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
    async fn vision_defaults_on_and_only_manual_false_disables_the_model() {
        let p = provider(BTreeMap::from([
            ("metadata".into(), false),
            ("image".into(), true),
        ]));
        assert_eq!(p.vision_support_override("metadata").await, Some(false));
        assert_eq!(p.vision_support_override("image").await, Some(true));
        assert_eq!(
            p.vision_support_override("gpt-name-is-not-evidence").await,
            None
        );
        assert!(p.supports_vision("new-model").await);
        assert!(p.supports_vision("gpt-name-is-not-evidence").await);
        assert!(!p.supports_vision("metadata").await);
        assert!(provider(BTreeMap::new()).supports_vision("metadata").await);
        let mut reference = ProviderModelRef::new("work", "metadata");
        reference.reasoning_effort = Some(ReasoningEffort::Low);
        let role = ResolvedModel::from_ref(p, &reference);
        assert_eq!(
            role.provider
                .vision_support_override(&role.model_name)
                .await,
            Some(false)
        );
    }
    #[tokio::test]
    async fn vision_factory_old_and_new_models_default_on_missing_or_null_fields() {
        let config = bamboo_config::Config::default();
        let temp = tempfile::tempdir().unwrap();
        for capability in [
            serde_json::json!(null),
            serde_json::json!({"old":{}, "new":{"supports_vision":null}}),
            serde_json::json!({"text":{"supports_vision":false}}),
        ] {
            let instance: bamboo_config::ProviderInstanceConfig =
                serde_json::from_value(serde_json::json!({
                    "provider_type":"openai", "api_key":"offline-fixture", "model":"old",
                    "runtime_models":["old","new","text"], "model_capabilities":capability,
                }))
                .unwrap();
            let provider = crate::provider_factory::create_provider_from_instance(
                &config,
                &instance,
                temp.path().to_owned(),
            )
            .await
            .unwrap();
            assert!(provider.supports_vision("old").await);
            assert!(provider.supports_vision("new").await);
            assert_eq!(
                provider.supports_vision("text").await,
                capability["text"]["supports_vision"] != false
            );
        }
        let legacy: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "api_key":"offline-fixture", "model":"old",
            }))
            .unwrap();
        let provider = crate::provider_factory::create_provider_from_instance(
            &config,
            &legacy,
            temp.path().to_owned(),
        )
        .await
        .unwrap();
        assert!(provider.supports_vision("old").await);
        assert!(provider.supports_vision("new").await);
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
                    .vision_support_override(model)
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

    #[tokio::test]
    async fn vision_and_reasoning_snapshots_preserve_outer_budget_identity() {
        struct Snapshotting {
            inner: Arc<dyn LLMProvider>,
        }
        #[async_trait]
        impl LLMProvider for Snapshotting {
            fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
                Ok(Some(self.inner.clone()))
            }
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> Result<LLMStream> {
                Ok(Box::pin(futures::stream::empty()))
            }
        }
        let inner: Arc<dyn LLMProvider> = Arc::new(LocalRefusal);
        let vision: Arc<dyn LLMProvider> = Arc::new(ModelVisionProvider {
            inner: Arc::new(Snapshotting { inner }),
            overrides: BTreeMap::from([("model-a".into(), false)]),
        });
        let reference =
            ProviderModelRef::new("fixture", "model-a").with_reasoning_effort(ReasoningEffort::Low);
        let role = ResolvedModel::from_ref(vision.clone(), &reference).provider;
        for original in [vision, role] {
            let expected = original
                .resource_allocation_identity()
                .unwrap_or(Arc::as_ptr(&original) as *const () as usize);
            let pinned = crate::provider::queued_snapshot_for(original).unwrap();
            assert_eq!(pinned.resource_allocation_identity(), Some(expected));
            assert_eq!(pinned.vision_support_override("model-a").await, Some(false));
            assert!(pinned.known_pre_dispatch_error().is_some());
        }
    }
}
