//! Keep a native SDK client's configured factory identity through dispatch.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use bamboo_domain::{CapabilityLoadingMode, Message, ToolSchema};
use bamboo_llm::provider::{queued_identity_for, ProviderModelInfo, QueuedRouteIdentity};
use bamboo_llm::provider_registry::SdkProviderBinding;
use bamboo_llm::{
    Config, LLMError, LLMProvider, LLMRequestOptions, LLMStream, PromptIR,
    ProviderVisibleToolFootprint,
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub(super) enum SdkBinding {
    Native(Arc<SdkProviderBinding>),
    Instance,
    Legacy,
    Opaque,
}

fn has_legacy(config: &Config, provider: &str) -> bool {
    let providers = config.providers();
    match provider {
        "openai" => providers.openai.is_some(),
        "anthropic" => providers.anthropic.is_some(),
        "gemini" => providers.gemini.is_some(),
        "bodhi" => providers.bodhi.is_some(),
        "copilot" => {
            providers.copilot.is_some()
                || (config.provider_instances.is_empty()
                    && config.effective_default_provider() == provider)
        }
        _ => false,
    }
}

/// A caller-owned client is authoritative for its identity. Remember only
/// whether its declared key came from configured metadata, so later disable,
/// removal, or source changes cannot silently reuse that client.
pub(super) fn caller_binding(config: &Config, provider: &str) -> SdkBinding {
    if config.provider_instances.contains_key(provider) {
        SdkBinding::Instance
    } else if has_legacy(config, provider) {
        SdkBinding::Legacy
    } else {
        SdkBinding::Opaque
    }
}

pub(super) struct BoundSdkProvider {
    pub(super) delegate: Arc<dyn LLMProvider>,
    pub(super) config: Arc<RwLock<Config>>,
    pub(super) binding: SdkBinding,
    pub(super) provider: String,
    pub(super) single_use: bool,
    pub(super) refused: AtomicBool,
}

impl BoundSdkProvider {
    fn validate(&self, model: Option<&str>) -> Result<(), LLMError> {
        if self.single_use && self.refused.load(Ordering::Acquire) {
            return Err(LLMError::Api(
                "sdk_provider_binding: this queued target already refused dispatch".into(),
            ));
        }
        let config = self.config.try_read().map_err(|_| {
            LLMError::Api("sdk_provider_binding: live provider configuration is busy".into())
        })?;
        let source_valid = match &self.binding {
            SdkBinding::Native(binding) => binding.validate(&config).is_ok(),
            SdkBinding::Instance => config
                .provider_instances
                .get(&self.provider)
                .is_some_and(|instance| instance.enabled),
            SdkBinding::Legacy => {
                !config.provider_instances.contains_key(&self.provider)
                    && has_legacy(&config, &self.provider)
            }
            SdkBinding::Opaque => {
                !config.provider_instances.contains_key(&self.provider)
                    && !has_legacy(&config, &self.provider)
            }
        };
        if !source_valid {
            return Err(LLMError::Api(
                "sdk_provider_binding: provider configuration changed; supply a current client"
                    .into(),
            ));
        }
        if let Some(model) = model {
            let models =
                bamboo_config::explicit_runtime_models_for_provider_key(&config, &self.provider)
                    .map_err(|_| {
                        LLMError::Api("sdk_provider_binding: invalid model admission".into())
                    })?;
            if models.is_some_and(|models| !models.iter().any(|allowed| allowed == model)) {
                return Err(LLMError::Api(
                    "sdk_provider_binding: model is not admitted for the selected provider".into(),
                ));
            }
        }
        Ok(())
    }

    fn dispatch(&self, model: Option<&str>) -> Result<(), LLMError> {
        let result = self.validate(model);
        if self.single_use && result.is_err() {
            self.refused.store(true, Ordering::Release);
        }
        result
    }
}

#[async_trait]
impl LLMProvider for BoundSdkProvider {
    fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
        Some(self.delegate.clone())
    }

    fn known_pre_dispatch_error(&self) -> Option<LLMError> {
        self.dispatch(None)
            .err()
            .or_else(|| self.delegate.known_pre_dispatch_error())
    }

    fn local_dispatch_refused(&self) -> bool {
        (self.single_use && self.refused.load(Ordering::Acquire))
            || self.delegate.local_dispatch_refused()
    }

    fn queue_requires_current_route(&self) -> bool {
        true
    }

    fn queued_route_identity(&self) -> QueuedRouteIdentity {
        queued_identity_for(&self.delegate)
    }

    fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>, LLMError> {
        // Each queued evaluation owns a separate refusal marker. Once a
        // target is single-use, never replace it with a fresh marker.
        if self.single_use {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            delegate: self.delegate.clone(),
            config: self.config.clone(),
            binding: self.binding.clone(),
            provider: self.provider.clone(),
            single_use: true,
            refused: AtomicBool::new(false),
        })))
    }

    fn resource_allocation_identity(&self) -> Option<usize> {
        Some(
            self.delegate
                .resource_allocation_identity()
                .unwrap_or(Arc::as_ptr(&self.delegate) as *const () as usize),
        )
    }

    async fn supports_vision(&self, model: &str) -> bool {
        self.delegate.supports_vision(model).await
    }

    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        self.delegate.vision_support_override(model).await
    }

    async fn capability_loading_mode(
        &self,
        model: &str,
        required_tool: Option<&str>,
    ) -> CapabilityLoadingMode {
        self.delegate
            .capability_loading_mode(model, required_tool)
            .await
    }

    async fn provider_visible_tool_footprint(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        model: &str,
        required_tool: Option<&str>,
    ) -> Result<ProviderVisibleToolFootprint, LLMError> {
        self.dispatch(Some(model))?;
        self.delegate
            .provider_visible_tool_footprint(ir, tools, model, required_tool)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.dispatch(Some(model))?;
        self.delegate.chat_stream(messages, tools, max, model).await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.dispatch(Some(model))?;
        self.delegate
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
    ) -> Result<LLMStream, LLMError> {
        self.dispatch(Some(model))?;
        self.delegate
            .chat_stream_ir(ir, tools, max, model, options)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>, LLMError> {
        self.dispatch(None)?;
        self.delegate.list_models().await
    }

    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>, LLMError> {
        self.dispatch(None)?;
        self.delegate.list_model_info().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::LLMChunk;

    struct Probe {
        calls: std::sync::atomic::AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl LLMProvider for Probe {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(LLMError::Api("synthetic upstream error".into()));
            }
            Ok(Box::pin(futures::stream::iter([Ok(LLMChunk::Done)])))
        }
    }

    fn fixture(fail: bool) -> (Arc<BoundSdkProvider>, Arc<RwLock<Config>>, Arc<Probe>) {
        let mut config = Config::default();
        config.provider = "copilot".into();
        config.provider_instances.clear();
        config.default_provider_instance = None;
        let binding = Arc::new(SdkProviderBinding::capture(&config, "copilot").unwrap());
        let config = Arc::new(RwLock::new(config));
        let probe = Arc::new(Probe {
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail,
        });
        let provider = Arc::new(BoundSdkProvider {
            delegate: probe.clone(),
            config: config.clone(),
            binding: SdkBinding::Native(binding),
            provider: "copilot".into(),
            single_use: false,
            refused: AtomicBool::new(false),
        });
        (provider, config, probe)
    }

    #[tokio::test]
    async fn sdk_queued_source_guard_refusal_is_single_use_and_does_not_poison_next_target() {
        let (root, config, probe) = fixture(false);
        let refused = root.queued_provider_snapshot().unwrap().unwrap();
        let hold = config.write().await;
        let error = refused.known_pre_dispatch_error().unwrap();
        assert!(error.to_string().contains("sdk_provider_binding:"));
        assert!(refused.local_dispatch_refused());
        drop(hold);

        // A second snapshot cannot clear this evaluation's refusal marker.
        assert!(refused.queued_provider_snapshot().unwrap().is_none());
        let error = refused
            .chat_stream(&[], &[], None, "main-a")
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("sdk_provider_binding:"));
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);

        // A later independent evaluation gets a fresh marker and can run.
        let next = root.queued_provider_snapshot().unwrap().unwrap();
        assert!(next.chat_stream(&[], &[], None, "main-a").await.is_ok());
        assert!(!next.local_dispatch_refused());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sdk_absent_overlay_source_never_treats_disabled_instance_as_absent() {
        let mut original = Config::default();
        original.provider = "openai".into();
        original.default_provider_instance = None;
        original.provider_instances.clear();
        original.providers_mut().openai = None;
        let binding = Arc::new(SdkProviderBinding::capture(&original, "openai").unwrap());
        let config = Arc::new(RwLock::new(original));
        let probe = Arc::new(Probe {
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail: false,
        });
        let provider = BoundSdkProvider {
            delegate: probe.clone(),
            config: config.clone(),
            binding: SdkBinding::Native(binding),
            provider: "openai".into(),
            single_use: false,
            refused: AtomicBool::new(false),
        };
        config.write().await.provider_instances.insert(
            "openai".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": false, "model": "main-a"
            }))
            .unwrap(),
        );
        assert!(provider.known_pre_dispatch_error().is_some());
        assert!(provider.list_model_info().await.is_err());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sdk_queued_source_guard_preserves_real_upstream_errors() {
        let (root, _, probe) = fixture(true);
        let target = root.queued_provider_snapshot().unwrap().unwrap();
        let error = target
            .chat_stream(&[], &[], None, "main-a")
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("synthetic upstream error"));
        assert!(!target.local_dispatch_refused());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    }
}
