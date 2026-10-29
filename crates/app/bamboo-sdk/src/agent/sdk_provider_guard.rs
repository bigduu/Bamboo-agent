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

pub(super) struct BoundSdkProvider {
    pub(super) delegate: Arc<dyn LLMProvider>,
    pub(super) config: Arc<RwLock<Config>>,
    pub(super) binding: Arc<SdkProviderBinding>,
    pub(super) provider: String,
    pub(super) single_use: bool,
    pub(super) refused: AtomicBool,
}

impl BoundSdkProvider {
    fn validate(&self, model: Option<&str>) -> Result<(), LLMError> {
        let config = self.config.try_read().map_err(|_| {
            LLMError::Api("sdk_provider_binding: live provider configuration is busy".into())
        })?;
        self.binding.validate(&config).map_err(|_| {
            LLMError::Api(
                "sdk_provider_binding: native provider configuration changed; rebuild the SDK client"
                    .into(),
            )
        })?;
        if let Some(model) = model {
            let models =
                bamboo_config::explicit_runtime_models_for_provider_key(&config, &self.provider)
                    .map_err(|_| {
                        LLMError::Api("sdk_provider_binding: invalid model admission".into())
                    })?;
            if models.is_some_and(|models| !models.iter().any(|allowed| allowed == model)) {
                return Err(LLMError::Api(
                    "sdk_provider_binding: model is not admitted for the native provider".into(),
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
        // Each queued evaluation owns a separate refusal marker. Keep the
        // guard around the selected client instead of extracting its delegate.
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
