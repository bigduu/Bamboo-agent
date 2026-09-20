use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::{Message, ProviderModelRef, ReasoningEffort, ToolSchema};

use crate::prompt_ir::PromptIR;
use crate::provider::{
    LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo, ProviderVisibleToolFootprint,
    Result,
};

/// A fully resolved model ready for LLM calls.
///
/// Contains both the provider instance and model name string, so call sites
/// don't need to worry about routing — they just use `.provider` and `.model_name`.
///
/// Created by the unified resolver functions in `model_config_helper`.
pub struct ResolvedModel {
    pub provider: Arc<dyn LLMProvider>,
    pub model_name: String,
}

impl ResolvedModel {
    pub fn new(provider: Arc<dyn LLMProvider>, model_name: impl Into<String>) -> Self {
        Self {
            provider,
            model_name: model_name.into(),
        }
    }

    /// Build a resolved role model while preserving the role's request policy.
    pub fn from_ref(provider: Arc<dyn LLMProvider>, model_ref: &ProviderModelRef) -> Self {
        Self {
            provider: provider_with_reasoning_effort(provider, model_ref.reasoning_effort),
            model_name: model_ref.model.clone(),
        }
    }
}

/// Attach an optional role-level reasoning preference to a provider handle.
///
/// The wrapper injects the preference at the final provider boundary, so every
/// consumer of a resolved role model (including legacy `chat_stream` callers)
/// observes the same setting without duplicating effort plumbing throughout
/// the runtime. Explicit call-site values remain the fallback when the role
/// does not specify an effort.
pub(crate) fn provider_with_reasoning_effort(
    provider: Arc<dyn LLMProvider>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Arc<dyn LLMProvider> {
    match reasoning_effort {
        Some(reasoning_effort) => Arc::new(RoleReasoningProvider {
            inner: provider,
            reasoning_effort,
        }),
        None => provider,
    }
}

struct RoleReasoningProvider {
    inner: Arc<dyn LLMProvider>,
    reasoning_effort: ReasoningEffort,
}

impl RoleReasoningProvider {
    fn options(&self, options: Option<&LLMRequestOptions>) -> LLMRequestOptions {
        let mut effective = options.cloned().unwrap_or_default();
        effective.reasoning_effort = Some(self.reasoning_effort);
        effective
    }
}

#[async_trait]
impl LLMProvider for RoleReasoningProvider {
    async fn capability_loading_mode(
        &self,
        model: &str,
        required_tool: Option<&str>,
    ) -> bamboo_domain::CapabilityLoadingMode {
        self.inner
            .capability_loading_mode(model, required_tool)
            .await
    }

    async fn provider_visible_tool_footprint(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        model: &str,
        required_tool: Option<&str>,
    ) -> Result<ProviderVisibleToolFootprint> {
        self.inner
            .provider_visible_tool_footprint(ir, tools, model, required_tool)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        let options = self.options(None);
        self.inner
            .chat_stream_with_options(messages, tools, max_output_tokens, model, Some(&options))
            .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        let options = self.options(options);
        self.inner
            .chat_stream_with_options(messages, tools, max_output_tokens, model, Some(&options))
            .await
    }

    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        let options = self.options(options);
        self.inner
            .chat_stream_ir(ir, tools, max_output_tokens, model, Some(&options))
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
    use std::sync::Mutex;

    use futures::stream;

    use super::*;
    use crate::LLMChunk;

    #[derive(Default)]
    struct CaptureProvider {
        efforts: Mutex<Vec<Option<ReasoningEffort>>>,
    }

    #[async_trait]
    impl LLMProvider for CaptureProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream> {
            unreachable!("role wrapper should use the options-aware path")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream> {
            self.efforts
                .lock()
                .unwrap()
                .push(options.and_then(|options| options.reasoning_effort));
            Ok(Box::pin(stream::empty::<Result<LLMChunk>>()))
        }
    }

    #[tokio::test]
    async fn role_effort_overrides_call_site_effort() {
        let inner = Arc::new(CaptureProvider::default());
        let provider = provider_with_reasoning_effort(inner.clone(), Some(ReasoningEffort::Low));
        let options = LLMRequestOptions {
            reasoning_effort: Some(ReasoningEffort::High),
            ..Default::default()
        };

        let _stream = provider
            .chat_stream_with_options(&[], &[], None, "model", Some(&options))
            .await
            .unwrap();

        assert_eq!(
            inner.efforts.lock().unwrap().as_slice(),
            &[Some(ReasoningEffort::Low)]
        );
    }
}
