use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use bamboo_agent_core::{tools::ToolSchema, Message};
use bamboo_domain::CapabilityLoadingMode;
use bamboo_llm::provider::{LLMProvider, LLMRequestOptions, Result};
use bamboo_llm::{LLMStream, PromptIR, ProviderVisibleToolFootprint};

/// An `LLMProvider` wrapper that always delegates to the latest provider stored in a shared lock.
///
/// This prevents stale provider snapshots after runtime config changes (provider/model/proxy),
/// while keeping the call sites ergonomic (`Arc<dyn LLMProvider>`).
pub struct ReloadableProvider {
    inner: Arc<RwLock<Arc<dyn LLMProvider>>>,
}

impl ReloadableProvider {
    pub fn new(inner: Arc<RwLock<Arc<dyn LLMProvider>>>) -> Self {
        Self { inner }
    }

    async fn current(&self) -> Arc<dyn LLMProvider> {
        self.inner.read().await.clone()
    }
}

#[async_trait]
impl LLMProvider for ReloadableProvider {
    async fn capability_loading_mode(
        &self,
        model: &str,
        required_tool: Option<&str>,
    ) -> CapabilityLoadingMode {
        self.current()
            .await
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
        self.current()
            .await
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
        let provider = self.current().await;
        provider
            .chat_stream(messages, tools, max_output_tokens, model)
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
        let provider = self.current().await;
        provider
            .chat_stream_with_options(messages, tools, max_output_tokens, model, options)
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
        // Delegate the canonical IR straight through so the underlying provider's
        // `chat_stream_ir` override is preserved (NOT collapsed to a flat list by
        // the trait default).
        let provider = self.current().await;
        provider
            .chat_stream_ir(ir, tools, max_output_tokens, model, options)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let provider = self.current().await;
        provider.list_models().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::{ProviderVisibleToolSegment, ProviderVisibleToolSegmentKind};

    struct FootprintProvider;

    #[async_trait]
    impl LLMProvider for FootprintProvider {
        async fn provider_visible_tool_footprint(
            &self,
            ir: &PromptIR,
            tools: &[ToolSchema],
            model: &str,
            required_tool: Option<&str>,
        ) -> Result<ProviderVisibleToolFootprint> {
            assert_eq!(ir.system_text, "forward this IR");
            assert!(tools.is_empty());
            assert_eq!(model, "forward-model");
            assert_eq!(required_tool, Some("load_skill"));
            Ok(ProviderVisibleToolFootprint {
                segments: vec![ProviderVisibleToolSegment {
                    kind: ProviderVisibleToolSegmentKind::ProviderLateBound,
                    serialized: r#"{"forwarded":true}"#.to_string(),
                }],
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream> {
            panic!("footprint forwarding test must not dispatch")
        }
    }

    fn wrapped(provider: Arc<dyn LLMProvider>) -> ReloadableProvider {
        ReloadableProvider::new(Arc::new(RwLock::new(provider)))
    }

    #[tokio::test]
    async fn capability_loading_mode_forwards_to_current_anthropic_provider() {
        let official = wrapped(Arc::new(
            bamboo_llm::providers::anthropic::AnthropicProvider::new("test-key"),
        ));
        assert_eq!(
            official
                .capability_loading_mode("claude-sonnet-4-6", None)
                .await,
            CapabilityLoadingMode::Progressive
        );
        assert_eq!(
            official
                .capability_loading_mode("claude-opus-4-1", None)
                .await,
            CapabilityLoadingMode::LegacyFullCatalog
        );
        assert_eq!(
            official
                .capability_loading_mode("claude-sonnet-4-6", Some("load_skill"))
                .await,
            CapabilityLoadingMode::LegacyFullCatalog
        );

        let custom = wrapped(Arc::new(
            bamboo_llm::providers::anthropic::AnthropicProvider::new("test-key")
                .with_base_url("https://compatible.example/v1"),
        ));
        assert_eq!(
            custom
                .capability_loading_mode("claude-sonnet-4-6", None)
                .await,
            CapabilityLoadingMode::LegacyFullCatalog
        );
    }

    #[tokio::test]
    async fn provider_visible_tool_footprint_forwards_to_current_provider() {
        let provider = wrapped(Arc::new(FootprintProvider));
        let ir = PromptIR {
            system_text: "forward this IR".to_string(),
            ..Default::default()
        };

        let footprint = provider
            .provider_visible_tool_footprint(&ir, &[], "forward-model", Some("load_skill"))
            .await
            .expect("forwarded footprint");

        assert_eq!(footprint.segments.len(), 1);
        assert_eq!(
            footprint.segments[0].kind,
            ProviderVisibleToolSegmentKind::ProviderLateBound
        );
        assert_eq!(footprint.segments[0].serialized, r#"{"forwarded":true}"#);
    }
}
