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
    fn known_pre_dispatch_error(&self) -> Option<bamboo_llm::LLMError> {
        match self.inner.try_read() {
            Ok(provider) => provider.known_pre_dispatch_error(),
            Err(_) => Some(bamboo_llm::LLMError::Api(
                "Provider is being reconfigured; retry the auxiliary evaluation".into(),
            )),
        }
    }

    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        self.current().await.vision_support_override(model).await
    }

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

    struct LocalRefusal;
    #[async_trait]
    impl LLMProvider for LocalRefusal {
        fn known_pre_dispatch_error(&self) -> Option<bamboo_llm::LLMError> {
            Some(bamboo_llm::LLMError::Auth("synthetic refusal".into()))
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
    async fn reloadable_provider_forwards_current_pre_dispatch_failure() {
        let inner: Arc<RwLock<Arc<dyn LLMProvider>>> =
            Arc::new(RwLock::new(Arc::new(LocalRefusal)));
        let wrapper = ReloadableProvider::new(inner.clone());
        assert!(matches!(wrapper.known_pre_dispatch_error(),
            Some(bamboo_llm::LLMError::Auth(message)) if message == "synthetic refusal"));
        *inner.write().await = Arc::new(FootprintProvider);
        assert!(wrapper.known_pre_dispatch_error().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn locked_reloadable_fails_closed_without_blocking_gold_preflight() {
        use bamboo_agent_core::{GoldCheckpoint, Session};
        use bamboo_engine::runtime::gold_evaluation::{evaluate_gold, GoldEvalFrame};
        use bamboo_engine::runtime::GoldConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        struct SyntheticGold(AtomicUsize);
        #[async_trait]
        impl LLMProvider for SyntheticGold {
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> Result<LLMStream> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Box::pin(futures::stream::iter([Ok(
                    bamboo_llm::LLMChunk::Done,
                )])))
            }
        }
        let actual = Arc::new(SyntheticGold(AtomicUsize::new(0)));
        let inner: Arc<RwLock<Arc<dyn LLMProvider>>> = Arc::new(RwLock::new(actual.clone()));
        let wrapper = Arc::new(ReloadableProvider::new(inner.clone()));
        assert!(wrapper.known_pre_dispatch_error().is_none());

        let write_guard = inner.write().await;
        let for_query = wrapper.clone();
        let mut query = tokio::task::spawn_blocking(move || for_query.known_pre_dispatch_error());
        let query_outcome = tokio::time::timeout(Duration::from_secs(2), &mut query).await;
        // Keep the lock through the real Gold evaluation below, including the
        // failure case; it must return before the writer is released.
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let provider: Arc<dyn LLMProvider> = wrapper.clone();
        let mut evaluation = tokio::spawn(async move {
            let session = Session::new("gold-lock-fixture", "fixture-model");
            let frame = GoldEvalFrame {
                event_tx: &tx,
                session_id: &session.id,
                model: "fixture-model",
                timeout_context: Default::default(),
                reasoning_effort: None,
                checkpoint: GoldCheckpoint::Terminal,
                iteration: 1,
            };
            evaluate_gold(&session, None, &GoldConfig::default(), provider, &frame).await
        });
        let evaluation_outcome =
            tokio::time::timeout(Duration::from_secs(2), &mut evaluation).await;
        // Always release the guard before failing an assertion, so a buggy
        // blocking implementation cannot strand a test worker on teardown.
        drop(write_guard);

        assert!(matches!(
            query_outcome.expect("preflight must not wait for the provider write lock").unwrap(),
            Some(bamboo_llm::LLMError::Api(message)) if message.contains("reconfigured")
        ));
        assert!(evaluation_outcome
            .expect("Gold must not wait for the provider write lock")
            .unwrap()
            .is_err());
        assert!(rx.try_recv().is_err(), "no Gold event should be emitted");
        assert_eq!(actual.0.load(Ordering::SeqCst), 0);
        assert!(wrapper.known_pre_dispatch_error().is_none());

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let session = Session::new("gold-lock-control", "fixture-model");
        let frame = GoldEvalFrame {
            event_tx: &tx,
            session_id: &session.id,
            model: "fixture-model",
            timeout_context: Default::default(),
            reasoning_effort: None,
            checkpoint: GoldCheckpoint::Terminal,
            iteration: 1,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            evaluate_gold(&session, None, &GoldConfig::default(), wrapper, &frame),
        )
        .await
        .expect("the unlocked provider should run promptly");
        assert!(result.is_ok());
        assert_eq!(actual.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            rx.try_recv(),
            Ok(bamboo_agent_core::AgentEvent::GoldEvaluationStarted { .. })
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(bamboo_agent_core::AgentEvent::GoldEvaluationCompleted { .. })
        ));
    }

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
    struct VisionMetadataProvider(bool);
    #[async_trait]
    impl LLMProvider for VisionMetadataProvider {
        async fn vision_support_override(&self, model: &str) -> Option<bool> {
            (model == "same").then_some(self.0)
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            panic!("no model dispatch")
        }
    }
    #[tokio::test]
    async fn vision_capability_follows_current_provider_after_reload() {
        let inner: Arc<RwLock<Arc<dyn LLMProvider>>> =
            Arc::new(RwLock::new(Arc::new(VisionMetadataProvider(true))));
        let p = ReloadableProvider::new(inner.clone());
        assert_eq!(p.vision_support_override("same").await, Some(true));
        *inner.write().await = Arc::new(VisionMetadataProvider(false));
        assert_eq!(p.vision_support_override("same").await, Some(false));
        assert_eq!(p.vision_support_override("unknown").await, None);
    }
}
