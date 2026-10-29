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
    initial_injected_provider: Option<Arc<dyn LLMProvider>>,
}

impl ReloadableProvider {
    pub fn new(inner: Arc<RwLock<Arc<dyn LLMProvider>>>) -> Self {
        Self::new_with_injected_provider(inner, None)
    }

    pub(crate) fn new_with_injected_provider(
        inner: Arc<RwLock<Arc<dyn LLMProvider>>>,
        initial_injected_provider: Option<Arc<dyn LLMProvider>>,
    ) -> Self {
        Self {
            inner,
            initial_injected_provider,
        }
    }

    async fn current(&self) -> Arc<dyn LLMProvider> {
        self.inner.read().await.clone()
    }
}

#[async_trait]
impl LLMProvider for ReloadableProvider {
    fn legacy_injected_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
        let Some(injected) = self.initial_injected_provider.as_ref() else {
            return Ok(None);
        };
        let current = self.inner.try_read().map_err(|_| {
            bamboo_llm::LLMError::Api(
                "Provider is being reconfigured; retry background work".into(),
            )
        })?;
        Ok(Arc::ptr_eq(&*current, injected).then(|| current.clone()))
    }

    fn queued_provider_snapshot(&self) -> Result<Option<Arc<dyn LLMProvider>>> {
        let current = self
            .inner
            .try_read()
            .map_err(|_| {
                bamboo_llm::LLMError::Api(
                    "Provider is being reconfigured; retry the queued evaluation".into(),
                )
            })?
            .clone();
        Ok(Some(bamboo_llm::provider::queued_snapshot_for(current)?))
    }

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
        // Observe only the main IR request, after selecting the concrete Arc
        // and before forwarding that same Arc. Other purposes cannot overwrite
        // an attempt's receipt. No task-local scope preserves SDK behavior.
        if options.and_then(|options| options.request_purpose.as_deref()) == Some("agent_loop") {
            let _ = bamboo_llm::provider::observe_main_dispatch(provider.clone());
        }
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
    async fn injected_background_snapshot_is_fixed_and_stops_after_reload() {
        let a: Arc<dyn LLMProvider> = Arc::new(FootprintProvider);
        let b: Arc<dyn LLMProvider> = Arc::new(FootprintProvider);
        let inner = Arc::new(RwLock::new(a.clone()));
        let wrapper =
            ReloadableProvider::new_with_injected_provider(inner.clone(), Some(a.clone()));
        let fixed = wrapper
            .legacy_injected_provider_snapshot()
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&fixed, &a));
        let guard = inner.write().await;
        assert!(wrapper.legacy_injected_provider_snapshot().is_err());
        drop(guard);
        *inner.write().await = b;
        assert!(wrapper
            .legacy_injected_provider_snapshot()
            .unwrap()
            .is_none());
        assert!(Arc::ptr_eq(&fixed, &a));
        assert!(ReloadableProvider::new(inner)
            .legacy_injected_provider_snapshot()
            .unwrap()
            .is_none());
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

    #[tokio::test]
    async fn queued_snapshot_tracks_the_actual_reloadable_provider() {
        use bamboo_llm::provider::{queued_identity_for, queued_snapshot_for, QueuedRouteIdentity};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Capture(Arc<AtomicUsize>);
        #[async_trait]
        impl LLMProvider for Capture {
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> Result<LLMStream> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        fn same(left: QueuedRouteIdentity, right: QueuedRouteIdentity) -> bool {
            match (left, right) {
                (QueuedRouteIdentity::Stable(a), QueuedRouteIdentity::Stable(b)) => {
                    Arc::ptr_eq(&a, &b)
                }
                _ => false,
            }
        }

        let a_calls = Arc::new(AtomicUsize::new(0));
        let b_calls = Arc::new(AtomicUsize::new(0));
        let a: Arc<dyn LLMProvider> = Arc::new(Capture(a_calls.clone()));
        let b: Arc<dyn LLMProvider> = Arc::new(Capture(b_calls.clone()));
        let inner = Arc::new(RwLock::new(a));
        let reloadable: Arc<dyn LLMProvider> = Arc::new(ReloadableProvider::new(inner.clone()));
        let first = queued_snapshot_for(reloadable.clone()).unwrap();
        let unchanged = queued_snapshot_for(reloadable.clone()).unwrap();
        assert!(same(
            queued_identity_for(&first),
            queued_identity_for(&unchanged)
        ));

        *inner.write().await = b;
        let replaced = queued_snapshot_for(reloadable.clone()).unwrap();
        assert!(!same(
            queued_identity_for(&first),
            queued_identity_for(&replaced)
        ));
        // A frozen dispatch handle cannot retarget to B after it was resolved.
        let mut stream = unchanged
            .chat_stream(&[], &[], None, "synthetic")
            .await
            .unwrap();
        assert!(futures::StreamExt::next(&mut stream).await.is_none());
        assert_eq!(a_calls.load(Ordering::SeqCst), 1);
        assert_eq!(b_calls.load(Ordering::SeqCst), 0);

        let _guard = inner.write().await;
        assert!(
            queued_snapshot_for(reloadable).is_err(),
            "busy reload must fail closed"
        );
    }

    #[tokio::test]
    async fn reloadable_snapshot_shares_real_auxiliary_semaphore_with_original() {
        use bamboo_domain::{ProviderModelRef, ReasoningEffort};
        use bamboo_engine::runtime::runner::test_acquire_auxiliary_budget;
        use bamboo_llm::{provider::queued_snapshot_for, ResolvedModel};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        struct Capture(Arc<AtomicUsize>);
        #[async_trait]
        impl LLMProvider for Capture {
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> Result<LLMStream> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let raw: Arc<dyn LLMProvider> = Arc::new(Capture(calls.clone()));
        let shared = Arc::new(RwLock::new(raw));
        let live_a: Arc<dyn LLMProvider> = Arc::new(ReloadableProvider::new(shared.clone()));
        let live_b: Arc<dyn LLMProvider> = Arc::new(ReloadableProvider::new(shared));
        let reference =
            ProviderModelRef::new("fixture", "model-a").with_reasoning_effort(ReasoningEffort::Low);
        let with_reasoning = ResolvedModel::from_ref(live_a.clone(), &reference).provider;
        for (index, original) in [live_a.clone(), with_reasoning].into_iter().enumerate() {
            let pinned = queued_snapshot_for(original.clone()).expect("snapshot");
            let original_key = original
                .resource_allocation_identity()
                .unwrap_or(Arc::as_ptr(&original) as *const () as usize);
            assert_eq!(pinned.resource_allocation_identity(), Some(original_key));
            let model = format!("budget-fixture-{index}");
            let permit = test_acquire_auxiliary_budget(&original, &model, 1).await;
            let mut waiting = Box::pin(test_acquire_auxiliary_budget(&pinned, &model, 1));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), waiting.as_mut())
                    .await
                    .is_err(),
                "the snapshot must wait on the original provider's sole permit"
            );
            assert_eq!(calls.load(Ordering::SeqCst), index);
            drop(permit);
            let _permit = tokio::time::timeout(Duration::from_secs(2), waiting)
                .await
                .unwrap();
            let mut stream = pinned.chat_stream(&[], &[], None, &model).await.unwrap();
            assert!(futures::StreamExt::next(&mut stream).await.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), index + 1);
        }
        // Different outer wrappers over one lock keep their existing distinct
        // budget keys; freezing one must not merge them.
        assert_ne!(
            Arc::as_ptr(&live_a) as *const () as usize,
            Arc::as_ptr(&live_b) as *const () as usize
        );
        let frozen_a = queued_snapshot_for(live_a.clone()).unwrap();
        let frozen_b = queued_snapshot_for(live_b.clone()).unwrap();
        assert_ne!(
            frozen_a.resource_allocation_identity(),
            frozen_b.resource_allocation_identity()
        );
        let weak = Arc::downgrade(&live_a);
        drop(live_a);
        assert!(
            weak.upgrade().is_some(),
            "the snapshot must keep its identity allocation alive"
        );
    }
}

#[cfg(test)]
mod main_dispatch_observation_tests {
    use super::*;
    use bamboo_llm::provider::{scope_main_dispatch, MainDispatchObserver};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Capture {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LLMProvider for Capture {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn chat_stream_ir(
            &self,
            _: &PromptIR,
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
            _: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[tokio::test]
    async fn observed_main_dispatch_forwards_the_same_selected_arc_after_swap() {
        let a = Arc::new(Capture::default());
        let b = Arc::new(Capture::default());
        let selected_a: Arc<dyn LLMProvider> = a.clone();
        let selected_b: Arc<dyn LLMProvider> = b.clone();
        let current = Arc::new(RwLock::new(selected_a.clone()));
        let reloadable = ReloadableProvider::new(current.clone());
        let seen = Arc::new(Mutex::new(Vec::<Arc<dyn LLMProvider>>::new()));
        let observer_seen = seen.clone();
        let observer_current = current.clone();
        let observer: MainDispatchObserver = Arc::new(move |selected| {
            observer_seen.lock().unwrap().push(selected);
            *observer_current
                .try_write()
                .expect("selection read lock must be released") = selected_b.clone();
        });
        let options = LLMRequestOptions {
            request_purpose: Some("agent_loop".into()),
            ..Default::default()
        };
        let _ = scope_main_dispatch(
            observer,
            reloadable.chat_stream_ir(&PromptIR::default(), &[], None, "main", Some(&options)),
        )
        .await
        .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(Arc::ptr_eq(&seen.lock().unwrap()[0], &selected_a));
        assert_eq!(a.calls.load(Ordering::SeqCst), 1);
        assert_eq!(b.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn non_main_and_unscoped_requests_do_not_publish_an_attempt_receipt() {
        let provider = Arc::new(Capture::default());
        let current: Arc<RwLock<Arc<dyn LLMProvider>>> = Arc::new(RwLock::new(provider.clone()));
        let reloadable = ReloadableProvider::new(current);
        let seen = Arc::new(AtomicUsize::new(0));
        let observer_seen = seen.clone();
        let observer: MainDispatchObserver = Arc::new(move |_| {
            observer_seen.fetch_add(1, Ordering::SeqCst);
        });
        let gold = LLMRequestOptions {
            request_purpose: Some("gold_evaluation".into()),
            ..Default::default()
        };
        scope_main_dispatch(observer, async {
            let _ = reloadable
                .chat_stream_ir(&PromptIR::default(), &[], None, "gold", Some(&gold))
                .await
                .unwrap();
            let _ = reloadable
                .chat_stream(&[], &[], None, "ordinary")
                .await
                .unwrap();
            let _ = reloadable
                .chat_stream_with_options(&[], &[], None, "ordinary", Some(&gold))
                .await
                .unwrap();
        })
        .await;
        let main = LLMRequestOptions {
            request_purpose: Some("agent_loop".into()),
            ..Default::default()
        };
        let _ = reloadable
            .chat_stream_ir(&PromptIR::default(), &[], None, "main", Some(&main))
            .await
            .unwrap();
        assert_eq!(seen.load(Ordering::SeqCst), 0);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    }
}
