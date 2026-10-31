use super::*;
use bamboo_agent_core::{Message, ToolSchema};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Capture(AtomicUsize);
#[async_trait::async_trait]
impl LLMProvider for Capture {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::empty()))
    }
}
struct Alias(Arc<dyn LLMProvider>);
#[async_trait::async_trait]
impl LLMProvider for Alias {
    fn resource_allocation_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.0) as *const () as usize)
    }
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.chat_stream(messages, tools, max, model).await
    }
}

async fn fixture(provider: Arc<dyn LLMProvider>) -> (tempfile::TempDir, Agent) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
            .await
            .unwrap(),
    );
    let storage: Arc<dyn bamboo_agent_core::storage::Storage> = store.clone();
    let persistence: Arc<dyn RuntimeSessionPersistence> =
        Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            dir.path().join("metrics.db"),
        )),
        7,
    );
    let agent = Agent::builder()
        .storage(storage)
        .persistence(persistence)
        .attachment_reader(store)
        .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
        .metrics_collector(metrics)
        .config(Arc::new(tokio::sync::RwLock::new(
            bamboo_llm::Config::default(),
        )))
        .provider(provider)
        .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::new()))
        .build()
        .unwrap();
    (dir, agent)
}

#[tokio::test]
async fn child_gold_composes_inherited_policy_and_rejects_unproven_targets() {
    use crate::runtime::config::GoldTargetResolver;
    let a = Arc::new(Capture::default());
    let b = Arc::new(Capture::default());
    let selected: Arc<dyn LLMProvider> = a.clone();
    let other: Arc<dyn LLMProvider> = b.clone();
    let (_dir, base) = fixture(selected.clone()).await;
    let inherited_calls = Arc::new(AtomicUsize::new(0));
    let managed_calls = Arc::new(AtomicUsize::new(0));
    let old_count = inherited_calls.clone();
    let inherited: GoldTargetResolver = Arc::new(move |model, delegate| {
        assert_eq!(model, "gold");
        old_count.fetch_add(1, Ordering::SeqCst);
        delegate
    });
    let managed_count = managed_calls.clone();
    let managed: GoldTargetResolver = Arc::new(move |_model, delegate| {
        managed_count.fetch_add(1, Ordering::SeqCst);
        delegate
    });
    let adapter = base.with_gold_target_resolver(inherited.clone());
    let child = adapter.with_child_resume_gold_target_resolver(selected.clone(), managed.clone());
    // Cloning for this resume does not replace the Root adapter's own policy.
    assert!(Arc::ptr_eq(
        adapter.runtime.gold_target_resolver.as_ref().unwrap(),
        &inherited
    ));
    let allowed = child.runtime.gold_target_resolver.as_ref().unwrap()("gold", selected.clone());
    allowed.chat_stream(&[], &[], None, "gold").await.unwrap();
    // The wrapper rechecks the live policy at actual dispatch.
    assert_eq!(inherited_calls.load(Ordering::SeqCst), 2);
    assert_eq!(managed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);

    // Another account AND an ostensibly transparent wrapper are unproved.
    for candidate in [
        other,
        Arc::new(Alias(selected.clone())) as Arc<dyn LLMProvider>,
    ] {
        let previous = inherited_calls.clone();
        let replacement = candidate.clone();
        let policy: GoldTargetResolver = Arc::new(move |_, _| {
            previous.fetch_add(1, Ordering::SeqCst);
            replacement.clone()
        });
        let child = base
            .with_gold_target_resolver(policy)
            .with_child_resume_gold_target_resolver(selected.clone(), managed.clone());
        let rejected =
            child.runtime.gold_target_resolver.as_ref().unwrap()("gold", selected.clone());
        assert!(rejected.known_pre_dispatch_error().is_some());
        assert!(!rejected.local_dispatch_refused());
        assert!(rejected.chat_stream(&[], &[], None, "gold").await.is_err());
        assert!(rejected.local_dispatch_refused());
        assert_eq!(managed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(a.0.load(Ordering::SeqCst), 1);
        assert_eq!(b.0.load(Ordering::SeqCst), 0);
    }
    assert_eq!(inherited_calls.load(Ordering::SeqCst), 4);
}

// A trusted wrapper explicitly attests its fixed inner route. Its own behavior
// is retained; merely copying the queue or allocation identity is not proof.
struct Transparent {
    inner: Arc<dyn LLMProvider>,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl LLMProvider for Transparent {
    fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
        Some(self.inner.clone())
    }
    fn resource_allocation_identity(&self) -> Option<usize> {
        self.inner.resource_allocation_identity()
    }
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.chat_stream(messages, tools, max, model).await
    }
}

fn transparent(inner: Arc<dyn LLMProvider>, calls: Arc<AtomicUsize>) -> Arc<dyn LLMProvider> {
    Arc::new(Transparent { inner, calls })
}

#[tokio::test]
async fn child_policy_keeps_attested_wrappers_and_exact_budgeted_delegate() {
    use crate::runtime::config::GoldTargetResolver;
    let a = Arc::new(Capture::default());
    let b = Arc::new(Capture::default());
    let raw: Arc<dyn LLMProvider> = a.clone();
    let budget_reservations = Arc::new(AtomicUsize::new(0));
    // Synthetic stand-in: its Arc is the budget-decorated delegate supplied
    // by execute_bound, not an inference from queue/allocation identity.
    let budgeted = transparent(raw.clone(), budget_reservations.clone());
    let wrapper_calls = Arc::new(AtomicUsize::new(0));
    let calls = wrapper_calls.clone();
    let inherited: GoldTargetResolver = Arc::new(move |_, delegate| {
        transparent(transparent(delegate, calls.clone()), calls.clone())
    });
    let resolver = compose_child_gold_policy(Some(inherited), Arc::new(|_, delegate| delegate));
    let selected = resolver("gold", budgeted.clone());
    selected.chat_stream(&[], &[], None, "gold").await.unwrap();
    assert_eq!(wrapper_calls.load(Ordering::SeqCst), 2);
    assert_eq!(budget_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);

    // A wrapper around raw A does not reach the exact budgeted Arc, even
    // though it ends at the same physical provider.
    let replacement = transparent(raw, wrapper_calls.clone());
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, _| replacement.clone())),
        Arc::new(|_, delegate| delegate),
    );
    let denied = resolver("gold", budgeted);
    assert!(denied.known_pre_dispatch_error().is_some());
    assert!(denied.chat_stream(&[], &[], None, "gold").await.is_err());
    assert_eq!(budget_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn child_policy_does_not_trust_forged_queue_or_allocation_identity() {
    use bamboo_llm::provider::QueuedRouteIdentity;
    struct Spoof {
        identity: Arc<dyn LLMProvider>,
        actual: Arc<dyn LLMProvider>,
    }
    #[async_trait::async_trait]
    impl LLMProvider for Spoof {
        fn queued_route_identity(&self) -> QueuedRouteIdentity {
            QueuedRouteIdentity::Stable(self.identity.clone())
        }
        fn resource_allocation_identity(&self) -> Option<usize> {
            Some(Arc::as_ptr(&self.identity) as *const () as usize)
        }
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.actual.chat_stream(messages, tools, max, model).await
        }
    }
    let a = Arc::new(Capture::default());
    let b = Arc::new(Capture::default());
    let selected: Arc<dyn LLMProvider> = a.clone();
    let spoof: Arc<dyn LLMProvider> = Arc::new(Spoof {
        identity: selected.clone(),
        actual: b.clone(),
    });
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, _| spoof.clone())),
        Arc::new(|_, delegate| delegate),
    );
    let denied = resolver("gold", selected);
    assert!(denied.known_pre_dispatch_error().is_some());
    assert!(denied.chat_stream(&[], &[], None, "gold").await.is_err());
    assert_eq!(a.0.load(Ordering::SeqCst), 0);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn child_policy_preserves_explicit_local_auth_refusal() {
    struct Denied {
        inner: Arc<dyn LLMProvider>,
    }
    #[async_trait::async_trait]
    impl LLMProvider for Denied {
        fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
            Some(self.inner.clone())
        }
        fn known_pre_dispatch_error(&self) -> Option<LLMError> {
            Some(LLMError::Auth("synthetic policy denied".into()))
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("a local policy refusal must not be dispatched")
        }
    }
    let selected = Arc::new(Capture::default());
    let rejected: Arc<dyn LLMProvider> = Arc::new(Denied {
        inner: selected.clone(),
    });
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, _| rejected.clone())),
        Arc::new(|_, delegate| delegate),
    );
    let denied = resolver("gold", selected.clone());
    assert!(matches!(
        denied.known_pre_dispatch_error(),
        Some(LLMError::Auth(_))
    ));
    assert!(matches!(
        denied.chat_stream(&[], &[], None, "gold").await,
        Err(LLMError::Auth(_))
    ));
    assert!(denied.local_dispatch_refused());
    assert_eq!(selected.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn child_policy_refuses_stale_inherited_target_without_dispatch() {
    use std::sync::atomic::AtomicBool;
    struct ReusedWrapper {
        inner: Arc<dyn LLMProvider>,
        refused: AtomicBool,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl LLMProvider for ReusedWrapper {
        fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
            Some(self.inner.clone())
        }
        fn local_dispatch_refused(&self) -> bool {
            self.refused.load(Ordering::SeqCst)
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(LLMError::Api("synthetic remote failure".into()))
        }
    }
    for stale_at_selection in [true, false] {
        let selected = Arc::new(Capture::default());
        let reused = Arc::new(ReusedWrapper {
            inner: selected.clone(),
            refused: AtomicBool::new(stale_at_selection),
            calls: AtomicUsize::new(0),
        });
        let current: Arc<dyn LLMProvider> = reused.clone();
        let resolver = compose_child_gold_policy(
            Some(Arc::new(move |_, _| current.clone())),
            Arc::new(|_, delegate| delegate),
        );
        let provider = resolver("gold", selected.clone());
        if !stale_at_selection {
            assert!(provider.known_pre_dispatch_error().is_none());
            reused.refused.store(true, Ordering::SeqCst);
        }
        assert!(provider.known_pre_dispatch_error().is_some());
        assert!(provider.chat_stream(&[], &[], None, "gold").await.is_err());
        assert!(provider.local_dispatch_refused());
        assert_eq!(reused.calls.load(Ordering::SeqCst), 0);
        assert_eq!(selected.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn child_policy_preserves_reasoning_wrapper_options() {
    use bamboo_domain::{ProviderModelRef, ReasoningEffort};
    use bamboo_llm::{LLMRequestOptions, ResolvedModel};
    use std::sync::Mutex;
    #[derive(Default)]
    struct EffortCapture(Mutex<Vec<Option<ReasoningEffort>>>);
    #[async_trait::async_trait]
    impl LLMProvider for EffortCapture {
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.chat_stream_with_options(messages, tools, max, model, None)
                .await
        }
        async fn chat_stream_with_options(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            self.0
                .lock()
                .unwrap()
                .push(options.and_then(|value| value.reasoning_effort));
            Ok(Box::pin(futures::stream::empty()))
        }
    }
    let capture = Arc::new(EffortCapture::default());
    let budgeted: Arc<dyn LLMProvider> = capture.clone();
    let inherited = Arc::new(|model: &str, delegate: Arc<dyn LLMProvider>| {
        let reference =
            ProviderModelRef::new("synthetic", model).with_reasoning_effort(ReasoningEffort::Xhigh);
        ResolvedModel::from_ref(delegate, &reference).provider
    });
    let resolver = compose_child_gold_policy(Some(inherited), Arc::new(|_, delegate| delegate));
    let selected = resolver("gold", budgeted);
    selected
        .chat_stream_with_options(&[], &[], None, "gold", None)
        .await
        .unwrap();
    assert_eq!(
        *capture.0.lock().unwrap(),
        vec![Some(ReasoningEffort::Xhigh)]
    );
}

#[tokio::test]
async fn child_policy_uses_current_attested_wrapper_at_dispatch() {
    use std::sync::atomic::AtomicBool;
    let a = Arc::new(Capture::default());
    let selected: Arc<dyn LLMProvider> = a.clone();
    let changed = Arc::new(AtomicBool::new(false));
    let state = changed.clone();
    let wrapper_calls = Arc::new(AtomicUsize::new(0));
    let calls = wrapper_calls.clone();
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, delegate| {
            if state.load(Ordering::SeqCst) {
                transparent(delegate, calls.clone())
            } else {
                delegate
            }
        })),
        Arc::new(|_, delegate| delegate),
    );
    let provider = resolver("gold", selected);
    assert!(provider.known_pre_dispatch_error().is_none());
    changed.store(true, Ordering::SeqCst);
    provider.chat_stream(&[], &[], None, "gold").await.unwrap();
    assert_eq!(wrapper_calls.load(Ordering::SeqCst), 1);
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
}

#[test]
fn child_policy_uses_budgeted_allocation_identity_for_fresh_wrappers() {
    use bamboo_llm::ProviderRegistry;
    use std::collections::HashMap;
    struct ExplicitBudget {
        inner: Arc<dyn LLMProvider>,
    }
    #[async_trait::async_trait]
    impl LLMProvider for ExplicitBudget {
        fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
            Some(self.inner.clone())
        }
        fn resource_allocation_identity(&self) -> Option<usize> {
            Some(77)
        }
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.inner.chat_stream(messages, tools, max, model).await
        }
    }
    let raw: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let explicit: Arc<dyn LLMProvider> = Arc::new(ExplicitBudget { inner: raw.clone() });
    for (selected, expected_identity) in [
        (raw.clone(), Arc::as_ptr(&raw) as *const () as usize),
        (explicit, 77),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let inherited: crate::runtime::config::GoldTargetResolver =
            Arc::new(move |_, delegate| transparent(delegate, calls.clone()));
        let config = Arc::new(tokio::sync::RwLock::new(bamboo_llm::Config::default()));
        let registry = Arc::new(ProviderRegistry::new(HashMap::new(), "fixture".into()));
        let expected = selected.clone();
        let managed: crate::runtime::config::GoldTargetResolver =
            Arc::new(move |model, delegate| {
                bamboo_llm::gold_admission::guarded_gold_target(
                    config.clone(),
                    registry.clone(),
                    bamboo_domain::ProviderModelRef::new("fixture", model),
                    expected.clone(),
                    delegate,
                )
            });
        let resolver = compose_child_gold_policy(Some(inherited), managed);
        let first = resolver("gold", selected.clone());
        let second = resolver("gold", selected);
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(
            first.resource_allocation_identity(),
            Some(expected_identity)
        );
        assert_eq!(
            second.resource_allocation_identity(),
            Some(expected_identity)
        );
    }
}

#[tokio::test]
async fn child_policy_rejects_changed_explicit_allocation_identity() {
    use std::sync::atomic::AtomicBool;
    struct Allocated {
        inner: Arc<dyn LLMProvider>,
        identity: usize,
    }
    #[async_trait::async_trait]
    impl LLMProvider for Allocated {
        fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
            Some(self.inner.clone())
        }
        fn resource_allocation_identity(&self) -> Option<usize> {
            Some(self.identity)
        }
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.inner.chat_stream(messages, tools, max, model).await
        }
    }
    let a = Arc::new(Capture::default());
    let changed = Arc::new(AtomicBool::new(false));
    let state = changed.clone();
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, delegate| {
            Arc::new(Allocated {
                inner: delegate,
                identity: if state.load(Ordering::SeqCst) { 2 } else { 1 },
            }) as Arc<dyn LLMProvider>
        })),
        Arc::new(|_, delegate| delegate),
    );
    let provider = resolver("gold", a.clone());
    assert!(provider.known_pre_dispatch_error().is_none());
    changed.store(true, Ordering::SeqCst);
    assert!(provider.chat_stream(&[], &[], None, "gold").await.is_err());
    assert!(provider.local_dispatch_refused());
    assert_eq!(a.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn child_policy_rejects_cycle_and_excessive_wrapper_depth() {
    use std::sync::Mutex;
    struct Cycle(Mutex<Option<Arc<dyn LLMProvider>>>);
    #[async_trait::async_trait]
    impl LLMProvider for Cycle {
        fn fixed_gold_policy_delegate(&self) -> Option<Arc<dyn LLMProvider>> {
            self.0.lock().unwrap().clone()
        }
        fn local_dispatch_refused(&self) -> bool {
            let next = self.0.lock().unwrap().clone();
            next.is_some_and(|provider| provider.local_dispatch_refused())
        }
        fn known_pre_dispatch_error(&self) -> Option<LLMError> {
            let next = self.0.lock().unwrap().clone();
            next.and_then(|provider| provider.known_pre_dispatch_error())
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("cyclic route must never dispatch")
        }
    }
    let selected: Arc<dyn LLMProvider> = Arc::new(Capture::default());
    let cycle = Arc::new(Cycle(Mutex::new(None)));
    *cycle.0.lock().unwrap() = Some(cycle.clone());
    let replacement: Arc<dyn LLMProvider> = cycle.clone();
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, _| replacement.clone())),
        Arc::new(|_, delegate| delegate),
    );
    assert!(resolver("gold", selected.clone())
        .known_pre_dispatch_error()
        .is_some());
    let use_cycle = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let switch = use_cycle.clone();
    let replacement: Arc<dyn LLMProvider> = cycle.clone();
    let dynamic = compose_child_gold_policy(
        Some(Arc::new(move |_, delegate| {
            if switch.load(Ordering::SeqCst) {
                replacement.clone()
            } else {
                delegate
            }
        })),
        Arc::new(|_, delegate| delegate),
    );
    let current = dynamic("gold", selected.clone());
    assert!(current.known_pre_dispatch_error().is_none());
    use_cycle.store(true, Ordering::SeqCst);
    assert!(current.known_pre_dispatch_error().is_some());
    assert!(current.chat_stream(&[], &[], None, "gold").await.is_err());
    assert!(current.local_dispatch_refused());
    *cycle.0.lock().unwrap() = None;

    let calls = Arc::new(AtomicUsize::new(0));
    let mut deep = selected.clone();
    for _ in 0..32 {
        deep = transparent(deep, calls.clone());
    }
    assert!(gold_policy_reaches_delegate(&deep, &selected));
    deep = transparent(deep, calls.clone());
    let resolver = compose_child_gold_policy(
        Some(Arc::new(move |_, _| deep.clone())),
        Arc::new(|_, delegate| delegate),
    );
    assert!(resolver("gold", selected)
        .known_pre_dispatch_error()
        .is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn child_policy_rechecks_after_gold_wait_and_emits_cancelled() {
    use crate::runtime::config::{GoldConfig, GoldTargetResolver};
    use crate::runtime::gold_evaluation::{
        evaluate_gold_with_dispatch, GoldEvalFrame, GoldEvaluationError,
    };
    use crate::runtime::stream::handler::StreamTimeoutContext;
    use bamboo_agent_core::{AgentEvent, GoldCheckpoint};
    use std::sync::atomic::AtomicBool;
    use tokio::sync::{mpsc, Semaphore};

    let a = Arc::new(Capture::default());
    let b = Arc::new(Capture::default());
    let budget_reservations = Arc::new(AtomicUsize::new(0));
    let budgeted = transparent(a.clone(), budget_reservations.clone());
    let enabled = Arc::new(AtomicBool::new(true));
    let state = enabled.clone();
    let replacement: Arc<dyn LLMProvider> = b.clone();
    let inherited: GoldTargetResolver = Arc::new(move |_, delegate| {
        if state.load(Ordering::SeqCst) {
            delegate
        } else {
            replacement.clone()
        }
    });
    let resolver = compose_child_gold_policy(Some(inherited), Arc::new(|_, delegate| delegate));
    let provider = resolver("gold", budgeted);
    let (tx, mut rx) = mpsc::channel(8);
    let gate = Arc::new(Semaphore::new(0));
    let acquire = gate.clone();
    let task = tokio::spawn(async move {
        let session = Session::new("synthetic-child-gold", "main");
        let config = GoldConfig {
            enabled: true,
            ..Default::default()
        };
        let frame = GoldEvalFrame {
            event_tx: &tx,
            session_id: "synthetic-child-gold",
            model: "gold",
            timeout_context: StreamTimeoutContext::new(Default::default(), None, Some("gold")),
            reasoning_effort: None,
            checkpoint: GoldCheckpoint::Terminal,
            iteration: 1,
        };
        evaluate_gold_with_dispatch(&session, None, &config, provider, &frame, async move {
            acquire.acquire_owned().await.unwrap()
        })
        .await
    });
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap(),
        Some(AgentEvent::GoldEvaluationStarted { .. })
    ));
    enabled.store(false, Ordering::SeqCst);
    gate.add_permits(1);
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(GoldEvaluationError::Routing(_))));
    assert!(
        matches!(rx.recv().await, Some(AgentEvent::GoldEvaluationCancelled { reason, .. }) if reason == "route_rejected")
    );
    assert_eq!(gate.available_permits(), 1);
    assert_eq!(budget_reservations.load(Ordering::SeqCst), 0);
    assert_eq!(a.0.load(Ordering::SeqCst), 0);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);
}
