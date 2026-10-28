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
    assert!(Arc::ptr_eq(&allowed, &selected));
    allowed.chat_stream(&[], &[], None, "gold").await.unwrap();
    assert_eq!(inherited_calls.load(Ordering::SeqCst), 1);
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
    assert_eq!(inherited_calls.load(Ordering::SeqCst), 3);
}
