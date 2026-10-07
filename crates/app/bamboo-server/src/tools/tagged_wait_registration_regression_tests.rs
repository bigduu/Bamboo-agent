//! #1312 deterministic tagged-wait race evidence. Test-only, no production hook.
//! Each barrier returns the exact real snapshot read before a competing writer.
//! No provider, scheduler, watchdog, sleep, or synthetic completion is used.

use super::*;
use bamboo_domain::{AgentRuntimeState, WaitingForChildrenState};
use bamboo_engine::execution::{ChildCompletion, ChildCompletionHandler};
use bamboo_storage::LockedSessionStore;

#[derive(Clone, Copy)]
enum ReadBoundary {
    Parent,
    ChildStatuses,
    ChildWaitCommit,
}

struct ReadBarrierStorage {
    inner: Arc<dyn Storage>,
    parent_id: String,
    boundary: ReadBoundary,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ReadBarrierStorage {
    fn new(inner: Arc<dyn Storage>, parent_id: &str, boundary: ReadBoundary) -> Arc<Self> {
        Arc::new(Self {
            inner,
            parent_id: parent_id.into(),
            boundary,
            armed: AtomicBool::new(true),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }

    async fn stop_after_read(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[async_trait::async_trait]
impl Storage for ReadBarrierStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        let captured = self.inner.load_session(id).await?;
        if id == self.parent_id && matches!(self.boundary, ReadBoundary::Parent) {
            self.stop_after_read().await;
        }
        Ok(captured)
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn save_runtime_state(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_runtime_state(session).await
    }
    async fn load_runtime_control_plane(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_control_plane(id).await
    }
    fn supports_atomic_child_wait_control_plane(&self) -> bool {
        self.inner.supports_atomic_child_wait_control_plane()
    }
    async fn compare_exchange_child_wait_control_plane(
        &self,
        expected: &Session,
        updated: &mut Session,
        runtime_only: bool,
        publish: bamboo_domain::RootActorRuntimePublisher,
    ) -> std::io::Result<bool> {
        if matches!(self.boundary, ReadBoundary::ChildWaitCommit) {
            self.stop_after_read().await;
        }
        self.inner
            .compare_exchange_child_wait_control_plane(expected, updated, runtime_only, publish)
            .await
    }
    async fn register_child_wait_control_plane(
        &self,
        expected: &Session,
        batch: &[(String, Option<String>)],
        policy: ChildWaitPolicy,
        check_terminal: bool,
        publish: bamboo_domain::RootActorRuntimePublisher,
    ) -> std::io::Result<(Session, usize)> {
        self.inner
            .register_child_wait_control_plane(expected, batch, policy, check_terminal, publish)
            .await
    }
    async fn list_child_run_statuses(
        &self,
        id: &str,
    ) -> std::io::Result<Vec<(String, Option<String>)>> {
        let captured = self.inner.list_child_run_statuses(id).await?;
        if id == self.parent_id && matches!(self.boundary, ReadBoundary::ChildStatuses) {
            self.stop_after_read().await;
        }
        Ok(captured)
    }
}

fn adapter_with_storage(
    h: &TestHarness,
    store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
) -> Arc<ChildSessionAdapter> {
    Arc::new(ChildSessionAdapter {
        session_store: store,
        storage,
        persistence,
        session_messenger: h.adapter.session_messenger.clone(),
        scheduler: h.adapter.scheduler.clone(),
        sessions_cache: h.adapter.sessions_cache.clone(),
        agent_runners: h.agent_runners.clone(),
        session_event_senders: h.adapter.session_event_senders.clone(),
        subagent_model_resolver: None,
        config: h.adapter.config.clone(),
        project_store: h.adapter.project_store.clone(),
        workspace_resolver: h.adapter.workspace_resolver.clone(),
        parent_wait_slots: Arc::default(),
        recovered_launches: Arc::default(),
    })
}

async fn coordinator_with_storage(
    h: &TestHarness,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
) -> Arc<bamboo_engine::ChildCompletionCoordinator> {
    // ActiveNotified records the policy's activation without running a model.
    h.activation
        .force_disposition(SessionActivationDisposition::ActiveNotified);
    let provider: Arc<dyn LLMProvider> = Arc::new(NoopProvider);
    let tools: Arc<dyn ToolExecutor> = Arc::new(NoopToolExecutor);
    let home = h.workspace_path.parent().unwrap().to_path_buf();
    let metrics = MetricsCollector::spawn(
        Arc::new(SqliteMetricsStorage::new(home.join("proof-metrics.db"))),
        7,
    );
    let agent = Arc::new(
        bamboo_engine::Agent::builder()
            .storage(storage.clone())
            .persistence(persistence.clone())
            .session_inbox(h.session_inbox.clone())
            .activation_router(h.activation_router.clone())
            .session_messenger(h.adapter.session_messenger.clone().unwrap())
            .attachment_reader(h.adapter.session_store.clone())
            .skill_manager(Arc::new(SkillManager::new()))
            .metrics_collector(metrics)
            .config(h.adapter.config.clone())
            .provider(provider.clone())
            .default_tools(tools.clone())
            .build()
            .unwrap(),
    );
    let registry = Arc::new(bamboo_llm::ProviderRegistry::new(
        HashMap::from([("test".into(), provider)]),
        "test".into(),
    ));
    let coordinator = Arc::new(bamboo_engine::ChildCompletionCoordinator::new(
        storage,
        persistence,
        h.adapter.sessions_cache.clone(),
        h.agent_runners.clone(),
        h.adapter.session_event_senders.clone(),
        agent,
        h.adapter.config.clone(),
        registry.clone(),
        Arc::new(bamboo_llm::ProviderModelRouter::new(registry)),
        home,
        None,
    ));
    coordinator.set_root_tools(tools).await;
    coordinator
}

fn waiting(session: &Session) -> Option<&WaitingForChildrenState> {
    session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
}

async fn seed_pending_children(h: &TestHarness) {
    let mut a = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    a.set_last_run_status("running");
    h.storage.save_session(&a).await.unwrap();
    let mut b = Session::new_child("proof-child-b", &h.parent_session_id, "gpt-5", "B");
    b.set_last_run_status("running");
    h.storage.save_session(&b).await.unwrap();
}

async fn sealed_completion(h: &TestHarness) -> ChildCompletion {
    sealed_completion_for(h, &h.child_session_id).await
}

async fn sealed_completion_for(h: &TestHarness, child_id: &str) -> ChildCompletion {
    let mut child = h.storage.load_session(child_id).await.unwrap().unwrap();
    child.set_last_run_status("completed");
    child.add_message(Message::assistant("sealed #1312 child A answer", None));
    bamboo_engine::test_utils::prepare_child_completion_source(
        &mut child,
        "proof-child-a-run",
        &Default::default(),
    );
    h.storage.save_session(&child).await.unwrap();
    let committed = h.storage.load_session(&child.id).await.unwrap().unwrap();
    let source = bamboo_engine::test_utils::committed_child_completion_source(&committed);
    assert!(
        source.is_some(),
        "real terminal seal must validate after commit"
    );
    ChildCompletion {
        parent_session_id: h.parent_session_id.clone(),
        child_session_id: child.id,
        status: "completed".into(),
        error: None,
        completed_at: committed.updated_at,
        source,
    }
}

async fn register(
    adapter: &ChildSessionAdapter,
    parent: &str,
    children: &[String],
    policy: ChildWaitPolicy,
    tag: &str,
) {
    assert_eq!(
        adapter
            .register_parent_wait_for_children_tagged(parent, children, policy, tag)
            .await
            .unwrap(),
        children.len(),
    );
}

/// Verify the committed full Session overlay, runtime sidecar, published cache,
/// and a freshly reopened store all agree. session.json alone intentionally
/// keeps its prior runtime fields when the production API saves only runtime.
async fn settled_views(h: &TestHarness, label: &str) -> Session {
    let full = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let sidecar = h
        .storage
        .load_runtime_control_plane(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let cached = h
        .adapter
        .sessions_cache
        .get(&h.parent_session_id)
        .unwrap()
        .read()
        .clone();
    let reopened = SessionStoreV2::new(h.workspace_path.parent().unwrap().into())
        .await
        .unwrap();
    let restarted = reopened
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    for (name, other) in [
        ("sidecar", sidecar),
        ("cache", cached),
        ("restart", restarted),
    ] {
        assert_eq!(
            full.agent_runtime_state, other.agent_runtime_state,
            "{label}: {name}"
        );
        assert_eq!(
            full.metadata.get("runtime.suspend_reason"),
            other.metadata.get("runtime.suspend_reason"),
            "{label}: {name} suspension",
        );
        if let Some(mirror) = other.metadata.get("agent.runtime.state") {
            let mirror: AgentRuntimeState = serde_json::from_str(mirror).unwrap();
            assert_eq!(
                Some(mirror),
                other.agent_runtime_state,
                "{label}: {name} mirror"
            );
        }
    }
    eprintln!(
        "#1312 {label}: wait={:?}; inbox={:?}; activations={}",
        waiting(&full),
        h.session_inbox.inspect(&h.parent_session_id).await.unwrap(),
        h.activation.calls.load(Ordering::SeqCst),
    );
    full
}

async fn terminal_before_registration(policy: ChildWaitPolicy, explicit: bool) {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    // Only A is active for the default-to-active-targets variant.
    h.storage.delete_session("proof-child-b").await.unwrap();
    let barrier = ReadBarrierStorage::new(
        h.storage.clone(),
        &h.parent_session_id,
        ReadBoundary::ChildStatuses,
    );
    let adapter = adapter_with_storage(
        &h,
        h.adapter.session_store.clone(),
        barrier.clone(),
        h.adapter.persistence.clone(),
    );
    let tool = SubAgentTool::new(adapter.clone(), adapter);
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    let mut args = json!({"action":"wait", "wait_for": policy.as_str()});
    if explicit {
        args["child_session_ids"] = json!([h.child_session_id]);
    }
    let parent_id = h.parent_session_id.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            args,
            subagent_test_ctx(&parent_id, "proof-wait-terminal-window"),
        )
        .await
    });
    barrier.entered.notified().await;
    // Terminal A commits and its sole real callback returns with no Parent wait.
    coordinator
        .on_child_completed(sealed_completion(&h).await)
        .await;
    assert_eq!(
        h.session_inbox
            .inspect(&h.parent_session_id)
            .await
            .unwrap()
            .pending,
        0
    );
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
    barrier.release.notify_one();
    let outcome = task.await.unwrap().unwrap();
    eprintln!("#1312 terminal window tool outcome={outcome:?}; explicit={explicit}");
    let parent = settled_views(&h, "terminal-before-tagged-registration").await;
    assert!(
        waiting(&parent).is_none(),
        "terminal Child's sole callback finished before registration; the tagged wait must not remain armed",
    );
    let ToolOutcome::Completed(result) = outcome else {
        panic!("already-terminal wait must complete immediately");
    };
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "immediate terminal satisfaction must not tell the Parent pipeline to suspend",
    );
}

async fn stale_registration(policy: ChildWaitPolicy, independent: bool) {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        policy,
        "proof-arm-a",
    )
    .await;
    let (store, storage, persistence) = if independent {
        let store = Arc::new(
            SessionStoreV2::new(h.workspace_path.parent().unwrap().into())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        assert!(!Arc::ptr_eq(&persistence, &h.adapter.persistence));
        (store, storage, persistence)
    } else {
        (
            h.adapter.session_store.clone(),
            h.storage.clone(),
            h.adapter.persistence.clone(),
        )
    };
    let barrier = ReadBarrierStorage::new(storage, &h.parent_session_id, ReadBoundary::Parent);
    let adapter = adapter_with_storage(&h, store, barrier.clone(), persistence);
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    let parent_id = h.parent_session_id.clone();
    let task = tokio::spawn(async move {
        register(
            &adapter,
            &parent_id,
            &["proof-child-b".into()],
            policy,
            "proof-arm-b",
        )
        .await;
    });
    barrier.entered.notified().await;
    coordinator
        .on_child_completed(sealed_completion(&h).await)
        .await;
    assert!(waiting(
        &h.storage
            .load_session(&h.parent_session_id)
            .await
            .unwrap()
            .unwrap()
    )
    .is_none());
    barrier.release.notify_one();
    task.await.unwrap();
    let parent = settled_views(
        &h,
        "registration-read-A / completion-clears-A / registration-B-commits",
    )
    .await;
    let wait = waiting(&parent).expect("new live B needs a durable wait");
    assert_eq!(
        wait.child_session_ids,
        vec!["proof-child-b"],
        "completed A must not be resurrected from the stale registration read"
    );
    assert_eq!(
        wait.registered_by_tool_call_id.as_deref(),
        Some("proof-arm-b")
    );
}

async fn stale_completion(policy: ChildWaitPolicy, independent: bool) {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        policy,
        "proof-arm-a",
    )
    .await;
    let first_arm = waiting(
        &h.storage
            .load_session(&h.parent_session_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
    .clone();
    let (storage, persistence): (Arc<dyn Storage>, _) = if independent {
        let store = Arc::new(
            SessionStoreV2::new(h.workspace_path.parent().unwrap().into())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store;
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        assert!(!Arc::ptr_eq(&persistence, &h.adapter.persistence));
        (storage, persistence)
    } else {
        (h.storage.clone(), h.adapter.persistence.clone())
    };
    let barrier = ReadBarrierStorage::new(storage, &h.parent_session_id, ReadBoundary::Parent);
    let coordinator = coordinator_with_storage(&h, barrier.clone(), persistence).await;
    let completion = sealed_completion(&h).await;
    let task = tokio::spawn(async move { coordinator.on_child_completed(completion).await });
    barrier.entered.notified().await;
    register(
        &h.adapter,
        &h.parent_session_id,
        &["proof-child-b".into()],
        policy,
        "proof-arm-b",
    )
    .await;
    let newer = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        waiting(&newer).unwrap().child_session_ids,
        vec![h.child_session_id.clone(), "proof-child-b".into()],
        "B's competing registration must be durable before the stale completion resumes",
    );
    assert_eq!(
        waiting(&newer).unwrap().registered_at,
        first_arm.registered_at
    );
    assert_eq!(
        waiting(&newer).unwrap().registered_by_tool_call_id,
        first_arm.registered_by_tool_call_id
    );
    barrier.release.notify_one();
    task.await.unwrap();
    let parent = settled_views(
        &h,
        "completion-read-A / registration-B-commits / completion-commits",
    )
    .await;
    match policy {
        ChildWaitPolicy::All => {
            let wait =
                waiting(&parent).expect("All must keep B's committed wait while B is running");
            assert!(wait.child_session_ids.contains(&"proof-child-b".into()));
            assert_eq!(wait.registered_at, first_arm.registered_at);
            assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
        }
        ChildWaitPolicy::Any => {
            assert!(
                waiting(&parent).is_none(),
                "Any over the same first arm is legitimately satisfied by A"
            );
            assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
        }
        ChildWaitPolicy::FirstError => unreachable!(),
    }
}

async fn sequential_baselines(policy: ChildWaitPolicy) {
    for complete_first in [false, true] {
        let h = build_test_harness_with_storage(None, None, true).await;
        seed_pending_children(&h).await;
        register(
            &h.adapter,
            &h.parent_session_id,
            &[h.child_session_id.clone()],
            policy,
            "proof-arm-a",
        )
        .await;
        let original = waiting(
            &h.storage
                .load_session(&h.parent_session_id)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .clone();
        let coordinator =
            coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
        if complete_first {
            coordinator
                .on_child_completed(sealed_completion(&h).await)
                .await;
        }
        register(
            &h.adapter,
            &h.parent_session_id,
            &["proof-child-b".into()],
            policy,
            "proof-arm-b",
        )
        .await;
        let registered = h
            .storage
            .load_session(&h.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        let wait = waiting(&registered).unwrap();
        if complete_first {
            assert_eq!(wait.child_session_ids, vec!["proof-child-b"]);
            assert_eq!(
                wait.registered_by_tool_call_id.as_deref(),
                Some("proof-arm-b")
            );
        } else {
            assert_eq!(
                wait.registered_at, original.registered_at,
                "existing first arm must be preserved"
            );
            assert_eq!(
                wait.registered_by_tool_call_id,
                original.registered_by_tool_call_id
            );
            coordinator
                .on_child_completed(sealed_completion(&h).await)
                .await;
        }
        let parent = settled_views(&h, "sequential tagged-wait baseline").await;
        if complete_first || policy == ChildWaitPolicy::All {
            assert!(waiting(&parent)
                .unwrap()
                .child_session_ids
                .contains(&"proof-child-b".into()));
        } else {
            assert!(waiting(&parent).is_none());
        }
    }
}

#[tokio::test]
async fn terminal_between_explicit_status_read_and_tagged_registration_all() {
    terminal_before_registration(ChildWaitPolicy::All, true).await;
}
#[tokio::test]
async fn terminal_between_explicit_status_read_and_tagged_registration_any() {
    terminal_before_registration(ChildWaitPolicy::Any, true).await;
}
#[tokio::test]
async fn terminal_between_active_status_read_and_tagged_registration_all() {
    terminal_before_registration(ChildWaitPolicy::All, false).await;
}
#[tokio::test]
async fn stale_tagged_registration_must_not_resurrect_completed_a_all() {
    stale_registration(ChildWaitPolicy::All, false).await;
}
#[tokio::test]
async fn stale_tagged_registration_must_not_resurrect_completed_a_any() {
    stale_registration(ChildWaitPolicy::Any, false).await;
}
#[tokio::test]
async fn independent_store_stale_tagged_registration_must_not_resurrect_a() {
    stale_registration(ChildWaitPolicy::All, true).await;
}
#[tokio::test]
async fn stale_completion_must_preserve_newly_registered_b_all() {
    stale_completion(ChildWaitPolicy::All, false).await;
}
#[tokio::test]
async fn stale_completion_legitimately_satisfies_same_first_arm_any() {
    stale_completion(ChildWaitPolicy::Any, false).await;
}
#[tokio::test]
async fn independent_store_stale_completion_must_preserve_b_all() {
    stale_completion(ChildWaitPolicy::All, true).await;
}
#[tokio::test]
async fn tagged_wait_sequential_first_arm_baselines_all() {
    sequential_baselines(ChildWaitPolicy::All).await;
}
#[tokio::test]
async fn tagged_wait_sequential_first_arm_baselines_any() {
    sequential_baselines(ChildWaitPolicy::Any).await;
}

async fn genuinely_later_arm_survives(policy: ChildWaitPolicy) {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        policy,
        "old-arm-a",
    )
    .await;
    let barrier = ReadBarrierStorage::new(
        h.storage.clone(),
        &h.parent_session_id,
        ReadBoundary::ChildWaitCommit,
    );
    let persistence = Arc::new(LockedSessionStore::new(barrier.clone()));
    let coordinator = coordinator_with_storage(&h, barrier.clone(), persistence).await;
    let completion = sealed_completion(&h).await;
    let task = tokio::spawn(async move { coordinator.on_child_completed(completion).await });
    barrier.entered.notified().await;
    // A's outcome is already staged, but its CAS has not acquired the writer.
    h.adapter
        .rollback_parent_wait_for_child(&h.parent_session_id, &h.child_session_id)
        .await
        .unwrap();
    register(
        &h.adapter,
        &h.parent_session_id,
        &["proof-child-b".into()],
        policy,
        "genuinely-later-arm-b",
    )
    .await;
    let before = settled_views(&h, "later B-only arm before stale A commit").await;
    assert_eq!(
        waiting(&before).unwrap().child_session_ids,
        vec!["proof-child-b"]
    );
    barrier.release.notify_one();
    task.await.unwrap();
    let after = settled_views(&h, "later B-only arm after stale A CAS").await;
    assert_eq!(waiting(&after), waiting(&before));
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn late_commit_must_not_release_genuinely_later_b_only_arm_all() {
    genuinely_later_arm_survives(ChildWaitPolicy::All).await;
}
#[tokio::test]
async fn late_commit_must_not_release_genuinely_later_b_only_arm_any() {
    genuinely_later_arm_survives(ChildWaitPolicy::Any).await;
}

#[tokio::test]
async fn initialized_owned_root_accepts_tagged_registration_and_sealed_completion() {
    use bamboo_domain::{ActorActivationFinish, RuntimeSessionPersistence};
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    let store = h.adapter.session_store.clone();
    let original = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let host = bamboo_engine::SessionRepository::new(
        h.adapter.sessions_cache.clone(),
        h.storage.clone(),
        h.adapter.persistence.clone(),
    )
    .with_root_actor_directory(store.clone());
    let mut binding = host
        .bind_root_actor_execution(&original, "owned-tagged-wait-run")
        .await
        .unwrap()
        .unwrap();
    let fence_before = store.inspect_actor(&original.id).await.unwrap();
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        ChildWaitPolicy::All,
        "owned-tagged-arm",
    )
    .await;
    let armed = settled_views(&h, "owned Root tagged arm").await;
    let bound = binding.persistence.clone();
    let mut stale_runner = armed.clone();
    stale_runner.agent_runtime_state.as_mut().unwrap().run_id = "owned-tagged-wait-run".into();
    bound.save_runtime_session(&mut stale_runner).await.unwrap();
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    coordinator
        .on_child_completed(sealed_completion(&h).await)
        .await;
    assert!(waiting(&settled_views(&h, "owned Root sealed completion").await).is_none());
    bound
        .save_finalized_runtime_session(&mut stale_runner)
        .await
        .unwrap();
    assert!(waiting(&settled_views(&h, "owned Root stale tagged finalization").await).is_none());
    assert_eq!(
        store.inspect_actor(&original.id).await.unwrap(),
        fence_before
    );
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            chrono::Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
}

async fn terminal_during_registration_writer(untracked: bool) {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    if untracked {
        register(
            &h.adapter,
            &h.parent_session_id,
            &[h.child_session_id.clone()],
            ChildWaitPolicy::Any,
            "already-armed-a",
        )
        .await;
    }
    let child_id = if untracked {
        "proof-child-b".to_string()
    } else {
        h.child_session_id.clone()
    };
    let (entered, release) = h
        .adapter
        .session_store
        .pause_child_wait_registration_after_status_read();
    let adapter = h.adapter.clone();
    let parent_id = h.parent_session_id.clone();
    let registered_child = child_id.clone();
    let registration = tokio::spawn(async move {
        register(
            &adapter,
            &parent_id,
            &[registered_child],
            ChildWaitPolicy::Any,
            "registration-holds-writer",
        )
        .await;
    });
    entered.wait().await;
    let completion = sealed_completion_for(&h, &child_id).await;
    let barrier = ReadBarrierStorage::new(
        h.storage.clone(),
        &h.parent_session_id,
        ReadBoundary::ChildWaitCommit,
    );
    let persistence = Arc::new(LockedSessionStore::new(barrier.clone()));
    let coordinator = coordinator_with_storage(&h, barrier.clone(), persistence).await;
    let callback = tokio::spawn(async move { coordinator.on_child_completed(completion).await });
    // The real callback captured the pre-registration parent but cannot settle
    // that observation until registration releases the same physical writer.
    barrier.entered.notified().await;
    assert!(!callback.is_finished());
    barrier.release.notify_one();
    release.wait().await;
    registration.await.unwrap();
    callback.await.unwrap();
    assert!(waiting(
        &settled_views(
            &h,
            "terminal commits while registration holds parent writer"
        )
        .await
    )
    .is_none());
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn no_wait_callback_must_observe_registration_at_physical_writer() {
    terminal_during_registration_writer(false).await;
}
#[tokio::test]
async fn untracked_callback_must_observe_new_membership_at_physical_writer() {
    terminal_during_registration_writer(true).await;
}

#[tokio::test]
async fn independent_store_terminal_sibling_absent_from_index_is_still_admitted() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let stale = Arc::new(
        SessionStoreV2::new(h.workspace_path.parent().unwrap().into())
            .await
            .unwrap(),
    );
    // B is created after the independent index loads, so id-based reads there
    // cannot see it. Canonical wait reads must still find its sealed result.
    seed_pending_children(&h).await;
    assert!(stale.load_session("proof-child-b").await.unwrap().is_none());
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone(), "proof-child-b".into()],
        ChildWaitPolicy::All,
        "cross-store-all-arm",
    )
    .await;
    let first =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    first
        .on_child_completed(sealed_completion_for(&h, "proof-child-b").await)
        .await;
    assert!(waiting(&settled_views(&h, "B terminal while A remains running").await).is_some());
    let storage: Arc<dyn Storage> = stale;
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let second = coordinator_with_storage(&h, storage, persistence).await;
    second.on_child_completed(sealed_completion(&h).await).await;
    assert!(waiting(
        &settled_views(&h, "stale independent index admits both sealed outcomes").await
    )
    .is_none());
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.session_inbox
            .inspect(&h.parent_session_id)
            .await
            .unwrap()
            .pending,
        2
    );
}

#[tokio::test]
async fn terminal_main_only_legacy_sibling_is_still_admitted_before_all_clear() {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone(), "proof-child-b".into()],
        ChildWaitPolicy::All,
        "legacy-main-only-arm",
    )
    .await;
    sealed_completion_for(&h, "proof-child-b").await;
    let path = h
        .workspace_path
        .parent()
        .unwrap()
        .join("sessions")
        .join(&h.parent_session_id)
        .join("children/proof-child-b/runtime.json");
    tokio::fs::remove_file(path).await.unwrap();
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    coordinator
        .on_child_completed(sealed_completion(&h).await)
        .await;
    assert!(waiting(&settled_views(&h, "legacy terminal main-only sibling").await).is_none());
    assert_eq!(
        h.session_inbox
            .inspect(&h.parent_session_id)
            .await
            .unwrap()
            .pending,
        2
    );
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn registration_and_completion_preserve_cache_local_inbox_admission() {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    let parent = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    h.adapter.sessions_cache.insert(
        parent.id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(parent)),
    );
    let cached = h.adapter.sessions_cache.get(&h.parent_session_id).unwrap();
    cached.update(|current| {
        current.session_inbox_admission_mut().last_admitted_sequence = 7;
    });
    let admission = cached.read().session_inbox_admission().cloned();
    drop(cached);
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        ChildWaitPolicy::All,
        "cache-local-admission-arm",
    )
    .await;
    assert_eq!(
        h.adapter
            .sessions_cache
            .get(&h.parent_session_id)
            .unwrap()
            .read()
            .session_inbox_admission(),
        admission.as_ref()
    );
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    coordinator
        .on_child_completed(sealed_completion(&h).await)
        .await;
    assert_eq!(
        h.adapter
            .sessions_cache
            .get(&h.parent_session_id)
            .unwrap()
            .read()
            .session_inbox_admission(),
        admission.as_ref()
    );
}

#[tokio::test]
async fn registration_does_not_reuse_a_cached_predecessor_transcript() {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    let durable = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    h.adapter.sessions_cache.insert(
        durable.id.clone(),
        Arc::new(bamboo_engine::SessionSnapshot::new(durable.clone())),
    );
    let cached = h.adapter.sessions_cache.get(&h.parent_session_id).unwrap();
    cached.update(|current| {
        current.created_at -= chrono::Duration::seconds(1);
        current
            .messages
            .push(Message::user("old lifetime must not leak"));
        current.session_inbox_admission_mut().last_admitted_sequence = 99;
    });
    drop(cached);
    register(
        &h.adapter,
        &h.parent_session_id,
        &[h.child_session_id.clone()],
        ChildWaitPolicy::All,
        "new-lifetime-arm",
    )
    .await;
    let cached = h.adapter.sessions_cache.get(&h.parent_session_id).unwrap();
    let cached = cached.read();
    assert_eq!(cached.created_at, durable.created_at);
    assert_eq!(cached.messages.len(), durable.messages.len());
    assert_eq!(
        cached.session_inbox_admission(),
        durable.session_inbox_admission()
    );
}

#[tokio::test]
async fn explicit_terminal_wait_completes_but_a_new_launch_still_arms() {
    for use_v2 in [false, true] {
        let h = build_test_harness_with_storage(None, None, use_v2).await;
        let mut child = h
            .storage
            .load_session(&h.child_session_id)
            .await
            .unwrap()
            .unwrap();
        child.set_last_run_status("completed");
        h.storage.save_session(&child).await.unwrap();
        assert_eq!(
            h.adapter
                .register_parent_wait_for_children_tagged(
                    &h.parent_session_id,
                    &[child.id.clone()],
                    ChildWaitPolicy::All,
                    "explicit-terminal-arm",
                )
                .await
                .unwrap(),
            0
        );
        assert!(waiting(
            &h.storage
                .load_session(&h.parent_session_id)
                .await
                .unwrap()
                .unwrap()
        )
        .is_none());
        h.adapter
            .register_parent_wait_for_child(&h.parent_session_id, &child.id, Some("new-launch-arm"))
            .await
            .unwrap();
        let parent = h
            .storage
            .load_session(&h.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        let wait = waiting(&parent).expect(
            "a new launch must arm before enqueue despite its predecessor's terminal status",
        );
        assert_eq!(wait.child_session_ids, vec![child.id]);
        assert_eq!(
            wait.registered_by_tool_call_id.as_deref(),
            Some("new-launch-arm")
        );
    }
}
