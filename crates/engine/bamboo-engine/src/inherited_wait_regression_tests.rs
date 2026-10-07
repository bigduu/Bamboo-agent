//! Controlled coordinator evidence for inherited Child-wait resurrection.
//! No provider execution, timing sleeps, or ParentQuestion fixture is needed.

use crate::runtime::execution::{ChildCompletion, ChildCompletionHandler, ChildCompletionSource};
use crate::{
    Agent, ChildCompletionCoordinator, SessionActivationLaunch, SessionActivationReserveOutcome,
    SessionActivationRouter, SessionActivationSpawner, SessionMessenger, SessionSnapshot,
};
use async_trait::async_trait;
use bamboo_domain::storage::Storage;
use bamboo_domain::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, Session, SessionActivationPolicy,
    SessionInboxBacklog, SessionInboxClaim, SessionInboxError, SessionInboxLimits,
    SessionInboxPort, SessionInboxReceipt, SessionMessageBody, SessionMessageEnvelope,
    SessionMessageId, WaitingForChildrenState,
};
use bamboo_llm::{Config, ProviderModelRouter, ProviderRegistry};
use bamboo_storage::{FileSessionInbox, LockedSessionStore, SessionStoreV2};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::RwLock;

struct NoTools;
#[async_trait]
impl bamboo_agent_core::tools::ToolExecutor for NoTools {
    async fn execute(
        &self,
        _: &bamboo_agent_core::tools::ToolCall,
    ) -> Result<bamboo_agent_core::tools::ToolResult, bamboo_agent_core::tools::ToolError> {
        panic!("no tool execution in persistence regression")
    }
    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        vec![]
    }
}
struct NoProvider;
#[async_trait]
impl bamboo_llm::LLMProvider for NoProvider {
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[bamboo_agent_core::tools::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        panic!("no provider execution in persistence regression")
    }
}
struct NoopSpawner;
#[async_trait]
impl SessionActivationSpawner for NoopSpawner {
    async fn reserve_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError> {
        Ok(SessionActivationReserveOutcome::Reserved(
            SessionActivationLaunch::new(format!("{target}-{generation}"), || {}),
        ))
    }
}

struct InboxProbe {
    inner: Arc<dyn SessionInboxPort>,
    delivered: Mutex<Vec<SessionMessageEnvelope>>,
    acks: AtomicUsize,
}
#[async_trait]
impl SessionInboxPort for InboxProbe {
    async fn deliver(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        let receipt = self.inner.deliver(envelope).await?;
        self.delivered.lock().unwrap().push(envelope.clone());
        Ok(receipt)
    }
    async fn mark_activation_eligible(
        &self,
        target: &str,
        generation: u64,
        policy: SessionActivationPolicy,
    ) -> Result<(), SessionInboxError> {
        self.inner
            .mark_activation_eligible(target, generation, policy)
            .await
    }
    async fn coordinator_activation_generation(
        &self,
        target: &str,
    ) -> Result<u64, SessionInboxError> {
        self.inner.coordinator_activation_generation(target).await
    }
    async fn claim(
        &self,
        target: &str,
        limit: usize,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        self.inner.claim(target, limit).await
    }
    async fn claim_for_turn(
        &self,
        target: &str,
        limit: usize,
        run: Option<&str>,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        self.inner.claim_for_turn(target, limit, run).await
    }
    async fn was_admitted(
        &self,
        target: &str,
        id: &SessionMessageId,
    ) -> Result<bool, SessionInboxError> {
        self.inner.was_admitted(target, id).await
    }
    async fn ack(&self, target: &str, claim: &SessionInboxClaim) -> Result<(), SessionInboxError> {
        self.inner.ack(target, claim).await?;
        self.acks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn inspect(&self, target: &str) -> Result<SessionInboxBacklog, SessionInboxError> {
        self.inner.inspect(target).await
    }
}

#[derive(Debug, Clone, Copy)]
enum Checkpoint {
    None,
    Ordinary,
    AppendSafe,
}
fn wait(session: &Session) -> Option<&WaitingForChildrenState> {
    session
        .agent_runtime_state
        .as_ref()
        .and_then(|s| s.waiting_for_children.as_ref())
}

async fn prove_callback_transition(checkpoint: Checkpoint) {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
    let storage: Arc<dyn Storage> = store.clone();
    let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
    let inbox = Arc::new(InboxProbe {
        inner: Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        )),
        delivered: Mutex::new(vec![]),
        acks: AtomicUsize::new(0),
    });
    let router = SessionActivationRouter::new();
    router.set_spawner(Arc::new(NoopSpawner)).await;
    let messenger = Arc::new(SessionMessenger::new(
        storage.clone(),
        inbox.clone(),
        router.clone(),
    ));
    let provider: Arc<dyn bamboo_llm::LLMProvider> = Arc::new(NoProvider);
    let config = Arc::new(RwLock::new(Config::default()));
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            temp.path().join("metrics.db"),
        )),
        7,
    );
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(NoTools);
    let agent = Arc::new(
        Agent::builder()
            .storage(storage.clone())
            .persistence(persistence.clone())
            .session_inbox(inbox.clone())
            .activation_router(router.clone())
            .session_messenger(messenger)
            .attachment_reader(store.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(metrics)
            .config(config.clone())
            .provider(provider.clone())
            .default_tools(tools.clone())
            .build()
            .unwrap(),
    );
    let registry = Arc::new(ProviderRegistry::new(
        HashMap::from([("test".into(), provider)]),
        "test".into(),
    ));
    let cache: crate::SessionCache = Arc::default();
    let coordinator = ChildCompletionCoordinator::new(
        storage,
        persistence.clone(),
        cache.clone(),
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(HashMap::new())),
        agent.clone(),
        config,
        registry.clone(),
        Arc::new(ProviderModelRouter::new(registry)),
        temp.path().into(),
        None,
    );
    coordinator.set_root_tools(tools).await;

    let mut parent = Session::new("inherited-callback-parent", "model");
    let inherited = WaitingForChildrenState::for_children(
        vec!["completed-child".into()],
        ChildWaitPolicy::All,
        chrono::Utc::now(),
    );
    assert!(inherited.registered_by_tool_call_id.is_none());
    let mut runtime = AgentRuntimeState::new("original-run");
    runtime.status = AgentStatusState::Suspended;
    runtime.waiting_for_children = Some(inherited.clone());
    parent.agent_runtime_state = Some(runtime);
    parent.metadata.insert(
        "runtime.suspend_reason".into(),
        "waiting_for_children".into(),
    );
    parent.add_message(bamboo_domain::Message::user("original task"));
    store.save_session(&parent).await.unwrap();
    // Bind through the same domain port and repository used by execution.
    // The completion coordinator keeps its independent, unbound Host view.
    let run_repository =
        crate::SessionRepository::new(cache.clone(), store.clone(), persistence.clone());
    let run_persistence = bamboo_domain::RuntimeSessionPersistence::bind_inherited_child_wait(
        &run_repository,
        bamboo_domain::InheritedChildWait::capture(&parent).unwrap(),
    )
    .unwrap();

    // The reply run carries the original wait even though its generic
    // suspension was interrupted. Capture this exact stale run snapshot.
    let mut reply = parent.clone();
    let state = reply.agent_runtime_state.as_mut().unwrap();
    state.run_id = "reply-run".into();
    state.status = AgentStatusState::Running;
    reply.metadata.remove("runtime.suspend_reason");
    run_persistence
        .save_runtime_session(&mut reply)
        .await
        .unwrap();
    cache.insert(
        reply.id.clone(),
        Arc::new(SessionSnapshot::new(reply.clone())),
    );
    let registration = router.register_run(&reply.id, "reply-run").await.unwrap();

    let mut child = Session::new_child("completed-child", &parent.id, "model", "Child");
    child.add_message(bamboo_domain::Message::assistant(
        "sealed child result",
        None,
    ));
    child.set_last_run_status("completed");
    ChildCompletionSource::prepare(&mut child, "child-run", &Default::default());
    store.save_session(&child).await.unwrap();
    let completion = ChildCompletion {
        parent_session_id: parent.id.clone(),
        child_session_id: child.id.clone(),
        status: "completed".into(),
        error: None,
        completed_at: child.updated_at,
        source: ChildCompletionSource::from_committed_session(
            &store.load_session(&child.id).await.unwrap().unwrap(),
        ),
    };
    assert!(
        completion.source.is_some(),
        "production terminal seal must validate after commit"
    );
    coordinator.on_child_completed(completion.clone()).await;
    assert!(
        wait(&store.load_session(&parent.id).await.unwrap().unwrap()).is_none(),
        "actual callback must first commit clear"
    );
    assert!(
        wait(&cache.get(&parent.id).unwrap().read()).is_none(),
        "actual callback must publish clear to cache"
    );
    let outcome = inbox.delivered.lock().unwrap()[0].clone();
    let SessionMessageBody::ChildOutcome(child_outcome) = &outcome.body else {
        panic!("expected one ChildOutcome")
    };
    assert_eq!(child_outcome.result.as_deref(), Some("sealed child result"));
    assert_eq!(
        outcome.correlation_id.as_deref(),
        Some("child_completion_after_run:reply-run")
    );
    assert_eq!(inbox.delivered.lock().unwrap().len(), 1);
    assert_eq!(inbox.inspect(&parent.id).await.unwrap().pending, 1);
    assert!(inbox
        .claim_for_turn(&parent.id, 8, Some("reply-run"))
        .await
        .unwrap()
        .is_empty());
    coordinator.on_child_completed(completion.clone()).await;
    assert_eq!(
        inbox.delivered.lock().unwrap().len(),
        1,
        "callback retry must not duplicate outcome"
    );

    match checkpoint {
        Checkpoint::None => {}
        Checkpoint::Ordinary => run_persistence
            .save_runtime_session(&mut reply)
            .await
            .unwrap(),
        Checkpoint::AppendSafe => run_persistence
            .checkpoint_runtime_session(&mut reply)
            .await
            .unwrap(),
    }
    let interim = [
        wait(&reply).is_some(),
        wait(&cache.get(&parent.id).unwrap().read()).is_some(),
        wait(&store.load_session(&parent.id).await.unwrap().unwrap()).is_some(),
    ];
    reply.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Completed;
    run_persistence
        .save_finalized_runtime_with_inherited_child_wait(&mut reply, &inherited)
        .await
        .unwrap();
    let final_wait = wait(&store.load_session(&parent.id).await.unwrap().unwrap()).is_some();
    let activation = coordinator.reserve_activation(&parent.id, 1).await.unwrap();
    let blocked = matches!(activation, SessionActivationReserveOutcome::NoWork);
    eprintln!("{checkpoint:?}: actual callback cleared wait and staged one reply-run deferred outcome; checkpoint caller/cache/durable={interim:?}; inherited-final durable_wait={final_wait}; real_successor_guard_NoWork={blocked}; ACKs={}", inbox.acks.load(Ordering::SeqCst));
    if !matches!(checkpoint, Checkpoint::None) {
        assert_eq!(
            interim, [false; 3],
            "stale checkpoint resurrected actual coordinator clear"
        );
    }
    assert!(!final_wait);
    assert!(
        !blocked,
        "successor must not be blocked by resurrected wait"
    );
    assert!(matches!(
        activation,
        SessionActivationReserveOutcome::Reserved(_)
    ));
    drop(activation); // Keep this a storage/admission test; never launch a provider.

    let mut successor = store.load_session(&parent.id).await.unwrap().unwrap();
    successor.agent_runtime_state.as_mut().unwrap().run_id = "successor-run".into();
    assert_eq!(
        agent
            .admit_session_inbox_at_safe_boundary_checked(&mut successor)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        agent
            .admit_session_inbox_at_safe_boundary_checked(&mut successor)
            .await
            .unwrap(),
        0
    );
    assert_eq!(inbox.acks.load(Ordering::SeqCst), 1);
    assert!(inbox.was_admitted(&parent.id, &outcome.id).await.unwrap());
    assert_eq!(inbox.inspect(&parent.id).await.unwrap().pending, 0);
    assert_eq!(inbox.inspect(&parent.id).await.unwrap().claimed, 0);
    let durable = store.load_session(&parent.id).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| m.id == outcome.id.as_str())
            .count(),
        1
    );
    coordinator.on_child_completed(completion).await;
    assert_eq!(inbox.delivered.lock().unwrap().len(), 1);
    registration.finish(1).await.unwrap();
    eprintln!("{checkpoint:?}: successor committed exactly one outcome, permanent receipt present, ACKs={}, backlog=0", inbox.acks.load(Ordering::SeqCst));
}

#[tokio::test]
async fn actual_callback_clear_survives_stale_ordinary_checkpoint() {
    prove_callback_transition(Checkpoint::Ordinary).await;
}
#[tokio::test]
async fn actual_callback_clear_survives_stale_append_safe_checkpoint() {
    prove_callback_transition(Checkpoint::AppendSafe).await;
}
#[tokio::test]
async fn actual_callback_without_intermediate_checkpoint_consumes_and_acks_once() {
    prove_callback_transition(Checkpoint::None).await;
}

#[tokio::test]
async fn inherited_root_wait_uses_actual_bound_full_runtime_and_owned_input_writers() {
    use bamboo_domain::{ActorActivationFinish, RuntimeSessionPersistence};
    for write in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
        let mut original = Session::new("inherited-owned-root", "model");
        let mut state = AgentRuntimeState::new("original-run");
        state.status = AgentStatusState::Suspended;
        state.waiting_for_children = Some(WaitingForChildrenState::for_children(
            vec!["owned-root-child".into()],
            ChildWaitPolicy::All,
            chrono::Utc::now(),
        ));
        original.agent_runtime_state = Some(state);
        original.add_message(bamboo_domain::Message::user("original task"));
        store.save_session(&original).await.unwrap();
        let independent = SessionStoreV2::new(directory.path().into()).await.unwrap();
        let host = crate::SessionRepository::new(
            Arc::default(),
            store.clone(),
            Arc::new(LockedSessionStore::new(store.clone())),
        )
        .with_root_actor_directory(store.clone());
        let mut binding = host
            .bind_root_actor_execution(&original, "reply-run")
            .await
            .unwrap()
            .unwrap();
        let bound = binding
            .persistence
            .bind_inherited_child_wait(
                bamboo_domain::InheritedChildWait::capture(&original).unwrap(),
            )
            .unwrap();
        assert_eq!(
            bound.root_actor_writer().unwrap().fence,
            binding.owner.fence
        );
        let mut incoming = original.clone();
        let runtime = incoming.agent_runtime_state.as_mut().unwrap();
        runtime.run_id = "reply-run".into();
        runtime.status = AgentStatusState::Running;
        bound.save_runtime_session(&mut incoming).await.unwrap();
        let mut cleared = incoming.clone();
        cleared
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .waiting_for_children = None;
        cleared.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Idle;
        independent
            .save_root_actor_runtime(&binding.owner, &cleared, true, Arc::new(|_| {}))
            .await
            .unwrap();
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let input = SessionMessageEnvelope::user_input(&original.id, "owned input after clear");
        match write {
            0 => bound.save_runtime_session(&mut incoming).await.unwrap(),
            1 => bound
                .checkpoint_runtime_session(&mut incoming)
                .await
                .unwrap(),
            2 => bound
                .save_runtime_control_plane(&mut incoming)
                .await
                .unwrap(),
            3 => {
                let receipt = inbox.deliver(&input).await.unwrap();
                inbox
                    .mark_activation_eligible(
                        &original.id,
                        receipt.generation,
                        SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                    .unwrap();
                let admitted = bound
                    .admit_root_inbox(&mut incoming, inbox.clone(), Some("reply-run"))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(admitted.merged, 1);
                assert!(admitted.admission_error.is_none());
                assert!(inbox.was_admitted(&original.id, &input.id).await.unwrap());
                assert_eq!(inbox.inspect(&original.id).await.unwrap().pending, 0);
                assert_eq!(inbox.inspect(&original.id).await.unwrap().claimed, 0);
                assert_eq!(
                    bound
                        .admit_root_inbox(&mut incoming, inbox.clone(), Some("reply-run"))
                        .await
                        .unwrap()
                        .unwrap()
                        .merged,
                    0
                );
            }
            _ => unreachable!(),
        }
        let durable = independent
            .load_session(&original.id)
            .await
            .unwrap()
            .unwrap();
        let cached = crate::read_cached_session(host.cache(), &original.id).unwrap();
        for saved in [&incoming, &durable, &cached] {
            let runtime = saved.agent_runtime_state.as_ref().unwrap();
            assert!(
                runtime.waiting_for_children.is_none(),
                "Root write mode {write}"
            );
            assert_eq!(runtime.status, AgentStatusState::Running);
            assert_eq!(runtime.run_id, "reply-run");
            assert_eq!(saved.created_at, original.created_at);
        }
        if write == 3 {
            assert_eq!(
                durable
                    .messages
                    .iter()
                    .filter(|message| bamboo_domain::is_matching_session_message(message, &input))
                    .count(),
                1
            );
        }
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
        // A new owner invalidates the old bound provenance view as well as its
        // ordinary Root capability. No stale Main or Host-cache publication.
        let mut successor = host
            .bind_root_actor_execution(&durable, "successor-run")
            .await
            .unwrap()
            .unwrap();
        let before = serde_json::to_value(&durable).unwrap();
        let cached_before =
            serde_json::to_value(crate::read_cached_session(host.cache(), &original.id)).unwrap();
        incoming.add_message(bamboo_domain::Message::assistant(
            "STALE_INHERITED_ROOT",
            None,
        ));
        assert!(bound.save_runtime_session(&mut incoming).await.is_err());
        assert_eq!(
            serde_json::to_value(store.load_session(&original.id).await.unwrap().unwrap()).unwrap(),
            before
        );
        assert_eq!(
            serde_json::to_value(crate::read_cached_session(host.cache(), &original.id)).unwrap(),
            cached_before
        );
        successor
            .directory
            .finish_activation(
                &successor.owner.fence,
                chrono::Utc::now(),
                ActorActivationFinish::Succeeded,
            )
            .await
            .unwrap();
        successor.disarm_abandonment();
    }
}

struct UnsupportedInheritedPersistence;
#[async_trait]
impl bamboo_domain::RuntimeSessionPersistence for UnsupportedInheritedPersistence {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        panic!("unsupported inherited binding must reject before any save")
    }
}

fn entrypoint_agent(
    directory: &std::path::Path,
    store: Arc<SessionStoreV2>,
    persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence>,
) -> Arc<Agent> {
    Arc::new(
        Agent::builder()
            .storage(store.clone())
            .persistence(persistence)
            .attachment_reader(store)
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    directory.join("entrypoint-metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(RwLock::new(Config::default())))
            .provider(Arc::new(NoProvider))
            .default_tools(Arc::new(NoTools))
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn actual_reservation_handoff_freezes_inherited_wait_and_none_before_first_save() {
    use crate::runtime::execution::{
        reserve_session_execution, spawn_session_execution, SessionExecutionArgs,
        SessionExecutionReserveOutcome,
    };
    for mode in 0..3 {
        let inherited_present = mode != 1;
        let unsupported = mode == 2;
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
        let mut original = Session::new("inherited-reservation-entry", "model");
        let mut state = AgentRuntimeState::new("original-run");
        state.status = AgentStatusState::Idle;
        let inherited = WaitingForChildrenState::for_children(
            vec!["original-entry-child".into()],
            ChildWaitPolicy::All,
            chrono::Utc::now(),
        );
        if inherited_present {
            state.waiting_for_children = Some(inherited.clone());
        }
        original.agent_runtime_state = Some(state);
        original.add_message(bamboo_domain::Message::user("entrypoint task"));
        store.save_session(&original).await.unwrap();
        let independent = SessionStoreV2::new(directory.path().into()).await.unwrap();
        let cache: crate::SessionCache = Arc::default();
        let host = crate::SessionRepository::new(
            cache.clone(),
            store.clone(),
            Arc::new(LockedSessionStore::new(store.clone())),
        );
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = if unsupported {
            Arc::new(UnsupportedInheritedPersistence)
        } else {
            Arc::new(host)
        };
        let agent = entrypoint_agent(directory.path(), store.clone(), persistence);
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let senders = Arc::new(RwLock::new(HashMap::new()));
        let (events, _) = tokio::sync::broadcast::channel(32);
        let SessionExecutionReserveOutcome::Reserved(mut reservation) =
            reserve_session_execution(&agent, &runners, &senders, &original.id, &events).await
        else {
            panic!("real entrypoint runner must reserve")
        };
        if !unsupported {
            reservation
                .bind_root_actor(&agent, &original)
                .await
                .unwrap();
        }
        let mut incoming = original.clone();
        let expected = if unsupported {
            Some(inherited.clone())
        } else if inherited_present {
            let mut cleared = original.clone();
            cleared
                .agent_runtime_state
                .as_mut()
                .unwrap()
                .waiting_for_children = None;
            independent.save_runtime_state(&cleared).await.unwrap();
            let persistence = reservation.execution_persistence().unwrap();
            persistence
                .save_runtime_session(&mut incoming)
                .await
                .unwrap();
            assert!(
                wait(&incoming).is_none(),
                "the actual pre-spawn adapter write retains clear"
            );
            assert!(wait(&crate::read_cached_session(&cache, &original.id).unwrap()).is_none());
            let captured = persistence.inherited_child_wait().unwrap();
            assert_eq!(captured.wait(), &inherited);
            // Rebinding the same reservation after reconciliation retains W's
            // provenance, rather than recapturing the now-cleared snapshot.
            reservation
                .bind_root_actor(&agent, &incoming)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(
                &persistence,
                &reservation.execution_persistence().unwrap()
            ));
            None
        } else {
            // None was captured at reservation time. A new first-arm wait
            // before the handoff is caller-owned, not newly "inherited".
            let new_wait = WaitingForChildrenState::for_children(
                vec!["new-entry-child".into()],
                ChildWaitPolicy::All,
                chrono::Utc::now(),
            );
            incoming
                .agent_runtime_state
                .as_mut()
                .unwrap()
                .waiting_for_children = Some(new_wait.clone());
            Some(new_wait)
        };
        reservation.cancel_token().cancel(); // deterministic no-provider execution
        let (mpsc_tx, mut mpsc_rx) = tokio::sync::mpsc::channel(64);
        let (acknowledger, history_commit_barrier) =
            crate::runtime::execution::event_forwarder::history_commit_barrier();
        spawn_session_execution(SessionExecutionArgs {
            agent,
            session_id: original.id.clone(),
            session: incoming,
            execution_reservation: reservation,
            tools_override: None,
            provider_override: None,
            model_roster: crate::runtime::model_roster::ModelRoster {
                model: Some("model".into()),
                ..Default::default()
            },
            reasoning_effort: None,
            reasoning_effort_source: "test".into(),
            auxiliary_model_resolver: None,
            disabled_filter_resolver: None,
            disabled_tools: None,
            disabled_skill_ids: None,
            selected_skill_ids: None,
            selected_skill_mode: None,
            mpsc_tx,
            history_commit_barrier,
            image_fallback: None,
            gold_config: None,
            guardian_config: None,
            guardian_spawner: None,
            bash_resume_hook: None,
            bash_completion_sink: None,
            app_data_dir: Some(directory.path().into()),
            run_budget: None,
            runners: runners.clone(),
            sessions_cache: cache.clone(),
            on_complete: None,
            child_completion_handler: None,
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while let Some(event) = mpsc_rx.recv().await {
                if matches!(
                    event,
                    bamboo_agent_core::AgentEvent::SessionHistoryCommitted { .. }
                ) {
                    acknowledger.acknowledge();
                }
            }
        })
        .await
        .expect("cancelled entrypoint completes without provider work");
        let durable = independent
            .load_session(&original.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wait(&durable), expected.as_ref());
        if unsupported {
            assert!(crate::read_cached_session(&cache, &original.id).is_none());
            assert_eq!(
                serde_json::to_value(&durable).unwrap(),
                serde_json::to_value(&original).unwrap()
            );
        } else {
            let cached = crate::read_cached_session(&cache, &original.id).unwrap();
            assert_eq!(wait(&cached), expected.as_ref());
        }
        assert!(!matches!(
            runners
                .read()
                .await
                .get(&original.id)
                .map(|runner| &runner.status),
            Some(crate::runtime::execution::runner_state::AgentStatus::Running)
        ));
    }
}
