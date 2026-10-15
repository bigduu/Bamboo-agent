use super::*;
use crate::runtime::{Agent, ExecuteRequestBuilder};

async fn fixture(
    dir: &std::path::Path,
    provider: Arc<FakeProvider>,
) -> (Arc<Agent>, Arc<dyn Storage>) {
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.join("sessions"))
            .await
            .unwrap(),
    );
    let storage: Arc<dyn Storage> = store.clone();
    let agent = Agent::builder()
        .storage(storage.clone())
        .persistence(Arc::new(bamboo_storage::LockedSessionStore::new(
            storage.clone(),
        )))
        .attachment_reader(store)
        .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
        .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                dir.join("metrics.db"),
            )),
            7,
        ))
        .config(Arc::new(tokio::sync::RwLock::new(
            bamboo_llm::Config::default(),
        )))
        .activation_router(crate::session_activation::SessionActivationRouter::new())
        .provider(provider)
        .memory_store(bamboo_memory::memory_store::MemoryStore::new(
            dir.join("jiandu"),
        ))
        .default_tools(Arc::new(FakeTools))
        .build()
        .unwrap();
    (Arc::new(agent), storage)
}

fn interrupted() -> Session {
    let mut session = Session::new("counted-fixture", "fake-only");
    session.add_message(Message::user(PROMPT));
    session.metadata.insert(
        LOOP_STATE_METADATA_KEY.into(),
        serde_json::to_string(&interrupted_state()).unwrap(),
    );
    let mut runtime = bamboo_domain::AgentRuntimeState::new(&session.id);
    runtime.execution_id = Some("old-owner".into());
    runtime.status = bamboo_domain::AgentStatusState::Running;
    session.agent_runtime_state = Some(runtime);
    session
}

#[tokio::test]
async fn counted_registered_refusal_uses_canonical_snapshot_and_skips_direct_final_save() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::default());
    let (agent, storage) = fixture(dir.path(), provider.clone()).await;
    let mut canonical = interrupted();
    canonical
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .no_human_approver = true;
    canonical
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .waiting_for_children = Some(bamboo_domain::WaitingForChildrenState::for_children(
        vec!["child".into()],
        bamboo_domain::ChildWaitPolicy::All,
        chrono::Utc::now(),
    ));
    storage.save_session(&canonical).await.unwrap();
    let mut rejected = canonical.clone();
    rejected.add_message(Message::user("unadmitted request must stay off disk"));
    rejected
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .no_human_approver = false;
    let (tx, mut rx) = mpsc::channel(512);
    let result = agent
        .execute_direct(
            &mut rejected,
            ExecuteRequestBuilder::new(PROMPT, tx, CancellationToken::new()).build(),
        )
        .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("recovery unsupported"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(rx.try_recv().is_err());
    let durable = storage.load_session(&canonical.id).await.unwrap().unwrap();
    assert_eq!(durable.messages, canonical.messages);
    assert_eq!(durable.last_run_status(), canonical.last_run_status());
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::RecoveryUnsupported
    );
    let mut expected_runtime = canonical.agent_runtime_state.unwrap();
    expected_runtime.status = bamboo_domain::AgentStatusState::Failed;
    assert_eq!(durable.agent_runtime_state, Some(expected_runtime));
    assert_eq!(rejected.messages.len(), 2);
    assert!(!rejected.agent_runtime_state.unwrap().no_human_approver);
}

#[tokio::test]
async fn counted_recovery_fences_only_exact_execution_and_preserves_suspension() {
    use bamboo_domain::AgentStatusState;
    for (identity, status, expected) in [
        (
            "typed",
            AgentStatusState::Initializing,
            AgentStatusState::Failed,
        ),
        ("typed", AgentStatusState::Running, AgentStatusState::Failed),
        (
            "typed",
            AgentStatusState::Finalizing,
            AgentStatusState::Failed,
        ),
        (
            "typed",
            AgentStatusState::Suspended,
            AgentStatusState::Suspended,
        ),
        (
            "foreign",
            AgentStatusState::Running,
            AgentStatusState::Running,
        ),
        (
            "legacy",
            AgentStatusState::Running,
            AgentStatusState::Failed,
        ),
        (
            "missing",
            AgentStatusState::Initializing,
            AgentStatusState::Initializing,
        ),
        (
            "legacy-foreign",
            AgentStatusState::Running,
            AgentStatusState::Running,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let (agent, storage) = fixture(dir.path(), provider.clone()).await;
        let mut session = interrupted();
        let runtime = session.agent_runtime_state.as_mut().unwrap();
        runtime.status = status;
        runtime.execution_id = match identity {
            "typed" => Some("old-owner".into()),
            "foreign" => Some("other-owner".into()),
            _ => None,
        };
        // A typed foreign owner must not fall back to this matching round.
        runtime.round.last_round_id = match identity {
            "legacy" | "foreign" => Some(crate::runtime::runner::round_prelude::build_round_id(
                &session.id,
                "old-owner",
                0,
            )),
            "legacy-foreign" => Some(crate::runtime::runner::round_prelude::build_round_id(
                &session.id,
                "other-owner",
                0,
            )),
            _ => None,
        };
        runtime.waiting_for_children = Some(bamboo_domain::WaitingForChildrenState::for_children(
            vec!["child".into()],
            bamboo_domain::ChildWaitPolicy::All,
            chrono::Utc::now(),
        ));
        let mut expected_runtime = runtime.clone();
        expected_runtime.status = expected;
        storage.save_session(&session).await.unwrap();
        let lease = agent.begin_direct_execution(&session.id).await.unwrap();
        let error = agent
            .check_direct_execution_admission(&mut session, &lease)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recovery unsupported"));
        assert_eq!(
            session.agent_runtime_state,
            Some(expected_runtime.clone()),
            "{identity}/{status:?}"
        );
        let durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            durable.agent_runtime_state,
            Some(expected_runtime),
            "{identity}/{status:?}"
        );
        assert_eq!(
            read_loop_state(&durable).unwrap().unwrap().status,
            LoopStatus::RecoveryUnsupported
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn counted_stale_registered_clone_cannot_erase_a_newer_durable_generation() {
    for stale in ["missing", "generation", "revision", "terminal", "birth"] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let (agent, storage) = fixture(dir.path(), provider.clone()).await;
        let canonical = interrupted();
        storage.save_session(&canonical).await.unwrap();
        let durable_before = storage.load_session(&canonical.id).await.unwrap().unwrap();
        let mut rejected = canonical.clone();
        match stale {
            "missing" => {
                rejected.metadata.remove(LOOP_STATE_METADATA_KEY);
            }
            "birth" => rejected.created_at -= chrono::Duration::seconds(1),
            _ => {
                let mut state = read_loop_state(&rejected).unwrap().unwrap();
                match stale {
                    "generation" => state.generation = uuid::Uuid::new_v4().to_string(),
                    "revision" => state.revision += 1,
                    "terminal" => state.status = LoopStatus::Failed,
                    _ => unreachable!(),
                }
                rejected.metadata.insert(
                    LOOP_STATE_METADATA_KEY.into(),
                    serde_json::to_string(&state).unwrap(),
                );
            }
        }
        rejected.add_message(Message::user("unadmitted stale request"));
        let before = serde_json::to_value(&rejected).unwrap();
        let (tx, mut rx) = mpsc::channel(512);
        let lease = agent.begin_direct_execution(&rejected.id).await.unwrap();
        let error = agent
            .execute_direct_registered(
                &mut rejected,
                ExecuteRequestBuilder::new(PROMPT, tx, CancellationToken::new()).build(),
                lease,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("reload required"),
            "{stale}: {error}"
        );
        assert_eq!(serde_json::to_value(&rejected).unwrap(), before, "{stale}");
        assert_eq!(
            serde_json::to_value(storage.load_session(&canonical.id).await.unwrap().unwrap())
                .unwrap(),
            serde_json::to_value(&durable_before).unwrap(),
            "{stale}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn counted_spawn_refusal_skips_completion_hooks_and_final_checkpoint() {
    use crate::runtime::execution::*;
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::default());
    let (agent, storage) = fixture(dir.path(), provider.clone()).await;
    let canonical = interrupted();
    storage.save_session(&canonical).await.unwrap();
    let mut rejected = canonical.clone();
    rejected.add_message(Message::user("not an admitted spawn"));
    let runners = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let senders = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let (sender, _) = tokio::sync::broadcast::channel(512);
    let reservation =
        match reserve_session_execution(&agent, &runners, &senders, &canonical.id, &sender).await {
            SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("fresh fixture must acquire ownership"),
        };
    let (tx, mut rx) = mpsc::channel(512);
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_calls = hooks.clone();
    let (_, barrier) = history_commit_barrier();
    spawn_session_execution(SessionExecutionArgs {
        agent: agent.clone(),
        session_id: canonical.id.clone(),
        session: rejected,
        execution_reservation: reservation,
        tools_override: None,
        provider_override: None,
        model_roster: crate::runtime::model_roster::ModelRoster {
            model: Some("fake-only".into()),
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
        mpsc_tx: tx,
        history_commit_barrier: barrier,
        image_fallback: None,
        gold_config: None,
        guardian_config: None,
        guardian_spawner: None,
        bash_resume_hook: None,
        bash_completion_sink: None,
        app_data_dir: None,
        run_budget: None,
        runners: runners.clone(),
        sessions_cache: Default::default(),
        on_complete: Some(Box::new(move |_, _| {
            Box::pin(async move {
                hook_calls.fetch_add(1, Ordering::SeqCst);
            })
        })),
        child_completion_handler: None,
    });
    let mut events = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
    })
    .await
    .unwrap();
    assert_eq!(complete_count(&events), 0);
    assert!(events.iter().any(
        |e| matches!(e, AgentEvent::Error { message } if message.contains("recovery unsupported"))
    ));
    assert_eq!(hooks.load(Ordering::SeqCst), 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let durable = storage.load_session(&canonical.id).await.unwrap().unwrap();
    assert_eq!(durable.messages, canonical.messages);
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::RecoveryUnsupported
    );
    assert_eq!(
        durable.agent_runtime_state.unwrap().status,
        bamboo_domain::AgentStatusState::Failed
    );
}

struct AdmissionPersistence {
    inner: Arc<dyn RuntimeSessionPersistence>,
    checkpoints: AtomicUsize,
    fail: bool,
    needs_actor: bool,
}

#[async_trait]
impl RuntimeSessionPersistence for AdmissionPersistence {
    fn root_actor_execution_required(&self, _: &Session) -> bool {
        self.needs_actor
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.checkpoints.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(std::io::Error::other(
                "synthetic admission checkpoint failure",
            ));
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn counted_recovery_requires_real_current_owner_and_existing_actor_writer() {
    for denial in ["no-proof", "other-router", "expired", "actor-writer"] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let (agent, storage) = fixture(dir.path(), provider).await;
        let mut session = interrupted();
        storage.save_session(&session).await.unwrap();
        let before = serde_json::to_value(&session).unwrap();
        let durable_before =
            serde_json::to_value(storage.load_session(&session.id).await.unwrap().unwrap())
                .unwrap();
        let router = agent.activation_router().unwrap();
        let mut registration = Some(
            router
                .register_run(&session.id, "genuine-replacement")
                .await
                .unwrap(),
        );
        let proof = registration.as_ref().unwrap().ownership();
        if denial == "expired" {
            registration.take().unwrap().abandon().await;
        }
        let foreign_router = crate::session_activation::SessionActivationRouter::new();
        let persistence = AdmissionPersistence {
            inner: agent.persistence().clone(),
            checkpoints: AtomicUsize::new(0),
            fail: false,
            needs_actor: denial == "actor-writer",
        };
        assert!(crate::runtime::counted_loop::admit_registered(
            &mut session,
            if denial == "no-proof" {
                None
            } else {
                Some(&proof)
            },
            Some(if denial == "other-router" {
                &foreign_router
            } else {
                router
            }),
            &persistence,
        )
        .await
        .is_err());
        assert_eq!(persistence.checkpoints.load(Ordering::SeqCst), 0);
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(
            serde_json::to_value(storage.load_session(&session.id).await.unwrap().unwrap())
                .unwrap(),
            durable_before
        );
        if let Some(registration) = registration {
            registration.abandon().await;
        }
    }
}

#[tokio::test]
async fn counted_admission_checkpoint_failure_has_no_outer_error_save() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::default());
    let (agent, storage) = fixture(dir.path(), provider.clone()).await;
    let mut session = interrupted();
    storage.save_session(&session).await.unwrap();
    let before = serde_json::to_value(&session).unwrap();
    let durable_before =
        serde_json::to_value(storage.load_session(&session.id).await.unwrap().unwrap()).unwrap();
    let persistence = Arc::new(AdmissionPersistence {
        inner: agent.persistence().clone(),
        checkpoints: AtomicUsize::new(0),
        fail: true,
        needs_actor: false,
    });
    let agent = agent.with_execution_persistence(persistence.clone());
    let (tx, mut rx) = mpsc::channel(512);
    let error = agent
        .execute_direct(
            &mut session,
            ExecuteRequestBuilder::new(PROMPT, tx, CancellationToken::new()).build(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint failed"));
    assert_eq!(persistence.checkpoints.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(
        serde_json::to_value(storage.load_session(&session.id).await.unwrap().unwrap()).unwrap(),
        durable_before
    );
}
