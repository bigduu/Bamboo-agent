//! Synthetic sources only. No real account, user history or reset transaction.
use super::*;
use crate::runtime::execution::{
    runner_lifecycle::{reserve_runner_core, ReserveOutcome},
    AgentRunner, AgentStatus,
};
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};

fn session() -> Session {
    let mut session = Session::new("root-outer-lifetime-fixture", "model");
    session.set_project_id_meta("root-lifetime-project");
    session
}

async fn settled(lifetime: &RootExecutionLifetime) -> RootOuterClosure {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observed = lifetime.outer_closure();
            if observed != RootOuterClosure::Pending {
                return observed;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("controlled actual outer join")
}

// Actual JoinHandle with synthetic tail/Actor observations, never a Root idle
// or Storage capability. The production spawn fixture below uses the real Actor.
async fn joined(lifetime: &Arc<RootExecutionLifetime>, session: &Session) {
    let admission = lifetime.admit(&Uuid::new_v4().to_string()).unwrap();
    let outer = admission.attach(session).unwrap();
    let tail = outer.clone();
    outer.retain(tokio::spawn(async move {
        tail.actor_finished(Ok(()));
        tail.tail_completed();
    }));
    assert!(matches!(
        settled(lifetime).await,
        RootOuterClosure::JoinedResourcesUnavailable { .. } | RootOuterClosure::Unconfirmed(_)
    ));
}

#[tokio::test]
async fn reset_reservation_blocks_common_raw_admission_until_last_physical_holder_drops() {
    let runners = Arc::new(RwLock::new(HashMap::<String, AgentRunner>::new()));
    let senders = Arc::default();
    let (events, _) = broadcast::channel(16);
    let session = session();
    let ReserveOutcome::Reserved(raw) =
        reserve_runner_core(&runners, &senders, &session.id, &events).await
    else {
        panic!("reserve");
    };
    let admission = raw.physical_admission.unwrap();
    let lifetime = admission.lifetime.clone();
    let outer = admission.attach(&session).unwrap();
    let tail = outer.clone();
    outer.retain(tokio::spawn(async move {
        tail.actor_finished(Ok(()));
        tail.tail_completed();
    }));
    settled(&lifetime).await;
    runners.write().await.get_mut(&session.id).unwrap().status = AgentStatus::Completed;
    let operation = Uuid::new_v4();
    let reservation = Arc::new(
        lifetime
            .reserve_reset_admission(&session, lifetime.admission_epoch(), operation)
            .unwrap(),
    );
    assert_eq!(reservation.operation_id(), operation);
    reservation.check_session(&session).unwrap();
    assert_eq!(
        reservation.require_physical_closure().unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    let physical_holder = reservation.clone();
    drop(reservation); // dropped HTTP/request owner must not release a held job
    {
        let mut runners = runners.write().await;
        assert!(
            super::super::runner_lifecycle::remove_runner_entry(&mut runners, &session.id)
                .await
                .is_none()
        );
        assert!(runners
            .get(&session.id)
            .unwrap()
            .root_reset_admission_reserved());
        assert!(!runners
            .get(&session.id)
            .unwrap()
            .cancel_token
            .is_cancelled());
    }
    assert!(matches!(
        reserve_runner_core(&runners, &senders, &session.id, &events).await,
        ReserveOutcome::AlreadyRunning(_)
    ));
    physical_holder.check_current().unwrap();
    drop(physical_holder);
    assert!(matches!(
        reserve_runner_core(&runners, &senders, &session.id, &events).await,
        ReserveOutcome::Reserved(_)
    ));
    assert_eq!(lifetime.admission_epoch(), 2);
    assert!(
        admission.attach(&session).is_err(),
        "old handoff cannot refresh epoch"
    );
}

#[tokio::test]
async fn unique_slot_removal_invalidates_tokens_but_snapshot_clone_does_not() {
    let mut runner = AgentRunner::new();
    let lifetime = runner.root_execution.clone();
    let session = session();
    joined(&lifetime, &session).await;
    let snapshot = runner.clone();
    assert!(snapshot
        .reserve_root_reset_admission(&session, lifetime.admission_epoch(), Uuid::new_v4())
        .is_err());
    drop(snapshot);
    let reset = runner
        .reserve_root_reset_admission(&session, lifetime.admission_epoch(), Uuid::new_v4())
        .unwrap();
    reset.check_current().unwrap();
    drop(runner); // same Drop used by idle sweep / raw remove / dev clear
    assert!(reset.check_current().is_err());
    let replacement = AgentRunner::new();
    assert_eq!(
        replacement.root_execution.outer_closure(),
        RootOuterClosure::Unknown
    );
    assert!(replacement
        .root_execution
        .reserve_reset_admission(&session, 0, Uuid::new_v4())
        .is_err());
    // A normal replacement preserves lineage without disarming an old reset.
    runner = AgentRunner::new();
    let lifetime = runner.root_execution.clone();
    let mut successor = AgentRunner::new();
    successor.root_execution = lifetime.clone();
    successor.transfer_root_slot(Some(&mut runner));
    drop(runner);
    assert!(!lifetime.closed.load(Ordering::Acquire));
    drop(successor);
    assert!(lifetime.closed.load(Ordering::Acquire));
}

#[tokio::test]
async fn unknown_handoff_panic_and_incomplete_tail_cannot_become_physical_closure() {
    let session = session();
    for mode in ["panic", "incomplete", "unobserved", "actor-error"] {
        let lifetime = Arc::new(RootExecutionLifetime::default());
        if mode == "unobserved" {
            lifetime.admit("unobserved-start").unwrap();
            joined(&lifetime, &session).await;
        } else {
            let admission = lifetime.admit(mode).unwrap();
            let outer = admission.attach(&session).unwrap();
            let tail = outer.clone();
            outer.retain(tokio::spawn(async move {
                if mode == "panic" {
                    panic!("synthetic outer failure");
                }
                if mode == "actor-error" {
                    tail.actor_finished(Err(io::Error::other("synthetic Actor finish failure")));
                    tail.tail_completed();
                }
                // Returning without the real final tail is not a completion.
            }));
        }
        assert!(matches!(
            settled(&lifetime).await,
            RootOuterClosure::Unconfirmed(_)
        ));
        assert!(lifetime
            .reserve_reset_admission(&session, lifetime.admission_epoch(), Uuid::new_v4())
            .is_err());
        assert!(lifetime.close_and_join().await.is_err());
    }
}

#[tokio::test]
async fn cancelled_shutdown_waiter_keeps_real_join_and_does_not_cancel_independent_work() {
    let lifetime = Arc::new(RootExecutionLifetime::default());
    let admission = lifetime.admit("retained-root").unwrap();
    let outer = admission.attach(&session()).unwrap();
    let release = Arc::new(Notify::new());
    let tail = outer.clone();
    let wait = release.clone();
    outer.retain(tokio::spawn(async move {
        wait.notified().await;
        tail.tail_completed();
    }));
    let independent_release = Arc::new(Notify::new());
    let independent_wait = independent_release.clone();
    let independent = tokio::spawn(async move {
        independent_wait.notified().await;
    });
    let owner = lifetime.clone();
    let waiter = tokio::spawn(async move { owner.close_and_join().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !lifetime.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(lifetime.outer_closure(), RootOuterClosure::Pending);
    assert!(lifetime.admit("new-root").is_err());
    assert!(!independent.is_finished());
    release.notify_one();
    lifetime.close_and_join().await.unwrap();
    assert_eq!(
        lifetime.outer_closure(),
        RootOuterClosure::JoinedResourcesUnavailable {
            actor_finished: false
        }
    );
    assert!(!independent.is_finished());
    independent_release.notify_one();
    independent.await.unwrap();
}

fn next_conversation(source: &Session) -> Session {
    let mut current = source.clone();
    current.conversation_reset = Some(bamboo_domain::ConversationResetState {
        schema_version: 1,
        conversation_revision: 1,
        receipts: vec![bamboo_domain::ConversationResetReceipt {
            operation_id: format!("0:{}", Uuid::new_v4()),
            birth_token: source.root_mode_birth_token(),
            expected_snapshot_sha256: "a".repeat(64),
            previous_revision: 0,
            conversation_revision: 1,
            retained_system_messages: 0,
            reset_at: source.updated_at,
        }],
    });
    current
}

#[tokio::test]
async fn reset_observation_keeps_original_birth_project_and_full_ledger() {
    for original in [session(), next_conversation(&session())] {
        let lifetime = Arc::new(RootExecutionLifetime::default());
        joined(&lifetime, &original).await;
        let reset = lifetime
            .reserve_reset_admission(&original, lifetime.admission_epoch(), Uuid::new_v4())
            .unwrap();
        for change in ["birth", "project", "ledger"] {
            let mut changed = original.clone();
            match change {
                "birth" => changed.id.push_str("-reborn"),
                "project" => changed.set_project_id_meta("other-project"),
                _ if changed.conversation_reset.is_none() => changed = next_conversation(&original),
                _ => {
                    changed.conversation_reset.as_mut().unwrap().receipts[0]
                        .expected_snapshot_sha256 = "b".repeat(64)
                }
            }
            assert!(reset.check_session(&changed).is_err());
        }
        let mut appended = original;
        appended.add_message(bamboo_domain::Message::user("same conversation"));
        reset.check_session(&appended).unwrap();
        assert_eq!(
            reset.require_physical_closure().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}

struct NoTools;
#[async_trait::async_trait]
impl bamboo_agent_core::tools::ToolExecutor for NoTools {
    async fn execute(
        &self,
        _: &bamboo_agent_core::tools::ToolCall,
    ) -> Result<bamboo_agent_core::tools::ToolResult, bamboo_agent_core::tools::ToolError> {
        panic!("no tool in synthetic Root closure");
    }
    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        vec![]
    }
}
struct NoProvider;
#[async_trait::async_trait]
impl bamboo_llm::LLMProvider for NoProvider {
    async fn chat_stream(
        &self,
        _: &[bamboo_agent_core::Message],
        _: &[bamboo_agent_core::tools::ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        panic!("cancelled synthetic Root must not call provider");
    }
}

#[tokio::test]
async fn actual_root_spawn_terminal_badge_precedes_real_join_and_actor_finish() {
    use crate::runtime::execution::{agent_spawn::*, history_commit_barrier};
    use bamboo_domain::Storage;
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(home.path().into())
            .await
            .unwrap(),
    );
    let session = session();
    store.save_session(&session).await.unwrap();
    let cache: crate::SessionCache = Arc::default();
    let repository = crate::SessionRepository::new(
        cache.clone(),
        store.clone(),
        Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
    );
    let agent = Arc::new(
        crate::Agent::builder()
            .storage(store.clone())
            .persistence(Arc::new(repository))
            .attachment_reader(store.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    home.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(RwLock::new(bamboo_llm::Config::default())))
            .provider(Arc::new(NoProvider))
            .default_tools(Arc::new(NoTools))
            .build()
            .unwrap(),
    );
    let runners = Arc::new(RwLock::new(HashMap::new()));
    let senders = Arc::default();
    let (events, _) = broadcast::channel(32);
    let SessionExecutionReserveOutcome::Reserved(mut reservation) =
        reserve_session_execution(&agent, &runners, &senders, &session.id, &events).await
    else {
        panic!("actual reserve");
    };
    reservation.bind_root_actor(&agent, &session).await.unwrap();
    assert!(
        reservation.root_actor_writer().is_some(),
        "fixture must use the real Root Actor finish path"
    );
    reservation.cancel_token().cancel(); // this fresh synthetic fixture only
    let lifetime = runners
        .read()
        .await
        .get(&session.id)
        .unwrap()
        .root_execution
        .clone();
    let pause = Arc::new(RootTailPause::default());
    *lifetime.before_actor_finish.lock().unwrap() = Some(pause.clone());
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let (ack, barrier) = history_commit_barrier();
    let drain = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if matches!(
                event,
                bamboo_agent_core::AgentEvent::SessionHistoryCommitted { .. }
            ) {
                ack.acknowledge();
            }
        }
    });
    spawn_session_execution(SessionExecutionArgs {
        agent,
        session_id: session.id.clone(),
        session: session.clone(),
        execution_reservation: reservation,
        tools_override: None,
        provider_override: None,
        model_roster: crate::runtime::model_roster::ModelRoster {
            model: Some("model".into()),
            ..Default::default()
        },
        reasoning_effort: None,
        reasoning_effort_source: "fixture".into(),
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
        app_data_dir: Some(home.path().into()),
        run_budget: None,
        runners: runners.clone(),
        sessions_cache: cache,
        on_complete: None,
        child_completion_handler: None,
    });
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        runners.read().await.get(&session.id).unwrap().status,
        AgentStatus::Cancelled
    ));
    assert_eq!(lifetime.outer_closure(), RootOuterClosure::Pending);
    assert!(lifetime
        .reserve_reset_admission(&session, lifetime.admission_epoch(), Uuid::new_v4())
        .is_err());
    pause.release.notify_one();
    assert_eq!(
        settled(&lifetime).await,
        RootOuterClosure::JoinedResourcesUnavailable {
            actor_finished: true
        }
    );
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .unwrap()
        .unwrap();
    let reset = runners
        .read()
        .await
        .get(&session.id)
        .unwrap()
        .reserve_root_reset_admission(&session, lifetime.admission_epoch(), Uuid::new_v4())
        .unwrap();
    reset.check_current().unwrap();
    assert_eq!(
        reset.require_physical_closure().unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    drop(reset);
    lifetime.close_and_join().await.unwrap();
    store.flush_search_index().await;
}
