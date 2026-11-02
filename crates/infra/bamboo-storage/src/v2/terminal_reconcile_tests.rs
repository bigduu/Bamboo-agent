//! Real TempDir commits and physical callbacks. No real Session or Worker.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn terminal_repair_terminal_matrix_ignores_boot_age_and_reconfirms_actor() {
    for (finish, status, typed) in [
        (
            ActorActivationFinish::Succeeded,
            "completed",
            AgentStatusState::Completed,
        ),
        (
            ActorActivationFinish::Cancelled,
            "cancelled",
            AgentStatusState::Cancelled,
        ),
        (
            ActorActivationFinish::Failed,
            "error",
            AgentStatusState::Failed,
        ),
    ] {
        let (home, store, _, activation) = fixture(AgentStatusState::Initializing).await;
        store
            .finish_activation(&activation.fence(), Utc::now(), finish)
            .await
            .unwrap();
        let mut zombie = store.load_session("terminal-child").await.unwrap().unwrap();
        zombie.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Initializing;
        zombie.updated_at = Utc::now() + chrono::Duration::seconds(1);
        zombie.metadata.insert(
            "agent.runtime.state".into(),
            serde_json::to_string(zombie.agent_runtime_state.as_ref().unwrap()).unwrap(),
        );
        let held = store.lock_session_lifecycle_exclusive().await.unwrap();
        fs::write(
            runtime_path(home.path()),
            serde_json::to_vec_pretty(&crate::v2::runtime_sidecar_snapshot(&zombie)).unwrap(),
        )
        .await
        .unwrap();
        drop(held);
        let main = fs::read(main_path(home.path())).await.unwrap();
        let hook = ActorWriteHook::install(
            &store,
            ACTOR_AUTHORITY_FILE,
            DurableWritePhase::BeforeReplace,
            false,
        );
        hook.release();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let publish: bamboo_domain::storage::RootActorRuntimePublisher = Arc::new(move |saved| {
            assert_eq!(saved.agent_runtime_state.as_ref().unwrap().status, typed);
            count.fetch_add(1, Ordering::SeqCst);
        });
        let expected = store.load_session(&zombie.id).await.unwrap().unwrap();
        let repaired = store
            .reconcile_session_runtime(
                &expected,
                Utc::now() - chrono::Duration::hours(1),
                Some("must not replace trusted cancellation"),
                publish.clone(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            hook.reconfirmed() > 0,
            "terminal authority needs actual sync, not readable bytes"
        );
        let bytes = fs::read(runtime_path(home.path())).await.unwrap();
        store
            .reconcile_session_runtime(
                &repaired,
                Utc::now() - chrono::Duration::hours(1),
                None,
                publish,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(fs::read(runtime_path(home.path())).await.unwrap(), bytes);
        assert_eq!(fs::read(main_path(home.path())).await.unwrap(), main);
        store.flush_search_index().await;
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        assert_terminal(&reopened, typed, status).await;
    }
}

#[tokio::test]
async fn terminal_repair_unactivated_host_requires_real_main_and_confirms_uncertain_commit() {
    use crate::v2::default_actor_context_tests::DefaultWriteHook;
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let parent = Session::new("terminal-parent", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("terminal-child", &parent, "model", "Child");
        child.agent_runtime_state = Some(AgentRuntimeState::new("host-internal-A"));
        child.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Initializing;
        child.set_last_run_status("running");
        store.save_session(&child).await.unwrap();
        store.begin_host_run(&child, "host-A").await.unwrap();
        let mut terminal = store.load_session(&child.id).await.unwrap().unwrap();
        terminal.set_last_run_status("cancelled");
        // Actual legacy Host final-save split, not an enum mapping fixture.
        let fault = DefaultWriteHook::install(&store, "session.json", phase, true);
        fault.release();
        assert!(store.save_session(&terminal).await.is_err());
        *store.default_write_hook.lock().unwrap() = None;
        let sync = ActorWriteHook::install(
            &store,
            "session.json",
            DurableWritePhase::BeforeReplace,
            false,
        );
        sync.release();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let publish: bamboo_domain::storage::RootActorRuntimePublisher = Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
        });
        let current = store.load_session(&child.id).await.unwrap().unwrap();
        let observed = store
            .reconcile_session_runtime(
                &current,
                Utc::now() - chrono::Duration::hours(1),
                None,
                publish.clone(),
            )
            .await
            .unwrap();
        if phase == DurableWritePhase::BeforeReplace {
            assert!(
                observed.is_none(),
                "leading runtime does not prove Main completion"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            store.save_session(&current).await.unwrap();
            let current = store.load_session(&child.id).await.unwrap().unwrap();
            store
                .reconcile_session_runtime(
                    &current,
                    Utc::now() - chrono::Duration::hours(1),
                    None,
                    publish,
                )
                .await
                .unwrap()
                .unwrap();
        } else {
            assert!(observed.is_some());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(sync.reconfirmed() > 0);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        assert_eq!(
            reopened
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap()
                .agent_runtime_state
                .unwrap()
                .status,
            AgentStatusState::Cancelled
        );
        assert!(!runtime_path(home.path())
            .with_file_name(ACTOR_AUTHORITY_FILE)
            .exists());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_repair_callback_holds_actual_guards_and_stale_a_cannot_replace_b() {
    let (_home, store, _, activation) = fixture(AgentStatusState::Running).await;
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled,
        )
        .await
        .unwrap();
    let expected = store.load_session("terminal-child").await.unwrap().unwrap();
    let (entered, receive) = std::sync::mpsc::sync_channel(1);
    let (release, wait_release) = std::sync::mpsc::sync_channel::<()>(1);
    let wait_release = std::sync::Mutex::new(wait_release);
    let repairing = store.clone();
    let a = expected.clone();
    let repair = tokio::spawn(async move {
        repairing
            .reconcile_session_runtime(
                &a,
                Utc::now(),
                None,
                Arc::new(move |_| {
                    entered.send(()).unwrap();
                    wait_release
                        .lock()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                }),
            )
            .await
    });
    tokio::task::spawn_blocking(move || {
        receive
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
    })
    .await
    .unwrap();
    let next = store.clone();
    let start = expected.clone();
    let b = tokio::spawn(async move { next.begin_host_run(&start, "host-B").await });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(
        !b.is_finished(),
        "cache callback escaped the actual Session/tree guards"
    );
    release.send(()).unwrap();
    repair.await.unwrap().unwrap().unwrap();
    b.await.unwrap().unwrap();
    let now = Utc::now();
    let b = store
        .claim_activation(&claim(&expected.id, "host-B", now))
        .await
        .unwrap();
    store.start_activation(&b.fence(), now).await.unwrap();
    let current = store.load_session(&expected.id).await.unwrap().unwrap();
    assert_eq!(current.host_run_id(), Some("host-B"));
    assert!(store
        .reconcile_session_runtime(
            &expected,
            Utc::now(),
            None,
            Arc::new(|_| panic!("late A callback"))
        )
        .await
        .unwrap()
        .is_none());
    assert!(store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled
        )
        .await
        .is_err());
    assert_eq!(
        serde_json::to_value(store.load_session(&expected.id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(current).unwrap()
    );
}

#[tokio::test]
async fn terminal_repair_cannot_close_marked_host_before_its_real_wait_handoff() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let mut in_flight = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(in_flight.last_run_status().as_deref(), Some("running"));
    assert!(store
        .reconcile_session_runtime(
            &in_flight,
            Utc::now() - chrono::Duration::hours(1),
            None,
            Arc::new(|_| panic!("unconfirmed Host terminal published"))
        )
        .await
        .unwrap()
        .is_none());
    let state = in_flight.agent_runtime_state.as_mut().unwrap();
    state.status = AgentStatusState::Suspended;
    state.suspension = Some(SuspensionState {
        reason: "awaiting_clarification".into(),
        suspended_at: Utc::now(),
        resumable: true,
        hook_point: None,
    });
    in_flight.set_last_run_status("suspended");
    store.save_session(&in_flight).await.unwrap();
    let waiting = store.load_session(&child.id).await.unwrap().unwrap();
    assert!(waiting.has_runtime_wait());
    assert_eq!(
        waiting.agent_runtime_state.as_ref().unwrap().status,
        AgentStatusState::Suspended
    );
    assert!(store
        .reconcile_session_runtime(
            &waiting,
            Utc::now(),
            None,
            Arc::new(|_| panic!("wait terminalized"))
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn terminal_repair_runtime_faults_do_not_publish_before_real_retry() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let (home, store, _, activation) = fixture(AgentStatusState::Initializing).await;
        let authority = ActorWriteHook::install(
            &store,
            ACTOR_AUTHORITY_FILE,
            DurableWritePhase::AfterReplace,
            true,
        );
        authority.release();
        assert!(store
            .finish_activation_observed(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Failed
            )
            .await
            .is_err());
        let runtime = ActorWriteHook::install(&store, "runtime.json", phase, true);
        runtime.release();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let publish: bamboo_domain::storage::RootActorRuntimePublisher = Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
        });
        let expected = store.load_session("terminal-child").await.unwrap().unwrap();
        assert!(store
            .reconcile_session_runtime(&expected, Utc::now(), None, publish.clone())
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let bytes = fs::read(runtime_path(home.path())).await.unwrap();
        let expected = store.load_session("terminal-child").await.unwrap().unwrap();
        let repaired = store
            .reconcile_session_runtime(&expected, Utc::now(), None, publish.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let confirmed = fs::read(runtime_path(home.path())).await.unwrap();
        if phase == DurableWritePhase::AfterReplace {
            assert_eq!(confirmed, bytes);
        } else {
            assert_ne!(
                confirmed, bytes,
                "retry really commits the missing terminal state"
            );
        }
        store
            .reconcile_session_runtime(&repaired, Utc::now(), None, publish)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(runtime.reconfirmed() > 0);
        assert_eq!(
            fs::read(runtime_path(home.path())).await.unwrap(),
            confirmed
        );
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        assert_terminal(&reopened, AgentStatusState::Failed, "error").await;
    }
}
