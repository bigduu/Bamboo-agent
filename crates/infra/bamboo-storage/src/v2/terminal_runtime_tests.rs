use super::*;
use crate::v2::{actor_directory_lifetime_tests::ActorWriteHook, DurableWritePhase};
use bamboo_domain::{
    ChildWaitPolicy, Storage, SuspensionState, WaitingForBashState, WaitingForChildrenState,
};

async fn fixture(
    initial: AgentStatusState,
) -> (
    tempfile::TempDir,
    Arc<SessionStoreV2>,
    Session,
    ActorActivation,
) {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let parent = Session::new("terminal-parent", "model");
    store.save_session(&parent).await.unwrap();
    let mut child = Session::new_child_of("terminal-child", &parent, "model", "Child");
    child.add_message(bamboo_domain::Message::user("retained retry input"));
    let mut state = AgentRuntimeState::new(&child.id);
    state.status = initial;
    state.round.current_round = 8;
    state.tools.active_tool_calls = 1;
    child.agent_runtime_state = Some(state);
    child.set_last_run_status("running");
    child.updated_at = Utc::now() - chrono::Duration::minutes(5);
    store.save_session(&child).await.unwrap();
    let now = Utc::now();
    let activation = store
        .claim_activation(&claim(&child.id, "host-A", now))
        .await
        .unwrap();
    let activation = store
        .start_activation(&activation.fence(), now)
        .await
        .unwrap();
    (home, store, child, activation)
}

fn claim(id: &str, run: &str, now: DateTime<Utc>) -> ActorActivationClaim {
    ActorActivationClaim {
        actor_id: id.into(),
        run_id: run.into(),
        lease_owner: run.into(),
        lease_expires_at: now + chrono::Duration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}

fn runtime_path(home: &Path) -> PathBuf {
    home.join("sessions/terminal-parent/children/terminal-child/runtime.json")
}

fn main_path(home: &Path) -> PathBuf {
    runtime_path(home).with_file_name("session.json")
}

async fn assert_terminal(
    store: &SessionStoreV2,
    expected: AgentStatusState,
    status: &str,
) -> Session {
    let child = store.load_session("terminal-child").await.unwrap().unwrap();
    let state = child.agent_runtime_state.as_ref().unwrap();
    assert_eq!(state.status, expected);
    assert_eq!(state.round.current_round, 8, "projection keeps accounting");
    assert_eq!(state.tools.active_tool_calls, 0);
    assert_eq!(child.last_run_status().as_deref(), Some(status));
    let mirror: AgentRuntimeState =
        serde_json::from_str(&child.metadata["agent.runtime.state"]).unwrap();
    assert_eq!(mirror, *state);
    assert_eq!(
        store
            .get_index_entry(&child.id)
            .await
            .unwrap()
            .last_run_status
            .as_deref(),
        Some(status)
    );
    child
}

#[tokio::test]
async fn terminal_runtime_finish_matrix_reopens_and_retries_without_rewriting_authority() {
    for initial in [AgentStatusState::Initializing, AgentStatusState::Running] {
        for (finish, typed, status) in [
            (
                ActorActivationFinish::Succeeded,
                AgentStatusState::Completed,
                "completed",
            ),
            (
                ActorActivationFinish::Cancelled,
                AgentStatusState::Cancelled,
                "cancelled",
            ),
            (
                ActorActivationFinish::Failed,
                AgentStatusState::Failed,
                "error",
            ),
        ] {
            let (home, store, _, activation) = fixture(initial).await;
            let main = std::fs::read(main_path(home.path())).unwrap();
            let committed = store
                .finish_activation(&activation.fence(), Utc::now(), finish)
                .await
                .unwrap();
            assert_terminal(&store, typed, status).await;
            let authority_path = runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE);
            let authority = std::fs::read(&authority_path).unwrap();
            let runtime = std::fs::read(runtime_path(home.path())).unwrap();
            drop(store);
            let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
            let retry = reopened
                .finish_activation(&activation.fence(), Utc::now(), finish)
                .await
                .unwrap();
            assert_eq!(retry, committed);
            assert_terminal(&reopened, typed, status).await;
            assert_eq!(authority, std::fs::read(authority_path).unwrap());
            assert_eq!(runtime, std::fs::read(runtime_path(home.path())).unwrap());
            assert_eq!(
                main,
                std::fs::read(main_path(home.path())).unwrap(),
                "repair never rewrites transcript/Main"
            );
            let opposite = if finish == ActorActivationFinish::Failed {
                ActorActivationFinish::Cancelled
            } else {
                ActorActivationFinish::Failed
            };
            assert!(reopened
                .finish_activation(&activation.fence(), Utc::now(), opposite)
                .await
                .is_err());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_runtime_authority_commit_survives_projection_io_fault_and_reopen() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let (home, store, child, activation) = fixture(AgentStatusState::Initializing).await;
        let hook = ActorWriteHook::install(&store, "runtime.json", phase, true);
        hook.release();
        assert!(store
            .finish_activation(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Cancelled
            )
            .await
            .is_err());
        let entry: ActorDirectoryEntry = serde_json::from_slice(
            &std::fs::read(runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            entry.activation.as_ref().unwrap().status,
            ActorActivationStatus::Cancelled
        );
        let authority =
            std::fs::read(runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE)).unwrap();
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        let expected = reopened.load_session(&child.id).await.unwrap().unwrap();
        let repaired = reopened
            .reconcile_child_runtime(&expected, Utc::now(), Some("restart"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            repaired.last_run_status().as_deref(),
            Some("cancelled"),
            "trusted cancellation wins over generic orphan error"
        );
        assert_terminal(&reopened, AgentStatusState::Cancelled, "cancelled").await;
        reopened
            .finish_activation(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        assert_eq!(
            authority,
            std::fs::read(runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE)).unwrap()
        );
    }
}

#[tokio::test]
async fn terminal_runtime_repairs_existing_terminal_initializing_record_without_migration() {
    let (home, store, _, activation) = fixture(AgentStatusState::Initializing).await;
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled,
        )
        .await
        .unwrap();
    let main = std::fs::read(main_path(home.path())).unwrap();
    let mut raw: serde_json::Value =
        serde_json::from_slice(&std::fs::read(runtime_path(home.path())).unwrap()).unwrap();
    raw["agent_runtime_state"]["status"] = "initializing".into();
    raw["agent_runtime_state"]["tools"]["active_tool_calls"] = 1.into();
    std::fs::write(runtime_path(home.path()), serde_json::to_vec(&raw).unwrap()).unwrap();
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
    let expected = reopened
        .load_session("terminal-child")
        .await
        .unwrap()
        .unwrap();
    reopened
        .reconcile_child_runtime(&expected, Utc::now(), None)
        .await
        .unwrap()
        .unwrap();
    let repaired = assert_terminal(&reopened, AgentStatusState::Cancelled, "cancelled").await;
    let bytes = std::fs::read(runtime_path(home.path())).unwrap();
    reopened
        .reconcile_child_runtime(&repaired, Utc::now(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, std::fs::read(runtime_path(home.path())).unwrap());
    assert_eq!(main, std::fs::read(main_path(home.path())).unwrap());
}

#[tokio::test]
async fn terminal_runtime_old_final_save_checkpoint_finish_and_recovery_cannot_overwrite_successor()
{
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    let mut old = store.load_session(&child.id).await.unwrap().unwrap();
    old.set_last_run_status("error");
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let latest = store.load_session(&child.id).await.unwrap().unwrap();
    store.begin_host_run(&latest, "host-B").await.unwrap();
    let now = Utc::now();
    let successor = store
        .claim_activation(&claim(&child.id, "host-B", now))
        .await
        .unwrap();
    store
        .start_activation(&successor.fence(), now)
        .await
        .unwrap();
    let expected = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(expected.host_run_id(), Some("host-B"));
    assert_eq!(
        expected.agent_runtime_state.as_ref().unwrap().run_id,
        child.id
    );
    assert_eq!(
        expected.agent_runtime_state.as_ref().unwrap().execution_id,
        None
    );
    assert!(store.save_session(&old).await.is_err());
    assert!(store.save_runtime_state(&old).await.is_err());
    let locked = crate::LockedSessionStore::new(store.clone());
    assert!(locked.merge_save_runtime(&mut old).await.is_err());
    assert!(store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded
        )
        .await
        .is_err());
    assert!(store
        .reconcile_child_runtime(&latest, Utc::now(), None)
        .await
        .unwrap()
        .is_none());
    assert!(store.begin_host_run(&latest, "host-C").await.is_err());
    let current = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(current).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[tokio::test]
async fn terminal_runtime_waits_are_not_completed_by_success_or_orphan_recovery() {
    for kind in [
        "suspended",
        "children",
        "bash",
        "suspension",
        "user",
        "question",
    ] {
        let (_home, store, mut child, activation) = fixture(AgentStatusState::Running).await;
        let state = child.agent_runtime_state.as_mut().unwrap();
        match kind {
            "suspended" => state.status = AgentStatusState::Suspended,
            "children" => {
                state.waiting_for_children = Some(WaitingForChildrenState::for_children(
                    vec!["grandchild".into()],
                    ChildWaitPolicy::All,
                    Utc::now(),
                ))
            }
            "bash" => {
                state.waiting_for_bash = Some(WaitingForBashState {
                    bash_ids: vec!["shell".into()],
                    registered_at: Utc::now(),
                    timeout_at: None,
                })
            }
            "suspension" => {
                state.suspension = Some(SuspensionState {
                    reason: "awaiting_clarification".into(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: None,
                })
            }
            "user" => {
                child.metadata.insert(
                    "runtime.suspend_reason".into(),
                    "awaiting_clarification".into(),
                );
            }
            "question" => child.set_pending_question(
                "ask-1".into(),
                "ask_user".into(),
                "Continue?".into(),
                vec![],
                true,
            ),
            _ => unreachable!(),
        }
        store.save_session(&child).await.unwrap();
        let expected = store.load_session(&child.id).await.unwrap().unwrap();
        store
            .finish_activation(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Succeeded,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap()
                .agent_runtime_state,
            expected.agent_runtime_state
        );
        assert!(store
            .reconcile_child_runtime(&expected, Utc::now(), None)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_runtime_host_final_save_io_fault_reopens_to_trusted_outcome() {
    use crate::v2::default_actor_context_tests::DefaultWriteHook;
    for (status, outcome, typed) in [
        (
            "completed",
            ActorActivationFinish::Succeeded,
            AgentStatusState::Completed,
        ),
        (
            "cancelled",
            ActorActivationFinish::Cancelled,
            AgentStatusState::Cancelled,
        ),
        (
            "error",
            ActorActivationFinish::Failed,
            AgentStatusState::Failed,
        ),
    ] {
        for file in ["runtime.json", "session.json"] {
            let (home, store, child, activation) = fixture(AgentStatusState::Initializing).await;
            store.begin_host_run(&child, "host-A").await.unwrap();
            let mut final_snapshot = store.load_session(&child.id).await.unwrap().unwrap();
            final_snapshot.set_last_run_status(status);
            final_snapshot.synchronize_terminal_runtime_status();
            let host = crate::LockedSessionStore::new(store.clone());
            let hook =
                DefaultWriteHook::install(&store, file, DurableWritePhase::BeforeReplace, true);
            hook.release();
            assert!(host.merge_save_runtime(&mut final_snapshot).await.is_err());
            // The fault is in actual Host filesystem publication. Only the
            // independently trusted exact activation result may close it.
            store
                .finish_activation(&activation.fence(), Utc::now(), outcome)
                .await
                .unwrap();
            drop(host);
            drop(store);
            let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
            let current = reopened.load_session(&child.id).await.unwrap().unwrap();
            reopened
                .reconcile_child_runtime(&current, Utc::now(), Some("restart"))
                .await
                .unwrap()
                .unwrap();
            assert_terminal(&reopened, typed, status).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_runtime_real_filesystem_final_save_serializes_with_successor_start() {
    use crate::v2::default_actor_context_tests::DefaultWriteHook;
    struct Release(Arc<DefaultWriteHook>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let (home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let mut completed = store.load_session(&child.id).await.unwrap().unwrap();
    completed.set_last_run_status("completed");
    completed.synchronize_terminal_runtime_status();
    store.save_session(&completed).await.unwrap();
    let second = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let expected = store.load_session(&child.id).await.unwrap().unwrap();
    let hook = DefaultWriteHook::install(
        &store,
        "runtime.json",
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let host = crate::LockedSessionStore::new(store.clone());
    let mut duplicate = expected.clone();
    let old_save = tokio::spawn(async move { host.merge_save_runtime(&mut duplicate).await });
    let barrier = hook.clone();
    tokio::task::spawn_blocking(move || barrier.wait())
        .await
        .unwrap();
    let mut starting =
        tokio::spawn(async move { second.begin_host_run(&expected, "host-B").await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut starting)
            .await
            .is_err(),
        "a second Store must wait for the actual old filesystem publication"
    );
    hook.release();
    old_save.await.unwrap().unwrap();
    starting.await.unwrap().unwrap();
    let final_state = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(final_state.host_run_id(), Some("host-B"));
    assert_eq!(final_state.last_run_status().as_deref(), Some("running"));
    assert_eq!(
        final_state.agent_runtime_state.unwrap().status,
        AgentStatusState::Initializing
    );
}

#[tokio::test]
async fn terminal_runtime_pending_retry_generation_survives_old_finish_and_reconciliation() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    let mut completed = store.load_session(&child.id).await.unwrap().unwrap();
    completed.set_last_run_status("cancelled");
    store.save_session(&completed).await.unwrap();
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled,
        )
        .await
        .unwrap();
    let old = store.load_session(&child.id).await.unwrap().unwrap();
    let mut queued = old.clone();
    queued.advance_child_launch_generation().unwrap();
    queued.set_last_run_status("pending");
    store.save_session(&queued).await.unwrap();
    let queued = store.load_session(&child.id).await.unwrap().unwrap();
    assert!(store
        .reconcile_child_runtime(&queued, Utc::now(), None)
        .await
        .unwrap()
        .is_none());
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap()
            .last_run_status()
            .as_deref(),
        Some("pending")
    );
    assert!(store.save_session(&old).await.is_err());
    assert!(store.save_runtime_state(&old).await.is_err());
    store.begin_host_run(&queued, "host-B").await.unwrap();
    let now = Utc::now();
    let next = store
        .claim_activation(&claim(&child.id, "host-B", now))
        .await
        .unwrap();
    store.start_activation(&next.fence(), now).await.unwrap();
    let resumed = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(resumed.host_run_id(), Some("host-B"));
    assert!(resumed.host_run_matches_launch());
    assert_eq!(
        resumed
            .messages
            .iter()
            .filter(|m| m.content == "retained retry input")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_runtime_uncertain_finish_readback_preserves_outcome_before_real_host_save() {
    let (home, store, mut child, activation) = fixture(AgentStatusState::Initializing).await;
    let hook = ActorWriteHook::install(
        &store,
        "runtime.json",
        DurableWritePhase::BeforeReplace,
        true,
    );
    hook.release();
    let confirmed = store
        .finish_activation_observed(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    assert_eq!(confirmed.status, ActorActivationStatus::Succeeded);
    child.set_last_run_status("completed");
    child.synchronize_terminal_runtime_status();
    let host = crate::LockedSessionStore::new(store.clone());
    host.merge_save_runtime(&mut child).await.unwrap();
    drop(host);
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
    assert_terminal(&reopened, AgentStatusState::Completed, "completed").await;
}

#[tokio::test]
async fn terminal_runtime_inflight_host_does_not_publish_terminal_before_broker_source_stage() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Initializing).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let mut staged = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(staged.last_run_status().as_deref(), Some("running"));
    assert_eq!(
        staged.agent_runtime_state.as_ref().unwrap().status,
        AgentStatusState::Initializing
    );
    staged.metadata.insert(
        "runtime.child_completion_source_v1".into(),
        "{\"pending_confirmation\":{}}".into(),
    );
    store.save_runtime_state(&staged).await.unwrap();
    let current = store.load_session(&child.id).await.unwrap().unwrap();
    assert!(store
        .reconcile_child_runtime(&current, Utc::now(), None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(current.last_run_status().as_deref(), Some("running"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_runtime_authority_after_replace_error_requires_durable_exact_retry() {
    let (home, store, _, activation) = fixture(AgentStatusState::Initializing).await;
    let hook = ActorWriteHook::install(
        &store,
        ACTOR_AUTHORITY_FILE,
        DurableWritePhase::AfterReplace,
        true,
    );
    hook.release();
    assert!(
        matches!(
            store
                .finish_activation_observed(
                    &activation.fence(),
                    Utc::now(),
                    ActorActivationFinish::Succeeded
                )
                .await,
            Err(ActorDirectoryError::Storage(_))
        ),
        "readable rename is not a directory fsync proof"
    );
    let authority =
        std::fs::read(runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE)).unwrap();
    let first: ActorDirectoryEntry = serde_json::from_slice(&authority).unwrap();
    let retry = store
        .finish_activation_observed(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    assert_eq!(Some(retry), first.activation);
    assert_eq!(
        authority,
        std::fs::read(runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE)).unwrap()
    );
    assert_terminal(&store, AgentStatusState::Completed, "completed").await;
}

#[tokio::test]
async fn terminal_runtime_finish_preserves_confirmed_timeout_status_error_and_source() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    let mut final_save = store.load_session(&child.id).await.unwrap().unwrap();
    final_save.set_last_run_status("timeout");
    final_save.set_last_run_error("owned child watchdog expired");
    let source = "{\"status\":\"timeout\",\"activation_run_id\":\"host-A\"}";
    final_save
        .metadata
        .insert("runtime.child_completion_source_v1".into(), source.into());
    let host = crate::LockedSessionStore::new(store.clone());
    host.merge_save_runtime(&mut final_save).await.unwrap();
    for _ in 0..2 {
        store
            .finish_activation(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Failed,
            )
            .await
            .unwrap();
        let saved = assert_terminal(&store, AgentStatusState::Failed, "timeout").await;
        assert_eq!(
            saved.last_run_error().as_deref(),
            Some("owned child watchdog expired")
        );
        assert_eq!(saved.metadata["runtime.child_completion_source_v1"], source);
        let repaired = store
            .reconcile_child_runtime(&saved, Utc::now(), Some("restart"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.last_run_status(), saved.last_run_status());
        assert_eq!(repaired.last_run_error(), saved.last_run_error());
        assert_eq!(
            repaired.metadata["runtime.child_completion_source_v1"],
            source
        );
    }
}

#[tokio::test]
async fn terminal_runtime_finish_preserves_skipped_error_and_source() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store.begin_host_run(&child, "host-A").await.unwrap();
    let mut skipped = store.load_session(&child.id).await.unwrap().unwrap();
    skipped.set_last_run_status("skipped");
    skipped.set_last_run_error("No pending message to execute");
    let source = "{\"status\":\"skipped\",\"activation_run_id\":\"host-A\"}";
    skipped
        .metadata
        .insert("runtime.child_completion_source_v1".into(), source.into());
    store.save_session(&skipped).await.unwrap();
    for _ in 0..2 {
        store
            .finish_activation(
                &activation.fence(),
                Utc::now(),
                ActorActivationFinish::Succeeded,
            )
            .await
            .unwrap();
        let saved = assert_terminal(&store, AgentStatusState::Completed, "skipped").await;
        assert_eq!(
            saved.last_run_error().as_deref(),
            Some("No pending message to execute")
        );
        assert_eq!(saved.metadata["runtime.child_completion_source_v1"], source);
        let repaired = store
            .reconcile_child_runtime(&saved, Utc::now(), Some("restart"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.last_run_status(), saved.last_run_status());
        assert_eq!(repaired.last_run_error(), saved.last_run_error());
        assert_eq!(
            repaired.metadata["runtime.child_completion_source_v1"],
            source
        );
    }
}

#[tokio::test]
async fn terminal_runtime_lost_activated_sidecar_is_not_reconstructed_from_stale_main() {
    let (home, store, child, activation) = fixture(AgentStatusState::Running).await;
    let old_main = std::fs::read(main_path(home.path())).unwrap();
    std::fs::remove_file(runtime_path(home.path())).unwrap();
    assert!(store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Cancelled
        )
        .await
        .is_err());
    assert!(!runtime_path(home.path()).exists());
    assert!(store.begin_host_run(&child, "new-run").await.is_err());
    assert!(store
        .reconcile_child_runtime(&child, Utc::now(), None)
        .await
        .is_err());
    assert!(!runtime_path(home.path()).exists());
    assert_eq!(old_main, std::fs::read(main_path(home.path())).unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn terminal_runtime_symlink_authority_witness_cannot_authorize_begin_or_repair() {
    let (home, store, child, _) = fixture(AgentStatusState::Running).await;
    let marker = runtime_path(home.path()).with_file_name(ACTOR_INITIALIZED_FILE);
    let real = marker.with_extension("saved");
    std::fs::rename(&marker, &real).unwrap();
    std::os::unix::fs::symlink(&real, &marker).unwrap();
    let runtime = std::fs::read(runtime_path(home.path())).unwrap();
    assert!(store.begin_host_run(&child, "host-A").await.is_err());
    assert!(store
        .reconcile_child_runtime(&child, Utc::now(), None)
        .await
        .is_err());
    assert_eq!(runtime, std::fs::read(runtime_path(home.path())).unwrap());
}

#[tokio::test]
async fn terminal_runtime_live_lease_is_not_orphaned_and_expiry_revokes_only_logical_authority() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    assert!(store
        .reconcile_child_runtime(&child, Utc::now(), Some("restart"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Running
    );
    // Deterministic old attempt: no sleeping and no physical Worker is claimed.
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Failed,
        )
        .await
        .unwrap();
    let now = Utc::now() - chrono::Duration::minutes(10);
    let old = store
        .claim_activation(&claim(&child.id, "expired", now))
        .await
        .unwrap();
    store.start_activation(&old.fence(), now).await.unwrap();
    let expected = store.load_session(&child.id).await.unwrap().unwrap();
    store
        .reconcile_child_runtime(
            &expected,
            Utc::now(),
            Some("logical expiry; physical stop unknown"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Failed
    );
    assert_terminal(&store, AgentStatusState::Failed, "error").await;
}

#[tokio::test]
async fn terminal_runtime_missing_authority_and_nonterminal_host_remain_unknown() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let parent = Session::new("terminal-parent", "model");
    store.save_session(&parent).await.unwrap();
    let mut child = Session::new_child_of("terminal-child", &parent, "model", "Child");
    child.agent_runtime_state = Some(AgentRuntimeState::new(&child.id));
    child.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
    child.set_last_run_status("running");
    store.save_session(&child).await.unwrap();
    let before = std::fs::read(runtime_path(home.path())).unwrap();
    let current = store.load_session(&child.id).await.unwrap().unwrap();
    assert!(store
        .reconcile_child_runtime(&current, Utc::now(), Some("no local runner"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(before, std::fs::read(runtime_path(home.path())).unwrap());
    assert!(!runtime_path(home.path())
        .with_file_name(ACTOR_AUTHORITY_FILE)
        .exists());
}

#[tokio::test]
async fn terminal_runtime_expiry_cannot_mutate_authority_before_host_conflict_check() {
    for status in ["completed", "cancelled"] {
        let (home, store, child, _) = fixture(AgentStatusState::Running).await;
        store.begin_host_run(&child, "host-A").await.unwrap();
        let mut final_save = store.load_session(&child.id).await.unwrap().unwrap();
        final_save.set_last_run_status(status);
        final_save.synchronize_terminal_runtime_status();
        final_save.metadata.insert(
            "runtime.child_completion_source_v1".into(),
            format!("{{\"status\":\"{status}\",\"activation_run_id\":\"host-A\"}}"),
        );
        store.save_session(&final_save).await.unwrap();
        let path = runtime_path(home.path()).with_file_name(ACTOR_AUTHORITY_FILE);
        let mut actor: ActorDirectoryEntry =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        actor.activation.as_mut().unwrap().lease_expires_at =
            Utc::now() - chrono::Duration::seconds(1);
        actor.validate().unwrap();
        std::fs::write(&path, serde_json::to_vec(&actor).unwrap()).unwrap();
        let authority = std::fs::read(&path).unwrap();
        let runtime = std::fs::read(runtime_path(home.path())).unwrap();
        let current = store.load_session(&child.id).await.unwrap().unwrap();
        assert!(store
            .reconcile_child_runtime(&current, Utc::now(), Some("restart"))
            .await
            .unwrap()
            .is_none());
        assert_eq!(authority, std::fs::read(&path).unwrap());
        assert_eq!(runtime, std::fs::read(runtime_path(home.path())).unwrap());
    }
}

#[tokio::test]
async fn terminal_runtime_replayed_finish_does_not_close_an_idle_next_input_gate() {
    let (home, store, child, activation) = fixture(AgentStatusState::Running).await;
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let mut ready = store.load_session(&child.id).await.unwrap().unwrap();
    ready.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Idle;
    ready.metadata.insert(
        "agent.runtime.state".into(),
        serde_json::to_string(ready.agent_runtime_state.as_ref().unwrap()).unwrap(),
    );
    store.save_runtime_state(&ready).await.unwrap();
    let before = std::fs::read(runtime_path(home.path())).unwrap();
    store
        .finish_activation(
            &activation.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    assert_eq!(before, std::fs::read(runtime_path(home.path())).unwrap());
    let latest = store.load_session(&child.id).await.unwrap().unwrap();
    let repaired = store
        .reconcile_child_runtime(&latest, Utc::now(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repaired.agent_runtime_state.as_ref().unwrap().status,
        AgentStatusState::Idle
    );
    assert_eq!(repaired.last_run_status().as_deref(), Some("completed"));
    // The first reconciliation binds the observed Actor fence. Subsequent
    // repair is byte-idempotent and keeps the already opened input gate.
    let reconciled = std::fs::read(runtime_path(home.path())).unwrap();
    store
        .reconcile_child_runtime(&repaired, Utc::now(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reconciled,
        std::fs::read(runtime_path(home.path())).unwrap()
    );
    // A new failed activation cannot inherit the predecessor's ready gate.
    let now = Utc::now();
    let next = store
        .claim_activation(&claim(&child.id, "legacy-B", now))
        .await
        .unwrap();
    store.start_activation(&next.fence(), now).await.unwrap();
    store
        .finish_activation(&next.fence(), Utc::now(), ActorActivationFinish::Failed)
        .await
        .unwrap();
    assert_terminal(&store, AgentStatusState::Failed, "error").await;
}

#[tokio::test]
async fn terminal_runtime_claimed_successor_refuses_old_saves_before_new_host_marker() {
    for old_marker in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut root = Session::new("startup-gap-root", "model");
        root.set_last_run_status("completed");
        root.synchronize_terminal_runtime_status();
        store.save_session(&root).await.unwrap();
        if old_marker {
            store.begin_host_run(&root, "host-A").await.unwrap();
            root = store.load_session(&root.id).await.unwrap().unwrap();
            root.set_last_run_status("completed");
            root.synchronize_terminal_runtime_status();
            store.save_session(&root).await.unwrap();
        }
        let stale = store.load_session(&root.id).await.unwrap().unwrap();
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&root.id, "host-B", now))
            .await
            .unwrap();
        let activation = store
            .start_activation(&activation.fence(), now)
            .await
            .unwrap();
        let owner = bamboo_domain::RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: root.created_at,
            conversation_generation: root.conversation_generation_observation(),
        };
        let mut startup = stale.clone();
        startup.set_last_run_status("running");
        startup.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
        store
            .save_root_actor_runtime(&owner, &startup, true, Arc::new(|_| {}))
            .await
            .unwrap();
        let path = home.path().join("sessions/startup-gap-root/runtime.json");
        let before = std::fs::read(&path).unwrap();
        for pending_source in [false, true] {
            let mut old = stale.clone();
            if pending_source {
                old.metadata.insert(
                    "runtime.child_completion_source_v1".into(),
                    "{\"pending_confirmation\":{}}".into(),
                );
            }
            assert!(store.save_runtime_state(&old).await.is_err());
            assert!(store.save_session(&old).await.is_err());
            assert_eq!(before, std::fs::read(&path).unwrap());
        }
        let current = store.load_session(&root.id).await.unwrap().unwrap();
        assert_eq!(current.last_run_status().as_deref(), Some("running"));
        assert_eq!(
            current.agent_runtime_state.as_ref().unwrap().status,
            AgentStatusState::Running
        );
        store.begin_host_run(&current, "host-B").await.unwrap();
        let mut running = store.load_session(&root.id).await.unwrap().unwrap();
        running.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
        store
            .save_root_actor_runtime(&owner, &running, true, Arc::new(|_| {}))
            .await
            .unwrap();
        assert_eq!(
            store
                .load_session(&root.id)
                .await
                .unwrap()
                .unwrap()
                .host_run_id(),
            Some("host-B")
        );
    }
}
