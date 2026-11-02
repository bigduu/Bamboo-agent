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
            .reconcile_child_runtime(&expected, Utc::now(), "error", Some("restart"))
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
        .reconcile_child_runtime(&expected, Utc::now(), "error", None)
        .await
        .unwrap()
        .unwrap();
    let repaired = assert_terminal(&reopened, AgentStatusState::Cancelled, "cancelled").await;
    let bytes = std::fs::read(runtime_path(home.path())).unwrap();
    reopened
        .reconcile_child_runtime(&repaired, Utc::now(), "error", None)
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
        .reconcile_child_runtime(&latest, Utc::now(), "error", None)
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
            .reconcile_child_runtime(&expected, Utc::now(), "error", None)
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
            assert_terminal(&reopened, typed, status).await;
            let current = reopened.load_session(&child.id).await.unwrap().unwrap();
            reopened
                .reconcile_child_runtime(&current, Utc::now(), "error", Some("restart"))
                .await
                .unwrap()
                .unwrap();
            assert_terminal(&reopened, typed, status).await;
        }
    }
}

#[tokio::test]
async fn terminal_runtime_live_lease_is_not_orphaned_and_expiry_revokes_only_logical_authority() {
    let (_home, store, child, activation) = fixture(AgentStatusState::Running).await;
    assert!(store
        .reconcile_child_runtime(&child, Utc::now(), "error", Some("restart"))
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
            "error",
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
