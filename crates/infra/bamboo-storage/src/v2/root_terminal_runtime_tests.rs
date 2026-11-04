//! Fresh synthetic Roots, actual Actor/Runtime/Main commits, no real Sessions.
use super::*;
use crate::v2::{default_actor_context_tests::DefaultWriteHook, DurableWritePhase};
use bamboo_domain::{
    Message, SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits,
    SessionInboxPort, SessionMessageEnvelope, Storage, SuspensionState,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn root_terminal_unactivated_legacy_host_is_unknown_after_actual_main_fault() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let mut root = Session::new("unactivated-root", "model");
        root.session_version = None;
        root.agent_runtime_state = Some(AgentRuntimeState::new("legacy-run-A"));
        root.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Initializing;
        root.set_last_run_status("running");
        store.save_session(&root).await.unwrap();
        root.set_last_run_status("completed");
        let fault = DefaultWriteHook::install(&store, "session.json", phase, true);
        fault.release();
        assert!(store.save_session(&root).await.is_err());
        *store.default_write_hook.lock().unwrap() = None;
        store.flush_search_index().await;
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        let observed = reopened.load_session(&root.id).await.unwrap().unwrap();
        let path = home.path().join("sessions/unactivated-root/runtime.json");
        let before = fs::read(&path).await.unwrap();
        assert!(reopened
            .reconcile_session_runtime(
                &observed,
                Utc::now(),
                None,
                Arc::new(|_| panic!("unactivated legacy Root has no current run proof"))
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(fs::read(&path).await.unwrap(), before);
        assert!(!path.with_file_name("actor-authority.json").exists());
        reopened.flush_search_index().await;
    }
}

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    root: Session,
    owner: RootActorRuntimeWrite,
    expires: DateTime<Utc>,
}
impl Fixture {
    async fn new(supervised: bool, seconds: i64) -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut root = Session::new("root-terminal-fixture", "model");
        root.set_project_id_meta("synthetic-project");
        root.session_version = None; // compatible legacy requires no migration
        root.add_message(Message::user("retained original input"));
        root.agent_runtime_state = Some(AgentRuntimeState::new(&root.id));
        root.agent_runtime_state
            .as_mut()
            .unwrap()
            .round
            .current_round = 8;
        root.agent_runtime_state
            .as_mut()
            .unwrap()
            .tools
            .active_tool_calls = 1;
        root.set_last_run_status("running");
        store.save_session(&root).await.unwrap();
        let raw = Arc::new(crate::FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let input = if supervised {
            let supervisor: bamboo_domain::SupervisorReference = (&store
                .get_or_create_default_supervisor("model")
                .await
                .unwrap())
                .into();
            let source = store
                .load_session(&supervisor.session_id)
                .await
                .unwrap()
                .unwrap();
            let target = store
                .inspect_supervisor_current_user_target(&supervisor, &root.id)
                .await
                .unwrap();
            let mut message =
                SessionMessageEnvelope::user_input(&root.id, "actual supervised Root input");
            message.id = bamboo_domain::supervisor_current_user_message_id(
                &supervisor,
                &target,
                "terminal-operation",
            );
            message.source = bamboo_domain::SessionMessageSource::Session {
                session_id: supervisor.session_id.clone(),
            };
            message.kind = bamboo_domain::SessionMessageKind::PeerMessage;
            message.correlation_id = Some("terminal-operation".into());
            let receipt = raw
                .deliver_supervisor_current_user_followup(
                    &bamboo_domain::SessionCallerReference::from(&source),
                    &supervisor,
                    &target,
                    &message,
                )
                .await
                .unwrap();
            Some((message, receipt.generation))
        } else {
            None
        };
        let now = Utc::now();
        let expires = now + chrono::Duration::seconds(seconds);
        let activation = store
            .claim_root_activation_observed(
                &ActorActivationClaim {
                    actor_id: root.id.clone(),
                    run_id: "root-run-A".into(),
                    lease_owner: "private-owner-A".into(),
                    lease_expires_at: expires,
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                },
                &root,
            )
            .await
            .unwrap();
        store
            .start_activation(&activation.fence(), now)
            .await
            .unwrap();
        let owner = RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: root.created_at,
            conversation_generation: root.conversation_generation_observation(),
        };
        if let Some((message, generation)) = input {
            let bound = store.bind_root_actor_inbox(&owner, raw.clone()).unwrap();
            let claim = bound
                .claim_owned(
                    &root.id,
                    1,
                    Some(&owner.fence.run_id),
                    &SessionInboxLeaseRequest {
                        consumer: SessionInboxConsumerId::new(),
                        now: Utc::now(),
                        duration: chrono::Duration::seconds(30)
                            .min(expires - Utc::now() - chrono::Duration::milliseconds(100)),
                    },
                )
                .await
                .unwrap()
                .remove(0);
            root.add_message(message.to_provider_message().unwrap());
            root.session_inbox_admission_mut()
                .record(message.id.clone(), generation);
            store
                .save_root_actor_input(&owner, &root, bound, &claim, Arc::new(|_| {}))
                .await
                .unwrap();
        } else {
            store
                .save_root_actor_runtime(&owner, &root, true, Arc::new(|_| {}))
                .await
                .unwrap();
        }
        root = store.load_session(&root.id).await.unwrap().unwrap();
        Self {
            home,
            store,
            root,
            owner,
            expires,
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        self.home
            .path()
            .join("sessions")
            .join(&self.root.id)
            .join(name)
    }
    async fn current(&self) -> Session {
        self.store
            .load_session(&self.root.id)
            .await
            .unwrap()
            .unwrap()
    }
    async fn reconcile(&self, cutoff: DateTime<Utc>) -> io::Result<Option<Session>> {
        self.store
            .reconcile_session_runtime(&self.current().await, cutoff, None, Arc::new(|_| {}))
            .await
    }
    async fn successor(&self, run: &str) -> RootActorRuntimeWrite {
        let current = self.current().await;
        let now = Utc::now();
        let a = self
            .store
            .claim_root_activation_observed(
                &ActorActivationClaim {
                    actor_id: current.id.clone(),
                    run_id: run.into(),
                    lease_owner: run.into(),
                    lease_expires_at: now + chrono::Duration::minutes(5),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                },
                &current,
            )
            .await
            .unwrap();
        self.store.start_activation(&a.fence(), now).await.unwrap();
        RootActorRuntimeWrite {
            fence: a.fence(),
            created_at: current.created_at,
            conversation_generation: current.conversation_generation_observation(),
        }
    }
}
async fn assert_title_preserves_observation(store: &SessionStoreV2, mut session: Session) {
    let before = session.metadata.get(ROOT_OBSERVATION).cloned();
    session.title = "title cannot mint a run observation".into();
    session.title_version += 1;
    session.metadata_version += 1;
    session.updated_at = Utc::now();
    store.save_manual_title(&session).await.unwrap();
    let saved = store.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(saved.metadata.get(ROOT_OBSERVATION), before.as_ref());
}
fn assert_typed(s: &Session, status: &str, typed: AgentStatusState) {
    assert_eq!(s.last_run_status().as_deref(), Some(status));
    let state = s.agent_runtime_state.as_ref().unwrap();
    assert_eq!(state.status, typed);
    assert_eq!(state.round.current_round, 8);
    assert_eq!(state.tools.active_tool_calls, 0);
    let mirror: AgentRuntimeState =
        serde_json::from_str(&s.metadata["agent.runtime.state"]).unwrap();
    assert_eq!(&mirror, state);
    assert_eq!(
        s.host_run_id(),
        None,
        "Root uses its private writer witness, not Child UUID admission"
    );
}
#[tokio::test]
async fn root_terminal_owned_finish_matrix_and_duplicate_reopen_preserve_main() {
    for initial in [AgentStatusState::Initializing, AgentStatusState::Running] {
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
            let f = Fixture::new(false, 300).await;
            let mut running = f.current().await;
            running.agent_runtime_state.as_mut().unwrap().status = initial;
            f.store
                .save_root_actor_runtime(&f.owner, &running, true, Arc::new(|_| {}))
                .await
                .unwrap();
            let main = fs::read(f.path("session.json")).await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = calls.clone();
            let publish: bamboo_domain::storage::RootActorRuntimePublisher = Arc::new(move |s| {
                assert_eq!(s.agent_runtime_state.as_ref().unwrap().status, typed);
                count.fetch_add(1, Ordering::SeqCst);
            });
            let finished = f
                .store
                .finish_root_activation_observed(
                    &f.owner,
                    Utc::now(),
                    finish,
                    Some(publish.clone()),
                )
                .await
                .unwrap();
            let authority = f.store.inspect_actor(&f.root.id).await.unwrap();
            assert_typed(&f.current().await, status, typed);
            assert_eq!(
                f.store
                    .finish_root_activation_observed(&f.owner, Utc::now(), finish, Some(publish))
                    .await
                    .unwrap(),
                finished
            );
            assert_eq!(f.store.inspect_actor(&f.root.id).await.unwrap(), authority);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            let reopened = SessionStoreV2::new(f.home.path().into()).await.unwrap();
            assert_typed(
                &reopened.load_session(&f.root.id).await.unwrap().unwrap(),
                status,
                typed,
            );
            assert_eq!(fs::read(f.path("session.json")).await.unwrap(), main);
            assert!(!f.current().await.metadata[ROOT_OBSERVATION].contains("private-owner-A"));
        }
    }
}
#[tokio::test]
async fn root_terminal_full_save_real_faults_use_only_confirmed_main_outcome() {
    for supervised in [false, true] {
        for (file, phase) in [
            ("runtime.json", DurableWritePhase::BeforeReplace),
            ("runtime.json", DurableWritePhase::AfterReplace),
            ("session.json", DurableWritePhase::BeforeReplace),
            ("session.json", DurableWritePhase::AfterReplace),
        ] {
            let f = Fixture::new(supervised, 300).await;
            let mut final_save = f.current().await;
            final_save.add_message(Message::assistant("exact Root result", None));
            final_save.set_last_run_status("completed");
            final_save.synchronize_terminal_runtime_status();
            let hook = DefaultWriteHook::install(&f.store, file, phase, true);
            hook.release();
            assert!(f
                .store
                .save_finalized_root_actor_runtime(
                    &f.owner,
                    &mut final_save,
                    None,
                    Arc::new(|_| {})
                )
                .await
                .is_err());
            *f.store.default_write_hook.lock().unwrap() = None;
            let main = fs::read(f.path("session.json")).await.unwrap();
            f.store
                .finish_root_activation_observed(
                    &f.owner,
                    Utc::now(),
                    ActorActivationFinish::Failed,
                    None,
                )
                .await
                .unwrap();
            let positive =
                supervised && file == "session.json" && phase == DurableWritePhase::AfterReplace;
            assert_typed(
                &f.current().await,
                if positive { "completed" } else { "error" },
                if positive {
                    AgentStatusState::Completed
                } else {
                    AgentStatusState::Failed
                },
            );
            let reopened = SessionStoreV2::new(f.home.path().into()).await.unwrap();
            let expected = reopened.load_session(&f.root.id).await.unwrap().unwrap();
            reopened
                .reconcile_session_runtime(
                    &expected,
                    Utc::now() - chrono::Duration::hours(1),
                    None,
                    Arc::new(|_| {}),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                fs::read(f.path("session.json")).await.unwrap(),
                main,
                "repair rewrote immutable Main"
            );
        }
    }
}
#[tokio::test]
async fn root_terminal_main_cancel_failure_and_suspension_override_diagnostic_only() {
    for (status, typed, activation) in [
        (
            "cancelled",
            AgentStatusState::Cancelled,
            ActorActivationStatus::Cancelled,
        ),
        (
            "error",
            AgentStatusState::Failed,
            ActorActivationStatus::Failed,
        ),
        (
            "suspended",
            AgentStatusState::Suspended,
            ActorActivationStatus::Succeeded,
        ),
    ] {
        let f = Fixture::new(true, 300).await;
        let mut final_save = f.current().await;
        final_save.set_last_run_status(status);
        if status == "suspended" {
            let state = final_save.agent_runtime_state.as_mut().unwrap();
            state.status = typed;
            state.suspension = Some(SuspensionState {
                reason: "awaiting_clarification".into(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: None,
            });
        } else {
            final_save.synchronize_terminal_runtime_status();
        }
        f.store
            .save_finalized_root_actor_runtime(&f.owner, &mut final_save, None, Arc::new(|_| {}))
            .await
            .unwrap();
        let main = fs::read(f.path("session.json")).await.unwrap();
        let finished = f
            .store
            .finish_root_activation_observed(
                &f.owner,
                Utc::now(),
                ActorActivationFinish::Failed,
                None,
            )
            .await
            .unwrap();
        assert_eq!(finished.status, activation);
        let after = f.current().await;
        assert_eq!(after.last_run_status().as_deref(), Some(status));
        assert_eq!(after.agent_runtime_state.as_ref().unwrap().status, typed);
        if status == "suspended" {
            assert!(after.has_runtime_wait());
        }
        f.reconcile(Utc::now() - chrono::Duration::hours(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(f.path("session.json")).await.unwrap(), main);
    }
}

#[tokio::test]
async fn root_terminal_scope_observation_survives_actor_refresh_without_project_aba() {
    for mode in ["one-ui", "two-ui", "project-aba", "manual-title"] {
        let f = Fixture::new(false, 300).await;
        f.store
            .finish_root_activation_observed(
                &f.owner,
                Utc::now(),
                ActorActivationFinish::Failed,
                None,
            )
            .await
            .unwrap();
        let original_marker = f.current().await.metadata[ROOT_OBSERVATION].clone();
        let steps = if mode == "one-ui" { 1 } else { 2 };
        for step in 0..steps {
            let mut current = f.current().await;
            current.metadata_version += 1;
            current.title = format!("ordinary UI revision {step}");
            if mode == "project-aba" {
                current.set_project_id_meta(if step == 0 {
                    "other-project"
                } else {
                    "synthetic-project"
                });
            }
            current.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Initializing;
            current.metadata.insert(
                "agent.runtime.state".into(),
                serde_json::to_string(current.agent_runtime_state.as_ref().unwrap()).unwrap(),
            );
            if mode == "manual-title" {
                // Seed only the old typed split before the actual title lane.
                let mut zombie = f.current().await;
                zombie.agent_runtime_state = current.agent_runtime_state.clone();
                zombie.metadata.insert(
                    "agent.runtime.state".into(),
                    current.metadata["agent.runtime.state"].clone(),
                );
                f.store.save_runtime_state(&zombie).await.unwrap();
                current.title_version += 1;
                current.title_generated = false;
                current.updated_at = Utc::now();
                let committed = f.store.save_manual_title_committed(&current).await.unwrap();
                f.store
                    .validate_title_observations(&committed)
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(&committed).unwrap(),
                    serde_json::to_value(f.current().await).unwrap()
                );
            } else {
                f.store.save_runtime_state(&current).await.unwrap();
            }
            // A fresh directory observation is not permission to refresh the
            // older writer marker or forget the Project transition history.
            f.store.inspect_actor(&f.root.id).await.unwrap();
            if mode != "manual-title" {
                assert_eq!(
                    f.current().await.metadata[ROOT_OBSERVATION],
                    original_marker
                );
            }
        }
        if mode == "project-aba" {
            let mut title = f.current().await;
            title.title = "title cannot launder Project ABA".into();
            title.title_version += 1;
            title.metadata_version += 1;
            title.updated_at = Utc::now();
            f.store.save_manual_title(&title).await.unwrap();
            assert_eq!(
                f.current().await.metadata[ROOT_OBSERVATION],
                original_marker
            );
        }
        let before = fs::read(f.path("runtime.json")).await.unwrap();
        let repaired = f
            .reconcile(Utc::now() - chrono::Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(
            repaired.is_some(),
            matches!(mode, "one-ui" | "manual-title")
        );
        if matches!(mode, "one-ui" | "manual-title") {
            assert_typed(&f.current().await, "error", AgentStatusState::Failed);
        } else {
            assert!(f
                .store
                .finish_root_activation_observed(
                    &f.owner,
                    Utc::now(),
                    ActorActivationFinish::Failed,
                    None
                )
                .await
                .is_err());
            assert_eq!(fs::read(f.path("runtime.json")).await.unwrap(), before);
        }
    }
}

#[tokio::test]
async fn root_terminal_first_write_failure_is_attributed_and_late_a_cannot_touch_c() {
    let f = Fixture::new(false, 300).await;
    f.store
        .finish_root_activation_observed(&f.owner, Utc::now(), ActorActivationFinish::Failed, None)
        .await
        .unwrap();
    let previous = f.current().await;
    let b = f.successor("root-run-B").await;
    assert_title_preserves_observation(&f.store, f.current().await).await;
    assert!(f
        .store
        .finish_root_activation_observed(&f.owner, Utc::now(), ActorActivationFinish::Failed, None)
        .await
        .is_err());
    assert!(f
        .store
        .reconcile_session_runtime(
            &previous,
            Utc::now() + chrono::Duration::seconds(1),
            None,
            Arc::new(|_| panic!("late A publication"))
        )
        .await
        .unwrap()
        .is_none());
    // No B-owned checkpoint has run. The actual B owner must establish its own
    // lifetime before its terminal fact, rather than leaving A marker/running.
    f.store
        .finish_root_activation_observed(&b, Utc::now(), ActorActivationFinish::Cancelled, None)
        .await
        .unwrap();
    assert_typed(&f.current().await, "cancelled", AgentStatusState::Cancelled);
    let expected = f.current().await;
    let _c = f.successor("root-run-C").await;
    let before = fs::read(f.path("runtime.json")).await.unwrap();
    assert!(f
        .store
        .reconcile_session_runtime(
            &expected,
            Utc::now() + chrono::Duration::seconds(1),
            None,
            Arc::new(|_| panic!("C overwritten"))
        )
        .await
        .unwrap()
        .is_none());
    assert_eq!(fs::read(f.path("runtime.json")).await.unwrap(), before);
}
#[tokio::test]
async fn root_terminal_wait_pending_and_later_idle_gate_are_preserved() {
    for mode in ["wait", "pending", "idle"] {
        let f = Fixture::new(false, 300).await;
        let mut current = f.current().await;
        if mode == "wait" {
            let state = current.agent_runtime_state.as_mut().unwrap();
            state.status = AgentStatusState::Suspended;
            state.suspension = Some(SuspensionState {
                reason: "awaiting_clarification".into(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: None,
            });
            current.set_last_run_status("suspended");
        } else if mode == "pending" {
            current.add_message(Message::user("a new pending input"));
            current.set_last_run_status("pending");
        }
        f.store
            .save_root_actor_runtime(&f.owner, &current, false, Arc::new(|_| {}))
            .await
            .unwrap();
        f.store
            .finish_root_activation_observed(
                &f.owner,
                Utc::now(),
                ActorActivationFinish::Succeeded,
                None,
            )
            .await
            .unwrap();
        if mode == "idle" {
            current = f.current().await;
            current.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Idle;
            current.metadata.insert(
                "agent.runtime.state".into(),
                serde_json::to_string(current.agent_runtime_state.as_ref().unwrap()).unwrap(),
            );
            f.store.save_runtime_state(&current).await.unwrap();
        }
        let before = f.current().await;
        f.reconcile(Utc::now() - chrono::Duration::hours(1))
            .await
            .unwrap();
        let after = f.current().await;
        assert_eq!(
            serde_json::to_value(&after).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
        if mode == "wait" {
            assert_eq!(
                after.agent_runtime_state.as_ref().unwrap().status,
                AgentStatusState::Suspended
            );
        }
        if mode == "pending" {
            assert_eq!(after.last_run_status().as_deref(), Some("pending"));
        }
        if mode == "idle" {
            assert_eq!(
                after.agent_runtime_state.as_ref().unwrap().status,
                AgentStatusState::Idle
            );
        }
    }
}
#[tokio::test]
async fn root_terminal_observation_cannot_be_injected_copied_or_legacy_fallback_on_corruption() {
    let f = Fixture::new(false, 300).await;
    let marker = f.root.metadata[ROOT_OBSERVATION].clone();
    let mut forged = f.current().await;
    forged
        .metadata
        .insert(ROOT_OBSERVATION.into(), "bad injection".into());
    f.store.save_runtime_state(&forged).await.unwrap();
    assert_eq!(f.current().await.metadata[ROOT_OBSERVATION], marker);
    let copied = f
        .store
        .copy_session(&f.root.id, "root-terminal-copy")
        .await
        .unwrap()
        .unwrap();
    assert!(!copied.metadata.contains_key(ROOT_OBSERVATION));
    assert_title_preserves_observation(&f.store, copied).await;
    f.store
        .finish_root_activation_observed(&f.owner, Utc::now(), ActorActivationFinish::Failed, None)
        .await
        .unwrap();
    let mut broken = f.current().await;
    let (rel, _, guards) = f.store.lock_actor_runtime(&f.root.id).await.unwrap();
    broken
        .metadata
        .insert(ROOT_OBSERVATION.into(), "{bad".into());
    f.store
        .write_actor_bytes(
            &f.path("runtime.json"),
            serde_json::to_vec_pretty(&crate::v2::runtime_sidecar_snapshot(&broken)).unwrap(),
            &guards,
        )
        .await
        .unwrap();
    drop(guards);
    let _ = rel;
    let before = fs::read(f.path("runtime.json")).await.unwrap();
    assert!(f.reconcile(Utc::now()).await.is_err());
    assert_eq!(fs::read(f.path("runtime.json")).await.unwrap(), before);
}
#[tokio::test]
async fn root_terminal_main_corrects_same_fence_diagnostic_without_unretiring_or_protecting_b() {
    for retired in [false, true] {
        let f = Fixture::new(true, 300).await;
        let mut completed = f.current().await;
        completed.set_last_run_status("completed");
        completed.synchronize_terminal_runtime_status();
        f.store
            .save_finalized_root_actor_runtime(&f.owner, &mut completed, None, Arc::new(|_| {}))
            .await
            .unwrap();
        // Simulate the older Host's diagnostic after its actual Main committed.
        f.store
            .finish_activation(&f.owner.fence, Utc::now(), ActorActivationFinish::Failed)
            .await
            .unwrap();
        if retired {
            f.store.retire_actor(&f.root.id, Utc::now()).await.unwrap();
        }
        let before = f.store.inspect_actor(&f.root.id).await.unwrap();
        f.reconcile(Utc::now() - chrono::Duration::hours(1))
            .await
            .unwrap()
            .unwrap();
        let after = f.store.inspect_actor(&f.root.id).await.unwrap();
        assert_eq!(
            after.activation.as_ref().unwrap().status,
            ActorActivationStatus::Succeeded
        );
        assert_eq!(
            after.activation.as_ref().unwrap().finished_at,
            before.activation.as_ref().unwrap().finished_at
        );
        assert_eq!(after.revision, before.revision + 1);
        assert_typed(&f.current().await, "completed", AgentStatusState::Completed);
        f.reconcile(Utc::now()).await.unwrap();
        assert_eq!(f.store.inspect_actor(&f.root.id).await.unwrap(), after);
        if retired {
            assert_eq!(after.actor.state, ActorLogicalState::Retired);
            let now = Utc::now();
            assert!(f
                .store
                .claim_activation(&ActorActivationClaim {
                    actor_id: f.root.id.clone(),
                    run_id: "forbidden".into(),
                    lease_owner: "forbidden".into(),
                    lease_expires_at: now + chrono::Duration::minutes(1),
                    inbox_generation: 0,
                    placement_ref: None,
                    now
                })
                .await
                .is_err());
        } else {
            let b = f.successor("root-run-B").await;
            f.store
                .finish_root_activation_observed(
                    &b,
                    Utc::now(),
                    ActorActivationFinish::Failed,
                    None,
                )
                .await
                .unwrap();
            assert_typed(&f.current().await, "error", AgentStatusState::Failed);
        }
    }
}
#[tokio::test]
async fn root_terminal_live_main_requires_expiry_and_fixed_restart_cutoff_renewal_wins() {
    for renewed in [false, true] {
        let f = Fixture::new(true, 10).await;
        let mut completed = f.current().await;
        completed.set_last_run_status("completed");
        completed.synchronize_terminal_runtime_status();
        f.store
            .save_finalized_root_actor_runtime(&f.owner, &mut completed, None, Arc::new(|_| {}))
            .await
            .unwrap();
        assert!(f
            .reconcile(Utc::now() + chrono::Duration::seconds(1))
            .await
            .unwrap()
            .is_none());
        while Utc::now() <= f.expires {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            f.reconcile(Utc::now() - chrono::Duration::hours(1))
                .await
                .unwrap()
                .is_none(),
            "new live Run excluded from pre-startup recovery"
        );
        if renewed {
            f.store
                .renew_activation(
                    &f.owner.fence,
                    Utc::now(),
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap();
        }
        let result = f
            .reconcile(Utc::now() + chrono::Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(result.is_some(), !renewed);
        let status = f
            .store
            .inspect_actor(&f.root.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status;
        assert_eq!(
            status,
            if renewed {
                ActorActivationStatus::Running
            } else {
                ActorActivationStatus::Succeeded
            }
        );
        if !renewed {
            assert!(f
                .store
                .renew_activation(
                    &f.owner.fence,
                    Utc::now(),
                    Utc::now() + chrono::Duration::minutes(5)
                )
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn root_terminal_uncertain_actor_and_runtime_writes_require_real_confirmation() {
    use crate::v2::actor_directory_lifetime_tests::ActorWriteHook;
    for (file, phase) in [
        ("actor-authority.json", DurableWritePhase::BeforeReplace),
        ("actor-authority.json", DurableWritePhase::AfterReplace),
        ("runtime.json", DurableWritePhase::BeforeReplace),
        ("runtime.json", DurableWritePhase::AfterReplace),
    ] {
        let f = Fixture::new(false, 300).await;
        let count = Arc::new(AtomicUsize::new(0));
        let mark = count.clone();
        let hook = ActorWriteHook::install(&f.store, file, phase, true);
        hook.release();
        assert!(f
            .store
            .finish_root_activation_observed(
                &f.owner,
                Utc::now(),
                ActorActivationFinish::Failed,
                Some(Arc::new(move |_| {
                    mark.fetch_add(1, Ordering::SeqCst);
                }))
            )
            .await
            .is_err());
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "uncertain commit cannot publish cache"
        );
        let observed = f.store.inspect_actor(&f.root.id).await.unwrap();
        if file == "actor-authority.json" && phase == DurableWritePhase::BeforeReplace {
            assert_eq!(
                observed.activation.unwrap().status,
                ActorActivationStatus::Running
            );
            assert!(f
                .reconcile(Utc::now() + chrono::Duration::seconds(1))
                .await
                .unwrap()
                .is_none());
            f.store
                .finish_root_activation_observed(
                    &f.owner,
                    Utc::now(),
                    ActorActivationFinish::Failed,
                    None,
                )
                .await
                .unwrap();
        } else {
            assert_eq!(
                observed.activation.unwrap().status,
                ActorActivationStatus::Failed
            );
            f.reconcile(Utc::now() - chrono::Duration::hours(1))
                .await
                .unwrap()
                .unwrap();
            assert!(
                hook.reconfirmed() > 0,
                "actual authority/runtime durability must be confirmed"
            );
        }
        assert_typed(&f.current().await, "error", AgentStatusState::Failed);
        let reopened = SessionStoreV2::new(f.home.path().into()).await.unwrap();
        assert_typed(
            &reopened.load_session(&f.root.id).await.unwrap().unwrap(),
            "error",
            AgentStatusState::Failed,
        );
    }
}

#[tokio::test]
async fn root_terminal_reset_context_cannot_reuse_previous_marker_or_actual_old_owner() {
    use bamboo_domain::{
        conversation_reset_snapshot_sha256, prepare_conversation_reset,
        ConversationResetPreparation, ConversationResetReadSafety, ConversationResetRequest,
    };
    let f = Fixture::new(false, 300).await;
    f.store
        .finish_root_activation_observed(&f.owner, Utc::now(), ActorActivationFinish::Failed, None)
        .await
        .unwrap();
    let old = f.current().await;
    let old_marker = old.metadata[ROOT_OBSERVATION].clone();
    // Synthetic future producer, exactly as the existing generation fixtures.
    // This is not a reset API or a bypass on any real Session.
    let mut source = old.clone();
    // The future producer must first migrate the legacy control-plane mirror;
    // reset deliberately refuses to reinterpret it. Only this synthetic source
    // is changed; the canonical old generation remains untouched below.
    source.metadata.remove("agent.runtime.state");
    let request = ConversationResetRequest {
        session_id: source.id.clone(),
        operation_id: format!("0:{}", Uuid::new_v4()),
        expected_birth_token: source.root_mode_birth_token(),
        expected_conversation_revision: 0,
        expected_snapshot_sha256: conversation_reset_snapshot_sha256(&source).unwrap(),
    };
    let preparation = prepare_conversation_reset(
        &source,
        &Default::default(),
        &request,
        ConversationResetReadSafety::IdleObserved,
        Utc::now(),
    )
    .unwrap();
    let ConversationResetPreparation::Prepared(prepared) = preparation else {
        panic!("synthetic settled ordinary Root reset preparation: {preparation:?}");
    };
    let mut next = prepared.proposed_session;
    next.conversation_reset = Some(prepared.proposed_state);
    next.agent_runtime_state = Some(AgentRuntimeState::new(&next.id));
    next.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Initializing;
    next.set_last_run_status("running");
    next.clear_last_run_error();
    next.metadata.insert(ROOT_OBSERVATION.into(), old_marker);
    f.store.flush_search_index().await;
    let held = f.store.lock_session_lifecycle_exclusive().await.unwrap();
    fs::write(
        f.path("session.json"),
        crate::v2::compact_main::serialize_main(&next).unwrap(),
    )
    .await
    .unwrap();
    fs::write(
        f.path("runtime.json"),
        serde_json::to_vec_pretty(&crate::v2::runtime_sidecar_snapshot(&next)).unwrap(),
    )
    .await
    .unwrap();
    SessionStoreV2::write_staged_root_tool_proof(
        &f.path("session.json").parent().unwrap().to_path_buf(),
        &next,
    )
    .await
    .unwrap();
    drop(held);
    let before = fs::read(f.path("runtime.json")).await.unwrap();
    assert!(f
        .reconcile(Utc::now() + chrono::Duration::seconds(1))
        .await
        .unwrap()
        .is_none());
    assert!(f
        .store
        .finish_root_activation_observed(&f.owner, Utc::now(), ActorActivationFinish::Failed, None)
        .await
        .is_err());
    assert_eq!(fs::read(f.path("runtime.json")).await.unwrap(), before);
    assert_eq!(
        f.current().await.agent_runtime_state.unwrap().status,
        AgentStatusState::Initializing
    );
}
