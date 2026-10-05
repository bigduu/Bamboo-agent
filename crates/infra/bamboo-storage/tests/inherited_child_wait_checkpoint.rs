//! Narrow regression evidence for a wait inherited by a later reply run.
//! Untagged wait provenance is captured once before the reply run's writes.

use bamboo_domain::storage::Storage;
use bamboo_domain::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, Session, WaitingForChildrenState,
};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Copy, Debug)]
enum Write {
    Ordinary,
    AppendSafe,
    RuntimeOnly,
}
impl Write {
    async fn save(self, store: &LockedSessionStore, incoming: &mut Session) -> Session {
        let published = Arc::new(Mutex::new(None));
        let cache = published.clone();
        let publish = move |saved: &Session, committed: bool| {
            assert!(committed);
            *cache.lock().unwrap() = Some(saved.clone());
        };
        match self {
            Self::Ordinary => store
                .merge_save_runtime_and_publish(incoming, publish)
                .await
                .unwrap(),
            Self::AppendSafe => store
                .checkpoint_runtime_session_and_publish(incoming, publish)
                .await
                .unwrap(),
            Self::RuntimeOnly => store
                .save_runtime_only_and_publish(incoming, |saved| publish(saved, true))
                .await
                .unwrap(),
        }
        let saved = published.lock().unwrap().take().unwrap();
        saved
    }
}

fn wait(child: &str) -> WaitingForChildrenState {
    WaitingForChildrenState::for_children(
        vec![child.into()],
        ChildWaitPolicy::All,
        chrono::Utc::now(),
    )
}
fn armed(id: &str, wait: &WaitingForChildrenState) -> Session {
    let mut session = Session::new(id, "model");
    let mut state = AgentRuntimeState::new("original-run");
    state.status = AgentStatusState::Suspended;
    state.waiting_for_children = Some(wait.clone());
    session.agent_runtime_state = Some(state);
    session.metadata.insert(
        "runtime.suspend_reason".into(),
        "waiting_for_children".into(),
    );
    session.set_last_run_status("suspended");
    session.add_message(bamboo_domain::Message::user("original task"));
    session
}
fn reply(original: &Session) -> Session {
    let mut incoming = original.clone();
    let runtime = incoming.agent_runtime_state.as_mut().unwrap();
    runtime.run_id = "reply-run".into();
    runtime.status = AgentStatusState::Running;
    runtime.suspension = None;
    incoming.metadata.remove("runtime.suspend_reason");
    incoming.add_message(bamboo_domain::Message::user(
        "continue while child finishes",
    ));
    incoming
}
fn current_wait(session: &Session) -> Option<&WaitingForChildrenState> {
    session
        .agent_runtime_state
        .as_ref()
        .and_then(|s| s.waiting_for_children.as_ref())
}
async fn clear(store: &SessionStoreV2, id: &str) {
    let mut durable = store.load_session(id).await.unwrap().unwrap();
    let runtime = durable.agent_runtime_state.as_mut().unwrap();
    runtime.waiting_for_children = None;
    runtime.status = AgentStatusState::Idle;
    runtime.suspension = None;
    durable.metadata.remove("runtime.suspend_reason");
    store.save_runtime_state(&durable).await.unwrap();
    assert!(current_wait(&store.load_session(id).await.unwrap().unwrap()).is_none());
}

/// A sequential transaction barrier: the merge has completed its latest read
/// when it calls the bound writer, but V2 has not acquired its physical lock.
/// An independent Store clears the wait at that exact boundary. No sleeps or
/// scheduling assumptions are involved, and both mutations use real V2 files.
struct ClearBeforePhysicalWrite {
    local: Arc<SessionStoreV2>,
    independent: Arc<SessionStoreV2>,
    armed: AtomicBool,
}
#[async_trait::async_trait]
impl Storage for ClearBeforePhysicalWrite {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            clear(&self.independent, &session.id).await;
        }
        self.local.save_session(session).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.local.load_session(id).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.local.delete_session(id).await
    }
    async fn load_runtime_control_plane(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.local.load_runtime_control_plane(id).await
    }
    async fn save_inherited_child_wait_finalized(
        &self,
        session: &mut Session,
        inherited: &WaitingForChildrenState,
        root_writer: Option<(
            bamboo_domain::RootActorRuntimeWrite,
            bamboo_domain::RootActorRuntimePublisher,
        )>,
    ) -> std::io::Result<()> {
        self.local
            .save_inherited_child_wait_finalized(session, inherited, root_writer)
            .await
    }
    async fn save_runtime_with_inherited_child_wait(
        &self,
        session: &mut Session,
        inherited: &bamboo_domain::InheritedChildWait,
        runtime_only: bool,
        root_writer: Option<(
            bamboo_domain::RootActorRuntimeWrite,
            bamboo_domain::RootActorRuntimePublisher,
        )>,
        input: Option<(
            Arc<dyn bamboo_domain::SessionInboxPort>,
            bamboo_domain::SessionInboxOwnedClaim,
        )>,
    ) -> std::io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            clear(&self.independent, &session.id).await;
        }
        self.local
            .save_runtime_with_inherited_child_wait(
                session,
                inherited,
                runtime_only,
                root_writer,
                input,
            )
            .await
    }
}

async fn prove_clear_survives(write: Write, between_read_and_write: bool) {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let inherited = wait("original-child");
    assert!(inherited.registered_by_tool_call_id.is_none());
    let original = armed("inherited-checkpoint", &inherited);
    storage.save_session(&original).await.unwrap();
    let independent = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let mut incoming = reply(&original);
    let barrier = Arc::new(ClearBeforePhysicalWrite {
        local: storage.clone(),
        independent: independent.clone(),
        armed: AtomicBool::new(between_read_and_write),
    });
    let store = LockedSessionStore::new(barrier)
        .bind_inherited_child_wait(bamboo_domain::InheritedChildWait::capture(&original).unwrap());
    if !between_read_and_write {
        clear(&independent, &original.id).await;
    }

    let cached = write.save(&store, &mut incoming).await;
    let durable = storage.load_session(&original.id).await.unwrap().unwrap();
    for snapshot in [&incoming, &cached, &durable] {
        let state = snapshot.agent_runtime_state.as_ref().unwrap();
        assert_eq!(state.run_id, "reply-run");
        assert_eq!(
            state.status,
            AgentStatusState::Running,
            "intermediate adoption cannot copy final-save status"
        );
    }
    let before = [
        current_wait(&incoming).is_some(),
        current_wait(&cached).is_some(),
        current_wait(&durable).is_some(),
    ];
    incoming.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Completed;
    store
        .merge_save_inherited_child_wait_and_publish(&mut incoming, &inherited, |_, committed| {
            assert!(committed)
        })
        .await
        .unwrap();
    let final_durable = storage.load_session(&original.id).await.unwrap().unwrap();
    let after = [
        current_wait(&incoming).is_some(),
        current_wait(&final_durable).is_some(),
    ];
    eprintln!("{write:?} independent_clear_between_read_and_write={between_read_and_write}: checkpoint caller/cache/durable wait={before:?}; inherited-final caller/durable wait={after:?}");
    assert_eq!(
        before, [false; 3],
        "stale inherited untagged wait resurrected at intermediate checkpoint"
    );
    assert_eq!(
        after, [false; 2],
        "final reconciliation must not retain an intermediate resurrection"
    );
}

#[tokio::test]
async fn inherited_untagged_wait_clear_survives_ordinary_save() {
    prove_clear_survives(Write::Ordinary, false).await;
}
#[tokio::test]
async fn inherited_untagged_wait_clear_survives_append_safe_checkpoint() {
    prove_clear_survives(Write::AppendSafe, false).await;
}
#[tokio::test]
async fn inherited_untagged_wait_clear_between_read_and_ordinary_write_is_atomic() {
    prove_clear_survives(Write::Ordinary, true).await;
}
#[tokio::test]
async fn inherited_untagged_wait_clear_between_read_and_append_safe_write_is_atomic() {
    prove_clear_survives(Write::AppendSafe, true).await;
}

#[tokio::test]
async fn inherited_untagged_wait_clear_between_read_and_runtime_only_write_is_atomic() {
    prove_clear_survives(Write::RuntimeOnly, true).await;
}

#[tokio::test]
async fn first_arm_and_distinct_current_run_untagged_wait_remain_caller_owned() {
    for write in [Write::Ordinary, Write::AppendSafe, Write::RuntimeOnly] {
        for inherited_present in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
            let original_wait = wait("old-child");
            let mut original = armed("current-run-new-wait", &original_wait);
            if !inherited_present {
                original
                    .agent_runtime_state
                    .as_mut()
                    .unwrap()
                    .waiting_for_children = None;
            }
            storage.save_session(&original).await.unwrap();
            let store = LockedSessionStore::new(storage.clone());
            let store = match bamboo_domain::InheritedChildWait::capture(&original) {
                Some(inherited) => store.bind_inherited_child_wait(inherited),
                None => store,
            };
            let mut incoming = reply(&original);
            let new_wait = wait("new-child");
            assert_ne!(new_wait, original_wait);
            incoming
                .agent_runtime_state
                .as_mut()
                .unwrap()
                .waiting_for_children = Some(new_wait.clone());
            clear(&storage, &original.id).await;
            let cache = write.save(&store, &mut incoming).await;
            for saved in [
                &incoming,
                &cache,
                &storage.load_session(&original.id).await.unwrap().unwrap(),
            ] {
                assert_eq!(current_wait(saved), Some(&new_wait));
            }
            store
                .merge_save_inherited_child_wait_and_publish(
                    &mut incoming,
                    &original_wait,
                    |_, committed| assert!(committed),
                )
                .await
                .unwrap();
            assert_eq!(current_wait(&incoming), Some(&new_wait));
        }
    }
}

#[tokio::test]
async fn existing_tagged_wait_clear_survives_intermediate_save() {
    for write in [Write::Ordinary, Write::AppendSafe] {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
        let mut tagged = wait("tagged-child");
        tagged.registered_by_tool_call_id = Some("tool-call".into());
        let original = armed("existing-tagged", &tagged);
        storage.save_session(&original).await.unwrap();
        let mut incoming = reply(&original);
        clear(&storage, &original.id).await;
        let store = LockedSessionStore::new(storage.clone());
        let cached = write.save(&store, &mut incoming).await;
        let durable = storage.load_session(&original.id).await.unwrap().unwrap();
        for saved in [&incoming, &cached, &durable] {
            assert!(current_wait(saved).is_none());
            assert_eq!(
                saved.agent_runtime_state.as_ref().unwrap().status,
                AgentStatusState::Running
            );
        }
    }
}

async fn prove_replacement_survives(write: Write) {
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let inherited = wait("original-child");
    let original = armed("inherited-replacement", &inherited);
    storage.save_session(&original).await.unwrap();
    let mut incoming = reply(&original);
    let replacement = wait("replacement-child");
    let mut latest = original.clone();
    latest
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .waiting_for_children = Some(replacement.clone());
    storage.save_runtime_state(&latest).await.unwrap();
    let store = LockedSessionStore::new(storage.clone())
        .bind_inherited_child_wait(bamboo_domain::InheritedChildWait::capture(&original).unwrap());
    let cached = write.save(&store, &mut incoming).await;
    let durable = storage.load_session(&original.id).await.unwrap().unwrap();
    let intermediate = [
        current_wait(&incoming),
        current_wait(&cached),
        current_wait(&durable),
    ]
    .map(|wait| wait == Some(&replacement));
    incoming.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Completed;
    store
        .merge_save_inherited_child_wait_and_publish(&mut incoming, &inherited, |_, committed| {
            assert!(committed)
        })
        .await
        .unwrap();
    let final_durable = storage.load_session(&original.id).await.unwrap().unwrap();
    let final_replacement = current_wait(&final_durable) == Some(&replacement);
    eprintln!("{write:?}: durable replacement retained in checkpoint caller/cache/durable={intermediate:?}; inherited-final durable_replacement={final_replacement}");
    assert_eq!(
        intermediate, [true; 3],
        "stale inherited wait must not overwrite durable replacement"
    );
    assert!(final_replacement);
}

#[tokio::test]
async fn inherited_untagged_replacement_survives_ordinary_save() {
    prove_replacement_survives(Write::Ordinary).await;
}
#[tokio::test]
async fn inherited_untagged_replacement_survives_append_safe_checkpoint() {
    prove_replacement_survives(Write::AppendSafe).await;
}

#[tokio::test]
async fn inherited_untagged_replacement_survives_runtime_only_save() {
    prove_replacement_survives(Write::RuntimeOnly).await;
}

#[tokio::test]
async fn inherited_clear_preserves_initializing_running_and_unrelated_suspension() {
    use bamboo_domain::SuspensionState;
    for write in [Write::Ordinary, Write::AppendSafe, Write::RuntimeOnly] {
        for status in [AgentStatusState::Initializing, AgentStatusState::Running] {
            let directory = tempfile::tempdir().unwrap();
            let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
            let inherited = wait("original-child");
            let original = armed("inherited-unrelated-suspension", &inherited);
            storage.save_session(&original).await.unwrap();
            let store = LockedSessionStore::new(storage.clone()).bind_inherited_child_wait(
                bamboo_domain::InheritedChildWait::capture(&original).unwrap(),
            );
            let mut incoming = reply(&original);
            let suspension = SuspensionState {
                reason: "pending_approval".into(),
                suspended_at: chrono::Utc::now(),
                resumable: true,
                hook_point: None,
            };
            let runtime = incoming.agent_runtime_state.as_mut().unwrap();
            runtime.status = status;
            runtime.suspension = Some(suspension.clone());
            incoming
                .metadata
                .insert("runtime.suspend_reason".into(), "pending_approval".into());
            incoming.metadata.insert(
                "agent.runtime.state".into(),
                serde_json::to_string(runtime).unwrap(),
            );
            clear(&storage, &original.id).await;
            let cached = write.save(&store, &mut incoming).await;
            let durable = storage.load_session(&original.id).await.unwrap().unwrap();
            for saved in [&incoming, &cached, &durable] {
                let runtime = saved.agent_runtime_state.as_ref().unwrap();
                assert!(runtime.waiting_for_children.is_none());
                assert_eq!(runtime.status, status);
                assert_eq!(runtime.run_id, "reply-run");
                assert_eq!(runtime.suspension.as_ref(), Some(&suspension));
                assert_eq!(saved.metadata["runtime.suspend_reason"], "pending_approval");
                let mirror: AgentRuntimeState =
                    serde_json::from_str(&saved.metadata["agent.runtime.state"]).unwrap();
                assert_eq!(&mirror, runtime);
            }
        }
    }
}

#[tokio::test]
async fn inherited_binding_rejects_mutated_birth_and_aba_without_publication() {
    for write in [Write::Ordinary, Write::AppendSafe, Write::RuntimeOnly] {
        for mutate_incoming in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let storage = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
            let original = armed("inherited-frozen-birth", &wait("original-child"));
            storage.save_session(&original).await.unwrap();
            let store = LockedSessionStore::new(storage.clone()).bind_inherited_child_wait(
                bamboo_domain::InheritedChildWait::capture(&original).unwrap(),
            );
            let mut incoming = reply(&original);
            if mutate_incoming {
                incoming.created_at += chrono::Duration::seconds(1);
            } else {
                storage.delete_session(&original.id).await.unwrap();
                storage
                    .recreate_root_session(&original.id, "replacement-model")
                    .await
                    .unwrap();
            }
            let before = storage.load_session(&original.id).await.unwrap().unwrap();
            let published = Arc::new(AtomicBool::new(false));
            let probe = published.clone();
            let publish = move |_: &Session, _: bool| {
                probe.store(true, Ordering::SeqCst);
            };
            let result = match write {
                Write::Ordinary => {
                    store
                        .merge_save_runtime_and_publish(&mut incoming, publish)
                        .await
                }
                Write::AppendSafe => {
                    store
                        .checkpoint_runtime_session_and_publish(&mut incoming, publish)
                        .await
                }
                Write::RuntimeOnly => {
                    store
                        .save_runtime_only_and_publish(&mut incoming, |saved| publish(saved, true))
                        .await
                }
            };
            let error = result.unwrap_err();
            assert!(error
                .get_ref()
                .is_some_and(|cause| cause.is::<bamboo_domain::SessionAuthorityConflict>()));
            assert!(!published.load(Ordering::SeqCst));
            let after = storage.load_session(&original.id).await.unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(before).unwrap(),
                serde_json::to_value(after).unwrap()
            );
        }
    }
}
