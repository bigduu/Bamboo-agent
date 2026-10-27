//! Cold synthetic storage fixtures. No provider, account or real-user Session.

use super::*;
use crate::v2::default_actor_context_tests::DefaultWriteHook;
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionCreateScope,
    HostSessionRight, HostSessionScopeBinding, HostSessionScopeToken, Message,
    DEFAULT_SUPERVISOR_SESSION_ID, HOST_SESSION_SCOPE_VERSION,
};
use std::collections::{BTreeMap, BTreeSet};

struct ReleaseHook(Arc<DefaultWriteHook>);
impl Drop for ReleaseHook {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn fixture() -> (tempfile::TempDir, SessionStoreV2, Session) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut caller = Session::new("caller", "model");
    caller.set_project_id_meta("caller-project");
    store.save_session(&caller).await.unwrap();
    (home, store, caller)
}

fn request_candidate(caller: &Session, operation: Uuid, workspace: &Path) -> Session {
    let (_, id) =
        host_session_create_identity(&SessionCallerReference::from(caller), operation).unwrap();
    let mut candidate = Session::new(id, "model");
    candidate.set_project_id_meta("explicit-project");
    candidate.set_workspace_path_meta(workspace.to_string_lossy());
    candidate
}

async fn install(
    store: &SessionStoreV2,
    caller: &Session,
    ancestors: Vec<ActorAncestorObservation>,
    operation: Uuid,
    candidate: &Session,
    previous_revision: u64,
    rights: BTreeSet<HostSessionRight>,
) -> HostSessionScopeToken {
    store
        .install_host_session_scope(
            previous_revision,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: Uuid::new_v4(),
                revision: previous_revision + 1,
                caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
                targets: BTreeMap::new(),
                creates: BTreeMap::from([(
                    operation,
                    HostSessionCreateScope {
                        operation_id: operation,
                        project_id: candidate.project_id_meta().unwrap().parse().unwrap(),
                        workspace_path: candidate.workspace_path_meta().unwrap().into(),
                        created_target_rights: rights,
                    },
                )]),
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn explicit_create_permit_creates_independent_root_for_each_original_role() {
    let (home, store, caller) = fixture().await;
    let mut child = Session::new_child_of("child", &caller, "model", "short child work");
    child.set_project_id_meta(caller.project_id_meta().unwrap());
    store.save_session(&child).await.unwrap();
    store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let supervisor = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let ancestor = ActorAncestorObservation {
        actor_id: caller.id.clone(),
        session_created_at: caller.created_at,
        metadata_version: caller.metadata_version,
    };
    for (source, ancestors) in [
        (&caller, vec![]),
        (&child, vec![ancestor]),
        (&supervisor, vec![]),
    ] {
        let operation = Uuid::new_v4();
        let candidate =
            request_candidate(source, operation, &home.path().join("explicit-workspace"));
        let token = install(
            &store,
            source,
            ancestors,
            operation,
            &candidate,
            0,
            BTreeSet::new(),
        )
        .await;
        store
            .create_host_scoped_root(
                &token,
                &SessionCallerReference::from(source),
                operation,
                Some(1),
                &candidate,
            )
            .await
            .unwrap();
        let created = store.load_session(&candidate.id).await.unwrap().unwrap();
        assert_eq!(created.kind, SessionKind::Root);
        assert_eq!(created.root_session_id, candidate.id);
        assert!(created.parent_session_id.is_none());
        assert!(created.authority_identity.is_ordinary());
        assert!(created.supervisor_management.is_none());
        assert!(created.messages.is_empty());
        assert!(store
            .list_host_scoped_targets(&token, &SessionCallerReference::from(source))
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .load_host_scoped_target(&token, &SessionCallerReference::from(source), &candidate.id)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn exact_retry_preserves_existing_work_and_rejects_a_recreated_birth() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let observed = SessionCallerReference::from(&caller);
    store
        .create_host_scoped_root(&token, &observed, operation, Some(1), &candidate)
        .await
        .unwrap();
    let mut worked = store.load_session(&candidate.id).await.unwrap().unwrap();
    worked.add_message(Message::assistant("independent work survives retry", None));
    store.save_session(&worked).await.unwrap();
    store
        .create_host_scoped_root(&token, &observed, operation, None, &candidate)
        .await
        .unwrap();
    let after = store.load_session(&candidate.id).await.unwrap().unwrap();
    assert_eq!(after.messages.len(), 1);
    assert_eq!(after.messages[0].content, "independent work survives retry");
    let mut different_birth = candidate.clone();
    different_birth.created_at += chrono::Duration::seconds(1);
    assert!(store
        .create_host_scoped_root(&token, &observed, operation, None, &different_birth)
        .await
        .is_err());
    assert_eq!(
        store
            .load_session(&candidate.id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        candidate.created_at
    );
}

#[tokio::test]
async fn project_workspace_operation_and_fresh_revision_are_all_required() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let observed = SessionCallerReference::from(&caller);
    let error = store
        .create_host_scoped_root(&token, &observed, operation, Some(0), &candidate)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    for changed in [
        {
            let mut c = candidate.clone();
            c.set_project_id_meta("another-project");
            c
        },
        {
            let mut c = candidate.clone();
            c.set_workspace_path_meta(home.path().join("another-workspace").to_string_lossy());
            c
        },
        request_candidate(&caller, Uuid::new_v4(), &home.path().join("workspace")),
    ] {
        assert!(store
            .create_host_scoped_root(&token, &observed, operation, Some(1), &changed)
            .await
            .is_err());
        assert!(store.load_session(&changed.id).await.unwrap().is_none());
    }
    assert!(store.load_session(&candidate.id).await.unwrap().is_none());
}

#[tokio::test]
async fn revoked_or_replaced_token_never_authorizes_creation_or_retry() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let observed = SessionCallerReference::from(&caller);
    let old = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let current = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        1,
        BTreeSet::new(),
    )
    .await;
    assert!(store
        .create_host_scoped_root(&old, &observed, operation, None, &candidate)
        .await
        .is_err());
    store
        .create_host_scoped_root(&current, &observed, operation, Some(2), &candidate)
        .await
        .unwrap();
    store
        .revoke_host_session_scope(&caller.id, 2)
        .await
        .unwrap();
    assert!(store
        .create_host_scoped_root(&current, &observed, operation, None, &candidate)
        .await
        .is_err());
    assert_eq!(
        store
            .load_session(&candidate.id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        candidate.created_at
    );
}

#[tokio::test]
async fn creation_never_silently_installs_recorded_target_rights() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::from([HostSessionRight::SendPeer]),
    )
    .await;
    let error = store
        .create_host_scoped_root(
            &token,
            &SessionCallerReference::from(&caller),
            operation,
            Some(1),
            &candidate,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(store.load_session(&candidate.id).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_root_on_an_existing_reservation_requires_explicit_recovery() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let error = store
        .create_host_scoped_root(
            &token,
            &SessionCallerReference::from(&caller),
            operation,
            None,
            &candidate,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(error
        .to_string()
        .contains("pending_root_absent_requires_recovery"));
    assert!(store.load_session(&candidate.id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_different_valid_generation_cannot_use_the_same_stable_creation_scope() {
    let (home, store, caller) = fixture().await;
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let state = caller.conversation_reset.clone().unwrap_or_default();
    let request = bamboo_domain::ConversationResetRequest {
        session_id: caller.id.clone(),
        operation_id: format!("0:{}", Uuid::new_v4()),
        expected_birth_token: caller.root_mode_birth_token(),
        expected_conversation_revision: 0,
        expected_snapshot_sha256: bamboo_domain::conversation_reset_snapshot_sha256(&caller)
            .unwrap(),
    };
    let bamboo_domain::ConversationResetPreparation::Prepared(prepared) =
        bamboo_domain::prepare_conversation_reset(
            &caller,
            &state,
            &request,
            bamboo_domain::ConversationResetReadSafety::IdleObserved,
            caller.updated_at + chrono::Duration::seconds(1),
        )
        .unwrap()
    else {
        panic!("pure reset preparation")
    };
    let mut different_generation = prepared.proposed_session;
    different_generation.conversation_reset = Some(prepared.proposed_state);
    let observed = SessionCallerReference::from(&different_generation);
    assert!(token.binding().caller.identity.matches_caller(&observed));
    let error = store
        .create_host_scoped_root(&token, &observed, operation, Some(1), &candidate)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("operation_caller_changed"));
    assert!(store.load_session(&candidate.id).await.unwrap().is_none());
}

#[tokio::test]
async fn observed_supervisor_creation_uses_its_role_not_a_legacy_host_acl() {
    let (home, store, ordinary) = fixture().await;
    let supervisor = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let reference = bamboo_domain::SupervisorReference::from(&supervisor);
    store
        .mutate_supervisor_management(&bamboo_domain::SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 0,
            mutation: bamboo_domain::SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["explicit-project".parse().unwrap()].into(),
            },
        })
        .await
        .unwrap();
    let supervisor = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let original = SessionCallerReference::from(&supervisor);
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&supervisor, operation, &home.path().join("workspace"));
    let token = install(
        &store,
        &supervisor,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let created = store
        .create_and_attach_supervised_root_observed(&original, &reference, 1, &candidate)
        .await
        .unwrap();
    assert!(created.changed);
    assert_eq!(
        store
            .load_session(&candidate.id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        candidate.created_at
    );
    store
        .revoke_host_session_scope(&supervisor.id, token.binding().revision)
        .await
        .unwrap();
    let replay = store
        .create_and_attach_supervised_root_observed(&original, &reference, 1, &candidate)
        .await
        .unwrap();
    assert!(!replay.changed);
    // The canonical role is checked with the complete original caller. An
    // ordinary Root cannot borrow it by supplying the Supervisor reference.
    assert!(store
        .create_and_attach_supervised_root_observed(
            &SessionCallerReference::from(&ordinary),
            &reference,
            1,
            &candidate
        )
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_supervisor_creation_retains_absence_and_rejects_another_generation() {
    let (home, store, _ordinary) = fixture().await;
    let store = Arc::new(store);
    let supervisor = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let reference = bamboo_domain::SupervisorReference::from(&supervisor);
    store
        .mutate_supervisor_management(&bamboo_domain::SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 0,
            mutation: bamboo_domain::SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["explicit-project".parse().unwrap()].into(),
            },
        })
        .await
        .unwrap();
    let supervisor = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&supervisor, operation, &home.path().join("workspace"));
    let original = SessionCallerReference::from(&supervisor);
    let bamboo_domain::ConversationResetPreparation::Prepared(prepared) =
        bamboo_domain::prepare_conversation_reset(
            &supervisor,
            &supervisor.conversation_reset.clone().unwrap_or_default(),
            &bamboo_domain::ConversationResetRequest {
                session_id: supervisor.id.clone(),
                operation_id: format!("0:{}", Uuid::new_v4()),
                expected_birth_token: supervisor.root_mode_birth_token(),
                expected_conversation_revision: 0,
                expected_snapshot_sha256: bamboo_domain::conversation_reset_snapshot_sha256(
                    &supervisor,
                )
                .unwrap(),
            },
            bamboo_domain::ConversationResetReadSafety::IdleObserved,
            supervisor.updated_at + chrono::Duration::seconds(1),
        )
        .unwrap()
    else {
        panic!("pure reset preparation")
    };
    let mut other_generation = prepared.proposed_session;
    other_generation.conversation_reset = Some(prepared.proposed_state);
    let stale = store
        .create_and_attach_supervised_root_observed(
            &SessionCallerReference::from(&other_generation),
            &reference,
            1,
            &candidate,
        )
        .await
        .unwrap_err();
    assert!(stale.to_string().contains("operation_caller_changed"));
    assert!(store.load_session(&candidate.id).await.unwrap().is_none());
    let hook = DefaultWriteHook::install(
        &store,
        ACTOR_TREE_REVISION_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = ReleaseHook(hook.clone());
    let worker = {
        let store = store.clone();
        let reference = reference.clone();
        let candidate = candidate.clone();
        tokio::spawn(async move {
            store
                .create_and_attach_supervised_root_observed(&original, &reference, 1, &candidate)
                .await
        })
    };
    hook.wait();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let installing = Arc::new(tokio::sync::Notify::new());
    let installer = {
        let store = store.clone();
        let supervisor = supervisor.clone();
        let candidate = candidate.clone();
        let installing = installing.clone();
        tokio::spawn(async move {
            installing.notify_one();
            install(
                &store,
                &supervisor,
                vec![],
                operation,
                &candidate,
                0,
                BTreeSet::new(),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), installing.notified())
        .await
        .unwrap();
    assert!(
        !installer.is_finished(),
        "scope publication passed the original physical job"
    );
    hook.release();
    tokio::time::timeout(Duration::from_secs(10), installer)
        .await
        .unwrap()
        .unwrap();
    assert!(!home
        .path()
        .join("sessions")
        .join(&candidate.id)
        .join("session.json")
        .exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_creation_keeps_tree_publication_and_scope_locked_until_physical_completion() {
    let (home, store, caller) = fixture().await;
    let store = Arc::new(store);
    let operation = Uuid::new_v4();
    let candidate = request_candidate(&caller, operation, &home.path().join("workspace"));
    let target_id = candidate.id.clone();
    let token = install(
        &store,
        &caller,
        vec![],
        operation,
        &candidate,
        0,
        BTreeSet::new(),
    )
    .await;
    let hook = DefaultWriteHook::install(
        &store,
        ACTOR_TREE_REVISION_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = ReleaseHook(hook.clone());
    let observed = SessionCallerReference::from(&caller);
    let job = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .create_host_scoped_root(&token, &observed, operation, Some(1), &candidate)
                .await
        })
    };
    hook.wait();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let mut revoke = Box::pin(store.revoke_host_session_scope(&caller.id, 1));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(revoke.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    for path in [
        home.path().join(SESSION_LIFECYCLE_LOCK_FILE),
        home.path().join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(&caller.id),
        store.session_write_lock_path(&target_id),
    ] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }
    hook.release();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), revoke)
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert!(home
        .path()
        .join("sessions")
        .join(&target_id)
        .join(ACTOR_TREE_REVISION_FILE)
        .is_file());
    assert!(!home
        .path()
        .join("sessions")
        .join(&target_id)
        .join("session.json")
        .exists());
}
