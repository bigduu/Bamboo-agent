//! Cold synthetic storage fixtures. No provider, account or real-user Session.

use super::*;
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionCreateScope,
    HostSessionRight, HostSessionScopeBinding, HostSessionScopeToken, Message,
    DEFAULT_SUPERVISOR_SESSION_ID, HOST_SESSION_SCOPE_VERSION,
};
use std::collections::{BTreeMap, BTreeSet};

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
    let child = Session::new_child_of("child", &caller, "model", "short child work");
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
