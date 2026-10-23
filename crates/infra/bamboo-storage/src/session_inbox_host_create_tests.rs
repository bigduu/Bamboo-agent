//! Real create-purpose final admission over disposable storage. Source fixtures
//! only until the integration owner receives the single Rust verification window.
use super::*;
use bamboo_domain::{
    host_root_first_input_identity, host_session_create_identity, HostScopeSessionObservation,
    HostSessionCreateScope, HostSessionScopeBinding, HostSessionScopeToken, Session,
    SessionCallerReference, SessionMessageBody, SessionMessageContent, SessionMessageKind, Storage,
    HOST_SESSION_SCOPE_VERSION,
};
use std::collections::BTreeMap;

async fn fixture() -> (
    tempfile::TempDir,
    Arc<SessionStoreV2>,
    FileSessionInbox,
    Session,
    Session,
    uuid::Uuid,
    HostSessionScopeToken,
) {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let mut caller = Session::new("caller", "model");
    caller.set_project_id_meta("caller-project");
    store.save_session(&caller).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    let (_, id) =
        host_session_create_identity(&SessionCallerReference::from(&caller), operation).unwrap();
    let mut target = Session::new(id, "model");
    target.set_project_id_meta("target-project");
    target.set_workspace_path_meta(home.path().join("workspace").to_string_lossy());
    let token = store
        .install_host_session_scope(
            0,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: uuid::Uuid::new_v4(),
                revision: 1,
                caller: HostScopeSessionObservation::capture(&caller, vec![]).unwrap(),
                targets: BTreeMap::new(),
                creates: BTreeMap::from([(
                    operation,
                    HostSessionCreateScope {
                        operation_id: operation,
                        project_id: target.project_id_meta().unwrap().parse().unwrap(),
                        workspace_path: target.workspace_path_meta().unwrap(),
                        created_target_rights: Default::default(),
                    },
                )]),
            },
        )
        .await
        .unwrap();
    store
        .create_host_scoped_root(
            &token,
            &SessionCallerReference::from(&caller),
            operation,
            Some(1),
            &target,
        )
        .await
        .unwrap();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    (home, store, inbox, caller, target, operation, token)
}

fn message(caller: &Session, target: &Session, operation: uuid::Uuid) -> SessionMessageEnvelope {
    let (id, thread_id) =
        host_root_first_input_identity(&SessionCallerReference::from(caller), operation, target)
            .unwrap();
    SessionMessageEnvelope {
        id,
        thread_id,
        source: SessionMessageSource::Session {
            session_id: caller.id.clone(),
        },
        target_session_id: target.id.clone(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text("exact first instruction")),
        created_at: Utc::now(),
        in_reply_to: None,
        attempt: None,
        correlation_id: Some(operation.to_string()),
    }
}

#[tokio::test]
async fn creation_only_permit_admits_exact_first_input_once_without_peer_or_inspect_rights() {
    let (_home, store, inbox, caller, target, operation, token) = fixture().await;
    let original = SessionCallerReference::from(&caller);
    let envelope = message(&caller, &target, operation);
    let first = inbox
        .deliver_host_root_first_input(&token, &original, operation, &target, &envelope)
        .await
        .unwrap();
    let mut retry = envelope.clone();
    retry.created_at += chrono::Duration::seconds(1);
    assert_eq!(
        first,
        inbox
            .deliver_host_root_first_input(&token, &original, operation, &target, &retry)
            .await
            .unwrap()
    );
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 1);
    assert!(store
        .load_host_scoped_target(&token, &original, &target.id)
        .await
        .is_err());
    assert!(inbox
        .deliver_host_scoped_peer_message(
            &token,
            &original,
            &envelope,
            SessionMessageDeliveryIntent::TriggerTurn
        )
        .await
        .is_err());
    retry.body = SessionMessageBody::Content(SessionMessageContent::text("changed instruction"));
    assert!(inbox
        .deliver_host_root_first_input(&token, &original, operation, &target, &retry)
        .await
        .is_err());
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 1);
}

#[tokio::test]
async fn first_input_rejects_forged_envelope_and_an_unrelated_reserved_root() {
    let (_home, _store, inbox, caller, target, operation, token) = fixture().await;
    let original = SessionCallerReference::from(&caller);
    let valid = message(&caller, &target, operation);
    for change in [
        "source",
        "id",
        "thread",
        "operation",
        "target",
        "kind",
        "reply",
        "attempt",
        "body",
    ] {
        let mut envelope = valid.clone();
        match change {
            "source" => envelope.source = SessionMessageSource::User,
            "id" => envelope.id = SessionMessageId::new(),
            "thread" => envelope.thread_id = None,
            "operation" => envelope.correlation_id = Some(uuid::Uuid::new_v4().to_string()),
            "target" => envelope.target_session_id = "unrelated-root".into(),
            "kind" => envelope.kind = SessionMessageKind::UserInput,
            "reply" => envelope.in_reply_to = Some(valid.id.clone()),
            "attempt" => envelope.attempt = Some(1),
            "body" => {
                envelope.body = SessionMessageBody::Content(SessionMessageContent::text("\0"))
            }
            _ => unreachable!(),
        }
        assert!(
            inbox
                .deliver_host_root_first_input(&token, &original, operation, &target, &envelope)
                .await
                .is_err(),
            "{change}"
        );
    }
    let mut unrelated = target.clone();
    unrelated.id = "unrelated-root".into();
    assert!(inbox
        .deliver_host_root_first_input(&token, &original, operation, &unrelated, &valid)
        .await
        .is_err());
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 0);
}

#[tokio::test]
async fn current_permit_caller_and_reserved_target_are_rechecked_even_on_receipt_retry() {
    for change in [
        "caller-birth",
        "caller-metadata",
        "target-birth",
        "target-metadata",
        "target-project",
        "target-workspace",
        "revoke",
    ] {
        let (_home, store, inbox, caller, target, operation, token) = fixture().await;
        let original = SessionCallerReference::from(&caller);
        let envelope = message(&caller, &target, operation);
        inbox
            .deliver_host_root_first_input(&token, &original, operation, &target, &envelope)
            .await
            .unwrap();
        if change == "revoke" {
            store
                .revoke_host_session_scope(&caller.id, 1)
                .await
                .unwrap();
        } else if change.ends_with("birth") {
            let observed = if change.starts_with("caller-") {
                &caller
            } else {
                &target
            };
            store.delete_session(&observed.id).await.unwrap();
            store
                .recreate_root_session(&observed.id, "new-model")
                .await
                .unwrap();
            let mut recreated = store.load_session(&observed.id).await.unwrap().unwrap();
            assert_ne!(recreated.created_at, observed.created_at);
            recreated.set_project_id_meta(observed.project_id_meta().unwrap());
            if let Some(workspace) = observed.workspace_path_meta() {
                recreated.set_workspace_path_meta(workspace);
            }
            recreated.metadata_version = recreated.metadata_version.checked_add(1).unwrap();
            store.save_session(&recreated).await.unwrap();
        } else {
            let mut changed = if change.starts_with("caller-") {
                caller.clone()
            } else {
                target.clone()
            };
            match change {
                "caller-metadata" | "target-metadata" => changed.metadata_version += 1,
                "target-project" => {
                    changed.set_project_id_meta("changed-project");
                    changed.metadata_version = changed.metadata_version.checked_add(1).unwrap();
                }
                "target-workspace" => changed.set_workspace_path_meta("/changed-workspace"),
                _ => unreachable!(),
            }
            store.save_session(&changed).await.unwrap();
        }
        assert!(
            inbox
                .deliver_host_root_first_input(&token, &original, operation, &target, &envelope)
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(
            inbox.inspect(&target.id).await.unwrap().generation,
            if change == "target-birth" { 0 } else { 1 }
        );
    }
}

#[test]
fn creation_first_input_retains_scope_and_physical_guards_after_awaiter_shutdown() {
    use super::owned_lifetime_tests::{probe, Fixture, Parked};
    let mut f = Fixture::new();
    let (caller, target, operation, token) = f.runtime.block_on(async {
        let caller = Session::new("creator", "model");
        f.store.save_session(&caller).await.unwrap();
        let operation = uuid::Uuid::new_v4();
        let original = SessionCallerReference::from(&caller);
        let (_, id) = host_session_create_identity(&original, operation).unwrap();
        let mut target = Session::new(id, "model");
        target.set_project_id_meta("project");
        target.set_workspace_path_meta(f._temp.path().join("workspace").to_string_lossy());
        let token = f
            .store
            .install_host_session_scope(
                0,
                HostSessionScopeBinding {
                    schema_version: HOST_SESSION_SCOPE_VERSION,
                    grant_id: uuid::Uuid::new_v4(),
                    revision: 1,
                    caller: HostScopeSessionObservation::capture(&caller, vec![]).unwrap(),
                    targets: BTreeMap::new(),
                    creates: BTreeMap::from([(
                        operation,
                        HostSessionCreateScope {
                            operation_id: operation,
                            project_id: target.project_id_meta().unwrap().parse().unwrap(),
                            workspace_path: target.workspace_path_meta().unwrap(),
                            created_target_rights: Default::default(),
                        },
                    )]),
                },
            )
            .await
            .unwrap();
        f.store
            .create_host_scoped_root(&token, &original, operation, Some(1), &target)
            .await
            .unwrap();
        (caller, target, operation, token)
    });
    f.dir = f
        .store
        .bamboo_home_dir()
        .join("sessions")
        .join(&target.id)
        .join("inbox");
    let envelope = message(&caller, &target, operation);
    let wrapper = FileSessionInbox::wrapper(&envelope, 1, None);
    let path = f
        .dir
        .join("new")
        .join(format!("{:020}-{}.json", 1, wrapper.id.0));
    let target_id = target.id.clone();
    let original = SessionCallerReference::from(&caller);
    let mut parked = Parked::start(
        f.inbox.clone(),
        "maildir_replace",
        path,
        move |inbox| async move {
            inbox
                .deliver_host_root_first_input(&token, &original, operation, &target, &envelope)
                .await
                .map(|_| ())
        },
    );
    parked.shutdown();
    f.assert_physical_locks(true);
    assert!(!probe(
        &f.store
            .bamboo_home_dir()
            .join(".runtime-task-transactions.lock")
    ));
    for id in [&caller.id, &target_id] {
        let hash = format!("{:x}.lock", Sha256::digest(id.as_bytes()));
        assert!(!probe(
            &f.store
                .bamboo_home_dir()
                .join(".session-write-locks")
                .join(&hash)
        ));
        assert!(!probe(
            &f.store
                .bamboo_home_dir()
                .join(".actor-tree-locks")
                .join(&hash)
        ));
    }
    let store = f.store.clone();
    let caller_id = caller.id.clone();
    let revoke = f.runtime.spawn(async move {
        store
            .revoke_host_session_scope(&caller_id, 1)
            .await
            .unwrap()
    });
    f.assert_waiting(&revoke);
    parked.barrier.release();
    assert_eq!(f.finish(revoke), 2);
    assert!(probe(&f.dir.join(OPERATION_LOCK_FILE)));
}
