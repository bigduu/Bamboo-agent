//! Exact final-admission authority races against the real file inbox.

use super::*;
use bamboo_domain::{
    Session, SessionMessageKind, Storage, SupervisorManagementMutation as Mutation,
    SupervisorManagementRequest, SupervisorReference,
};
use std::time::Duration;
use tokio::sync::Notify;

async fn fixture() -> (
    tempfile::TempDir,
    Arc<SessionStoreV2>,
    FileSessionInbox,
    SupervisorReference,
) {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let reference: SupervisorReference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    let mut target = Session::new("target", "original-model");
    target.set_project_id_meta("project-a");
    target.metadata_version = 3;
    store.save_session(&target).await.unwrap();
    mutate(
        &store,
        &reference,
        0,
        Mutation::ConfigureProjectScope {
            allowed_projects: ["project-a".parse().unwrap()].into(),
        },
    )
    .await
    .unwrap();
    mutate(
        &store,
        &reference,
        1,
        Mutation::Attach {
            target_session_id: "target".into(),
        },
    )
    .await
    .unwrap();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    (home, store, inbox, reference)
}

async fn mutate(
    store: &SessionStoreV2,
    reference: &SupervisorReference,
    revision: u64,
    mutation: Mutation,
) -> std::io::Result<bamboo_domain::SupervisorManagementReceipt> {
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: revision,
            mutation,
        })
        .await
}

fn envelope(reference: &SupervisorReference) -> SessionMessageEnvelope {
    let mut message = SessionMessageEnvelope::user_input("target", "continue original work");
    message.source = SessionMessageSource::Session {
        session_id: reference.session_id.clone(),
    };
    message.kind = SessionMessageKind::PeerMessage;
    message
}

fn bound_envelope(
    reference: &SupervisorReference,
    target: &bamboo_domain::SupervisorCurrentUserTarget,
) -> SessionMessageEnvelope {
    let mut message = envelope(reference);
    let operation = uuid::Uuid::new_v4().to_string();
    message.id = bamboo_domain::supervisor_current_user_message_id(reference, target, &operation);
    message.correlation_id = Some(operation);
    message
}

#[tokio::test]
async fn supervisor_first_intent_survives_pending_ack_reopen_and_legacy_retry() {
    for legacy_first in [true, false] {
        let (_home, store, inbox, reference) = fixture().await;
        let supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let caller = bamboo_domain::SessionCallerReference::from(&supervisor);
        let target = store
            .inspect_supervisor_current_user_target(&reference, "target")
            .await
            .unwrap();
        let message = bound_envelope(&reference, &target);
        let first = if legacy_first {
            // An old v1 transport may have been written before provenance was
            // available. A new protected retry must not invent its origin.
            inbox
                .deliver_with_activation_intent(
                    &message,
                    SessionActivationPolicy::RespectSpecificWait,
                    None,
                )
                .await
                .unwrap()
        } else {
            inbox
                .deliver_supervisor_current_user_followup(&caller, &reference, &target, &message)
                .await
                .unwrap()
        };
        assert_eq!(
            inbox
                .deliver_supervisor_current_user_followup(&caller, &reference, &target, &message)
                .await
                .unwrap(),
            first
        );
        assert_eq!(
            inbox
                .deliver_supervisor_followup(&reference, target.created_at, &message)
                .await
                .unwrap(),
            first
        );
        let mut changed = message.clone();
        changed.body = bamboo_domain::SessionMessageBody::Content(
            bamboo_domain::SessionMessageContent::text("different instruction"),
        );
        assert!(inbox
            .deliver_supervisor_current_user_followup(&caller, &reference, &target, &changed)
            .await
            .is_err());
        let claim = inbox.claim("target", 1).await.unwrap().remove(0);
        assert_eq!(
            inbox
                .deliver_supervisor_followup(&reference, target.created_at, &message)
                .await
                .unwrap(),
            first
        );
        let mut persisted = store.load_session("target").await.unwrap().unwrap();
        persisted.add_message(message.to_provider_message().unwrap());
        persisted
            .session_inbox_admission_mut()
            .record(message.id.clone(), claim.generation);
        store.save_session(&persisted).await.unwrap();
        inbox.ack("target", &claim).await.unwrap();

        let reopened = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
        assert_eq!(
            reopened
                .deliver_supervisor_current_user_followup(&caller, &reference, &target, &message)
                .await
                .unwrap(),
            first
        );
        assert_eq!(
            reopened
                .deliver_supervisor_followup(&reference, target.created_at, &message)
                .await
                .unwrap(),
            first
        );
        let receipt = FileSessionInbox::admitted_receipt(
            &store.sessions_root_dir().join("target/inbox"),
            &message,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(receipt.delivery, first);
        assert_eq!(
            receipt
                .intent
                .as_ref()
                .and_then(|intent| intent.supervised_root_origin())
                .is_some(),
            !legacy_first
        );
        let state = reopened.inspect("target").await.unwrap();
        assert_eq!((state.generation, state.pending + state.claimed), (1, 0));
    }
}

#[tokio::test]
async fn supervisor_current_user_followup_rejects_changed_target_but_not_legacy_acl() {
    for change in [
        "detach",
        "scope",
        "project",
        "version",
        "birth",
        "supervisor",
        "damage",
    ] {
        let (_home, store, inbox, reference) = fixture().await;
        let caller = bamboo_domain::SessionCallerReference::from(
            &store
                .load_session(&reference.session_id)
                .await
                .unwrap()
                .unwrap(),
        );
        let target = store
            .inspect_supervisor_current_user_target(&reference, "target")
            .await
            .unwrap();
        let message = bound_envelope(&reference, &target);
        assert!(
            store
                .inspect_supervisor_link(&reference, "target")
                .await
                .unwrap()
                .authorized
        );
        match change {
            "detach" => {
                mutate(
                    &store,
                    &reference,
                    2,
                    Mutation::Detach {
                        target_session_id: "target".into(),
                    },
                )
                .await
                .unwrap();
            }
            "scope" => {
                mutate(
                    &store,
                    &reference,
                    2,
                    Mutation::ConfigureProjectScope {
                        allowed_projects: Default::default(),
                    },
                )
                .await
                .unwrap();
            }
            "project" | "version" => {
                let mut target = store.load_session("target").await.unwrap().unwrap();
                if change == "project" {
                    target.set_project_id_meta("project-b");
                }
                target.metadata_version += 1;
                store.save_session(&target).await.unwrap();
            }
            "birth" => {
                let original = store.load_session("target").await.unwrap().unwrap();
                store.delete_session("target").await.unwrap();
                let mut target = store
                    .recreate_root_session("target", "new-model")
                    .await
                    .unwrap();
                target.set_project_id_meta("project-a");
                target.metadata_version += 1;
                store.save_session(&target).await.unwrap();
                // Restore the old numeric fence only after the fresh Root's
                // Project mutation has followed its canonical next-revision CAS.
                target.metadata_version = original.metadata_version;
                assert_ne!(target.created_at, original.created_at);
                store.save_session(&target).await.unwrap();
            }
            "supervisor" => {
                store.delete_session(&reference.session_id).await.unwrap();
                store
                    .get_or_create_default_supervisor("replacement")
                    .await
                    .unwrap();
            }
            "damage" => {
                tokio::fs::write(
                    store.sessions_root_dir().join("target/runtime.json"),
                    b"broken",
                )
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        let delivered = inbox
            .deliver_supervisor_current_user_followup(&caller, &reference, &target, &message)
            .await;
        let legacy_only = matches!(change, "detach" | "scope");
        assert_eq!(delivered.is_ok(), legacy_only, "{change}: {delivered:?}");
        assert_eq!(
            tokio::fs::try_exists(store.sessions_root_dir().join("target/inbox/generation"))
                .await
                .unwrap(),
            legacy_only,
            "{change}"
        );
    }
}

#[tokio::test]
async fn supervisor_followup_retains_relationship_and_project_locks_until_receipt() {
    for change in ["detach", "scope", "project", "version"] {
        let (home, store, mut inbox, reference) = fixture().await;
        let independent = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let target_for_mutation = independent.load_session("target").await.unwrap().unwrap();
        let caller = bamboo_domain::SessionCallerReference::from(
            &store
                .load_session(&reference.session_id)
                .await
                .unwrap()
                .unwrap(),
        );
        let target = store
            .inspect_supervisor_current_user_target(&reference, "target")
            .await
            .unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        inbox.followup_authority_pause = Some((entered.clone(), release.clone()));
        let message = bound_envelope(&reference, &target);
        let pending_inbox = inbox.clone();
        let pending_ref = reference.clone();
        let pending_message = message.clone();
        let pending_caller = caller.clone();
        let pending_target = target.clone();
        let delivery = tokio::spawn(async move {
            pending_inbox
                .deliver_supervisor_current_user_followup(
                    &pending_caller,
                    &pending_ref,
                    &pending_target,
                    &pending_message,
                )
                .await
        });
        entered.notified().await;
        let mutation_ref = reference.clone();
        let mutation = async move {
            match change {
                "detach" => {
                    mutate(
                        &independent,
                        &mutation_ref,
                        2,
                        Mutation::Detach {
                            target_session_id: "target".into(),
                        },
                    )
                    .await
                    .unwrap();
                }
                "scope" => {
                    mutate(
                        &independent,
                        &mutation_ref,
                        2,
                        Mutation::ConfigureProjectScope {
                            allowed_projects: Default::default(),
                        },
                    )
                    .await
                    .unwrap();
                }
                "project" | "version" => {
                    let mut target = target_for_mutation;
                    if change == "project" {
                        target.set_project_id_meta("project-b");
                    }
                    target.metadata_version += 1;
                    independent.save_session(&target).await.unwrap();
                }
                _ => unreachable!(),
            }
        };
        tokio::pin!(mutation);
        // Poll the actual mutation until it has yielded to a held authority
        // lock before allowing the inbox writer to issue its receipt.
        std::future::poll_fn(|cx| {
            use std::future::Future;
            assert!(mutation.as_mut().poll(cx).is_pending(), "{change}");
            std::task::Poll::Ready(())
        })
        .await;
        release.notify_one();
        let receipt = tokio::time::timeout(Duration::from_secs(3), delivery)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        mutation.await;
        assert_eq!(receipt.generation, 1);
        assert_eq!(inbox.inspect("target").await.unwrap().pending, 1);
        // A later invocation must revalidate even when an exact receipt already exists.
        inbox.followup_authority_pause = None;
        let replay = inbox
            .deliver_supervisor_current_user_followup(&caller, &reference, &target, &message)
            .await;
        if matches!(change, "detach" | "scope") {
            assert_eq!(replay.unwrap().generation, receipt.generation);
        } else {
            assert!(replay.is_err());
        }
        assert!(
            store
                .inspect_supervisor_link(&reference, "target")
                .await
                .unwrap()
                .authorized
        );
    }
}

#[tokio::test]
async fn supervisor_followup_held_lifecycle_does_not_reenter_behind_a_waiting_writer() {
    let (_home, store, mut inbox, reference) = fixture().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    inbox.followup_authority_pause = Some((entered.clone(), release.clone()));
    let message = envelope(&reference);
    let target_birth = store
        .load_session("target")
        .await
        .unwrap()
        .unwrap()
        .created_at;
    let delivery = tokio::spawn(async move {
        inbox
            .deliver_supervisor_followup(&reference, target_birth, &message)
            .await
    });
    entered.notified().await;
    // delete_session_recursive reaches the exclusive lifecycle acquisition first.
    // Polling once registers its waiting writer before letting admission resume.
    let mut deleting = std::pin::pin!(store.delete_session_recursive("target", true));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(deleting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    release.notify_one();
    let receipt = tokio::time::timeout(Duration::from_secs(3), delivery)
        .await
        .expect("held-lifecycle admission must not deadlock behind writer")
        .unwrap()
        .unwrap();
    assert_eq!(receipt.generation, 1);
    assert!(tokio::time::timeout(Duration::from_secs(3), deleting)
        .await
        .unwrap()
        .unwrap());
}

#[tokio::test]
async fn supervisor_observed_followup_rejects_old_birth_after_reopen() {
    let (home, store, inbox, reference) = fixture().await;
    let caller = bamboo_domain::SessionCallerReference::from(
        &store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap(),
    );
    let birth = store
        .load_session("target")
        .await
        .unwrap()
        .unwrap()
        .created_at;
    let message = envelope(&reference);
    let first = inbox
        .deliver_unscoped_supervisor_followup(&caller, &reference, birth, &message)
        .await
        .unwrap();
    assert_eq!(first.generation, 1);
    store.delete_session("target").await.unwrap();
    store
        .recreate_root_session("target", "replacement")
        .await
        .unwrap();

    // A reopened adapter has neither the old Inbox receipt nor an in-memory
    // operation map. The original caller's birth still fences the new target.
    let reopened = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let fresh_inbox = FileSessionInbox::new(reopened, SessionInboxLimits::default());
    let before = fresh_inbox.inspect("target").await.unwrap();
    assert_eq!(before.generation, 0);
    let error = fresh_inbox
        .deliver_unscoped_supervisor_followup(&caller, &reference, birth, &message)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("target lifetime changed"),
        "{error}"
    );
    let after = fresh_inbox.inspect("target").await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.pending, before.pending);
}

#[tokio::test]
async fn supervisor_followup_cannot_impersonate_user_runtime_or_another_session() {
    let (_home, store, inbox, reference) = fixture().await;
    let target_birth = store
        .load_session("target")
        .await
        .unwrap()
        .unwrap()
        .created_at;
    let mut user = SessionMessageEnvelope::user_input("target", "not a peer");
    assert!(inbox
        .deliver_supervisor_followup(&reference, target_birth, &user)
        .await
        .is_err());
    user.source = SessionMessageSource::Session {
        session_id: "other".into(),
    };
    user.kind = SessionMessageKind::PeerMessage;
    assert!(inbox
        .deliver_supervisor_followup(&reference, target_birth, &user)
        .await
        .is_err());
    user.source = SessionMessageSource::Runtime {
        subsystem: "supervisor".into(),
    };
    assert!(inbox
        .deliver_supervisor_followup(&reference, target_birth, &user)
        .await
        .is_err());
    assert_eq!(inbox.inspect("target").await.unwrap().generation, 0);
}
