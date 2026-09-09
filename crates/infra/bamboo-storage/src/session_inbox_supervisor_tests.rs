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

#[tokio::test]
async fn supervisor_followup_final_admission_rejects_changed_authority_without_a_receipt() {
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
        assert!(
            inbox
                .deliver_supervisor_followup(&reference, &envelope(&reference))
                .await
                .is_err(),
            "{change}"
        );
        assert!(
            !tokio::fs::try_exists(store.sessions_root_dir().join("target/inbox/generation"))
                .await
                .unwrap(),
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
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        inbox.followup_authority_pause = Some((entered.clone(), release.clone()));
        let message = envelope(&reference);
        let pending_inbox = inbox.clone();
        let pending_ref = reference.clone();
        let pending_message = message.clone();
        let delivery = tokio::spawn(async move {
            pending_inbox
                .deliver_supervisor_followup(&pending_ref, &pending_message)
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
        assert!(inbox
            .deliver_supervisor_followup(&reference, &message)
            .await
            .is_err());
        assert!(
            !store
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
    let delivery = tokio::spawn(async move {
        inbox
            .deliver_supervisor_followup(&reference, &message)
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
async fn supervisor_followup_cannot_impersonate_user_runtime_or_another_session() {
    let (_home, _store, inbox, reference) = fixture().await;
    let mut user = SessionMessageEnvelope::user_input("target", "not a peer");
    assert!(inbox
        .deliver_supervisor_followup(&reference, &user)
        .await
        .is_err());
    user.source = SessionMessageSource::Session {
        session_id: "other".into(),
    };
    user.kind = SessionMessageKind::PeerMessage;
    assert!(inbox
        .deliver_supervisor_followup(&reference, &user)
        .await
        .is_err());
    user.source = SessionMessageSource::Runtime {
        subsystem: "supervisor".into(),
    };
    assert!(inbox
        .deliver_supervisor_followup(&reference, &user)
        .await
        .is_err());
    assert_eq!(inbox.inspect("target").await.unwrap().generation, 0);
}
