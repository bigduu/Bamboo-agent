//! Final Host admission and owned-job fences over synthetic TempDir data.
use super::owned_lifetime_tests::{probe, Fixture, Parked};
use super::*;
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionRight,
    HostSessionScopeBinding, HostSessionScopeToken, HostSessionTargetScope, Session,
    SessionCallerReference, SessionMessageBody, SessionMessageContent, SessionMessageKind, Storage,
    HOST_SESSION_SCOPE_VERSION,
};
use std::collections::BTreeMap;

fn message(caller: &Session, target: &Session) -> SessionMessageEnvelope {
    let id = bamboo_domain::session_peer_message_id(
        &caller.id,
        caller.created_at,
        &target.id,
        target.created_at,
        "host-operation",
    );
    SessionMessageEnvelope {
        thread_id: Some(id.to_string()),
        id,
        source: SessionMessageSource::Session {
            session_id: caller.id.clone(),
        },
        target_session_id: target.id.clone(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text("exact instruction")),
        created_at: Utc::now(),
        in_reply_to: None,
        attempt: None,
        correlation_id: Some("host-operation".into()),
    }
}

fn ancestor(session: &Session) -> ActorAncestorObservation {
    ActorAncestorObservation {
        actor_id: session.id.clone(),
        session_created_at: session.created_at,
        metadata_version: session.metadata_version,
    }
}

async fn install(
    store: &SessionStoreV2,
    caller: &Session,
    caller_ancestors: Vec<ActorAncestorObservation>,
    target: &Session,
    target_ancestors: Vec<ActorAncestorObservation>,
) -> HostSessionScopeToken {
    store
        .install_host_session_scope(
            0,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: uuid::Uuid::new_v4(),
                revision: 1,
                caller: HostScopeSessionObservation::capture(caller, caller_ancestors).unwrap(),
                targets: BTreeMap::from([(
                    target.id.clone(),
                    HostSessionTargetScope {
                        observation: HostScopeSessionObservation::capture(target, target_ancestors)
                            .unwrap(),
                        rights: [HostSessionRight::SendPeer].into(),
                    },
                )]),
                creates: BTreeMap::new(),
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn final_send_checks_canonical_project_metadata_birth_and_complete_ancestor_chain() {
    for changed in [
        "caller-project",
        "target-project",
        "target-metadata",
        "caller-birth",
        "target-birth",
        "caller-ancestor",
        "target-ancestor",
    ] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut left = Session::new("left", "model");
        left.set_project_id_meta("left-project");
        let mut right = Session::new("right", "model");
        right.set_project_id_meta("right-project");
        let child_case = changed.ends_with("ancestor");
        let mut caller = if child_case {
            Session::new_child_of("left-child", &left, "model", "short")
        } else {
            left.clone()
        };
        let mut target = if child_case {
            Session::new_child_of("right-child", &right, "model", "short")
        } else {
            right.clone()
        };
        // Child construction does not infer the Host's explicit Project grant.
        caller.set_project_id_meta("left-project");
        target.set_project_id_meta("right-project");
        for session in [&left, &right, &caller, &target] {
            store.save_session(session).await.unwrap();
        }
        let token = install(
            &store,
            &caller,
            if child_case {
                vec![ancestor(&left)]
            } else {
                vec![]
            },
            &target,
            if child_case {
                vec![ancestor(&right)]
            } else {
                vec![]
            },
        )
        .await;
        let original = SessionCallerReference::from(&caller);
        let envelope = message(&caller, &target);
        match changed {
            "caller-project" => {
                caller.set_project_id_meta("changed-project");
                store.save_session(&caller).await.unwrap();
            }
            "target-project" => {
                target.set_project_id_meta("changed-project");
                store.save_session(&target).await.unwrap();
            }
            "target-metadata" => {
                target.metadata_version += 1;
                store.save_session(&target).await.unwrap();
            }
            "caller-birth" | "target-birth" => {
                let id = if changed == "caller-birth" {
                    &caller.id
                } else {
                    &target.id
                };
                store.delete_session(id).await.unwrap();
                store.recreate_root_session(id, "new-model").await.unwrap();
            }
            "caller-ancestor" => {
                left.metadata_version += 1;
                store.save_session(&left).await.unwrap();
            }
            "target-ancestor" => {
                right.metadata_version += 1;
                store.save_session(&right).await.unwrap();
            }
            _ => unreachable!(),
        }
        let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
        assert!(
            inbox
                .deliver_host_scoped_peer_message(
                    &token,
                    &original,
                    &envelope,
                    SessionMessageDeliveryIntent::QueueOnly
                )
                .await
                .is_err(),
            "{changed}"
        );
        assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 0);
    }
}

#[tokio::test]
async fn raw_host_port_rejects_forged_source_thread_reply_kind_and_operation() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let caller = Session::new("caller", "model");
    let mut target = Session::new("target", "model");
    target.set_project_id_meta("target-project");
    for session in [&caller, &target] {
        store.save_session(session).await.unwrap();
    }
    let token = install(&store, &caller, vec![], &target, vec![]).await;
    let original = SessionCallerReference::from(&caller);
    let valid = message(&caller, &target);
    let inbox = FileSessionInbox::new(store, SessionInboxLimits::default());
    for changed in [
        "source",
        "thread",
        "reply",
        "id",
        "operation",
        "body",
        "kind",
        "attempt",
        "target",
    ] {
        let mut envelope = valid.clone();
        match changed {
            "source" => {
                envelope.source = SessionMessageSource::Session {
                    session_id: "somebody-else".into(),
                }
            }
            "thread" => envelope.thread_id = Some("different-thread".into()),
            "reply" => envelope.in_reply_to = Some(valid.id.clone()),
            "id" => envelope.id = SessionMessageId::new(),
            "operation" => envelope.correlation_id = None,
            "kind" => envelope.kind = SessionMessageKind::UserInput,
            "attempt" => envelope.attempt = Some(1),
            "target" => envelope.target_session_id = "ungranted-target".into(),
            "body" => {
                envelope.body = SessionMessageBody::Content(SessionMessageContent::text("\0"))
            }
            _ => unreachable!(),
        }
        assert!(
            inbox
                .deliver_host_scoped_peer_message(
                    &token,
                    &original,
                    &envelope,
                    SessionMessageDeliveryIntent::QueueOnly
                )
                .await
                .is_err(),
            "{changed}"
        );
    }
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 0);
}

#[test]
fn scoped_publication_and_all_physical_guards_survive_started_job_shutdown() {
    for scoped in [false, true] {
        physical_retention(scoped);
    }
}

fn physical_retention(scoped: bool) {
    let f = Fixture::new();
    let (caller, target, token) = f.runtime.block_on(async {
        let mut target = f.store.load_session("target").await.unwrap().unwrap();
        target.set_project_id_meta("target-project");
        f.store.save_session(&target).await.unwrap();
        let mut caller = if scoped {
            Session::new("host-caller", "model")
        } else {
            Session::new_child_of("legacy-caller", &target, "model", "short work")
        };
        caller.set_project_id_meta("target-project");
        f.store.save_session(&caller).await.unwrap();
        let token = if scoped {
            Some(install(&f.store, &caller, vec![], &target, vec![]).await)
        } else {
            None
        };
        (caller, target, token)
    });
    let original = SessionCallerReference::from(&caller);
    let envelope = message(&caller, &target);
    let wrapper = FileSessionInbox::wrapper(&envelope, 1, None);
    let path = f
        .dir
        .join("new")
        .join(format!("{:020}-{}.json", 1, wrapper.id.0));
    let job_token = token.clone();
    let source_actor = bamboo_domain::ActorSession::from_session(&caller).unwrap();
    let target_actor = bamboo_domain::ActorSession::from_session(&target).unwrap();
    let mut parked = Parked::start(
        f.inbox.clone(),
        "maildir_replace",
        path,
        move |inbox| async move {
            match job_token {
                Some(token) => inbox
                    .deliver_host_scoped_peer_message(
                        &token,
                        &original,
                        &envelope,
                        SessionMessageDeliveryIntent::QueueOnly,
                    )
                    .await
                    .map(|_| ()),
                None => inbox
                    .deliver_unscoped_peer_message(
                        &original,
                        &envelope,
                        &source_actor,
                        &target_actor,
                        Some(SessionMessageDeliveryIntent::QueueOnly),
                    )
                    .await
                    .map(|_| ()),
            }
        },
    );
    parked.shutdown();
    f.assert_physical_locks(true);
    assert!(!probe(
        &f.store
            .bamboo_home_dir()
            .join(".runtime-task-transactions.lock")
    ));
    for id in [&caller.id, &target.id] {
        let hash = format!("{:x}.lock", Sha256::digest(id.as_bytes()));
        assert!(!probe(
            &f.store
                .bamboo_home_dir()
                .join(".session-write-locks")
                .join(&hash)
        ));
    }
    for root in std::collections::BTreeSet::from([&caller.root_session_id, &target.root_session_id])
    {
        let hash = format!("{:x}.lock", Sha256::digest(root.as_bytes()));
        assert!(!probe(
            &f.store
                .bamboo_home_dir()
                .join(".actor-tree-locks")
                .join(hash)
        ));
    }
    let store = f.store.clone();
    let caller_id = caller.id.clone();
    let install_caller = caller.clone();
    let install_target = target.clone();
    let revoke = f.runtime.spawn(async move {
        if scoped {
            store
                .revoke_host_session_scope(&caller_id, 1)
                .await
                .unwrap()
        } else {
            install(
                &store,
                &install_caller,
                vec![ancestor(&install_target)],
                &install_target,
                vec![],
            )
            .await;
            1
        }
    });
    let other = f.other_store.clone();
    let target_id = target.id.clone();
    let mutation = scoped.then(|| {
        f.runtime.spawn(async move {
            let mut target = other.load_session(&target_id).await.unwrap().unwrap();
            target.metadata_version += 1;
            other.save_session(&target).await.unwrap();
        })
    });
    f.assert_waiting(&revoke);
    if let Some(mutation) = &mutation {
        f.assert_waiting(mutation);
    }
    parked.barrier.release();
    assert_eq!(f.finish(revoke), if scoped { 2 } else { 1 });
    if let Some(mutation) = mutation {
        f.finish(mutation);
    }
    let current = f.runtime.block_on(
        f.store
            .find_host_session_scope(&SessionCallerReference::from(&caller)),
    );
    if scoped {
        assert!(current.is_err());
    } else {
        assert!(current.unwrap().is_some());
    }
    let backlog = f.runtime.block_on(f.other.inspect(&target.id)).unwrap();
    assert_eq!(backlog.generation, 1);
    assert_eq!(backlog.pending, 1);
}
