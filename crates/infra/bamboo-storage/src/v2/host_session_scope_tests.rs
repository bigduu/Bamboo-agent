//! Synthetic canonical scope reads; no provider or real-account data.

use super::*;
use bamboo_domain::{
    ActorAncestorObservation, HostSessionScopeBinding, HostSessionTargetScope, Message,
    DEFAULT_SUPERVISOR_SESSION_ID, HOST_SESSION_SCOPE_VERSION,
};
use std::collections::BTreeMap;

async fn fixture() -> (tempfile::TempDir, SessionStoreV2, Session, Session) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut caller = Session::new("caller", "model");
    caller.set_project_id_meta("caller-project");
    store.save_session(&caller).await.unwrap();
    let mut target = Session::new("explicit-target", "model");
    target.set_project_id_meta("target-project");
    target.add_message(Message::assistant("persisted independent work", None));
    store.save_session(&target).await.unwrap();
    (home, store, caller, target)
}

fn binding(
    caller: &Session,
    ancestors: Vec<ActorAncestorObservation>,
    target: &Session,
    rights: impl IntoIterator<Item = HostSessionRight>,
    revision: u64,
) -> HostSessionScopeBinding {
    HostSessionScopeBinding {
        schema_version: HOST_SESSION_SCOPE_VERSION,
        grant_id: uuid::Uuid::new_v4(),
        revision,
        caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
        targets: BTreeMap::from([(
            target.id.clone(),
            HostSessionTargetScope {
                observation: HostScopeSessionObservation::capture(target, vec![]).unwrap(),
                rights: rights.into_iter().collect(),
            },
        )]),
        creates: BTreeMap::new(),
    }
}

#[tokio::test]
async fn explicit_scope_reads_work_for_root_child_and_supervisor_without_role_impersonation() {
    let (_home, store, caller, target) = fixture().await;
    let child = Session::new_child_of("child", &caller, "model", "short work");
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
    for (session, ancestors) in [
        (&caller, vec![]),
        (&child, vec![ancestor]),
        (&supervisor, vec![]),
    ] {
        let observed = SessionCallerReference::from(session);
        assert!(store
            .find_host_session_scope(&observed)
            .await
            .unwrap()
            .is_none());
        let grant = binding(
            session,
            ancestors,
            &target,
            [
                HostSessionRight::ListMetadata,
                HostSessionRight::InspectBoundedProgress,
            ],
            1,
        );
        let token = store.install_host_session_scope(0, grant).await.unwrap();
        assert!(store
            .find_host_session_scope(&observed)
            .await
            .unwrap()
            .unwrap()
            .same_installation(&token));
        let page = store
            .list_host_scoped_targets(&token, &observed)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].observation.identity.session_id, target.id);
        let read = store
            .load_host_scoped_target(&token, &observed, &target.id)
            .await
            .unwrap();
        assert_eq!(read.messages[0].content, "persisted independent work");
        assert_eq!(read.authority_identity, target.authority_identity);
        assert!(store
            .load_host_scoped_target(&token, &observed, "ungranted-target")
            .await
            .is_err());
    }
}

#[tokio::test]
async fn list_and_inspect_rights_remain_distinct_and_revocation_has_no_global_fallback() {
    let (_home, store, caller, target) = fixture().await;
    let observed = SessionCallerReference::from(&caller);
    let token = store
        .install_host_session_scope(
            0,
            binding(
                &caller,
                vec![],
                &target,
                [HostSessionRight::ListMetadata],
                1,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .list_host_scoped_targets(&token, &observed)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .load_host_scoped_target(&token, &observed, &target.id)
        .await
        .is_err());
    store
        .revoke_host_session_scope(&caller.id, 1)
        .await
        .unwrap();
    assert!(store.find_host_session_scope(&observed).await.is_err());
    assert!(store
        .list_host_scoped_targets(&token, &observed)
        .await
        .is_err());
    assert!(store
        .load_host_scoped_target(&token, &observed, &target.id)
        .await
        .is_err());
}

#[tokio::test]
async fn changed_target_project_or_metadata_requires_explicit_new_installation() {
    let (_home, store, caller, mut target) = fixture().await;
    let observed = SessionCallerReference::from(&caller);
    let token = store
        .install_host_session_scope(
            0,
            binding(
                &caller,
                vec![],
                &target,
                [HostSessionRight::InspectBoundedProgress],
                1,
            ),
        )
        .await
        .unwrap();
    target.set_project_id_meta("changed-target-project");
    target.metadata_version += 1;
    store.save_session(&target).await.unwrap();
    assert!(store
        .load_host_scoped_target(&token, &observed, &target.id)
        .await
        .is_err());
    let replacement = store
        .install_host_session_scope(
            1,
            binding(
                &caller,
                vec![],
                &target,
                [HostSessionRight::InspectBoundedProgress],
                2,
            ),
        )
        .await
        .unwrap();
    assert!(store
        .load_host_scoped_target(&replacement, &observed, &target.id)
        .await
        .is_ok());
    assert!(store
        .load_host_scoped_target(&token, &observed, &target.id)
        .await
        .is_err());
}

#[tokio::test]
async fn forged_child_ancestor_revision_never_publishes_scope_or_adopts_a_new_parent() {
    let (_home, store, caller, target) = fixture().await;
    let child = Session::new_child_of("child", &caller, "model", "bounded work");
    store.save_session(&child).await.unwrap();
    let ancestor = ActorAncestorObservation {
        actor_id: caller.id.clone(),
        session_created_at: caller.created_at,
        metadata_version: caller.metadata_version + 1,
    };
    let grant = binding(
        &child,
        vec![ancestor],
        &target,
        [HostSessionRight::InspectBoundedProgress],
        1,
    );
    assert!(store.install_host_session_scope(0, grant).await.is_err());
    assert!(store
        .find_host_session_scope(&SessionCallerReference::from(&child))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn opposite_cross_root_reads_use_one_canonical_lock_order() {
    let (_home, store, caller, target) = fixture().await;
    let a = SessionCallerReference::from(&caller);
    let b = SessionCallerReference::from(&target);
    let a_token = store
        .install_host_session_scope(
            0,
            binding(
                &caller,
                vec![],
                &target,
                [HostSessionRight::InspectBoundedProgress],
                1,
            ),
        )
        .await
        .unwrap();
    let b_token = store
        .install_host_session_scope(
            0,
            binding(
                &target,
                vec![],
                &caller,
                [HostSessionRight::InspectBoundedProgress],
                1,
            ),
        )
        .await
        .unwrap();
    let (a_read, b_read) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            store.load_host_scoped_target(&a_token, &a, &target.id),
            store.load_host_scoped_target(&b_token, &b, &caller.id)
        )
    })
    .await
    .expect("opposite scoped reads must not deadlock");
    assert!(a_read.is_ok());
    assert!(b_read.is_ok());
}

#[tokio::test]
async fn host_send_keeps_the_stable_grant_but_never_upgrades_an_original_conversation_ledger() {
    use bamboo_domain::{
        conversation_reset_snapshot_sha256, prepare_conversation_reset,
        ConversationResetPreparation, ConversationResetReadSafety, ConversationResetRequest,
        SessionInboxLimits, SessionInboxPort, SessionMessageBody, SessionMessageContent,
        SessionMessageDeliveryIntent, SessionMessageEnvelope, SessionMessageKind,
        SessionMessageSource,
    };
    let (_home, store, caller, target) = fixture().await;
    let original = SessionCallerReference::from(&caller);
    let token = store
        .install_host_session_scope(
            0,
            binding(&caller, vec![], &target, [HostSessionRight::SendPeer], 1),
        )
        .await
        .unwrap();
    let request = ConversationResetRequest {
        session_id: caller.id.clone(),
        operation_id: format!("0:{}", uuid::Uuid::new_v4()),
        expected_birth_token: caller.root_mode_birth_token(),
        expected_conversation_revision: 0,
        expected_snapshot_sha256: conversation_reset_snapshot_sha256(&caller).unwrap(),
    };
    let ConversationResetPreparation::Prepared(prepared) = prepare_conversation_reset(
        &caller,
        &Default::default(),
        &request,
        ConversationResetReadSafety::IdleObserved,
        Utc::now(),
    )
    .unwrap() else {
        panic!("pure synthetic preparation")
    };
    let mut canonical = prepared.proposed_session;
    canonical.conversation_reset = Some(prepared.proposed_state);
    // Synthetic future-producer bytes in this fresh TempDir, with no actor.
    // This fixture does not invoke a reset endpoint or add a production writer.
    let directory = store.sessions_root_dir().join(&caller.id);
    fs::write(
        directory.join("session.json"),
        compact_main::serialize_main(&canonical).unwrap(),
    )
    .await
    .unwrap();
    fs::write(
        directory.join(RUNTIME_SIDECAR_FILE),
        serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&canonical)).unwrap(),
    )
    .await
    .unwrap();
    SessionStoreV2::write_staged_root_tool_proof(&directory, &canonical)
        .await
        .unwrap();
    let id = bamboo_domain::session_peer_message_id(
        &caller.id,
        caller.created_at,
        &target.id,
        target.created_at,
        "ledger-operation",
    );
    let envelope = SessionMessageEnvelope {
        thread_id: Some(id.to_string()),
        id,
        source: SessionMessageSource::Session {
            session_id: caller.id.clone(),
        },
        target_session_id: target.id.clone(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text("bounded instruction")),
        created_at: Utc::now(),
        in_reply_to: None,
        attempt: None,
        correlation_id: Some("ledger-operation".into()),
    };
    let store = std::sync::Arc::new(store);
    assert!(store
        .find_host_session_scope(&original)
        .await
        .unwrap()
        .unwrap()
        .same_installation(&token));
    let inbox = crate::FileSessionInbox::new(store, SessionInboxLimits::default());
    assert!(inbox
        .deliver_host_scoped_peer_message(
            &token,
            &original,
            &envelope,
            SessionMessageDeliveryIntent::QueueOnly
        )
        .await
        .is_err());
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 0);
    let current = SessionCallerReference::from(&canonical);
    let first = inbox
        .deliver_host_scoped_peer_message(
            &token,
            &current,
            &envelope,
            SessionMessageDeliveryIntent::QueueOnly,
        )
        .await
        .unwrap();
    assert_eq!(first.delivery.generation, 1);
    let mut different_ledger = current;
    let receipt = different_ledger
        .conversation_reset
        .as_mut()
        .unwrap()
        .receipts
        .last_mut()
        .unwrap();
    receipt.expected_snapshot_sha256 = if receipt.expected_snapshot_sha256 == "a".repeat(64) {
        "b".repeat(64)
    } else {
        "a".repeat(64)
    };
    assert!(inbox
        .deliver_host_scoped_peer_message(
            &token,
            &different_ledger,
            &envelope,
            SessionMessageDeliveryIntent::QueueOnly
        )
        .await
        .is_err());
    assert_eq!(inbox.inspect(&target.id).await.unwrap().generation, 1);
}

#[tokio::test]
async fn host_send_hint_and_physical_writer_ignore_missing_or_wrong_global_index() {
    use bamboo_domain::{
        SessionInboxLimits, SessionInboxPort, SessionMessageDeliveryIntent, SessionMessageEnvelope,
        SessionMessageKind, SessionMessageSource,
    };
    for mode in ["missing", "wrong"] {
        let (_home, store, caller, target) = fixture().await;
        let poison = Session::new("ungranted-poison", "model");
        store.save_session(&poison).await.unwrap();
        let poison_dir = store.sessions_root_dir().join(&poison.id).join("inbox");
        fs::create_dir_all(&poison_dir).await.unwrap();
        // Reading a wrong transport directory as a hint would fail with
        // NotADirectory. A SendOnly operation must never inspect this path.
        fs::write(poison_dir.join("new"), b"untouched poison marker")
            .await
            .unwrap();
        let token = store
            .install_host_session_scope(
                0,
                binding(&caller, vec![], &target, [HostSessionRight::SendPeer], 1),
            )
            .await
            .unwrap();
        {
            let mut index = store.index.write().await;
            if mode == "missing" {
                index.sessions.remove(&target.id);
            } else {
                index.sessions.get_mut(&target.id).unwrap().rel_path =
                    SessionStoreV2::root_rel_path(&poison.id);
            }
        }
        let id = bamboo_domain::session_peer_message_id(
            &caller.id,
            caller.created_at,
            &target.id,
            target.created_at,
            "index-operation",
        );
        let mut envelope = SessionMessageEnvelope::user_input(&target.id, "canonical-only input");
        envelope.id = id.clone();
        envelope.source = SessionMessageSource::Session {
            session_id: caller.id.clone(),
        };
        envelope.kind = SessionMessageKind::PeerMessage;
        envelope.thread_id = Some(id.to_string());
        envelope.correlation_id = Some("index-operation".into());
        let target_dir = store.sessions_root_dir().join(&target.id).join("inbox");
        let inbox =
            crate::FileSessionInbox::new(std::sync::Arc::new(store), SessionInboxLimits::default());
        let receipt = inbox
            .deliver_host_scoped_peer_message(
                &token,
                &SessionCallerReference::from(&caller),
                &envelope,
                SessionMessageDeliveryIntent::QueueOnly,
            )
            .await
            .unwrap();
        assert_eq!(receipt.delivery.generation, 1, "{mode}");
        assert_eq!(
            fs::read_to_string(target_dir.join("generation"))
                .await
                .unwrap()
                .trim(),
            "1"
        );
        let mut reader = fs::read_dir(target_dir.join("new")).await.unwrap();
        assert!(reader.next_entry().await.unwrap().is_some());
        assert!(reader.next_entry().await.unwrap().is_none());
        assert_eq!(
            fs::read(poison_dir.join("new")).await.unwrap(),
            b"untouched poison marker"
        );
    }
}
