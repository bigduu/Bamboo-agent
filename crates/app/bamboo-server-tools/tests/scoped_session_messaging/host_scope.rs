//! Host routing over fresh synthetic Sessions; no real grant installation.
use super::*;
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionRight,
    HostSessionScopeBinding, HostSessionScopeToken, HostSessionTargetScope,
    HOST_SESSION_SCOPE_VERSION,
};
use std::collections::BTreeMap;

async fn install(
    f: &Fixture,
    caller: &Session,
    ancestors: Vec<ActorAncestorObservation>,
    target: &Session,
    rights: &[HostSessionRight],
    revision: u64,
) -> HostSessionScopeToken {
    f.store
        .install_host_session_scope(
            revision - 1,
            HostSessionScopeBinding {
                schema_version: HOST_SESSION_SCOPE_VERSION,
                grant_id: uuid::Uuid::new_v4(),
                revision,
                caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
                targets: BTreeMap::from([(
                    target.id.clone(),
                    HostSessionTargetScope {
                        observation: HostScopeSessionObservation::capture(target, vec![]).unwrap(),
                        rights: rights.iter().copied().collect(),
                    },
                )]),
                creates: BTreeMap::new(),
            },
        )
        .await
        .unwrap()
}

fn ancestor(session: &Session) -> ActorAncestorObservation {
    ActorAncestorObservation {
        actor_id: session.id.clone(),
        session_created_at: session.created_at,
        metadata_version: session.metadata_version,
    }
}

/// Existing adapters that do not implement the new port remain unsupported.
struct LegacyOnlyInbox(Arc<FileSessionInbox>);

#[async_trait]
impl SessionInboxPort for LegacyOnlyInbox {
    async fn deliver(
        &self,
        envelope: &bamboo_domain::SessionMessageEnvelope,
    ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
        self.0.deliver(envelope).await
    }
    async fn mark_activation_eligible(
        &self,
        id: &str,
        generation: u64,
        policy: bamboo_domain::SessionActivationPolicy,
    ) -> Result<(), bamboo_domain::SessionInboxError> {
        self.0
            .mark_activation_eligible(id, generation, policy)
            .await
    }
    async fn claim(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError> {
        self.0.claim(id, limit).await
    }
    async fn was_admitted(
        &self,
        id: &str,
        message: &bamboo_domain::SessionMessageId,
    ) -> Result<bool, bamboo_domain::SessionInboxError> {
        self.0.was_admitted(id, message).await
    }
    async fn ack(
        &self,
        id: &str,
        claim: &bamboo_domain::SessionInboxClaim,
    ) -> Result<(), bamboo_domain::SessionInboxError> {
        self.0.ack(id, claim).await
    }
    async fn inspect(
        &self,
        id: &str,
    ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
        self.0.inspect(id).await
    }
}

#[tokio::test]
async fn unsupported_host_inbox_never_retries_through_legacy_delivery() {
    let f = Fixture::new().await;
    install(
        &f,
        &f.sender,
        vec![ancestor(&f.root)],
        &f.recipient,
        &[HostSessionRight::SendPeer],
        1,
    )
    .await;
    let messenger = Arc::new(SessionMessenger::new(
        f.store.clone(),
        Arc::new(LegacyOnlyInbox(f.inbox.clone())),
        f.activation.clone(),
    ));
    let sender = CanonicalSessionSendAdapter::new(f.store.clone(), messenger);
    let error = sender
        .send(
            &Fixture::ctx(&f.sender),
            ScopedSessionSendRequest {
                target_session_id: f.recipient.id.clone(),
                operation_id: "unsupported-operation".into(),
                message: "bounded input".into(),
                delivery_intent: Some(SessionMessageDeliveryIntent::QueueOnly),
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ScopedSessionSendError::Delivery(bamboo_engine::SessionMessengerError::Inbox(bamboo_domain::SessionInboxError::Storage(message))) if message.contains("unsupported"))
    );
    assert_eq!(
        f.inbox.inspect(&f.recipient.id).await.unwrap().generation,
        0
    );
    assert!(f.activation.requests.lock().await.is_empty());
}

#[tokio::test]
async fn send_only_cross_root_works_for_root_child_and_supervisor_without_inspect() {
    let f = Fixture::new().await;
    let mut target = f.new_root("explicit-other-root").await;
    target.set_project_id_meta("explicit-other-project");
    f.store.save_session(&target).await.unwrap();
    let supervisor = f.supervisor(&[]).await;
    for (caller, ancestors) in [
        (&f.root, vec![]),
        (&f.sender, vec![ancestor(&f.root)]),
        (&supervisor, vec![]),
    ] {
        let token = install(
            &f,
            caller,
            ancestors,
            &target,
            &[HostSessionRight::SendPeer],
            1,
        )
        .await;
        let original = SessionCallerReference::from(caller);
        assert!(f
            .store
            .list_host_scoped_targets(&token, &original)
            .await
            .unwrap()
            .is_empty());
        assert!(f
            .store
            .load_host_scoped_target(&token, &original, &target.id)
            .await
            .is_err());
        let receipt = result(
            f.tool()
                .invoke(
                    send(&target.id, &format!("op-{}", caller.id), Some("queue_only")),
                    Fixture::ctx(caller),
                )
                .await
                .unwrap(),
        );
        assert_eq!(receipt["admitted"], true);
        assert_eq!(receipt["terminal"], "not_observed");
        assert_eq!(receipt["activation"], "not_requested");
    }
    assert!(f.activation.requests.lock().await.is_empty());
    let claims = f.inbox.claim(&target.id, 10).await.unwrap();
    assert_eq!(claims.len(), 3);
    for caller in [&f.root, &f.sender, &supervisor] {
        assert!(claims.iter().any(|claim| claim.envelope.source
            == SessionMessageSource::Session {
                session_id: caller.id.clone()
            }));
    }
    assert!(f
        .store
        .load_session(&target.id)
        .await
        .unwrap()
        .unwrap()
        .messages
        .is_empty());
}

#[tokio::test]
async fn installed_scope_restrictions_and_revocation_never_fall_back_to_same_tree() {
    let f = Fixture::new().await;
    let target = f.new_root("granted-other-root").await;
    // The never-installed namespace retains the existing same-tree route.
    f.tool()
        .invoke(
            send(&f.recipient.id, "legacy", Some("queue_only")),
            Fixture::ctx(&f.sender),
        )
        .await
        .unwrap();
    assert!(f
        .tool()
        .invoke(
            send(&target.id, "no-grant", Some("queue_only")),
            Fixture::ctx(&f.sender)
        )
        .await
        .is_err());
    install(
        &f,
        &f.sender,
        vec![ancestor(&f.root)],
        &target,
        &[HostSessionRight::SendPeer],
        1,
    )
    .await;
    assert!(f
        .tool()
        .invoke(
            send(&f.recipient.id, "no-fallback", Some("queue_only")),
            Fixture::ctx(&f.sender)
        )
        .await
        .is_err());
    assert!(f
        .tool()
        .invoke(
            send(&target.id, "intent-required", None),
            Fixture::ctx(&f.sender)
        )
        .await
        .is_err());
    f.store
        .revoke_host_session_scope(&f.sender.id, 1)
        .await
        .unwrap();
    assert!(f
        .tool()
        .invoke(
            send(&f.recipient.id, "revoked-no-fallback", Some("queue_only")),
            Fixture::ctx(&f.sender)
        )
        .await
        .is_err());
    assert_eq!(
        f.inbox.inspect(&f.recipient.id).await.unwrap().generation,
        1
    );
    assert_eq!(f.inbox.inspect(&target.id).await.unwrap().generation, 0);
}

#[tokio::test]
async fn list_or_inspect_scope_never_borrows_an_attached_supervisors_send_right() {
    for right in [
        HostSessionRight::ListMetadata,
        HostSessionRight::InspectBoundedProgress,
    ] {
        let f = Fixture::new().await;
        let target = f.new_root("attached-target").await;
        let supervisor = f.supervisor(&[&target.id]).await;
        install(&f, &supervisor, vec![], &target, &[right], 1).await;
        assert!(f
            .tool()
            .invoke(
                send(&target.id, "no-role-borrow", Some("trigger_turn")),
                Fixture::ctx(&supervisor)
            )
            .await
            .is_err());
        f.store
            .revoke_host_session_scope(&supervisor.id, 1)
            .await
            .unwrap();
        assert!(f
            .tool()
            .invoke(
                send(&target.id, "no-revocation-fallback", Some("trigger_turn")),
                Fixture::ctx(&supervisor)
            )
            .await
            .is_err());
        assert_eq!(f.inbox.inspect(&target.id).await.unwrap().generation, 0);
        assert!(f.activation.requests.lock().await.is_empty());
    }
}

#[tokio::test]
async fn grant_reissue_keeps_operation_identity_and_freezes_body_and_intent() {
    let f = Fixture::new().await;
    let target = f.new_root("retry-target").await;
    let original = SessionCallerReference::from(&f.root);
    let old = install(
        &f,
        &f.root,
        vec![],
        &target,
        &[HostSessionRight::SendPeer],
        1,
    )
    .await;
    let args = send(&target.id, "stable-operation", Some("queue_only"));
    let first = result(
        f.tool()
            .invoke(args.clone(), Fixture::ctx(&f.root))
            .await
            .unwrap(),
    );
    let new = install(
        &f,
        &f.root,
        vec![],
        &target,
        &[HostSessionRight::SendPeer],
        2,
    )
    .await;
    assert!(!old.same_installation(&new));
    assert!(f
        .messenger
        .send_host_scoped_peer(
            &old,
            &original,
            &target.id,
            "stable-operation",
            "one bounded instruction",
            SessionMessageDeliveryIntent::QueueOnly
        )
        .await
        .is_err());
    let retry = result(
        f.tool()
            .invoke(args.clone(), Fixture::ctx(&f.root))
            .await
            .unwrap(),
    );
    assert_eq!(first["message_id"], retry["message_id"]);
    assert_eq!(first["inbox_generation"], retry["inbox_generation"]);
    let mut changed = args.clone();
    changed["message"] = json!("changed meaning");
    assert!(f
        .tool()
        .invoke(changed, Fixture::ctx(&f.root))
        .await
        .is_err());
    let mut changed = args;
    changed["delivery_intent"] = json!("trigger_turn");
    assert!(f
        .tool()
        .invoke(changed, Fixture::ctx(&f.root))
        .await
        .is_err());
    assert_eq!(f.inbox.inspect(&target.id).await.unwrap().generation, 1);
    assert!(f.activation.requests.lock().await.is_empty());
}

#[tokio::test]
async fn host_trigger_failure_preserves_receipt_and_acked_retry_does_not_reactivate() {
    let f = Fixture::new().await;
    let target = f.new_root("activation-target").await;
    install(
        &f,
        &f.root,
        vec![],
        &target,
        &[HostSessionRight::SendPeer],
        1,
    )
    .await;
    let sender = CanonicalSessionSendAdapter::new(f.store.clone(), f.messenger.clone());
    let request = ScopedSessionSendRequest {
        target_session_id: target.id.clone(),
        operation_id: "trigger-operation".into(),
        message: "instruction".into(),
        delivery_intent: Some(SessionMessageDeliveryIntent::TriggerTurn),
    };
    f.activation.fail.store(true, Ordering::SeqCst);
    let error = sender
        .send(&Fixture::ctx(&f.root), request.clone())
        .await
        .unwrap_err();
    let ScopedSessionSendError::Delivery(bamboo_engine::SessionMessengerError::Activation {
        receipt,
        ..
    }) = error
    else {
        panic!("durable activation failure retains its receipt")
    };
    assert_eq!(receipt.generation, 1);
    f.activation.fail.store(false, Ordering::SeqCst);
    let retry = sender
        .send(&Fixture::ctx(&f.root), request.clone())
        .await
        .unwrap();
    assert_eq!(retry.delivery, receipt);
    let claim = f.inbox.claim(&target.id, 1).await.unwrap().remove(0);
    let mut saved = f.store.load_session(&target.id).await.unwrap().unwrap();
    saved.add_message(claim.envelope.to_provider_message().unwrap());
    saved
        .session_inbox_admission_mut()
        .record(claim.envelope.id.clone(), claim.generation);
    f.store.save_session(&saved).await.unwrap();
    f.inbox.ack(&target.id, &claim).await.unwrap();
    let before = f.activation.requests.lock().await.len();
    let completed_retry = sender.send(&Fixture::ctx(&f.root), request).await.unwrap();
    assert_eq!(completed_retry.delivery, receipt);
    assert_eq!(completed_retry.activation, None);
    assert_eq!(f.activation.requests.lock().await.len(), before);
}
