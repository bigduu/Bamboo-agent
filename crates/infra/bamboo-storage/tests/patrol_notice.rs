use bamboo_domain::{
    SessionInboxLimits, SessionInboxPort, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SessionRuntimeInstruction, Storage, SupervisorReference,
};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use std::sync::Arc;

fn notice(target: &str) -> SessionMessageEnvelope {
    let mut envelope = SessionMessageEnvelope::user_input(target, "");
    envelope.id = SessionMessageId::stable("patrol-notice-test", &serde_json::json!(target));
    envelope.source = SessionMessageSource::Runtime {
        subsystem: "ticket_patrol".into(),
    };
    envelope.kind = SessionMessageKind::RuntimeInstruction;
    envelope.body = SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
        instruction: "ticket_patrol_escalation_v1".into(),
        content: Some(SessionMessageContent::text(
            "Inspect current facts; no recovery is established.",
        )),
        data: Some(serde_json::json!({"operation":"one-incident"})),
        provider_message: None,
    });
    envelope
}

#[tokio::test]
async fn real_inbox_stages_one_notice_reopens_and_dedupes_without_activation() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let bootstrap = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let supervisor: SupervisorReference = (&bootstrap).into();
    let mut session = store
        .load_session(&bootstrap.session_id)
        .await
        .unwrap()
        .unwrap();
    session.set_root_orchestration_only(true).unwrap();
    store.save_session(&session).await.unwrap();
    let envelope = notice(&bootstrap.session_id);
    let gate = bamboo_domain::AdmissionGate::default();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let first = inbox
        .stage_supervisor_patrol_notice(
            &supervisor,
            session.root_tool_authority_revision,
            &envelope,
            &gate,
        )
        .await
        .unwrap();
    let before = inbox.inspect(&bootstrap.session_id).await.unwrap();
    assert_eq!(before.pending, 1);
    assert!(!before.activation_pending());
    assert!(!before.interrupt_pending());
    drop(inbox);
    drop(store);
    let reopened = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let inbox = FileSessionInbox::new(reopened, SessionInboxLimits::default());
    assert_eq!(
        inbox.existing_staged_delivery(&envelope).await.unwrap(),
        Some(first.clone())
    );
    let retry = inbox
        .stage_supervisor_patrol_notice(
            &supervisor,
            session.root_tool_authority_revision,
            &envelope,
            &gate,
        )
        .await
        .unwrap();
    assert_eq!(retry, first);
    assert_eq!(inbox.inspect(&bootstrap.session_id).await.unwrap(), before);
}

#[tokio::test]
async fn stale_incarnation_revision_mode_and_forged_origin_leave_no_notice() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(directory.path().into()).await.unwrap());
    let bootstrap = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let reference: SupervisorReference = (&bootstrap).into();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let mut session = store
        .load_session(&bootstrap.session_id)
        .await
        .unwrap()
        .unwrap();
    let envelope = notice(&bootstrap.session_id);
    let gate = bamboo_domain::AdmissionGate::default();
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, 0, &envelope, &gate)
        .await
        .is_err());
    session.set_root_orchestration_only(true).unwrap();
    store.save_session(&session).await.unwrap();
    let revision = session.root_tool_authority_revision;
    let cancelled = bamboo_domain::AdmissionGate::default();
    cancelled.cancel_if_pending();
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision, &envelope, &cancelled)
        .await
        .is_err());
    let expired = bamboo_domain::AdmissionGate::with_deadline(std::time::Instant::now());
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision, &envelope, &expired)
        .await
        .is_err());
    let wrong = SupervisorReference {
        incarnation_id: uuid::Uuid::new_v4(),
        ..reference.clone()
    };
    assert!(inbox
        .stage_supervisor_patrol_notice(&wrong, revision, &envelope, &gate)
        .await
        .is_err());
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision + 1, &envelope, &gate)
        .await
        .is_err());
    let mut forged = envelope.clone();
    forged.source = SessionMessageSource::User;
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision, &forged, &gate)
        .await
        .is_err());
    let mut target = envelope.clone();
    target.target_session_id = "ordinary-root".into();
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision, &target, &gate)
        .await
        .is_err());
    session.set_root_orchestration_only(false).unwrap();
    store.save_session(&session).await.unwrap();
    assert!(inbox
        .stage_supervisor_patrol_notice(&reference, revision, &envelope, &gate)
        .await
        .is_err());
    assert_eq!(
        inbox.inspect(&bootstrap.session_id).await.unwrap().pending,
        0
    );
}
