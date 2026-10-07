use super::*;
use bamboo_domain::{
    AdmissionGate, Session, SessionChildOutcome, SessionMessageBody, SessionMessageContent,
    SessionMessageKind, SessionRuntimeInstruction, Storage,
};
use tempfile::TempDir;

async fn fixture() -> (TempDir, Arc<SessionStoreV2>, FileSessionInbox) {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
    store
        .save_session(&Session::new("parent", "model"))
        .await
        .unwrap();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    (temp, store, inbox)
}

async fn reopen(temp: &TempDir) -> FileSessionInbox {
    let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
    FileSessionInbox::new(store, SessionInboxLimits::default())
}

async fn v1_only_mark(inbox: &FileSessionInbox, generation: u64) -> Result<(), SessionInboxError> {
    let _lifecycle = inbox.lock_lifecycle().await?;
    let dir = inbox.inbox_dir("parent").await?;
    let _guard = inbox.lock_operation(&dir).await?;
    let path = dir.join(ACTIVATION_GENERATION_FILE);
    let raw = tokio::fs::read_to_string(&path)
        .await
        .map_err(|_| SessionInboxError::Storage("v1 read failed".into()))?;
    let current = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| SessionInboxError::Storage("v1 parse failed".into()))?;
    if generation > current {
        atomic_write(&path, generation.to_string().as_bytes())
            .await
            .map_err(|_| SessionInboxError::Storage("v1 write failed".into()))?;
    }
    Ok(())
}

fn staged_child() -> SessionMessageEnvelope {
    let mut envelope = SessionMessageEnvelope::user_input("parent", "child result");
    envelope.source = SessionMessageSource::Session {
        session_id: "child".into(),
    };
    envelope.kind = SessionMessageKind::ChildOutcome;
    envelope.body = SessionMessageBody::ChildOutcome(SessionChildOutcome {
        child_session_id: "child".into(),
        status: "completed".into(),
        result: Some("child result".into()),
        error: None,
        provider_message: None,
    });
    envelope
}

fn staged_bash() -> SessionMessageEnvelope {
    let mut envelope = SessionMessageEnvelope::user_input("parent", "bash result");
    envelope.source = SessionMessageSource::Runtime {
        subsystem: "bash".into(),
    };
    envelope.kind = SessionMessageKind::RuntimeInstruction;
    envelope.body = SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
        instruction: "bash_completion".into(),
        content: Some(SessionMessageContent::text("bash result")),
        data: None,
        provider_message: None,
    });
    envelope
}

#[tokio::test]
async fn immediate_restart_recovery_never_promotes_staged_child_or_bash_siblings() {
    for policy in [
        SessionActivationPolicy::RespectSpecificWait,
        SessionActivationPolicy::InterruptSpecificWait,
    ] {
        let (temp, _store, inbox) = fixture().await;
        let child = staged_child();
        let first = inbox.deliver(&child).await.unwrap();
        let immediate = SessionMessageEnvelope::user_input("parent", "steer only this turn");
        let second = inbox
            .deliver_with_activation_intent(&immediate, policy, None)
            .await
            .unwrap();
        let bash = staged_bash();
        let third = inbox.deliver(&bash).await.unwrap();
        assert_eq!(
            (first.generation, second.generation, third.generation),
            (1, 2, 3)
        );

        // Simulate a crash immediately after the message rename: no activation
        // prefix exists, yet only the immediate message is restart-eligible.
        let dir = inbox.inbox_dir("parent").await.unwrap();
        assert_eq!(
            FileSessionInbox::read_activation_watermark(&dir)
                .await
                .unwrap(),
            (0, true)
        );
        assert!(!dir.join(INTERRUPT_GENERATION_FILE).exists());
        let restarted = reopen(&temp).await;
        let backlog = restarted.inspect("parent").await.unwrap();
        assert!(backlog.activation_pending());
        assert_eq!(backlog.activation_generation, 2);
        assert_eq!(backlog.coordinator_generation, 0);
        assert_eq!(
            backlog.interrupt_pending(),
            policy == SessionActivationPolicy::InterruptSpecificWait
        );
        let claims = restarted.claim("parent", 128).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, immediate.id);
        assert_eq!(claims[0].activation_policy, policy);
        assert_eq!(
            restarted
                .deliver_with_activation_intent(&immediate, policy, None)
                .await
                .unwrap(),
            second
        );
        restarted.ack("parent", &claims[0]).await.unwrap();
        let after_ack = reopen(&temp).await;
        assert_eq!(
            after_ack
                .deliver_with_activation_intent(&immediate, policy, None)
                .await
                .unwrap(),
            second
        );
        let remaining = after_ack.inspect("parent").await.unwrap();
        assert_eq!(remaining.pending + remaining.claimed, 2);
        assert!(!remaining.activation_pending());
        assert!(!remaining.interrupt_pending());

        // The older generation is still inert until its coordinator releases
        // exactly that prefix; the later Bash sibling remains staged.
        after_ack
            .mark_activation_eligible("parent", 1, SessionActivationPolicy::RespectSpecificWait)
            .await
            .unwrap();
        let released = after_ack.claim("parent", 128).await.unwrap();
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].envelope.id, child.id);
        assert_eq!(
            released[0].activation_policy,
            SessionActivationPolicy::RespectSpecificWait
        );
        after_ack.ack("parent", &released[0]).await.unwrap();
        assert!(!after_ack
            .inspect("parent")
            .await
            .unwrap()
            .activation_pending());
        after_ack
            .mark_activation_eligible("parent", 3, SessionActivationPolicy::RespectSpecificWait)
            .await
            .unwrap();
        let released = after_ack.claim("parent", 128).await.unwrap();
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].envelope.id, bash.id);
        assert_eq!(
            released[0].activation_policy,
            SessionActivationPolicy::RespectSpecificWait
        );
    }
}

#[tokio::test]
async fn failed_watermark_write_cannot_erase_committed_immediate_intent() {
    let (temp, _store, mut inbox) = fixture().await;
    let envelope = SessionMessageEnvelope::user_input("parent", "immediate");
    let receipt = inbox
        .deliver_with_activation_intent(
            &envelope,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let dir = inbox.inbox_dir("parent").await.unwrap();
    inbox.activation_write_failure = true;
    assert!(inbox
        .mark_activation_eligible(
            "parent",
            receipt.generation,
            SessionActivationPolicy::RespectSpecificWait
        )
        .await
        .is_err());
    assert_eq!(
        FileSessionInbox::read_activation_watermark(&dir)
            .await
            .unwrap(),
        (0, true)
    );
    let restarted = reopen(&temp).await;
    assert!(restarted
        .inspect("parent")
        .await
        .unwrap()
        .activation_pending());
    let claims = restarted.claim("parent", 1).await.unwrap();
    assert_eq!(claims[0].envelope.id, receipt.id);
    assert_eq!(
        restarted
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None
            )
            .await
            .unwrap(),
        receipt
    );
}

#[tokio::test]
async fn exact_id_retry_binds_staged_and_immediate_policy_before_and_after_ack() {
    let (temp, _store, inbox) = fixture().await;
    let staged = SessionMessageEnvelope::user_input("parent", "staged");
    inbox.deliver(&staged).await.unwrap();
    assert!(matches!(
        inbox
            .deliver_with_activation_intent(
                &staged,
                SessionActivationPolicy::InterruptSpecificWait,
                None
            )
            .await,
        Err(SessionInboxError::InvalidClaim(_))
    ));
    let immediate = SessionMessageEnvelope::user_input("parent", "immediate");
    let receipt = inbox
        .deliver_with_activation_intent(
            &immediate,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    for after_ack in [false, true] {
        if after_ack {
            let claims = inbox.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            inbox.ack("parent", &claims[0]).await.unwrap();
        }
        let restarted = reopen(&temp).await;
        assert!(matches!(
            restarted.deliver(&immediate).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        assert!(matches!(
            restarted
                .deliver_with_activation_intent(
                    &immediate,
                    SessionActivationPolicy::InterruptSpecificWait,
                    None
                )
                .await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        assert_eq!(
            restarted
                .deliver_with_activation_intent(
                    &immediate,
                    SessionActivationPolicy::RespectSpecificWait,
                    None
                )
                .await
                .unwrap(),
            receipt
        );
    }
    let remaining = inbox.inspect("parent").await.unwrap();
    assert_eq!(remaining.generation, 2);
    assert_eq!(remaining.pending + remaining.claimed, 1);
    assert!(!remaining.activation_pending());
}

#[tokio::test]
async fn cancelled_before_publication_has_no_intent_and_cancel_after_preserves_exact_receipt() {
    let (temp, _store, mut inbox) = fixture().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    inbox.admission_commit_pause = Some((entered.clone(), release.clone()));
    let envelope = SessionMessageEnvelope::user_input("parent", "cancel before");
    let gate = AdmissionGate::default();
    let delivery = tokio::spawn({
        let inbox = inbox.clone();
        let envelope = envelope.clone();
        let gate = gate.clone();
        async move {
            inbox
                .deliver_with_activation_intent(
                    &envelope,
                    SessionActivationPolicy::InterruptSpecificWait,
                    Some(&gate),
                )
                .await
        }
    });
    entered.notified().await;
    gate.cancel_if_pending();
    release.notify_one();
    assert!(matches!(
        delivery.await.unwrap(),
        Err(SessionInboxError::AdmissionCancelled)
    ));
    assert!(!reopen(&temp)
        .await
        .inspect("parent")
        .await
        .unwrap()
        .activation_pending());
    let dir = inbox.inbox_dir("parent").await.unwrap();
    assert_eq!(
        FileSessionInbox::read_activation_watermark(&dir)
            .await
            .unwrap(),
        (0, true)
    );
    inbox.admission_commit_pause = None;
    let committed = SessionMessageEnvelope::user_input("parent", "cancel after");
    let committed_gate = AdmissionGate::default();
    let receipt = inbox
        .deliver_with_activation_intent(
            &committed,
            SessionActivationPolicy::InterruptSpecificWait,
            Some(&committed_gate),
        )
        .await
        .unwrap();
    committed_gate.cancel_if_pending();
    let restarted = reopen(&temp).await;
    assert!(restarted
        .inspect("parent")
        .await
        .unwrap()
        .activation_pending());
    assert_eq!(
        restarted
            .deliver_with_activation_intent(
                &committed,
                SessionActivationPolicy::InterruptSpecificWait,
                Some(&committed_gate)
            )
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(restarted.claim("parent", 128).await.unwrap().len(), 1);
}

#[tokio::test]
async fn immediate_upgrade_preserves_legacy_prefix_and_fences_v1_only_writers() {
    let (temp, _store, inbox) = fixture().await;
    let first = inbox.deliver(&staged_child()).await.unwrap();
    inbox
        .mark_activation_eligible(
            "parent",
            first.generation,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .unwrap();
    let dir = inbox.inbox_dir("parent").await.unwrap();
    let path = dir.join(ACTIVATION_GENERATION_FILE);
    assert_eq!(
        tokio::fs::read_to_string(&path)
            .await
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap(),
        1
    );
    assert_eq!(
        FileSessionInbox::read_activation_watermark(&dir)
            .await
            .unwrap(),
        (1, false)
    );
    let staged = inbox.deliver(&staged_bash()).await.unwrap();
    let immediate = SessionMessageEnvelope::user_input("parent", "immediate");
    let receipt = inbox
        .deliver_with_activation_intent(
            &immediate,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let upgraded = tokio::fs::read(&path).await.unwrap();
    assert_eq!(
        FileSessionInbox::read_activation_watermark(&dir)
            .await
            .unwrap(),
        (1, true)
    );
    // The previous version reads this integer before writing a larger prefix
    // or claiming anything. Its exact parser rejects the v2 body, so it cannot
    // release the intervening staged generation as part of immediate wakeup.
    assert!(std::str::from_utf8(&upgraded)
        .unwrap()
        .trim()
        .parse::<u64>()
        .is_err());
    assert!(v1_only_mark(&inbox, receipt.generation).await.is_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), upgraded);
    let restarted = reopen(&temp).await;
    let claims = restarted.claim("parent", 128).await.unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| claim.generation)
            .collect::<Vec<_>>(),
        vec![first.generation, receipt.generation]
    );
    assert!(!claims
        .iter()
        .any(|claim| claim.generation == staged.generation));
    restarted
        .mark_activation_eligible(
            "parent",
            staged.generation,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .unwrap();
    assert_eq!(
        FileSessionInbox::read_activation_watermark(&dir)
            .await
            .unwrap(),
        (staged.generation, true)
    );
    // A downgraded control file cannot authorize an already published intent.
    atomic_write(&path, staged.generation.to_string().as_bytes())
        .await
        .unwrap();
    assert!(matches!(
        restarted.inspect("parent").await,
        Err(SessionInboxError::InvalidClaim(_))
    ));
}

#[tokio::test]
async fn malformed_intent_errors_are_static_and_zero_generation_is_never_eligible() {
    let (_temp, _store, inbox) = fixture().await;
    let dir = inbox.inbox_dir("parent").await.unwrap();
    let envelope = SessionMessageEnvelope::user_input("parent", "legacy");
    let zero = FileSessionInbox::wrapper(&envelope, 0, None);
    Mailbox::at(&dir).deliver(&zero).await.unwrap();
    assert!(inbox.claim("parent", 128).await.unwrap().is_empty());
    assert!(!inbox.inspect("parent").await.unwrap().activation_pending());
    let malformed = serde_json::json!({"session_inbox_activation_intent": {"version": 1, "policy": "private-sentinel"}});
    let error = FileSessionInbox::activation_intent(&malformed)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("private-sentinel"));
    assert!(error.contains("invalid SessionInbox activation intent"));
}

#[tokio::test]
async fn intent_scan_bounds_actual_growth_after_stat_and_keeps_malformed_file() {
    let (_temp, _store, mut inbox) = fixture().await;
    inbox.limits.max_payload_bytes = 1024;
    let envelope = SessionMessageEnvelope::user_input("parent", "immediate");
    inbox
        .deliver_with_activation_intent(
            &envelope,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let dir = inbox.inbox_dir("parent").await.unwrap();
    let path = FileSessionInbox::valid_queue_entries(&dir, "new")
        .await
        .unwrap()[0]
        .2
        .clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    inbox.intent_scan_pause = Some((entered.clone(), release.clone()));
    let reading = tokio::spawn({
        let inbox = inbox.clone();
        async move { inbox.inspect("parent").await }
    });
    entered.notified().await;
    let mut bytes = b"private-growth-sentinel-invalid-json".to_vec();
    bytes.resize(inbox.max_transport_bytes() + 128, b'x');
    tokio::fs::write(&path, &bytes).await.unwrap();
    release.notify_one();
    let error = reading.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("inbox activation scan exceeds byte limit"));
    assert!(!error.contains("private-growth-sentinel"));
    assert_eq!(
        tokio::fs::metadata(&path).await.unwrap().len(),
        bytes.len() as u64
    );
    inbox.intent_scan_pause = None;
    // The metadata precheck also fails closed and does not quarantine/delete.
    assert!(inbox.inspect("parent").await.is_err());
    assert!(path.exists());
}

#[tokio::test]
async fn immediate_pretty_transport_bound_rejects_before_message_publication() {
    let (_temp, _store, mut inbox) = fixture().await;
    inbox.limits.max_payload_bytes = 1024;
    let mut data = serde_json::json!("private-transport-sentinel");
    for _ in 0..100 {
        data = serde_json::json!({"n": data});
    }
    let mut envelope = staged_bash();
    if let SessionMessageBody::RuntimeInstruction(instruction) = &mut envelope.body {
        instruction.data = Some(data);
    }
    assert!(serde_json::to_vec(&envelope).unwrap().len() <= inbox.limits.max_payload_bytes);
    let error = inbox
        .deliver_with_activation_intent(
            &envelope,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SessionInboxError::PayloadTooLarge { .. }));
    assert!(!error.to_string().contains("private-transport-sentinel"));
    let backlog = inbox.inspect("parent").await.unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
    assert!(!backlog.activation_pending());
}
