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

fn peer_envelope(source: &str, target: &str) -> SessionMessageEnvelope {
    let mut envelope = SessionMessageEnvelope::user_input(target, "peer feedback");
    envelope.source = SessionMessageSource::Session {
        session_id: source.into(),
    };
    envelope.kind = SessionMessageKind::PeerMessage;
    envelope.thread_id = Some("peer-thread".into());
    envelope.correlation_id = Some("peer-operation".into());
    envelope
}

#[tokio::test]
async fn peer_admission_preserves_legacy_lineage_precise_intent_and_ack_retry() {
    let (_temp, store, inbox) = fixture().await;
    let parent = store.load_session("parent").await.unwrap().unwrap();
    // Existing owned-tree membership accepts a legacy child without this witness.
    let child = Session::new_child("a-child", "parent", "model", "child");
    assert!(child.parent_created_at.is_none());
    store.save_session(&child).await.unwrap();
    let source = bamboo_domain::ActorSession::from_session(&child).unwrap();
    let target = bamboo_domain::ActorSession::from_session(&parent).unwrap();
    let staged = staged_child();
    inbox.deliver(&staged).await.unwrap();
    let envelope = peer_envelope(&child.id, &parent.id);
    let receipt = inbox
        .deliver_peer_message(&envelope, &source, &target)
        .await
        .unwrap();
    assert_eq!(
        inbox
            .deliver_peer_message(&envelope, &source, &target)
            .await
            .unwrap(),
        receipt
    );
    let claims = inbox.claim("parent", 128).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].envelope, envelope);
    assert_eq!(
        claims[0].activation_policy,
        SessionActivationPolicy::InterruptSpecificWait
    );
    inbox.ack("parent", &claims[0]).await.unwrap();
    assert_eq!(
        inbox
            .deliver_peer_message(&envelope, &source, &target)
            .await
            .unwrap(),
        receipt
    );
    let backlog = inbox.inspect("parent").await.unwrap();
    assert_eq!(backlog.pending, 1);
    assert_eq!(backlog.coordinator_generation, 0);
    assert!(!backlog.activation_pending());
    assert!(inbox.claim("parent", 128).await.unwrap().is_empty());
}

#[test]
fn peer_admission_retains_authority_through_commit_with_lifecycle_writer_queued() {
    use super::owned_lifetime_tests::{Fixture, Parked};
    let fixture = Fixture::new();
    let child = Session::new_child("a-child", "target", "model", "child");
    let (source, target) = fixture.runtime.block_on(async {
        fixture.store.save_session(&child).await.unwrap();
        let parent = fixture.store.load_session("target").await.unwrap().unwrap();
        (
            bamboo_domain::ActorSession::from_session(&child).unwrap(),
            bamboo_domain::ActorSession::from_session(&parent).unwrap(),
        )
    });
    let envelope = peer_envelope(&child.id, "target");
    let delivery = envelope.clone();
    let mut parked = Parked::start(
        fixture.inbox.clone(),
        "acquire",
        fixture.dir.clone(),
        move |inbox| async move {
            inbox
                .deliver_peer_message(&delivery, &source, &target)
                .await
                .map(|_| ())
        },
    );
    fixture.assert_physical_locks(false);
    let writer_store = fixture.store.clone();
    let mut writer = Box::pin(writer_store.delete_session_recursive("a-child", true));
    fixture.runtime.block_on(std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(writer.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    }));
    // Polling once registers the waiting writer on this same Store before
    // resuming admission; no scheduler timing assumption is needed.
    parked.result().unwrap();
    assert!(fixture.runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(10), writer)
            .await
            .unwrap()
            .unwrap()
    }));
    let claims = fixture
        .runtime
        .block_on(fixture.other.claim("target", 1))
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].envelope, envelope);
    assert_eq!(
        claims[0].activation_policy,
        SessionActivationPolicy::InterruptSpecificWait
    );
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
async fn pending_child_lookup_keeps_permissions_and_excludes_acknowledged_outcomes() {
    let (_temp, _store, inbox) = fixture().await;
    let child = staged_child();
    let receipt = inbox.deliver(&child).await.unwrap();
    let mut other = staged_child();
    if let SessionMessageBody::ChildOutcome(outcome) = &mut other.body {
        outcome.child_session_id = "other-child".into();
    }
    inbox.deliver(&other).await.unwrap();
    inbox.deliver(&staged_bash()).await.unwrap();
    let before = inbox.inspect("parent").await.unwrap();
    assert_eq!(
        inbox
            .pending_child_outcomes("parent", "child")
            .await
            .unwrap(),
        vec![child.clone()]
    );
    assert!(inbox
        .pending_child_outcomes("parent", "missing")
        .await
        .unwrap()
        .is_empty());
    let after = inbox.inspect("parent").await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.coordinator_generation, 0);
    assert_eq!(after.activation_generation, 0);
    assert!(inbox.claim("parent", 128).await.unwrap().is_empty());

    inbox
        .mark_message_activation_eligible(
            "parent",
            &receipt,
            SessionActivationPolicy::InterruptSpecificWait,
        )
        .await
        .unwrap();
    let claim = inbox.claim("parent", 128).await.unwrap().remove(0);
    let dir = inbox.inbox_dir("parent").await.unwrap();
    let path = dir.join("cur").join(&claim.claim_id);
    let transport = tokio::fs::read(&path).await.unwrap();
    assert_eq!(
        inbox
            .pending_child_outcomes("parent", "child")
            .await
            .unwrap(),
        vec![child]
    );
    assert_eq!(tokio::fs::read(&path).await.unwrap(), transport);
    inbox.ack("parent", &claim).await.unwrap();
    assert!(inbox
        .pending_child_outcomes("parent", "child")
        .await
        .unwrap()
        .is_empty());

    // An ACK can survive a crash before cur removal. Lookup remains read-only
    // and must not return that already consumed canonical envelope.
    tokio::fs::write(&path, &transport).await.unwrap();
    assert!(inbox
        .pending_child_outcomes("parent", "child")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), transport);
    let malformed = dir.join("new").join("malformed.json");
    tokio::fs::write(&malformed, b"unchanged").await.unwrap();
    assert!(inbox
        .pending_child_outcomes("parent", "child")
        .await
        .is_err());
    assert_eq!(tokio::fs::read(&malformed).await.unwrap(), b"unchanged");
}

#[tokio::test]
async fn pending_child_lookup_skips_invalid_bodies_without_mutating_or_granting_them() {
    let mut mismatched = staged_bash();
    mismatched.kind = SessionMessageKind::ChildOutcome;
    let mut empty_history = staged_child();
    let SessionMessageBody::ChildOutcome(outcome) = &mut empty_history.body else {
        unreachable!()
    };
    outcome.provider_message = Some(bamboo_domain::SessionProviderMessage {
        content: SessionMessageContent::text(""),
        metadata: serde_json::Map::new(),
        never_compress: false,
    });
    let mut forged_history = empty_history.clone();
    let SessionMessageBody::ChildOutcome(outcome) = &mut forged_history.body else {
        unreachable!()
    };
    let presentation = outcome.provider_message.as_mut().unwrap();
    presentation.content = SessionMessageContent::text("forged history");
    presentation.metadata.insert(
        "session_message".into(),
        serde_json::json!({"forged": true}),
    );
    let invalid_bodies = [
        serde_json::json!({"unrelated": "not an envelope"}),
        serde_json::to_value(mismatched).unwrap(),
        serde_json::to_value(empty_history).unwrap(),
        serde_json::to_value(forged_history).unwrap(),
    ];
    for queue in ["new", "cur"] {
        for body in &invalid_bodies {
            assert!(
                !serde_json::from_value::<SessionMessageEnvelope>(body.clone())
                    .is_ok_and(|envelope| envelope.validate().is_ok())
            );
            let (_temp, _store, inbox) = fixture().await;
            let child = staged_child();
            let receipt = inbox.deliver(&child).await.unwrap();
            let dir = inbox.inbox_dir("parent").await.unwrap();
            let good_path = FileSessionInbox::valid_queue_entries(&dir, "new")
                .await
                .unwrap()
                .remove(0)
                .2;
            let good_bytes = tokio::fs::read(&good_path).await.unwrap();
            let mut wrapper = FileSessionInbox::wrapper(&staged_bash(), 2, None);
            wrapper.body = body.clone();
            let bad_bytes = serde_json::to_vec(&wrapper).unwrap();
            let bad_path = dir
                .join(queue)
                .join("00000000000000000002-invalid-body.json");
            tokio::fs::create_dir_all(bad_path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&bad_path, &bad_bytes).await.unwrap();

            assert_eq!(
                inbox
                    .pending_child_outcomes("parent", "child")
                    .await
                    .unwrap(),
                vec![child.clone()]
            );
            assert!(inbox
                .pending_child_outcomes("parent", "missing")
                .await
                .unwrap()
                .is_empty());
            assert_eq!(tokio::fs::read(&bad_path).await.unwrap(), bad_bytes);
            assert_eq!(tokio::fs::read(&good_path).await.unwrap(), good_bytes);
            assert_eq!(
                FileSessionInbox::read_activation_watermark(&dir)
                    .await
                    .unwrap(),
                (0, false)
            );
            assert_eq!(
                FileSessionInbox::read_interrupt_generation(&dir)
                    .await
                    .unwrap(),
                0
            );
            assert!(!FileSessionInbox::admitted_path(&dir, &receipt.id).exists());
            assert_eq!(
                FileSessionInbox::count_json(&dir.join("admitted"))
                    .await
                    .unwrap(),
                0
            );
            assert!(!dir.join("corrupt").exists());
        }
    }
}

#[tokio::test]
async fn pending_child_lookup_preserves_transport_authority_and_receipt_errors() {
    for failure in ["target", "intent", "lease", "receipt", "io"] {
        let (_temp, _store, inbox) = fixture().await;
        let child = staged_child();
        inbox.deliver(&child).await.unwrap();
        let dir = inbox.inbox_dir("parent").await.unwrap();
        let path = FileSessionInbox::valid_queue_entries(&dir, "new")
            .await
            .unwrap()
            .remove(0)
            .2;
        let mut transport: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        match failure {
            "target" => transport["body"]["target_session_id"] = serde_json::json!("other"),
            "intent" => {
                transport["body"][COORDINATOR_INTENT_KEY] =
                    serde_json::to_value(SessionInboxActivationIntent::new(
                        SessionActivationPolicy::InterruptSpecificWait,
                    ))
                    .unwrap();
            }
            "lease" => transport["session_inbox_lease"] = serde_json::json!({"version": 0}),
            "receipt" => {
                let receipt = FileSessionInbox::admitted_path(&dir, &child.id);
                tokio::fs::create_dir_all(receipt.parent().unwrap())
                    .await
                    .unwrap();
                tokio::fs::write(
                    receipt,
                    serde_json::to_vec(&serde_json::json!({
                        "id": child.id,
                        "generation": 2,
                        "semantic_digest": FileSessionInbox::semantic_digest(&child).unwrap()
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            }
            "io" => {
                tokio::fs::remove_file(&path).await.unwrap();
                tokio::fs::create_dir(&path).await.unwrap();
            }
            _ => unreachable!(),
        }
        if failure != "io" {
            tokio::fs::write(&path, serde_json::to_vec(&transport).unwrap())
                .await
                .unwrap();
        }
        let error = inbox
            .pending_child_outcomes("parent", "child")
            .await
            .unwrap_err();
        if failure == "io" {
            assert!(matches!(error, SessionInboxError::Storage(_)));
        } else {
            assert!(matches!(error, SessionInboxError::InvalidClaim(_)));
        }
        assert!(!dir.join("corrupt").exists());
    }
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

#[tokio::test]
async fn exact_promotion_survives_restart_without_releasing_staged_siblings_or_changing_retries() {
    for original in [None, Some(SessionActivationPolicy::RespectSpecificWait)] {
        let (temp, _store, inbox) = fixture().await;
        inbox.deliver(&staged_bash()).await.unwrap();
        let child = staged_child();
        let receipt = match original {
            Some(policy) => {
                inbox
                    .deliver_with_activation_intent(&child, policy, None)
                    .await
            }
            None => inbox.deliver(&child).await,
        }
        .unwrap();
        if original.is_none() {
            assert_eq!(
                inbox.existing_staged_delivery(&child).await.unwrap(),
                Some(receipt.clone())
            );
            assert!(inbox
                .existing_staged_delivery(&staged_child())
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                inbox.inspect("parent").await.unwrap().generation,
                receipt.generation
            );
            let mut mismatch = child.clone();
            mismatch.body = staged_child().body;
            if let SessionMessageBody::ChildOutcome(outcome) = &mut mismatch.body {
                outcome.result = Some("changed result".into());
            }
            assert!(inbox.existing_staged_delivery(&mismatch).await.is_err());
        }
        inbox.deliver(&staged_bash()).await.unwrap();
        for policy in [
            SessionActivationPolicy::RespectSpecificWait,
            SessionActivationPolicy::InterruptSpecificWait,
            SessionActivationPolicy::RespectSpecificWait,
        ] {
            inbox
                .mark_message_activation_eligible("parent", &receipt, policy)
                .await
                .unwrap();
        }
        let restarted = reopen(&temp).await;
        let backlog = restarted.inspect("parent").await.unwrap();
        assert_eq!(backlog.coordinator_generation, 0);
        assert_eq!(backlog.activation_generation, receipt.generation);
        assert!(
            backlog.interrupt_pending(),
            "a later Respect cannot downgrade the grant"
        );
        let retry = match original {
            Some(policy) => {
                restarted
                    .deliver_with_activation_intent(&child, policy, None)
                    .await
            }
            None => restarted.deliver(&child).await,
        }
        .unwrap();
        assert_eq!(retry, receipt);
        assert!(
            restarted
                .deliver_with_activation_intent(
                    &child,
                    SessionActivationPolicy::InterruptSpecificWait,
                    None
                )
                .await
                .is_err(),
            "promotion must not rewrite immutable delivery intent"
        );
        let claims = restarted.claim("parent", 128).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope, child);
        assert_eq!(
            claims[0].activation_policy,
            SessionActivationPolicy::InterruptSpecificWait
        );
        restarted.ack("parent", &claims[0]).await.unwrap();
        let after_ack = reopen(&temp).await;
        if original.is_none() {
            assert_eq!(
                after_ack.existing_staged_delivery(&child).await.unwrap(),
                Some(receipt.clone())
            );
        }
        after_ack
            .mark_message_activation_eligible(
                "parent",
                &receipt,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let retry = match original {
            Some(policy) => {
                after_ack
                    .deliver_with_activation_intent(&child, policy, None)
                    .await
            }
            None => after_ack.deliver(&child).await,
        }
        .unwrap();
        assert_eq!(retry, receipt);
        let backlog = after_ack.inspect("parent").await.unwrap();
        assert_eq!(backlog.pending, 2);
        assert_eq!(backlog.claimed, 0);
        assert!(
            !backlog.activation_pending(),
            "ACK promotion must not resurrect work"
        );
        let mut wrong = receipt.clone();
        wrong.generation += 1;
        assert!(after_ack
            .mark_message_activation_eligible(
                "parent",
                &wrong,
                SessionActivationPolicy::InterruptSpecificWait
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn exact_promotion_preserves_owned_claim_identity_and_ack_across_restart() {
    for before_claim in [false, true] {
        let (temp, _store, inbox) = fixture().await;
        let child = staged_child();
        let receipt = inbox.deliver(&child).await.unwrap();
        inbox
            .mark_message_activation_eligible(
                "parent",
                &receipt,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .unwrap();
        if before_claim {
            inbox
                .mark_message_activation_eligible(
                    "parent",
                    &receipt,
                    SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
        }
        let request = bamboo_domain::SessionInboxLeaseRequest {
            consumer: bamboo_domain::SessionInboxConsumerId::new(),
            now: Utc::now(),
            duration: chrono::Duration::minutes(5),
        };
        let claim = inbox
            .claim_owned("parent", 1, None, &request)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            claim.claim.activation_policy,
            if before_claim {
                SessionActivationPolicy::InterruptSpecificWait
            } else {
                SessionActivationPolicy::RespectSpecificWait
            }
        );
        let restarted = reopen(&temp).await;
        restarted
            .mark_message_activation_eligible(
                "parent",
                &receipt,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        assert!(restarted
            .inspect("parent")
            .await
            .unwrap()
            .interrupt_pending());
        assert_eq!(restarted.deliver(&child).await.unwrap(), receipt);
        let repeated = restarted
            .claim_owned("parent", 1, None, &request)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            repeated, claim,
            "promotion must not invalidate an existing lease"
        );
        restarted
            .ack_owned("parent", &claim, request.now)
            .await
            .unwrap();
        restarted
            .mark_message_activation_eligible(
                "parent",
                &receipt,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        assert!(!restarted
            .inspect("parent")
            .await
            .unwrap()
            .activation_pending());
    }
}

#[tokio::test]
async fn exact_promotion_rejects_wrong_pending_identity_and_cancelled_deliveries() {
    let (_temp, _store, inbox) = fixture().await;
    let child = staged_child();
    let receipt = inbox.deliver(&child).await.unwrap();
    inbox.deliver(&staged_bash()).await.unwrap();
    for wrong in [
        SessionInboxReceipt {
            id: receipt.id.clone(),
            generation: receipt.generation + 1,
        },
        SessionInboxReceipt {
            id: SessionMessageId::new(),
            generation: receipt.generation,
        },
        SessionInboxReceipt {
            id: receipt.id.clone(),
            generation: 0,
        },
    ] {
        assert!(inbox
            .mark_message_activation_eligible(
                "parent",
                &wrong,
                SessionActivationPolicy::InterruptSpecificWait
            )
            .await
            .is_err());
    }
    assert!(!inbox.inspect("parent").await.unwrap().activation_pending());
    let mut guidance = SessionMessageEnvelope::user_input("parent", "withdrawn");
    guidance.correlation_id = Some("session-guidance".into());
    let cancelled = inbox.deliver(&guidance).await.unwrap();
    assert!(inbox.cancel_guidance("parent", &guidance.id).await.unwrap());
    assert!(inbox
        .mark_message_activation_eligible(
            "parent",
            &cancelled,
            SessionActivationPolicy::InterruptSpecificWait
        )
        .await
        .is_err());
    assert!(!inbox.inspect("parent").await.unwrap().activation_pending());
}
