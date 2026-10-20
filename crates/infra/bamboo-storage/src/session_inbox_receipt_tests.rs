//! Exact selection reuses the real owned lease, physical lock and ACK path.
use super::*;
use bamboo_domain::{
    Session, SessionInboxConsumerId, SessionInboxFailureOutcome, SessionInboxFailureReport,
    SessionInboxLeaseRequest, Storage,
};
use chrono::Duration;
use tempfile::TempDir;

async fn fixture() -> (TempDir, Arc<SessionStoreV2>, FileSessionInbox) {
    let temp = TempDir::new().unwrap();
    let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
    store
        .save_session(&Session::new("target", "model"))
        .await
        .unwrap();
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    (temp, store, inbox)
}

async fn reopen(temp: &TempDir) -> FileSessionInbox {
    FileSessionInbox::new(
        Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap()),
        SessionInboxLimits::default(),
    )
}

fn request(now: chrono::DateTime<Utc>) -> SessionInboxLeaseRequest {
    SessionInboxLeaseRequest {
        consumer: SessionInboxConsumerId::new(),
        now,
        duration: Duration::seconds(10),
    }
}

async fn immediate(inbox: &FileSessionInbox, text: &str) -> SessionInboxReceipt {
    inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input("target", text),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap()
}

async fn assert_only_pending(inbox: &FileSessionInbox, count: usize) {
    let backlog = inbox.inspect("target").await.unwrap();
    assert_eq!(backlog.pending, count);
    assert_eq!(backlog.claimed, 0);
}

#[tokio::test]
async fn receipt_claim_selects_exact_delivery_without_claiming_other_eligible_or_staged_inputs() {
    let (_temp, _, inbox) = fixture().await;
    let staged = inbox
        .deliver(&SessionMessageEnvelope::user_input("target", "staged"))
        .await
        .unwrap();
    let first = immediate(&inbox, "earlier eligible").await;
    let selected = immediate(&inbox, "selected eligible").await;
    let last = immediate(&inbox, "later eligible").await;
    let claim = inbox
        .claim_owned_receipt("target", &selected, None, &request(Utc::now()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.claim.envelope.id, selected.id);
    assert_eq!(claim.claim.generation, selected.generation);
    assert_eq!(
        claim.claim.activation_policy,
        SessionActivationPolicy::RespectSpecificWait
    );
    let backlog = inbox.inspect("target").await.unwrap();
    assert_eq!(
        (
            backlog.pending,
            backlog.claimed,
            backlog.coordinator_generation
        ),
        (3, 1, 0)
    );
    let leases = inbox
        .inspect_owned_leases("target", 128, Utc::now())
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].generation, selected.generation);
    let other = inbox
        .claim_owned("target", 128, None, &request(Utc::now()))
        .await
        .unwrap();
    assert_eq!(
        other
            .iter()
            .map(|c| c.claim.envelope.id.clone())
            .collect::<Vec<_>>(),
        vec![first.id, last.id]
    );
    assert!(!other.iter().any(|c| c.claim.envelope.id == staged.id));
}

#[tokio::test]
async fn receipt_claim_wrong_missing_zero_and_cross_target_selectors_never_fall_back() {
    let (_temp, store, inbox) = fixture().await;
    let first = immediate(&inbox, "first").await;
    let second = immediate(&inbox, "second").await;
    let lease = request(Utc::now());
    for receipt in [
        SessionInboxReceipt {
            id: first.id.clone(),
            generation: second.generation,
        },
        SessionInboxReceipt {
            id: second.id.clone(),
            generation: first.generation,
        },
        SessionInboxReceipt {
            id: SessionMessageId::parse("not-delivered").unwrap(),
            generation: first.generation,
        },
        SessionInboxReceipt {
            id: first.id.clone(),
            generation: second.generation + 1,
        },
    ] {
        assert!(inbox
            .claim_owned_receipt("target", &receipt, None, &lease)
            .await
            .unwrap()
            .is_none());
        assert_only_pending(&inbox, 2).await;
    }
    let zero = SessionInboxReceipt {
        id: first.id.clone(),
        generation: 0,
    };
    assert!(inbox
        .claim_owned_receipt("target", &zero, None, &lease)
        .await
        .is_err());
    store
        .save_session(&Session::new("other", "model"))
        .await
        .unwrap();
    inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input("other", "other input"),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    assert!(inbox
        .claim_owned_receipt("other", &first, None, &lease)
        .await
        .unwrap()
        .is_none());
    assert_only_pending(&inbox, 2).await;
    let other = inbox.inspect("other").await.unwrap();
    assert_eq!((other.pending, other.claimed), (1, 0));
}

#[tokio::test]
async fn receipt_claim_same_consumer_is_idempotent_and_independent_consumer_cannot_join() {
    let (temp, _, inbox) = fixture().await;
    let selected = immediate(&inbox, "selected").await;
    let independent = reopen(&temp).await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned_receipt("target", &selected, None, &first)
        .await
        .unwrap()
        .unwrap();
    let mut retry = first.clone();
    retry.now += Duration::seconds(1);
    retry.duration += Duration::seconds(5);
    assert_eq!(
        independent
            .claim_owned_receipt("target", &selected, None, &retry)
            .await
            .unwrap(),
        Some(claim.clone())
    );
    assert!(independent
        .claim_owned_receipt("target", &selected, None, &request(retry.now))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        independent
            .inspect_owned_leases("target", 128, retry.now)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(claim.lease.expires_at, first.expires_at().unwrap());
}

#[tokio::test]
async fn receipt_claim_independent_contenders_have_one_winner_and_do_not_consume_sibling() {
    let (temp, _, inbox) = fixture().await;
    let sibling = immediate(&inbox, "sibling").await;
    let selected = immediate(&inbox, "selected").await;
    let independent = reopen(&temp).await;
    let now = Utc::now();
    let a = request(now);
    let b = request(now);
    let (left, right) = tokio::join!(
        inbox.claim_owned_receipt("target", &selected, None, &a),
        independent.claim_owned_receipt("target", &selected, None, &b),
    );
    let claims = [left.unwrap(), right.unwrap()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].claim.envelope.id, selected.id);
    let pending = inbox
        .claim_owned("target", 128, None, &request(now))
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].claim.envelope.id, sibling.id);
}

#[tokio::test]
async fn receipt_claim_checkpoint_ack_remains_terminal_across_retry_and_restart() {
    let (temp, store, inbox) = fixture().await;
    let selected = immediate(&inbox, "selected").await;
    let sibling = immediate(&inbox, "sibling").await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned_receipt("target", &selected, None, &first)
        .await
        .unwrap()
        .unwrap();
    let mut session = store.load_session("target").await.unwrap().unwrap();
    session.add_message(claim.claim.envelope.to_provider_message().unwrap());
    session
        .session_inbox_admission_mut()
        .record(selected.id.clone(), selected.generation);
    store.save_session(&session).await.unwrap();
    inbox.ack_owned("target", &claim, Utc::now()).await.unwrap();
    let independent = reopen(&temp).await;
    for lease in [first, request(Utc::now() + Duration::minutes(1))] {
        assert!(independent
            .claim_owned_receipt("target", &selected, None, &lease)
            .await
            .unwrap()
            .is_none());
    }
    assert!(independent
        .was_admitted("target", &selected.id)
        .await
        .unwrap());
    assert_only_pending(&independent, 1).await;
    assert_eq!(
        independent
            .claim_owned_receipt("target", &sibling, None, &request(Utc::now()))
            .await
            .unwrap()
            .unwrap()
            .claim
            .envelope
            .id,
        sibling.id
    );
    let durable = store.load_session("target").await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| m.id == selected.id.as_str())
            .count(),
        1
    );
}

#[tokio::test]
async fn receipt_claim_expiry_and_release_reuse_epochs_and_reject_stale_tokens() {
    let (temp, _, inbox) = fixture().await;
    let selected = immediate(&inbox, "selected").await;
    let first = request(Utc::now());
    let old = inbox
        .claim_owned_receipt("target", &selected, None, &first)
        .await
        .unwrap()
        .unwrap();
    let independent = reopen(&temp).await;
    let next_request = request(old.lease.expires_at);
    let next = independent
        .claim_owned_receipt("target", &selected, None, &next_request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.claim.envelope, old.claim.envelope);
    assert_eq!(next.claim.generation, selected.generation);
    assert_eq!(next.lease.epoch, old.lease.epoch + 1);
    assert_ne!(next.lease.incarnation, old.lease.incarnation);
    assert!(inbox
        .ack_owned("target", &old, next_request.now)
        .await
        .is_err());
    assert!(inbox.release_owned("target", &old).await.is_err());

    // Use a separate current wall-clock lease for the actual release path.
    let released_receipt = immediate(&inbox, "release").await;
    let released = inbox
        .claim_owned_receipt("target", &released_receipt, None, &request(Utc::now()))
        .await
        .unwrap()
        .unwrap();
    inbox.release_owned("target", &released).await.unwrap();
    let recovered = independent
        .claim_owned_receipt("target", &released_receipt, None, &request(Utc::now()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.claim.envelope, released.claim.envelope);
    assert_eq!(recovered.lease.epoch, released.lease.epoch + 1);
    assert!(inbox
        .renew_owned("target", &released, &request(Utc::now()))
        .await
        .is_err());
    let inspection = independent
        .inspect_owned_leases("target", 128, Utc::now())
        .await
        .unwrap();
    assert!(inspection.iter().all(|entry| entry.failure_count == 0));
}

#[tokio::test]
async fn receipt_claim_does_not_grant_staged_inputs_or_interrupt_successor_waits() {
    let (_temp, _, inbox) = fixture().await;
    let staged = inbox
        .deliver(&SessionMessageEnvelope::user_input("target", "staged"))
        .await
        .unwrap();
    let mut deferred = SessionMessageEnvelope::user_input("target", "after run");
    deferred.correlation_id = Some("session-guidance-after-run:run-a".into());
    let deferred_receipt = inbox
        .deliver_with_activation_intent(
            &deferred,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let lease = request(Utc::now());
    assert!(inbox
        .claim_owned_receipt("target", &staged, Some("run-a"), &lease)
        .await
        .unwrap()
        .is_none());
    assert!(inbox
        .claim_owned_receipt("target", &deferred_receipt, Some("run-a"), &lease)
        .await
        .unwrap()
        .is_none());
    assert_only_pending(&inbox, 2).await;
    let successor = inbox
        .claim_owned_receipt("target", &deferred_receipt, Some("run-b"), &lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(successor.claim.envelope, deferred);
    assert_eq!(
        successor.claim.activation_policy,
        SessionActivationPolicy::RespectSpecificWait
    );
    assert_eq!(
        inbox
            .coordinator_activation_generation("target")
            .await
            .unwrap(),
        0
    );
    inbox
        .mark_message_activation_eligible(
            "target",
            &staged,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .unwrap();
    assert_eq!(
        inbox
            .claim_owned_receipt("target", &staged, Some("run-b"), &lease)
            .await
            .unwrap()
            .unwrap()
            .claim
            .envelope
            .id,
        staged.id
    );
}

#[tokio::test]
async fn receipt_claim_does_not_bypass_retry_deadline_or_dead_letter() {
    let (_temp, _, inbox) = fixture().await;
    let selected = immediate(&inbox, "failing").await;
    let sibling = immediate(&inbox, "sibling").await;
    let mut now = Utc::now();
    for failure_count in 1..=3 {
        let claim = inbox
            .claim_owned_receipt("target", &selected, None, &request(now))
            .await
            .unwrap()
            .unwrap();
        let failed_at = now + Duration::seconds(1);
        let outcome = inbox
            .fail_owned(
                "target",
                &claim,
                &SessionInboxFailureReport {
                    now: failed_at,
                    error_code: "fixture_failure".into(),
                },
            )
            .await
            .unwrap();
        assert!(inbox
            .claim_owned_receipt("target", &selected, None, &request(failed_at))
            .await
            .unwrap()
            .is_none());
        match outcome {
            SessionInboxFailureOutcome::RetryScheduled {
                failure_count: count,
                retry_after,
            } => {
                assert_eq!(count, failure_count);
                assert!(inbox
                    .claim_owned_receipt(
                        "target",
                        &selected,
                        None,
                        &request(retry_after - Duration::milliseconds(1))
                    )
                    .await
                    .unwrap()
                    .is_none());
                now = retry_after;
            }
            SessionInboxFailureOutcome::DeadLettered {
                failure_count: count,
            } => {
                assert_eq!(count, 3);
                assert!(inbox
                    .claim_owned_receipt(
                        "target",
                        &selected,
                        None,
                        &request(failed_at + Duration::days(1))
                    )
                    .await
                    .unwrap()
                    .is_none());
            }
        }
    }
    assert_eq!(
        inbox
            .claim_owned("target", 128, None, &request(now))
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.claim.envelope.id)
            .collect::<Vec<_>>(),
        vec![sibling.id]
    );
}

#[test]
fn receipt_claim_cancelled_caller_keeps_real_filesystem_job_locked_until_recoverable() {
    use super::owned_lifetime_tests::{Fixture, Parked};
    let fixture = Fixture::new();
    let input = fixture.deliver();
    let selected = SessionInboxReceipt {
        id: input.id.clone(),
        generation: 1,
    };
    let request = fixture.request(0);
    let path = std::fs::read_dir(fixture.dir.join("new"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut parked = Parked::start(
        fixture.inbox.clone(),
        "replace",
        path,
        move |inbox| async move {
            inbox
                .claim_owned_receipt("target", &selected, None, &request)
                .await
                .map(|_| ())
        },
    );
    parked.shutdown();
    fixture.assert_physical_locks(true);
    let successor = fixture.successor(11);
    fixture.assert_waiting(&successor);
    parked.barrier.release();
    let claims = fixture.finish(successor);
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].claim.envelope, input);
    assert_eq!(claims[0].lease.epoch, 2);
}
