use super::*;
use bamboo_domain::{
    Session, SessionInboxAdministrationPrincipal, SessionInboxConsumerId,
    SessionInboxFailureOutcome, SessionInboxFailureReport, SessionInboxLeaseRequest, Storage,
};
use chrono::{DateTime, Duration};
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
async fn principal(store: &Arc<SessionStoreV2>) -> SessionInboxAdministrationPrincipal {
    let session = store.load_session("target").await.unwrap().unwrap();
    SessionInboxAdministrationPrincipal::authenticated_host_owner_for(&session)
}
fn request(now: DateTime<Utc>) -> SessionInboxLeaseRequest {
    SessionInboxLeaseRequest {
        consumer: SessionInboxConsumerId::new(),
        now,
        duration: Duration::seconds(10),
    }
}
async fn immediate(inbox: &FileSessionInbox, text: &str) -> SessionMessageEnvelope {
    let envelope = SessionMessageEnvelope::user_input("target", text);
    inbox
        .deliver_with_activation_intent(
            &envelope,
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    envelope
}

#[test]
fn explicit_retry_delay_is_exponential_bounded_and_stable() {
    let id = SessionMessageId::parse("retry-envelope").unwrap();
    let generation = 17;
    for (failure_count, lower, upper) in [
        (1, 30_000, 37_500),
        (2, 60_000, 75_000),
        (3, 120_000, 150_000),
        (4, 240_000, 300_000),
        (5, 300_000, 300_000),
    ] {
        let delay = owned::retry_delay(&id, generation, failure_count).num_milliseconds();
        assert!((lower..=upper).contains(&delay));
        assert_eq!(
            delay,
            owned::retry_delay(&id, generation, failure_count).num_milliseconds()
        );
    }
    let offsets = (0..16)
        .map(|index| {
            owned::retry_delay(
                &SessionMessageId::parse(format!("retry-envelope-{index}")).unwrap(),
                generation,
                1,
            )
            .num_milliseconds()
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(offsets.len() > 1, "jitter should spread distinct envelopes");
}

#[tokio::test]
async fn wake_readiness_does_not_fence_legacy_root_turn_claim() {
    let (_temp, _store, inbox) = fixture().await;
    let envelope = immediate(&inbox, "root input").await;
    let readiness = inbox
        .inspect_wake_readiness("target", Utc::now())
        .await
        .unwrap();
    assert_eq!(readiness.ready.unwrap().id, envelope.id);
    let dir = inbox.inbox_dir("target").await.unwrap();
    assert!(!FileSessionInbox::owned_enabled(&dir).await.unwrap());
    let claims = inbox
        .claim_for_turn("target", 1, Some("root-run"))
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].envelope.id, envelope.id);
}

#[tokio::test]
async fn wake_readiness_excludes_staged_and_tracks_exact_lease_expiry_after_restart() {
    let (temp, _, inbox) = fixture().await;
    let staged = SessionMessageEnvelope::user_input("target", "staged");
    inbox.deliver(&staged).await.unwrap();
    let immediate = immediate(&inbox, "immediate").await;
    let now = Utc::now();
    let first = inbox.inspect_wake_readiness("target", now).await.unwrap();
    let ready = first.ready.unwrap();
    assert_eq!(ready.id, immediate.id);
    assert_eq!(ready.generation, 2);
    assert_eq!(ready.lease_epoch, None);
    assert_eq!(first.next_due_at, None);

    let claimed = inbox
        .claim_owned("target", 1, None, &request(now))
        .await
        .unwrap()
        .remove(0);
    let reopened = reopen(&temp).await;
    let before = reopened
        .inspect_wake_readiness("target", claimed.lease.expires_at - Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(before.ready, None);
    assert_eq!(before.next_due_at, Some(claimed.lease.expires_at));
    let due = reopened
        .inspect_wake_readiness("target", claimed.lease.expires_at)
        .await
        .unwrap()
        .ready
        .unwrap();
    assert_eq!(due.id, immediate.id);
    assert_eq!(due.generation, claimed.claim.generation);
    assert_eq!(due.lease_epoch, Some(claimed.lease.epoch));
    assert_eq!(due.ready_after, Some(claimed.lease.expires_at));

    let reclaimed = reopened
        .claim_owned("target", 1, None, &request(claimed.lease.expires_at))
        .await
        .unwrap()
        .remove(0);
    reopened
        .ack_owned("target", &reclaimed, claimed.lease.expires_at)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .inspect_wake_readiness("target", claimed.lease.expires_at)
            .await
            .unwrap()
            .ready,
        None
    );
    reopened
        .mark_activation_eligible("target", 1, SessionActivationPolicy::RespectSpecificWait)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .inspect_wake_readiness("target", claimed.lease.expires_at)
            .await
            .unwrap()
            .ready
            .unwrap()
            .id,
        staged.id
    );
}

#[tokio::test]
async fn wake_readiness_waits_for_failure_retry_and_excludes_dead_letter() {
    let (_temp, _, inbox) = fixture().await;
    let envelope = immediate(&inbox, "poison").await;
    let mut now = Utc::now();
    for failure_count in 1..=3 {
        let claim = inbox
            .claim_owned("target", 1, None, &request(now))
            .await
            .unwrap()
            .remove(0);
        let failed_at = now + Duration::seconds(1);
        let outcome = inbox
            .fail_owned(
                "target",
                &claim,
                &SessionInboxFailureReport {
                    now: failed_at,
                    error_code: "consumer_rejected".into(),
                },
            )
            .await
            .unwrap();
        if failure_count == 3 {
            assert_eq!(
                outcome,
                SessionInboxFailureOutcome::DeadLettered { failure_count }
            );
            let after = inbox
                .inspect_wake_readiness("target", failed_at + Duration::hours(1))
                .await
                .unwrap();
            assert_eq!(after.ready, None);
            assert_eq!(after.next_due_at, None);
            break;
        }
        let SessionInboxFailureOutcome::RetryScheduled { retry_after, .. } = outcome else {
            panic!("expected scheduled retry");
        };
        let before = inbox
            .inspect_wake_readiness("target", retry_after - Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(before.ready, None);
        assert_eq!(before.next_due_at, Some(retry_after));
        let ready = inbox
            .inspect_wake_readiness("target", retry_after)
            .await
            .unwrap()
            .ready
            .unwrap();
        assert_eq!(ready.id, envelope.id);
        assert_eq!(ready.generation, claim.claim.generation);
        assert_eq!(ready.lease_epoch, Some(claim.lease.epoch));
        assert_eq!(ready.ready_after, Some(retry_after));
        now = retry_after;
    }
}

#[tokio::test]
async fn expiry_reclaims_do_not_count_as_poison_failures() {
    let (temp, store, inbox) = fixture().await;
    let envelope = immediate(&inbox, "slow but valid").await;
    let mut next = request(Utc::now());
    for epoch in 1..=5 {
        let current = reopen(&temp).await;
        let claim = current
            .claim_owned("target", 1, None, &next)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(claim.claim.envelope.id, envelope.id);
        assert_eq!(claim.lease.epoch, epoch);
        let inspection = current
            .inspect_owned_leases("target", 1, next.now)
            .await
            .unwrap();
        assert_eq!(inspection[0].failure_count, 0);
        assert_eq!(inspection[0].reclaim_count, epoch - 1);
        assert_eq!(inspection[0].manual_retry_count, 0);
        assert_eq!(inspection[0].last_error_code, None);
        next = request(claim.lease.expires_at);
    }
    assert!(inbox
        .inspect_dead_letters("target", 1, &principal(&store).await,)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn explicit_failures_back_off_dead_letter_and_manual_retry_keep_exact_identity() {
    let (temp, store, inbox) = fixture().await;
    let envelope = immediate(&inbox, "poison candidate").await;
    let original = request(Utc::now());
    let first = inbox
        .claim_owned("target", 1, None, &original)
        .await
        .unwrap()
        .remove(0);
    let invalid = SessionInboxFailureReport {
        now: original.now + Duration::seconds(1),
        error_code: "raw error: bearer secret".into(),
    };
    assert!(inbox.fail_owned("target", &first, &invalid).await.is_err());
    let report = SessionInboxFailureReport {
        now: invalid.now,
        error_code: "consumer_rejected".into(),
    };
    let retry_at = match inbox.fail_owned("target", &first, &report).await.unwrap() {
        SessionInboxFailureOutcome::RetryScheduled {
            failure_count,
            retry_after,
        } => {
            assert_eq!(failure_count, 1);
            retry_after
        }
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_eq!(
        retry_at - report.now,
        owned::retry_delay(&envelope.id, first.claim.generation, 1)
    );
    assert!(inbox.ack_owned("target", &first, report.now).await.is_err());
    let before = request(retry_at - Duration::seconds(1));
    assert!(inbox
        .claim_owned("target", 1, None, &before)
        .await
        .unwrap()
        .is_empty());
    let inspection = inbox
        .inspect_owned_leases("target", 1, before.now)
        .await
        .unwrap();
    assert_eq!(inspection[0].failure_count, 1);
    assert_eq!(inspection[0].reclaim_count, 0);
    assert_eq!(inspection[0].manual_retry_count, 0);
    assert_eq!(
        inspection[0].last_error_code.as_deref(),
        Some("consumer_rejected")
    );
    assert_eq!(inspection[0].retry_after, Some(retry_at));

    let reopened = reopen(&temp).await;
    assert_eq!(
        reopened
            .inspect_wake_readiness("target", retry_at - Duration::milliseconds(1))
            .await
            .unwrap()
            .next_due_at,
        Some(retry_at)
    );
    let second_request = request(retry_at);
    let second = reopened
        .claim_owned("target", 1, None, &second_request)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(second.lease.epoch, first.lease.epoch + 1);
    let second_report = SessionInboxFailureReport {
        now: retry_at + Duration::seconds(1),
        error_code: "consumer_rejected".into(),
    };
    let retry_at = match reopened
        .fail_owned("target", &second, &second_report)
        .await
        .unwrap()
    {
        SessionInboxFailureOutcome::RetryScheduled {
            failure_count,
            retry_after,
        } => {
            assert_eq!(failure_count, 2);
            retry_after
        }
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_eq!(
        retry_at - second_report.now,
        owned::retry_delay(&envelope.id, second.claim.generation, 2)
    );
    let third_request = request(retry_at);
    let third = reopened
        .claim_owned("target", 1, None, &third_request)
        .await
        .unwrap()
        .remove(0);
    let third_report = SessionInboxFailureReport {
        now: retry_at + Duration::seconds(1),
        error_code: "consumer_rejected".into(),
    };
    assert_eq!(
        reopened
            .fail_owned("target", &third, &third_report)
            .await
            .unwrap(),
        SessionInboxFailureOutcome::DeadLettered { failure_count: 3 }
    );
    assert!(reopened
        .claim_owned(
            "target",
            1,
            None,
            &request(third_report.now + Duration::hours(1))
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(reopened.inspect("target").await.unwrap().claimed, 0);
    assert_eq!(
        reopened
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap()
            .generation,
        first.claim.generation
    );

    let after_restart = reopen(&temp).await;
    let principal = principal(&store).await;
    let dead = after_restart
        .inspect_dead_letters("target", 1, &principal)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].id, envelope.id);
    assert_eq!(dead[0].generation, first.claim.generation);
    assert_eq!(dead[0].failure_count, 3);
    assert_eq!(dead[0].last_error_code, "consumer_rejected");
    let mut wrong_birth = store.load_session("target").await.unwrap().unwrap();
    wrong_birth.created_at += Duration::seconds(1);
    let stale_principal =
        SessionInboxAdministrationPrincipal::authenticated_host_owner_for(&wrong_birth);
    assert!(after_restart
        .retry_dead_letter(
            "target",
            &envelope.id,
            first.claim.generation,
            third_report.now + Duration::seconds(1),
            &stale_principal,
        )
        .await
        .is_err());
    let mut wrong_target = store.load_session("target").await.unwrap().unwrap();
    wrong_target.id = "other-target".into();
    let wrong_principal =
        SessionInboxAdministrationPrincipal::authenticated_host_owner_for(&wrong_target);
    assert!(after_restart
        .inspect_dead_letters("target", 1, &wrong_principal)
        .await
        .is_err());
    assert!(after_restart
        .retry_dead_letter(
            "target",
            &envelope.id,
            first.claim.generation + 1,
            third_report.now + Duration::seconds(1),
            &principal,
        )
        .await
        .is_err());
    let retry = after_restart
        .retry_dead_letter(
            "target",
            &envelope.id,
            first.claim.generation,
            third_report.now + Duration::seconds(1),
            &principal,
        )
        .await
        .unwrap();
    assert_eq!(retry.id, envelope.id);
    assert_eq!(retry.generation, first.claim.generation);
    let manual_request = request(third_report.now + Duration::seconds(2));
    let manual = after_restart
        .claim_owned("target", 1, None, &manual_request)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(manual.claim.envelope, envelope);
    assert_eq!(manual.claim.envelope.id, envelope.id);
    assert_eq!(manual.claim.generation, first.claim.generation);
    assert_eq!(manual.lease.epoch, third.lease.epoch + 1);
    assert!(after_restart
        .ack_owned("target", &third, manual_request.now)
        .await
        .is_err());
    let inspection = after_restart
        .inspect_owned_leases("target", 1, manual_request.now)
        .await
        .unwrap();
    assert_eq!(inspection[0].failure_count, 0);
    assert_eq!(inspection[0].reclaim_count, 0);
    assert_eq!(inspection[0].manual_retry_count, 1);
    assert_eq!(inspection[0].last_error_code, None);
    after_restart
        .ack_owned("target", &manual, manual_request.now)
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_dead_letter_rotation_stays_inert_and_recovers_on_reopen() {
    let (temp, store, inbox) = fixture().await;
    immediate(&inbox, "rotation recovery").await;
    let mut now = Utc::now();
    for _ in 0..2 {
        let request = request(now);
        let claim = inbox
            .claim_owned("target", 1, None, &request)
            .await
            .unwrap()
            .remove(0);
        let report = SessionInboxFailureReport {
            now: request.now + Duration::seconds(1),
            error_code: "consumer_rejected".into(),
        };
        let outcome = inbox.fail_owned("target", &claim, &report).await.unwrap();
        let SessionInboxFailureOutcome::RetryScheduled { retry_after, .. } = outcome else {
            panic!("expected scheduled retry");
        };
        now = retry_after;
    }
    let third_request = request(now);
    let claim = inbox
        .claim_owned("target", 1, None, &third_request)
        .await
        .unwrap()
        .remove(0);
    let report = SessionInboxFailureReport {
        now: third_request.now + Duration::seconds(1),
        error_code: "consumer_rejected".into(),
    };
    let mut interrupted = reopen(&temp).await;
    interrupted.owned_fs_hook = Some(Arc::new(|event, _| {
        if event == "rotate" {
            Err(std::io::Error::other("injected dead-letter rotate failure"))
        } else {
            Ok(())
        }
    }));
    assert!(interrupted
        .fail_owned("target", &claim, &report)
        .await
        .is_err());
    let recovered = reopen(&temp).await;
    let dead = recovered
        .inspect_dead_letters("target", 1, &principal(&store).await)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].generation, claim.claim.generation);
    assert!(recovered
        .claim_owned("target", 1, None, &request(report.now + Duration::hours(1)))
        .await
        .unwrap()
        .is_empty());
    assert!(recovered
        .ack_owned("target", &claim, report.now)
        .await
        .is_err());
}

#[tokio::test]
async fn two_stores_exclude_live_owner_and_reclaim_exact_message_after_expiry() {
    let (temp, _, inbox) = fixture().await;
    let envelope = immediate(&inbox, "exact input").await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    let other = reopen(&temp).await;
    let mut successor = request(first.now + Duration::seconds(1));
    assert!(other
        .claim_owned("target", 1, None, &successor)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        inbox.claim_owned("target", 1, None, &first).await.unwrap()[0],
        claim
    );
    successor.now = claim.lease.expires_at;
    assert!(inbox
        .ack_owned("target", &claim, successor.now)
        .await
        .is_err());
    let next = other
        .claim_owned("target", 1, None, &successor)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(next.claim.envelope, envelope);
    assert_eq!(next.claim.generation, claim.claim.generation);
    assert_eq!(next.claim.activation_policy, claim.claim.activation_policy);
    assert_eq!(next.lease.epoch, 2);
    assert_ne!(next.claim.claim_id, claim.claim.claim_id);
    assert!(inbox
        .ack_owned("target", &claim, successor.now)
        .await
        .is_err());
    assert!(inbox.renew_owned("target", &claim, &first).await.is_err());
    assert_eq!(
        other
            .claim_owned("target", 1, None, &successor)
            .await
            .unwrap()[0],
        next
    );
    assert!(inbox.claim("target", 1).await.is_err());
    assert!(inbox.ack("target", &next.claim).await.is_err());
    let evidence = inbox
        .inspect_owned_leases("target", 1, successor.now)
        .await
        .unwrap();
    assert_eq!(evidence[0].epoch, 2);
    assert_eq!(evidence[0].reclaim_count, 1);
    assert!(!evidence[0].expired);
    let rendered = format!("{evidence:?}");
    assert!(!rendered.contains(first.consumer.as_str()));
    assert!(!rendered.contains("exact input"));
    assert!(inbox
        .inspect_owned_leases("target", 0, successor.now)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn renewal_holds_operation_barrier_before_competing_expiry_claim() {
    let (temp, _, mut inbox) = fixture().await;
    immediate(&inbox, "renew").await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    let other = reopen(&temp).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    inbox.owned_renew_pause = Some((entered.clone(), release.clone()));
    let mut renewal = first.clone();
    renewal.now += Duration::seconds(5);
    let stale = claim.clone();
    let renew = tokio::spawn(async move { inbox.renew_owned("target", &claim, &renewal).await });
    entered.notified().await;
    renew.abort();
    assert!(renew.await.unwrap_err().is_cancelled());
    let competing = request(first.now + Duration::seconds(10));
    let reclaim =
        tokio::spawn(async move { other.claim_owned("target", 1, None, &competing).await });
    tokio::task::yield_now().await;
    assert!(!reclaim.is_finished());
    release.notify_one();
    assert!(reclaim.await.unwrap().unwrap().is_empty());
    let current = reopen(&temp).await;
    let now = first.now + Duration::seconds(6);
    let mut recovered = first.clone();
    recovered.now = now;
    let renewed = current
        .claim_owned("target", 1, None, &recovered)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(renewed.lease.epoch, 1);
    assert_eq!(renewed.lease.expires_at, first.now + Duration::seconds(15));
    assert!(current.ack_owned("target", &stale, now).await.is_err());
    let mut tampered = renewed.clone();
    tampered.lease.expires_at += Duration::seconds(1);
    assert!(current.ack_owned("target", &tampered, now).await.is_err());
    current.ack_owned("target", &renewed, now).await.unwrap();
    assert!(current.ack_owned("target", &stale, now).await.is_err());
    assert!(current.ack_owned("target", &tampered, now).await.is_err());
    current
        .ack_owned("target", &renewed, now + Duration::hours(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn checkpoint_then_ack_barrier_wins_expiry_and_exact_terminal_retry_survives_reopen() {
    let (temp, store, mut inbox) = fixture().await;
    let envelope = immediate(&inbox, "checkpoint once").await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    // The caller contract remains explicit: checkpoint before invoking ACK.
    let mut session = store.load_session("target").await.unwrap().unwrap();
    session.add_message(envelope.to_provider_message().unwrap());
    session
        .session_inbox_admission_mut()
        .record(envelope.id.clone(), claim.claim.generation);
    store.save_session(&session).await.unwrap();
    let other = reopen(&temp).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    inbox.owned_ack_pause = Some((entered.clone(), release.clone()));
    let ack_claim = claim.clone();
    let ack = tokio::spawn(async move { inbox.ack_owned("target", &ack_claim, first.now).await });
    entered.notified().await;
    ack.abort();
    assert!(ack.await.unwrap_err().is_cancelled());
    let later = request(first.now + Duration::seconds(11));
    let later_now = later.now;
    let reclaim = tokio::spawn(async move { other.claim_owned("target", 1, None, &later).await });
    tokio::task::yield_now().await;
    assert!(!reclaim.is_finished());
    release.notify_one();
    assert!(reclaim.await.unwrap().unwrap().is_empty());
    let reopened = reopen(&temp).await;
    reopened
        .ack_owned("target", &claim, later_now)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None
            )
            .await
            .unwrap()
            .generation,
        claim.claim.generation
    );
    let durable = store.load_session("target").await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| message.id == envelope.id.as_str())
            .count(),
        1
    );
    let mut forged = claim.clone();
    forged.lease.epoch += 1;
    assert!(reopened
        .ack_owned("target", &forged, later_now)
        .await
        .is_err());
}

#[tokio::test]
async fn old_v2_held_ack_cannot_delete_wrapper_before_or_after_path_rotation() {
    let (temp, _, inbox) = fixture().await;
    let envelope = immediate(&inbox, "old held input").await;
    let legacy = inbox.claim("target", 1).await.unwrap().remove(0);
    let dir = inbox.inbox_dir("target").await.unwrap();
    let first = request(Utc::now());
    let mut faulted = reopen(&temp).await;
    faulted.owned_after_write_failure = true;
    assert!(faulted
        .claim_owned("target", 1, None, &first)
        .await
        .is_err());
    // Crash after atomic leased-wrapper write, before its rename: old path
    // still exists, and the actual old v2 decoder cannot parse its new kind.
    let old_path = dir.join("cur").join(&legacy.claim_id);
    let bytes = tokio::fs::read(&old_path).await.unwrap();
    assert!(serde_json::from_slice::<InboxMessage>(&bytes).is_err());
    assert!(old_v2_ack(&inbox, "target", &legacy).await.is_err());
    assert!(tokio::fs::try_exists(&old_path).await.unwrap());
    let current = reopen(&temp).await;
    let claim = current
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claim.lease.epoch, 1);
    assert!(!tokio::fs::try_exists(&old_path).await.unwrap());
    assert!(old_v2_ack(&inbox, "target", &legacy).await.is_err());
    let successor = request(claim.lease.expires_at);
    let next = current
        .claim_owned("target", 1, None, &successor)
        .await
        .unwrap()
        .remove(0);
    assert!(old_v2_ack(&inbox, "target", &legacy).await.is_err());
    assert!(
        tokio::fs::try_exists(dir.join("cur").join(&next.claim.claim_id))
            .await
            .unwrap()
    );
    assert_eq!(
        current
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None
            )
            .await
            .unwrap()
            .generation,
        1
    );
    let bytes = tokio::fs::read(dir.join("cur").join(&next.claim.claim_id))
        .await
        .unwrap();
    assert!(serde_json::from_slice::<InboxMessage>(&bytes).is_err());
    current
        .ack_owned("target", &next, successor.now)
        .await
        .unwrap();
    // Old terminal retry may observe the permanent receipt, but has no path
    // left to delete and cannot create another queue incarnation.
    old_v2_ack(&inbox, "target", &legacy).await.unwrap();
    assert!(current
        .claim_owned("target", 1, None, &successor)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn owned_reclaim_keeps_staged_prefix_and_per_item_policy_in_both_orders() {
    for policies in [
        [
            SessionActivationPolicy::RespectSpecificWait,
            SessionActivationPolicy::InterruptSpecificWait,
        ],
        [
            SessionActivationPolicy::InterruptSpecificWait,
            SessionActivationPolicy::RespectSpecificWait,
        ],
    ] {
        let (temp, _, inbox) = fixture().await;
        let staged = SessionMessageEnvelope::user_input("target", "staged sibling");
        inbox.deliver(&staged).await.unwrap();
        let mut inputs = Vec::new();
        for (index, policy) in policies.into_iter().enumerate() {
            let envelope = SessionMessageEnvelope::user_input("target", format!("input {index}"));
            inbox
                .deliver_with_activation_intent(&envelope, policy, None)
                .await
                .unwrap();
            inputs.push(envelope);
        }
        let first = request(Utc::now());
        let claims = inbox
            .claim_owned("target", 128, None, &first)
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        let other = reopen(&temp).await;
        let successor = request(first.expires_at().unwrap());
        let reclaimed = other
            .claim_owned("target", 128, None, &successor)
            .await
            .unwrap();
        for (index, claim) in reclaimed.iter().enumerate() {
            assert_eq!(claim.claim.envelope, inputs[index]);
            assert_eq!(claim.claim.activation_policy, policies[index]);
            assert_eq!(claim.lease.epoch, 2);
            other
                .ack_owned("target", claim, successor.now)
                .await
                .unwrap();
        }
        assert!(other
            .claim_owned("target", 128, None, &successor)
            .await
            .unwrap()
            .is_empty());
        other
            .mark_activation_eligible("target", 1, SessionActivationPolicy::RespectSpecificWait)
            .await
            .unwrap();
        let released = other
            .claim_owned("target", 1, None, &successor)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(released.claim.envelope, staged);
        assert_eq!(
            released.claim.activation_policy,
            SessionActivationPolicy::RespectSpecificWait
        );
    }
}

#[tokio::test]
async fn format_upgrade_fences_old_claim_and_interrupt_writer_and_preserves_v3() {
    let (_temp, _store, inbox) = fixture().await;
    immediate(&inbox, "format").await;
    let request = request(Utc::now());
    inbox
        .claim_owned("target", 1, None, &request)
        .await
        .unwrap();
    let dir = inbox.inbox_dir("target").await.unwrap();
    let activation_before = tokio::fs::read(dir.join(ACTIVATION_GENERATION_FILE))
        .await
        .unwrap();
    let interrupt_before = tokio::fs::read(dir.join(INTERRUPT_GENERATION_FILE))
        .await
        .unwrap();
    // These are the exact parsing guards in v2 claim and Interrupt mark.
    let old: VersionedActivationWatermark = serde_json::from_slice(&activation_before).unwrap();
    assert_ne!(old.version, 2);
    assert!(std::str::from_utf8(&interrupt_before)
        .unwrap()
        .trim()
        .parse::<u64>()
        .is_err());
    assert!(inbox.claim_for_turn("target", 1, None).await.is_err());
    let staged = SessionMessageEnvelope::user_input("target", "later release");
    let receipt = inbox.deliver(&staged).await.unwrap();
    inbox
        .mark_activation_eligible(
            "target",
            receipt.generation,
            SessionActivationPolicy::InterruptSpecificWait,
        )
        .await
        .unwrap();
    for file in [ACTIVATION_GENERATION_FILE, INTERRUPT_GENERATION_FILE] {
        let value: VersionedActivationWatermark =
            serde_json::from_slice(&tokio::fs::read(dir.join(file)).await.unwrap()).unwrap();
        assert_eq!(value.version, 3);
    }
    assert!(inbox.claim("target", 1).await.is_err());
}

#[tokio::test]
async fn receipt_before_remove_failure_is_terminal_and_never_reclaimed() {
    let (temp, _, mut inbox) = fixture().await;
    let envelope = immediate(&inbox, "terminal crash").await;
    let first = request(Utc::now());
    let claim = inbox
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    inbox.owned_ack_after_receipt_failure = true;
    assert!(inbox.ack_owned("target", &claim, first.now).await.is_err());
    assert!(inbox.was_admitted("target", &envelope.id).await.unwrap());
    let other = reopen(&temp).await;
    let successor = request(claim.lease.expires_at);
    assert!(other
        .claim_owned("target", 1, None, &successor)
        .await
        .unwrap()
        .is_empty());
    other
        .ack_owned("target", &claim, successor.now)
        .await
        .unwrap();
    assert!(other
        .inspect_owned_leases("target", 1, successor.now)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        other
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None
            )
            .await
            .unwrap()
            .generation,
        claim.claim.generation
    );
}

#[tokio::test]
async fn interrupted_upgrade_ignores_old_interrupt_write_and_rejects_missing_snapshot() {
    let (temp, _, inbox) = fixture().await;
    let staged = SessionMessageEnvelope::user_input("target", "not authorized");
    inbox.deliver(&staged).await.unwrap();
    let dir = inbox.inbox_dir("target").await.unwrap();
    let first = request(Utc::now());
    let mut faulted = reopen(&temp).await;
    faulted.owned_after_header_failure = true;
    assert!(faulted
        .claim_owned("target", 1, None, &first)
        .await
        .is_err());
    assert!(old_v2_mark(
        &inbox,
        "target",
        1,
        SessionActivationPolicy::RespectSpecificWait
    )
    .await
    .is_err());
    assert!(old_v2_mark(
        &inbox,
        "target",
        1,
        SessionActivationPolicy::InterruptSpecificWait
    )
    .await
    .is_err());
    // v2 writes INT before reading ACT; the failed operation really leaves
    // its integer behind. ACT3's committed snapshot prevents authority adoption.
    assert_eq!(
        tokio::fs::read_to_string(dir.join(INTERRUPT_GENERATION_FILE))
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(
        FileSessionInbox::read_interrupt_generation(&dir)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        FileSessionInbox::read_activation_generation(&dir)
            .await
            .unwrap(),
        0
    );
    assert!(old_v2_activation(&dir).await.is_err());
    let other = reopen(&temp).await;
    assert!(other
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .is_empty());
    let before = tokio::fs::read(dir.join(INTERRUPT_GENERATION_FILE))
        .await
        .unwrap();
    assert!(old_v2_mark(
        &inbox,
        "target",
        1,
        SessionActivationPolicy::InterruptSpecificWait
    )
    .await
    .is_err());
    assert_eq!(
        tokio::fs::read(dir.join(INTERRUPT_GENERATION_FILE))
            .await
            .unwrap(),
        before
    );
    other
        .mark_activation_eligible("target", 1, SessionActivationPolicy::RespectSpecificWait)
        .await
        .unwrap();
    let claim = other
        .claim_owned("target", 1, None, &first)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        claim.claim.activation_policy,
        SessionActivationPolicy::RespectSpecificWait
    );

    for raw in [
        serde_json::json!({"version":3,"generation":1}),
        serde_json::json!({"version":3,"generation":1,"interrupt_snapshot":"private-sentinel"}),
    ] {
        atomic_write(
            &dir.join(ACTIVATION_GENERATION_FILE),
            &serde_json::to_vec(&raw).unwrap(),
        )
        .await
        .unwrap();
        let error = other
            .claim_owned("target", 1, None, &first)
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("private-sentinel"));
        assert!(other.inspect("target").await.is_err());
    }
}

// Frozen v2 ACK body from 20e6276b. It reads InboxMessage.kind at the held
// physical path, never either version header, then validates envelope/generation
// before receipt+remove. Helper semantic/receipt parsing remains v2-compatible.
async fn old_v2_ack(
    inbox: &FileSessionInbox,
    target_session_id: &str,
    claim: &SessionInboxClaim,
) -> Result<(), SessionInboxError> {
    FileSessionInbox::validate_claim_name(&claim.claim_id)?;
    if claim.envelope.target_session_id != target_session_id {
        return Err(SessionInboxError::InvalidClaim(
            "claim target mismatch".to_string(),
        ));
    }
    let _lifecycle = inbox.lock_lifecycle().await?;
    let dir = inbox.inbox_dir(target_session_id).await?;
    let _guard = inbox.lock_operation(&dir).await?;
    let cur_path = dir.join("cur").join(&claim.claim_id);
    let bytes = match tokio::fs::read(&cur_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            // Idempotent retry only when the exact permanent receipt is
            // already present. A missing claim without that proof is stale.
            return match FileSessionInbox::admitted_receipt(&dir, &claim.envelope).await? {
                Some(receipt) if receipt.delivery.generation == claim.generation => Ok(()),
                _ => Err(SessionInboxError::InvalidClaim(format!(
                    "canonical claim no longer exists: {}",
                    claim.claim_id
                ))),
            };
        }
        Err(error) => {
            return Err(SessionInboxError::Storage(format!(
                "read claimed message {}: {error}",
                cur_path.display()
            )));
        }
    };
    let wrapper: InboxMessage = serde_json::from_slice(&bytes).map_err(|error| {
        SessionInboxError::InvalidClaim(format!(
            "decode canonical claim {}: {error}",
            claim.claim_id
        ))
    })?;
    if wrapper.kind != InboxKind::SessionEnvelope {
        return Err(SessionInboxError::InvalidClaim(format!(
            "canonical claim {} has kind {:?}",
            claim.claim_id, wrapper.kind
        )));
    }
    let intent = FileSessionInbox::activation_intent(&wrapper.body)?;
    let persisted: SessionMessageEnvelope =
        serde_json::from_value(wrapper.body).map_err(|error| {
            SessionInboxError::InvalidClaim(format!(
                "decode canonical envelope {}: {error}",
                claim.claim_id
            ))
        })?;
    let filename_generation = FileSessionInbox::claim_generation(&claim.claim_id)?;
    if filename_generation != claim.generation
        || persisted.id != claim.envelope.id
        || persisted.target_session_id != target_session_id
        || persisted != claim.envelope
    {
        return Err(SessionInboxError::InvalidClaim(format!(
            "canonical claim mismatch for {}",
            claim.claim_id
        )));
    }

    let admitted_path = FileSessionInbox::admitted_path(&dir, &claim.envelope.id);
    if let Some(existing) = FileSessionInbox::admitted_receipt(&dir, &claim.envelope).await? {
        if existing.delivery.generation != claim.generation || existing.intent != intent {
            return Err(SessionInboxError::InvalidClaim(format!(
                "admitted receipt generation mismatch for {}",
                claim.envelope.id
            )));
        }
    }
    let mut receipt = serde_json::json!({
        "id": claim.envelope.id,
        "generation": claim.generation,
        "semantic_digest": FileSessionInbox::semantic_digest(&claim.envelope)?,
        "admitted_at": Utc::now(),
    });
    if let Some(intent) = intent {
        receipt.as_object_mut().expect("receipt object").insert(
            ACTIVATION_INTENT_KEY.into(),
            serde_json::to_value(intent).expect("activation intent serialization"),
        );
    }
    let receipt = serde_json::to_vec_pretty(&receipt)
        .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
    let admitted_dir = admitted_path.parent().ok_or_else(|| {
        SessionInboxError::Storage(format!(
            "admitted receipt has no parent: {}",
            admitted_path.display()
        ))
    })?;
    tokio::fs::create_dir_all(admitted_dir)
        .await
        .map_err(|error| {
            SessionInboxError::Storage(format!(
                "create admitted receipt directory {}: {error}",
                admitted_dir.display()
            ))
        })?;
    atomic_write(&admitted_path, &receipt)
        .await
        .map_err(|error| {
            SessionInboxError::Storage(format!("persist admitted receipt: {error}"))
        })?;

    match tokio::fs::remove_file(&cur_path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SessionInboxError::Storage(format!(
            "remove claimed message {}: {error}",
            cur_path.display()
        ))),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OldV2Header {
    version: u32,
    generation: u64,
}
async fn old_v2_activation(dir: &Path) -> Result<(u64, bool), SessionInboxError> {
    match tokio::fs::read_to_string(dir.join(ACTIVATION_GENERATION_FILE)).await {
        Ok(raw) => {
            if let Ok(generation) = raw.trim().parse::<u64>() {
                return Ok((generation, false));
            }
            let header: OldV2Header = serde_json::from_str(&raw)
                .map_err(|_| SessionInboxError::Storage("invalid v2 header".into()))?;
            if header.version != 2 {
                return Err(SessionInboxError::Storage("unsupported v2 header".into()));
            }
            Ok((header.generation, true))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok((0, false)),
        Err(error) => Err(SessionInboxError::Storage(error.to_string())),
    }
}
async fn old_v2_interrupt(dir: &Path) -> Result<u64, SessionInboxError> {
    match tokio::fs::read_to_string(dir.join(INTERRUPT_GENERATION_FILE)).await {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| SessionInboxError::Storage("invalid v2 interrupt".into())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(error) => Err(SessionInboxError::Storage(error.to_string())),
    }
}
async fn old_v2_write_activation(
    dir: &Path,
    generation: u64,
    versioned: bool,
) -> Result<(), SessionInboxError> {
    let bytes = if versioned {
        serde_json::to_vec(&OldV2Header {
            version: 2,
            generation,
        })
        .unwrap()
    } else {
        generation.to_string().into_bytes()
    };
    atomic_write(&dir.join(ACTIVATION_GENERATION_FILE), &bytes)
        .await
        .map_err(|error| SessionInboxError::Storage(error.to_string()))
}
// Frozen complete v2 mark body from 20e6276b, with the original v2 readers/writer.
async fn old_v2_mark(
    inbox: &FileSessionInbox,
    target_session_id: &str,
    generation: u64,
    policy: SessionActivationPolicy,
) -> Result<(), SessionInboxError> {
    let _lifecycle = inbox.lock_lifecycle().await?;
    let dir = inbox.inbox_dir(target_session_id).await?;
    let _guard = inbox.lock_operation(&dir).await?;
    let delivered_generation = FileSessionInbox::read_generation(&dir).await?;
    if generation == 0 || generation > delivered_generation {
        return Err(SessionInboxError::InvalidClaim(format!(
                "activation generation {generation} is outside delivered range 1..={delivered_generation}"
            )));
    }
    // Publish the interrupt policy before the activation watermark. The
    // two values live in separate atomic files, so a crash may expose
    // `interrupt > activation`; it must never expose a newly eligible
    // explicit steering prefix as RespectSpecificWait.
    if policy == SessionActivationPolicy::InterruptSpecificWait {
        let current_interrupt = old_v2_interrupt(&dir).await?;
        if generation > current_interrupt {
            atomic_write(
                &dir.join(INTERRUPT_GENERATION_FILE),
                generation.to_string().as_bytes(),
            )
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist inbox interrupt generation: {error}"))
            })?;
        }
    }
    let (current, versioned) = old_v2_activation(&dir).await?;
    if generation > current {
        #[cfg(test)]
        if inbox.activation_write_failure {
            return Err(SessionInboxError::Storage(
                "injected activation watermark write failure".into(),
            ));
        }
        old_v2_write_activation(&dir, generation, versioned).await?;
    }
    Ok(())
}
