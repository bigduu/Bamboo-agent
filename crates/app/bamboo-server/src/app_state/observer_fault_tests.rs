//! Administrative fault reporting uses the real parent Maildir and native Host.
use super::*;
use bamboo_domain::{ObserverSubscriptionFailure as Failure, SessionMessageEnvelope};

const FAULT_TEXT: &str = "Observer subscription exhausted its bounded retries";
const USER_TEXT: &str = "ORIGINAL_USER_INPUT_MUST_SURVIVE_FAULT";

async fn exhaust(
    f: &Fixture,
    code: Failure,
    now: DateTime<Utc>,
) -> bamboo_domain::ObserverTerminalFault {
    for n in 0..8 {
        f.state
            .session_store
            .record_observer_subscription_failure(
                &f.reference,
                OBSERVER,
                now + chrono::Duration::seconds(n),
                code,
            )
            .await
            .unwrap();
    }
    f.subscription().await.terminal_fault.unwrap()
}

async fn fault(f: &Fixture) -> bamboo_domain::ObserverTerminalFault {
    f.subscription().await.terminal_fault.unwrap()
}

#[tokio::test]
async fn fault_cap_from_actual_publication_failure_notifies_idle_parent_once() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let legacy = f.subscription().await;
    #[derive(serde::Serialize)]
    struct Schema3Subscription<'a> {
        filter: &'a BTreeSet<ObserverChangeKind>,
        min_interval_ms: u32,
        window: &'a bamboo_domain::ObserverSubscriptionWindow,
        failures: u8,
        retry_at: Option<DateTime<Utc>>,
        last_error: Option<Failure>,
    }
    let original = Schema3Subscription {
        filter: &legacy.filter,
        min_interval_ms: legacy.min_interval_ms,
        window: &legacy.window,
        failures: legacy.failures,
        retry_at: legacy.retry_at,
        last_error: legacy.last_error,
    };
    assert_eq!(
        serde_json::to_vec(&legacy).unwrap(),
        serde_json::to_vec(&original).unwrap(),
        "the original schema3 byte encoder stays unchanged when the field is absent"
    );
    assert!(serde_json::to_value(&legacy)
        .unwrap()
        .get("terminal_fault")
        .is_none());
    let view = f.publish(ROOT_A).await;
    let manifest = std::path::PathBuf::from(view["manifest_path"].as_str().unwrap());
    let directory = manifest.parent().unwrap().parent().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    std::fs::write(directory, b"publication blocked").unwrap();
    let now = Utc::now();
    for n in 0..8 {
        f.state
            .observer_subscriptions
            .tick(now + chrono::Duration::minutes(n * 6))
            .await;
    }
    let original = fault(&f).await;
    assert_eq!(original.payload.code, Failure::PublicationUnavailable);
    assert_eq!(original.payload.failures, 8);
    assert!(!original.reported);
    let mut old_writer = load(&f.state, &f.reference.session_id)
        .await
        .supervisor_management
        .unwrap();
    assert_eq!(old_writer.schema_version, 4);
    old_writer.schema_version = 3;
    assert!(old_writer.validate(f.reference.incarnation_id).is_err());
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::hours(1))
        .await;
    settled(&f.state, &f.reference.session_id).await;
    assert!(fault(&f).await.reported);
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    for n in 1..4 {
        f.state
            .observer_subscriptions
            .tick(now + chrono::Duration::hours(n + 1))
            .await;
    }
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    let parent = load(&f.state, &f.reference.session_id).await;
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(FAULT_TEXT))
            .count(),
        1
    );
    assert_eq!(fault(&f).await.event_id, original.event_id);
    f.state.shutdown().await;
}

#[tokio::test]
async fn fault_preserves_live_work_wait_and_original_user_input() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let work = f.child("fault-held-work", false, WORK).await;
    let parent = load(&f.state, &f.reference.session_id).await;
    let creator = f.state.supervisor_observer_creator();
    creator
        .register_parent_wait_for_child(&parent.id, &work.id, Some("fault-original-work-wait"))
        .await
        .unwrap();
    creator.enqueue_child_run(&parent, &work).await.unwrap();
    tokio::time::timeout(WAIT, f.probe.work_entered.notified())
        .await
        .unwrap();
    let waiting = load(&f.state, &parent.id)
        .await
        .agent_runtime_state
        .unwrap()
        .waiting_for_children;
    f.state
        .session_messenger
        .admit(SessionMessageEnvelope::user_input(&parent.id, USER_TEXT))
        .await
        .unwrap();
    let now = Utc::now();
    exhaust(&f, Failure::InboxUnavailable, now).await;
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::minutes(1))
        .await;
    assert!(fault(&f).await.reported);
    assert_eq!(
        load(&f.state, &parent.id)
            .await
            .agent_runtime_state
            .unwrap()
            .waiting_for_children,
        waiting
    );
    assert!(f
        .state
        .session_activation_router
        .current_run_id(&work.id)
        .await
        .is_some());
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    f.probe.work_release.notify_one();
    settled(&f.state, &work.id).await;
    let parent = settled(&f.state, &parent.id).await;
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(USER_TEXT))
            .count(),
        1
    );
    assert_eq!(
        parent
            .messages
            .iter()
            .filter(|message| message.content.contains(FAULT_TEXT))
            .count(),
        1
    );
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    f.state.shutdown().await;
}

#[tokio::test]
async fn fault_parent_inbox_before_checkpoint_reopens_and_never_replays_provider() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let now = Utc::now();
    let original = exhaust(&f, Failure::PublicationUnavailable, now).await;
    let inbox = &f.state.observer_subscriptions.inbox;
    inbox.interrupt_next_observer_fault_checkpoint_for_testing();
    let due = now + chrono::Duration::minutes(1);
    assert!(inbox
        .deliver_supervisor_observer_fault(&f.reference, OBSERVER, due)
        .await
        .is_err());
    let physical = inbox.backlog(&f.reference.session_id).await.unwrap();
    assert_eq!(physical.generation, 1);
    assert!(!fault(&f).await.reported);
    // This is an actual persisted Maildir transport produced by the adapter.
    let entry = std::fs::read_dir(
        f.home
            .path()
            .join("sessions")
            .join(&f.reference.session_id)
            .join("inbox/new"),
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let wrapper: Value = serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
    let envelope: bamboo_domain::SessionMessageEnvelope =
        serde_json::from_value(wrapper["body"].clone()).unwrap();
    assert_eq!(
        envelope.kind,
        bamboo_domain::SessionMessageKind::RuntimeInstruction
    );
    assert_eq!(envelope.id, original.event_id);
    let mut forged = envelope.clone();
    forged.id = bamboo_domain::SessionMessageId::new();
    forged.source = bamboo_domain::SessionMessageSource::Runtime {
        subsystem: "untrusted-runtime-label".into(),
    };
    assert!(
        inbox.deliver(&forged).await.is_err(),
        "a typed diagnostic cannot use generic admission even under another runtime label"
    );
    assert_eq!(
        inbox.backlog(&f.reference.session_id).await.unwrap(),
        physical
    );
    let bamboo_domain::SessionMessageBody::RuntimeInstruction(body) = envelope.body else {
        panic!("typed diagnostic");
    };
    assert_eq!(body.instruction, "observer_subscription_fault_v1");
    assert_eq!(
        body.data,
        Some(serde_json::to_value(&original.payload).unwrap())
    );
    f.state
        .session_store
        .record_observer_subscription_failure(
            &f.reference,
            OBSERVER,
            due,
            Failure::AuthorityUnavailable,
        )
        .await
        .unwrap();
    assert_eq!(fault(&f).await.payload, original.payload);
    f.state.shutdown().await;
    let store = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
    let inbox = FileSessionInbox::new(store, bamboo_domain::SessionInboxLimits::default());
    let receipt = inbox
        .deliver_supervisor_observer_fault(&f.reference, OBSERVER, due)
        .await
        .unwrap();
    assert_eq!(receipt.delivery.id, original.event_id);
    assert_eq!(
        inbox.backlog(&f.reference.session_id).await.unwrap(),
        physical
    );
    let recovered = host(f.home.path(), f.probe.clone()).await;
    crate::app_state::wake_reconciler::reconcile_startup(
        recovered.session_store.clone(),
        recovered.session_activation_router.clone(),
        recovered.mcp_proxy_shutdown.clone(),
    )
    .await;
    settled(&recovered, &f.reference.session_id).await;
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    recovered.shutdown().await;
    let again = host(f.home.path(), f.probe.clone()).await;
    crate::app_state::wake_reconciler::reconcile_startup(
        again.session_store.clone(),
        again.session_activation_router.clone(),
        again.mcp_proxy_shutdown.clone(),
    )
    .await;
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    again.shutdown().await;
}

#[tokio::test]
async fn fault_missing_source_can_report_but_revoked_registration_cannot() {
    let f = Fixture::new().await;
    f.register(all()).await;
    std::fs::rename(
        f.home.path().join("sessions").join(ROOT_A),
        f.home.path().join("unavailable-source"),
    )
    .unwrap();
    let child = load(&f.state, OBSERVER).await;
    assert!(f
        .state
        .storage
        .load_supervisor_observer_context_target(&SessionCallerReference::from(&child), ROOT_A)
        .await
        .is_err());
    let now = Utc::now();
    let original = exhaust(&f, Failure::AuthorityUnavailable, now).await;
    f.state
        .observer_subscriptions
        .tick(now + chrono::Duration::minutes(1))
        .await;
    let parent = settled(&f.state, &f.reference.session_id).await;
    let report = parent
        .messages
        .iter()
        .find(|message| message.content.contains(FAULT_TEXT))
        .unwrap();
    assert!(!report.content.contains(PRIVATE));
    assert!(!report.content.contains(ROOT_A));
    assert!(!report.content.contains("view_revision"));
    assert!(fault(&f).await.reported);
    assert_eq!(fault(&f).await.payload, original.payload);
    f.state.shutdown().await;
    let f = Fixture::new().await;
    f.register(all()).await;
    exhaust(&f, Failure::AuthorityUnavailable, now).await;
    f.state
        .session_store
        .revoke_supervisor_observer(&f.reference, f.revision().await, OBSERVER)
        .await
        .unwrap();
    let before = f
        .state
        .session_inbox
        .backlog(&f.reference.session_id)
        .await
        .unwrap();
    assert!(f
        .state
        .observer_subscriptions
        .inbox
        .deliver_supervisor_observer_fault(
            &f.reference,
            OBSERVER,
            now + chrono::Duration::minutes(1)
        )
        .await
        .is_err());
    assert_eq!(
        f.state
            .session_inbox
            .backlog(&f.reference.session_id)
            .await
            .unwrap(),
        before
    );
    assert!(!fault(&f).await.reported);
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
}

#[tokio::test]
async fn fault_delivery_failure_is_bounded_durable_and_preserves_original_payload() {
    let f = Fixture::new().await;
    f.register(all()).await;
    let now = Utc::now();
    let original = exhaust(&f, Failure::PublicationUnavailable, now).await;
    let bounded = Arc::new(FileSessionInbox::new(
        f.state.session_store.clone(),
        bamboo_domain::SessionInboxLimits {
            max_backlog: 0,
            ..Default::default()
        },
    ));
    let coordinator = ObserverSubscriptionCoordinator::new(
        f.state.session_store.clone(),
        bounded.clone(),
        f.state.session_activation_router.clone(),
        f.state.storage.clone(),
        true,
    );
    for n in 0..8 {
        coordinator
            .tick(now + chrono::Duration::minutes(n * 6 + 1))
            .await;
    }
    let blocked = fault(&f).await;
    assert!(!blocked.reported);
    assert_eq!(blocked.delivery_failures, 8);
    assert!(!blocked.ready(now + chrono::Duration::days(1)));
    assert_eq!(blocked.payload, original.payload);
    assert_eq!(blocked.event_id, original.event_id);
    let before = f
        .state
        .session_inbox
        .backlog(&f.reference.session_id)
        .await
        .unwrap();
    assert_eq!(before.generation, 0);
    coordinator.tick(now + chrono::Duration::days(1)).await;
    assert_eq!(fault(&f).await, blocked);
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
    let store = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
    let inbox = FileSessionInbox::new(store.clone(), Default::default());
    assert!(inbox
        .deliver_supervisor_observer_fault(&f.reference, OBSERVER, now + chrono::Duration::days(1))
        .await
        .is_err());
    let parent = store
        .load_root_authority(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        parent.supervisor_management.unwrap().subscriptions[OBSERVER]
            .terminal_fault
            .as_ref(),
        Some(&blocked)
    );
    assert_eq!(
        inbox.backlog(&f.reference.session_id).await.unwrap(),
        before
    );
}
