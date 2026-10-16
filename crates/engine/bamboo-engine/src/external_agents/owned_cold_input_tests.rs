// Deterministic existing-API coverage for the completed plain Child boundary.
// These fixtures exercise real Store/Inbox admission; no provider is contacted.
async fn completed_plain_inputs_fixture(
    count: usize,
    queue_only: bool,
) -> (
    tempfile::TempDir,
    Arc<bamboo_storage::SessionStoreV2>,
    Arc<bamboo_tools::permission::PermissionConfig>,
    SessionInboxRuntimeBinding,
    Session,
    Vec<bamboo_domain::SessionMessageEnvelope>,
) {
    use bamboo_domain::{SessionMessageDeliveryIntent, SessionMessageKind, SessionMessageSource};
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap(),
    );
    let parent = Session::new("cold-input-parent", "model");
    store.save_session(&parent).await.unwrap();
    let mut child = Session::new_child_of("cold-input-child", &parent, "model", "initial task");
    child.add_message(bamboo_agent_core::Message::assistant(
        "initial result",
        None,
    ));
    child.set_last_run_status("completed");
    store.save_session(&child).await.unwrap();
    store.ensure_actor(&child.id).await.unwrap();
    let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
    bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
    let now = chrono::Utc::now();
    let old = store
        .claim_activation(&ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: "cold-input-old-run".into(),
            lease_owner: "cold-input-old-owner".into(),
            lease_expires_at: now + chrono::Duration::minutes(1),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    store.start_activation(&old.fence(), now).await.unwrap();
    store
        .finish_activation(&old.fence(), now, ActorActivationFinish::Succeeded)
        .await
        .unwrap();
    let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
        store.clone(),
        Default::default(),
    ));
    let source = bamboo_domain::ActorSession::from_session(&parent).unwrap();
    let target = bamboo_domain::ActorSession::from_session(&child).unwrap();
    let mut envelopes = Vec::new();
    for slot in 0..count {
        let mut envelope = bamboo_domain::SessionMessageEnvelope::user_input(
            &child.id,
            format!("cold input {slot}"),
        );
        envelope.source = SessionMessageSource::Session {
            session_id: parent.id.clone(),
        };
        envelope.kind = SessionMessageKind::PeerMessage;
        envelope.thread_id = Some(format!("cold-thread-{slot}"));
        envelope.correlation_id = Some(format!("cold-operation-{slot}"));
        let intent = if queue_only || slot + 1 < count {
            SessionMessageDeliveryIntent::QueueOnly
        } else {
            SessionMessageDeliveryIntent::TriggerTurn
        };
        inbox
            .deliver_peer_message_with_intent(&envelope, &source, &target, None, intent)
            .await
            .unwrap();
        envelopes.push(envelope);
    }
    let binding = actor_binding(
        store.clone(),
        inbox,
        Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
    );
    (temp, store, policy, binding, child, envelopes)
}

#[tokio::test]
async fn completed_plain_queue_then_followup_checkpoints_both_before_dispatch() {
    let (_temp, store, policy, binding, mut child, envelopes) =
        completed_plain_inputs_fixture(2, false).await;
    let _owner = binding
        .router
        .register_run(&child.id, "cold-input-new-run")
        .await
        .unwrap();
    let (activation, prepared) = PlainActorActivation::start_and_prepare(
        store.clone(),
        &mut child,
        &binding,
        Some("cold-input-new-run"),
        Some(policy),
        true,
        Some("cold-input-probed-worker"),
    )
    .await
    .expect("completed zero-tool Child must admit QueueOnly plus followup");
    assert!(prepared.is_some());
    let canonical = store.load_session(&child.id).await.unwrap().unwrap();
    let ids = envelopes
        .iter()
        .map(|envelope| envelope.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        canonical
            .messages
            .iter()
            .filter(|message| ids.contains(&message.id.as_str()))
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>(),
        ids,
        "both real inputs must be checkpointed once and in order"
    );
    for envelope in &envelopes {
        assert!(canonical
            .session_inbox_admission()
            .unwrap()
            .contains(&envelope.id));
        assert!(
            !binding
                .inbox
                .was_admitted(&child.id, &envelope.id)
                .await
                .unwrap(),
            "checkpoint must not fabricate worker confirmation or Host ACK"
        );
    }
    let backlog = binding.inbox.inspect(&child.id).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (0, 2));
    assert_eq!(activation.fence.attempt, 2);
    activation
        .finish(ActorActivationFinish::Failed)
        .await
        .unwrap();
}

#[tokio::test]
async fn completed_plain_batch_keeps_unsupported_backlogs_and_identity_unchanged() {
    for case in [
        "three-inputs",
        "queue-only",
        "no-release",
        "changed-birth",
        "tool-history",
    ] {
        let (_temp, store, policy, binding, mut child, envelopes) = completed_plain_inputs_fixture(
            if case == "three-inputs" { 3 } else { 2 },
            case == "queue-only",
        )
        .await;
        let before = store.load_session(&child.id).await.unwrap().unwrap();
        let _owner = binding
            .router
            .register_run(&child.id, "cold-input-new-run")
            .await
            .unwrap();
        if case == "changed-birth" {
            child.created_at += chrono::Duration::milliseconds(1);
        }
        if case == "tool-history" {
            let mut message = bamboo_agent_core::Message::assistant("tool", None);
            message.role = Role::Tool;
            child.add_message(message);
        }
        assert!(
            PlainActorActivation::start(
                store.clone(),
                &child,
                &binding,
                Some("cold-input-new-run"),
                Some(policy),
                true,
                (case != "no-release").then_some("cold-input-probed-worker"),
            )
            .await
            .is_err(),
            "{case}: no Actor claim or dispatch"
        );
        assert_eq!(
            store
                .inspect_actor(&child.id)
                .await
                .unwrap()
                .actor
                .current_attempt,
            1,
            "{case}"
        );
        let actual = store.load_session(&child.id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&actual.messages).unwrap(),
            serde_json::to_value(&before.messages).unwrap(),
            "{case}"
        );
        let backlog = binding.inbox.inspect(&child.id).await.unwrap();
        assert_eq!(
            (backlog.pending, backlog.claimed),
            (envelopes.len(), 0),
            "{case}"
        );
        for envelope in envelopes {
            assert!(
                !binding
                    .inbox
                    .was_admitted(&child.id, &envelope.id)
                    .await
                    .unwrap(),
                "{case}"
            );
        }
    }
}
