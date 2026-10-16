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
    for count in [2, 3, 5] {
        let (_temp, store, policy, binding, mut child, envelopes) =
            completed_plain_inputs_fixture(count, false).await;
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
            "every selected input must be checkpointed once and in order"
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
        assert_eq!((backlog.pending, backlog.claimed), (0, count));
        assert_eq!(activation.fence.attempt, 2);
        activation
            .finish(ActorActivationFinish::Failed)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn completed_plain_batch_keeps_unsupported_backlogs_and_identity_unchanged() {
    for case in [
        "over-batch-limit",
        "queue-only",
        "no-release",
        "changed-birth",
        "tool-history",
    ] {
        let (_temp, store, policy, binding, mut child, envelopes) = completed_plain_inputs_fixture(
            if case == "over-batch-limit" {
                bamboo_domain::SessionInboxLimits::default().max_claim_batch + 1
            } else {
                3
            },
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

#[tokio::test]
async fn completed_plain_batch_release_preserves_order_exact_retry_and_partial_ack() {
    use bamboo_subagent::proto::{InitialInputControl, InitialInputReleaseRequest};
    for count in [2, 4] {
        for case in [
            "accepted",
            "early-terminal",
            "wrong-order",
            "changed-retry-nonce",
        ] {
            let (_temp, store, policy, binding, mut child, envelopes) =
                completed_plain_inputs_fixture(count, false).await;
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
                Some(policy.clone()),
                true,
                Some("cold-input-probed-worker"),
            )
            .await
            .unwrap();
            let (prefix, deliveries) = prepared.unwrap();
            assert_eq!(deliveries.len(), count);
            let revision = policy.policy_revision();
            let run = RunSpec {
                assignment: "initial task".into(),
                logical_session: Some(LogicalSessionIdentity {
                    session_id: child.id.clone(),
                    parent_session_id: child.parent_session_id.clone(),
                    root_session_id: child.root_session_id.clone(),
                    creation: Some(bamboo_subagent::proto::ChildCreationIdentity {
                        created_at: child.created_at,
                        spawn_depth: child.spawn_depth,
                    }),
                }),
                project_id: None,
                reasoning_effort: None,
                permission_policy: Some(bamboo_subagent::proto::PermissionPolicyContext {
                    revision,
                    requested_mode: "default".into(),
                    effective_mode: "default".into(),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    session_id: child.id.clone(),
                    workspace_path: child.workspace.clone(),
                    environment_lease: None,
                    inherit_session_grants: false,
                    policy: serde_json::to_value(policy.to_serializable()).unwrap(),
                }),
                messages: prefix,
                activation_run_id: Some("cold-input-new-run".into()),
                execution_epoch: 7,
                initial_session_messages: deliveries,
                secrets: Default::default(),
            };
            let requests = run
                .initial_session_messages
                .iter()
                .map(|delivery| {
                    InitialInputReleaseRequest::from_run(
                        &run,
                        delivery,
                        uuid::Uuid::new_v4().to_string(),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let first = requests[0].clone();
            let second = requests[1].clone();
            let control = |request| ChildFrame::Event {
                event: serde_json::to_value(InitialInputControl::Request { request }).unwrap(),
            };
            let mut frames = VecDeque::from([permission_posture_frame(&child.id, revision)]);
            if case == "wrong-order" {
                frames.push_back(control(second));
            } else {
                frames.push_back(control(first.clone()));
                if case == "accepted" {
                    frames.push_back(control(first));
                    frames.extend(requests[1..].iter().cloned().map(control));
                } else if case == "changed-retry-nonce" {
                    let mut wrong = first;
                    wrong.nonce = uuid::Uuid::new_v4().to_string();
                    frames.push_back(control(wrong));
                }
            }
            frames.push_back(ChildFrame::Terminal {
                status: TerminalStatus::Completed,
                result: Some("batch result".into()),
                error: None,
                final_event_watermark: None,
                transcript: Vec::new(),
            });
            let mut link = ConfirmationSequenceLink {
                frames,
                sent: Vec::new(),
            };
            let (events, _events_rx) = mpsc::channel(16);
            let cancel = CancellationToken::new();
            let (_live_tx, mut live_rx) = mpsc::unbounded_channel();
            let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
            let epochs = AtomicU64::new(7);
            let result = drive(ActorDriveContext {
                client: &mut link,
                parent_session_id: "cold-input-parent",
                child_session_id: "cold-input-child",
                child_attempt: 1,
                approval_registry: None,
                approval_decider: None,
                approval_reviewer: None,
                escalation_bridge: None,
                event_tx: &events,
                cancel_token: &cancel,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: &mut child,
                expected_permission_posture: Some(expected_default_permission_posture(revision)),
                expected_creation: None,
                session_inbox_runtime: Some(&binding),
                actor_directory_store: None,
                canonical_subagent_tool: None,
                ticket_service: None,
                activation_run_id: Some("cold-input-new-run"),
                execution_epoch: 7,
                expected_source_actor_id: "cold-input-child",
                initial_inflight_claims: VecDeque::new(),
                first_frame_timeout: Some(Duration::from_secs(1)),
                plain_actor: true,
                remote_environment_lease: false,
                readonly_output: None,
                local_history_tools: None,
                local_history_read_only: false,
                strict_history_output: None,
                plain_input: Some(&activation),
                canonical_activation: None,
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: Some((&run, &epochs)),
            })
            .await;
            assert_eq!(result.is_ok(), case == "accepted", "{case}: {result:?}");
            let releases = link
                .sent
                .iter()
                .filter_map(|frame| match frame {
                    ParentFrame::InitialInputRelease { release } => Some(release),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if case == "accepted" {
                assert_eq!(releases.len(), count + 1);
                assert_eq!(
                    releases[0], releases[1],
                    "exact retry preserves nonce and deadline"
                );
                assert_eq!(
                    releases.last().unwrap().request.envelope_id,
                    envelopes.last().unwrap().id.as_str()
                );
            }
            assert_eq!(
                binding
                    .inbox
                    .was_admitted(&child.id, &envelopes[0].id)
                    .await
                    .unwrap(),
                case != "wrong-order",
                "{case}"
            );
            assert_eq!(
                binding
                    .inbox
                    .was_admitted(&child.id, &envelopes[1].id)
                    .await
                    .unwrap(),
                case == "accepted",
                "{case}"
            );
            assert!(
                !child
                    .messages
                    .iter()
                    .any(|message| message.content == "batch result"),
                "worker terminal is not a Host transcript append"
            );
            activation
                .finish(ActorActivationFinish::Failed)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn completed_plain_batch_rejects_unsupported_member_before_any_checkpoint() {
    use bamboo_domain::{SessionMessageDeliveryIntent, SessionMessageKind, SessionMessageSource};
    let (_temp, store, policy, binding, mut child, _) =
        completed_plain_inputs_fixture(0, false).await;
    let parent = store
        .load_session("cold-input-parent")
        .await
        .unwrap()
        .unwrap();
    let source = bamboo_domain::ActorSession::from_session(&parent).unwrap();
    let target = bamboo_domain::ActorSession::from_session(&child).unwrap();
    let before = serde_json::to_value(&child.messages).unwrap();
    let mut inputs = Vec::new();
    for (slot, text) in [
        "valid input".to_string(),
        "x".repeat(8193),
        "wake after bad member".into(),
    ]
    .into_iter()
    .enumerate()
    {
        let mut envelope = bamboo_domain::SessionMessageEnvelope::user_input(&child.id, text);
        envelope.kind = SessionMessageKind::PeerMessage;
        envelope.source = SessionMessageSource::Session {
            session_id: parent.id.clone(),
        };
        envelope.thread_id = Some(format!("unsupported-batch-thread-{slot}"));
        envelope.correlation_id = Some(format!("unsupported-batch-operation-{slot}"));
        binding
            .inbox
            .deliver_peer_message_with_intent(
                &envelope,
                &source,
                &target,
                None,
                if slot < 2 {
                    SessionMessageDeliveryIntent::QueueOnly
                } else {
                    SessionMessageDeliveryIntent::TriggerTurn
                },
            )
            .await
            .unwrap();
        inputs.push(envelope);
    }
    let _owner = binding
        .router
        .register_run(&child.id, "cold-input-new-run")
        .await
        .unwrap();
    assert!(PlainActorActivation::start_and_prepare(
        store.clone(),
        &mut child,
        &binding,
        Some("cold-input-new-run"),
        Some(policy.clone()),
        true,
        Some("cold-input-probed-worker"),
    )
    .await
    .is_err());
    let canonical = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(&canonical.messages).unwrap(), before);
    for envelope in inputs {
        assert!(!binding
            .inbox
            .was_admitted(&child.id, &envelope.id)
            .await
            .unwrap());
    }
    let entry = store.inspect_actor(&child.id).await.unwrap();
    assert_eq!(entry.actor.current_attempt, 2);
    assert_eq!(
        entry.activation.unwrap().status,
        bamboo_domain::ActorActivationStatus::Failed
    );

    let backlog = binding.inbox.inspect(&child.id).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (1, 2));
    drop(_owner);
    let reopened = Arc::new(
        bamboo_storage::SessionStoreV2::new(_temp.path().into())
            .await
            .unwrap(),
    );
    let reopened_inbox: Arc<dyn SessionInboxPort> = Arc::new(
        bamboo_storage::FileSessionInbox::new(reopened.clone(), Default::default()),
    );
    let retry_binding = actor_binding(
        reopened.clone(),
        reopened_inbox,
        Arc::new(bamboo_storage::LockedSessionStore::new(reopened.clone())),
    );
    let _retry_owner = retry_binding
        .router
        .register_run(&child.id, "cold-input-retry-run")
        .await
        .unwrap();
    let mut retry = reopened.load_session(&child.id).await.unwrap().unwrap();
    assert!(PlainActorActivation::start_and_prepare(
        reopened.clone(),
        &mut retry,
        &retry_binding,
        Some("cold-input-retry-run"),
        Some(policy),
        true,
        Some("cold-input-retry-worker")
    )
    .await
    .is_err());
    assert_eq!(
        reopened
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .actor
            .current_attempt,
        2,
        "reopen cannot invent multi-claim recovery"
    );
    assert_eq!(
        serde_json::to_value(
            &reopened
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap()
                .messages
        )
        .unwrap(),
        before
    );
    let after_retry = retry_binding.inbox.inspect(&child.id).await.unwrap();
    assert_eq!((after_retry.pending, after_retry.claimed), (1, 2));
}
