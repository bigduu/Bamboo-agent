// The real worker/runtime and physical Host receipts; the provider is recorded.
#[tokio::test]
async fn initial_release_two_inputs_waits_for_every_owned_host_receipt() {
    use bamboo_domain::{
        ActorActivationClaim, ActorDirectoryPort, SessionInboxConsumerId, SessionInboxLeaseRequest,
        SessionInboxLimits, SessionInboxPort,
    };
    use bamboo_subagent::executor::SteerMessage;
    use bamboo_subagent::proto::{InitialInputRelease, InitialInputReleaseRequest};
    for case in ["accepted", "wrong-second-nonce", "cancel-before-second"] {
        let host_temp = tempfile::tempdir().unwrap();
        let host = Arc::new(SessionStoreV2::new(host_temp.path().into()).await.unwrap());
        let parent = Session::new("batch-release-parent", "model");
        host.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("batch-release-child", &parent, "model", "base task");
        child.add_message(Message::assistant("prior result", None));
        host.save_session(&child).await.unwrap();
        host.ensure_actor(&child.id).await.unwrap();
        let inbox =
            bamboo_storage::FileSessionInbox::new(host.clone(), SessionInboxLimits::default());
        let mut envelopes = Vec::new();
        for text in ["BATCH_FIRST", "BATCH_SECOND"] {
            let envelope = SessionMessageEnvelope::user_input(&child.id, text);
            let receipt = inbox.deliver(&envelope).await.unwrap();
            inbox
                .mark_activation_eligible(
                    &child.id,
                    receipt.generation,
                    bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
                )
                .await
                .unwrap();
            envelopes.push(envelope);
        }
        let now = chrono::Utc::now();
        let deadline = now + chrono::Duration::seconds(30);
        let consumer = SessionInboxConsumerId::new();
        let claims = inbox
            .claim_owned(
                &child.id,
                2,
                Some("batch-release-run"),
                &SessionInboxLeaseRequest {
                    consumer: consumer.clone(),
                    now,
                    duration: deadline - now,
                },
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        let actor = host
            .claim_activation(&ActorActivationClaim {
                actor_id: child.id.clone(),
                run_id: "batch-release-run".into(),
                lease_owner: consumer.as_str().into(),
                lease_expires_at: deadline,
                inbox_generation: claims[1].claim.generation,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        host.start_activation(&actor.fence(), now).await.unwrap();
        let prefix = child
            .messages
            .iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect();
        for claim in &claims {
            child = inbox
                .checkpoint_actor_input(bamboo_storage::ActorInputCheckpoint {
                    fence: actor.fence(),
                    expected_created_at: child.created_at,
                    claim: claim.clone(),
                    expected_messages: child.messages.clone(),
                    expected_provider_transcript: child.provider_transcript.clone(),
                    expected_admission: child.session_inbox_admission().cloned(),
                })
                .await
                .unwrap()
                .session;
        }
        let deliveries = claims
            .iter()
            .map(|claim| SessionMessageDelivery {
                target_session_id: child.id.clone(),
                envelope: claim.claim.envelope.clone(),
                canonical_claim_generation: claim.claim.generation,
                activation_run_id: "batch-release-run".into(),
                activation_policy: claim.claim.activation_policy,
            })
            .collect();
        let mut run = protocol_run(&child.id, "batch-release-run", deliveries);
        run.logical_session = Some(LogicalSessionIdentity {
            session_id: child.id.clone(),
            parent_session_id: child.parent_session_id.clone(),
            root_session_id: child.root_session_id.clone(),
            creation: Some(bamboo_subagent::proto::ChildCreationIdentity {
                created_at: child.created_at,
                spawn_depth: child.spawn_depth,
            }),
        });
        run.messages = prefix;
        let provider = Arc::new(RecordingWorkerProvider::default());
        let (_worker_temp, mut executor, _worker_store, _worker_inbox) =
            worker_protocol_fixture(provider.clone()).await;
        executor.initial_input_release_required = true;
        executor.child_creation_identity = true;
        let (events, _events_rx, mut controls) = EventSink::channel_with_control();
        let (steer_tx, steer) = SteerInbox::channel();
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let worker_run = run.clone();
        let worker = tokio::spawn(async move {
            ChildExecutor::run(&executor, worker_run, events, steer, worker_cancel).await
        });
        for slot in 0..2 {
            let control = tokio::time::timeout(std::time::Duration::from_secs(5), controls.recv())
                .await
                .unwrap()
                .expect("worker must request each initial release before provider entry");
            let ExecutorControl::InitialInputReleaseRequest(request) = control else {
                panic!("unexpected control")
            };
            assert_eq!(
                request,
                InitialInputReleaseRequest::from_run(
                    &run,
                    &run.initial_session_messages[slot],
                    request.nonce.clone()
                )
                .unwrap()
            );
            assert!(
                provider.calls.lock().unwrap().is_empty(),
                "{case}: all releases precede SDK/provider admission"
            );
            if slot == 1 && case == "cancel-before-second" {
                cancel.cancel();
                break;
            }
            let mut release = InitialInputRelease {
                request,
                expires_at: deadline.min(claims[slot].lease.expires_at),
            };
            if slot == 1 && case == "wrong-second-nonce" {
                release.request.nonce = uuid::Uuid::new_v4().to_string();
            } else {
                host.validate_fence(&actor.fence(), chrono::Utc::now())
                    .await
                    .unwrap();
                inbox
                    .ack_owned(&child.id, &claims[slot], chrono::Utc::now())
                    .await
                    .unwrap();
            }
            steer_tx
                .send(SteerMessage::InitialInputRelease(release))
                .unwrap();
        }
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        {
            let calls = provider.calls.lock().unwrap();
            if case == "accepted" {
                assert_eq!(outcome.status, bamboo_subagent::TerminalStatus::Completed);
                assert_eq!(calls.len(), 1);
                let actual = calls[0]
                    .iter()
                    .filter(|message| {
                        message.content == "BATCH_FIRST" || message.content == "BATCH_SECOND"
                    })
                    .map(|message| message.content.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(actual, ["BATCH_FIRST", "BATCH_SECOND"]);
            } else {
                assert_ne!(outcome.status, bamboo_subagent::TerminalStatus::Completed);
                assert!(
                    calls.is_empty(),
                    "{case}: partial release cannot enter provider"
                );
            }
        }
        assert!(inbox
            .was_admitted(&child.id, &envelopes[0].id)
            .await
            .unwrap());
        assert_eq!(
            inbox
                .was_admitted(&child.id, &envelopes[1].id)
                .await
                .unwrap(),
            case == "accepted"
        );
    }
}
