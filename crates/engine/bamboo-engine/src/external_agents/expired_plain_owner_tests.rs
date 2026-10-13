// Included inside actor_adapter::tests to reuse its existing storage fixtures.
mod expired_plain_owner_tests {
    use super::*;

    // A storage-backed admission matrix, not independent running-Host proof.
    // Controlled past timestamps seed expiry without editing authority files.
    async fn check_admission(case: &str) {
        eprintln!("expired initial owner admission case: {case}");
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap(),
        );
        let parent = Session::new("expired-parent", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("expired-child", &parent, "model", "original");
        child.add_message(bamboo_agent_core::Message::user("original"));
        if case == "history" {
            child.add_message(bamboo_agent_core::Message::assistant("prior reply", None));
        }
        store.save_session(&child).await.unwrap();
        let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
        bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
        let now = chrono::Utc::now();
        let old_now = now - chrono::Duration::minutes(2);
        let old = store
            .claim_activation(&ActorActivationClaim {
                actor_id: child.id.clone(),
                run_id: "old-run".into(),
                lease_owner: "old-owner".into(),
                lease_expires_at: if case == "unexpired" {
                    now + chrono::Duration::minutes(5)
                } else {
                    now - chrono::Duration::minutes(1)
                },
                inbox_generation: u64::from(case == "old-generation"),
                placement_ref: (case != "missing-provenance").then(|| {
                    bamboo_domain::ActorPlacementRef {
                        class: if case == "nonlocal-provenance" {
                            bamboo_domain::ActorPlacementClass::Schedulable
                        } else {
                            bamboo_domain::ActorPlacementClass::Local
                        },
                        lease_id: if case == "malformed-provenance" {
                            "owned-initial-release-v1:../worker".into()
                        } else {
                            "owned-initial-release-v1:old-worker".into()
                        },
                        // A live nonlocal activation needs a real positive
                        // slot epoch even though this Local-only caller refuses it.
                        slot_epoch: (case == "nonlocal-provenance").then_some(1),
                    }
                }),
                now: old_now,
            })
            .await
            .unwrap_or_else(|error| panic!("{case}: seed old activation: {error:?}"));
        store
            .start_activation(&old.fence(), old_now)
            .await
            .unwrap_or_else(|error| panic!("{case}: start old activation: {error:?}"));
        let inbox = bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );
        let mut envelopes = Vec::new();
        let count = match case {
            "queue-empty" => 0,
            "queue-two" => 2,
            _ => 1,
        };
        for index in 0..count {
            let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
                &child.id,
                format!("new input {index}"),
            );
            let receipt = inbox.deliver(&envelope).await.unwrap();
            if case != "not-eligible" {
                inbox
                    .mark_activation_eligible(
                        &child.id,
                        receipt.generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                    .unwrap();
            }
            envelopes.push(envelope);
        }
        if case == "queue-claimed" {
            let claims = inbox
                .claim_owned(
                    &child.id,
                    1,
                    Some("old-run"),
                    &SessionInboxLeaseRequest {
                        consumer: SessionInboxConsumerId::new(),
                        now,
                        duration: chrono::Duration::minutes(5),
                    },
                )
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
        }
        let before = store.load_session(&child.id).await.unwrap().unwrap();
        let before_backlog = inbox.inspect(&child.id).await.unwrap();
        if case == "changed-birth" {
            child.created_at += chrono::Duration::milliseconds(1);
        }
        let second = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap(),
        );
        let binding = actor_binding(
            second.clone(),
            Arc::new(bamboo_storage::FileSessionInbox::new(
                second.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            )),
            Arc::new(bamboo_storage::LockedSessionStore::new(second.clone())),
        );
        let _registration = binding
            .router
            .register_run(&child.id, "replacement-run")
            .await
            .unwrap();
        let prepared = PlainActorActivation::start_and_prepare(
            second.clone(),
            &mut child,
            &binding,
            Some("replacement-run"),
            Some(policy),
            case != "readonly",
            (case != "release-missing").then_some("probed-replacement-worker"),
        )
        .await;
        for envelope in &envelopes {
            assert!(
                !inbox.was_admitted(&before.id, &envelope.id).await.unwrap(),
                "{case}: preparation never grants ACK/release"
            );
        }
        if case != "accepted" {
            assert!(prepared.is_err(), "{case}: replacement must be refused");
            let actual = second.load_session(&before.id).await.unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(&actual.messages).unwrap(),
                serde_json::to_value(&before.messages).unwrap(),
                "{case}: refused admission preserves canonical history"
            );
            let current = second.inspect_actor(&before.id).await.unwrap();
            assert_eq!(current.activation.unwrap().fence(), old.fence(), "{case}");
            let backlog = inbox.inspect(&before.id).await.unwrap();
            assert_eq!(
                (backlog.pending, backlog.claimed),
                (before_backlog.pending, before_backlog.claimed),
                "{case}: refused admission does not consume input"
            );
            return;
        }
        let (replacement, delivery) =
            prepared.expect("expired initial owner accepts one new input");
        assert!(!replacement.recovering_pre_ack);
        let (prefix, delivery) = delivery.expect("replacement checkpoints its new input");
        assert_eq!(prefix.len(), before.messages.len());
        assert_eq!(delivery.envelope, envelopes[0]);
        assert_eq!(delivery.activation_run_id, "replacement-run");
        assert!(replacement.fence.attempt > old.attempt);
        assert!(replacement.fence.lease_epoch > old.lease_epoch);
        assert_ne!(replacement.fence.lease_owner, old.lease_owner);
        assert!(second
            .validate_fence(&old.fence(), chrono::Utc::now())
            .await
            .is_err());
        let actual = second.load_session(&before.id).await.unwrap().unwrap();
        assert_eq!(actual.created_at, before.created_at);
        assert_eq!(actual.parent_session_id, before.parent_session_id);
        assert_eq!(actual.root_session_id, before.root_session_id);
        assert_eq!(actual.messages.len(), before.messages.len() + 1);
        assert_eq!(actual.messages.last().unwrap().id, envelopes[0].id.as_str());
        let backlog = inbox.inspect(&before.id).await.unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 1));
    }

    #[tokio::test]
    async fn expired_initial_owner_accepts_one_new_input_on_independent_store() {
        check_admission("accepted").await;
    }

    #[tokio::test]
    async fn expired_initial_owner_refuses_unsupported_replacement_boundaries() {
        for case in [
            "unexpired",
            "missing-provenance",
            "malformed-provenance",
            "nonlocal-provenance",
            "readonly",
            "release-missing",
            "history",
            "old-generation",
            "queue-empty",
            "queue-two",
            "queue-claimed",
            "not-eligible",
            "changed-birth",
        ] {
            check_admission(case).await;
        }
    }
}
