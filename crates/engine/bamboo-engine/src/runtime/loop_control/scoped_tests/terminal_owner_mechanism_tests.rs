//! GREEN-only proof boundary tests for the new target-owned retirement path.
//! Every ownership is issued by a real router registration; no writer/result
//! is reconstructed from metadata and no control command is marked Applied.
use super::*;

#[tokio::test]
async fn stale_closed_transport_callback_cannot_close_same_run_replacement_instance() {
    let mut h = Fixture::new(true).await;
    let stale = h.policy().owner.ownership().unwrap();
    let binding = h.policy().live_supervisor_binding.get().unwrap().clone();
    let old = h.registration.take().unwrap();
    assert_eq!(old.finish(0).await.unwrap(), None);
    assert!(h.router.current_run_id(TARGET).await.is_none());

    // Reuse the exact string intentionally; only the actual private instance
    // can distinguish the old ownership from this new registered owner.
    let replacement = h
        .router
        .register_session_run(&h.session, OWNER)
        .await
        .unwrap();
    assert!(stale.close_scoped_transport(&binding).await.is_err());
    let mut policy = h.policy().clone();
    policy.bind_owner(replacement.ownership(), &h.session);
    policy
        .bind_execution(EXECUTION, CancellationToken::new())
        .unwrap();
    policy
        .register_supervisor_control(&h.session, &h.storage)
        .await
        .unwrap();
    let observed = h
        .router
        .observe_supervisor_loop(
            &h.supervisor,
            TARGET,
            Some(h.link_revision),
            LoopControlAction::Status,
        )
        .await
        .unwrap();
    assert_eq!(observed.owner_run_id, OWNER);
    assert_eq!(observed.execution_id, EXECUTION);
    h.registration = Some(replacement);
    h.config.loop_control = Some(policy);
    h.close().await;
}

#[tokio::test]
async fn shared_inbox_rejects_second_runtime_grant_without_poisoning_actual_runtime() {
    let mut h = Fixture::new(true).await;
    let other = SessionActivationRouter::new();
    other.set_inbox(h.inbox.clone());
    let registration = other
        .register_session_run(&h.session, "other-host-run")
        .await
        .unwrap();
    let mut policy = h.policy().clone();
    policy.bind_owner(registration.ownership(), &h.session);
    policy
        .bind_execution(EXECUTION, CancellationToken::new())
        .unwrap();
    let refused = policy
        .register_supervisor_control(&h.session, &h.storage)
        .await;
    assert!(
        refused.is_err(),
        "another runtime must not attach a second host scope to the same Inbox"
    );
    assert!(
        other
            .observe_supervisor_loop(
                &h.supervisor,
                TARGET,
                Some(h.link_revision),
                LoopControlAction::Status,
            )
            .await
            .is_err(),
        "a failed bind must not publish a partial grant"
    );

    // The original real runtime still observes its exact installed binding.
    let observed = h
        .router
        .observe_supervisor_loop(
            &h.supervisor,
            TARGET,
            Some(h.link_revision),
            LoopControlAction::Status,
        )
        .await
        .unwrap();
    assert_eq!(observed, *h.policy().live_supervisor_binding.get().unwrap());
    assert!(h.status(None).await.is_ok());
    assert_eq!(registration.finish(0).await.unwrap(), None);
    h.close().await;
}
