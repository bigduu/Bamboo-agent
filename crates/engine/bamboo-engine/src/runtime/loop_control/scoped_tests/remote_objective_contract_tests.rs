//! Direct Messenger acceptance uses the same remote contract as the model tool.
use super::*;

#[tokio::test]
async fn direct_remote_objective_over_scalar_limit_rejects_before_inbox() {
    let mut h = Fixture::new(true).await;
    let before = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!(
        (before.generation, before.pending, before.claimed),
        (0, 0, 0)
    );
    for objective in [
        "x".repeat(4097),
        "界".repeat(5000),
        format!("{}abcd", "aé界😀".repeat(1638)),
    ] {
        let command = start(&objective, 3, 12);
        command.validate().unwrap(); // Still legal on the standalone self path.
        let error = h
            .send(&command)
            .await
            .expect_err("remote scalar limit must precede delivery");
        assert!(
            matches!(&error, SessionMessengerError::InvalidEnvelope(_)),
            "{error:?}"
        );
        let after = h.inbox.inspect(TARGET).await.unwrap();
        assert_eq!(
            (after.generation, after.pending, after.claimed),
            (before.generation, before.pending, before.claimed)
        );
        let durable = h.durable().await;
        assert!(counted_loop::read_loop_state(&durable).unwrap().is_none());
        assert!(scoped_loop_consumptions(&durable).unwrap().is_empty());
        assert_eq!(
            serde_json::to_value(&durable.messages).unwrap(),
            h.original_messages
        );
        h.assert_no_activation();
    }
    h.close().await;
}

#[tokio::test]
async fn direct_remote_objective_scalar_boundary_keeps_canonical_text_and_real_admission_proof() {
    for objective in [
        "x".repeat(4096),
        "界".repeat(4096),
        "😀".repeat(4096),
        "aé界😀".repeat(1024),
        format!("{}\u{feff}", "\u{0085}".repeat(4095)),
    ] {
        let mut h = Fixture::new(true).await;
        let command = start(&objective, 3, 12);
        command.validate().unwrap();
        let queued = h.send(&command).await.unwrap();
        assert_eq!(queued["admitted"], true);
        assert_eq!(queued["configured"], false);
        h.drain(0).await.unwrap();
        let configured = h.durable().await;
        assert_eq!(
            counted_loop::read_loop_state(&configured)
                .unwrap()
                .unwrap()
                .objective,
            objective
        );
        assert_eq!(
            serde_json::to_value(&configured.messages).unwrap(),
            h.original_messages
        );
        let proof = consumption(&configured, &command);
        assert!(proof.configured);
        assert!(proof.rejection.is_none());
        assert_eq!(
            h.status(operation_id(&command)).await.unwrap()["objective_applied_actual"],
            false
        );
        assert!(h.admit().await);
        let durable = h.durable().await;
        let admitted = durable.messages.last().unwrap();
        assert_eq!(admitted.role, Role::User);
        assert_eq!(admitted.content, objective);
        assert_eq!(
            admitted.metadata.as_ref().unwrap()["runtime_kind"],
            "counted_loop_iteration"
        );
        let status = h.status(operation_id(&command)).await.unwrap();
        assert_eq!(status["objective_applied_actual"], true);
        assert_eq!(
            status["operation_receipt"],
            serde_json::to_value(&proof).unwrap()
        );
        assert_eq!(
            consumption(&durable, &command),
            proof,
            "admission does not rewrite the receipt"
        );
        h.assert_no_activation();
        h.close().await;
    }
}
