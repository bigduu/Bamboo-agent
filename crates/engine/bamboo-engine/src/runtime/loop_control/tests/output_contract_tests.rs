//! Owner receipt preflight and canonical-versus-output feedback contracts.

use super::*;

fn assert_service_cap_rejection(result: &ToolResult, cap: u32) {
    assert!(!result.success, "cap {cap}: {}", result.result);
    assert!(
        output_limits::fits(result, cap),
        "rejection must also fit cap {cap}"
    );
    // A cap too small even for the error object is represented truthfully by
    // an empty failure, rather than truncated/malformed JSON or a fake receipt.
    if !result.result.is_empty() {
        assert_eq!(value(result)["error"], "output_budget_too_small");
    }
}

async fn assert_service_cap_durable(h: &Harness, expected: &LoopState) {
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        counted_loop::read_loop_state(&durable).unwrap().as_ref(),
        Some(expected)
    );
}

#[tokio::test]
async fn self_control_active_update_and_stop_preflight_caps_before_any_command_effect() {
    for stop in [false, true] {
        for cap in [1, 16, 64] {
            let mut h = Harness::new().await;
            assert!(h.command(&start("A", 3, 20), 0).await.success);
            h.admit().await;
            let before = h.state();
            let generation = h.policy().generation().unwrap().to_string();
            assert!(h.policy().active_counted().is_some());
            assert!(!h.policy().stop.is_cancelled());
            let command = if stop {
                h.stop()
            } else {
                h.update(Some("B".into()), Some(2), Some(10))
            };
            let operation = operation_id(&command).unwrap().to_string();

            let rejected = h.command_with_cap(&command, 1, cap).await;
            assert_service_cap_rejection(&rejected, cap);
            assert_eq!(
                h.state(),
                before,
                "stop={stop}, cap={cap}: state/ledger changed"
            );
            assert_eq!(h.policy().generation(), Some(generation.as_str()));
            assert!(h.policy().active_counted().is_some());
            assert!(
                !h.policy().stop.is_cancelled(),
                "child stop must not cancel its parent"
            );
            assert_service_cap_durable(&h, &before).await;

            // CountedLoopConfig's child token is deliberately private. Probe
            // it through the real admission boundary instead of adding a test
            // accessor or inspecting Debug output. This is charged only AFTER
            // the rejected-command snapshot equality above has been checked.
            h.admit().await;
            let after_probe = h.state();
            assert_eq!(after_probe.control, before.control);
            assert_eq!(
                (
                    after_probe.iterations_started,
                    after_probe.iterations_completed
                ),
                (1, 0)
            );
            assert_eq!(after_probe.rounds_started, before.rounds_started + 1);

            // The rejected attempt did not consume this UUID or its CAS.
            let accepted = h.command_with_cap(&command, 2, 0).await;
            assert!(
                accepted.success,
                "stop={stop}, cap={cap}: {}",
                accepted.result
            );
            let receipt: LoopControlReceipt = serde_json::from_str(&accepted.result).unwrap();
            assert_eq!(receipt.operation_id, operation);
            assert_eq!(receipt.accepted_revision, 2);
            assert_eq!(receipt.applied, stop);
            assert!(receipt.hook_feedback.is_none());
            assert!(receipt.hook_feedback_omitted_bytes.is_none());
            let committed = h.state();
            assert_eq!(
                (
                    committed.iterations_started,
                    committed.iterations_completed,
                    committed.rounds_started
                ),
                (1, 0, 3)
            );
            let control = committed.control.as_ref().unwrap();
            assert_eq!(control.operations.len(), 2);
            assert_eq!(control.operations.last().unwrap().receipt, receipt);
            assert_eq!(control.stop_requested, stop);
            assert_eq!(control.control_revision, 2);
            assert_service_cap_durable(&h, &committed).await;

            // Delivery of an already accepted receipt can fail at a smaller
            // cap; its immutable operation and prior effects remain unchanged.
            let duplicate_rejected = h.command_with_cap(&command, 2, cap).await;
            assert_service_cap_rejection(&duplicate_rejected, cap);
            assert_eq!(h.state(), committed);
            assert_service_cap_durable(&h, &committed).await;
            let duplicate = h.command_with_cap(&command, 2, 0).await;
            assert!(duplicate.success);
            assert_eq!(duplicate.result, accepted.result);
            assert_eq!(h.state(), committed);
            assert_service_cap_durable(&h, &committed).await;

            let admitted =
                counted_loop::before_round(&mut h.session, &h.config, "actual-execution")
                    .await
                    .unwrap();
            assert_eq!(
                admitted, !stop,
                "only the successful Stop may close admission"
            );
            let final_state = h.state();
            assert_eq!(final_state.control, committed.control);
            assert_eq!(final_state.iterations_started, committed.iterations_started);
            assert_eq!(
                final_state.iterations_completed,
                committed.iterations_completed
            );
            if stop {
                assert_eq!(final_state.status, LoopStatus::Stopped);
                assert_eq!(final_state.rounds_started, committed.rounds_started);
            } else {
                assert_eq!(final_state.status, LoopStatus::Running);
                assert_eq!(final_state.rounds_started, committed.rounds_started + 1);
            }
            assert_service_cap_durable(&h, &final_state).await;
        }
    }
}

#[tokio::test]
async fn self_control_output_feedback_is_forbidden_in_persisted_canonical_receipts() {
    let mut h = Harness::new().await;
    assert!(h.command(&start("A", 3, 20), 0).await.success);
    let clean = h.state();
    let canonical = clean.control.as_ref().unwrap().operations[0]
        .receipt
        .clone();
    let canonical_bytes = serde_json::to_string(&canonical).unwrap();
    assert!(!canonical_bytes.contains("hook_feedback"));
    let legacy: LoopControlReceipt = serde_json::from_str(&canonical_bytes).unwrap();
    assert_eq!(legacy, canonical);
    assert!(legacy.hook_feedback.is_none());
    assert!(legacy.hook_feedback_omitted_bytes.is_none());
    let original_metadata = h.session.metadata.clone();

    // A clean terminal baseline permits recovery inspection, so the failures
    // below cannot pass merely because every persisted live loop is refused.
    let mut retired = clean.clone();
    retired.status = LoopStatus::Failed;
    let mut retired_session = h.session.clone();
    retired_session.metadata.insert(
        counted_loop::LOOP_STATE_METADATA_KEY.into(),
        serde_json::to_string(&retired).unwrap(),
    );
    assert!(counted_loop::reject_recovery(&retired_session).is_ok());
    h.storage.save_session(&retired_session).await.unwrap();
    let proof = h.registration.ownership();
    let mut clean_candidate = retired_session.clone();
    counted_loop::admit_registered(
        &mut clean_candidate,
        Some(&proof),
        Some(&h.router),
        h.config.persistence.as_deref().unwrap(),
    )
    .await
    .unwrap();

    for omitted_bytes in [false, true] {
        let mut bad = retired.clone();
        let receipt = &mut bad.control.as_mut().unwrap().operations[0].receipt;
        if omitted_bytes {
            receipt.hook_feedback_omitted_bytes = Some(0);
        } else {
            receipt.hook_feedback = Some(String::new());
        }
        assert!(counted_loop::encode_next_state(&bad).is_err());
        let mut stage_attempt = h.session.clone();
        assert!(counted_loop::stage_state(&mut stage_attempt, bad.clone()).is_err());
        assert_eq!(stage_attempt.metadata, original_metadata);
        assert_eq!(h.session.metadata, original_metadata);

        let mut corrupt = retired_session.clone();
        corrupt.metadata.insert(
            counted_loop::LOOP_STATE_METADATA_KEY.into(),
            serde_json::to_string(&bad).unwrap(),
        );
        assert!(counted_loop::read_loop_state(&corrupt).is_err());
        assert!(counted_loop::reject_recovery(&corrupt).is_err());
        h.storage.save_session(&corrupt).await.unwrap();
        let durable = h.storage.load_session(&corrupt.id).await.unwrap().unwrap();
        assert!(counted_loop::read_loop_state(&durable).is_err());
        let mut candidate = durable.clone();
        let before = serde_json::to_value(&candidate).unwrap();
        assert!(counted_loop::admit_registered(
            &mut candidate,
            Some(&proof),
            Some(&h.router),
            h.config.persistence.as_deref().unwrap()
        )
        .await
        .is_err());
        assert_eq!(serde_json::to_value(&candidate).unwrap(), before);
        assert_eq!(serde_json::to_string(&canonical).unwrap(), canonical_bytes);
        assert!(!serde_json::to_string(&clean)
            .unwrap()
            .contains("hook_feedback"));
    }
}

#[tokio::test]
async fn self_control_feedback_projection_parses_public_dto_without_polluting_canonical_receipt() {
    let mut h = Harness::new().await;
    let command = start("A", 3, 20);
    let accepted = h.command(&command, 0).await;
    assert!(accepted.success);
    let before = h.state();
    let canonical = before.control.as_ref().unwrap().operations[0]
        .receipt
        .clone();
    let canonical_bytes = serde_json::to_string(&canonical).unwrap();
    let mut projected = accepted.clone();
    output_limits::append_feedback(
        &mut projected,
        vec!["quoted \"feedback\"\n界".repeat(1_000)],
        256,
    );
    assert!(projected.success);
    assert!(output_limits::fits(&projected, 256));
    let mut public: LoopControlReceipt = serde_json::from_str(&projected.result).unwrap();
    assert!(public.hook_feedback.is_some());
    assert!(public
        .hook_feedback_omitted_bytes
        .is_some_and(|bytes| bytes > 0));
    public.hook_feedback = None;
    public.hook_feedback_omitted_bytes = None;
    assert_eq!(public, canonical);
    assert_eq!(serde_json::to_string(&public).unwrap(), canonical_bytes);
    assert_eq!(
        h.state(),
        before,
        "rendering output cannot mutate the operation ledger"
    );
    assert_service_cap_durable(&h, &before).await;
    let replay = h.command_with_cap(&command, 0, 0).await;
    assert!(replay.success);
    assert_eq!(replay.result, accepted.result);
    assert_eq!(h.state(), before);
    assert_service_cap_durable(&h, &before).await;
}
