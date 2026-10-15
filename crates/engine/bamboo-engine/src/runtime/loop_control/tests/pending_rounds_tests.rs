//! Tests-only fragment for runtime/loop_control/tests/pending_rounds_tests.rs.
//! Parent tests.rs adds only `mod pending_rounds_tests;`.
//! Reuses Harness, its public request parser, real owner/Store/checkpoint, and
//! existing counted admission/terminal primitives. No production API change,
//! counter fabrication, provider, process, or new execution owner is used.

use super::*;

async fn assert_pending_rounds_checkpoint(h: &Harness, expected: &LoopState) {
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        counted_loop::read_loop_state(&durable).unwrap().as_ref(),
        Some(expected),
        "the exact state, pending objective, revisions and operation receipts must be durable"
    );
    assert_eq!(h.policy().generation(), Some(expected.generation.as_str()));
    assert!(h.policy().active_counted().is_some());
    assert!(!h.policy().stop.is_cancelled());
    assert!(!h.cancel.is_cancelled());
}

fn assert_pending_rounds_receipt(
    result: &ToolResult,
    before: &LoopState,
    after: &LoopState,
    effective_iteration: u32,
) -> LoopControlReceipt {
    assert!(result.success, "{}", result.result);
    let receipt: LoopControlReceipt = serde_json::from_str(&result.result).unwrap();
    let old = before.control.as_ref().unwrap();
    let control = after.control.as_ref().unwrap();
    assert_eq!(receipt.generation, before.generation);
    assert_eq!(receipt.accepted_revision, old.control_revision + 1);
    assert_eq!(receipt.applied_revision_at_acceptance, old.applied_revision);
    assert_eq!(receipt.objective_revision, 2);
    assert_eq!(receipt.effective_iteration, Some(effective_iteration));
    assert!(!receipt.applied);
    assert_eq!(control.control_revision, old.control_revision + 1);
    assert_eq!(control.applied_revision, old.applied_revision);
    assert_eq!(control.operations.len(), old.operations.len() + 1);
    assert_eq!(
        &control.operations[..old.operations.len()],
        old.operations.as_slice(),
        "earlier receipts must remain immutable"
    );
    assert_eq!(control.operations.last().unwrap().receipt, receipt);
    assert_eq!(after.objective, "A");
    assert_eq!(after.objective_revision, 1);
    assert_eq!(
        control.pending_objective,
        Some(PendingLoopObjective {
            objective: "B".into(),
            objective_revision: 2,
        })
    );
    assert_eq!(
        (
            after.iterations_started,
            after.iterations_completed,
            after.rounds_started
        ),
        (
            before.iterations_started,
            before.iterations_completed,
            before.rounds_started
        ),
        "accepting a command must not refund any consumption"
    );
    receipt
}

#[tokio::test]
async fn self_control_active_new_objective_requires_two_future_rounds() {
    let mut h = Harness::new().await;
    let start_command = start("A", 2, 20);
    let start_result = h.command(&start_command, 0).await;
    assert!(start_result.success, "{}", start_result.result);
    h.admit().await;
    let before = h.state();
    assert_eq!(
        (
            before.iterations_started,
            before.iterations_completed,
            before.rounds_started
        ),
        (1, 0, 2)
    );
    assert!(before.control.as_ref().unwrap().pending_objective.is_none());

    let command = h.update(Some("B".into()), None, Some(3));
    let result = h.command(&command, 1).await;
    // On cbe6dc91 this is an ordinary successful receipt, so the intended RED
    // occurs here, not at fixture construction, parser validation, or a panic
    // from fabricating a terminal/admission boundary.
    denied(&result, "no_budget_for_new_objective");
    assert_eq!(
        h.state(),
        before,
        "rejection must not install the new objective or receipt"
    );
    assert_pending_rounds_checkpoint(&h, &before).await;
    let retry = h.command(&command, 1).await;
    denied(&retry, "no_budget_for_new_objective");
    assert_eq!(retry.result, result.result);
    assert_eq!(h.state(), before);
    assert_pending_rounds_checkpoint(&h, &before).await;
    assert_eq!(
        h.command(&start_command, 1).await.result,
        start_result.result,
        "the original immutable receipt remains retrievable before CAS"
    );
    assert_eq!(h.state(), before);
    assert_pending_rounds_checkpoint(&h, &before).await;
}

#[tokio::test]
async fn self_control_pending_objective_bound_only_update_cannot_leave_one_future_round() {
    let mut h = Harness::new().await;
    let start_command = start("A", 2, 20);
    let start_result = h.command(&start_command, 0).await;
    assert!(start_result.success, "{}", start_result.result);
    h.admit().await;
    let update_b = h.update(Some("B".into()), None, None);
    let update_result = h.command(&update_b, 1).await;
    assert!(update_result.success, "{}", update_result.result);
    let before = h.state();
    let control = before.control.as_ref().unwrap();
    assert_eq!((control.control_revision, control.applied_revision), (2, 1));
    assert_eq!(control.pending_objective.as_ref().unwrap().objective, "B");
    assert_eq!(
        (
            before.iterations_started,
            before.iterations_completed,
            before.rounds_started
        ),
        (1, 0, 2)
    );

    // Cover the round-only patch and the same finite limits supplied together.
    // Both must preserve the already-pending goal and every accepted receipt.
    for iterations in [None, Some(2)] {
        let command = h.update(None, iterations, Some(3));
        let result = h.command(&command, 1).await;
        denied(&result, "no_budget_for_new_objective");
        assert_eq!(
            h.state(),
            before,
            "bound-only {iterations:?} changed pending state"
        );
        assert_pending_rounds_checkpoint(&h, &before).await;
    }
    assert_eq!(h.command(&update_b, 1).await.result, update_result.result);
    assert_eq!(
        h.command(&start_command, 1).await.result,
        start_result.result
    );
    assert_eq!(
        h.state(),
        before,
        "immutable receipt replay must not apply the pending goal"
    );
    assert_pending_rounds_checkpoint(&h, &before).await;
}

#[tokio::test]
async fn self_control_active_new_or_pending_objective_accepts_exactly_two_future_rounds() {
    for pending_before in [false, true] {
        let mut h = Harness::new().await;
        assert!(h.command(&start("A", 2, 20), 0).await.success);
        h.admit().await;
        if pending_before {
            let update_b = h.update(Some("B".into()), None, None);
            assert!(h.command(&update_b, 1).await.success);
        }
        let before = h.state();
        let command = h.update((!pending_before).then(|| "B".into()), None, Some(4));
        let result = h.command(&command, 1).await;
        assert!(
            result.success,
            "pending_before={pending_before}: {}",
            result.result
        );
        let accepted = h.state();
        let receipt = assert_pending_rounds_receipt(&result, &before, &accepted, 2);
        assert_eq!((accepted.max_iterations, accepted.max_total_rounds), (2, 4));
        assert_eq!(accepted.max_total_rounds - accepted.rounds_started, 2);
        if pending_before {
            assert_eq!(
                accepted.control.as_ref().unwrap().pending_objective,
                before.control.as_ref().unwrap().pending_objective
            );
        }
        assert_pending_rounds_checkpoint(&h, &accepted).await;

        // A tool-containing provider round cannot also supply the no-tool
        // terminal answer. Charge its later current-iteration round first,
        // then use the existing terminal and next-iteration admission seams.
        // This is a service test of those real primitives, not a model run.
        h.admit().await;
        let current_terminal_round = h.state();
        assert_eq!(
            (
                current_terminal_round.iterations_started,
                current_terminal_round.iterations_completed,
                current_terminal_round.rounds_started
            ),
            (1, 0, 3)
        );
        assert_eq!(current_terminal_round.objective, "A");
        assert_eq!(current_terminal_round.control, accepted.control);
        assert!(matches!(
            counted_loop::terminal(&mut h.session, &h.config)
                .await
                .unwrap(),
            counted_loop::TerminalDecision::Continue
        ));
        let completed_a = h.state();
        assert_eq!(
            (
                completed_a.iterations_started,
                completed_a.iterations_completed,
                completed_a.rounds_started
            ),
            (1, 1, 3)
        );
        assert_eq!(completed_a.objective, "A");
        assert!(completed_a
            .control
            .as_ref()
            .unwrap()
            .pending_objective
            .is_some());
        h.admit().await;
        let admitted_b = h.state();
        assert_eq!(
            (
                admitted_b.iterations_started,
                admitted_b.iterations_completed,
                admitted_b.rounds_started
            ),
            (2, 1, 4)
        );
        assert_eq!(
            (admitted_b.objective.as_str(), admitted_b.objective_revision),
            ("B", 2)
        );
        let applied = admitted_b.control.as_ref().unwrap();
        assert!(applied.pending_objective.is_none());
        assert_eq!(applied.applied_revision, applied.control_revision);
        assert_eq!(applied.iteration_objective_revision, 2);
        assert_eq!(
            applied.operations,
            accepted.control.as_ref().unwrap().operations
        );
        assert_eq!(applied.operations.last().unwrap().receipt, receipt);
        assert_eq!(h.command(&command, 3).await.result, result.result);
        assert_eq!(
            h.state(),
            admitted_b,
            "receipt delivery cannot reset the consumed budget"
        );
        assert_pending_rounds_checkpoint(&h, &admitted_b).await;
        assert!(matches!(
            counted_loop::terminal(&mut h.session, &h.config)
                .await
                .unwrap(),
            counted_loop::TerminalDecision::Finalize
        ));
        let finalizing = h.state();
        assert_eq!(finalizing.status, LoopStatus::Finalizing);
        assert_eq!(
            (
                finalizing.iterations_started,
                finalizing.iterations_completed,
                finalizing.rounds_started
            ),
            (2, 2, 4)
        );
        assert_pending_rounds_checkpoint(&h, &finalizing).await;
    }
}

#[tokio::test]
async fn self_control_pre_first_new_or_pending_objective_can_use_one_future_round() {
    for pending_before in [false, true] {
        let mut h = Harness::new().await;
        let start_command = start("A", 1, 20);
        let start_result = h.command(&start_command, 0).await;
        assert!(start_result.success, "{}", start_result.result);
        if pending_before {
            let update_b = h.update(Some("B".into()), None, None);
            assert!(h.command(&update_b, 0).await.success);
        }
        let before = h.state();
        assert_eq!(
            (
                before.iterations_started,
                before.iterations_completed,
                before.rounds_started
            ),
            (0, 0, 1)
        );
        assert_eq!(before.control.as_ref().unwrap().applied_revision, 0);
        let command = h.update((!pending_before).then(|| "B".into()), None, Some(2));
        // This is still the same initial provider/tool batch (round index 0).
        // No counted iteration has been admitted or needs a terminal round.
        let result = h.command(&command, 0).await;
        assert!(
            result.success,
            "pending_before={pending_before}: {}",
            result.result
        );
        let accepted = h.state();
        let receipt = assert_pending_rounds_receipt(&result, &before, &accepted, 1);
        assert_eq!((accepted.max_iterations, accepted.max_total_rounds), (1, 2));
        assert_eq!(accepted.max_total_rounds - accepted.rounds_started, 1);
        assert_eq!(
            accepted
                .control
                .as_ref()
                .unwrap()
                .iteration_objective_revision,
            0
        );
        if pending_before {
            assert_eq!(
                accepted.control.as_ref().unwrap().pending_objective,
                before.control.as_ref().unwrap().pending_objective
            );
        }
        assert_pending_rounds_checkpoint(&h, &accepted).await;
        h.admit().await;
        let admitted_b = h.state();
        assert_eq!(
            (
                admitted_b.iterations_started,
                admitted_b.iterations_completed,
                admitted_b.rounds_started
            ),
            (1, 0, 2)
        );
        assert_eq!(
            (admitted_b.objective.as_str(), admitted_b.objective_revision),
            ("B", 2)
        );
        let applied = admitted_b.control.as_ref().unwrap();
        assert!(applied.pending_objective.is_none());
        assert_eq!(applied.applied_revision, applied.control_revision);
        assert_eq!(applied.iteration_objective_revision, 2);
        assert_eq!(
            applied.operations,
            accepted.control.as_ref().unwrap().operations
        );
        assert_eq!(applied.operations.last().unwrap().receipt, receipt);
        assert_eq!(h.command(&command, 1).await.result, result.result);
        assert_eq!(
            h.command(&start_command, 1).await.result,
            start_result.result
        );
        assert_eq!(h.state(), admitted_b);
        assert_pending_rounds_checkpoint(&h, &admitted_b).await;
        assert!(matches!(
            counted_loop::terminal(&mut h.session, &h.config)
                .await
                .unwrap(),
            counted_loop::TerminalDecision::Finalize
        ));
        let finalizing = h.state();
        assert_eq!(finalizing.status, LoopStatus::Finalizing);
        assert_eq!(
            (
                finalizing.iterations_started,
                finalizing.iterations_completed,
                finalizing.rounds_started
            ),
            (1, 1, 2)
        );
        assert_pending_rounds_checkpoint(&h, &finalizing).await;
    }
}
