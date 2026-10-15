//! Owner receipt and bounded existing hook-context projection contracts.

use super::*;

const FEEDBACK_CONTEXT_SEPARATOR: &str = "\n\n---\n\n";

fn service_feedback_body(context: &str) -> serde_json::Value {
    let body: serde_json::Value =
        serde_json::from_str(context).expect("one complete associated JSON context");
    assert!(body.is_object());
    assert!(body["tool_call_id"].is_string());
    assert!(body["hook_feedback"].is_string());
    assert!(body.as_object().unwrap().keys().all(|key| matches!(
        key.as_str(),
        "tool_call_id" | "hook_feedback" | "hook_feedback_omitted_bytes"
    )));
    body
}

fn assert_service_feedback_budget(base: &ToolResult, context: &str, cap: u32) {
    use bamboo_compression::TokenCounter;
    let counter = bamboo_compression::TiktokenTokenCounter::default();
    if cap != 0 {
        assert!(
            counter
                .count_text(&base.result)
                .saturating_add(counter.count_text(context))
                .saturating_add(counter.count_text(FEEDBACK_CONTEXT_SEPARATOR))
                <= cap,
            "base ToolResult + JSON context + full standard separator exceed cap {cap}"
        );
    }
}

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

#[test]
fn self_control_feedback_context_counts_full_json_and_separator_with_utf8() {
    use bamboo_compression::TokenCounter;
    let counter = bamboo_compression::TiktokenTokenCounter::default();
    let base = ToolResult::text(true, r#"{"accepted":true}"#);
    let base_bytes = base.result.clone();
    let call_id = "call-\"quoted\"-界\\line\nnext";
    let feedback = "native \"feedback\"\\path\n界😀".repeat(2_000);

    let full = output_limits::feedback_context(&base, call_id, vec![feedback.clone()], 0)
        .expect("cap zero retains existing unlimited semantics");
    let full_body = service_feedback_body(&full);
    assert_eq!(full_body["tool_call_id"], call_id);
    assert_eq!(full_body["hook_feedback"], feedback);
    assert_eq!(
        full_body
            .get("hook_feedback_omitted_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        0
    );
    assert_eq!(base.result, base_bytes);
    assert!(base.success);

    let base_tokens = counter.count_text(&base.result);
    for cap in [1, base_tokens, base_tokens.saturating_add(1)] {
        assert!(
            output_limits::feedback_context(&base, call_id, vec![feedback.clone()], cap).is_none(),
            "base and minimum complete association envelope cannot fit cap {cap}"
        );
    }
    let mut delivered_truncated_context = false;
    for cap in [64, 128, 256, 512] {
        let Some(context) =
            output_limits::feedback_context(&base, call_id, vec![feedback.clone()], cap)
        else {
            continue;
        };
        assert_service_feedback_budget(&base, &context, cap);
        let body = service_feedback_body(&context);
        assert_eq!(body["tool_call_id"], call_id);
        let kept = body["hook_feedback"].as_str().unwrap();
        assert!(
            feedback.starts_with(kept),
            "retain a valid UTF-8 prefix, never split a codepoint"
        );
        let omitted = body
            .get("hook_feedback_omitted_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as usize;
        assert_eq!(kept.len() + omitted, feedback.len());
        delivered_truncated_context |= omitted > 0;
        assert_eq!(
            base.result, base_bytes,
            "context is separate from the immutable paired receipt"
        );
        assert!(base.success);
    }
    assert!(
        delivered_truncated_context,
        "exercise an actually delivered bounded prefix, not only None"
    );

    // Metadata and JSON escaping must fit too; raw feedback token length alone
    // is insufficient. An oversized call identity cannot be truncated or forged.
    assert!(
        output_limits::feedback_context(&base, &"界\\\"".repeat(1_000), vec!["x".into()], 64)
            .is_none()
    );
    for empty in [Vec::<String>::new(), vec![" \n\t ".into()]] {
        assert!(output_limits::feedback_context(&base, call_id, empty, 0).is_none());
    }
}

#[tokio::test]
async fn self_control_feedback_context_leaves_canonical_receipt_bytes_and_ledger_unchanged() {
    let mut h = Harness::new().await;
    let command = start("A", 3, 20);
    let accepted = h.command(&command, 0).await;
    assert!(accepted.success);
    let before = h.state();
    let canonical = before.control.as_ref().unwrap().operations[0]
        .receipt
        .clone();
    let canonical_bytes = serde_json::to_string(&canonical).unwrap();
    assert_eq!(accepted.result, canonical_bytes);
    assert!(!canonical_bytes.contains("hook_feedback"));
    let feedback = "quoted \"feedback\"\n界😀".repeat(1_000);
    for cap in [128, 256, 512, 0] {
        let rendered = output_limits::feedback_context(
            &accepted,
            "actual-associated-call",
            vec![feedback.clone()],
            cap,
        );
        if let Some(context) = rendered {
            assert_service_feedback_budget(&accepted, &context, cap);
            let body = service_feedback_body(&context);
            assert_eq!(body["tool_call_id"], "actual-associated-call");
            let kept = body["hook_feedback"].as_str().unwrap();
            assert!(feedback.starts_with(kept));
            if cap == 0 {
                assert_eq!(kept, feedback);
            }
        } else {
            assert_ne!(cap, 0, "unlimited nonempty feedback must be retained");
        }
        let strict: LoopControlReceipt = serde_json::from_str(&accepted.result).unwrap();
        assert_eq!(strict, canonical);
        assert_eq!(serde_json::to_string(&strict).unwrap(), canonical_bytes);
        assert_eq!(
            h.state(),
            before,
            "pure rendering cannot mutate state or the immutable operation ledger"
        );
        assert_service_cap_durable(&h, &before).await;
    }
    let replay = h.command_with_cap(&command, 0, 0).await;
    assert!(replay.success);
    assert_eq!(replay.result, canonical_bytes);
    assert_eq!(h.state(), before);
    assert_service_cap_durable(&h, &before).await;
}
