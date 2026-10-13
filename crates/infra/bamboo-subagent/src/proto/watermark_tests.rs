use super::*;
use serde_json::json;

fn run(epoch: u64) -> RunSpec {
    serde_json::from_value(json!({
        "assignment":"read", "execution_epoch":epoch, "activation_run_id":"current",
        "logical_session":{"session_id":"child", "parent_session_id":"parent", "root_session_id":"root"}
    })).unwrap()
}

#[test]
fn strict_watermark_requires_final_flush_and_resets_each_execution_epoch() {
    let first = run(7);
    let identity = first.logical_session.as_ref().unwrap();
    let mut batcher = ActorEventBatcher::for_run(&first, None, None).with_durable_events(true);
    let empty = batcher.final_watermark().unwrap();
    assert_eq!(empty.final_seq, 0);
    assert!(empty.matches_consumed_run(identity, "current", 7, 0));
    assert!(batcher
        .push(json!({"type":"token", "content":"tail"}))
        .is_empty());
    assert!(
        batcher.final_watermark().is_none(),
        "pending tail is not closed"
    );
    assert_eq!(batcher.flush().unwrap().last_seq, 1);
    let final_marker = batcher.final_watermark().unwrap();
    assert!(final_marker.matches_consumed_run(identity, "current", 7, 1));
    assert!(!final_marker.matches_consumed_run(identity, "current", 7, 0));
    assert!(!final_marker.matches_consumed_run(identity, "current", 7, 2));

    let next = ActorEventBatcher::for_run(&run(8), None, None).with_durable_events(true);
    assert_eq!(next.final_watermark().unwrap().final_seq, 0);
    assert!(!final_marker.matches_consumed_run(identity, "current", 8, 1));
    assert!(next
        .final_watermark()
        .unwrap()
        .matches_consumed_run(identity, "current", 8, 0));
}

#[test]
fn watermark_binds_version_logical_identity_activation_and_epoch() {
    let spec = run(7);
    let identity = spec.logical_session.as_ref().unwrap();
    let original = ActorEventBatcher::for_run(&spec, None, None)
        .with_durable_events(true)
        .final_watermark()
        .unwrap();
    for case in [
        "version",
        "logical",
        "parent",
        "root",
        "activation",
        "empty_activation",
        "epoch",
        "legacy",
        "missing_logical",
        "missing_activation",
    ] {
        let mut marker = original.clone();
        match case {
            "version" => marker.version = 99,
            "logical" => marker.logical_session.as_mut().unwrap().session_id = "other".into(),
            "parent" => {
                marker.logical_session.as_mut().unwrap().parent_session_id = Some("other".into())
            }
            "root" => marker.logical_session.as_mut().unwrap().root_session_id = "other".into(),
            "activation" => marker.activation_id = Some("old".into()),
            "empty_activation" => marker.activation_id = Some("".into()),
            "epoch" => marker.execution_epoch = 6,
            "legacy" => marker.execution_epoch = 0,
            "missing_logical" => marker.logical_session = None,
            "missing_activation" => marker.activation_id = None,
            _ => unreachable!(),
        }
        assert!(
            !marker.matches_consumed_run(identity, "current", 7, 0),
            "{case}"
        );
    }
    let frame = ChildFrame::Terminal {
        status: TerminalStatus::Completed,
        result: Some("done".into()),
        error: None,
        transcript: vec![],
        final_event_watermark: Some(original),
    };
    assert_eq!(ChildFrame::from_text(&frame.to_text()).unwrap(), frame);
    // Decode the actual wire discriminator rather than assuming its spelling.
    let mut wire = serde_json::to_value(&frame).unwrap();
    wire.as_object_mut()
        .unwrap()
        .remove("final_event_watermark");
    assert!(matches!(
        serde_json::from_value::<ChildFrame>(wire).unwrap(),
        ChildFrame::Terminal {
            final_event_watermark: None,
            ..
        }
    ));
    let outcome: crate::ChildOutcome =
        serde_json::from_value(json!({"status":"completed", "result":"done", "error":null}))
            .unwrap();
    assert!(outcome.final_event_watermark.is_none());
}

#[test]
fn lossy_legacy_and_saturated_sequences_cannot_certify_strict_completion() {
    assert!(ActorEventBatcher::for_run(&run(7), None, None)
        .final_watermark()
        .is_none());
    assert!(ActorEventBatcher::for_run(&run(0), None, None)
        .with_durable_events(true)
        .final_watermark()
        .is_none());
    let mut batcher = ActorEventBatcher::for_run(&run(7), None, None).with_durable_events(true);
    batcher.next_seq = u64::MAX - 1;
    batcher.push(json!({"type":"complete"}));
    assert!(batcher.final_watermark().is_none());
}
