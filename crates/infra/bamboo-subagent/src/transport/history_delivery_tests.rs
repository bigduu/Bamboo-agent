use super::*;
use futures_util::poll;
use serde_json::json;

fn history_batches() -> Vec<ActorEventBatch> {
    let spec: RunSpec =
        serde_json::from_value(json!({"assignment":"read", "execution_epoch":1})).unwrap();
    let mut batcher = ActorEventBatcher::for_run(&spec, None, None).with_durable_events(true);
    let mut batches = Vec::new();
    for event in [
        json!({"type":"token", "content":"before tool"}),
        json!({"type":"reasoning_token", "content":"inspect file"}),
        json!({"type":"runner_progress", "progress":1}),
        json!({"type":"tool_complete", "tool_call_id":"read-1", "result":"file evidence"}),
        json!({"type":"complete"}),
    ] {
        batches.extend(batcher.push(event));
    }
    batches.extend(batcher.flush());
    batches
}

#[tokio::test]
async fn selected_history_backpressures_every_batch_without_sequence_gaps() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut next_seq = 1;
    for batch in history_batches() {
        tx.send(ChildFrame::Event {
            event: json!({"sentinel":true}),
        })
        .await
        .unwrap();
        let mut send = Box::pin(send_direct_event_batch(&tx, batch));
        assert!(
            poll!(&mut send).is_pending(),
            "selected history must wait when the queue is full"
        );
        assert!(matches!(rx.recv().await, Some(ChildFrame::Event { .. })));
        assert!(send.await);
        let Some(ChildFrame::EventBatch { batch }) = rx.recv().await else {
            panic!("missing history batch")
        };
        assert_eq!(batch.first_seq, next_seq);
        next_seq = batch.last_seq + 1;
    }
    assert_eq!(next_seq, 6);
}

#[tokio::test]
async fn legacy_live_batches_still_drop_at_full_queue() {
    for event in [json!({"type":"token"}), json!({"type":"runner_progress"})] {
        let spec =
            serde_json::from_value(json!({"assignment":"legacy", "execution_epoch":1})).unwrap();
        let mut batcher = ActorEventBatcher::for_run(&spec, None, None);
        assert!(batcher.push(event).is_empty());
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(ChildFrame::Event {
            event: json!({"sentinel":true}),
        })
        .await
        .unwrap();
        let mut send = Box::pin(send_direct_event_batch(&tx, batcher.flush().unwrap()));
        assert!(matches!(poll!(&mut send), std::task::Poll::Ready(true)));
        assert!(matches!(rx.recv().await, Some(ChildFrame::Event { .. })));
        assert!(rx.try_recv().is_err());
    }
}

struct HistoryExecutor;

#[async_trait::async_trait]
impl ChildExecutor for HistoryExecutor {
    fn requires_contiguous_events(&self) -> bool {
        true
    }

    async fn run(
        &self,
        _spec: RunSpec,
        events: EventSink,
        _steer: SteerInbox,
        _cancel: CancellationToken,
    ) -> crate::ChildOutcome {
        for batch in history_batches() {
            for event in batch.events {
                events.emit(event).await;
            }
        }
        crate::ChildOutcome::completed("file evidence")
    }
}

fn history_run(event_tx: mpsc::Sender<ChildFrame>) -> ActiveRun {
    let spec = serde_json::from_value(json!({"assignment":"read", "execution_epoch":1})).unwrap();
    let (_steer_tx, steer) = SteerInbox::channel();
    let (control_tx, _control_rx) = mpsc::channel(1);
    start_run(
        Arc::new(HistoryExecutor),
        spec,
        steer,
        CancellationToken::new(),
        control_tx,
        event_tx,
        Arc::new(Mutex::new(HashMap::new())),
    )
}

#[tokio::test]
async fn executor_opt_in_reaches_direct_transport_and_terminal_follows_history() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut run = history_run(tx);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut next_seq = 1;
        loop {
            match rx.recv().await.expect("terminal must arrive") {
                ChildFrame::EventBatch { batch } => {
                    assert_eq!(batch.qos, ActorEventQos::Durable);
                    assert_eq!(batch.first_seq, next_seq);
                    next_seq = batch.last_seq + 1;
                }
                ChildFrame::Terminal {
                    status,
                    result,
                    final_event_watermark,
                    ..
                } => {
                    assert_eq!(status, TerminalStatus::Completed);
                    assert_eq!(result.as_deref(), Some("file evidence"));
                    assert_eq!(next_seq, 6);
                    assert_eq!(final_event_watermark.unwrap().final_seq, 5);
                    break;
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        run.task.join().await.unwrap();
    })
    .await
    .expect("bounded history run must complete as its consumer drains");
}

#[tokio::test]
async fn selected_history_full_queue_has_bounded_cancel_and_disconnect() {
    // Poll an actual full-queue send before disconnecting, so this also covers
    // a sender already blocked on capacity rather than only an early close.
    let (tx, rx) = mpsc::channel(1);
    tx.send(ChildFrame::Event {
        event: json!({"sentinel":true}),
    })
    .await
    .unwrap();
    let mut send = Box::pin(send_direct_event_batch(&tx, history_batches().remove(0)));
    assert!(poll!(&mut send).is_pending());
    drop(rx);
    assert!(matches!(poll!(&mut send), std::task::Poll::Ready(false)));

    for disconnect in [false, true] {
        let (tx, rx) = mpsc::channel(1);
        tx.send(ChildFrame::Event {
            event: json!({"sentinel":true}),
        })
        .await
        .unwrap();
        let mut run = history_run(tx);
        if disconnect {
            drop(rx);
            tokio::time::timeout(std::time::Duration::from_secs(2), run.task.join())
                .await
                .expect("disconnect must release blocked event forwarding")
                .unwrap();
        } else {
            tokio::time::timeout(
                DIRECT_RUN_DRAIN_TIMEOUT + std::time::Duration::from_secs(1),
                run.stop(None),
            )
            .await
            .expect("cancellation must bound a run even while its event queue stays full");
            drop(rx);
        }
    }
}

struct ClosureExecutor {
    strict: bool,
    emit_tail: bool,
}

#[async_trait::async_trait]
impl ChildExecutor for ClosureExecutor {
    fn requires_contiguous_events(&self) -> bool {
        self.strict
    }
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        _steer: SteerInbox,
        _cancel: CancellationToken,
    ) -> crate::ChildOutcome {
        if self.emit_tail {
            events
                .emit(json!({"type":"token", "content":"final unbatched tail"}))
                .await;
        }
        let mut outcome = crate::ChildOutcome::completed("done");
        // Executors cannot certify delivery. The transport must replace this.
        outcome.final_event_watermark = Some(crate::ActorEventWatermark {
            version: 99,
            logical_session: spec.logical_session,
            activation_id: spec.activation_run_id,
            execution_epoch: spec.execution_epoch,
            final_seq: 999,
        });
        outcome
    }
}

#[tokio::test]
async fn direct_watermark_is_transport_owned_and_follows_empty_or_flushed_tail() {
    for (strict, emit_tail, epoch) in [
        (true, false, 7),
        (true, true, 7),
        (true, true, 8),
        (true, true, 0),
        (false, true, 7),
    ] {
        let spec: RunSpec = serde_json::from_value(json!({
            "assignment":"read", "execution_epoch":epoch, "activation_run_id":"current",
            "logical_session":{"session_id":"child", "parent_session_id":"parent", "root_session_id":"root"}
        })).unwrap();
        let identity = spec.logical_session.clone().unwrap();
        let (_steer_tx, steer) = SteerInbox::channel();
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (event_tx, mut rx) = mpsc::channel(1);
        let mut run = start_run(
            Arc::new(ClosureExecutor { strict, emit_tail }),
            spec,
            steer,
            CancellationToken::new(),
            control_tx,
            event_tx,
            Arc::new(Mutex::new(HashMap::new())),
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut consumed = 0;
            loop {
                match rx.recv().await.expect("terminal arrives") {
                    ChildFrame::EventBatch { batch } => {
                        assert_eq!(batch.first_seq, 1);
                        consumed = batch.last_seq;
                    }
                    ChildFrame::Event { .. } => consumed += 1,
                    ChildFrame::Terminal {
                        final_event_watermark,
                        ..
                    } => {
                        assert_eq!(consumed, u64::from(emit_tail));
                        if strict && epoch != 0 {
                            assert!(final_event_watermark
                                .unwrap()
                                .matches_consumed_run(&identity, "current", epoch, consumed));
                        } else {
                            assert!(final_event_watermark.is_none());
                        }
                        break;
                    }
                    other => panic!("unexpected frame {other:?}"),
                }
            }
            run.task.join().await.unwrap();
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn direct_failed_final_flush_cannot_return_a_completeness_watermark() {
    let spec = serde_json::from_value(json!({"assignment":"read", "execution_epoch":7})).unwrap();
    let batcher = ActorEventBatcher::for_run(&spec, None, None).with_durable_events(true);
    let (tx, events) = mpsc::channel(1);
    tx.send(json!({"type":"token", "content":"unflushed tail"}))
        .await
        .unwrap();
    drop(tx);
    let (out, receiver) = mpsc::channel(1);
    drop(receiver);
    assert!(forward_direct_events(events, batcher, false, out)
        .await
        .is_err());
}
