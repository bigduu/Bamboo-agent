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
                ChildFrame::Terminal { status, result, .. } => {
                    assert_eq!(status, TerminalStatus::Completed);
                    assert_eq!(result.as_deref(), Some("file evidence"));
                    assert_eq!(next_seq, 6);
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
