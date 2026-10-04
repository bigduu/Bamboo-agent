use super::*;
use bamboo_subagent::{ActorEventBatcher, RunSpec};
use futures_util::poll;
use serde_json::json;

#[tokio::test]
async fn selected_history_waits_for_capacity_and_broker_receipt() {
    let (control, _control_rx) = tokio::sync::mpsc::channel(1);
    let (events, mut rx) = tokio::sync::mpsc::channel(1);
    let uplink = ActorBrokerUplink {
        control,
        events,
        source: AgentRef {
            session_id: "child".into(),
            role: None,
        },
    };
    let correlation_id = MsgId::new();
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
    let mut next_seq = 1;
    for batch in batches {
        uplink
            .events
            .send(ActorEventCommand::Live {
                to: "sentinel".into(),
                correlation_id: correlation_id.clone(),
                batch: batch.clone(),
            })
            .await
            .unwrap();
        let mut send = Box::pin(uplink.send_event_batch("parent", &correlation_id, batch));
        assert!(
            poll!(&mut send).is_pending(),
            "selected history must wait when the queue is full"
        );
        assert!(matches!(
            rx.recv().await,
            Some(ActorEventCommand::Live { .. })
        ));
        assert!(
            poll!(&mut send).is_pending(),
            "enqueue must still wait for broker receipt"
        );
        let Some(ActorEventCommand::Durable {
            to,
            message,
            result,
        }) = rx.recv().await
        else {
            panic!("missing durable history batch")
        };
        assert_eq!(to, "parent");
        assert_eq!(message.correlation_id.as_ref(), Some(&correlation_id));
        let batch: ActorEventBatch = serde_json::from_value(message.body).unwrap();
        assert_eq!(batch.first_seq, next_seq);
        next_seq = batch.last_seq + 1;
        result.send(Ok(MsgId::new())).unwrap();
        assert!(send.await);
    }
    assert_eq!(next_seq, 6);
}

#[tokio::test]
async fn legacy_live_batches_still_drop_at_full_uplink() {
    let (control, _control_rx) = tokio::sync::mpsc::channel(1);
    let (events, mut rx) = tokio::sync::mpsc::channel(1);
    let uplink = ActorBrokerUplink {
        control,
        events,
        source: AgentRef {
            session_id: "child".into(),
            role: None,
        },
    };
    let correlation_id = MsgId::new();
    for event in [json!({"type":"token"}), json!({"type":"runner_progress"})] {
        let spec =
            serde_json::from_value(json!({"assignment":"legacy", "execution_epoch":1})).unwrap();
        let mut batcher = ActorEventBatcher::for_run(&spec, None, None);
        assert!(batcher.push(event).is_empty());
        let batch = batcher.flush().unwrap();
        uplink
            .events
            .send(ActorEventCommand::Live {
                to: "sentinel".into(),
                correlation_id: correlation_id.clone(),
                batch: batch.clone(),
            })
            .await
            .unwrap();
        let mut send = Box::pin(uplink.send_event_batch("parent", &correlation_id, batch));
        assert!(matches!(poll!(&mut send), std::task::Poll::Ready(true)));
        let Some(ActorEventCommand::Live { to, .. }) = rx.recv().await else {
            panic!("missing sentinel")
        };
        assert_eq!(to, "sentinel");
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn blocked_history_uplink_stops_when_queue_or_receipt_closes() {
    for close_before_enqueue in [true, false] {
        let (control, _control_rx) = tokio::sync::mpsc::channel(1);
        let (events, mut rx) = tokio::sync::mpsc::channel(1);
        let uplink = ActorBrokerUplink {
            control,
            events,
            source: AgentRef {
                session_id: "child".into(),
                role: None,
            },
        };
        let correlation_id = MsgId::new();
        let spec =
            serde_json::from_value(json!({"assignment":"read", "execution_epoch":1})).unwrap();
        let mut batcher = ActorEventBatcher::for_run(&spec, None, None);
        let batch = batcher
            .push(json!({"type":"tool_complete", "result":"file evidence"}))
            .pop()
            .unwrap();
        if close_before_enqueue {
            uplink
                .events
                .send(ActorEventCommand::Live {
                    to: "sentinel".into(),
                    correlation_id: correlation_id.clone(),
                    batch: batch.clone(),
                })
                .await
                .unwrap();
        }
        let mut send = Box::pin(uplink.send_event_batch("parent", &correlation_id, batch));
        assert!(poll!(&mut send).is_pending());
        if close_before_enqueue {
            drop(rx);
        } else {
            let Some(ActorEventCommand::Durable { result, .. }) = rx.recv().await else {
                panic!("missing durable command")
            };
            drop(result);
        }
        assert!(matches!(poll!(&mut send), std::task::Poll::Ready(false)));
    }
}

struct HistoryExecutor;

#[async_trait::async_trait]
impl bamboo_subagent::ChildExecutor for HistoryExecutor {
    fn requires_contiguous_events(&self) -> bool {
        true
    }

    async fn run(
        &self,
        _spec: RunSpec,
        events: bamboo_subagent::EventSink,
        _steer: bamboo_subagent::SteerInbox,
        _cancel: CancellationToken,
    ) -> bamboo_subagent::ChildOutcome {
        for event in [
            json!({"type":"token", "content":"before tool"}),
            json!({"type":"reasoning_token", "content":"inspect file"}),
            json!({"type":"runner_progress", "progress":1}),
            json!({"type":"tool_complete", "tool_call_id":"read-1", "result":"file evidence"}),
            json!({"type":"complete"}),
        ] {
            events.emit(event).await;
        }
        bamboo_subagent::ChildOutcome::completed("file evidence")
    }
}

#[tokio::test]
async fn executor_opt_in_reaches_broker_run_and_outcome_follows_receipts() {
    let (control, _control_rx) = tokio::sync::mpsc::channel(1);
    let (events, mut rx) = tokio::sync::mpsc::channel(1);
    let me = AgentRef {
        session_id: "child".into(),
        role: None,
    };
    let uplink = ActorBrokerUplink {
        control,
        events,
        source: me.clone(),
    };
    let run_id = MsgId::new();
    let message = InboxMessage {
        id: run_id.clone(),
        from: AgentRef {
            session_id: "parent".into(),
            role: None,
        },
        kind: InboxKind::Run,
        body: json!({"assignment":"read", "execution_epoch":1}),
        created_at: Utc::now(),
        correlation_id: None,
    };
    let coords = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let waiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let tree_waiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    tokio::time::timeout(Duration::from_secs(2), async {
        let run = handle_run(
            &HistoryExecutor,
            &me,
            message,
            CancellationToken::new(),
            &coords,
            &waiters,
            &tree_waiters,
            &uplink,
            Duration::from_secs(1),
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
            false,
        );
        let receive = async {
            let mut next_seq = 1;
            loop {
                let Some(ActorEventCommand::Durable {
                    to,
                    message,
                    result,
                }) = rx.recv().await
                else {
                    panic!("selected history must use receipt-confirmed delivery")
                };
                assert_eq!(to, "parent");
                assert_eq!(message.correlation_id.as_ref(), Some(&run_id));
                match message.kind {
                    InboxKind::Event => {
                        let batch: ActorEventBatch = serde_json::from_value(message.body).unwrap();
                        assert_eq!(batch.qos, ActorEventQos::Durable);
                        assert_eq!(batch.first_seq, next_seq);
                        next_seq = batch.last_seq + 1;
                    }
                    InboxKind::Outcome => {
                        assert_eq!(next_seq, 6);
                        assert_eq!(message.body["result"], "file evidence");
                        result.send(Ok(MsgId::new())).unwrap();
                        break;
                    }
                    _ => panic!("unexpected ordered message"),
                }
                result.send(Ok(MsgId::new())).unwrap();
            }
        };
        let (handled, ()) = tokio::join!(run, receive);
        assert!(matches!(handled, Handled::Ack));
    })
    .await
    .expect("run should complete after every history and outcome receipt");
    assert!(coords.lock().unwrap().is_empty());
}
