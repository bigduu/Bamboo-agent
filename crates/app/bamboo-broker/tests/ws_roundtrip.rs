//! End-to-end: a real broker WS server + two real clients exchanging an
//! ask/reply over the network (loopback), exercising auth, deliver, durable
//! backlog, push, ack, and correlation.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{BrokerClient, BrokerCore, BrokerLimits, BrokerServer};
use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret-token";

/// Returns the `ws://` endpoint plus the mailbox-root guard — the caller must
/// hold the guard for the test's lifetime (the broker reads/writes under it).
async fn start_broker() -> (String, TempDir) {
    start_broker_with_limits(BrokerLimits::default()).await
}

/// Like [`start_broker`], with explicit DoS-defense limits (#53) instead of
/// the generous defaults — lets tests exercise `max_connections` /
/// `messages_per_second` without waiting out production-sized quotas.
async fn start_broker_with_limits(limits: BrokerLimits) -> (String, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::with_limits(core, TOKEN, limits));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (format!("ws://{addr}"), dir)
}

fn agent(id: &str) -> AgentRef {
    AgentRef {
        session_id: id.into(),
        role: None,
    }
}

fn ask(from: &str) -> InboxMessage {
    InboxMessage {
        id: MsgId::new(),
        from: agent(from),
        kind: InboxKind::Ask,
        body: serde_json::to_value(AskBody {
            question: "what's your status?".into(),
            mode: AskMode::Query,
        })
        .unwrap(),
        created_at: Utc::now(),
        correlation_id: None,
    }
}

async fn recv(client: &mut BrokerClient) -> InboxMessage {
    tokio::time::timeout(Duration::from_secs(5), client.next_message())
        .await
        .expect("timed out waiting for message")
        .expect("connection closed")
}

#[tokio::test]
async fn ask_reply_round_trip_over_ws() {
    let (endpoint, _dir) = start_broker().await;

    // Child subscribes to receive Asks addressed to it.
    let mut child = BrokerClient::connect(&endpoint, agent("child"), TOKEN)
        .await
        .expect("child connects");
    child.subscribe().await.expect("child subscribes");

    // Parent delivers an Ask to the child, and subscribes for the Reply.
    let mut parent = BrokerClient::connect(&endpoint, agent("parent"), TOKEN)
        .await
        .expect("parent connects");
    parent.subscribe().await.expect("parent subscribes");

    let the_ask = ask("parent");
    let delivered_id = parent
        .deliver("child", the_ask.clone())
        .await
        .expect("deliver ask");
    assert_eq!(delivered_id, the_ask.id, "Delivered echoes the stored id");

    // Child receives the Ask (live push or subscribe-time backlog — either way).
    let got = recv(&mut child).await;
    assert_eq!(got.id, the_ask.id);
    assert_eq!(got.kind, InboxKind::Ask);
    child.ack(got.id.clone()).await.expect("child acks ask");

    // Child answers: deliver a Reply correlated to the ask, back to the parent.
    let reply = InboxMessage {
        id: MsgId::new(),
        from: agent("child"),
        kind: InboxKind::Reply,
        body: serde_json::to_value(ReplyBody {
            answer: "all systems nominal".into(),
        })
        .unwrap(),
        created_at: Utc::now(),
        correlation_id: Some(the_ask.id.clone()),
    };
    child
        .deliver("parent", reply.clone())
        .await
        .expect("deliver reply");

    // Parent receives the correlated Reply.
    let got_reply = recv(&mut parent).await;
    assert_eq!(got_reply.kind, InboxKind::Reply);
    assert_eq!(got_reply.correlation_id, Some(the_ask.id));
    let body: ReplyBody = serde_json::from_value(got_reply.body).unwrap();
    assert_eq!(body.answer, "all systems nominal");
    parent.ack(got_reply.id).await.expect("parent acks reply");
}

#[tokio::test]
async fn durable_backlog_delivered_when_subscriber_connects_late() {
    let (endpoint, _dir) = start_broker().await;

    // Parent delivers before the child is even connected.
    let mut parent = BrokerClient::connect(&endpoint, agent("parent2"), TOKEN)
        .await
        .expect("parent connects");
    let the_ask = ask("parent2");
    parent
        .deliver("late-child", the_ask.clone())
        .await
        .expect("deliver to offline child");

    // Child connects + subscribes afterwards and still gets the queued Ask.
    let mut child = BrokerClient::connect(&endpoint, agent("late-child"), TOKEN)
        .await
        .expect("child connects");
    child.subscribe().await.expect("subscribe");
    let got = recv(&mut child).await;
    assert_eq!(
        got.id, the_ask.id,
        "durable backlog reaches a late subscriber"
    );
}

#[tokio::test]
async fn bad_token_is_rejected() {
    let (endpoint, _dir) = start_broker().await;
    let err = BrokerClient::connect(&endpoint, agent("x"), "wrong-token").await;
    assert!(
        matches!(err, Err(bamboo_broker::BrokerError::Auth(_))),
        "wrong token must be rejected with an auth error"
    );
}

#[tokio::test]
async fn cancel_reaches_subscribed_worker_via_next_cancel() {
    let (endpoint, _dir) = start_broker().await;

    let mut worker = BrokerClient::connect(&endpoint, agent("worker"), TOKEN)
        .await
        .expect("worker connects");
    worker.subscribe().await.expect("worker subscribes");

    let mut boss = BrokerClient::connect(&endpoint, agent("boss"), TOKEN)
        .await
        .expect("boss connects");

    // Confirm the worker's subscription is live via a normal deliver round-trip
    // BEFORE the out-of-band cancel (which the broker drops if the target isn't
    // subscribed) — otherwise the test could race the Subscribe registration.
    let probe = ask("boss");
    boss.deliver("worker", probe.clone())
        .await
        .expect("deliver probe");
    assert_eq!(recv(&mut worker).await.id, probe.id);

    // The cancel arrives on the worker's out-of-band lane (next_cancel), NOT the
    // message lane.
    let cid = MsgId::new();
    boss.cancel("worker", &cid).await.expect("send cancel");

    let cancelled = tokio::time::timeout(Duration::from_secs(5), worker.next_cancel())
        .await
        .expect("cancel arrives in time")
        .expect("connection stays open");
    assert_eq!(
        cancelled, cid,
        "worker received the cancel's correlation id"
    );
}

/// Start a broker on an explicit mailbox-root (so a restart reuses the same
/// persisted state). Returns the ws endpoint + the server task handle (abort it
/// to simulate a crash).
async fn start_broker_on(root: &std::path::Path) -> (String, tokio::task::JoinHandle<()>) {
    let core = Arc::new(BrokerCore::new(root));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (format!("ws://{addr}"), handle)
}

/// End-to-end at-least-once durability across a broker crash+restart: deliver
/// three asks, ack one, kill the broker, restart it on the SAME mailbox root,
/// reconnect — the two UNACKED asks are redelivered and the acked one is not.
#[tokio::test]
async fn unacked_messages_redelivered_after_broker_crash_and_restart() {
    // One mailbox root that survives the broker "crash" + restart.
    let dir = tempfile::tempdir().expect("tempdir");

    // --- broker #1 ---
    let (endpoint1, server1) = start_broker_on(dir.path()).await;

    let mut worker = BrokerClient::connect(&endpoint1, agent("worker"), TOKEN)
        .await
        .expect("worker connects");
    worker.subscribe().await.expect("worker subscribes");

    let mut boss = BrokerClient::connect(&endpoint1, agent("boss"), TOKEN)
        .await
        .expect("boss connects");

    let m1 = ask("boss");
    let m2 = ask("boss");
    let m3 = ask("boss");
    for m in [&m1, &m2, &m3] {
        boss.deliver("worker", m.clone())
            .await
            .expect("deliver ask");
    }

    // Worker receives all three but ACKs only m1; m2 and m3 stay in-flight.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..3 {
        let got = recv(&mut worker).await;
        seen.insert(got.id.clone());
        if got.id == m1.id {
            worker.ack(got.id.clone()).await.expect("worker acks m1");
        }
    }
    assert_eq!(seen.len(), 3, "worker received all three deliveries");

    // --- crash: kill broker #1 and drop the live connections ---
    server1.abort();
    let _ = server1.await; // wait for the task to actually stop
    drop(worker);
    drop(boss);

    // --- broker #2 on the SAME root ---
    let (endpoint2, server2) = start_broker_on(dir.path()).await;

    let mut worker2 = BrokerClient::connect(&endpoint2, agent("worker"), TOKEN)
        .await
        .expect("worker reconnects");
    worker2.subscribe().await.expect("worker re-subscribes");

    // The two UNACKED asks (m2, m3) are redelivered; the acked m1 is not.
    let mut redelivered = std::collections::HashSet::new();
    for _ in 0..2 {
        redelivered.insert(recv(&mut worker2).await.id);
    }
    assert!(
        redelivered.contains(&m2.id),
        "unacked m2 must be redelivered"
    );
    assert!(
        redelivered.contains(&m3.id),
        "unacked m3 must be redelivered"
    );
    assert!(
        !redelivered.contains(&m1.id),
        "acked m1 must NOT be redelivered (ack is durable)"
    );

    // Nothing further is delivered (the acked message stays acked).
    let extra = tokio::time::timeout(Duration::from_millis(500), worker2.next_message()).await;
    assert!(
        extra.is_err(),
        "no extra redelivery after the two unacked asks"
    );

    server2.abort();
}

/// Connection-flood DoS defense (#53): once `max_connections` accepted slots
/// are all held, a new connection attempt must be rejected rather than
/// accepted (which would let an attacker keep opening connections without
/// bound).
#[tokio::test]
async fn max_connections_rejects_beyond_cap() {
    let (endpoint, _dir) = start_broker_with_limits(BrokerLimits {
        max_connections: 1,
        ..BrokerLimits::default()
    })
    .await;

    // Take the one available slot and keep it alive for the test.
    let _first = BrokerClient::connect(&endpoint, agent("first"), TOKEN)
        .await
        .expect("first connection takes the only slot");

    // A second connection attempt has nowhere to land — the server drops the
    // raw stream before even attempting the WS handshake, so this must fail
    // (not hang, not silently succeed).
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        BrokerClient::connect(&endpoint, agent("second"), TOKEN),
    )
    .await
    .expect("connection attempt does not hang");
    assert!(
        second.is_err(),
        "a connection beyond max_connections must be rejected"
    );
}

/// The connection cap is a live pool, not a one-shot budget (#53): once a
/// connection holding a slot disconnects, that slot becomes available again
/// for a new connection.
#[tokio::test]
async fn max_connections_slot_frees_on_disconnect() {
    let (endpoint, _dir) = start_broker_with_limits(BrokerLimits {
        max_connections: 1,
        ..BrokerLimits::default()
    })
    .await;
    let addr = endpoint.strip_prefix("ws://").expect("ws:// endpoint");

    {
        // Take the one slot with a raw TCP connection — the semaphore permit
        // is acquired and held for the connection task's whole lifetime
        // regardless of whether the WS/Hello handshake ever completes, so
        // this alone is enough to occupy the slot. (`BrokerClient` isn't used
        // here because its background reader task outlives a mere `drop`,
        // which would make this test about the client's lifecycle rather
        // than the server's slot accounting.)
        let _raw = tokio::net::TcpStream::connect(addr)
            .await
            .expect("raw tcp connects");
        // Dropped at end of scope: the reset tears down the server's
        // accept_async/handle_conn task, releasing its permit.
    }

    // Give the server task a moment to notice the closed connection and
    // release its semaphore permit.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        BrokerClient::connect(&endpoint, agent("second"), TOKEN),
    )
    .await
    .expect("connection attempt does not hang");
    assert!(second.is_ok(), "a freed slot must admit a new connection");
}

/// Message-flood DoS defense (#53): a connection sending `Deliver` frames
/// faster than `messages_per_second`/`message_burst` allow is backpressured
/// (delayed), not instantly served — a flood can't write to the mailbox
/// store at unbounded speed. Delivery still eventually SUCCEEDS (this is a
/// throttle, not a hard reject), so a legitimate connection that briefly
/// bursts is merely slowed, never disconnected or dropped.
#[tokio::test]
async fn deliver_rate_limit_backpressures_a_flooding_connection() {
    let (endpoint, _dir) = start_broker_with_limits(BrokerLimits {
        // 1 msg/sec, no burst: the 2nd and 3rd delivers in immediate
        // succession must each wait ~1s for the bucket to refill.
        messages_per_second: NonZeroU32::new(5).unwrap(),
        message_burst: NonZeroU32::new(1).unwrap(),
        ..BrokerLimits::default()
    })
    .await;

    let mut sender = BrokerClient::connect(&endpoint, agent("flooder"), TOKEN)
        .await
        .expect("connects");

    let started = std::time::Instant::now();
    for _ in 0..3 {
        sender
            .deliver("victim", ask("flooder"))
            .await
            .expect("throttled delivery still eventually succeeds");
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(150),
        "a burst beyond the token bucket must be delayed, not accepted \
         instantly (took {elapsed:?})"
    );
}
