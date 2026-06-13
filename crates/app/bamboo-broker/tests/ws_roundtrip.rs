//! End-to-end: a real broker WS server + two real clients exchanging an
//! ask/reply over the network (loopback), exercising auth, deliver, durable
//! backlog, push, ack, and correlation.

use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{BrokerClient, BrokerCore, BrokerServer};
use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret-token";

/// Returns the `ws://` endpoint plus the mailbox-root guard — the caller must
/// hold the guard for the test's lifetime (the broker reads/writes under it).
async fn start_broker() -> (String, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
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
