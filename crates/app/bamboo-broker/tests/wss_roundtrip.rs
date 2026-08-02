//! End-to-end: a real broker WS server terminating TLS (`wss://`, #48) + a
//! real client, exchanging an ask/reply over an encrypted loopback connection
//! — the `wss://` counterpart to `ws_roundtrip.rs`'s plaintext coverage.
//!
//! Generates a throwaway self-signed cert via `openssl` (mirrors
//! `bamboo-server`'s `server/tls.rs` test precedent), skipping gracefully if
//! `openssl` isn't available in the test environment. The cert carries a
//! `subjectAltName=IP:127.0.0.1` extension so rustls's hostname verification
//! (which — unlike some older stacks — checks ONLY the SAN, never falls back
//! to the CN) accepts it for a `wss://127.0.0.1:PORT` endpoint.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{BrokerClient, BrokerCore, BrokerServer};
use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret-token";

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

/// Generate a throwaway self-signed cert + PKCS#8 key via `openssl`, with:
/// - `subjectAltName=IP:127.0.0.1` so a live TLS handshake against
///   `wss://127.0.0.1:PORT` passes hostname verification (rustls checks ONLY
///   the SAN, never falls back to the CN), and
/// - `basicConstraints=critical,CA:FALSE`, overriding openssl 3.x's default
///   of `CA:TRUE` on an `-x509` self-signed cert. `client_config_trusting_cert`
///   adds this SAME cert directly to the client's `RootCertStore` (as its own
///   trust anchor) while the server presents it as the TLS end-entity/leaf
///   certificate; current `rustls-webpki` rejects a leaf cert that is marked
///   as a CA (`CaUsedAsEndEntity`) — being a trust anchor doesn't need
///   `CA:TRUE` (anchors aren't constraint-checked the way intermediate/leaf
///   certs are), so `CA:FALSE` satisfies both roles at once.
///
/// Returns `None` (skip) if `openssl` is unavailable or too old to support
/// `-addext`.
fn gen_self_signed(dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap(),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
        ])
        .status();
    match status {
        Ok(s) if s.success() => Some((cert, key)),
        _ => None,
    }
}

/// Start a TLS-terminated broker; returns the `wss://` endpoint, mailbox-root
/// guard, and an observability handle for deterministic lifecycle assertions.
async fn start_tls_broker(cert: &Path, key: &Path) -> (String, TempDir, Arc<BrokerServer>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(
        BrokerServer::new(core, TOKEN)
            .with_tls(cert, key)
            .expect("valid self-signed cert/key builds a TLS acceptor"),
    );
    assert!(server.is_tls(), "with_tls must flip is_tls() on");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let serving = Arc::clone(&server);
    tokio::spawn(async move {
        let _ = serving.serve(listener).await;
    });
    (format!("wss://{addr}"), dir, server)
}

async fn wait_for_no_connections(server: &BrokerServer) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.active_connections() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "TLS broker retained {} active connections",
            server.active_connections()
        )
    });
}

#[tokio::test]
async fn ask_reply_round_trip_over_wss_with_trusted_self_signed_cert() {
    let cert_dir = tempfile::tempdir().expect("tempdir");
    let Some((cert, key)) = gen_self_signed(cert_dir.path()) else {
        eprintln!("skipping wss round-trip test: openssl unavailable");
        return;
    };
    let (endpoint, _mailbox_dir, _server) = start_tls_broker(&cert, &key).await;

    // Trust EXACTLY this self-signed cert (the homelab/cross-network
    // quick-start path, #48) — no OS trust-store changes.
    let tls_config = bamboo_broker::client_config_trusting_cert(&cert)
        .expect("client_config_trusting_cert builds from the same cert");

    let mut child =
        BrokerClient::connect_with_tls(&endpoint, agent("child"), TOKEN, Some(tls_config.clone()))
            .await
            .expect("child connects over wss://");
    child.subscribe().await.expect("child subscribes");

    let mut parent =
        BrokerClient::connect_with_tls(&endpoint, agent("parent"), TOKEN, Some(tls_config))
            .await
            .expect("parent connects over wss://");
    parent.subscribe().await.expect("parent subscribes");

    let the_ask = ask("parent");
    let delivered_id = parent
        .deliver("child", the_ask.clone())
        .await
        .expect("deliver ask over TLS");
    assert_eq!(delivered_id, the_ask.id);

    let got = recv(&mut child).await;
    assert_eq!(got.id, the_ask.id);
    assert_eq!(got.kind, InboxKind::Ask);
    child.ack(got.id.clone()).await.expect("child acks ask");

    let reply = InboxMessage {
        id: MsgId::new(),
        from: agent("child"),
        kind: InboxKind::Reply,
        body: serde_json::to_value(ReplyBody {
            answer: "all systems nominal, encrypted".into(),
        })
        .unwrap(),
        created_at: Utc::now(),
        correlation_id: Some(the_ask.id.clone()),
    };
    child
        .deliver("parent", reply)
        .await
        .expect("deliver reply over TLS");

    let got_reply = recv(&mut parent).await;
    assert_eq!(got_reply.kind, InboxKind::Reply);
    assert_eq!(got_reply.correlation_id, Some(the_ask.id));
    let body: ReplyBody = serde_json::from_value(got_reply.body).unwrap();
    assert_eq!(body.answer, "all systems nominal, encrypted");
}

/// The reader/source ownership fix must release TLS transports too: retaining
/// the `SplitStream` would otherwise keep the rustls session, TCP socket, and
/// server semaphore permit alive exactly like plaintext WebSockets.
#[tokio::test]
async fn short_lived_wss_clients_release_on_drop_and_explicit_close() {
    let cert_dir = tempfile::tempdir().expect("tempdir");
    let Some((cert, key)) = gen_self_signed(cert_dir.path()) else {
        eprintln!("skipping wss lifecycle test: openssl unavailable");
        return;
    };
    let (endpoint, _mailbox_dir, server) = start_tls_broker(&cert, &key).await;
    let tls_config = bamboo_broker::client_config_trusting_cert(&cert)
        .expect("client config trusts generated certificate");

    let mut dropped = BrokerClient::connect_with_tls(
        &endpoint,
        agent("tls-drop"),
        TOKEN,
        Some(tls_config.clone()),
    )
    .await
    .expect("drop client connects over wss");
    dropped
        .list_connected("absent-role")
        .await
        .expect("presence query over wss");
    assert_eq!(server.active_connections(), 1);
    drop(dropped);
    wait_for_no_connections(&server).await;

    let mut graceful =
        BrokerClient::connect_with_tls(&endpoint, agent("tls-close"), TOKEN, Some(tls_config))
            .await
            .expect("close client connects over wss");
    graceful
        .list_connected("absent-role")
        .await
        .expect("presence query over wss");
    graceful.close().await.expect("bounded graceful close");
    wait_for_no_connections(&server).await;
}

/// Security regression: a client that does NOT explicitly trust the
/// self-signed cert (i.e. plain [`BrokerClient::connect`], the OS native root
/// store) must be REJECTED — a self-signed cert must never be silently
/// trusted by default. This is the negative twin of the round-trip test
/// above: it proves `connect_with_tls(..., None)` and `connect_with_tls(...,
/// Some(trusting_config))` behave differently, i.e. the trust config is
/// actually being applied, not ignored.
#[tokio::test]
async fn wss_with_untrusted_self_signed_cert_is_rejected() {
    let cert_dir = tempfile::tempdir().expect("tempdir");
    let Some((cert, key)) = gen_self_signed(cert_dir.path()) else {
        eprintln!("skipping untrusted-cert rejection test: openssl unavailable");
        return;
    };
    let (endpoint, _mailbox_dir, _server) = start_tls_broker(&cert, &key).await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        BrokerClient::connect(&endpoint, agent("untrusting"), TOKEN),
    )
    .await
    .expect("connect attempt does not hang");
    assert!(
        result.is_err(),
        "a self-signed cert must be rejected when the client doesn't explicitly trust it"
    );
}

/// `with_tls` is fail-fast (#48): a missing cert/key must error out of
/// `BrokerServer` construction, never silently fall back to plaintext.
#[tokio::test]
async fn with_tls_fails_fast_on_missing_cert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let result = BrokerServer::new(core, TOKEN).with_tls(
        Path::new("/nonexistent/bamboo-broker/cert.pem"),
        Path::new("/nonexistent/bamboo-broker/key.pem"),
    );
    match result {
        Err(bamboo_broker::BrokerError::Tls(m)) => {
            assert!(m.contains("cert_file"), "error should name cert_file: {m}");
        }
        Err(other) => panic!("expected a Tls error naming cert_file, got {other:?}"),
        Ok(_) => panic!("missing cert must fail, not build a BrokerServer"),
    }
}
