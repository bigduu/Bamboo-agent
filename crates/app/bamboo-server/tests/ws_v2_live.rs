//! Live WebSocket integration harness for `GET /v2/stream` (issue #187, Epic #181).
//!
//! These tests stand up a REAL bound `bamboo-server` on an ephemeral loopback
//! port and drive a REAL `awc` WebSocket client through the actual ws_v2 driver /
//! forwarder concurrency — the path that `test::init_service` (which never binds a
//! socket) and the in-module unit tests cannot exercise end to end. The
//! concurrency-heavy driver, the per-channel queue fair-merge, the #186
//! child-event hold-open, and the #195/#189 hello auth-gate all get covered over a
//! real handshake here.
//!
//! Determinism: every server binds `127.0.0.1:0` (ephemeral port), every wait is a
//! bounded `tokio::time::timeout`, and assertions are on received frames — never
//! on fixed sleeps. The unauthorized auth-deadline is shortened (process-wide) via
//! `BAMBOO_WS_AUTH_DEADLINE_MS` so the "closed when no hello arrives" path resolves
//! in well under a second instead of the production 10s; it is set ONCE at the top
//! of the binary (see [`init_short_auth_deadline`]) and is long enough that every
//! authorized/local connection authorizes inside the window.

use std::sync::Once;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use awc::ws;
use bamboo_agent_core::AgentEvent;
use bamboo_config::{AccessControlConfig, DeviceCredential};
use bamboo_domain::SessionKind;
use bamboo_server::routes::configure_routes;
use bamboo_server::{AgentRunner, AgentStatus, AppState};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpListener;

/// The shortened unauthorized auth deadline for the whole test binary. Long
/// enough that an authorized/local connection always authenticates inside it, but
/// short enough to assert the "no hello → closed" path quickly.
const TEST_AUTH_DEADLINE_MS: u64 = 1500;

/// The shortened ping interval for the whole test binary (#533): every
/// authorized connection receives an app-level `sys` keepalive data frame at
/// this cadence, so the keepalive tests resolve in milliseconds and every
/// OTHER test doubles as an interleaving check (the generic envelope helpers
/// skip `sys` frames exactly like a tolerant client does).
const TEST_PING_INTERVAL_MS: u64 = 200;

/// A generous-but-bounded ceiling for any single frame wait.
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

static INIT: Once = Once::new();

/// Shorten the ws_v2 unauthorized auth deadline + ping interval process-wide.
/// Production never sets these env vars, so the 10s/15s defaults are untouched;
/// the test binary sets them once so the deadline-close path and the `sys`
/// keepalive cadence are fast and deterministic.
fn init_short_auth_deadline() {
    INIT.call_once(|| {
        std::env::set_var(
            "BAMBOO_WS_AUTH_DEADLINE_MS",
            TEST_AUTH_DEADLINE_MS.to_string(),
        );
        std::env::set_var(
            "BAMBOO_WS_PING_INTERVAL_MS",
            TEST_PING_INTERVAL_MS.to_string(),
        );
    });
}

/// A bound test server: keeps the `AppState`, the `TempDir` (so the data dir
/// outlives the run), the base `ws://` URL, and the running `actix_web` server
/// handle (dropped → stopped on teardown).
struct TestServer {
    state: web::Data<AppState>,
    base_ws_url: String,
    _tmp: TempDir,
    server_handle: actix_web::dev::ServerHandle,
    _join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    /// Build an `AppState` (optionally pre-seeding a device + password config),
    /// bind the full route table on an ephemeral loopback port, and spawn it.
    async fn start(configure: impl FnOnce(&mut bamboo_config::Config)) -> Self {
        init_short_auth_deadline();
        let tmp = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(tmp.path().to_path_buf()).await.unwrap());
        {
            let mut config = state.config.write().await;
            configure(&mut config);
        }

        // Bind first to learn the ephemeral port, then hand the listener to actix.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let std_listener = listener.into_std().unwrap();
        std_listener.set_nonblocking(false).unwrap();

        let state_for_factory = state.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(state_for_factory.clone())
                .configure(configure_routes)
        })
        .workers(1)
        .listen(std_listener)
        .unwrap()
        .run();

        let server_handle = server.handle();
        let join = tokio::spawn(server);

        TestServer {
            state,
            base_ws_url: format!("ws://127.0.0.1:{port}/v2/stream"),
            _tmp: tmp,
            server_handle,
            _join: join,
        }
    }

    async fn stop(self) {
        self.server_handle.stop(false).await;
    }
}

/// One live WS connection: the framed read/write half of an `awc` upgrade.
type WsConn = actix_codec::Framed<awc::BoxedSocket, ws::Codec>;

/// Open a WS connection as a LOCAL client (no Host override → `127.0.0.1` host,
/// so `is_local_request` is true and the connection is pre-authorized).
async fn connect_local(server: &TestServer) -> WsConn {
    let (_resp, framed) = awc::Client::new()
        .ws(&server.base_ws_url)
        .connect()
        .await
        .expect("local ws upgrade");
    framed
}

/// Open a WS connection as a NON-LOCAL client by overriding the `Host` header to
/// a public name. `awc` only injects a `Host` when one is absent, so this makes
/// `is_local_request` return false (mirrors the `access_control` unit test
/// `remote_host_is_not_local_even_when_peer_is_loopback`). With an active device
/// configured, such a connection is NOT pre-authorized and must `hello`.
async fn connect_remote(server: &TestServer) -> WsConn {
    let (_resp, framed) = awc::Client::new()
        .ws(&server.base_ws_url)
        .set_header(awc::http::header::HOST, "bamboo.example.com")
        .connect()
        .await
        .expect("remote ws upgrade (open per #189)");
    framed
}

/// Open a LOCAL WS connection negotiating the `bamboo.v2.msgpack` subprotocol
/// (v2-P3, #181). Returns the framed socket AND the subprotocol the server ECHOED
/// on the upgrade response (per RFC 6455 the server echoes the single selected
/// subprotocol), so the test can assert the handshake negotiated msgpack.
async fn connect_local_msgpack(server: &TestServer) -> (WsConn, Option<String>) {
    let (resp, framed) = awc::Client::new()
        .ws(&server.base_ws_url)
        .protocols(["bamboo.v2.msgpack"])
        .connect()
        .await
        .expect("local msgpack ws upgrade");
    let echoed = resp
        .headers()
        .get(awc::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    (framed, echoed)
}

/// Send a client frame as a BINARY MessagePack frame (the inbound shape a msgpack
/// client uses): the JSON `value` is re-encoded with `to_vec_named` so the wire
/// bytes carry the SAME logical schema the server's `decode_client_frame` expects.
async fn send_msgpack(conn: &mut WsConn, value: Value) {
    let bytes = rmp_serde::to_vec_named(&value).expect("encode client frame as msgpack");
    conn.send(ws::Message::Binary(bytes.into()))
        .await
        .expect("send msgpack client frame");
}

/// Receive the next BINARY frame and decode the MessagePack envelope back to a
/// JSON `Value` (msgpack maps → JSON objects), so assertions read the SAME field
/// names/values as the JSON path. Skips Ping/Pong. Returns `None` on close.
async fn next_msgpack_envelope(conn: &mut WsConn) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, conn.next())
            .await
            .expect("frame did not arrive before timeout")?;
        match frame.expect("ws protocol error") {
            ws::Frame::Binary(bytes) => {
                let value: Value = rmp_serde::from_slice(&bytes).expect("envelope is msgpack");
                // Skip interleaved `sys` keepalives (#533), like a tolerant client.
                if value["ch"] == "sys" {
                    continue;
                }
                return Some(value);
            }
            ws::Frame::Text(bytes) => panic!(
                "msgpack mode must yield BINARY frames, got text: {}",
                String::from_utf8_lossy(&bytes)
            ),
            ws::Frame::Ping(_) | ws::Frame::Pong(_) => continue,
            ws::Frame::Close(_) => return None,
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Send a JSON client frame.
async fn send_json(conn: &mut WsConn, value: Value) {
    conn.send(ws::Message::Text(value.to_string().into()))
        .await
        .expect("send client frame");
}

/// Receive the next TEXT frame as JSON within [`RECV_TIMEOUT`], skipping
/// transport Ping/Pong frames. Returns `None` on close/stream-end.
async fn next_envelope(conn: &mut WsConn) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, conn.next())
            .await
            .expect("frame did not arrive before timeout")?;
        match frame.expect("ws protocol error") {
            ws::Frame::Text(bytes) => {
                let value: Value = serde_json::from_slice(&bytes).expect("envelope is JSON");
                // Skip interleaved `sys` keepalives (#533), like a tolerant client.
                if value["ch"] == "sys" {
                    continue;
                }
                return Some(value);
            }
            ws::Frame::Ping(_) | ws::Frame::Pong(_) => continue,
            ws::Frame::Close(_) => return None,
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Receive the next `sys` keepalive envelope (skipping everything else) within
/// [`RECV_TIMEOUT`]. Decodes text frames as JSON and binary frames as msgpack,
/// so one helper serves both subprotocols.
async fn next_sys_keepalive(conn: &mut WsConn) -> Value {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, conn.next())
            .await
            .expect("sys keepalive did not arrive before timeout")
            .expect("connection closed while waiting for sys keepalive");
        let value: Value = match frame.expect("ws protocol error") {
            ws::Frame::Text(bytes) => serde_json::from_slice(&bytes).expect("envelope is JSON"),
            ws::Frame::Binary(bytes) => rmp_serde::from_slice(&bytes).expect("envelope is msgpack"),
            _ => continue,
        };
        if value["ch"] == "sys" {
            return value;
        }
    }
}

/// Receive an application-level pong and assert its negotiated WS frame kind.
/// Interleaved protocol heartbeats and legacy `sys` keepalives are ignored.
async fn next_app_pong(conn: &mut WsConn, msgpack: bool) -> Value {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, conn.next())
            .await
            .expect("application pong did not arrive before timeout")
            .expect("connection closed while waiting for application pong")
            .expect("ws protocol error");
        let value: Value = match frame {
            ws::Frame::Text(bytes) if !msgpack => {
                serde_json::from_slice(&bytes).expect("pong is JSON")
            }
            ws::Frame::Binary(bytes) if msgpack => {
                rmp_serde::from_slice(&bytes).expect("pong is msgpack")
            }
            ws::Frame::Text(bytes) => panic!(
                "msgpack pong must be BINARY, got text: {}",
                String::from_utf8_lossy(&bytes)
            ),
            ws::Frame::Binary(_) => panic!("JSON pong must be TEXT"),
            ws::Frame::Ping(_) | ws::Frame::Pong(_) => continue,
            ws::Frame::Close(_) => panic!("connection closed before application pong"),
            other => panic!("unexpected ws frame: {other:?}"),
        };
        if value["ch"] == "sys" {
            continue;
        }
        return value;
    }
}

/// Assert the connection is closed (or yields no more data) within [`RECV_TIMEOUT`]
/// — i.e. a Close frame or stream end, and never another text envelope.
async fn expect_closed(conn: &mut WsConn) {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let next = tokio::time::timeout(remaining, conn.next())
            .await
            .expect("connection did not close before timeout");
        match next {
            None => return,
            Some(Ok(ws::Frame::Close(_))) => return,
            Some(Ok(ws::Frame::Ping(_))) | Some(Ok(ws::Frame::Pong(_))) => continue,
            Some(Ok(ws::Frame::Text(bytes))) => {
                panic!(
                    "expected the connection to be closed, got a text frame: {}",
                    String::from_utf8_lossy(&bytes)
                )
            }
            Some(Ok(other)) => panic!("expected close, got {other:?}"),
            Some(Err(_)) => return, // a transport error is also a closed socket
        }
    }
}

/// Register a session in the store so `subscribe agent.{id}` is honored and
/// `has_running_child` can resolve its tree. Persisting via `save_session`
/// updates the same in-memory index `get_index_entry` reads (the store and the
/// persistence backend are the SAME `SessionStoreV2` instance).
async fn register_session(state: &AppState, session: &mut bamboo_agent_core::Session) {
    state.save_session(session).await;
}

/// Insert a runner for `session_id` with the given status (so `has_running_child`
/// sees a Running child).
async fn set_runner_status(state: &AppState, session_id: &str, status: AgentStatus) {
    let mut runners = state.agent_runners.write().await;
    let runner = runners
        .entry(session_id.to_string())
        .or_insert_with(AgentRunner::new);
    runner.status = status;
}

/// Build a device credential with a KNOWN token, mirroring the server's
/// `issue_device_token` hash construction (`SHA-256(hex_decode(salt) || token)`,
/// hex-encoded). The integration test lives in a separate crate and cannot reach
/// the `pub(crate)` `issue_device_token`, so it reproduces the (stable, documented
/// on `DeviceCredential`) hashing here rather than widening production visibility.
fn with_device(config: &mut bamboo_config::Config) -> (DeviceCredential, String) {
    use sha2::{Digest, Sha256};

    let device_id = "bamboo_test01device".to_string();
    let token = "bd1_testtokentesttokentesttoken00".to_string();
    let salt_hex = "0123456789abcdef0123456789abcdef".to_string();

    let mut hasher = Sha256::new();
    hasher.update(hex::decode(&salt_hex).unwrap());
    hasher.update(token.as_bytes());
    let token_hash = hex::encode(hasher.finalize());

    let cred = DeviceCredential {
        device_id,
        label: "test-device".to_string(),
        token_hash,
        token_salt: salt_hex,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        last_used_at: None,
        revoked: false,
    };
    config.access_control = Some(AccessControlConfig {
        password_enabled: false,
        password_hash: None,
        password_salt: None,
        updated_at: None,
        devices: vec![cred.clone()],
    });
    (cred, token)
}

// ── Scenario 1: subscribe → event → unsubscribe ─────────────────────────────

/// A local client subscribes to an `agent.{sid}` channel, the server pushes an
/// `AgentEvent` into that session's broadcast, and the client receives the
/// `{ch,seq,event}` envelope. After unsubscribe, the forwarder stops: a further
/// push is not delivered.
#[actix_web::test]
async fn subscribe_event_unsubscribe_roundtrip() {
    let server = TestServer::start(|_| {}).await;
    let sid = "sess_round";

    let mut root = bamboo_agent_core::Session::new(sid, "test-model");
    register_session(&server.state, &mut root).await;

    let mut conn = connect_local(&server).await;
    let ch = format!("agent.{sid}");
    send_json(&mut conn, json!({"type": "subscribe", "ch": ch})).await;

    // The forwarder subscribes to the broadcast asynchronously after our
    // subscribe frame is processed. Push the event with a bounded retry so the
    // test is robust to that handoff WITHOUT a fixed sleep: re-send until the
    // first matching envelope arrives. The event payload is identical each time,
    // so a re-send only ever produces (at most) the one envelope we assert on
    // first.
    let received = {
        let mut got = None;
        let overall = tokio::time::Instant::now() + RECV_TIMEOUT;
        while tokio::time::Instant::now() < overall {
            server
                .state
                .get_session_event_sender(sid)
                .await
                .send(AgentEvent::Token {
                    content: "Hello".into(),
                })
                .ok();
            if let Ok(Some(env)) =
                tokio::time::timeout(Duration::from_millis(150), next_envelope(&mut conn)).await
            {
                got = Some(env);
                break;
            }
        }
        got.expect("a token envelope must arrive on the agent channel")
    };

    assert_eq!(received["ch"], ch.as_str());
    assert!(received["seq"].as_u64().unwrap() >= 1);
    assert_eq!(received["event"]["type"], "token");
    assert_eq!(received["event"]["content"], "Hello");

    // Unsubscribe, then confirm the forwarder stopped: drain any in-flight token
    // re-sends, then push a DISTINCT marker and assert it never arrives.
    send_json(&mut conn, json!({"type": "unsubscribe", "ch": ch})).await;
    // Allow any already-queued frame to flush, then push the post-unsubscribe
    // marker repeatedly; none of it must reach the client.
    while let Ok(Some(_)) =
        tokio::time::timeout(Duration::from_millis(200), next_envelope(&mut conn)).await
    {}
    for _ in 0..5 {
        server
            .state
            .get_session_event_sender(sid)
            .await
            .send(AgentEvent::Token {
                content: "AFTER_UNSUB".into(),
            })
            .ok();
    }
    let leaked = tokio::time::timeout(Duration::from_millis(400), next_envelope(&mut conn)).await;
    match leaked {
        Err(_) => {} // timed out: nothing arrived — the forwarder is gone.
        Ok(None) => {}
        Ok(Some(env)) => assert_ne!(
            env["event"]["content"], "AFTER_UNSUB",
            "no event must arrive after unsubscribe"
        ),
    }

    server.stop().await;
}

// ── Scenario 2: feed cursor resume ──────────────────────────────────────────

/// Publish a couple of ChangeEvents to the account feed, then connect and
/// `subscribe {ch:"feed", since:N}`. The client must receive the backfill from
/// the cursor plus the live tail with no dup/drop.
#[actix_web::test]
async fn feed_cursor_resume_backfill_and_live_tail() {
    let server = TestServer::start(|_| {}).await;

    // Two durable change events BEFORE the client connects (seq 1, 2).
    for i in 0..2 {
        server.state.account_sink.record(
            None,
            &AgentEvent::SessionDeleted {
                session_id: format!("pre_{i}"),
            },
        );
    }
    // Wait until the writer has assigned both seqs (bounded).
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    while server.state.account_sink.latest_seq() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "feed writer never reached seq 2"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut conn = connect_local(&server).await;
    // Resume from cursor 1 → expect backfill of seq 2 (seq 1 already seen).
    send_json(
        &mut conn,
        json!({"type": "subscribe", "ch": "feed", "since": 1}),
    )
    .await;

    let backfill = next_envelope(&mut conn)
        .await
        .expect("backfill envelope from cursor");
    assert_eq!(backfill["ch"], "feed");
    assert_eq!(
        backfill["seq"], 2,
        "backfill resumes strictly AFTER the cursor"
    );
    assert_eq!(backfill["event"]["event"]["type"], "session_deleted");

    // Now a live event (seq 3) must tail in with no dup of seq 2.
    server.state.account_sink.record(
        None,
        &AgentEvent::SessionDeleted {
            session_id: "live_3".into(),
        },
    );
    let live = next_envelope(&mut conn).await.expect("live tail envelope");
    assert_eq!(live["ch"], "feed");
    assert_eq!(
        live["seq"], 3,
        "live tail continues monotonically, no dup/drop"
    );
    assert_eq!(live["event"]["event"]["session_id"], "live_3");

    server.stop().await;
}

// ── Scenario 3a: terminal with NO child closes the channel ───────────────────

/// A terminal `AgentEvent` on a session with no running child closes the
/// `agent.{sid}` channel: the client receives the terminal control frame.
#[actix_web::test]
async fn agent_terminal_without_child_closes_channel() {
    let server = TestServer::start(|_| {}).await;
    let sid = "sess_term";

    let mut root = bamboo_agent_core::Session::new(sid, "test-model");
    register_session(&server.state, &mut root).await;

    let mut conn = connect_local(&server).await;
    let ch = format!("agent.{sid}");
    send_json(&mut conn, json!({"type": "subscribe", "ch": ch})).await;

    // Push terminal events on a bounded retry until the terminal control frame
    // arrives (robust to the subscribe→broadcast handoff with no fixed sleep).
    let control = {
        let mut got = None;
        let overall = tokio::time::Instant::now() + RECV_TIMEOUT;
        while tokio::time::Instant::now() < overall {
            server
                .state
                .get_session_event_sender(sid)
                .await
                .send(AgentEvent::Complete {
                    usage: Default::default(),
                })
                .ok();
            // Drain until we see a control frame (the Complete event itself comes
            // first, then the terminal control).
            if let Ok(Some(env)) =
                tokio::time::timeout(Duration::from_millis(200), next_envelope(&mut conn)).await
            {
                if env.get("control").is_some() {
                    got = Some(env);
                    break;
                }
            }
        }
        got.expect("a terminal control frame must arrive")
    };

    assert_eq!(control["ch"], ch.as_str());
    assert_eq!(control["control"]["type"], "terminal");
    assert_eq!(control["control"]["reason"], "complete");

    server.stop().await;
}

/// #588 regression guard: a session that completed while the prior socket was
/// half-open has durable assistant history but no in-memory runner after a
/// restart. A late subscription must receive the synthesized completion and
/// terminal control without waiting for another broadcast event.
#[actix_web::test]
async fn completed_session_late_subscribe_replays_terminal_once() {
    let server = TestServer::start(|_| {}).await;
    let sid = "sess_completed_offline";

    let mut root = bamboo_agent_core::Session::new(sid, "test-model");
    root.add_message(bamboo_agent_core::Message::assistant("finished", None));
    register_session(&server.state, &mut root).await;

    let mut conn = connect_local(&server).await;
    let ch = format!("agent.{sid}");
    send_json(&mut conn, json!({"type": "subscribe", "ch": ch})).await;

    let terminal_event = next_envelope(&mut conn)
        .await
        .expect("late subscriber receives synthesized completion");
    assert_eq!(terminal_event["ch"], ch);
    assert_eq!(terminal_event["seq"], 1);
    assert_eq!(terminal_event["event"]["type"], "complete");

    let terminal_control = next_envelope(&mut conn)
        .await
        .expect("late subscriber receives terminal control");
    assert_eq!(terminal_control["ch"], ch);
    assert_eq!(terminal_control["seq"], 2);
    assert_eq!(terminal_control["control"]["type"], "terminal");

    server.stop().await;
}

// ── Scenario 3b: terminal WITH a running child holds the channel open ─────────

/// #186 regression guard: an agent terminal while a child sub-agent is still
/// "running" must NOT close the `agent.{sid}` channel; a later SubAgentCompleted
/// (after the child stops running) closes it. This is LIVE-tested: we register a
/// real child session in the store and a Running runner for it so
/// `has_running_child` resolves true, then flip it before the completion.
#[actix_web::test]
async fn agent_terminal_with_running_child_holds_open_then_closes() {
    let server = TestServer::start(|_| {}).await;
    let root_id = "sess_parent";
    let child_id = "sess_child";

    let mut root = bamboo_agent_core::Session::new(root_id, "test-model");
    register_session(&server.state, &mut root).await;
    let mut child = bamboo_agent_core::Session::new_child(child_id, root_id, "test-model", "child");
    assert_eq!(child.kind, SessionKind::Child);
    register_session(&server.state, &mut child).await;
    // The child is RUNNING → has_running_child(root) is true.
    set_runner_status(&server.state, child_id, AgentStatus::Running).await;

    let mut conn = connect_local(&server).await;
    let ch = format!("agent.{root_id}");
    send_json(&mut conn, json!({"type": "subscribe", "ch": ch})).await;

    // Drive a terminal on the PARENT until its echoed Complete event arrives (so
    // we know the forwarder is subscribed and processed the terminal). While the
    // child runs, NO terminal control must follow.
    let mut saw_complete_event = false;
    let overall = tokio::time::Instant::now() + RECV_TIMEOUT;
    while tokio::time::Instant::now() < overall && !saw_complete_event {
        server
            .state
            .get_session_event_sender(root_id)
            .await
            .send(AgentEvent::Complete {
                usage: Default::default(),
            })
            .ok();
        if let Ok(Some(env)) =
            tokio::time::timeout(Duration::from_millis(200), next_envelope(&mut conn)).await
        {
            // It must be the Complete EVENT, never a terminal control, while the
            // child still runs.
            if env.get("control").is_some() {
                panic!("channel closed (terminal control) while a child was still running (#186)");
            }
            if env["event"]["type"] == "complete" {
                saw_complete_event = true;
            }
        }
    }
    assert!(
        saw_complete_event,
        "the parent Complete event should be forwarded while holding the channel open"
    );

    // Assert the channel stays OPEN: no terminal control during a quiet window.
    if let Ok(Some(env)) =
        tokio::time::timeout(Duration::from_millis(400), next_envelope(&mut conn)).await
    {
        assert!(
            env.get("control").is_none(),
            "channel must stay open while the child runs (#186); got control {env}"
        );
    }

    // The child stops running, then a SubAgentCompleted arrives → close now.
    set_runner_status(&server.state, child_id, AgentStatus::Completed).await;
    let control = {
        let mut got = None;
        let overall = tokio::time::Instant::now() + RECV_TIMEOUT;
        while tokio::time::Instant::now() < overall {
            server
                .state
                .get_session_event_sender(root_id)
                .await
                .send(AgentEvent::SubAgentCompleted {
                    parent_session_id: root_id.into(),
                    child_session_id: child_id.into(),
                    status: "completed".into(),
                    error: None,
                })
                .ok();
            if let Ok(Some(env)) =
                tokio::time::timeout(Duration::from_millis(200), next_envelope(&mut conn)).await
            {
                if env.get("control").is_some() {
                    got = Some(env);
                    break;
                }
            }
        }
        got.expect("a terminal control must arrive once the last child completes")
    };
    assert_eq!(control["control"]["type"], "terminal");
    assert_eq!(control["control"]["reason"], "complete");

    server.stop().await;
}

// ── Scenario 4: hello auth-gate (#189/#195) ──────────────────────────────────

/// A REMOTE (non-local) connection with an active device configured is NOT
/// pre-authorized: a subscribe WITHOUT a valid hello serves NO channel data and
/// the socket is CLOSED by the (shortened) auth deadline.
#[actix_web::test]
async fn remote_without_hello_is_closed_by_deadline() {
    let mut captured = None;
    let server = TestServer::start(|config| {
        captured = Some(with_device(config));
    })
    .await;
    let _ = captured; // device exists so a credential is required for remotes.

    let mut conn = connect_remote(&server).await;
    // A subscribe before any hello must be ignored: no channel data.
    send_json(&mut conn, json!({"type": "subscribe", "ch": "feed"})).await;
    // Also push a feed event; an unauthorized connection must not receive it.
    server.state.account_sink.record(
        None,
        &AgentEvent::SessionDeleted {
            session_id: "nope".into(),
        },
    );

    // The connection must be CLOSED by the deadline, having served nothing.
    expect_closed(&mut conn).await;

    server.stop().await;
}

/// A REMOTE connection that sends a VALID hello authorizes; a subsequent
/// subscribe then works (feed backfill is served).
#[actix_web::test]
async fn remote_with_valid_hello_authorizes_then_subscribe_works() {
    let mut captured = None;
    let server = TestServer::start(|config| {
        captured = Some(with_device(config));
    })
    .await;
    let (cred, token) = captured.unwrap();

    // A durable feed event so the subscribe has something to backfill.
    server.state.account_sink.record(
        None,
        &AgentEvent::SessionDeleted {
            session_id: "evt".into(),
        },
    );
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    while server.state.account_sink.latest_seq() < 1 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut conn = connect_remote(&server).await;
    send_json(
        &mut conn,
        json!({"type": "hello", "device_id": cred.device_id, "token": token}),
    )
    .await;
    // After authorizing, a feed subscribe from cursor 0 backfills seq 1.
    send_json(
        &mut conn,
        json!({"type": "subscribe", "ch": "feed", "since": 0}),
    )
    .await;

    let env = next_envelope(&mut conn)
        .await
        .expect("an authorized remote must receive feed data");
    assert_eq!(env["ch"], "feed");
    assert_eq!(env["seq"], 1);

    server.stop().await;
}

/// A REMOTE connection that sends an INVALID hello is closed immediately.
#[actix_web::test]
async fn remote_with_invalid_hello_is_closed() {
    let mut captured = None;
    let server = TestServer::start(|config| {
        captured = Some(with_device(config));
    })
    .await;
    let (cred, _token) = captured.unwrap();

    let mut conn = connect_remote(&server).await;
    send_json(
        &mut conn,
        json!({"type": "hello", "device_id": cred.device_id, "token": "bd1_wrongwrongwrong"}),
    )
    .await;

    // An invalid credential closes the socket (well before the deadline).
    expect_closed(&mut conn).await;

    server.stop().await;
}

// ── Scenario 5: msgpack subprotocol (v2-P3, #181) ───────────────────────────

/// A local client negotiates `bamboo.v2.msgpack`, the server ECHOES it on the
/// handshake response, the client subscribes over a BINARY msgpack frame, the
/// server pushes an `AgentEvent`, and the client decodes the BINARY msgpack
/// envelope and asserts `ch`/`seq`/`event.content`. This proves the full binary
/// path end to end (real handshake echo + real binary inbound + real binary
/// outbound), not just the unit round-trips.
#[actix_web::test]
async fn msgpack_subprotocol_subscribe_event_roundtrip() {
    let server = TestServer::start(|_| {}).await;
    let sid = "sess_msgpack";

    let mut root = bamboo_agent_core::Session::new(sid, "test-model");
    register_session(&server.state, &mut root).await;

    let (mut conn, echoed) = connect_local_msgpack(&server).await;
    // The server MUST echo the selected subprotocol on the upgrade response.
    assert_eq!(
        echoed.as_deref(),
        Some("bamboo.v2.msgpack"),
        "server must echo the negotiated subprotocol on the handshake"
    );

    let ch = format!("agent.{sid}");
    // Subscribe over a BINARY msgpack frame (client→server msgpack works).
    send_msgpack(&mut conn, json!({"type": "subscribe", "ch": ch})).await;

    // Same bounded-retry handoff as the JSON scenario, but decoding BINARY frames.
    let received = {
        let mut got = None;
        let overall = tokio::time::Instant::now() + RECV_TIMEOUT;
        while tokio::time::Instant::now() < overall {
            server
                .state
                .get_session_event_sender(sid)
                .await
                .send(AgentEvent::Token {
                    content: "Hello".into(),
                })
                .ok();
            if let Ok(Some(env)) =
                tokio::time::timeout(Duration::from_millis(150), next_msgpack_envelope(&mut conn))
                    .await
            {
                got = Some(env);
                break;
            }
        }
        got.expect("a token envelope must arrive as a BINARY msgpack frame")
    };

    // The decoded msgpack envelope carries the SAME logical schema as JSON.
    assert_eq!(received["ch"], ch.as_str());
    assert!(received["seq"].as_u64().unwrap() >= 1);
    assert_eq!(received["event"]["type"], "token");
    assert_eq!(received["event"]["content"], "Hello");

    server.stop().await;
}

/// A client that offers NO subprotocol still gets the JSON path with NO echoed
/// subprotocol — the legacy handshake is byte-for-byte unchanged (zero-regression
/// guard for v2-P3).
#[actix_web::test]
async fn no_subprotocol_stays_json_with_no_echo() {
    let server = TestServer::start(|_| {}).await;

    let (resp, mut conn) = awc::Client::new()
        .ws(&server.base_ws_url)
        .connect()
        .await
        .expect("local ws upgrade");
    // No subprotocol offered → none echoed (legacy handshake unchanged).
    assert!(
        resp.headers()
            .get(awc::http::header::SEC_WEBSOCKET_PROTOCOL)
            .is_none(),
        "a client offering no subprotocol must get no echo"
    );

    // And the wire is still JSON TEXT: a feed event arrives as a text envelope.
    server.state.account_sink.record(
        None,
        &AgentEvent::SessionDeleted {
            session_id: "j".into(),
        },
    );
    send_json(
        &mut conn,
        json!({"type": "subscribe", "ch": "feed", "since": 0}),
    )
    .await;
    let env = next_envelope(&mut conn)
        .await
        .expect("JSON feed envelope on the default path");
    assert_eq!(env["ch"], "feed");

    server.stop().await;
}

// ── Scenario 6: app-level sys keepalive (#533) ──────────────────────────────

/// An authorized JSON connection receives the `sys` keepalive DATA frame on the
/// ping cadence — the browser-observable liveness signal (protocol pings are
/// invisible to JS). Two in a row prove it's periodic, not a one-off.
#[actix_web::test]
async fn sys_keepalive_data_frames_arrive_on_json_connection() {
    let server = TestServer::start(|_| {}).await;
    let mut conn = connect_local(&server).await;

    for _ in 0..2 {
        let env = next_sys_keepalive(&mut conn).await;
        assert_eq!(env["ch"], "sys");
        assert_eq!(env["seq"], 0);
        assert_eq!(env["control"]["type"], "keepalive");
    }

    server.stop().await;
}

/// The msgpack subprotocol carries the same keepalive as a BINARY frame with
/// identical decoded shape.
#[actix_web::test]
async fn sys_keepalive_data_frames_arrive_on_msgpack_connection() {
    let server = TestServer::start(|_| {}).await;
    let (mut conn, subprotocol) = connect_local_msgpack(&server).await;
    assert_eq!(subprotocol.as_deref(), Some("bamboo.v2.msgpack"));

    let env = next_sys_keepalive(&mut conn).await;
    assert_eq!(env["ch"], "sys");
    assert_eq!(env["control"]["type"], "keepalive");

    server.stop().await;
}

/// An UNAUTHORIZED connection gets NO `sys` keepalive: it is served nothing
/// (same posture as channel data) and the auth deadline closes it. The frames
/// observed until close must be protocol-level only.
#[actix_web::test]
async fn sys_keepalive_not_sent_to_unauthorized_connection() {
    let mut captured = None;
    let server = TestServer::start(|config| {
        captured = Some(with_device(config));
    })
    .await;
    let _ = captured; // a device exists → remotes require a hello.

    let mut conn = connect_remote(&server).await;
    // expect_closed panics on ANY text frame — which is exactly the assertion:
    // no sys keepalive may be served before authorization, and the deadline
    // (1.5s here) spans several ping ticks (200ms), so a leak would surface.
    expect_closed(&mut conn).await;

    server.stop().await;
}

// ── Scenario 7: application Ping/Pong ACK (#588) ───────────────────────────

/// The default JSON transport accepts a browser-visible ping and returns the
/// exact top-level pong as a TEXT frame (no channel envelope fields).
#[actix_web::test]
async fn json_ping_returns_top_level_text_pong() {
    let server = TestServer::start(|_| {}).await;
    let mut conn = connect_local(&server).await;

    send_json(&mut conn, json!({"type": "ping"})).await;
    let pong = next_app_pong(&mut conn, false).await;
    assert_eq!(pong, json!({"type": "pong"}));

    server.stop().await;
}

/// A negotiated MessagePack connection accepts a binary named-map ping and
/// returns the same top-level pong as a BINARY named map.
#[actix_web::test]
async fn msgpack_ping_returns_top_level_binary_pong() {
    let server = TestServer::start(|_| {}).await;
    let (mut conn, subprotocol) = connect_local_msgpack(&server).await;
    assert_eq!(subprotocol.as_deref(), Some("bamboo.v2.msgpack"));

    send_msgpack(&mut conn, json!({"type": "ping"})).await;
    let pong = next_app_pong(&mut conn, true).await;
    assert_eq!(pong, json!({"type": "pong"}));

    server.stop().await;
}
