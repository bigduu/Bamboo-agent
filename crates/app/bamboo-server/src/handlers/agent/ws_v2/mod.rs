//! v2-P1 unified WebSocket multiplex: `GET /v2/stream`.
//!
//! One WebSocket replaces the two v1 SSE streams (`GET /events/{id}` +
//! `GET /stream`) plus a minimal `stop` control uplink, so a mobile client uses
//! ONE connection instead of two SSE + scattered POSTs. The v1 SSE/REST
//! endpoints stay UNCHANGED (dual-track per `docs/api-v2-transport.md` §8.1).
//!
//! ## Channels (server→client)
//! - `feed` — the account `ChangeEvent` stream (reuses `plan_replay` + journal).
//! - `agent.{session_id}` — a per-session `AgentEvent` stream (reuses the v1
//!   critical-event replay + the v1 `Coalescer` for token batching).
//!
//! ## Control (client→server, P1)
//! - `stop` only — cancels a session (reuses the v1 stop discipline).
//!
//! ## Design
//! Each subscribed channel runs its OWN forwarder task with its OWN broadcast
//! receiver (per-channel lag independence; §10-Q3) AND its OWN bounded outbound
//! `mpsc` queue. The driver holds a `StreamMap<channel, ReceiverStream>` and
//! drains every per-channel queue with a fair (randomized-start) merge, so a burst
//! on one channel can no longer head-of-line another at the socket (RFC §10-Q3 —
//! see `forwarders.rs` for the honest fairness guarantee). A per-channel
//! `JoinHandle` lets `unsubscribe`/teardown abort exactly that forwarder and drop
//! its queue, leaving no orphaned broadcast reader.
//!
//! ## Encoding (v2-P3, #181)
//! The wire encoding is negotiated ONCE at the upgrade from the offered
//! `Sec-WebSocket-Protocol`: `bamboo.v2.msgpack` selects binary MessagePack,
//! anything else (or nothing) stays JSON text (the default — desktop /
//! debuggability). The SAME envelope schema is carried either way; only the
//! serialization + WS frame type (Text vs Binary) differs. See `envelope::Encoding`.
//!
//! ## DEFERRED (later slices; see PR / §5.3)
//! - `execute` / `approve` over control (handler refactors) — clients keep REST.
//!
//! ## Auth (v2-P2, #181 / #189)
//! `/v2/stream` is on the public route whitelist, so the upgrade OPENS without
//! a middleware credential — this is the only way a browser device-token client
//! (which cannot set `Authorization`/`X-Device-Id` headers on a WS upgrade) can
//! present its token, via the `hello` frame. The handler is therefore the
//! AUTHORITATIVE gate:
//!
//! - `pre_authorized` is computed from the SAME allow-decision the middleware
//!   uses for every other route (`request_is_authorized`): a local bypass, a
//!   verified password cookie (cookies ARE sent on the upgrade), or a header
//!   device token. These connections stay frictionless — exactly as before.
//! - While NOT authorized, the ONLY frame that does anything is `hello` carrying
//!   a VALID `device_id` + `token`. A `subscribe`/`unsubscribe`/`stop` received
//!   before authorization is IGNORED — no forwarder, no cancel, no channel data.
//! - A token-less `hello` NEVER authorizes a connection that wasn't already
//!   pre-authorized.
//! - An unauthenticated socket that never sends a valid `hello` within
//!   [`AUTH_DEADLINE`] is CLOSED, so it cannot linger.
//! - The token is NEVER logged.

mod envelope;
mod forwarders;

use std::collections::HashMap;
use std::time::Duration;

use actix_web::{web, HttpRequest, Responder};
use actix_ws::Message;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamMap;

use serde::Deserialize;

use self::envelope::{
    decode_client_frame, pong_frame, sys_keepalive_envelope, Channel, ClientFrame, Encoding,
    OutFrame, SUBPROTOCOL_JSON, SUBPROTOCOL_MSGPACK,
};
use self::forwarders::{
    spawn_agent_forwarder, spawn_agent_terminal_forwarder, spawn_feed_forwarder, OutboundTx,
};
use crate::app_state::AppState;
use crate::handlers::agent::events::MAX_BATCH_MS;
use crate::handlers::agent::stop::cancel_session;

/// Bound on EACH per-channel outbound mpsc (RFC §10-Q3). Caps how far ahead one
/// channel's forwarder may run before the WS writer applies backpressure to that
/// channel ALONE — a slow socket no longer lets a bursting channel starve the
/// queue of another, because each channel owns its own bounded buffer and the
/// driver drains them with a fair merge.
const OUTBOUND_BUFFER: usize = 64;

/// Reserved connection-level queue for heartbeat/control frames. A capacity of
/// one intentionally coalesces probes while the socket writer is busy.
const SYS_CHANNEL: &str = "sys";
const SYS_OUTBOUND_BUFFER: usize = 1;

/// Best-effort enqueue for connection-level control traffic. Heartbeats are
/// probes, so retaining an old one is never worth blocking the socket read loop.
fn try_enqueue_sys(sys_tx: &mpsc::Sender<OutFrame>, frame: Option<OutFrame>) {
    if let Some(frame) = frame {
        let _ = sys_tx.try_send(frame);
    }
}

/// WS ping interval. Each tick sends BOTH the protocol-level ping (server-side
/// write probe) and the app-level `sys` keepalive data frame (client-side
/// liveness signal, #533).
///
/// 2s (15s → 5s in #543, then → 2s): the client watchdog cannot detect a dead
/// socket faster than a few missed keepalives, and a user actively watching a
/// running session should not stare at a dead screen for long — at 2s the
/// watchdog resolves in ~6s. The frames are tiny (a ping + a ~50-byte text
/// frame); the cost is negligible even for remote clients. The lotus watchdog
/// ADAPTS its threshold to the observed cadence (3×, clamped), so old-server ×
/// new-client pairings stay safe in both directions.
const PING_INTERVAL: Duration = Duration::from_secs(2);

/// Env var the live integration test sets to SHORTEN the ping interval so the
/// `sys` keepalive cadence is asserted in milliseconds, not 15s. Read once per
/// connection in [`drive`]. Production NEVER sets it.
const PING_INTERVAL_OVERRIDE_ENV: &str = "BAMBOO_WS_PING_INTERVAL_MS";

/// The effective ping interval: the [`PING_INTERVAL`] default unless
/// [`PING_INTERVAL_OVERRIDE_ENV`] is set to a valid millisecond count (test-only).
fn ping_interval() -> Duration {
    std::env::var(PING_INTERVAL_OVERRIDE_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(PING_INTERVAL)
}

/// How long an UNAUTHORIZED connection may stay open before it must present a
/// valid `hello` device token. A connection that opens the (now public) upgrade
/// without a header/cookie credential and never authenticates is CLOSED when
/// this elapses, so an unauthenticated socket can never linger (#189). Once the
/// connection is authorized this deadline is disarmed.
const AUTH_DEADLINE: Duration = Duration::from_secs(10);

/// Env var the live integration test sets to SHORTEN the auth deadline so the
/// "closed when no hello arrives" path is asserted in milliseconds, not 10s. It
/// is read once per connection in [`drive`]. Production NEVER sets it, so the
/// 10s [`AUTH_DEADLINE`] default is unchanged.
const AUTH_DEADLINE_OVERRIDE_ENV: &str = "BAMBOO_WS_AUTH_DEADLINE_MS";

/// The effective unauthorized deadline: the [`AUTH_DEADLINE`] default unless
/// [`AUTH_DEADLINE_OVERRIDE_ENV`] is set to a valid millisecond count (test-only).
fn auth_deadline() -> Duration {
    std::env::var(AUTH_DEADLINE_OVERRIDE_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(AUTH_DEADLINE)
}

/// The verdict for one client frame while a connection is NOT yet authorized.
///
/// Pure decision over the parsed frame (no `AppState`), so the auth-gating
/// contract is unit-testable without a live WS driver: while unauthorized the
/// ONLY frame that can change anything is a `hello`, and only a `hello` carrying
/// a credential is even a candidate to authorize.
#[derive(Debug, PartialEq, Eq)]
enum UnauthorizedAction {
    /// A `hello` carrying `device_id` + `token` — verify it; valid → authorize,
    /// invalid → close.
    VerifyHello,
    /// A token-less `hello` while unauthorized — a harmless no-op that does NOT
    /// grant access (the deadline still governs).
    TokenlessHelloNoop,
    /// Any other frame (`subscribe`/`unsubscribe`/`stop`/unknown) while
    /// unauthorized — IGNORE it (serve no channel data, cancel nothing).
    Ignore,
}

impl UnauthorizedAction {
    /// Classify a parsed frame for an unauthorized connection.
    fn classify(frame: &ClientFrame) -> Self {
        match frame {
            ClientFrame::Hello {
                device_id: Some(_),
                token: Some(_),
            } => UnauthorizedAction::VerifyHello,
            ClientFrame::Hello { .. } => UnauthorizedAction::TokenlessHelloNoop,
            _ => UnauthorizedAction::Ignore,
        }
    }
}

/// What the driver should do with a frame after the auth gate ran.
#[derive(Debug, PartialEq, Eq)]
enum GateOutcome {
    /// The frame was fully handled by the gate (the connection is/was
    /// unauthorized, or it just authorized): keep the socket open, do NOT fall
    /// through to channel dispatch.
    Handled,
    /// An invalid credential was presented: CLOSE the connection.
    Close,
    /// The connection is authorized and this frame should proceed to the normal
    /// channel dispatch (subscribe/unsubscribe/stop/hello-rebind).
    Dispatch,
}

/// The AppState-aware auth gate, factored out of `handle_client_frame` so it is
/// unit-testable WITHOUT a live WS `Session`. It owns the entire `!authorized`
/// decision plus the credential verification, and flips `authorized` to `true`
/// only on a VERIFIED `hello` device token.
///
/// Invariants (the security review hammers these):
/// - While `!*authorized`, a `subscribe`/`unsubscribe`/`stop` returns
///   [`GateOutcome::Handled`] (ignored — never `Dispatch`), so no channel is
///   served and no session cancelled before authorization.
/// - A token-less `hello` NEVER sets `*authorized` when it was `false`.
/// - A credentialed `hello` with a VALID token sets `*authorized = true`.
/// - A credentialed `hello` with an INVALID token returns [`GateOutcome::Close`].
/// - The token is NEVER logged.
async fn apply_auth_gate(
    state: &web::Data<AppState>,
    frame: &ClientFrame,
    authorized: &mut bool,
) -> GateOutcome {
    if *authorized {
        return GateOutcome::Dispatch;
    }

    match UnauthorizedAction::classify(frame) {
        UnauthorizedAction::VerifyHello => {
            let ClientFrame::Hello {
                device_id: Some(device_id),
                token: Some(token),
            } = frame
            else {
                unreachable!("VerifyHello implies a credentialed Hello");
            };
            let config = state.config.read().await.clone();
            if crate::handlers::settings::verify_device_token(&config, device_id, token) {
                *authorized = true;
                // Bind device id for logging. NEVER log the token.
                tracing::debug!("ws_v2: hello verified for device {device_id}; authorized");
                GateOutcome::Handled
            } else {
                tracing::warn!(
                    "ws_v2: hello rejected — invalid device credential for {device_id}; closing"
                );
                GateOutcome::Close
            }
        }
        UnauthorizedAction::TokenlessHelloNoop => {
            // A token-less hello does NOT authorize a non-pre-authorized
            // connection. Keep the socket open; the deadline still governs.
            tracing::debug!("ws_v2: token-less hello while unauthorized — not granting access");
            GateOutcome::Handled
        }
        UnauthorizedAction::Ignore => {
            // subscribe/unsubscribe/stop/unknown before auth: serve nothing,
            // cancel nothing. Tolerate hello-after-subscribe ordering.
            tracing::debug!("ws_v2: ignoring frame on unauthorized connection");
            GateOutcome::Handled
        }
    }
}

/// Query parameters for the `GET /v2/stream` upgrade.
#[derive(Debug, Default, Deserialize)]
pub struct StreamQuery {
    /// Token-coalescing window in milliseconds for `agent.{sid}` channels.
    /// `0` (default) = no coalescing (desktop). Mobile passes e.g. `50`.
    #[serde(default)]
    pub batch_ms: u64,
}

/// Negotiate the wire [`Encoding`] from the upgrade's `Sec-WebSocket-Protocol`
/// header (v2-P3, §5.3 / §7.2). Returns the chosen encoding AND the subprotocol
/// token the server must ECHO on the upgrade response (per RFC 6455 the server
/// echoes the single selected subprotocol).
///
/// - Offers including `bamboo.v2.msgpack` → `(Msgpack, Some("bamboo.v2.msgpack"))`.
/// - Offers including only `bamboo.v2` → `(Json, Some("bamboo.v2"))`.
/// - No (recognized) subprotocol offered → `(Json, None)` — today's behavior is
///   preserved byte-for-byte for clients that offer nothing.
///
/// `bamboo.v2.msgpack` wins if BOTH are offered (the client opted into binary).
fn negotiate_encoding(req: &HttpRequest) -> (Encoding, Option<&'static str>) {
    let offered = req
        .headers()
        .get(actix_web::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // The header is a comma-separated list of subprotocol tokens.
    let mut has_msgpack = false;
    let mut has_json = false;
    for tok in offered.split(',') {
        match tok.trim() {
            SUBPROTOCOL_MSGPACK => has_msgpack = true,
            SUBPROTOCOL_JSON => has_json = true,
            _ => {}
        }
    }
    if has_msgpack {
        (Encoding::Msgpack, Some(SUBPROTOCOL_MSGPACK))
    } else if has_json {
        (Encoding::Json, Some(SUBPROTOCOL_JSON))
    } else {
        (Encoding::Json, None)
    }
}

/// `GET /v2/stream` — upgrade to the unified WS multiplex.
///
/// The upgrade itself is PUBLIC (whitelisted in `is_public_access_route`), so a
/// browser device-token client can open the socket and authenticate via `hello`
/// (it cannot set headers on a WS upgrade). Auth is enforced in `drive`: a
/// connection that the middleware WOULD have allowed is `pre_authorized` here
/// (local bypass / verified password cookie / header device token), via the same
/// `request_is_authorized` allow-decision the middleware uses; everything else
/// must present a verified `hello` before any channel is served, on a deadline.
///
/// The wire [`Encoding`] is negotiated from `Sec-WebSocket-Protocol` (v2-P3):
/// `bamboo.v2.msgpack` → binary MessagePack; otherwise JSON text (default). The
/// selected subprotocol is ECHOED on the upgrade response per RFC 6455.
pub async fn handler(
    state: web::Data<AppState>,
    query: web::Query<StreamQuery>,
    req: HttpRequest,
    body: web::Payload,
) -> actix_web::Result<impl Responder> {
    // Clamp the untrusted `batch_ms` the same way the v1 SSE handler does.
    let batch_ms = query.batch_ms.min(MAX_BATCH_MS);

    // Negotiate the wire encoding from the offered subprotocols (v2-P3).
    let (encoding, selected_subprotocol) = negotiate_encoding(&req);

    // Pre-authorize exactly the connections the middleware would have allowed on
    // a gated route: local bypass, a verified password cookie (sent on the
    // upgrade), or a header device token. This preserves every existing client
    // with ZERO change; a remote browser device-token client is NOT pre-auth and
    // must send a valid `hello`.
    let pre_authorized = {
        let config = state.config.read().await.clone();
        crate::handlers::settings::request_is_authorized(&req, &config)
    };

    let (mut response, session, msg_stream) = actix_ws::handle(&req, body)?;

    // Echo the selected subprotocol on the upgrade RESPONSE (RFC 6455). A client
    // that offered no recognized subprotocol gets none, keeping the legacy
    // handshake byte-for-byte unchanged.
    if let Some(proto) = selected_subprotocol {
        response.headers_mut().insert(
            actix_web::http::header::SEC_WEBSOCKET_PROTOCOL,
            actix_web::http::header::HeaderValue::from_static(proto),
        );
    }

    actix_web::rt::spawn(drive(
        state,
        session,
        msg_stream,
        batch_ms,
        pre_authorized,
        encoding,
    ));

    Ok(response)
}

/// The per-connection driver: owns the WS `session` (write) + `msg_stream`
/// (read), the shared outbound mpsc, the per-channel forwarder handles, and the
/// keepalive ping timer.
async fn drive(
    state: web::Data<AppState>,
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
    batch_ms: u64,
    pre_authorized: bool,
    encoding: Encoding,
) {
    // Per-channel outbound (RFC §10-Q3): every subscribed channel owns its OWN
    // bounded queue. `forwarders` keeps the task handle (for unsubscribe/teardown
    // abort) and `queues` keeps the matching receiver in a `StreamMap` the driver
    // drains with a fair (randomized-start) merge, so a burst on one channel cannot
    // head-of-line another at the socket. The two maps are keyed by the SAME
    // channel id and mutated together (see [`subscribe`] / unsubscribe / teardown).
    let mut forwarders: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut queues: StreamMap<String, ReceiverStream<OutFrame>> = StreamMap::new();
    let (sys_tx, sys_rx) = mpsc::channel::<OutFrame>(SYS_OUTBOUND_BUFFER);
    queues.insert(SYS_CHANNEL.to_string(), ReceiverStream::new(sys_rx));
    let mut ping = tokio::time::interval(ping_interval());
    ping.tick().await; // skip the immediate tick

    // Authorization state (#189). Seeded from the upgrade-time decision so local
    // / cookie / header clients are authorized immediately; everything else must
    // present a valid `hello` before any channel is served.
    let mut authorized = pre_authorized;
    // The unauthorized-deadline timer. Pinned + biased to fire while `!authorized`
    // and disarmed once the connection authorizes, so an unauthenticated socket
    // is closed but an authorized one runs indefinitely.
    let auth_deadline = tokio::time::sleep(auth_deadline());
    tokio::pin!(auth_deadline);

    loop {
        tokio::select! {
            // While unauthorized, close the socket once the deadline elapses. The
            // `!authorized` guard disarms this arm the moment the connection
            // authorizes (a never-completing branch is simply never selected).
            _ = &mut auth_deadline, if !authorized => {
                tracing::debug!("ws_v2: closing unauthorized connection after auth deadline");
                break;
            }
            // Drain the per-channel queues to the WS with a fair merge. The
            // `StreamMap` randomizes its poll start each call, so no single channel's
            // queue is favored — a bursting channel cannot starve another at the
            // socket. `(channel, frame)` is yielded; only the frame goes on the
            // wire. The frame is ALREADY encoded per the connection's `Encoding`
            // (the forwarder did the encode), so the driver only picks the WS frame
            // type: `session.text` for a JSON `Text`, `session.binary` for a
            // MessagePack `Binary`. If the write fails the peer is gone — break and
            // tear down.
            Some((_ch, frame)) = queues.next() => {
                let write = match frame {
                    OutFrame::Text(s) => session.text(s).await,
                    OutFrame::Binary(b) => session.binary(b).await,
                };
                if write.is_err() {
                    break;
                }
            }
            // Keepalive. Liveness is write-driven (a dead peer surfaces as a
            // failed `ping`/`text` write); we do NOT track Pong arrivals or run a
            // pong timeout — same one-directional keepalive contract as the v1 SSE
            // stream.
            //
            // TWO frames go out per tick (#533):
            // - a protocol-level ping — the server-side write probe (unchanged);
            // - an app-level `{ch:"sys", control:{type:"keepalive"}}` DATA frame —
            //   browsers never expose protocol pings to JS, so without a data
            //   frame a client on a half-open socket (sleep/wake, NAT idle
            //   eviction) has NO observable liveness signal and sits "open"
            //   forever after the server tears down. The sys frame is what the
            //   lotus watchdog keys on to force a reconnect. Old clients ignore
            //   the unknown `sys` channel, so this is backward-compatible.
            _ = ping.tick() => {
                if session.ping(b"").await.is_err() {
                    break;
                }
                // Only an AUTHORIZED connection gets the data frame: an
                // unauthenticated socket is served nothing (same posture as
                // channel data) and is closed by the auth deadline anyway.
                if authorized {
                    if let Some(frame) = sys_keepalive_envelope().encode(encoding) {
                        let write = match frame {
                            OutFrame::Text(s) => session.text(s).await,
                            OutFrame::Binary(b) => session.binary(b).await,
                        };
                        if write.is_err() {
                            break;
                        }
                    }
                }
            }
            // Client frames. In JSON mode the client sends TEXT frames; in msgpack
            // mode it sends BINARY frames. A frame carrying the inbound bytes for the
            // ACTIVE encoding is decoded + dispatched; a frame of the other kind is
            // ignored (a Binary frame in JSON mode, a Text frame in msgpack mode),
            // exactly as a malformed frame is — it never tears down the connection.
            msg = msg_stream.next() => {
                match msg {
                    // The inbound frame type that matches the active encoding.
                    Some(Ok(Message::Text(text))) if encoding == Encoding::Json => {
                        let keep_open = handle_client_bytes(
                            &state, &mut forwarders, &mut queues,
                            &sys_tx, batch_ms, encoding, text.as_bytes(), &mut authorized,
                        )
                        .await;
                        if !keep_open {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) if encoding == Encoding::Msgpack => {
                        let keep_open = handle_client_bytes(
                            &state, &mut forwarders, &mut queues,
                            &sys_tx, batch_ms, encoding, &bytes, &mut authorized,
                        )
                        .await;
                        if !keep_open {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if session.pong(&bytes).await.is_err() {
                            break;
                        }
                    }
                    // A frame of the WRONG kind for the active encoding (a Binary
                    // frame in JSON mode / a Text frame in msgpack mode), plus Pong /
                    // Continuation / Nop: ignore, never disconnect.
                    Some(Ok(Message::Text(_)))
                    | Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Pong(_)))
                    | Some(Ok(Message::Continuation(_)))
                    | Some(Ok(Message::Nop)) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::debug!("ws_v2: message stream error: {e}");
                        break;
                    }
                }
            }
            else => break,
        }
    }

    // Clean teardown: abort every forwarder so no orphaned broadcast reader
    // survives the connection, and drop every per-channel queue (clearing the
    // `StreamMap` drops each receiver).
    for (_ch, handle) in forwarders.drain() {
        handle.abort();
    }
    queues.clear();
    let _ = session.close(None).await;
}

/// Decode one inbound frame's bytes per the connection's [`Encoding`] (serde_json
/// for `Json`, rmp-serde for `Msgpack`) and dispatch the resulting [`ClientFrame`].
/// A malformed body logs and is ignored — it NEVER tears down the connection, in
/// EITHER encoding (the msgpack decode error is treated exactly like a malformed
/// JSON text frame).
///
/// This is the thin decode step in front of [`handle_client_frame`]; the dispatch
/// and auth logic is encoding-agnostic (the SAME `ClientFrame` flows through both
/// paths), so the JSON behavior is byte-for-byte unchanged.
///
/// Returns `false` to signal the driver to CLOSE the connection (a `hello` that
/// presents an INVALID device credential); `true` to keep it open.
async fn handle_client_bytes(
    state: &web::Data<AppState>,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    queues: &mut StreamMap<String, ReceiverStream<OutFrame>>,
    sys_tx: &mpsc::Sender<OutFrame>,
    batch_ms: u64,
    encoding: Encoding,
    bytes: &[u8],
    authorized: &mut bool,
) -> bool {
    let frame: ClientFrame = match decode_client_frame(encoding, bytes) {
        Ok(f) => f,
        Err(e) => {
            tracing::debug!("ws_v2: ignoring malformed client frame: {e}");
            return true;
        }
    };
    handle_client_frame(
        state, forwarders, queues, sys_tx, batch_ms, encoding, frame, authorized,
    )
    .await
}

/// Dispatch one decoded client frame. A malformed/unknown frame logs and is
/// ignored — it never tears down the connection.
///
/// `authorized` is the per-connection auth state (#189). While it is `false` the
/// ONLY frame that does anything is a `hello` carrying a VALID device token (it
/// flips `authorized` to `true`); EVERY other frame — including a
/// `subscribe`/`unsubscribe`/`stop` — is IGNORED, so a remote unauthenticated
/// connection gets NO channel data and cancels nothing. The driver's deadline
/// closes a socket that never authorizes.
///
/// Returns `false` to signal the driver to CLOSE the connection (a `hello` that
/// presents an INVALID device credential); `true` to keep it open.
async fn handle_client_frame(
    state: &web::Data<AppState>,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    queues: &mut StreamMap<String, ReceiverStream<OutFrame>>,
    sys_tx: &mpsc::Sender<OutFrame>,
    batch_ms: u64,
    encoding: Encoding,
    frame: ClientFrame,
    authorized: &mut bool,
) -> bool {
    // Auth gate (#189). Until the connection is authorized, no frame may serve a
    // channel or cancel a session — the only frame that can change anything is a
    // `hello` that carries a verifiable device credential.
    match apply_auth_gate(state, &frame, authorized).await {
        GateOutcome::Handled => return true,
        GateOutcome::Close => return false,
        GateOutcome::Dispatch => {}
    }

    // Like all server data, heartbeat acknowledgements are emitted only after
    // authorization. They carry no channel envelope but share the connection's
    // reserved sys queue and single socket writer.
    if frame == ClientFrame::Ping {
        // Drop-on-full is deliberate. The next client heartbeat retries, and
        // the read loop must never wait behind a slow socket writer.
        try_enqueue_sys(sys_tx, pong_frame(encoding));
        return true;
    }

    // Authorized path: full dispatch.
    match frame {
        ClientFrame::Hello { device_id, token } => {
            // Already authorized. A credentialed hello re-verifies as identity
            // binding; an invalid one still closes. A token-less hello on an
            // already-authorized connection is a harmless no-op.
            match (device_id, token) {
                (Some(device_id), Some(token)) => {
                    let config = state.config.read().await.clone();
                    if crate::handlers::settings::verify_device_token(&config, &device_id, &token) {
                        // NEVER log the token.
                        tracing::debug!("ws_v2: hello verified for device {device_id}");
                    } else {
                        tracing::warn!(
                            "ws_v2: hello rejected — invalid device credential for {device_id}; closing"
                        );
                        return false;
                    }
                }
                _ => {
                    tracing::debug!("ws_v2: token-less hello on authorized connection (no-op)");
                }
            }
        }
        ClientFrame::Subscribe { ch, since } => {
            subscribe(state, forwarders, queues, batch_ms, encoding, &ch, since).await;
        }
        ClientFrame::Unsubscribe { ch } => {
            if let Some(handle) = forwarders.remove(&ch) {
                handle.abort();
                // Drop this channel's queue too: removing it from the StreamMap
                // drops the receiver so the (now-aborted) forwarder's sender is
                // dead and no stale frame can leak onto the socket.
                queues.remove(&ch);
                tracing::debug!("ws_v2: unsubscribed {ch}");
            }
        }
        ClientFrame::Stop { session_id } => {
            let cancelled = cancel_session(state, &session_id).await;
            tracing::debug!("ws_v2: stop {session_id} -> cancelled={cancelled}");
        }
        ClientFrame::Ping => unreachable!("authorized ping handled above"),
        ClientFrame::Unknown => {
            tracing::debug!("ws_v2: ignoring unknown client frame type");
        }
    }
    true
}

/// Subscribe to a channel, replacing any existing forwarder for the same `ch`
/// (a re-subscribe with a new cursor aborts the old one first, so there is never
/// a duplicate reader on one channel).
async fn subscribe(
    state: &web::Data<AppState>,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    queues: &mut StreamMap<String, ReceiverStream<OutFrame>>,
    batch_ms: u64,
    encoding: Encoding,
    ch: &str,
    since: Option<u64>,
) {
    let Some(channel) = Channel::parse(ch) else {
        tracing::debug!("ws_v2: ignoring subscribe to unknown channel {ch}");
        return;
    };

    // Replace any prior forwarder for this exact channel id, dropping its queue
    // (a re-subscribe with a new cursor must not leave the old queue behind).
    if let Some(old) = forwarders.remove(ch) {
        old.abort();
    }
    queues.remove(ch);

    // This channel's OWN bounded outbound queue (RFC §10-Q3). The forwarder owns
    // the sender; the driver drains the receiver via the fair `StreamMap` merge.
    // Carries already-encoded [`OutFrame`]s (Text/Binary per the connection's
    // `Encoding`).
    let (out_tx, out_rx) = mpsc::channel::<OutFrame>(OUTBOUND_BUFFER);
    let out_tx: OutboundTx = out_tx;

    let handle = match channel {
        Channel::Feed => {
            // Subscribe FIRST so events written during journal replay are buffered
            // in the ring and delivered in the live phase (no gap) — exactly the
            // v1 SSE handoff discipline.
            let receiver = state.account_sink.subscribe();
            let latest_at_start = state.account_sink.latest_seq();
            let events_dir = state.account_sink.events_dir().to_path_buf();
            let since = since.unwrap_or(0);
            spawn_feed_forwarder(
                out_tx,
                encoding,
                receiver,
                events_dir,
                since,
                latest_at_start,
            )
        }
        Channel::Agent(sid) => {
            // The session must exist; otherwise ignore (parity with the v1
            // events handler's 404, but here we just skip the subscribe).
            if state.session_store.get_index_entry(&sid).await.is_none() {
                tracing::debug!("ws_v2: ignoring subscribe to unknown session {sid}");
                return;
            }

            let sender = state.get_session_event_sender(&sid).await;
            let receiver = sender.subscribe();
            // Keep the notification relay running for this session (parity with
            // the v1 events handler).
            state.ensure_notification_relay(&sid, sender.clone());

            // Snapshot the runner for critical-event + budget replay (mirrors
            // `events/handler.rs:80-89`).
            let runner_snapshot = {
                let runners = state.agent_runners.read().await;
                runners.get(&sid).cloned()
            };
            let budget_event_to_replay = runner_snapshot
                .as_ref()
                .and_then(|runner| runner.last_budget_event.clone());
            let critical_events_to_replay: Vec<_> = runner_snapshot
                .as_ref()
                .map(|runner| runner.last_critical_events.clone())
                .unwrap_or_default();

            // Match the v1 SSE late-subscribe contract. A session that
            // completed while this socket was half-open must replay cached
            // state and a synthesized terminal exactly once, rather than open
            // a live receiver that will never publish another event.
            let runner_status = runner_snapshot.as_ref().map(|runner| runner.status.clone());
            if can_attempt_terminal_replay(runner_status.as_ref(), &receiver) {
                if let Some(terminal_event) =
                    crate::handlers::agent::events::terminal_event_if_ready(
                        state,
                        &sid,
                        runner_status.clone(),
                    )
                    .await
                {
                    // Storage and descendant checks above are asynchronous. A
                    // new runner can be reserved while they run, before it has
                    // emitted its first broadcast event. Re-read the runner so
                    // that Pending/Running wins over the stale terminal
                    // snapshot even while the receiver is still empty.
                    // We subscribed before the async storage/child checks. If a
                    // live event arrived meanwhile, preserve its ordering by
                    // handing the still-buffered receiver to the live forwarder
                    // instead of sending a synthetic terminal ahead of it.
                    if current_runner_allows_terminal_replay(state, &sid, &receiver).await {
                        return finish_subscribe(
                            forwarders,
                            queues,
                            ch,
                            out_rx,
                            spawn_agent_terminal_forwarder(
                                out_tx,
                                encoding,
                                ch.to_string(),
                                budget_event_to_replay,
                                critical_events_to_replay,
                                terminal_event,
                            ),
                            since,
                        );
                    }
                }
            }

            spawn_agent_forwarder(
                state.clone(),
                sid.clone(),
                out_tx,
                encoding,
                ch.to_string(),
                receiver,
                budget_event_to_replay,
                critical_events_to_replay,
                batch_ms,
            )
        }
    };
    finish_subscribe(forwarders, queues, ch, out_rx, handle, since);
}

fn can_attempt_terminal_replay(
    runner_status: Option<&crate::app_state::AgentStatus>,
    receiver: &tokio::sync::broadcast::Receiver<bamboo_agent_core::AgentEvent>,
) -> bool {
    matches!(
        runner_status,
        None | Some(crate::app_state::AgentStatus::Completed)
            | Some(crate::app_state::AgentStatus::Cancelled)
            | Some(crate::app_state::AgentStatus::Error(_))
    ) && receiver.is_empty()
}

/// Re-read the runner after asynchronous terminal checks. This is deliberately
/// a separate helper so the subscribe race can be covered without relying on
/// scheduler timing: a runner reserved during storage I/O must invalidate the
/// stale terminal snapshot before the one-shot forwarder is installed.
async fn current_runner_allows_terminal_replay(
    state: &web::Data<AppState>,
    session_id: &str,
    receiver: &tokio::sync::broadcast::Receiver<bamboo_agent_core::AgentEvent>,
) -> bool {
    let current_runner_status = {
        let runners = state.agent_runners.read().await;
        runners.get(session_id).map(|runner| runner.status.clone())
    };
    can_attempt_terminal_replay(current_runner_status.as_ref(), receiver)
}

fn finish_subscribe(
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    queues: &mut StreamMap<String, ReceiverStream<OutFrame>>,
    ch: &str,
    out_rx: mpsc::Receiver<OutFrame>,
    handle: tokio::task::JoinHandle<()>,
    since: Option<u64>,
) {
    forwarders.insert(ch.to_string(), handle);
    // Register this channel's queue receiver into the fair-merge drain. Keyed by
    // the SAME channel id as `forwarders`, so unsubscribe/teardown drops both.
    queues.insert(ch.to_string(), ReceiverStream::new(out_rx));
    tracing::debug!("ws_v2: subscribed {ch} (since={since:?})");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use bamboo_config::{AccessControlConfig, DeviceCredential};
    use tempfile::tempdir;

    fn hello(device_id: Option<&str>, token: Option<&str>) -> ClientFrame {
        ClientFrame::Hello {
            device_id: device_id.map(str::to_string),
            token: token.map(str::to_string),
        }
    }

    #[test]
    fn sys_queue_is_best_effort_and_drops_when_full() {
        let (tx, mut rx) = mpsc::channel(SYS_OUTBOUND_BUFFER);
        let first = OutFrame::Text("first".into());
        try_enqueue_sys(&tx, Some(first.clone()));
        // Must return immediately and leave the already-queued frame intact.
        try_enqueue_sys(&tx, Some(OutFrame::Text("newer".into())));
        assert_eq!(rx.try_recv().unwrap(), first);
        assert!(rx.try_recv().is_err());
    }

    // ── Subprotocol negotiation (v2-P3) ───────────────────────────────────────

    fn negotiate_for(header: Option<&str>) -> (Encoding, Option<&'static str>) {
        let mut req = actix_web::test::TestRequest::default();
        if let Some(h) = header {
            req = req.insert_header((actix_web::http::header::SEC_WEBSOCKET_PROTOCOL, h));
        }
        negotiate_encoding(&req.to_http_request())
    }

    #[test]
    fn negotiate_encoding_branches() {
        // No header / empty → JSON default, NO echo (unchanged for old clients).
        assert_eq!(negotiate_for(None), (Encoding::Json, None));
        assert_eq!(negotiate_for(Some("")), (Encoding::Json, None));
        // Explicit JSON subprotocol → JSON, echo it.
        assert_eq!(
            negotiate_for(Some("bamboo.v2")),
            (Encoding::Json, Some(SUBPROTOCOL_JSON))
        );
        // Msgpack offered → Msgpack, echo it.
        assert_eq!(
            negotiate_for(Some("bamboo.v2.msgpack")),
            (Encoding::Msgpack, Some(SUBPROTOCOL_MSGPACK))
        );
        // Multi-offer: msgpack preferred regardless of order, and whitespace is trimmed.
        assert_eq!(
            negotiate_for(Some("bamboo.v2.msgpack, bamboo.v2")),
            (Encoding::Msgpack, Some(SUBPROTOCOL_MSGPACK))
        );
        assert_eq!(
            negotiate_for(Some("  bamboo.v2 ,  bamboo.v2.msgpack ")),
            (Encoding::Msgpack, Some(SUBPROTOCOL_MSGPACK))
        );
        // Unknown-only offer → JSON, and NO bogus echo (echoing a non-offered
        // subprotocol would violate RFC 6455).
        assert_eq!(
            negotiate_for(Some("some.other.proto")),
            (Encoding::Json, None)
        );
    }

    // ── Pure classification (no AppState) ─────────────────────────────────────

    #[test]
    fn classify_unauthorized_frame_actions() {
        // A credentialed hello is the only authorize candidate.
        assert_eq!(
            UnauthorizedAction::classify(&hello(Some("d"), Some("t"))),
            UnauthorizedAction::VerifyHello
        );
        // A token-less hello (any missing field) is a no-op, never an authorizer.
        assert_eq!(
            UnauthorizedAction::classify(&hello(None, None)),
            UnauthorizedAction::TokenlessHelloNoop
        );
        assert_eq!(
            UnauthorizedAction::classify(&hello(Some("d"), None)),
            UnauthorizedAction::TokenlessHelloNoop
        );
        assert_eq!(
            UnauthorizedAction::classify(&hello(None, Some("t"))),
            UnauthorizedAction::TokenlessHelloNoop
        );
        // Every channel-touching / control frame is IGNORED while unauthorized.
        assert_eq!(
            UnauthorizedAction::classify(&ClientFrame::Subscribe {
                ch: "feed".into(),
                since: None
            }),
            UnauthorizedAction::Ignore
        );
        assert_eq!(
            UnauthorizedAction::classify(&ClientFrame::Unsubscribe { ch: "feed".into() }),
            UnauthorizedAction::Ignore
        );
        assert_eq!(
            UnauthorizedAction::classify(&ClientFrame::Stop {
                session_id: "s".into()
            }),
            UnauthorizedAction::Ignore
        );
        assert_eq!(
            UnauthorizedAction::classify(&ClientFrame::Ping),
            UnauthorizedAction::Ignore
        );
        assert_eq!(
            UnauthorizedAction::classify(&ClientFrame::Unknown),
            UnauthorizedAction::Ignore
        );
    }

    // ── AppState-aware gate (no live Session) ─────────────────────────────────

    async fn app_state_with_device() -> (web::Data<AppState>, DeviceCredential, String) {
        let dir = tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let (cred, token) = crate::handlers::settings::issue_device_token("test-device");
        {
            let mut config = state.config.write().await;
            config.access_control = Some(AccessControlConfig {
                password_enabled: false,
                password_hash: None,
                password_salt: None,
                updated_at: None,
                devices: vec![cred.clone()],
            });
        }
        (state, cred, token)
    }

    #[actix_web::test]
    async fn subscribe_while_unauthorized_is_ignored_and_stays_unauthorized() {
        let (state, _cred, _token) = app_state_with_device().await;
        let mut authorized = false;
        let frame = ClientFrame::Subscribe {
            ch: "feed".into(),
            since: None,
        };
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        // The driver keeps the socket open but does NOT dispatch (no forwarder).
        assert_eq!(outcome, GateOutcome::Handled);
        assert!(!authorized, "a subscribe must never authorize a connection");
    }

    #[actix_web::test]
    async fn stop_while_unauthorized_is_ignored() {
        let (state, _cred, _token) = app_state_with_device().await;
        let mut authorized = false;
        let frame = ClientFrame::Stop {
            session_id: "sess".into(),
        };
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        assert_eq!(outcome, GateOutcome::Handled);
        assert!(!authorized);
    }

    #[actix_web::test]
    async fn valid_hello_authorizes() {
        let (state, cred, token) = app_state_with_device().await;
        let mut authorized = false;
        let frame = hello(Some(&cred.device_id), Some(&token));
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        assert_eq!(outcome, GateOutcome::Handled);
        assert!(authorized, "a valid hello must authorize the connection");
    }

    #[actix_web::test]
    async fn invalid_hello_closes_and_does_not_authorize() {
        let (state, cred, _token) = app_state_with_device().await;
        let mut authorized = false;
        let frame = hello(Some(&cred.device_id), Some("bd1_wrongwrongwrong"));
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        assert_eq!(outcome, GateOutcome::Close);
        assert!(!authorized, "an invalid hello must never authorize");
    }

    #[actix_web::test]
    async fn tokenless_hello_does_not_authorize_unauthorized_connection() {
        let (state, _cred, _token) = app_state_with_device().await;
        let mut authorized = false;
        let frame = hello(None, None);
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        assert_eq!(outcome, GateOutcome::Handled);
        assert!(
            !authorized,
            "a token-less hello must NEVER authorize a non-pre-authorized connection"
        );
    }

    #[actix_web::test]
    async fn pre_authorized_connection_dispatches_subscribe() {
        let (state, _cred, _token) = app_state_with_device().await;
        // Pre-authorized (local / cookie / header equivalent): the gate passes
        // every frame straight through to channel dispatch.
        let mut authorized = true;
        let frame = ClientFrame::Subscribe {
            ch: "feed".into(),
            since: None,
        };
        let outcome = apply_auth_gate(&state, &frame, &mut authorized).await;
        assert_eq!(outcome, GateOutcome::Dispatch);
        assert!(authorized);
    }

    #[actix_web::test]
    async fn ping_is_ignored_before_auth_and_enqueued_after_auth() {
        let (state, _cred, _token) = app_state_with_device().await;
        let mut forwarders = HashMap::new();
        let mut queues = StreamMap::new();
        let (sys_tx, mut sys_rx) = mpsc::channel(SYS_OUTBOUND_BUFFER);

        let mut authorized = false;
        assert!(
            handle_client_frame(
                &state,
                &mut forwarders,
                &mut queues,
                &sys_tx,
                0,
                Encoding::Json,
                ClientFrame::Ping,
                &mut authorized,
            )
            .await
        );
        assert!(
            sys_rx.try_recv().is_err(),
            "unauthorized ping must be silent"
        );

        authorized = true;
        assert!(
            handle_client_frame(
                &state,
                &mut forwarders,
                &mut queues,
                &sys_tx,
                0,
                Encoding::Json,
                ClientFrame::Ping,
                &mut authorized,
            )
            .await
        );
        assert_eq!(
            sys_rx.try_recv().unwrap(),
            OutFrame::Text(r#"{"type":"pong"}"#.into())
        );
    }

    #[actix_web::test]
    async fn client_cannot_subscribe_or_unsubscribe_reserved_sys_queue() {
        let (state, _cred, _token) = app_state_with_device().await;
        let mut forwarders = HashMap::new();
        let mut queues = StreamMap::new();
        let (sys_tx, sys_rx) = mpsc::channel(SYS_OUTBOUND_BUFFER);
        queues.insert(SYS_CHANNEL.to_string(), ReceiverStream::new(sys_rx));
        let mut authorized = true;

        for frame in [
            ClientFrame::Subscribe {
                ch: SYS_CHANNEL.into(),
                since: None,
            },
            ClientFrame::Unsubscribe {
                ch: SYS_CHANNEL.into(),
            },
        ] {
            assert!(
                handle_client_frame(
                    &state,
                    &mut forwarders,
                    &mut queues,
                    &sys_tx,
                    0,
                    Encoding::Json,
                    frame,
                    &mut authorized,
                )
                .await
            );
            assert!(
                queues.contains_key(SYS_CHANNEL),
                "client channel operations must not remove reserved sys queue"
            );
        }

        try_enqueue_sys(&sys_tx, pong_frame(Encoding::Json));
        let (channel, frame) = queues.next().await.expect("sys queue remains drainable");
        assert_eq!(channel, SYS_CHANNEL);
        assert_eq!(frame, OutFrame::Text(r#"{"type":"pong"}"#.into()));
    }

    #[test]
    fn queued_agent_event_blocks_synthetic_terminal_replay() {
        let (tx, rx) = tokio::sync::broadcast::channel(4);
        assert!(can_attempt_terminal_replay(None, &rx));
        tx.send(bamboo_agent_core::AgentEvent::Token {
            content: "first-live-token".into(),
        })
        .expect("receiver is subscribed");
        assert!(
            !can_attempt_terminal_replay(None, &rx),
            "a queued live frame must win the race with synthetic terminal replay"
        );
    }

    #[test]
    fn pending_or_running_runner_blocks_synthetic_terminal_replay() {
        let (_tx, rx) = tokio::sync::broadcast::channel(4);
        assert!(!can_attempt_terminal_replay(
            Some(&crate::app_state::AgentStatus::Pending),
            &rx,
        ));
        assert!(!can_attempt_terminal_replay(
            Some(&crate::app_state::AgentStatus::Running),
            &rx,
        ));
    }

    #[actix_web::test]
    async fn current_runner_reread_blocks_stale_terminal_snapshot() {
        let (state, _cred, _token) = app_state_with_device().await;
        let (_tx, rx) = tokio::sync::broadcast::channel(4);
        let session_id = "runner-reserved-during-terminal-check";

        assert!(
            current_runner_allows_terminal_replay(&state, session_id, &rx).await,
            "the initial no-runner snapshot permits a terminal check"
        );

        {
            let mut runners = state.agent_runners.write().await;
            let mut runner = crate::app_state::AgentRunner::new();
            runner.status = crate::app_state::AgentStatus::Running;
            runners.insert(session_id.to_string(), runner);
        }

        assert!(
            !current_runner_allows_terminal_replay(&state, session_id, &rx).await,
            "the post-await re-read must observe the newly Running runner"
        );
    }

    #[actix_web::test]
    async fn expired_startup_subscribe_waits_for_locked_live_reconcile() {
        let dir = tempdir().expect("temporary app data");
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let session_id = "ws-admission-startup-race";
        let channel = format!("agent.{session_id}");
        let mut session = bamboo_agent_core::Session::new(session_id, "test-model");
        session.add_message(bamboo_agent_core::Message::user("slow startup"));
        crate::handlers::agent::events::mark_pending_turn(&mut session);
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        );
        state.save_session(&mut session).await;

        // Exercise the real WS admission function, not just the forwarder. An
        // execute owner appearing around the async terminal checks must force a
        // live subscription until the locked exact-work CAS can run.
        let startup_guard =
            crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);
        let mut forwarders = HashMap::new();
        let mut queues = StreamMap::new();
        subscribe(
            &state,
            &mut forwarders,
            &mut queues,
            0,
            Encoding::Json,
            &channel,
            None,
        )
        .await;

        assert!(
            tokio::time::timeout(Duration::from_millis(350), queues.next())
                .await
                .is_err(),
            "an in-flight execute owner must prevent a one-shot terminal"
        );
        drop(startup_guard);

        let (_, terminal_event) = tokio::time::timeout(Duration::from_secs(2), queues.next())
            .await
            .expect("locked reconcile emits startup failure")
            .expect("agent queue remains installed");
        let OutFrame::Text(terminal_event) = terminal_event else {
            panic!("JSON subscription must emit text frames");
        };
        let terminal_event: serde_json::Value =
            serde_json::from_str(&terminal_event).expect("terminal event JSON");
        assert_eq!(terminal_event["event"]["type"], "error");
        assert!(terminal_event["event"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("was not started")));

        let (_, terminal_control) = tokio::time::timeout(Duration::from_secs(2), queues.next())
            .await
            .expect("terminal control follows failure")
            .expect("agent queue drains terminal control");
        let OutFrame::Text(terminal_control) = terminal_control else {
            panic!("JSON subscription must emit text frames");
        };
        let terminal_control: serde_json::Value =
            serde_json::from_str(&terminal_control).expect("terminal control JSON");
        assert_eq!(terminal_control["control"]["type"], "terminal");

        let stored = state
            .storage
            .load_session(session_id)
            .await
            .expect("load session")
            .expect("stored session");
        assert_eq!(stored.last_run_status().as_deref(), Some("error"));
        assert!(crate::handlers::agent::events::startup_work_id(&stored).is_none());
    }
}
