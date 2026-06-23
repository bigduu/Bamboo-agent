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
//! drains every per-channel queue with a fair (rotating-start) merge, so a burst
//! on one channel can no longer head-of-line another at the socket (RFC §10-Q3 —
//! see `forwarders.rs` for the honest fairness guarantee). A per-channel
//! `JoinHandle` lets `unsubscribe`/teardown abort exactly that forwarder and drop
//! its queue, leaving no orphaned broadcast reader.
//!
//! ## DEFERRED (later slices; see PR / §5.3)
//! - `execute` / `approve` over control (handler refactors) — clients keep REST.
//! - msgpack subprotocol (v2-P3) — JSON text frames only.
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

use self::envelope::{Channel, ClientFrame};
use self::forwarders::{spawn_agent_forwarder, spawn_feed_forwarder, OutboundTx};
use crate::app_state::AppState;
use crate::handlers::agent::events::MAX_BATCH_MS;
use crate::handlers::agent::stop::cancel_session;

/// Bound on EACH per-channel outbound mpsc (RFC §10-Q3). Caps how far ahead one
/// channel's forwarder may run before the WS writer applies backpressure to that
/// channel ALONE — a slow socket no longer lets a bursting channel starve the
/// queue of another, because each channel owns its own bounded buffer and the
/// driver drains them with a fair merge.
const OUTBOUND_BUFFER: usize = 64;

/// WS ping interval (~15s), mirroring the v1 SSE `[KEEPALIVE]` cadence.
const PING_INTERVAL: Duration = Duration::from_secs(15);

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

/// The AppState-aware auth gate, factored out of `handle_client_text` so it is
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

/// `GET /v2/stream` — upgrade to the unified WS multiplex.
///
/// The upgrade itself is PUBLIC (whitelisted in `is_public_access_route`), so a
/// browser device-token client can open the socket and authenticate via `hello`
/// (it cannot set headers on a WS upgrade). Auth is enforced in `drive`: a
/// connection that the middleware WOULD have allowed is `pre_authorized` here
/// (local bypass / verified password cookie / header device token), via the same
/// `request_is_authorized` allow-decision the middleware uses; everything else
/// must present a verified `hello` before any channel is served, on a deadline.
pub async fn handler(
    state: web::Data<AppState>,
    query: web::Query<StreamQuery>,
    req: HttpRequest,
    body: web::Payload,
) -> actix_web::Result<impl Responder> {
    // Clamp the untrusted `batch_ms` the same way the v1 SSE handler does.
    let batch_ms = query.batch_ms.min(MAX_BATCH_MS);

    // Pre-authorize exactly the connections the middleware would have allowed on
    // a gated route: local bypass, a verified password cookie (sent on the
    // upgrade), or a header device token. This preserves every existing client
    // with ZERO change; a remote browser device-token client is NOT pre-auth and
    // must send a valid `hello`.
    let pre_authorized = {
        let config = state.config.read().await.clone();
        crate::handlers::settings::request_is_authorized(&req, &config)
    };

    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;

    actix_web::rt::spawn(drive(state, session, msg_stream, batch_ms, pre_authorized));

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
) {
    // Per-channel outbound (RFC §10-Q3): every subscribed channel owns its OWN
    // bounded queue. `forwarders` keeps the task handle (for unsubscribe/teardown
    // abort) and `queues` keeps the matching receiver in a `StreamMap` the driver
    // drains with a fair (rotating-start) merge, so a burst on one channel cannot
    // head-of-line another at the socket. The two maps are keyed by the SAME
    // channel id and mutated together (see [`subscribe`] / unsubscribe / teardown).
    let mut forwarders: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut queues: StreamMap<String, ReceiverStream<String>> = StreamMap::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);
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
            // `StreamMap` rotates its poll start each call, so no single channel's
            // queue is favored — a bursting channel cannot starve another at the
            // socket. `(channel, frame)` is yielded; only the frame goes on the
            // wire. If the write fails the peer is gone — break and tear down.
            Some((_ch, text)) = queues.next() => {
                if session.text(text).await.is_err() {
                    break;
                }
            }
            // Keepalive ping. Liveness is write-driven (a dead peer surfaces as a
            // failed `ping`/`text` write); we do NOT track Pong arrivals or run a
            // pong timeout — same one-directional keepalive contract as the v1 SSE
            // stream.
            _ = ping.tick() => {
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
            // Client frames.
            msg = msg_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let keep_open = handle_client_text(
                            &state, &mut forwarders, &mut queues, &mut session,
                            batch_ms, &text, &mut authorized,
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
                    // Pong / Binary (no msgpack in P1) / Continuation / Nop: ignore.
                    Some(Ok(Message::Pong(_)))
                    | Some(Ok(Message::Binary(_)))
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

/// Parse + dispatch one client text frame. A malformed/unknown frame logs and is
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
async fn handle_client_text(
    state: &web::Data<AppState>,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    queues: &mut StreamMap<String, ReceiverStream<String>>,
    _session: &mut actix_ws::Session,
    batch_ms: u64,
    text: &str,
    authorized: &mut bool,
) -> bool {
    let frame: ClientFrame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(e) => {
            tracing::debug!("ws_v2: ignoring malformed client frame: {e}");
            return true;
        }
    };

    // Auth gate (#189). Until the connection is authorized, no frame may serve a
    // channel or cancel a session — the only frame that can change anything is a
    // `hello` that carries a verifiable device credential.
    match apply_auth_gate(state, &frame, authorized).await {
        GateOutcome::Handled => return true,
        GateOutcome::Close => return false,
        GateOutcome::Dispatch => {}
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
            subscribe(state, forwarders, queues, batch_ms, &ch, since).await;
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
    queues: &mut StreamMap<String, ReceiverStream<String>>,
    batch_ms: u64,
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
    let (out_tx, out_rx) = mpsc::channel::<String>(OUTBOUND_BUFFER);
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
            spawn_feed_forwarder(out_tx, receiver, events_dir, since, latest_at_start)
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

            spawn_agent_forwarder(
                state.clone(),
                sid.clone(),
                out_tx,
                ch.to_string(),
                receiver,
                budget_event_to_replay,
                critical_events_to_replay,
                batch_ms,
            )
        }
    };

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
}
