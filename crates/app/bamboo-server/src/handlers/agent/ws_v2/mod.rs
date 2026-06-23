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
//! receiver (per-channel lag independence; §10-Q3). Forwarders push fully-built
//! envelope text frames onto one shared `mpsc` that the driver drains to the WS
//! `session`. A per-channel `JoinHandle` lets `unsubscribe`/teardown abort
//! exactly that forwarder, leaving no orphaned broadcast reader.
//!
//! ## DEFERRED (later slices; see PR / §5.3)
//! - `execute` / `approve` over control (handler refactors) — clients keep REST.
//! - per-device tokens / `hello` validation / `/v2/pair` (v2-P2) — `hello` is
//!   accepted and ignored; auth is the scope middleware only.
//! - msgpack subprotocol (v2-P3) — JSON text frames only.

mod envelope;
mod forwarders;

use std::collections::HashMap;
use std::time::Duration;

use actix_web::{web, HttpRequest, Responder};
use actix_ws::Message;
use futures::StreamExt;
use tokio::sync::mpsc;

use serde::Deserialize;

use self::envelope::{Channel, ClientFrame};
use self::forwarders::{spawn_agent_forwarder, spawn_feed_forwarder, OutboundTx};
use crate::app_state::AppState;
use crate::handlers::agent::events::MAX_BATCH_MS;
use crate::handlers::agent::stop::cancel_session;

/// Bound on the shared outbound mpsc. Caps how far ahead the (collectively)
/// forwarders may run before the WS writer applies backpressure.
const OUTBOUND_BUFFER: usize = 256;

/// WS ping interval (~15s), mirroring the v1 SSE `[KEEPALIVE]` cadence.
const PING_INTERVAL: Duration = Duration::from_secs(15);

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
/// Behind the same access-password scope middleware as `/api/v1` (registered in
/// `routes/agent.rs`); `local_bypass` keeps desktop loopback frictionless.
pub async fn handler(
    state: web::Data<AppState>,
    query: web::Query<StreamQuery>,
    req: HttpRequest,
    body: web::Payload,
) -> actix_web::Result<impl Responder> {
    // Clamp the untrusted `batch_ms` the same way the v1 SSE handler does.
    let batch_ms = query.batch_ms.min(MAX_BATCH_MS);

    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;

    actix_web::rt::spawn(drive(state, session, msg_stream, batch_ms));

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
) {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(OUTBOUND_BUFFER);
    let mut forwarders: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await; // skip the immediate tick

    loop {
        tokio::select! {
            // Drain forwarder output to the WS. If the write fails the peer is
            // gone — break and tear down.
            Some(text) = out_rx.recv() => {
                if session.text(text).await.is_err() {
                    break;
                }
            }
            // Keepalive ping.
            _ = ping.tick() => {
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
            // Client frames.
            msg = msg_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_text(
                            &state, &out_tx, &mut forwarders, &mut session, batch_ms, &text,
                        )
                        .await;
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
    // survives the connection.
    for (_ch, handle) in forwarders.drain() {
        handle.abort();
    }
    let _ = session.close(None).await;
}

/// Parse + dispatch one client text frame. A malformed/unknown frame logs and is
/// ignored — it never tears down the connection.
async fn handle_client_text(
    state: &web::Data<AppState>,
    out_tx: &OutboundTx,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    _session: &mut actix_ws::Session,
    batch_ms: u64,
    text: &str,
) {
    let frame: ClientFrame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(e) => {
            tracing::debug!("ws_v2: ignoring malformed client frame: {e}");
            return;
        }
    };

    match frame {
        ClientFrame::Hello { .. } => {
            // v2-P1: auth is the scope middleware only. Accept + ignore (token
            // validation lands in v2-P2).
            tracing::debug!("ws_v2: hello frame accepted (P1 no-op)");
        }
        ClientFrame::Subscribe { ch, since } => {
            subscribe(state, out_tx, forwarders, batch_ms, &ch, since).await;
        }
        ClientFrame::Unsubscribe { ch } => {
            if let Some(handle) = forwarders.remove(&ch) {
                handle.abort();
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
}

/// Subscribe to a channel, replacing any existing forwarder for the same `ch`
/// (a re-subscribe with a new cursor aborts the old one first, so there is never
/// a duplicate reader on one channel).
async fn subscribe(
    state: &web::Data<AppState>,
    out_tx: &OutboundTx,
    forwarders: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    batch_ms: u64,
    ch: &str,
    since: Option<u64>,
) {
    let Some(channel) = Channel::parse(ch) else {
        tracing::debug!("ws_v2: ignoring subscribe to unknown channel {ch}");
        return;
    };

    // Replace any prior forwarder for this exact channel id.
    if let Some(old) = forwarders.remove(ch) {
        old.abort();
    }

    let handle = match channel {
        Channel::Feed => {
            // Subscribe FIRST so events written during journal replay are buffered
            // in the ring and delivered in the live phase (no gap) — exactly the
            // v1 SSE handoff discipline.
            let receiver = state.account_sink.subscribe();
            let latest_at_start = state.account_sink.latest_seq();
            let events_dir = state.account_sink.events_dir().to_path_buf();
            let since = since.unwrap_or(0);
            spawn_feed_forwarder(out_tx.clone(), receiver, events_dir, since, latest_at_start)
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
                out_tx.clone(),
                ch.to_string(),
                receiver,
                budget_event_to_replay,
                critical_events_to_replay,
                batch_ms,
            )
        }
    };

    forwarders.insert(ch.to_string(), handle);
    tracing::debug!("ws_v2: subscribed {ch} (since={since:?})");
}
