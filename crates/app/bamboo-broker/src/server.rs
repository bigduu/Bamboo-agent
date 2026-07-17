//! WebSocket server: a thin network shell over [`BrokerCore`].
//!
//! One task per connection: a `Hello` handshake (Bearer token + bind to a
//! session mailbox), then a `select` loop that pumps client frames into the core
//! and forwards the subscription stream back out as `Message` frames. Reachable
//! on any address (`0.0.0.0:PORT`) so workers deployed via subprocess / Docker /
//! SSH can dial home over the network.
//!
//! TLS (`wss://`, #48): [`BrokerServer::with_tls`] wraps every accepted
//! `TcpStream` in a `tokio_rustls::TlsAcceptor` before the WS upgrade, using
//! the SAME cert/key-loading helper `bamboo-subagent`'s own `WsServer::bind_tls`
//! uses (`bamboo_subagent::transport::build_server_config`) — one hardened
//! implementation, two call sites. Plaintext `ws://` (no `with_tls` call)
//! stays the default — zero regression for loopback/trusted-link use.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};

use crate::core::{BrokerCore, PushItem};
use crate::error::{BrokerError, BrokerResult};
use crate::proto::{BrokerFrame, ClientFrame};

/// Un-keyed (single-bucket) token-bucket limiter for one connection's
/// `Deliver` rate (#53).
type DeliverLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Bounded, configurable limits [`BrokerServer`] enforces against a
/// malicious/misbehaving client (#53 — connection-flood and message-flood DoS
/// defense). Defaults are deliberately generous:
///
/// - The broker is reachable on any address by design — subprocess/Docker/SSH
///   workers dial home over the network — so unlike bamboo-server's HTTP
///   sidecar (loopback-only, see `is_loopback_bind`), there is no "trusted
///   local caller" to exempt here. Loopback and remote connections share the
///   one limit.
/// - A single connection relaying a live Run's LLM token stream
///   (`serve::handle_run`) can legitimately emit 100+ `Deliver`s/sec — so
///   `messages_per_second`/`message_burst` are sized well above that, and
///   exceeding them backpressures (delays) the connection rather than
///   dropping it, so a brief legitimate burst is absorbed instead of
///   punished.
#[derive(Debug, Clone, Copy)]
pub struct BrokerLimits {
    /// Max concurrent accepted WebSocket connections. Beyond this, new
    /// connections are dropped immediately (no WS handshake attempted) until
    /// a slot frees up — bounds the "open thousands of connections" memory
    /// exhaustion vector.
    pub max_connections: usize,
    /// Sustained per-connection `Deliver`-frame rate (frames/sec) — the
    /// specific frame kind that writes into a mailbox and can flood disk (the
    /// concrete vector in #53). Other frame kinds (`Ack`/`Cancel`/
    /// `ListConnected`/`Subscribe`) are not gated by this bucket.
    pub messages_per_second: NonZeroU32,
    /// Burst allowance layered on `messages_per_second` (token-bucket
    /// capacity) — absorbs short bursts (e.g. several event streams
    /// overlapping) without delaying them.
    pub message_burst: NonZeroU32,
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            // SAFETY-of-intent: literals are non-zero; `NonZeroU32::new(..).unwrap()`
            // in a const fn context is not stable here, so this is a plain runtime
            // unwrap of a value we control.
            messages_per_second: NonZeroU32::new(500).expect("500 is non-zero"),
            message_burst: NonZeroU32::new(1000).expect("1000 is non-zero"),
        }
    }
}

/// The network front-end. Holds the routing [`BrokerCore`], the expected
/// Bearer token, and the DoS-defense [`BrokerLimits`]; clones cheaply
/// (everything behind `Arc`).
pub struct BrokerServer {
    core: Arc<BrokerCore>,
    token: String,
    limits: BrokerLimits,
    /// One permit per currently-accepted connection, capacity ==
    /// `limits.max_connections`. Held for the lifetime of each connection's
    /// task (#53).
    connection_slots: Arc<Semaphore>,
    /// TLS acceptor for terminating `wss://` before the WS upgrade — `None`
    /// (the default) keeps the historical bare-`TcpStream` `ws://` behavior.
    /// Set via [`with_tls`](Self::with_tls). #48.
    tls: Option<TlsAcceptor>,
}

impl BrokerServer {
    /// Serve with [`BrokerLimits::default`].
    pub fn new(core: Arc<BrokerCore>, token: impl Into<String>) -> Self {
        Self::with_limits(core, token, BrokerLimits::default())
    }

    /// Serve with explicit connection/message limits (#53).
    pub fn with_limits(
        core: Arc<BrokerCore>,
        token: impl Into<String>,
        limits: BrokerLimits,
    ) -> Self {
        Self {
            core,
            token: token.into(),
            connection_slots: Arc::new(Semaphore::new(limits.max_connections)),
            limits,
            tls: None,
        }
    }

    /// Terminate TLS (`wss://`) on every accepted connection before the WS
    /// upgrade, built from a PEM `cert_file`/`key_file` pair (#48). Reuses
    /// `bamboo_subagent::transport::build_server_config` (rustls + explicit
    /// `ring` provider) — the exact cert/key-loading logic
    /// `bamboo-subagent`'s own `WsServer::bind_tls` uses — so the workspace
    /// resolves a single crypto provider and never hits the rustls 0.23
    /// no-default-provider panic.
    ///
    /// **Fail-fast:** a missing/unreadable/unparseable cert or key returns
    /// `Err` immediately — this never silently falls back to plaintext.
    pub fn with_tls(mut self, cert_file: &Path, key_file: &Path) -> BrokerResult<Self> {
        let server_config = bamboo_subagent::transport::build_server_config(cert_file, key_file)
            .map_err(BrokerError::Tls)?;
        self.tls = Some(TlsAcceptor::from(Arc::new(server_config)));
        Ok(self)
    }

    /// `true` once [`with_tls`](Self::with_tls) has configured a TLS acceptor
    /// — i.e. this server terminates `wss://`, not bare `ws://`.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Accept connections forever, one task each. Returns only on a listener
    /// error (per-connection errors are logged and isolated).
    ///
    /// Enforces `limits.max_connections`: once the pool of connection slots is
    /// exhausted, a newly-accepted TCP stream is dropped immediately — no WS
    /// handshake is attempted for a connection we're refusing, so an
    /// over-the-cap flood costs us only an `accept()` + drop, not a full
    /// handshake dance. #53.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> BrokerResult<()> {
        loop {
            let (stream, peer) = listener
                .accept()
                .await
                .map_err(|e| BrokerError::Transport(format!("accept: {e}")))?;
            let server = Arc::clone(&self);
            match Arc::clone(&server.connection_slots).try_acquire_owned() {
                Ok(permit) => {
                    tokio::spawn(async move {
                        let _permit = permit; // held for the connection's lifetime
                                              // The TLS handshake (when configured) runs inside this
                                              // spawned task, not on the accept loop, so a slow/hostile
                                              // client can't stall acceptance of other connections —
                                              // mirrors `bamboo_subagent::transport::WsServer::serve`'s
                                              // same tradeoff (#48).
                        let result = match &server.tls {
                            Some(acceptor) => match acceptor.accept(stream).await {
                                Ok(tls_stream) => server.handle_conn(tls_stream).await,
                                Err(e) => Err(BrokerError::Tls(format!(
                                    "tls accept handshake ({peer}): {e}"
                                ))),
                            },
                            None => server.handle_conn(stream).await,
                        };
                        if let Err(e) = result {
                            tracing::debug!("broker connection ended: {e}");
                        }
                    });
                }
                Err(_) => {
                    tracing::warn!(
                        max_connections = server.limits.max_connections,
                        %peer,
                        "broker: max_connections reached — rejecting connection"
                    );
                    drop(stream);
                }
            }
        }
    }

    /// Perform the WS upgrade + serve loop over an already-TLS-terminated (or
    /// plaintext) stream. Generic over the stream type so ONE implementation
    /// serves both bare `TcpStream` (`ws://`) and `TlsStream<TcpStream>`
    /// (`wss://`) connections (#48) — mirrors
    /// `bamboo_subagent::transport::handle_conn`'s same genericization.
    async fn handle_conn<S>(&self, stream: S) -> BrokerResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let ws = accept_async(stream)
            .await
            .map_err(|e| BrokerError::Transport(format!("ws accept: {e}")))?;
        let (mut sink, mut source) = ws.split();
        // One token bucket per connection for `Deliver` frames (#53) — sized
        // per `limits`, generous enough for a single live event stream (see
        // [`BrokerLimits`]'s doc comment).
        let deliver_limiter: DeliverLimiter = RateLimiter::direct(
            Quota::per_second(self.limits.messages_per_second)
                .allow_burst(self.limits.message_burst),
        );

        // 1. Handshake — the first frame must be a Hello with a valid token.
        let (session_id, role) = match read_client_frame(&mut source).await? {
            Some(ClientFrame::Hello { agent, token }) => {
                if token != self.token {
                    let _ = send(
                        &mut sink,
                        BrokerFrame::Error {
                            reason: "invalid token".into(),
                            id: None,
                        },
                    )
                    .await;
                    return Err(BrokerError::Auth("invalid token".into()));
                }
                // Keep the role: it makes this connection discoverable as a live
                // actor of role X via the bus's subscriber table (Phase 3).
                (agent.session_id, agent.role)
            }
            Some(_) => {
                let _ = send(
                    &mut sink,
                    BrokerFrame::Error {
                        reason: "expected hello".into(),
                        id: None,
                    },
                )
                .await;
                return Err(BrokerError::Protocol("expected hello first".into()));
            }
            None => return Ok(()), // closed before handshake
        };
        send(&mut sink, BrokerFrame::Welcome).await?;

        // 2. Serve: client frames in, subscription stream out.
        let mut sub_rx: Option<mpsc::UnboundedReceiver<PushItem>> = None;
        let outcome = loop {
            tokio::select! {
                biased;
                frame = read_client_frame(&mut source) => {
                    match frame {
                        Ok(Some(ClientFrame::Deliver { to, message })) => {
                            // Per-connection Deliver throttle (#53): backpressure
                            // (delay) rather than drop/disconnect, so a legitimate
                            // burst (e.g. overlapping event streams) just waits
                            // instead of losing the connection or the message.
                            //
                            // NOTE: this `.await` runs inside the `select!`'s
                            // frame-read arm, so while it's pending the sibling
                            // `pushed = next_pushed(...)` arm is NOT polled — a
                            // connection that is BOTH delivering and subscribed
                            // (the protocol allows one connection to do both)
                            // has its own inbound pushes queue in the unbounded
                            // channel for the throttle's duration. Given the
                            // generous defaults and the "backpressure, don't
                            // punish" intent this is an accepted tradeoff, not a
                            // bug — flagged here so it isn't rediscovered as a
                            // surprise (review finding on #491/#53).
                            deliver_limiter.until_ready().await;
                            // Keep the id BEFORE `core.deliver` takes `&message`,
                            // so a rejection (e.g. `MailboxFull`) can be
                            // correlated back to THIS `Deliver` — otherwise the
                            // sender only ever sees a generic receipt timeout,
                            // never the specific reason (review finding #1 on
                            // #491/#53).
                            let msg_id = message.id.clone();
                            match self.core.deliver(&to, &message).await {
                                Ok(id) => {
                                    if send(&mut sink, BrokerFrame::Delivered { id }).await.is_err() {
                                        break Ok(());
                                    }
                                }
                                Err(e) => {
                                    let _ = send(
                                        &mut sink,
                                        BrokerFrame::Error {
                                            reason: e.to_string(),
                                            id: Some(msg_id),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                        Ok(Some(ClientFrame::Subscribe)) => match self.core.subscribe(&session_id, role.as_deref()).await {
                            Ok(rx) => sub_rx = Some(rx),
                            Err(e) => {
                                let _ = send(&mut sink, BrokerFrame::Error { reason: e.to_string(), id: None }).await;
                            }
                        },
                        Ok(Some(ClientFrame::Ack { id })) => {
                            if let Err(e) = self.core.ack(&session_id, &id).await {
                                let _ = send(&mut sink, BrokerFrame::Error { reason: e.to_string(), id: None }).await;
                            }
                        }
                        // A second Hello is meaningless mid-session; ignore.
                        Ok(Some(ClientFrame::Hello { .. })) => {}
                        // Out-of-band, fire-and-forget cancel: signal the target's
                        // live subscriber (if any). No Delivered receipt, no mailbox
                        // write — pure control signal. #50.
                        Ok(Some(ClientFrame::Cancel { to, correlation_id })) => {
                            self.core.cancel(&to, &correlation_id).await;
                        }
                        // Presence query: which actors are connected serving `role`.
                        // The subscriber table IS the registry (Phase 3).
                        Ok(Some(ClientFrame::ListConnected { role })) => {
                            let ids = self.core.connected_by_role(&role).await;
                            if send(&mut sink, BrokerFrame::Connected { ids }).await.is_err() {
                                break Ok(());
                            }
                        }
                        Ok(None) => break Ok(()),   // client closed
                        Err(e) => break Err(e),
                    }
                }
                pushed = next_pushed(&mut sub_rx) => {
                    match pushed {
                        Some(PushItem::Message(m)) => {
                            if send(&mut sink, BrokerFrame::Message { message: m }).await.is_err() {
                                break Ok(());
                            }
                        }
                        Some(PushItem::Cancel(correlation_id)) => {
                            if send(&mut sink, BrokerFrame::Cancel { correlation_id }).await.is_err() {
                                break Ok(());
                            }
                        }
                        None => sub_rx = None, // subscription channel closed
                    }
                }
            }
        };

        self.core.unsubscribe(&session_id).await;
        outcome
    }
}

/// Await the next pushed message, or never resolve when not subscribed (so the
/// `select` arm is inert until a `Subscribe` arrives).
async fn next_pushed(rx: &mut Option<mpsc::UnboundedReceiver<PushItem>>) -> Option<PushItem> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

/// Read the next client frame, skipping ping/pong/binary. `Ok(None)` on close.
/// Generic over the underlying stream (plain `TcpStream` or
/// `TlsStream<TcpStream>`, #48) so plaintext and TLS connections share one
/// implementation.
async fn read_client_frame<S>(
    source: &mut SplitStream<WebSocketStream<S>>,
) -> BrokerResult<Option<ClientFrame>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match source.next().await {
            Some(Ok(Message::Text(t))) => {
                return ClientFrame::from_text(&t)
                    .map(Some)
                    .map_err(|e| BrokerError::Protocol(format!("bad client frame: {e}")));
            }
            Some(Ok(Message::Close(_))) | None => return Ok(None),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(BrokerError::Transport(format!("ws: {e}"))),
        }
    }
}

async fn send<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: BrokerFrame,
) -> BrokerResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    sink.send(Message::text(frame.to_text()))
        .await
        .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
}
