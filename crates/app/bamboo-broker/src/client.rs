//! Broker client. Connects, does the `Hello` handshake, then runs a background
//! reader that demuxes incoming frames into a `messages` stream and a
//! `delivered` receipt stream — so one connection can both deliver and
//! subscribe (the parent does both: deliver an Ask, subscribe for the Reply).
//!
//! TLS (`wss://`, #48): a bare `ws://` endpoint always worked unencrypted; a
//! `wss://` endpoint now works TWO ways —
//! - **CA-signed cert** (Let's Encrypt, a corporate/internal CA already
//!   installed in the OS trust store, …): zero configuration.
//!   [`BrokerClient::connect`] negotiates TLS against the OS's native root
//!   store (`tokio-tungstenite`'s `rustls-tls-native-roots` feature).
//! - **Self-signed cert** (the common homelab/cross-network quick-start):
//!   [`BrokerClient::connect_with_tls`] takes an explicit
//!   `rustls::ClientConfig` — build one with
//!   [`client_config_trusting_cert`] (or
//!   `bamboo_subagent::transport::client_config_trusting_cert` directly) to
//!   trust exactly the worker/broker's own cert with no OS trust-store
//!   changes.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bamboo_subagent::{AgentRef, InboxMessage, MsgId};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;

use crate::error::{BrokerError, BrokerResult};
use crate::proto::{BrokerFrame, ClientFrame};

/// Build a rustls [`rustls::ClientConfig`] that trusts exactly the
/// certificate(s) in `cert_file` (PEM) — for connecting to a broker or
/// broker-agent serving a self-signed cert over `wss://` (#48). Thin wrapper
/// over `bamboo_subagent::transport::client_config_trusting_cert` (same
/// helper `ChildClient`'s direct-WS path already uses) that maps its
/// `Result<_, String>` into [`BrokerError::Tls`].
pub fn client_config_trusting_cert(cert_file: &Path) -> BrokerResult<rustls::ClientConfig> {
    bamboo_subagent::transport::client_config_trusting_cert(cert_file).map_err(BrokerError::Tls)
}

pub(crate) type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

/// Upper bound on how long [`BrokerClient::deliver`] waits for the broker's
/// `Delivered` receipt before giving up. The receipt only means the broker
/// durably accepted the `Deliver` (not that the worker replied), so it should
/// arrive promptly; 30s is a generous cap that still guarantees a caller can
/// never hang indefinitely if the broker dies after receiving `Deliver`.
const DELIVER_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

/// Each phase of explicit client shutdown is bounded independently. Sending a
/// Close frame can stall behind a wedged transport, and a reader can fail to
/// observe cooperative cancellation, so neither phase may keep a caller alive
/// indefinitely.
pub(crate) const CLIENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Send and flush a WebSocket Close frame without trusting the transport to
/// make progress forever. Shared by the ordinary and multiplexed client
/// ownership shapes.
pub(crate) async fn send_close(sink: &mut WsSink) -> BrokerResult<()> {
    match tokio::time::timeout(CLIENT_SHUTDOWN_TIMEOUT, sink.send(Message::Close(None))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(BrokerError::Transport(format!("send websocket close: {e}"))),
        Err(_) => Err(BrokerError::Transport(
            "timed out sending websocket close".into(),
        )),
    }
}

/// Why the WebSocket reader stopped. Keep this deliberately low-cardinality:
/// the supervisor logs the category, never frame contents, credentials, actor
/// ids, or endpoint details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderExit {
    IntentionalShutdown,
    PeerClose,
    PeerEof,
    TransportError,
}

impl ReaderExit {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IntentionalShutdown => "intentional_shutdown",
            Self::PeerClose => "peer_close",
            Self::PeerEof => "peer_eof",
            Self::TransportError => "transport_error",
        }
    }
}

/// Authoritative ownership of the background reader. This value moves with
/// the connection when a [`BrokerClient`] becomes a multiplexed client, so no
/// public client shape can detach the read half from its owner.
///
/// The async shutdown path cooperatively cancels and awaits the reader. `Drop`
/// is the non-async safety net: it marks the stop intentional, signals
/// cancellation, and directly aborts the reader task (not merely the
/// supervisor that owns its `JoinHandle`). Aborting the reader drops the split
/// WebSocket source and therefore releases the underlying transport.
pub(crate) struct ReaderLifecycle {
    cancellation: CancellationToken,
    reader_abort: tokio::task::AbortHandle,
    supervisor: Option<tokio::task::JoinHandle<()>>,
    intentional_shutdown: Arc<AtomicBool>,
}

impl ReaderLifecycle {
    pub(crate) fn mark_intentional_shutdown(&self) {
        self.intentional_shutdown.store(true, Ordering::SeqCst);
    }

    fn begin_intentional_shutdown(&self) {
        self.mark_intentional_shutdown();
        self.cancellation.cancel();
    }

    /// Cooperatively stop the reader and await its supervisor. If cooperative
    /// cleanup misses its bound, directly abort the reader and give the
    /// supervisor one final bounded chance to observe that outcome.
    pub(crate) async fn shutdown(&mut self) {
        self.begin_intentional_shutdown();
        let Some(mut supervisor) = self.supervisor.take() else {
            return;
        };

        if tokio::time::timeout(CLIENT_SHUTDOWN_TIMEOUT, &mut supervisor)
            .await
            .is_ok()
        {
            return;
        }

        self.reader_abort.abort();
        if tokio::time::timeout(CLIENT_SHUTDOWN_TIMEOUT, &mut supervisor)
            .await
            .is_err()
        {
            // The reader has already been aborted. This only prevents a
            // pathological supervisor from becoming another detached task.
            supervisor.abort();
        }
    }
}

impl Drop for ReaderLifecycle {
    fn drop(&mut self) {
        self.begin_intentional_shutdown();
        self.reader_abort.abort();
    }
}

/// One demuxed serve event from [`BrokerClient::next_message_or_cancel`]: either
/// the next inbound message or the next out-of-band cancel. The inner `Option` is
/// `None` once that lane's channel closes (the reader exited). #45.
pub enum ServeEvent {
    /// Next inbound message (`None` once the connection closes).
    Message(Option<InboxMessage>),
    /// Next out-of-band cancel correlation id (`None` once the cancel lane closes).
    Cancel(Option<MsgId>),
}

/// A connected broker client bound to one session mailbox.
pub struct BrokerClient {
    /// Declared first so Drop cancels the reader before the transport sink and
    /// response receivers are released.
    reader: ReaderLifecycle,
    sink: WsSink,
    messages: mpsc::UnboundedReceiver<InboxMessage>,
    delivered: mpsc::UnboundedReceiver<MsgId>,
    /// Correlated rejections for an in-flight `Deliver` (e.g. `MailboxFull`),
    /// demuxed independently of `delivered` so `deliver()` can distinguish
    /// "the broker turned this down" from "no receipt arrived in time" — see
    /// [`BrokerClient::deliver_with_receipt_timeout`]. #491/#53.
    errors: mpsc::UnboundedReceiver<(MsgId, String)>,
    /// Out-of-band cancel signals (the timed-out ask's correlation id), demuxed
    /// by the background reader independently of `messages` — so a Cancel reaches
    /// the worker even while its work loop is blocked on an in-flight run. #50.
    cancels: mpsc::UnboundedReceiver<MsgId>,
    /// Answers to [`ClientFrame::ListConnected`] — the connected actor ids of a
    /// queried role (Phase 3 presence query). One reply per request; `&mut self`
    /// on `list_connected` keeps requests serialized.
    connected: mpsc::UnboundedReceiver<Vec<String>>,
    /// Cleared by [`reader_supervisor`] the instant the background reader exits
    /// (clean close / panic / cancellation), so callers can tell "no messages
    /// right now" (`next_message() -> None` but still alive) apart from "the
    /// connection died". See [`BrokerClient::reader_alive`].
    reader_alive: Arc<AtomicBool>,
    /// Deterministic unit-test seam for exercising callers' ack-failure paths.
    /// Compiled out of production clients entirely.
    #[cfg(test)]
    fail_next_ack: Arc<AtomicBool>,
}

impl BrokerClient {
    /// Connect to `endpoint` (`ws://host:port` or `wss://…`), authenticate with
    /// `token`, and bind this connection to `agent.session_id`.
    ///
    /// A `wss://` endpoint is negotiated against the OS's native TLS root
    /// store (works with any CA-signed cert, e.g. Let's Encrypt, or a
    /// self-signed cert whose CA the operator already installed locally) — no
    /// extra configuration. To trust a self-signed cert WITHOUT touching the
    /// OS trust store (the common homelab/cross-network quick-start), use
    /// [`connect_with_tls`](Self::connect_with_tls) with a config from
    /// [`client_config_trusting_cert`]. #48.
    pub async fn connect(endpoint: &str, agent: AgentRef, token: &str) -> BrokerResult<Self> {
        Self::connect_with_tls(endpoint, agent, token, None).await
    }

    /// Like [`connect`](Self::connect), but for `wss://` lets the caller
    /// supply a custom rustls [`rustls::ClientConfig`] — e.g. one built by
    /// [`client_config_trusting_cert`] that trusts exactly a self-signed
    /// worker/broker cert. `None` falls back to the OS native root store
    /// (identical to [`connect`](Self::connect)). Ignored for plaintext
    /// `ws://`. #48.
    pub async fn connect_with_tls(
        endpoint: &str,
        agent: AgentRef,
        token: &str,
        tls_config: Option<rustls::ClientConfig>,
    ) -> BrokerResult<Self> {
        let request = endpoint
            .into_client_request()
            .map_err(|e| BrokerError::Transport(format!("bad endpoint '{endpoint}': {e}")))?;
        let connector = tls_config.map(|cfg| Connector::Rustls(Arc::new(cfg)));
        let (ws, _resp) = connect_async_tls_with_config(request, None, false, connector)
            .await
            .map_err(|e| BrokerError::Transport(format!("connect {endpoint}: {e}")))?;
        let (mut sink, mut source) = ws.split();

        // Handshake: send Hello, wait for Welcome (or Error).
        sink.send(Message::text(
            ClientFrame::Hello {
                agent,
                token: token.into(),
            }
            .to_text(),
        ))
        .await
        .map_err(|e| BrokerError::Transport(format!("send hello: {e}")))?;

        loop {
            match source.next().await {
                Some(Ok(Message::Text(t))) => match BrokerFrame::from_text(&t) {
                    Ok(BrokerFrame::Welcome) => break,
                    Ok(BrokerFrame::Error { reason, .. }) => return Err(BrokerError::Auth(reason)),
                    Ok(_) => continue,
                    Err(e) => return Err(BrokerError::Protocol(format!("bad broker frame: {e}"))),
                },
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(BrokerError::Transport(format!("ws: {e}"))),
                None => return Err(BrokerError::Transport("closed during handshake".into())),
            }
        }

        let (msg_tx, messages) = mpsc::unbounded_channel();
        let (del_tx, delivered) = mpsc::unbounded_channel();
        let (err_tx, errors) = mpsc::unbounded_channel();
        let (cancel_tx, cancels) = mpsc::unbounded_channel();
        let (conn_tx, connected) = mpsc::unbounded_channel();
        let cancellation = CancellationToken::new();
        let reader_cancellation = cancellation.clone();
        // The demux loop pushes `Message`/`Delivered`/`Cancel`/`Error` frames
        // into their respective channels. Cancellation is owned by the public
        // client lifecycle, so dropping that owner always reaches this source
        // half directly instead of detaching it behind a supervisor.
        let reader = tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = reader_cancellation.cancelled() => {
                        break ReaderExit::IntentionalShutdown;
                    }
                    frame = source.next() => frame,
                };
                match frame {
                    Some(Ok(Message::Text(t))) => match BrokerFrame::from_text(&t) {
                        Ok(BrokerFrame::Message { message }) => {
                            let _ = msg_tx.send(message);
                        }
                        Ok(BrokerFrame::Delivered { id }) => {
                            let _ = del_tx.send(id);
                        }
                        // Correlated to a specific `Deliver` (e.g.
                        // `MailboxFull`) — route it to `errors` so the waiting
                        // `deliver()` call can see the REASON instead of just
                        // timing out. An uncorrelated `Error` (id: None) post-
                        // handshake has no in-flight waiter to route to (the
                        // handshake is the only pre-`Welcome` request); log it
                        // rather than silently dropping it. #491/#53.
                        Ok(BrokerFrame::Error {
                            reason,
                            id: Some(id),
                        }) => {
                            let _ = err_tx.send((id, reason));
                        }
                        Ok(BrokerFrame::Error { reason, id: None }) => {
                            tracing::warn!(
                                "broker sent an uncorrelated error frame post-handshake: {reason}"
                            );
                        }
                        Ok(BrokerFrame::Cancel { correlation_id }) => {
                            let _ = cancel_tx.send(correlation_id);
                        }
                        Ok(BrokerFrame::Connected { ids }) => {
                            let _ = conn_tx.send(ids);
                        }
                        _ => {}
                    },
                    Some(Ok(Message::Close(_))) => break ReaderExit::PeerClose,
                    Some(Err(_)) => break ReaderExit::TransportError,
                    None => break ReaderExit::PeerEof,
                    _ => {}
                }
            }
        });

        // Supervise the reader so its termination is *observable* instead of
        // silently dropped (issue #52): log every exit kind and flip a shared
        // flag callers can poll. The movable lifecycle below owns cancellation,
        // the reader's AbortHandle, and this supervisor, so neither ordinary
        // clients nor multiplexed clients can detach the source half.
        let reader_alive = Arc::new(AtomicBool::new(true));
        let intentional_shutdown = Arc::new(AtomicBool::new(false));
        let reader_abort = reader.abort_handle();
        let supervisor = tokio::spawn(reader_supervisor(
            reader,
            reader_alive.clone(),
            intentional_shutdown.clone(),
        ));
        let reader = ReaderLifecycle {
            cancellation,
            reader_abort,
            supervisor: Some(supervisor),
            intentional_shutdown,
        };

        Ok(Self {
            reader,
            sink,
            messages,
            delivered,
            errors,
            cancels,
            connected,
            reader_alive,
            #[cfg(test)]
            fail_next_ack: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Gracefully close this client without allowing cleanup to hang.
    ///
    /// The shutdown is intentionally two-stage: first send and flush a
    /// WebSocket Close frame within a small bound, then cooperatively cancel
    /// and await the reader/supervisor within their own bound. If the reader
    /// does not cooperate, its lifecycle directly aborts it. Dropping the
    /// future or the client at any point still runs the same direct-abort Drop
    /// fallback.
    pub async fn close(mut self) -> BrokerResult<()> {
        // Mark intent before sending Close: a fast peer can echo Close and end
        // the reader before `send` returns, and that is still our shutdown,
        // not an unexpected reader death.
        self.reader.mark_intentional_shutdown();
        let close_result = send_close(&mut self.sink).await;

        // Always clean up the source half, even when sending Close failed or
        // timed out. Cleanup itself is bounded and escalates to direct abort.
        self.reader.shutdown().await;
        close_result
    }

    /// Durably enqueue `message` into session `to`'s mailbox; returns the stored
    /// id once the broker confirms (`Delivered`).
    ///
    /// Bounded by [`DELIVER_RECEIPT_TIMEOUT`]: if the broker accepts the
    /// `Deliver` frame but crashes (or stalls) before sending `Delivered`, the
    /// receipt wait fails instead of hanging the caller forever.
    pub async fn deliver(&mut self, to: &str, message: InboxMessage) -> BrokerResult<MsgId> {
        self.deliver_with_receipt_timeout(to, message, DELIVER_RECEIPT_TIMEOUT)
            .await
    }

    /// [`deliver`](Self::deliver) with an explicit receipt-wait bound. Private
    /// so tests can drive the timeout with a tiny value instead of waiting out
    /// the real [`DELIVER_RECEIPT_TIMEOUT`].
    async fn deliver_with_receipt_timeout(
        &mut self,
        to: &str,
        message: InboxMessage,
        receipt_timeout: Duration,
    ) -> BrokerResult<MsgId> {
        let expected = message.id.clone();
        self.send(ClientFrame::Deliver {
            to: to.into(),
            message,
        })
        .await?;
        // CORRELATE the outcome to THIS message's id, racing TWO lanes: the
        // `delivered` receipt (success) and the `errors` lane (an explicit
        // broker rejection, e.g. `MailboxFull` — #491/#53). Both are shared
        // streams on a reused connection: a late arrival from a PRIOR
        // deliver() that already timed out (broker stalled but stayed
        // connected) can still be sitting in either channel, and returning it
        // would hand back a stale, wrong outcome. So skip anything that isn't
        // ours. Deliver is `&mut self`, so only one deliver awaits at a time —
        // every non-matching id is therefore a stale prior outcome, safe to
        // drop. The single timeout bounds the whole correlation loop
        // (deadline, not per-recv). #114.
        let deadline = tokio::time::Instant::now() + receipt_timeout;
        let correlate = async {
            loop {
                tokio::select! {
                    biased;
                    // Bias the rejection lane: an explicit "no" is a clear,
                    // actionable signal (back off) and should win a race
                    // against a coincidentally-arriving stale receipt.
                    err = self.errors.recv() => match err {
                        Some((id, reason)) if id == expected => {
                            return Err(BrokerError::Rejected(reason));
                        }
                        Some(_stale) => continue,
                        None => {
                            return Err(BrokerError::Transport(
                                "connection closed before delivery receipt".into(),
                            ));
                        }
                    },
                    id = self.delivered.recv() => match id {
                        Some(id) if id == expected => return Ok(id),
                        Some(_stale) => continue,
                        None => {
                            return Err(BrokerError::Transport(
                                "connection closed before delivery receipt".into(),
                            ));
                        }
                    },
                }
            }
        };
        match tokio::time::timeout_at(deadline, correlate).await {
            Ok(outcome) => outcome,
            Err(_) => Err(BrokerError::Transport(
                "timed out waiting for delivery receipt from broker".into(),
            )),
        }
    }

    /// Start receiving this client's own mailbox. Pushed messages arrive via
    /// [`next_message`](Self::next_message).
    pub async fn subscribe(&mut self) -> BrokerResult<()> {
        self.send(ClientFrame::Subscribe).await
    }

    /// Ask the broker which actors are currently connected serving `role` — the
    /// bus-native live-actor registry (Phase 3). `&mut self` serializes requests,
    /// so the single `connected` reply is unambiguously ours. Bounded by
    /// [`DELIVER_RECEIPT_TIMEOUT`] so a stalled broker can't hang the caller.
    pub async fn list_connected(&mut self, role: &str) -> BrokerResult<Vec<String>> {
        self.send(ClientFrame::ListConnected { role: role.into() })
            .await?;
        match tokio::time::timeout(DELIVER_RECEIPT_TIMEOUT, self.connected.recv()).await {
            Ok(Some(ids)) => Ok(ids),
            Ok(None) => Err(BrokerError::Transport(
                "connection closed before connected-actors reply".into(),
            )),
            Err(_) => Err(BrokerError::Transport(
                "timed out waiting for connected-actors reply from broker".into(),
            )),
        }
    }

    /// Next pushed message, or `None` once the connection closes.
    ///
    /// A `None` here used to be ambiguous — "no messages right now" vs. "the
    /// reader task died". It still returns `None` in both cases (callers already
    /// handle that), but now it emits a `warn` when the reader is known to have
    /// exited, and [`BrokerClient::reader_alive`] lets a caller tell the two
    /// apart. Happy path (a message arrives) is unchanged.
    pub async fn next_message(&mut self) -> Option<InboxMessage> {
        let msg = self.messages.recv().await;
        if msg.is_none() && !self.reader_alive.load(Ordering::SeqCst) {
            tracing::warn!(
                "broker next_message() returned None: reader task exited (connection closed)"
            );
        }
        msg
    }

    /// Await the next out-of-band cancel (the correlation id of the run to
    /// abort), demuxed independently of `next_message` so it can be received
    /// while the work loop is busy. `None` when the reader has exited. #50.
    pub async fn next_cancel(&mut self) -> Option<MsgId> {
        self.cancels.recv().await
    }

    /// Await whichever arrives first — the next inbound message or the next
    /// out-of-band cancel — over a SINGLE `&mut self` borrow. The concurrent
    /// serve loop races this against its (client-free) completion channel; doing
    /// the message/cancel demux inside one method keeps the two receiver borrows
    /// disjoint here instead of leaking two simultaneous `&mut self` borrows into
    /// the loop's outer `select!` (which the borrow checker rejects), so the loop
    /// can still call `deliver`/`ack` on the same client between events. #45.
    pub async fn next_message_or_cancel(&mut self) -> ServeEvent {
        tokio::select! {
            biased;
            // Bias the cancel lane so an abort is observed promptly even under a
            // steady inbound stream — preserves #50's cancel latency.
            cancel = self.cancels.recv() => ServeEvent::Cancel(cancel),
            msg = self.messages.recv() => {
                if msg.is_none() && !self.reader_alive.load(Ordering::SeqCst) {
                    tracing::warn!(
                        "broker next_message() returned None: reader task exited (connection closed)"
                    );
                }
                ServeEvent::Message(msg)
            }
        }
    }

    /// `true` while the background reader task is still running. Flips to
    /// `false` the moment the reader exits — clean close, panic, or
    /// cancellation — with the cause logged by [`reader_supervisor`]. Use this
    /// after a `next_message() -> None` to distinguish "the connection died"
    /// from "the broker is just idle".
    pub fn reader_alive(&self) -> bool {
        self.reader_alive.load(Ordering::SeqCst)
    }

    /// Acknowledge a processed message so the broker deletes it.
    pub async fn ack(&mut self, id: MsgId) -> BrokerResult<()> {
        #[cfg(test)]
        if self.fail_next_ack.swap(false, Ordering::SeqCst) {
            return Err(BrokerError::Transport("injected broker ack failure".into()));
        }
        self.send(ClientFrame::Ack { id }).await
    }

    /// Arm one synthetic ack failure without perturbing production behavior.
    #[cfg(test)]
    pub(crate) fn fail_next_ack_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.fail_next_ack)
    }

    /// Consume this (already connected + subscribed) client into a multiplexed
    /// request/reply driver so concurrent correlated requests on ONE connection
    /// no longer serialize behind a single exclusive lock. Ownership of the
    /// background reader moves into the mux alongside the sink and channels, so
    /// dropping either client shape terminates the same transport. The mux's
    /// router drains `messages` and routes by `correlation_id`. For a replies-only
    /// connection (e.g. the MCP proxy) where every inbound message is a
    /// correlated reply. #56/#788.
    pub fn into_multiplexed(self, me: AgentRef) -> crate::mux::MultiplexedClient {
        crate::mux::MultiplexedClient::spawn(
            self.reader,
            self.sink,
            self.messages,
            self.delivered,
            self.reader_alive,
            me,
        )
    }

    /// Out-of-band, fire-and-forget cancel: ask the broker to signal session
    /// `to` to abort the run correlated to `correlation_id`. Returns once the
    /// frame is sent (no receipt — it's a control signal, deliberately off the
    /// durable/acked path). #50.
    pub async fn cancel(&mut self, to: &str, correlation_id: &MsgId) -> BrokerResult<()> {
        self.send(ClientFrame::Cancel {
            to: to.into(),
            correlation_id: correlation_id.clone(),
        })
        .await
    }

    async fn send(&mut self, frame: ClientFrame) -> BrokerResult<()> {
        self.sink
            .send(Message::text(frame.to_text()))
            .await
            .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
    }
}

/// Await the reader task and make its exit observable (issue #52).
///
/// - intentional client shutdown: `debug!`
/// - peer Close / EOF / transport error: categorized `warn!`
/// - panic (`JoinError::is_panic`): `error!` with the panic message
/// - unexpected cancellation / abort: `error!`
///
/// In every case the shared `reader_alive` flag is cleared so callers can tell
/// "the connection died" from "no messages right now". This owns the reader
/// handle, so it resolves — and the supervisor task ends — exactly when the
/// reader does: a clean disconnect leaves no leaked task.
async fn reader_supervisor(
    reader: tokio::task::JoinHandle<ReaderExit>,
    reader_alive: Arc<AtomicBool>,
    intentional_shutdown: Arc<AtomicBool>,
) {
    let outcome = reader.await;
    // Best-effort, eventually-consistent death signal: this store races the
    // reader dropping `msg_tx` (which is what makes `next_message()` return
    // `None`), so a caller can observe `None` a beat *before* this flips the
    // flag. That only means a possibly-missed warn on the very first post-death
    // `None`; it is not a correctness guarantee and callers must not rely on
    // `reader_alive` being false the instant `next_message()` returns `None`.
    reader_alive.store(false, Ordering::SeqCst);
    match outcome {
        Err(err) if err.is_panic() => {
            tracing::error!(
                "broker reader task panicked: {}",
                panic_payload_message(err.into_panic())
            );
        }
        Ok(exit) if intentional_shutdown.load(Ordering::SeqCst) => {
            tracing::debug!(
                reader_exit = exit.as_str(),
                "broker reader stopped during intentional client shutdown"
            );
        }
        Err(_) if intentional_shutdown.load(Ordering::SeqCst) => {
            tracing::debug!(
                reader_exit = "aborted",
                "broker reader stopped during intentional client shutdown"
            );
        }
        Ok(ReaderExit::IntentionalShutdown) => {
            // Cancellation is only issued by ReaderLifecycle after it records
            // intent. Keep this defensive arm low-noise if those operations
            // are ever reordered in the future.
            tracing::debug!(
                reader_exit = ReaderExit::IntentionalShutdown.as_str(),
                "broker reader stopped during intentional client shutdown"
            );
        }
        Ok(ReaderExit::PeerClose) => {
            tracing::warn!(
                reader_exit = ReaderExit::PeerClose.as_str(),
                "broker reader stopped after peer Close frame"
            );
        }
        Ok(ReaderExit::PeerEof) => {
            tracing::warn!(
                reader_exit = ReaderExit::PeerEof.as_str(),
                "broker reader stopped after peer EOF"
            );
        }
        Ok(ReaderExit::TransportError) => {
            tracing::warn!(
                reader_exit = ReaderExit::TransportError.as_str(),
                "broker reader stopped after transport error"
            );
        }
        Err(err) => {
            tracing::error!("broker reader task ended unexpectedly (cancelled/aborted): {err}");
        }
    }
}

/// Best-effort stringification of a panic payload (`&'static str`, `String`, or
/// a fallback) for logging — a panic while *reporting* a reader panic would be
/// the worst possible outcome, so this must never itself panic.
fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::{AskBody, AskMode, InboxKind};
    use chrono::Utc;
    use tokio_tungstenite::accept_async;

    fn test_agent(id: &str) -> AgentRef {
        AgentRef {
            session_id: id.into(),
            role: None,
        }
    }

    fn test_ask(from: &str) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: test_agent(from),
            kind: InboxKind::Ask,
            body: serde_json::to_value(AskBody {
                question: "ping".into(),
                mode: AskMode::Query,
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    /// Spawn a raw WS server that completes the client's `Hello`→`Welcome`
    /// handshake, then silently drains every later frame without EVER sending a
    /// `Delivered` receipt — so a client `deliver()` can only time out. The
    /// connection is held open (not closed) so the client's receipt channel is
    /// never closed: the wait must hit the timeout, not a `recv() -> None`.
    async fn broker_that_never_acks() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = accept_async(stream).await.expect("ws upgrade");
            let (mut sink, mut source) = ws.split();
            // First text frame is the client's `Hello`; answer with `Welcome`.
            if let Some(Ok(Message::Text(_))) = source.next().await {
                let _ = sink
                    .send(Message::text(BrokerFrame::Welcome.to_text()))
                    .await;
            }
            // Drain the `Deliver` (and anything else) but never reply with
            // `Delivered`.
            while let Some(Ok(_)) = source.next().await {}
        });
        format!("ws://{addr}")
    }

    /// Regression test for issue #51: when the broker never sends `Delivered`,
    /// `deliver()` must return a clear timeout error promptly instead of
    /// hanging the caller forever.
    #[tokio::test]
    async fn deliver_times_out_when_broker_never_sends_receipt() {
        let endpoint = broker_that_never_acks().await;
        let mut client = BrokerClient::connect(&endpoint, test_agent("parent"), "ignored")
            .await
            .expect("handshake completes");

        // Inject a tiny receipt bound via the private entry point so the test
        // is fast; the outer timeout guards against a regression to an
        // unbounded recv hanging the whole suite.
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            client.deliver_with_receipt_timeout(
                "child",
                test_ask("parent"),
                Duration::from_millis(50),
            ),
        )
        .await;

        let result = outcome.expect("deliver() resolved instead of hanging");
        assert!(
            matches!(result, Err(BrokerError::Transport(ref m)) if m.contains("timed out")),
            "expected a timeout transport error, got {result:?}",
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "deliver() should fail fast, but took {:?}",
            started.elapsed(),
        );
    }

    /// Issue #52: a broker that completes the handshake then closes the
    /// connection, so the client's reader loop exits on its own (clean close).
    async fn broker_that_closes_after_handshake() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = accept_async(stream).await.expect("ws upgrade");
            let (mut sink, mut source) = ws.split();
            // First text frame is the client's `Hello`; answer with `Welcome`.
            if let Some(Ok(Message::Text(_))) = source.next().await {
                let _ = sink
                    .send(Message::text(BrokerFrame::Welcome.to_text()))
                    .await;
            }
            // Close cleanly so the client reader's `Ok(Close)` / stream-end arm
            // fires and the demux loop returns on its own.
            let _ = sink.send(Message::Close(None)).await;
            // Drain anything the client still sends until its side closes too.
            while let Some(Ok(_)) = source.next().await {}
        });
        format!("ws://{addr}")
    }

    /// Issue #52: when the connection closes (reader loop returns), the death
    /// must be *observable* to a caller instead of a silent
    /// `next_message() -> None`. The supervisor flips `reader_alive`, so after
    /// the close:
    ///   - `next_message()` returns `None`, and
    ///   - `reader_alive()` is `false`.
    /// (Logging is exercised by the same supervisor path; we assert the
    /// caller-facing surface here.)
    #[tokio::test]
    async fn reader_death_is_surfaced_when_connection_closes() {
        let endpoint = broker_that_closes_after_handshake().await;
        let mut client = BrokerClient::connect(&endpoint, test_agent("parent"), "ignored")
            .await
            .expect("handshake completes");

        // The reader is alive right after a successful handshake.
        assert!(
            client.reader_alive(),
            "reader should be alive immediately after connect"
        );

        // Server closed → reader loop returns → `messages` channel drains to None.
        let msg = tokio::time::timeout(Duration::from_secs(2), client.next_message())
            .await
            .expect("next_message() resolved instead of hanging");
        assert!(msg.is_none(), "no message expected after the close");

        // The supervisor flips the flag once the reader resolves; give the
        // runtime a (short, bounded) moment to schedule that.
        let flagged_dead = tokio::time::timeout(Duration::from_secs(2), async {
            while client.reader_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        assert!(
            flagged_dead,
            "reader should be marked dead once the connection closed"
        );
    }

    /// A broker that completes the handshake then echoes a `Delivered` receipt for
    /// each `Deliver`, but only after a small delay — so a `deliver()` with a tiny
    /// receipt timeout misses its receipt (which then lands, late, in the shared
    /// `delivered` channel as a stale entry).
    async fn broker_that_echoes_receipts_after_delay() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = accept_async(stream).await.expect("ws upgrade");
            let (mut sink, mut source) = ws.split();
            if let Some(Ok(Message::Text(_))) = source.next().await {
                let _ = sink
                    .send(Message::text(BrokerFrame::Welcome.to_text()))
                    .await;
            }
            while let Some(Ok(Message::Text(txt))) = source.next().await {
                if let Ok(ClientFrame::Deliver { message, .. }) = ClientFrame::from_text(&txt) {
                    let id = message.id.clone();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = sink
                        .send(Message::text(BrokerFrame::Delivered { id }.to_text()))
                        .await;
                }
            }
        });
        format!("ws://{addr}")
    }

    /// Regression for issue #114: after a `deliver()` times out, its late
    /// `Delivered` receipt is left in the shared channel. A SUBSEQUENT `deliver()`
    /// on the reused client must return ITS OWN id (correlated to its message),
    /// never the stale prior receipt.
    #[tokio::test]
    async fn deliver_skips_a_stale_receipt_from_a_prior_timed_out_deliver() {
        let endpoint = broker_that_echoes_receipts_after_delay().await;
        let mut client = BrokerClient::connect(&endpoint, test_agent("parent"), "ignored")
            .await
            .expect("connect");

        // deliver(A) times out (10ms < the broker's 50ms echo delay); A's receipt
        // then lands in the shared channel as a stale entry.
        let msg_a = test_ask("a");
        let id_a = msg_a.id.clone();
        let res_a = client
            .deliver_with_receipt_timeout("target", msg_a, Duration::from_millis(10))
            .await;
        assert!(res_a.is_err(), "deliver(A) times out before its receipt");

        // Let A's late receipt arrive and buffer in the channel.
        tokio::time::sleep(Duration::from_millis(120)).await;

        // deliver(B) must return B's id — correlation skips A's stale receipt.
        let msg_b = test_ask("b");
        let id_b = msg_b.id.clone();
        assert_ne!(id_a, id_b);
        let res_b = client
            .deliver_with_receipt_timeout("target", msg_b, Duration::from_secs(5))
            .await;
        assert_eq!(
            res_b.expect("deliver(B) succeeds"),
            id_b,
            "deliver(B) returns its own id, not A's stale receipt"
        );
    }

    /// A broker that completes the handshake then, for every `Deliver`, replies
    /// with a correlated `Error` frame (as `BrokerServer` now does for
    /// `MailboxFull`, #491/#53) instead of `Delivered` — never acking the
    /// message. The connection is held open so the client can only learn the
    /// outcome from the `Error` frame, never a `recv() -> None`.
    async fn broker_that_rejects_every_deliver(reason: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = accept_async(stream).await.expect("ws upgrade");
            let (mut sink, mut source) = ws.split();
            if let Some(Ok(Message::Text(_))) = source.next().await {
                let _ = sink
                    .send(Message::text(BrokerFrame::Welcome.to_text()))
                    .await;
            }
            while let Some(Ok(Message::Text(txt))) = source.next().await {
                if let Ok(ClientFrame::Deliver { message, .. }) = ClientFrame::from_text(&txt) {
                    let _ = sink
                        .send(Message::text(
                            BrokerFrame::Error {
                                reason: reason.to_string(),
                                id: Some(message.id),
                            }
                            .to_text(),
                        ))
                        .await;
                }
            }
        });
        format!("ws://{addr}")
    }

    /// Regression for the review finding on #491/#53: a `MailboxFull` (or any
    /// other) rejection of a `Deliver` must reach the CALLER of `deliver()` as
    /// a distinct, fast `BrokerError::Rejected` — not get silently dropped by
    /// the post-handshake demux, leaving the caller to burn the full receipt
    /// timeout and see a misleading `BrokerError::Transport("timed out...")`
    /// that looks like a lost/stalled connection rather than an explicit "no".
    #[tokio::test]
    async fn deliver_returns_rejected_when_broker_sends_a_correlated_error() {
        let endpoint =
            broker_that_rejects_every_deliver("mailbox 'child' is full (2 pending messages)").await;
        let mut client = BrokerClient::connect(&endpoint, test_agent("parent"), "ignored")
            .await
            .expect("handshake completes");

        let started = std::time::Instant::now();
        // Real (non-test-only) `deliver()`, with the production 30s timeout —
        // if the rejection were still being swallowed, this would hang for the
        // full 30s instead of returning promptly.
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            client.deliver("child", test_ask("parent")),
        )
        .await
        .expect("deliver() resolves promptly instead of hanging out the receipt timeout");

        match outcome {
            Err(BrokerError::Rejected(reason)) => {
                assert!(
                    reason.contains("full"),
                    "rejection reason should be the broker's verbatim message, got: {reason}"
                );
            }
            other => panic!("expected BrokerError::Rejected, got {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the rejection must reach the caller fast, not via the receipt timeout, took {:?}",
            started.elapsed(),
        );
    }

    /// Mirrors #114 (stale-receipt skip) for the new rejection lane: after a
    /// `deliver()` times out, a LATE `Error` for that same timed-out call must
    /// not be mistaken for the outcome of a SUBSEQUENT `deliver()` on the reused
    /// client.
    #[tokio::test]
    async fn deliver_skips_a_stale_error_from_a_prior_timed_out_deliver() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let endpoint = format!("ws://{addr}");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = accept_async(stream).await.expect("ws upgrade");
            let (mut sink, mut source) = ws.split();
            if let Some(Ok(Message::Text(_))) = source.next().await {
                let _ = sink
                    .send(Message::text(BrokerFrame::Welcome.to_text()))
                    .await;
            }
            // First Deliver (A): reply with its Error only after a delay, so
            // deliver(A)'s tiny timeout expires first and the Error lands late.
            if let Some(Ok(Message::Text(txt))) = source.next().await {
                if let Ok(ClientFrame::Deliver { message, .. }) = ClientFrame::from_text(&txt) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = sink
                        .send(Message::text(
                            BrokerFrame::Error {
                                reason: "stale rejection for A".into(),
                                id: Some(message.id),
                            }
                            .to_text(),
                        ))
                        .await;
                }
            }
            // Second Deliver (B): reply promptly with a real Delivered receipt.
            if let Some(Ok(Message::Text(txt))) = source.next().await {
                if let Ok(ClientFrame::Deliver { message, .. }) = ClientFrame::from_text(&txt) {
                    let _ = sink
                        .send(Message::text(
                            BrokerFrame::Delivered { id: message.id }.to_text(),
                        ))
                        .await;
                }
            }
            // Hold the connection open (as `broker_that_never_acks` above
            // does, for the same reason): dropping `sink`/`source` here would
            // close the socket right after the send, racing the client's
            // demux for no reason this test cares about. Just drain.
            while let Some(Ok(_)) = source.next().await {}
        });

        let mut client = BrokerClient::connect(&endpoint, test_agent("parent"), "ignored")
            .await
            .expect("connect");

        let msg_a = test_ask("a");
        let id_a = msg_a.id.clone();
        let res_a = client
            .deliver_with_receipt_timeout("target", msg_a, Duration::from_millis(10))
            .await;
        assert!(res_a.is_err(), "deliver(A) times out before its rejection");

        // Let A's late Error arrive and buffer in the errors channel.
        tokio::time::sleep(Duration::from_millis(120)).await;

        let msg_b = test_ask("b");
        let id_b = msg_b.id.clone();
        assert_ne!(id_a, id_b);
        let res_b = client
            .deliver_with_receipt_timeout("target", msg_b, Duration::from_secs(5))
            .await;
        assert_eq!(
            res_b.expect("deliver(B) succeeds, not misrouted to A's stale rejection"),
            id_b,
        );
    }
}
