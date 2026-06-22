//! Broker client. Connects, does the `Hello` handshake, then runs a background
//! reader that demuxes incoming frames into a `messages` stream and a
//! `delivered` receipt stream — so one connection can both deliver and
//! subscribe (the parent does both: deliver an Ask, subscribe for the Reply).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bamboo_subagent::{AgentRef, InboxMessage, MsgId};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::error::{BrokerError, BrokerResult};
use crate::proto::{BrokerFrame, ClientFrame};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

/// Upper bound on how long [`BrokerClient::deliver`] waits for the broker's
/// `Delivered` receipt before giving up. The receipt only means the broker
/// durably accepted the `Deliver` (not that the worker replied), so it should
/// arrive promptly; 30s is a generous cap that still guarantees a caller can
/// never hang indefinitely if the broker dies after receiving `Deliver`.
const DELIVER_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

/// A connected broker client bound to one session mailbox.
pub struct BrokerClient {
    sink: WsSink,
    messages: mpsc::UnboundedReceiver<InboxMessage>,
    delivered: mpsc::UnboundedReceiver<MsgId>,
    /// Out-of-band cancel signals (the timed-out ask's correlation id), demuxed
    /// by the background reader independently of `messages` — so a Cancel reaches
    /// the worker even while its work loop is blocked on an in-flight run. #50.
    cancels: mpsc::UnboundedReceiver<MsgId>,
    /// Cleared by [`reader_supervisor`] the instant the background reader exits
    /// (clean close / panic / cancellation), so callers can tell "no messages
    /// right now" (`next_message() -> None` but still alive) apart from "the
    /// connection died". See [`BrokerClient::reader_alive`].
    reader_alive: Arc<AtomicBool>,
    /// Supervisor that awaits the reader's `JoinHandle` and logs its outcome;
    /// ends on its own the moment the reader resolves (clean disconnect → no
    /// leaked task). Replaces the old ignored `_reader` handle.
    _supervisor: tokio::task::JoinHandle<()>,
}

impl BrokerClient {
    /// Connect to `endpoint` (`ws://host:port` or `wss://…`), authenticate with
    /// `token`, and bind this connection to `agent.session_id`.
    pub async fn connect(endpoint: &str, agent: AgentRef, token: &str) -> BrokerResult<Self> {
        let (ws, _resp) = connect_async(endpoint)
            .await
            .map_err(|e| BrokerError::Transport(format!("connect {endpoint}: {e}")))?;
        let (mut sink, mut source) = ws.split();

        // Handshake: send Hello, wait for Welcome (or Error).
        sink.send(Message::Text(
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
                    Ok(BrokerFrame::Error { reason }) => return Err(BrokerError::Auth(reason)),
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
        let (cancel_tx, cancels) = mpsc::unbounded_channel();
        // The demux loop pushes `Message`/`Delivered`/`Cancel` frames into their
        // respective channels and ends when the stream closes or errors.
        let reader = tokio::spawn(async move {
            while let Some(frame) = source.next().await {
                match frame {
                    Ok(Message::Text(t)) => match BrokerFrame::from_text(&t) {
                        Ok(BrokerFrame::Message { message }) => {
                            let _ = msg_tx.send(message);
                        }
                        Ok(BrokerFrame::Delivered { id }) => {
                            let _ = del_tx.send(id);
                        }
                        Ok(BrokerFrame::Cancel { correlation_id }) => {
                            let _ = cancel_tx.send(correlation_id);
                        }
                        _ => {}
                    },
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });

        // Supervise the reader so its termination is *observable* instead of
        // silently dropped (issue #52): log every exit kind and flip a shared
        // flag callers can poll. The supervisor owns the reader handle and ends
        // the instant the reader resolves — a clean disconnect leaves no leaked
        // task. Holding this must not change the reader's behavior, so the
        // reader body above is left exactly as it was.
        let reader_alive = Arc::new(AtomicBool::new(true));
        let supervisor = tokio::spawn(reader_supervisor(reader, reader_alive.clone()));

        Ok(Self {
            sink,
            messages,
            delivered,
            cancels,
            reader_alive,
            _supervisor: supervisor,
        })
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
        self.send(ClientFrame::Deliver {
            to: to.into(),
            message,
        })
        .await?;
        match tokio::time::timeout(receipt_timeout, self.delivered.recv()).await {
            Ok(Some(id)) => Ok(id),
            Ok(None) => Err(BrokerError::Transport(
                "connection closed before delivery receipt".into(),
            )),
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
        self.send(ClientFrame::Ack { id }).await
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
            .send(Message::Text(frame.to_text()))
            .await
            .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
    }
}

/// Await the reader task and make its exit observable (issue #52).
///
/// - clean end (the demux loop returned / stream closed): `warn!`
/// - panic (`JoinError::is_panic`): `error!` with the panic message
/// - other `JoinError` (cancelled / aborted): `error!`
///
/// In every case the shared `reader_alive` flag is cleared so callers can tell
/// "the connection died" from "no messages right now". This owns the reader
/// handle, so it resolves — and the supervisor task ends — exactly when the
/// reader does: a clean disconnect leaves no leaked task.
async fn reader_supervisor(reader: tokio::task::JoinHandle<()>, reader_alive: Arc<AtomicBool>) {
    let outcome = reader.await;
    // Best-effort, eventually-consistent death signal: this store races the
    // reader dropping `msg_tx` (which is what makes `next_message()` return
    // `None`), so a caller can observe `None` a beat *before* this flips the
    // flag. That only means a possibly-missed warn on the very first post-death
    // `None`; it is not a correctness guarantee and callers must not rely on
    // `reader_alive` being false the instant `next_message()` returns `None`.
    reader_alive.store(false, Ordering::SeqCst);
    match outcome {
        Ok(()) => {
            tracing::warn!("broker reader task ended; connection closed");
        }
        Err(err) if err.is_panic() => {
            tracing::error!(
                "broker reader task panicked: {}",
                panic_payload_message(err.into_panic())
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
                    .send(Message::Text(BrokerFrame::Welcome.to_text()))
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
                    .send(Message::Text(BrokerFrame::Welcome.to_text()))
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
}
