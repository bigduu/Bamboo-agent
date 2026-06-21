//! Broker client. Connects, does the `Hello` handshake, then runs a background
//! reader that demuxes incoming frames into a `messages` stream and a
//! `delivered` receipt stream — so one connection can both deliver and
//! subscribe (the parent does both: deliver an Ask, subscribe for the Reply).

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
    _reader: tokio::task::JoinHandle<()>,
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
                        _ => {}
                    },
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(Self {
            sink,
            messages,
            delivered,
            _reader: reader,
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
    pub async fn next_message(&mut self) -> Option<InboxMessage> {
        self.messages.recv().await
    }

    /// Acknowledge a processed message so the broker deletes it.
    pub async fn ack(&mut self, id: MsgId) -> BrokerResult<()> {
        self.send(ClientFrame::Ack { id }).await
    }

    async fn send(&mut self, frame: ClientFrame) -> BrokerResult<()> {
        self.sink
            .send(Message::Text(frame.to_text()))
            .await
            .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
    }
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
}
