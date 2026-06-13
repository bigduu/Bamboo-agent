//! Broker client. Connects, does the `Hello` handshake, then runs a background
//! reader that demuxes incoming frames into a `messages` stream and a
//! `delivered` receipt stream — so one connection can both deliver and
//! subscribe (the parent does both: deliver an Ask, subscribe for the Reply).

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
    pub async fn deliver(&mut self, to: &str, message: InboxMessage) -> BrokerResult<MsgId> {
        self.send(ClientFrame::Deliver {
            to: to.into(),
            message,
        })
        .await?;
        self.delivered.recv().await.ok_or_else(|| {
            BrokerError::Transport("connection closed before delivery receipt".into())
        })
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
