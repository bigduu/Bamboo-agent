//! WebSocket server: a thin network shell over [`BrokerCore`].
//!
//! One task per connection: a `Hello` handshake (Bearer token + bind to a
//! session mailbox), then a `select` loop that pumps client frames into the core
//! and forwards the subscription stream back out as `Message` frames. Reachable
//! on any address (`0.0.0.0:PORT`) so workers deployed via subprocess / Docker /
//! SSH can dial home over the network.

use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};

use crate::core::{BrokerCore, PushItem};
use crate::error::{BrokerError, BrokerResult};
use crate::proto::{BrokerFrame, ClientFrame};

type Ws = WebSocketStream<TcpStream>;

/// The network front-end. Holds the routing [`BrokerCore`] and the expected
/// Bearer token; clones cheaply (everything behind `Arc`).
pub struct BrokerServer {
    core: Arc<BrokerCore>,
    token: String,
}

impl BrokerServer {
    pub fn new(core: Arc<BrokerCore>, token: impl Into<String>) -> Self {
        Self {
            core,
            token: token.into(),
        }
    }

    /// Accept connections forever, one task each. Returns only on a listener
    /// error (per-connection errors are logged and isolated).
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> BrokerResult<()> {
        loop {
            let (stream, _peer) = listener
                .accept()
                .await
                .map_err(|e| BrokerError::Transport(format!("accept: {e}")))?;
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = server.handle_conn(stream).await {
                    tracing::debug!("broker connection ended: {e}");
                }
            });
        }
    }

    async fn handle_conn(&self, stream: TcpStream) -> BrokerResult<()> {
        let ws = accept_async(stream)
            .await
            .map_err(|e| BrokerError::Transport(format!("ws accept: {e}")))?;
        let (mut sink, mut source) = ws.split();

        // 1. Handshake — the first frame must be a Hello with a valid token.
        let (session_id, role) = match read_client_frame(&mut source).await? {
            Some(ClientFrame::Hello { agent, token }) => {
                if token != self.token {
                    let _ = send(
                        &mut sink,
                        BrokerFrame::Error {
                            reason: "invalid token".into(),
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
                            match self.core.deliver(&to, &message).await {
                                Ok(id) => {
                                    if send(&mut sink, BrokerFrame::Delivered { id }).await.is_err() {
                                        break Ok(());
                                    }
                                }
                                Err(e) => {
                                    let _ = send(&mut sink, BrokerFrame::Error { reason: e.to_string() }).await;
                                }
                            }
                        }
                        Ok(Some(ClientFrame::Subscribe)) => match self.core.subscribe(&session_id, role.as_deref()).await {
                            Ok(rx) => sub_rx = Some(rx),
                            Err(e) => {
                                let _ = send(&mut sink, BrokerFrame::Error { reason: e.to_string() }).await;
                            }
                        },
                        Ok(Some(ClientFrame::Ack { id })) => {
                            if let Err(e) = self.core.ack(&session_id, &id).await {
                                let _ = send(&mut sink, BrokerFrame::Error { reason: e.to_string() }).await;
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
async fn read_client_frame(source: &mut SplitStream<Ws>) -> BrokerResult<Option<ClientFrame>> {
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

async fn send(sink: &mut SplitSink<Ws, Message>, frame: BrokerFrame) -> BrokerResult<()> {
    sink.send(Message::text(frame.to_text()))
        .await
        .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
}
