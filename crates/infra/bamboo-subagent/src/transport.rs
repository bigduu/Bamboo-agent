//! WebSocket transport (design §6): full-duplex parent↔child link.
//!
//! - Child side: [`WsServer`] accepts a connection and drives a [`ChildExecutor`] — `Run` starts a
//!   task whose events stream out as `ChildFrame::Event`, then a `ChildFrame::Terminal`; `Cancel`
//!   trips the run's token (out-of-band, never queued behind events).
//! - Parent side: [`ChildClient`] sends [`ParentFrame`]s and reads [`ChildFrame`]s.

use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, connect_async, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use crate::executor::{ChildExecutor, EventSink};
use crate::proto::{ChildFrame, ParentFrame, RunSpec};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ws: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("decode: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("protocol: {0}")]
    Protocol(String),
}

pub type TransportResult<T> = Result<T, TransportError>;

// ---- child side --------------------------------------------------------------

/// A loopback WebSocket server an actor runs to receive work.
pub struct WsServer {
    listener: TcpListener,
    addr: SocketAddr,
}

impl WsServer {
    /// Bind `127.0.0.1:0` (ephemeral port).
    pub async fn bind_loopback() -> TransportResult<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        Ok(Self { listener, addr })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The reachable `ws://127.0.0.1:<port>` endpoint to advertise.
    pub fn ws_endpoint(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Serve exactly one connection (owned child / demo), then return.
    pub async fn serve_one<E: ChildExecutor + ?Sized>(self, executor: Arc<E>) -> TransportResult<()> {
        let (stream, _) = self.listener.accept().await?;
        handle_conn(stream, executor).await
    }

    /// Serve exactly one connection, but give up if no client connects within
    /// `accept_timeout` (orphan defense: a spawned worker whose parent died
    /// before connecting must not linger forever). An ACTIVE connection is
    /// never cut short — the timeout only guards the accept.
    pub async fn serve_one_with_accept_timeout<E: ChildExecutor + ?Sized>(
        self,
        executor: Arc<E>,
        accept_timeout: std::time::Duration,
    ) -> TransportResult<()> {
        let (stream, _) = tokio::time::timeout(accept_timeout, self.listener.accept())
            .await
            .map_err(|_| {
                TransportError::Protocol(format!(
                    "no connection within {accept_timeout:?}; exiting"
                ))
            })??;
        handle_conn(stream, executor).await
    }

    /// Serve connections forever (long-running service agent).
    pub async fn serve<E: ChildExecutor + ?Sized>(self, executor: Arc<E>) -> TransportResult<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let exec = executor.clone();
            tokio::spawn(async move {
                let _ = handle_conn(stream, exec).await;
            });
        }
    }
}

async fn handle_conn<E: ChildExecutor + ?Sized>(stream: TcpStream, executor: Arc<E>) -> TransportResult<()> {
    let ws = accept_async(stream).await?;
    let (ws_tx, mut ws_rx) = ws.split();
    // One writer task owns the sink; runs push frames through this channel (decouples read/write).
    let (out_tx, out_rx) = mpsc::unbounded_channel::<ChildFrame>();
    let writer = tokio::spawn(writer_task(ws_tx, out_rx));

    let mut active_cancel: Option<CancellationToken> = None;
    while let Some(msg) = ws_rx.next().await {
        match msg? {
            Message::Text(t) => match ParentFrame::from_text(t.as_str()) {
                Ok(ParentFrame::Run(spec)) => {
                    let cancel = CancellationToken::new();
                    active_cancel = Some(cancel.clone());
                    start_run(executor.clone(), spec, cancel, out_tx.clone());
                }
                Ok(ParentFrame::Cancel) => {
                    if let Some(c) = &active_cancel {
                        c.cancel();
                    }
                }
                Ok(ParentFrame::Message { .. }) => { /* multi-turn: slice 4 */ }
                Err(_) => { /* ignore malformed frame */ }
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    drop(out_tx);
    let _ = writer.await;
    Ok(())
}

async fn writer_task(
    mut ws_tx: SplitSink<WebSocketStream<TcpStream>, Message>,
    mut out_rx: mpsc::UnboundedReceiver<ChildFrame>,
) {
    while let Some(frame) = out_rx.recv().await {
        if ws_tx.send(Message::text(frame.to_text())).await.is_err() {
            break;
        }
    }
    let _ = ws_tx.close().await;
}

fn start_run<E: ChildExecutor + ?Sized>(
    executor: Arc<E>,
    spec: RunSpec,
    cancel: CancellationToken,
    out_tx: mpsc::UnboundedSender<ChildFrame>,
) {
    let (sink, mut ev_rx) = EventSink::channel();
    let out_fwd = out_tx.clone();
    // forward executor events -> wire, ends when the executor drops the sink
    let fwd = tokio::spawn(async move {
        while let Some(e) = ev_rx.recv().await {
            if out_fwd.send(ChildFrame::Event { event: e }).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        let outcome = executor.run(spec, sink, cancel).await;
        let _ = fwd.await; // flush all events before the terminal frame
        let _ = out_tx.send(ChildFrame::Terminal {
            status: outcome.status,
            result: outcome.result,
            error: outcome.error,
        });
    });
}

// ---- parent side -------------------------------------------------------------

type ClientStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Parent-side connection to a child actor.
pub struct ChildClient {
    tx: SplitSink<ClientStream, Message>,
    rx: SplitStream<ClientStream>,
}

impl ChildClient {
    pub async fn connect(endpoint: &str) -> TransportResult<Self> {
        let (ws, _resp) = connect_async(endpoint).await?;
        let (tx, rx) = ws.split();
        Ok(Self { tx, rx })
    }

    pub async fn send(&mut self, frame: ParentFrame) -> TransportResult<()> {
        self.tx.send(Message::text(frame.to_text())).await?;
        Ok(())
    }

    /// Next child frame, or `None` once the connection closes.
    pub async fn next_frame(&mut self) -> TransportResult<Option<ChildFrame>> {
        while let Some(msg) = self.rx.next().await {
            match msg? {
                Message::Text(t) => return Ok(Some(ChildFrame::from_text(t.as_str())?)),
                Message::Close(_) => return Ok(None),
                _ => continue,
            }
        }
        Ok(None)
    }

    pub async fn close(mut self) -> TransportResult<()> {
        self.tx.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::EchoExecutor;
    use crate::proto::{ChildFrame, TerminalStatus};

    /// In-process server + client over a real loopback socket (no subprocess).
    #[tokio::test]
    async fn loopback_run_streams_events_then_terminal() {
        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move { server.serve_one(Arc::new(EchoExecutor)).await });

        let mut client = ChildClient::connect(&endpoint).await.unwrap();
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "one two".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();

        let mut events = Vec::new();
        let mut terminal = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            match frame {
                ChildFrame::Event { event } => events.push(event),
                ChildFrame::Terminal { status, result, .. } => {
                    terminal = Some((status, result));
                    break;
                }
            }
        }

        let (status, result) = terminal.expect("terminal frame");
        assert_eq!(status, TerminalStatus::Completed);
        assert_eq!(result.as_deref(), Some("echo: one two"));
        assert!(events.iter().any(|e| e["content"] == "one "));

        let _ = client.close().await;
        let _ = srv.await;
    }

    /// Orphan defense: with no client, the accept-timeout variant returns
    /// instead of hanging forever.
    #[tokio::test]
    async fn accept_timeout_fires_when_nobody_connects() {
        let server = WsServer::bind_loopback().await.unwrap();
        let result = server
            .serve_one_with_accept_timeout(
                Arc::new(EchoExecutor),
                std::time::Duration::from_millis(50),
            )
            .await;
        assert!(matches!(result, Err(TransportError::Protocol(_))));
    }
}
