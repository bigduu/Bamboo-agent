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

use crate::executor::{ChildExecutor, EventSink, SteerInbox};
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
    /// Bind an arbitrary address. Pass `0.0.0.0:PORT` for a remotely-reachable
    /// worker/broker; [`bind_loopback`](Self::bind_loopback) is the local default.
    /// (TLS — `wss://` — is added with the broker in a later phase; see
    /// `docs/remote-actor-plan.md` §3.2.)
    pub async fn bind(addr: SocketAddr) -> TransportResult<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        Ok(Self { listener, addr })
    }

    /// Bind `127.0.0.1:0` (ephemeral port) — the local default.
    pub async fn bind_loopback() -> TransportResult<Self> {
        Self::bind((std::net::Ipv4Addr::LOCALHOST, 0).into()).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The reachable `ws://127.0.0.1:<port>` endpoint to advertise.
    pub fn ws_endpoint(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Serve exactly one connection (owned child / demo), then return.
    pub async fn serve_one<E: ChildExecutor + ?Sized>(
        self,
        executor: Arc<E>,
    ) -> TransportResult<()> {
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

    /// Serve connection-after-connection for a **reusable** actor, one at a time.
    ///
    /// Unlike [`serve`], connections are handled serially (the next accept waits
    /// for the current run to finish) so a pooled worker never runs two
    /// assignments at once — preserving per-run context isolation. Unlike
    /// [`serve_one`], the worker survives a connection close and is ready for the
    /// next assignment. Exits when no new connection arrives within `idle_timeout`
    /// (so an unreused worker reclaims itself instead of lingering forever). An
    /// active run is never cut short — the timeout only guards the idle accept.
    pub async fn serve_reusable_with_idle_timeout<E: ChildExecutor + ?Sized>(
        self,
        executor: Arc<E>,
        idle_timeout: std::time::Duration,
    ) -> TransportResult<()> {
        loop {
            let accept = tokio::time::timeout(idle_timeout, self.listener.accept()).await;
            let (stream, _) = match accept {
                Ok(res) => res?,
                Err(_) => return Ok(()), // idle: reclaim self, clean exit
            };
            // Handle this assignment to completion before accepting the next.
            let _ = handle_conn(stream, executor.clone()).await;
        }
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

async fn handle_conn<E: ChildExecutor + ?Sized>(
    stream: TcpStream,
    executor: Arc<E>,
) -> TransportResult<()> {
    let ws = accept_async(stream).await?;
    let (ws_tx, mut ws_rx) = ws.split();
    // One writer task owns the sink; runs push frames through this channel (decouples read/write).
    let (out_tx, out_rx) = mpsc::unbounded_channel::<ChildFrame>();
    let writer = tokio::spawn(writer_task(ws_tx, out_rx));

    let mut active_cancel: Option<CancellationToken> = None;
    let mut active_steer: Option<mpsc::UnboundedSender<String>> = None;
    while let Some(msg) = ws_rx.next().await {
        match msg? {
            Message::Text(t) => match ParentFrame::from_text(t.as_str()) {
                Ok(ParentFrame::Run(spec)) => {
                    // A new run supersedes any active run: cancel the previous
                    // task *before* starting the new one so the old run stops
                    // emitting events into the shared `out_tx`. Without this the
                    // old task is orphaned and its output interleaves with the
                    // new run's. (The old steer sender is dropped implicitly by
                    // the reassignment of `active_steer` below.)
                    if let Some(prev) = active_cancel.take() {
                        prev.cancel();
                    }
                    let cancel = CancellationToken::new();
                    let (steer_tx, steer_rx) = SteerInbox::channel();
                    active_cancel = Some(cancel.clone());
                    active_steer = Some(steer_tx);
                    start_run(executor.clone(), spec, steer_rx, cancel, out_tx.clone());
                }
                Ok(ParentFrame::Cancel) => {
                    if let Some(c) = &active_cancel {
                        c.cancel();
                    }
                }
                Ok(ParentFrame::Message { text }) => {
                    // In-band steering: hand to the active run's inbox; the
                    // executor admits it at its next safe point.
                    if let Some(steer) = &active_steer {
                        let _ = steer.send(text);
                    }
                }
                Err(_) => { /* ignore malformed frame */ }
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Parent disconnected / closed: cancel any still-active run so its spawned
    // task ends and stops feeding the writer. Without this `writer.await` blocks
    // until the orphaned run finishes on its own.
    if let Some(c) = active_cancel {
        c.cancel();
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
    steer: SteerInbox,
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
        let outcome = executor.run(spec, sink, steer, cancel).await;
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

    async fn run_once(endpoint: &str, assignment: &str) -> (TerminalStatus, Option<String>) {
        let mut client = ChildClient::connect(endpoint).await.unwrap();
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: assignment.into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();
        let mut terminal = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            if let ChildFrame::Terminal { status, result, .. } = frame {
                terminal = Some((status, result));
                break;
            }
        }
        let _ = client.close().await;
        terminal.expect("terminal frame")
    }

    /// A reusable worker serves connection-after-connection (the pool's reuse
    /// path), then reclaims itself once left idle past the timeout.
    #[tokio::test]
    async fn reusable_server_serves_sequential_connections_then_idles_out() {
        use std::time::Duration;

        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move {
            server
                .serve_reusable_with_idle_timeout(
                    Arc::new(EchoExecutor),
                    Duration::from_millis(400),
                )
                .await
        });

        // Two SEQUENTIAL assignments land on the SAME warm worker.
        let (s1, r1) = run_once(&endpoint, "first").await;
        assert_eq!(s1, TerminalStatus::Completed);
        assert_eq!(r1.as_deref(), Some("echo: first"));

        let (s2, r2) = run_once(&endpoint, "second").await;
        assert_eq!(s2, TerminalStatus::Completed);
        assert_eq!(r2.as_deref(), Some("echo: second"));

        // No further connection: the worker idles out and exits cleanly.
        let exited = tokio::time::timeout(Duration::from_secs(5), srv).await;
        assert!(
            matches!(exited, Ok(Ok(Ok(())))),
            "reusable server should idle out cleanly, got {exited:?}"
        );
    }

    /// Mid-run steering: a `Message` frame reaches the active run's SteerInbox.
    #[tokio::test]
    async fn message_frame_routes_to_active_steer_inbox() {
        use crate::executor::{ChildExecutor, ChildOutcome, SteerInbox};

        /// Completes only after receiving one steering message, echoing it back.
        struct SteerEcho;
        #[async_trait::async_trait]
        impl ChildExecutor for SteerEcho {
            async fn run(
                &self,
                _spec: RunSpec,
                events: EventSink,
                mut steer: SteerInbox,
                _cancel: CancellationToken,
            ) -> ChildOutcome {
                let steered = steer.recv().await.unwrap_or_default();
                events.emit(serde_json::json!({"type": "token", "content": steered.clone()}));
                ChildOutcome::completed(format!("steered: {steered}"))
            }
        }

        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move { server.serve_one(Arc::new(SteerEcho)).await });

        let mut client = ChildClient::connect(&endpoint).await.unwrap();
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "start".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();
        // mid-run in-band message
        client
            .send(ParentFrame::Message {
                text: "change course".into(),
            })
            .await
            .unwrap();

        let mut terminal = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            if let ChildFrame::Terminal { status, result, .. } = frame {
                terminal = Some((status, result));
                break;
            }
        }
        let (status, result) = terminal.expect("terminal");
        assert_eq!(status, TerminalStatus::Completed);
        assert_eq!(result.as_deref(), Some("steered: change course"));

        let _ = client.close().await;
        let _ = srv.await;
    }

    /// Service-agent mode: two concurrent connections to one `serve()` must be
    /// fully isolated — each client sees only its own run's events/terminal,
    /// and per-connection cancel state (the round-2 fix) must not cross talk.
    #[tokio::test(flavor = "multi_thread")]
    async fn service_agent_concurrent_no_crosstalk() {
        use crate::executor::EchoExecutor;

        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move { server.serve(Arc::new(EchoExecutor)).await });

        // Client A holds its run open (300ms sleep) while client B runs to
        // completion — guaranteed overlap.
        let endpoint_a = endpoint.clone();
        let a = tokio::spawn(async move {
            let mut client = ChildClient::connect(&endpoint_a).await.unwrap();
            client
                .send(ParentFrame::Run(RunSpec {
                    assignment: "__sleep_ms:300 alpha only".into(),
                    reasoning_effort: None,
                    messages: Vec::new(),
                }))
                .await
                .unwrap();
            collect_stream(client).await
        });
        let endpoint_b = endpoint.clone();
        let b = tokio::spawn(async move {
            // Give A a head start so its run is live when B's traffic flows.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut client = ChildClient::connect(&endpoint_b).await.unwrap();
            client
                .send(ParentFrame::Run(RunSpec {
                    assignment: "beta only".into(),
                    reasoning_effort: None,
                    messages: Vec::new(),
                }))
                .await
                .unwrap();
            collect_stream(client).await
        });

        let (tokens_a, result_a) = a.await.unwrap();
        let (tokens_b, result_b) = b.await.unwrap();

        // Each stream carries exactly its own run.
        assert_eq!(result_a.as_deref(), Some("echo: alpha only"));
        assert_eq!(result_b.as_deref(), Some("echo: beta only"));
        assert!(
            tokens_a.iter().all(|t| !t.contains("beta")),
            "client A saw B's tokens: {tokens_a:?}"
        );
        assert!(
            tokens_b.iter().all(|t| !t.contains("alpha")),
            "client B saw A's tokens: {tokens_b:?}"
        );

        srv.abort();
    }

    /// Drain a client until Terminal; returns (token contents, result).
    async fn collect_stream(mut client: ChildClient) -> (Vec<String>, Option<String>) {
        let mut tokens = Vec::new();
        let mut result = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            match frame {
                ChildFrame::Event { event } => {
                    if let Some(t) = event["content"].as_str() {
                        tokens.push(t.to_string());
                    }
                }
                ChildFrame::Terminal {
                    status, result: r, ..
                } => {
                    assert_eq!(status, TerminalStatus::Completed);
                    result = r;
                    break;
                }
            }
        }
        let _ = client.close().await;
        (tokens, result)
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

    /// Defect A: a second `Run` must cancel the previously active run so the old
    /// task ends promptly — its `Terminal(Cancelled)` arrives *before* any
    /// explicit `Cancel` is sent, proving the supersession, not orphaning.
    #[tokio::test]
    async fn second_run_cancels_previous() {
        use crate::executor::{ChildExecutor, ChildOutcome, SteerInbox};
        use std::time::Duration;

        /// Emits one `token` then parks on its cancel token; always ends
        /// `Cancelled`. Forces a run that can only finish via cancellation.
        struct WaitForCancel;
        #[async_trait::async_trait]
        impl ChildExecutor for WaitForCancel {
            async fn run(
                &self,
                _spec: RunSpec,
                events: EventSink,
                _steer: SteerInbox,
                cancel: CancellationToken,
            ) -> ChildOutcome {
                events.emit(serde_json::json!({"type": "token", "content": "go"}));
                cancel.cancelled().await;
                ChildOutcome::cancelled()
            }
        }

        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move { server.serve_one(Arc::new(WaitForCancel)).await });

        let mut client = ChildClient::connect(&endpoint).await.unwrap();

        // First run: goes live, emits its token, then parks on cancellation.
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "first".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();
        loop {
            match client.next_frame().await.unwrap() {
                Some(ChildFrame::Event { event }) if event["content"] == "go" => break,
                Some(_) => continue,
                None => panic!("connection closed before first token"),
            }
        }

        // Second run: must supersede (cancel) the first — defect A fix.
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "second".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();

        // The first run's Terminal must now arrive with no explicit Cancel —
        // driven solely by the new Run frame. Bound it so a regression fails
        // fast instead of hanging on the parked first run.
        let mut first_terminal: Option<TerminalStatus> = None;
        let drain = async {
            while let Some(frame) = client.next_frame().await.unwrap() {
                if let ChildFrame::Terminal { status, .. } = frame {
                    first_terminal = Some(status);
                    break;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .expect("first run's terminal never arrived — new Run did not cancel it");
        assert_eq!(
            first_terminal.expect("first terminal status"),
            TerminalStatus::Cancelled,
            "previous run should be cancelled by the new Run frame"
        );

        // The second run is still parked; only an explicit Cancel ends it.
        client.send(ParentFrame::Cancel).await.unwrap();

        let mut second_terminal: Option<TerminalStatus> = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            if let ChildFrame::Terminal { status, .. } = frame {
                second_terminal = Some(status);
                break;
            }
        }
        assert_eq!(
            second_terminal.expect("second terminal status"),
            TerminalStatus::Cancelled
        );

        let _ = client.close().await;
        let _ = srv.await;
    }
}
