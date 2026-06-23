//! WebSocket transport (design §6): full-duplex parent↔child link.
//!
//! - Child side: [`WsServer`] accepts a connection and drives a [`ChildExecutor`] — `Run` starts a
//!   task whose events stream out as `ChildFrame::Event`, then a `ChildFrame::Terminal`; `Cancel`
//!   trips the run's token (out-of-band, never queued behind events).
//! - Parent side: [`ChildClient`] sends [`ParentFrame`]s and reads [`ChildFrame`]s.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, accept_hdr_async, connect_async, MaybeTlsStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::executor::{ChildExecutor, EventSink, HostBridge, HostRequestKind, SteerInbox};
use crate::proto::{ChildFrame, ParentFrame, RunSpec};

/// Pending host-callback replies, keyed by request id, shared between the
/// per-run pump (which inserts) and the connection read loop (which resolves
/// them on [`ParentFrame::ApprovalReply`]).
type PendingReplies = Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>;

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
    /// TLS configuration / handshake failure (bad cert/key, provider, etc.).
    #[error("tls: {0}")]
    Tls(String),
}

pub type TransportResult<T> = Result<T, TransportError>;

// ---- child side --------------------------------------------------------------

/// A WebSocket server an actor runs to receive work.
///
/// Three knobs, all optional and independent (remote-actor-plan §3.2 / §3.4):
/// - `listener`/`addr`: where it binds (`bind_loopback` for local default,
///   `bind`/`bind_tls` for a remotely-reachable worker on `0.0.0.0:PORT`).
/// - `tls`: when `Some`, every accepted connection is wrapped in TLS before the
///   WS upgrade and the advertised endpoint is `wss://`.
/// - `expected_token`: when `Some`, the WS upgrade is rejected (401) unless the
///   client presents `Authorization: Bearer <token>`. None ⇒ accept any
///   (loopback default, zero regression).
pub struct WsServer {
    listener: TcpListener,
    addr: SocketAddr,
    tls: Option<TlsAcceptor>,
    expected_token: Option<String>,
}

impl WsServer {
    /// Bind an arbitrary address with NO TLS and NO token. Pass `0.0.0.0:PORT`
    /// for a remotely-reachable worker/broker; [`bind_loopback`](Self::bind_loopback)
    /// is the local default. Use [`bind_with_token`](Self::bind_with_token) /
    /// [`bind_tls`](Self::bind_tls) to require auth.
    pub async fn bind(addr: SocketAddr) -> TransportResult<Self> {
        Self::bind_with_token(addr, None).await
    }

    /// Bind plaintext but optionally require a bearer token on the WS upgrade.
    ///
    /// A token WITHOUT TLS is insecure (the bearer crosses the wire in the
    /// clear) — acceptable only on a trusted/loopback link (e.g. the
    /// deterministic `remote_connect_e2e` test). For real remote exposure use
    /// [`bind_tls`](Self::bind_tls).
    pub async fn bind_with_token(
        addr: SocketAddr,
        expected_token: Option<String>,
    ) -> TransportResult<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        Ok(Self {
            listener,
            addr,
            tls: None,
            expected_token,
        })
    }

    /// Bind with TLS termination (`wss://`) and an optional bearer token.
    ///
    /// Builds a `tokio_rustls::TlsAcceptor` from `cert_file`/`key_file` (PEM)
    /// against an explicit `ring` provider — the same approach as the server's
    /// HTTP TLS face (`bamboo-server`'s `build_rustls_config`) so the workspace
    /// resolves a single crypto provider and never hits the rustls 0.23
    /// no-default-provider panic.
    ///
    /// **Fail-fast:** a missing/unreadable/unparseable cert or key returns
    /// `Err` — it never silently downgrades to plaintext.
    pub async fn bind_tls(
        addr: SocketAddr,
        cert_file: &Path,
        key_file: &Path,
        expected_token: Option<String>,
    ) -> TransportResult<Self> {
        let server_config =
            build_server_config(cert_file, key_file).map_err(TransportError::Tls)?;
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        Ok(Self {
            listener,
            addr,
            tls: Some(acceptor),
            expected_token,
        })
    }

    /// Bind `127.0.0.1:0` (ephemeral port) — the local default (no TLS, no token).
    pub async fn bind_loopback() -> TransportResult<Self> {
        Self::bind((std::net::Ipv4Addr::LOCALHOST, 0).into()).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The reachable endpoint to advertise: `wss://` when TLS is active,
    /// `ws://` otherwise.
    pub fn ws_endpoint(&self) -> String {
        let scheme = if self.tls.is_some() { "wss" } else { "ws" };
        format!("{scheme}://{}", self.addr)
    }

    /// Serve exactly one connection (owned child / demo), then return.
    pub async fn serve_one<E: ChildExecutor + ?Sized>(
        self,
        executor: Arc<E>,
    ) -> TransportResult<()> {
        let (stream, _) = self.listener.accept().await?;
        accept_and_handle(stream, &self.tls, &self.expected_token, executor).await
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
        accept_and_handle(stream, &self.tls, &self.expected_token, executor).await
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
            let _ =
                accept_and_handle(stream, &self.tls, &self.expected_token, executor.clone()).await;
        }
    }

    /// Serve connections forever (long-running service agent).
    pub async fn serve<E: ChildExecutor + ?Sized>(self, executor: Arc<E>) -> TransportResult<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let exec = executor.clone();
            // The TLS handshake + auth callback run inside the spawned task (not
            // on the accept loop) so a slow/hostile client cannot stall the
            // server from accepting other connections. Clones are cheap:
            // `TlsAcceptor` is `Arc`-backed, the token is a short `String`.
            let tls = self.tls.clone();
            let token = self.expected_token.clone();
            tokio::spawn(async move {
                let _ = accept_and_handle(stream, &tls, &token, exec).await;
            });
        }
    }
}

/// Apply TLS (if configured) + the bearer-auth WS upgrade to a freshly-accepted
/// `TcpStream`, then drive the connection. Plaintext + no-token reduces to the
/// historical `accept_async(stream)` path (zero regression).
async fn accept_and_handle<E: ChildExecutor + ?Sized>(
    stream: TcpStream,
    tls: &Option<TlsAcceptor>,
    expected_token: &Option<String>,
    executor: Arc<E>,
) -> TransportResult<()> {
    match tls {
        Some(acceptor) => {
            // Terminate TLS before the WS upgrade so the bearer + frames ride an
            // encrypted link.
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_err(|e| TransportError::Tls(format!("accept handshake: {e}")))?;
            let ws = ws_upgrade(tls_stream, expected_token).await?;
            handle_conn(ws, executor).await
        }
        None => {
            let ws = ws_upgrade(stream, expected_token).await?;
            handle_conn(ws, executor).await
        }
    }
}

/// Perform the WS upgrade over an arbitrary stream, validating the bearer token
/// at the upgrade layer when one is expected. When `expected_token` is `None`
/// this is exactly `accept_async` (any client accepted) — zero regression.
async fn ws_upgrade<S>(
    stream: S,
    expected_token: &Option<String>,
) -> TransportResult<tokio_tungstenite::WebSocketStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match expected_token {
        None => Ok(accept_async(stream).await?),
        Some(token) => {
            let callback = BearerCallback {
                expected: token.clone(),
            };
            Ok(accept_hdr_async(stream, callback).await?)
        }
    }
}

/// WS-upgrade callback that enforces `Authorization: Bearer <token>`. Rejecting
/// here means the WebSocket never establishes — the wire protocol
/// (`ParentFrame`/`ChildFrame`) stays byte-identical; auth lives entirely at the
/// HTTP upgrade (remote-actor-plan §6.3). The token is never logged.
struct BearerCallback {
    expected: String,
}

impl Callback for BearerCallback {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        let presented = request
            .headers()
            .get(tokio_tungstenite::tungstenite::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let ok = matches!(presented, Some(h) if bearer_matches(h, &self.expected));
        if ok {
            Ok(response)
        } else {
            let err = ErrorResponse::new(Some("unauthorized: bad or missing bearer token".into()));
            let (mut parts, body) = err.into_parts();
            parts.status = StatusCode::UNAUTHORIZED;
            Err(ErrorResponse::from_parts(parts, body))
        }
    }
}

/// Constant-form check that an `Authorization` header is `Bearer <expected>`.
fn bearer_matches(header: &str, expected: &str) -> bool {
    header
        .strip_prefix("Bearer ")
        .map(|t| t == expected)
        .unwrap_or(false)
}

/// Build a rustls [`rustls::ServerConfig`] from PEM cert/key files against an
/// explicit `ring` provider (mirrors `bamboo-server`'s `build_rustls_config`).
fn build_server_config(cert_file: &Path, key_file: &Path) -> Result<rustls::ServerConfig, String> {
    use std::fs::File;
    use std::io::BufReader;

    let cert_path = cert_file.display();
    let key_path = key_file.display();

    let cf = File::open(cert_file).map_err(|e| format!("open cert_file '{cert_path}': {e}"))?;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(cf))
            .collect::<Result<_, _>>()
            .map_err(|e| format!("parse cert_file '{cert_path}': {e}"))?;
    if certs.is_empty() {
        return Err(format!(
            "no certificates in cert_file '{cert_path}' (expected PEM CERTIFICATE blocks)"
        ));
    }

    let kf = File::open(key_file).map_err(|e| format!("open key_file '{key_path}': {e}"))?;
    let key = match rustls_pemfile::private_key(&mut BufReader::new(kf)) {
        Ok(Some(k)) => k,
        Ok(None) => {
            return Err(format!(
                "no private key in key_file '{key_path}' (expected PKCS#8/RSA/SEC1)"
            ))
        }
        Err(e) => return Err(format!("parse key_file '{key_path}': {e}")),
    };

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            format!("rustls rejected cert/key (cert '{cert_path}', key '{key_path}'): {e}")
        })
}

/// Drive an already-upgraded WS connection (generic over the underlying stream
/// so it serves both plaintext `TcpStream` and `TlsStream<TcpStream>`).
async fn handle_conn<S, E>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    executor: Arc<E>,
) -> TransportResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    E: ChildExecutor + ?Sized,
{
    let (ws_tx, mut ws_rx) = ws.split();
    // One writer task owns the sink; runs push frames through this channel (decouples read/write).
    let (out_tx, out_rx) = mpsc::unbounded_channel::<ChildFrame>();
    let writer = tokio::spawn(writer_task(ws_tx, out_rx));

    // Host-callback replies for gated-tool approvals the active run proxies back
    // to the host over this WS. Shared with each run's pump.
    let pending: PendingReplies = Arc::new(Mutex::new(HashMap::new()));

    let mut active_cancel: Option<CancellationToken> = None;
    let mut active_steer: Option<mpsc::UnboundedSender<String>> = None;
    while let Some(msg) = ws_rx.next().await {
        match msg? {
            Message::Text(t) => match ParentFrame::from_text(t.as_str()) {
                // Phase 2: the host's decision on a proxied gated-tool approval.
                // Routed through the `pending` correlation map (body =
                // {"approved": bool}); a worker that proxied an `ApprovalRequest`
                // awaits its oneshot here.
                Ok(ParentFrame::ApprovalReply { id, approved }) => {
                    if let Some(reply) = pending.lock().expect("pending lock").remove(&id) {
                        let _ = reply.send(serde_json::json!({ "approved": approved }));
                    }
                }
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
                    start_run(
                        executor.clone(),
                        spec,
                        steer_rx,
                        cancel,
                        out_tx.clone(),
                        pending.clone(),
                    );
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

async fn writer_task<S>(
    mut ws_tx: SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
    mut out_rx: mpsc::UnboundedReceiver<ChildFrame>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    pending: PendingReplies,
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

    // Host-callback pump: each gated-tool approval the executor proxies back
    // becomes an `ApprovalRequest` on the wire; its reply (an `ApprovalReply`
    // resolved in `handle_conn`) is delivered to the awaiting oneshot via
    // `pending`.
    let (bridge, mut req_rx) = HostBridge::channel();
    let sink = sink.with_host_bridge(bridge);
    let out_req = out_tx.clone();
    let pending_for_pump = pending.clone();
    let pump = tokio::spawn(async move {
        while let Some(req) = req_rx.recv().await {
            // Random + unguessable: the worker's pending map and the host-side
            // pending-approval registry both key on this exact string, so a
            // sequential `sa-{n}` would let an external POST guess a live
            // request_id. A v4 uuid removes that. Keep the `sa-` prefix.
            let id = format!("sa-{}", Uuid::new_v4());
            pending_for_pump
                .lock()
                .expect("pending lock")
                .insert(id.clone(), req.reply);
            let frame = match req.kind {
                HostRequestKind::Approval => ChildFrame::ApprovalRequest { id, body: req.body },
            };
            if out_req.send(frame).is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let outcome = executor.run(spec, sink, steer, cancel).await;
        let _ = fwd.await; // flush all events before the terminal frame
        pump.abort(); // no more host callbacks after the run ends
        let _ = out_tx.send(ChildFrame::Terminal {
            status: outcome.status,
            result: outcome.result,
            error: outcome.error,
            transcript: outcome.transcript,
        });
    });
}

// ---- parent side -------------------------------------------------------------

type ClientStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Parent-side connection to a child actor.
pub struct ChildClient {
    tx: SplitSink<ClientStream, Message>,
    rx: SplitStream<ClientStream>,
}

impl ChildClient {
    /// Connect with no bearer token (loopback / trusted-link default). Identical
    /// to `connect_with_auth(endpoint, None)` — zero regression.
    pub async fn connect(endpoint: &str) -> TransportResult<Self> {
        Self::connect_with_auth(endpoint, None).await
    }

    /// Connect to a worker endpoint, optionally presenting a bearer token on the
    /// WS upgrade (`Authorization: Bearer <token>`). A `wss://` endpoint flows
    /// through `MaybeTlsStream` transparently. The token is never logged.
    pub async fn connect_with_auth(endpoint: &str, token: Option<&str>) -> TransportResult<Self> {
        let (ws, _resp) = match token {
            None => connect_async(endpoint).await?,
            Some(token) => {
                use tokio_tungstenite::tungstenite::client::IntoClientRequest;
                let mut request = endpoint.into_client_request().map_err(TransportError::Ws)?;
                let value = format!("Bearer {token}")
                    .parse()
                    .map_err(|e| TransportError::Protocol(format!("bad bearer header: {e}")))?;
                request.headers_mut().insert(
                    tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
                    value,
                );
                connect_async(request).await?
            }
        };
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
                ChildFrame::ApprovalRequest { .. } => {}
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

    /// `bind_with_token` gates the WS upgrade: the right bearer connects + runs,
    /// a wrong / missing bearer is rejected at the upgrade (no WS, no Terminal).
    #[tokio::test]
    async fn bearer_token_gates_the_upgrade() {
        let server = WsServer::bind_with_token(
            (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            Some("T-secret".into()),
        )
        .await
        .unwrap();
        let endpoint = server.ws_endpoint();
        // ws:// (no TLS) even though a token is set — token alone never upgrades scheme.
        assert!(endpoint.starts_with("ws://"));
        let srv = tokio::spawn(async move { server.serve(Arc::new(EchoExecutor)).await });

        // Correct token: connects and runs to completion.
        let mut client = ChildClient::connect_with_auth(&endpoint, Some("T-secret"))
            .await
            .expect("correct bearer should connect");
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "go".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();
        let mut terminal = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            if let ChildFrame::Terminal { status, .. } = frame {
                terminal = Some(status);
                break;
            }
        }
        assert_eq!(terminal, Some(TerminalStatus::Completed));
        let _ = client.close().await;

        // Wrong token: the upgrade is rejected — connect itself errors.
        let bad = ChildClient::connect_with_auth(&endpoint, Some("WRONG")).await;
        assert!(bad.is_err(), "wrong bearer must be rejected at the upgrade");

        // Missing token: same rejection.
        let missing = ChildClient::connect(&endpoint).await;
        assert!(
            missing.is_err(),
            "missing bearer must be rejected when a token is required"
        );

        srv.abort();
    }

    /// `bind_tls` fails fast on a missing cert/key (never falls back to plaintext).
    #[tokio::test]
    async fn bind_tls_fails_fast_on_missing_cert() {
        let result = WsServer::bind_tls(
            (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            Path::new("/nonexistent/bamboo-subagent/cert.pem"),
            Path::new("/nonexistent/bamboo-subagent/key.pem"),
            None,
        )
        .await;
        match result {
            Err(TransportError::Tls(m)) if m.contains("cert_file") => {}
            Err(other) => panic!("expected a TLS error naming cert_file, got {other:?}"),
            Ok(_) => panic!("missing cert must fail"),
        }
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

    /// Phase 2: a worker that hits a gated tool proxies an approval to the host
    /// over the per-child WS and blocks for the decision — the FULL cross-process
    /// round-trip (worker `approval_call` → `ChildFrame::ApprovalRequest` →
    /// host `ParentFrame::ApprovalReply` → the awaiting call resolves).
    #[tokio::test]
    async fn approval_request_round_trips_to_host_and_back() {
        use crate::executor::{ChildExecutor, ChildOutcome, SteerInbox};

        /// Proxies one approval to the host and reports the decision.
        struct ApprovalProber;
        #[async_trait::async_trait]
        impl ChildExecutor for ApprovalProber {
            async fn run(
                &self,
                _spec: RunSpec,
                events: EventSink,
                _steer: SteerInbox,
                _cancel: CancellationToken,
            ) -> ChildOutcome {
                let Some(host) = events.host() else {
                    return ChildOutcome::error("no host bridge wired");
                };
                match host
                    .approval_call(serde_json::json!({
                        "tool_name": "Write",
                        "permission_type": "WriteFile",
                        "resource": "/tmp/x",
                        "question": "approve?",
                    }))
                    .await
                {
                    Ok(decision) => ChildOutcome::completed(format!(
                        "approved: {}",
                        decision["approved"].as_bool().unwrap_or(false)
                    )),
                    Err(e) => ChildOutcome::error(format!("bridge: {e}")),
                }
            }
        }

        let server = WsServer::bind_loopback().await.unwrap();
        let endpoint = server.ws_endpoint();
        let srv = tokio::spawn(async move { server.serve_one(Arc::new(ApprovalProber)).await });

        let mut client = ChildClient::connect(&endpoint).await.unwrap();
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: "go".into(),
                reasoning_effort: None,
                messages: Vec::new(),
            }))
            .await
            .unwrap();

        let mut terminal = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            match frame {
                ChildFrame::ApprovalRequest { id, body } => {
                    assert_eq!(body["resource"], "/tmp/x");
                    assert_eq!(body["tool_name"], "Write");
                    client
                        .send(ParentFrame::ApprovalReply { id, approved: true })
                        .await
                        .unwrap();
                }
                ChildFrame::Terminal { status, result, .. } => {
                    terminal = Some((status, result));
                    break;
                }
                _ => {}
            }
        }
        let (status, result) = terminal.expect("terminal");
        assert_eq!(status, TerminalStatus::Completed);
        assert_eq!(result.as_deref(), Some("approved: true"));

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
                ChildFrame::ApprovalRequest { .. } => {}
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
