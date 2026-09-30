//! Request/reply MULTIPLEXER over a single broker connection.
//!
//! The MCP proxy holds one dedicated, replies-only broker connection (only
//! `McpRequest` out / `McpReply` in). Today every proxied tool call holds an
//! exclusive lock across its whole round-trip, so parallel MCP tool calls (e.g.
//! two screenshots) serialize. This driver removes that: requests register a
//! waiter keyed by `correlation_id`, send their frame (serialized only for the
//! byte write), then await their reply concurrently. A background router drains
//! the client's `messages` channel and fulfils the matching waiter per inbound
//! correlated reply. #56.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use bamboo_subagent::{AgentRef, AskBody, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use futures_util::SinkExt;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::client::{send_close, ReaderLifecycle, WsSink, CLIENT_SHUTDOWN_TIMEOUT};
use crate::error::{BrokerError, BrokerResult};
use crate::proto::ClientFrame;

const CANCEL_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const ORPHAN_ACK_TIMEOUT: Duration = Duration::from_secs(1);

struct ReplyWaiter {
    target: String,
    kind: InboxKind,
    sender: oneshot::Sender<InboxMessage>,
}

type Pending = Arc<StdMutex<HashMap<MsgId, ReplyWaiter>>>;

struct PendingRequest {
    pending: Pending,
    id: MsgId,
    sink: Arc<Mutex<WsSink>>,
    target: String,
    sent: bool,
    replied: bool,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.id);
        if self.sent && !self.replied {
            let sink = Arc::clone(&self.sink);
            let target = self.target.clone();
            let id = self.id.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let frame = ClientFrame::Cancel {
                        to: target,
                        correlation_id: id,
                    };
                    let _ = tokio::time::timeout(CANCEL_SEND_TIMEOUT, async {
                        sink.lock().await.send(Message::text(frame.to_text())).await
                    })
                    .await;
                });
            }
        }
    }
}

/// A multiplexed request/reply driver over one broker connection. Built from a
/// connected + subscribed [`crate::client::BrokerClient`] via
/// `BrokerClient::into_multiplexed`.
pub struct MultiplexedClient {
    /// Authoritative ownership of the WebSocket reader. Dropping the mux
    /// directly aborts that reader via `ReaderLifecycle::drop` instead of
    /// detaching it behind its supervisor. #788.
    reader: ReaderLifecycle,
    /// Locked only for the duration of one frame write — never across a reply wait.
    sink: Arc<Mutex<WsSink>>,
    /// correlation_id (== the request msg.id) -> reply waiter.
    pending: Pending,
    /// Delivery sends and their exact receipt correlation are serialized. Reply
    /// waits never hold this lock, so independent requests still overlap.
    delivery_lock: Mutex<()>,
    delivered: Mutex<mpsc::UnboundedReceiver<MsgId>>,
    errors: Mutex<mpsc::UnboundedReceiver<(MsgId, String)>>,
    ack_lock: Mutex<()>,
    ack_results: Mutex<mpsc::UnboundedReceiver<(MsgId, MsgId, bool, Option<String>)>>,
    /// Shared with the (still-running) background reader; flips false when it dies.
    reader_alive: Arc<AtomicBool>,
    /// The correlation router; ends when `messages` closes (reader death) or when
    /// this client is dropped.
    router: JoinHandle<()>,
    me: AgentRef,
}

impl MultiplexedClient {
    /// Spawn the correlation router over a connected client's parts. The reader
    /// that feeds `messages` keeps running independently; this router drains it
    /// and routes by `correlation_id`.
    pub(crate) fn spawn(
        reader: ReaderLifecycle,
        sink: WsSink,
        mut messages: mpsc::UnboundedReceiver<InboxMessage>,
        delivered: mpsc::UnboundedReceiver<MsgId>,
        errors: mpsc::UnboundedReceiver<(MsgId, String)>,
        ack_results: mpsc::UnboundedReceiver<(MsgId, MsgId, bool, Option<String>)>,
        reader_alive: Arc<AtomicBool>,
        me: AgentRef,
    ) -> Self {
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let routed = pending.clone();
        let sink = Arc::new(Mutex::new(sink));
        let router_sink = Arc::clone(&sink);
        let router = tokio::spawn(async move {
            while let Some(msg) = messages.recv().await {
                if let Some(cid) = msg.correlation_id.clone() {
                    let (waiter, active_invalid) = {
                        let mut pending = routed.lock().unwrap_or_else(|error| error.into_inner());
                        match pending.get(&cid) {
                            Some(waiter)
                                if waiter.kind == msg.kind
                                    && waiter.target == msg.from.session_id =>
                            {
                                (pending.remove(&cid), false)
                            }
                            Some(_) => (None, true),
                            None => (None, false),
                        }
                    };
                    if active_invalid {
                        // A forged or malformed correlation cannot consume
                        // the real request's waiter or its durable message.
                        continue;
                    }
                    if let Some(waiter) = waiter {
                        match waiter.sender.send(msg) {
                            Ok(()) => continue,
                            Err(returned) => {
                                // The caller was cancelled after the router
                                // removed its waiter. Treat the reply as late.
                                retire_orphan_reply(&router_sink, &returned).await;
                                continue;
                            }
                        }
                    }
                }
                // Only our namespaced, abandoned request ids are safe to
                // retire. Other mailbox traffic belongs to its own consumer.
                retire_orphan_reply(&router_sink, &msg).await;
            }
            // `messages` closed == the reader exited == the connection is dead.
            // Drop every pending sender so all in-flight waiters resolve to an
            // error instead of hanging forever.
            routed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
        });

        Self {
            reader,
            sink,
            pending,
            delivery_lock: Mutex::new(()),
            delivered: Mutex::new(delivered),
            errors: Mutex::new(errors),
            ack_lock: Mutex::new(()),
            ack_results: Mutex::new(ack_results),
            reader_alive,
            router,
            me,
        }
    }

    /// Gracefully close the underlying WebSocket and await bounded cleanup of
    /// both the reader supervisor and correlation router. Drop remains a
    /// direct-abort fallback if this future is cancelled or cleanup stalls.
    pub async fn close(mut self) -> BrokerResult<()> {
        self.reader.mark_intentional_shutdown();
        let close_result = {
            let mut sink = self.sink.lock().await;
            send_close(&mut *sink).await
        };
        self.reader.shutdown().await;

        if tokio::time::timeout(CLIENT_SHUTDOWN_TIMEOUT, &mut self.router)
            .await
            .is_err()
        {
            self.router.abort();
        }
        close_result
    }

    /// True while the underlying reader is still running. False once the
    /// connection has dropped (use to trigger a reconnect).
    pub fn reader_alive(&self) -> bool {
        self.reader_alive.load(Ordering::SeqCst)
    }

    /// Send an MCP request and return its durably acknowledged MCP reply.
    pub async fn request(
        &self,
        target: &str,
        kind: InboxKind,
        body: Value,
        timeout: Duration,
    ) -> BrokerResult<Value> {
        let reply_kind = match kind {
            InboxKind::McpRequest => InboxKind::McpReply,
            InboxKind::Ask => InboxKind::Reply,
            _ => {
                return Err(BrokerError::Protocol(
                    "unsupported multiplexed request kind".into(),
                ))
            }
        };
        self.request_validated(target, kind, body, reply_kind, timeout, |body| {
            match reply_kind {
                InboxKind::McpReply => {
                    serde_json::from_value::<crate::mcp::McpReply>(body.clone()).map_err(
                        |error| BrokerError::Protocol(format!("bad MCP reply body: {error}")),
                    )?;
                }
                InboxKind::Reply => {
                    serde_json::from_value::<ReplyBody>(body.clone()).map_err(|error| {
                        BrokerError::Protocol(format!("bad reply body: {error}"))
                    })?;
                }
                _ => unreachable!("reply kind was checked above"),
            }
            Ok(body.clone())
        })
        .await
    }

    /// Ask on this caller's one subscribed connection. A typed Reply is parsed
    /// before ACK so a malformed answer remains recoverable.
    pub async fn ask(
        &self,
        target: &str,
        question: &str,
        mode: bamboo_subagent::AskMode,
        timeout: Duration,
    ) -> BrokerResult<String> {
        let body = serde_json::to_value(AskBody {
            question: question.to_owned(),
            mode,
        })
        .expect("AskBody serializes");
        self.request_validated(
            target,
            InboxKind::Ask,
            body,
            InboxKind::Reply,
            timeout,
            |body| {
                let reply: ReplyBody = serde_json::from_value(body.clone())
                    .map_err(|error| BrokerError::Protocol(format!("bad reply body: {error}")))?;
                Ok(reply.answer)
            },
        )
        .await
    }

    async fn request_validated<T>(
        &self,
        target: &str,
        kind: InboxKind,
        body: Value,
        reply_kind: InboxKind,
        timeout: Duration,
        parse: impl FnOnce(&Value) -> BrokerResult<T>,
    ) -> BrokerResult<T> {
        let prefix = if kind == InboxKind::Ask {
            "broker-ask-"
        } else {
            "broker-mcp-"
        };
        let msg = InboxMessage {
            // The namespace lets a later connection retire only replies to
            // requests owned by this driver, preserving other mailbox traffic.
            id: MsgId(format!("{prefix}{}", MsgId::new().as_str())),
            from: self.me.clone(),
            kind,
            body,
            created_at: Utc::now(),
            correlation_id: None,
        };
        let qid = msg.id.clone();
        let (tx, rx) = oneshot::channel();

        // Register the waiter BEFORE sending, so a fast reply can't arrive before
        // the waiter exists (the router would otherwise drop it).
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                qid.clone(),
                ReplyWaiter {
                    target: target.to_owned(),
                    kind: reply_kind,
                    sender: tx,
                },
            );
        // Register cleanup before the first await: caller cancellation can
        // happen while waiting for a receipt or a reply, not only at timeout.
        let mut pending_request = PendingRequest {
            pending: self.pending.clone(),
            id: qid.clone(),
            sink: Arc::clone(&self.sink),
            target: target.to_owned(),
            sent: false,
            replied: false,
        };
        // One absolute deadline covers lock admission, exact delivery receipt,
        // reply routing and exact durable ACK. The Drop guard handles caller
        // cancellation at any await in the sequence.
        let deadline = Instant::now() + timeout;
        let result = tokio::time::timeout_at(deadline, async {
            pending_request.sent = true; // a cancelled send is uncertain
            self.deliver_exact(target, msg).await?;
            let reply = rx
                .await
                .map_err(|_| BrokerError::Transport("connection closed before reply".into()))?;
            pending_request.replied = true;
            let parsed = parse(&reply.body)?;
            self.ack_confirmed(reply.id).await?;
            Ok(parsed)
        })
        .await;
        match result {
            Ok(Err(BrokerError::Rejected(reason))) => {
                // The broker definitively did not enqueue this request.
                // A scoped peer may not be allowed to send Cancel, and there
                // is no worker run to cancel after an explicit rejection.
                pending_request.sent = false;
                Err(BrokerError::Rejected(reason))
            }
            Ok(result) => result,
            Err(_) => {
                // Best effort cancellation is bounded separately. Disarm the
                // Drop path to avoid sending the same signal twice.
                if pending_request.sent && !pending_request.replied {
                    let _ =
                        tokio::time::timeout(CANCEL_SEND_TIMEOUT, self.cancel(target, &qid)).await;
                    pending_request.sent = false;
                }
                Err(BrokerError::Transport(format!(
                    "request to '{target}' timed out after {timeout:?}"
                )))
            }
        }
    }

    async fn deliver_exact(&self, to: &str, message: InboxMessage) -> BrokerResult<()> {
        let _delivery = self.delivery_lock.lock().await;
        let id = message.id.clone();
        let frame = ClientFrame::Deliver {
            to: to.into(),
            message,
        };
        {
            let mut sink = self.sink.lock().await;
            sink.send(Message::text(frame.to_text()))
                .await
                .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))?;
        }
        let mut delivered = self.delivered.lock().await;
        let mut errors = self.errors.lock().await;
        loop {
            tokio::select! {
                biased;
                error = errors.recv() => match error {
                    Some((received, reason)) if received == id => return Err(BrokerError::Rejected(reason)),
                    Some(_) => continue,
                    None => return Err(BrokerError::Transport("connection closed before delivery receipt".into())),
                },
                receipt = delivered.recv() => match receipt {
                    Some(received) if received == id => return Ok(()),
                    Some(_) => continue,
                    None => return Err(BrokerError::Transport("connection closed before delivery receipt".into())),
                },
            }
        }
    }

    async fn ack_confirmed(&self, id: MsgId) -> BrokerResult<()> {
        let _ack = self.ack_lock.lock().await;
        let request_id = MsgId::new();
        let frame = ClientFrame::AckWithReceipt {
            id: id.clone(),
            request_id: request_id.clone(),
        };
        self.sink
            .lock()
            .await
            .send(Message::text(frame.to_text()))
            .await
            .map_err(|error| BrokerError::Transport(format!("ws send: {error}")))?;
        let mut results = self.ack_results.lock().await;
        loop {
            match results.recv().await {
                Some((received, request, accepted, reason))
                    if received == id && request == request_id =>
                {
                    return if accepted {
                        Ok(())
                    } else {
                        Err(BrokerError::Rejected(
                            reason.unwrap_or_else(|| "broker rejected durable ACK".into()),
                        ))
                    };
                }
                Some(_) => continue,
                None => {
                    return Err(BrokerError::Transport(
                        "connection closed before ACK receipt".into(),
                    ))
                }
            }
        }
    }

    /// Out-of-band, fire-and-forget cancel (mirrors `BrokerClient::cancel`).
    async fn cancel(&self, to: &str, correlation_id: &MsgId) -> BrokerResult<()> {
        let frame = ClientFrame::Cancel {
            to: to.into(),
            correlation_id: correlation_id.clone(),
        };
        let mut sink = self.sink.lock().await;
        sink.send(Message::text(frame.to_text()))
            .await
            .map_err(|e| BrokerError::Transport(format!("ws send: {e}")))
    }
}

fn owned_reply(message: &InboxMessage) -> bool {
    let Some(correlation) = message.correlation_id.as_ref() else {
        return false;
    };
    let prefix = match message.kind {
        InboxKind::Reply if serde_json::from_value::<ReplyBody>(message.body.clone()).is_ok() => {
            "broker-ask-"
        }
        InboxKind::McpReply
            if serde_json::from_value::<crate::mcp::McpReply>(message.body.clone()).is_ok() =>
        {
            "broker-mcp-"
        }
        _ => return false,
    };
    correlation
        .as_str()
        .strip_prefix(prefix)
        .is_some_and(|suffix| uuid::Uuid::parse_str(suffix).is_ok())
}

async fn retire_orphan_reply(sink: &Arc<Mutex<WsSink>>, message: &InboxMessage) {
    if !owned_reply(message) {
        tracing::debug!("broker mux: leaving unrelated durable message unacknowledged");
        return;
    }
    let frame = ClientFrame::Ack {
        id: message.id.clone(),
    };
    let _ = tokio::time::timeout(ORPHAN_ACK_TIMEOUT, async {
        sink.lock().await.send(Message::text(frame.to_text())).await
    })
    .await;
}

impl Drop for MultiplexedClient {
    fn drop(&mut self) {
        // ReaderLifecycle directly aborts the WebSocket reader when its field
        // drops. Abort the router here as well so the mux leaves no detached
        // per-connection task even if the runtime has not yet delivered the
        // reader channel closure.
        self.router.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BrokerClient;
    use crate::core::BrokerCore;
    use crate::server::BrokerServer;
    use std::sync::Arc as StdArc;
    use tokio::net::TcpListener;

    const TOKEN: &str = "t";

    async fn start() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = StdArc::new(BrokerCore::new(dir.path()));
        let server = StdArc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    fn agent(id: &str) -> AgentRef {
        AgentRef {
            session_id: id.into(),
            role: None,
        }
    }

    async fn mux(endpoint: &str, id: &str) -> MultiplexedClient {
        let mut c = BrokerClient::connect(endpoint, agent(id), TOKEN)
            .await
            .unwrap();
        c.subscribe().await.unwrap();
        c.into_multiplexed(agent(id))
    }

    /// Two concurrent requests whose replies arrive OUT OF ORDER each resolve to
    /// their OWN reply — proving correlation routing, not FIFO stealing.
    #[tokio::test]
    async fn concurrent_requests_route_replies_by_correlation_id() {
        let (endpoint, _dir) = start().await;
        let client = mux(&endpoint, "worker").await;

        // A responder that collects 2 requests and replies to them in REVERSE order.
        let mut responder = BrokerClient::connect(&endpoint, agent("orch"), TOKEN)
            .await
            .unwrap();
        responder.subscribe().await.unwrap();
        tokio::spawn(async move {
            let m1 = responder.next_message().await.unwrap();
            let m2 = responder.next_message().await.unwrap();
            for m in [m2, m1] {
                let reply = InboxMessage {
                    id: MsgId::new(),
                    from: agent("orch"),
                    kind: InboxKind::McpReply,
                    body: m.body.clone(), // echo the request body
                    created_at: Utc::now(),
                    correlation_id: Some(m.id.clone()),
                };
                responder.deliver("worker", reply).await.unwrap();
            }
        });

        let t = Duration::from_secs(5);
        let (r1, r2) = tokio::join!(
            client.request(
                "orch",
                InboxKind::McpRequest,
                serde_json::json!({ "n": 1 }),
                t
            ),
            client.request(
                "orch",
                InboxKind::McpRequest,
                serde_json::json!({ "n": 2 }),
                t
            ),
        );
        assert_eq!(r1.unwrap(), serde_json::json!({ "n": 1 }));
        assert_eq!(r2.unwrap(), serde_json::json!({ "n": 2 }));
    }

    /// A request with no responder times out, returns the timeout error, and
    /// unregisters its waiter (no leak / no mis-route of a later stray).
    #[tokio::test]
    async fn request_times_out_when_no_reply() {
        let (endpoint, _dir) = start().await;
        let client = mux(&endpoint, "worker2").await;

        let err = client
            .request(
                "nobody",
                InboxKind::McpRequest,
                serde_json::json!({}),
                Duration::from_millis(200),
            )
            .await;
        assert!(err.is_err(), "request to a non-responder times out");
        assert!(
            client
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty(),
            "the timed-out waiter was unregistered"
        );
    }

    #[tokio::test]
    async fn cancelled_request_releases_its_correlation_waiter() {
        let (endpoint, _dir) = start().await;
        let client = Arc::new(mux(&endpoint, "cancel-request").await);
        let request = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .request(
                        "absent-worker",
                        InboxKind::McpRequest,
                        serde_json::json!({}),
                        Duration::from_secs(60),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !client
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(client
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty());
    }
}
