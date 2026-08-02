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
use std::sync::Arc;
use std::time::Duration;

use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, MsgId};
use chrono::Utc;
use futures_util::SinkExt;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::client::{send_close, ReaderLifecycle, WsSink, CLIENT_SHUTDOWN_TIMEOUT};
use crate::error::{BrokerError, BrokerResult};
use crate::proto::ClientFrame;

/// Bound on the delivery-receipt wait (mirrors `client::DELIVER_RECEIPT_TIMEOUT`).
const DELIVER_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

type Pending = Arc<Mutex<HashMap<MsgId, oneshot::Sender<InboxMessage>>>>;

/// A multiplexed request/reply driver over one broker connection. Built from a
/// connected + subscribed [`crate::client::BrokerClient`] via
/// `BrokerClient::into_multiplexed`.
pub struct MultiplexedClient {
    /// Authoritative ownership of the WebSocket reader. Dropping the mux
    /// directly aborts that reader via `ReaderLifecycle::drop` instead of
    /// detaching it behind its supervisor. #788.
    reader: ReaderLifecycle,
    /// Locked only for the duration of one frame write — never across a reply wait.
    sink: Mutex<WsSink>,
    /// correlation_id (== the request msg.id) -> reply waiter.
    pending: Pending,
    /// Delivery receipts. Consumed one-at-a-time (any receipt confirms enqueue —
    /// receipts are FIFO over one socket), so concurrent sends don't race it.
    delivered: Mutex<mpsc::UnboundedReceiver<MsgId>>,
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
        reader_alive: Arc<AtomicBool>,
        me: AgentRef,
    ) -> Self {
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let routed = pending.clone();
        let router = tokio::spawn(async move {
            while let Some(msg) = messages.recv().await {
                if let Some(cid) = msg.correlation_id.clone() {
                    if let Some(tx) = routed.lock().await.remove(&cid) {
                        let _ = tx.send(msg);
                        continue;
                    }
                }
                // No waiter: a late reply whose request already timed out and
                // unregistered, or a stray. Drop it.
                tracing::debug!("mcp mux: dropping uncorrelated/late reply");
            }
            // `messages` closed == the reader exited == the connection is dead.
            // Drop every pending sender so all in-flight waiters resolve to an
            // error instead of hanging forever.
            routed.lock().await.clear();
        });

        Self {
            reader,
            sink: Mutex::new(sink),
            pending,
            delivered: Mutex::new(delivered),
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
        let close_result = send_close(self.sink.get_mut()).await;
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

    /// Send a `kind`/`body` request to `target` and await its correlated reply,
    /// concurrently with any other in-flight requests. The reply wait holds NO
    /// lock, so N concurrent calls overlap their round-trips.
    pub async fn request(
        &self,
        target: &str,
        kind: InboxKind,
        body: Value,
        timeout: Duration,
    ) -> BrokerResult<Value> {
        let msg = InboxMessage {
            id: MsgId::new(),
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
        self.pending.lock().await.insert(qid.clone(), tx);

        if let Err(e) = self.deliver(target, msg).await {
            self.pending.lock().await.remove(&qid);
            return Err(e);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(reply)) => Ok(reply.body),
            Ok(Err(_)) => {
                // Sender dropped == the router cleared pending == connection dead.
                self.pending.lock().await.remove(&qid);
                Err(BrokerError::Transport(
                    "connection closed before reply".into(),
                ))
            }
            Err(_) => {
                // Timed out: drop our waiter so a late reply isn't mis-routed, and
                // tell the worker to stop the abandoned run (out-of-band, #50 parity).
                self.pending.lock().await.remove(&qid);
                let _ = self.cancel(target, &qid).await;
                Err(BrokerError::Transport(format!(
                    "request to '{target}' timed out after {timeout:?}"
                )))
            }
        }
    }

    /// Send a `Deliver` frame (under the brief sink lock), then consume ONE
    /// delivery receipt (serialized on `delivered`).
    ///
    /// Under concurrency the receipt consumed may be a DIFFERENT in-flight
    /// deliver's (receipts are FIFO over one socket; we take the next one, not a
    /// by-id match). So this confirms "the connection is live and *some*
    /// concurrent deliver was enqueued", not necessarily THIS message's. That is
    /// safe ONLY because the sole caller, `request()`, ignores this returned id
    /// and gates real success on the correlated REPLY (with its own timeout): a
    /// frame that was secretly dropped simply produces no reply → a timeout, never
    /// a false success. INVARIANT: keep `deliver` private and `request` its only
    /// consumer — a caller that trusted the receipt as a per-message ack would be
    /// wrong under concurrency.
    async fn deliver(&self, to: &str, message: InboxMessage) -> BrokerResult<MsgId> {
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
        match tokio::time::timeout(DELIVER_RECEIPT_TIMEOUT, delivered.recv()).await {
            Ok(Some(_)) => Ok(id),
            Ok(None) => Err(BrokerError::Transport(
                "connection closed before delivery receipt".into(),
            )),
            Err(_) => Err(BrokerError::Transport(
                "timed out waiting for delivery receipt from broker".into(),
            )),
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
            client.pending.lock().await.is_empty(),
            "the timed-out waiter was unregistered"
        );
    }
}
