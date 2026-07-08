use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tracing::{trace, warn};

use crate::error::{McpError, Result};
use crate::protocol::models::*;
use crate::types::{McpCallResult, McpTool};

/// Capacity of the server-notification queue. Notifications are dispatched off
/// the SAME inbound message-handler loop as JSON-RPC responses, so a *blocking*
/// send here would wedge that loop — and stall all response delivery — once the
/// buffer fills. Sends into it are therefore non-blocking (drop-on-full); see
/// the `handle_message` private method.
///
/// The RECEIVE side is drained by a dedicated consumer that takes the receiver
/// via [`take_notification_receiver`](McpProtocolClient::take_notification_receiver)
/// and awaits `recv()` on it (the manager's per-connection drain task). So in
/// steady state the queue is continuously emptied; the drop-on-full behavior is
/// only a safety valve for a burst that momentarily outruns the consumer, never
/// the normal path. #366.
const NOTIFICATION_CHANNEL_CAPACITY: usize = 100;

/// Transport trait for MCP communication
#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send(&self, message: String) -> Result<()>;

    /// Returns the inbound message channel receiver for efficient, non-polling
    /// consumption.
    ///
    /// The receiver yields `Some(message)` for each inbound message and `None`
    /// when the transport disconnects or its background reader task ends (EOF /
    /// stream error). A consumer that awaits `receiver.recv()` parks with zero
    /// wakeups while idle — no polling, no sleep, no per-iteration lock.
    ///
    /// Should be called once right after [`connect`](Self::connect). Returns
    /// `None` when the transport is not connected or the receiver was already
    /// taken. Once taken, [`receive`](Self::receive) is no longer usable.
    async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>>;

    /// Polls for a single inbound message with a short internal timeout.
    ///
    /// This is the legacy receive path retained for backward compatibility and
    /// unit tests. Production callers should prefer
    /// [`take_message_receiver`](Self::take_message_receiver) to avoid
    /// busy-waiting.
    async fn receive(&self) -> Result<Option<String>>;

    fn is_connected(&self) -> bool;
}

/// Pending request waiting for response
struct PendingRequest {
    sender: oneshot::Sender<Result<JsonRpcResponse>>,
}

/// MCP protocol client
pub struct McpProtocolClient {
    transport: Arc<RwLock<Box<dyn McpTransport>>>,
    next_id: AtomicU64,
    pending_requests: Arc<RwLock<std::collections::HashMap<u64, PendingRequest>>>,
    message_handler: Option<tokio::task::JoinHandle<()>>,
    notification_tx: mpsc::Sender<JsonRpcNotification>,
    /// Receiver half of the server-notification queue. Wrapped in
    /// `Mutex<Option<..>>` so a single production consumer can take ownership via
    /// [`take_notification_receiver`](Self::take_notification_receiver) and drain
    /// it with `recv().await` (no client lock held across the await), while unit
    /// tests can still poll it in place via
    /// [`try_receive_notification`](Self::try_receive_notification). #366.
    notification_rx: Mutex<Option<mpsc::Receiver<JsonRpcNotification>>>,
    /// Whether the notification queue is currently in a full/dropping episode.
    /// Gates the drop `warn!` to once per episode instead of once per dropped
    /// notification, so a chatty server can't produce continuous warn spam. #366.
    notification_queue_full: Arc<AtomicBool>,
}

impl McpProtocolClient {
    pub fn new(transport: Box<dyn McpTransport>) -> Self {
        let (notification_tx, notification_rx) = mpsc::channel(NOTIFICATION_CHANNEL_CAPACITY);
        Self {
            transport: Arc::new(RwLock::new(transport)),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(std::collections::HashMap::new())),
            message_handler: None,
            notification_tx,
            notification_rx: Mutex::new(Some(notification_rx)),
            notification_queue_full: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let mut transport = self.transport.write().await;
        transport.connect().await?;

        // Take the inbound message receiver once — the handler will own it and
        // consume messages directly from the channel without touching the
        // transport (no per-iteration RwLock, no polling, no sleep).
        let receiver = transport.take_message_receiver().await;
        drop(transport);

        if let Some(receiver) = receiver {
            self.start_message_handler(receiver);
        } else {
            warn!(
                "Transport did not provide a message receiver; \
                 message handler will not be started"
            );
        }

        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        // Abort the handler first so it stops consuming from the channel.
        if let Some(handler) = self.message_handler.take() {
            handler.abort();
        }

        let mut transport = self.transport.write().await;
        transport.disconnect().await
    }

    /// Spawns the message handler task that consumes inbound messages directly
    /// from the channel receiver.
    ///
    /// The task parks efficiently on `receiver.recv().await` — zero wakeups
    /// while idle, no transport lock acquisition, no sleep. It exits cleanly
    /// when the channel closes (transport disconnected / background reader
    /// ended) or is aborted during [`disconnect`](Self::disconnect).
    fn start_message_handler(&mut self, mut receiver: mpsc::Receiver<String>) {
        let pending_requests = self.pending_requests.clone();
        let notification_tx = self.notification_tx.clone();
        let notification_queue_full = self.notification_queue_full.clone();

        let handler = tokio::spawn(async move {
            // Await the next message from the channel. When the channel closes
            // (all senders dropped — transport disconnected, EOF, or shutdown),
            // `recv()` returns `None` and the loop exits gracefully.
            while let Some(message) = receiver.recv().await {
                // Raw inbound wire messages can be extremely noisy and may contain secrets.
                trace!("Received message (bytes={})", message.len());
                if let Err(e) = Self::handle_message(
                    &message,
                    &pending_requests,
                    &notification_tx,
                    &notification_queue_full,
                )
                .await
                {
                    warn!("Failed to handle message: {}", e);
                }
            }
            trace!("MCP message handler exited (channel closed)");
        });

        self.message_handler = Some(handler);
    }

    async fn handle_message(
        message: &str,
        pending_requests: &RwLock<std::collections::HashMap<u64, PendingRequest>>,
        notification_tx: &mpsc::Sender<JsonRpcNotification>,
        notification_queue_full: &AtomicBool,
    ) -> Result<()> {
        // Try to parse as response
        if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(message) {
            let mut pending = pending_requests.write().await;
            if let Some(request) = pending.remove(&response.id) {
                trace!("MCP JSON-RPC response matched (id={})", response.id);
                let _ = request.sender.send(Ok(response));
            } else {
                // Common in transport/proxy bugs: responses arrive but the client never registered
                // the request, or IDs got out of sync.
                warn!(
                    "MCP JSON-RPC response had no pending request (id={})",
                    response.id
                );
            }
            return Ok(());
        }

        // Try to parse as notification
        if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(message) {
            trace!(
                "MCP JSON-RPC notification received (method={})",
                notification.method
            );
            // Non-blocking: this handler loop ALSO matches JSON-RPC responses to
            // their pending requests, so a blocking `send().await` would wedge
            // response delivery once the (undrained) queue fills — timing out
            // every in-flight call and, under auto-reconnect, causing a recycle
            // storm. Drop the notification rather than ever stall responses.
            match notification_tx.try_send(notification) {
                Ok(()) => {
                    // Queue accepted again → end the drop episode so the next
                    // saturation logs once more. #366.
                    notification_queue_full.store(false, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    // Log once per full/dropping episode, not once per dropped
                    // notification — a chatty server would otherwise emit
                    // continuous warn-level spam (and feed alerting). #366.
                    if Self::note_dropped_notification(notification_queue_full) {
                        warn!(
                            "MCP notification queue full (cap={}); dropping notifications until it drains (first dropped method={})",
                            NOTIFICATION_CHANNEL_CAPACITY, dropped.method
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    trace!("MCP notification receiver dropped; ignoring notification")
                }
            }
            return Ok(());
        }

        Err(McpError::Protocol("Unknown message type".to_string()))
    }

    /// Records that a notification was dropped because the queue was full, and
    /// returns whether this drop OPENS a new full/dropping episode — i.e. whether
    /// the caller should emit the drop `warn!`.
    ///
    /// The first drop of an episode returns `true` (log once); every subsequent
    /// drop while the queue stays full returns `false` (stay quiet), so a chatty
    /// server can't produce continuous warn spam. The episode ends when a
    /// notification is next accepted (`handle_message` clears the flag), after
    /// which the next saturation logs again. Extracted so the once-per-episode
    /// cadence is unit-testable without a tracing subscriber. #366.
    fn note_dropped_notification(queue_full: &AtomicBool) -> bool {
        !queue_full.swap(true, Ordering::Relaxed)
    }

    async fn send_request(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: u64,
    ) -> Result<JsonRpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let request = JsonRpcRequest::new(id, method, params);
        let request_json = serde_json::to_string(&request)?;
        trace!(
            "MCP JSON-RPC request send (id={}, method={}, timeout_ms={})",
            id,
            method,
            timeout_ms
        );

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(id, PendingRequest { sender: tx });
        }

        let transport = self.transport.read().await;
        if let Err(e) = transport.send(request_json).await {
            // Avoid leaking pending requests on send failure.
            self.pending_requests.write().await.remove(&id);
            warn!(
                "MCP JSON-RPC request send failed (id={}, method={}): {}",
                id, method, e
            );
            return Err(e);
        }
        drop(transport);

        match tokio::time::timeout(tokio::time::Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(Ok(response))) => {
                if let Some(error) = response.error {
                    Err(McpError::Protocol(format!(
                        "{}: {}",
                        error.code, error.message
                    )))
                } else {
                    Ok(response)
                }
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(McpError::Disconnected),
            Err(_) => {
                self.pending_requests.write().await.remove(&id);
                warn!(
                    "MCP JSON-RPC request timed out (id={}, method={}, timeout_ms={})",
                    id, method, timeout_ms
                );
                Err(McpError::Timeout(format!(
                    "Request {} timed out after {}ms",
                    id, timeout_ms
                )))
            }
        }
    }

    pub async fn initialize(&self, timeout_ms: u64) -> Result<McpInitializeResult> {
        let request = McpInitializeRequest::default();
        let params = serde_json::to_value(request)?;

        let response = self
            .send_request("initialize", Some(params), timeout_ms)
            .await?;

        let result: McpInitializeResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
        )?;

        // Send initialized notification
        let initialized = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        let transport = self.transport.read().await;
        transport.send(serde_json::to_string(&initialized)?).await?;

        Ok(result)
    }

    pub async fn list_tools(&self, timeout_ms: u64) -> Result<Vec<McpTool>> {
        let response = self.send_request("tools/list", None, timeout_ms).await?;

        let result: McpToolListResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
        )?;

        Ok(result
            .tools
            .into_iter()
            .map(|t| McpTool {
                name: t.name,
                description: t.description,
                parameters: t.input_schema.unwrap_or_else(|| serde_json::json!({})),
            })
            .collect())
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<McpCallResult> {
        let request = McpToolCallRequest {
            name: name.to_string(),
            arguments: Some(arguments),
        };
        let params = serde_json::to_value(request)?;

        let response = self
            .send_request("tools/call", Some(params), timeout_ms)
            .await?;

        let result: McpToolCallResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
        )?;

        Ok(McpCallResult {
            content: result.content,
            is_error: result.is_error,
        })
    }

    pub async fn ping(&self, timeout_ms: u64) -> Result<()> {
        self.send_request("ping", None, timeout_ms).await?;
        Ok(())
    }

    /// Non-blocking poll of the next buffered server notification. Retained for
    /// unit tests; production drains via [`take_notification_receiver`] instead.
    /// Returns `None` if the queue is empty or the receiver was already taken.
    ///
    /// [`take_notification_receiver`]: Self::take_notification_receiver
    pub async fn try_receive_notification(&self) -> Option<JsonRpcNotification> {
        let mut guard = self.notification_rx.lock().await;
        guard.as_mut()?.try_recv().ok()
    }

    /// Takes ownership of the server-notification receiver so a dedicated consumer
    /// can drain the queue by awaiting `recv()` on it directly — no client lock
    /// held across the await, and the queue is actually emptied instead of
    /// silently filling to capacity and dropping every later notification.
    ///
    /// Called once per connection by the manager's drain task (see
    /// [`McpServerManager`](crate::McpServerManager)). Returns `None` if the
    /// receiver was already taken. When the client is dropped or disconnected all
    /// notification senders close, so the consumer's `recv()` yields `None` and it
    /// exits cleanly. #366.
    pub async fn take_notification_receiver(&self) -> Option<mpsc::Receiver<JsonRpcNotification>> {
        self.notification_rx.lock().await.take()
    }

    pub async fn is_connected(&self) -> bool {
        let transport = self.transport.read().await;
        transport.is_connected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::sync::Mutex as TokioMutex;

    // Mock transport for testing — backed by an mpsc channel so that
    // `take_message_receiver` hands the client a real receiver.
    struct MockTransport {
        connected: bool,
        messages_sent: Arc<RwLock<Vec<String>>>,
        message_rx: TokioMutex<Option<mpsc::Receiver<String>>>,
        // Holding the sender keeps the channel open (idle handler parks, no EOF).
        _message_tx: Option<mpsc::Sender<String>>,
    }

    impl MockTransport {
        fn new() -> Self {
            let (tx, rx) = mpsc::channel(100);
            Self {
                connected: false,
                messages_sent: Arc::new(RwLock::new(Vec::new())),
                message_rx: TokioMutex::new(Some(rx)),
                _message_tx: Some(tx),
            }
        }

        fn with_response(message: String) -> Self {
            Self::with_messages(vec![message])
        }

        /// Pre-loads `messages` into the channel then drops the sender,
        /// simulating a server that sends N messages and closes (EOF).
        fn with_messages(messages: Vec<String>) -> Self {
            let (tx, rx) = mpsc::channel(100);
            for msg in &messages {
                let _ = tx.try_send(msg.clone());
            }
            drop(tx);
            Self {
                connected: false,
                messages_sent: Arc::new(RwLock::new(Vec::new())),
                message_rx: TokioMutex::new(Some(rx)),
                _message_tx: None,
            }
        }
    }

    #[async_trait]
    impl McpTransport for MockTransport {
        async fn connect(&mut self) -> Result<()> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.connected = false;
            Ok(())
        }

        async fn send(&self, message: String) -> Result<()> {
            let mut sent = self.messages_sent.write().await;
            sent.push(message);
            Ok(())
        }

        async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
            self.message_rx.lock().await.take()
        }

        async fn receive(&self) -> Result<Option<String>> {
            let mut guard = self.message_rx.lock().await;
            match guard.as_mut() {
                None => Err(McpError::Disconnected),
                Some(rx) => {
                    match tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv())
                        .await
                    {
                        Ok(Some(msg)) => Ok(Some(msg)),
                        Ok(None) => Err(McpError::Disconnected),
                        Err(_) => Ok(None),
                    }
                }
            }
        }

        fn is_connected(&self) -> bool {
            self.connected
        }
    }

    // Echo transport: replies to each request with a response carrying the SAME
    // JSON-RPC id after a delay, simulating a real stdio server that multiplexes
    // concurrent requests over one pipe and may reply out of order. #148.
    struct EchoTransport {
        connected: bool,
        message_rx: TokioMutex<Option<mpsc::Receiver<String>>>,
        response_tx: mpsc::Sender<String>,
        /// Base reply delay; the per-request delay is `base - n*5` (see `send`),
        /// so it must exceed `max_n * 5` to stay positive.
        base_delay_ms: u64,
    }

    impl EchoTransport {
        fn new(base_delay_ms: u64) -> Self {
            let (tx, rx) = mpsc::channel(100);
            Self {
                connected: false,
                message_rx: TokioMutex::new(Some(rx)),
                response_tx: tx,
                base_delay_ms,
            }
        }
    }

    #[async_trait]
    impl McpTransport for EchoTransport {
        async fn connect(&mut self) -> Result<()> {
            self.connected = true;
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<()> {
            self.connected = false;
            Ok(())
        }
        async fn send(&self, message: String) -> Result<()> {
            let req: serde_json::Value = serde_json::from_str(&message).expect("valid request");
            let id = req["id"].clone();
            let tx = self.response_tx.clone();
            // Reply time is INVERTED vs. submission order: a higher `n` gets a
            // shorter delay, so the LAST-issued request's response arrives FIRST.
            // This forces a deterministically reversed arrival order, so a
            // (hypothetical, buggy) match-by-arrival-order implementation would
            // fail here — not just pass by coincidence of scheduling. #148.
            let n = req["params"]["n"].as_u64().unwrap_or(0);
            let delay = self.base_delay_ms.saturating_sub(n * 5);
            tokio::spawn(async move {
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "echoed_id": id },
                });
                let _ = tx.send(resp.to_string()).await;
            });
            Ok(())
        }
        async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
            self.message_rx.lock().await.take()
        }
        async fn receive(&self) -> Result<Option<String>> {
            Err(McpError::Disconnected)
        }
        fn is_connected(&self) -> bool {
            self.connected
        }
    }

    #[tokio::test]
    async fn concurrent_same_server_requests_correlate_by_id() {
        // #148: serve_mcp_proxy (post-#144) issues N CONCURRENT call_tool to the
        // SAME stdio server. Verify the client correlates each response to its own
        // request by JSON-RPC id — even when replies arrive out of order — rather
        // than reading "the next line". A crossed id would mis-route or time out;
        // the stdin Mutex (transports/stdio.rs) also keeps concurrent writes from
        // interleaving on the wire. Base 100ms − n*5 keeps all 16 delays positive
        // (100..25) while reversing arrival order (see EchoTransport::send).
        let mut client = McpProtocolClient::new(Box::new(EchoTransport::new(100)));
        client.connect().await.expect("connect");
        let client = Arc::new(client);

        let mut handles = Vec::new();
        for i in 0..16u64 {
            let c = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                c.send_request("tools/call", Some(serde_json::json!({ "n": i })), 2000)
                    .await
                    .expect("concurrent request should succeed")
            }));
        }
        for handle in handles {
            let resp = handle.await.expect("task join");
            // The echoed id in the result MUST equal this response's own request id.
            let echoed = resp
                .result
                .as_ref()
                .and_then(|r| r.get("echoed_id"))
                .and_then(|v| v.as_u64());
            assert_eq!(
                echoed,
                Some(resp.id),
                "each concurrent response must carry its OWN request id"
            );
        }
    }

    #[tokio::test]
    async fn test_client_new() {
        let transport = Box::new(MockTransport::new());
        let client = McpProtocolClient::new(transport);
        assert!(client.message_handler.is_none());
    }

    #[tokio::test]
    async fn test_client_connect() {
        let transport = Box::new(MockTransport::new());
        let mut client = McpProtocolClient::new(transport);

        let result = client.connect().await;
        assert!(result.is_ok());
        assert!(client.message_handler.is_some());
        assert!(client.is_connected().await);
    }

    #[tokio::test]
    async fn test_client_disconnect() {
        let transport = Box::new(MockTransport::new());
        let mut client = McpProtocolClient::new(transport);

        client.connect().await.unwrap();
        assert!(client.is_connected().await);

        let result = client.disconnect().await;
        assert!(result.is_ok());
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_client_is_connected() {
        let transport = Box::new(MockTransport::new());
        let mut client = McpProtocolClient::new(transport);

        assert!(!client.is_connected().await);
        client.connect().await.unwrap();
        assert!(client.is_connected().await);
    }

    #[test]
    fn test_json_rpc_request_new() {
        let request =
            JsonRpcRequest::new(1, "test/method", Some(serde_json::json!({"key": "value"})));
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.id, 1);
        assert_eq!(request.method, "test/method");
        assert!(request.params.is_some());
    }

    #[tokio::test]
    async fn test_send_request_timeout() {
        let transport = Box::new(MockTransport::new()); // Won't respond
        let client = McpProtocolClient::new(transport);

        let result = client.send_request("test", None, 100).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Timeout(_) => {}
            _ => panic!("Expected Timeout error"),
        }
    }

    #[tokio::test]
    async fn test_send_request_receives_response() {
        let response = JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: 1,
            result: Some(serde_json::json!({"status": "ok"})),
            error: None,
        };
        let message = serde_json::to_string(&response).unwrap();

        let transport = Box::new(MockTransport::with_response(message));
        let mut client = McpProtocolClient::new(transport);
        client.connect().await.unwrap();

        let result = client
            .send_request("test/method", None, 1000)
            .await
            .unwrap();
        assert_eq!(result.id, 1);
        assert!(result.result.is_some());
    }

    #[test]
    fn test_pending_request() {
        let (tx, _rx) = oneshot::channel();
        let _pending = PendingRequest { sender: tx };

        // Send a response
        let response = JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: 1,
            result: Some(serde_json::json!({"status": "ok"})),
            error: None,
        };

        // Use a separate sender since tx was moved into pending
        let (tx2, rx2): (oneshot::Sender<Result<JsonRpcResponse>>, _) = oneshot::channel();
        tx2.send(Ok(response)).unwrap();

        // Receive it
        let result = rx2.blocking_recv().unwrap().unwrap();
        assert_eq!(result.id, 1);
        assert!(result.result.is_some());
    }

    #[tokio::test]
    async fn full_notification_queue_does_not_block_response_dispatch() {
        use std::collections::HashMap;

        // A deliberately tiny notification queue, filled to capacity — mirrors a
        // chatty server whose notifications outrun the (undrained) queue.
        let (notif_tx, _notif_rx) = mpsc::channel::<JsonRpcNotification>(1);
        let fill: JsonRpcNotification = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"notifications/message","params":{}}"#,
        )
        .unwrap();
        notif_tx
            .try_send(fill)
            .expect("first notification fills the cap-1 queue");

        let queue_full = AtomicBool::new(false);

        // A pending request awaiting its JSON-RPC response.
        let pending: RwLock<HashMap<u64, PendingRequest>> = RwLock::new(HashMap::new());
        let (resp_tx, resp_rx) = oneshot::channel();
        pending
            .write()
            .await
            .insert(7, PendingRequest { sender: resp_tx });

        // 1) Handling ANOTHER notification while the queue is full must return
        //    promptly. The old blocking `send().await` would hang here forever,
        //    wedging the shared handler loop (and all response delivery with it).
        let notif_json = r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{}}"#;
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            McpProtocolClient::handle_message(notif_json, &pending, &notif_tx, &queue_full),
        )
        .await
        .expect("handle_message must not block on a full notification queue")
        .expect("handle_message returns Ok");

        // The drop `warn!` fires once per episode: the queue is still full, so a
        // second dropped notification must NOT re-flag (episode already open). #366.
        assert!(
            queue_full.load(Ordering::Relaxed),
            "queue-full episode should be flagged after the first drop"
        );

        // 2) A JSON-RPC response must still be dispatched to its pending request,
        //    even though the notification queue is saturated.
        let resp_json = r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#;
        McpProtocolClient::handle_message(resp_json, &pending, &notif_tx, &queue_full)
            .await
            .expect("handle_message returns Ok");
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
            .await
            .expect("response must be delivered despite a full notification queue");
        assert!(
            delivered.is_ok(),
            "the pending request should receive its response"
        );
    }

    /// Verifies that the channel-based handler delivers N messages in order
    /// and exits cleanly when the channel closes (simulated EOF).
    #[tokio::test]
    async fn test_handler_delivers_messages_in_order_and_exits_on_eof() {
        let n = 10;
        let messages: Vec<String> = (0..n)
            .map(|i| {
                serde_json::to_string(&JsonRpcNotification {
                    jsonrpc: "2.0".to_string(),
                    method: format!("test/event/{i}"),
                    params: None,
                })
                .unwrap()
            })
            .collect();

        let transport = Box::new(MockTransport::with_messages(messages));
        let mut client = McpProtocolClient::new(transport);
        client.connect().await.unwrap();

        // Give the handler time to process all messages.
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Drain received notifications.
        let mut received: Vec<String> = Vec::new();
        while let Some(notif) = client.try_receive_notification().await {
            received.push(notif.method);
        }
        assert_eq!(received.len(), n, "all notifications should be delivered");
        for (i, method) in received.iter().enumerate() {
            assert_eq!(method, &format!("test/event/{i}"), "order preserved");
        }

        // After all messages consumed + sender dropped (EOF), the handler
        // should have exited cleanly.
        if let Some(handler) = &client.message_handler {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            assert!(
                handler.is_finished(),
                "handler should have exited after channel closed (EOF)"
            );
        }

        let _ = client.disconnect().await;
    }

    /// #366: with a real consumer, the notification queue is emptied — even after
    /// it saturates and over-capacity notifications are dropped (non-blocking).
    /// Before the fix the queue had NO production consumer, so it filled to
    /// capacity and every later notification was dropped forever.
    #[tokio::test]
    async fn drain_consumer_empties_saturated_notification_channel() {
        use std::collections::HashMap;

        let (tx, mut rx) = mpsc::channel::<JsonRpcNotification>(NOTIFICATION_CHANNEL_CAPACITY);
        let pending: RwLock<HashMap<u64, PendingRequest>> = RwLock::new(HashMap::new());
        let queue_full = AtomicBool::new(false);

        // Fill exactly to capacity via the production send path — all accepted.
        for i in 0..NOTIFICATION_CHANNEL_CAPACITY {
            let json = format!(r#"{{"jsonrpc":"2.0","method":"notifications/message/{i}"}}"#);
            McpProtocolClient::handle_message(&json, &pending, &tx, &queue_full)
                .await
                .expect("handle_message returns Ok");
        }
        assert!(
            !queue_full.load(Ordering::Relaxed),
            "filling to exactly capacity should not open a drop episode"
        );

        // Over-fill: extra notifications DROP (non-blocking) rather than block —
        // the #363 invariant. A blocking send would hang here forever.
        for i in 0..10 {
            let json = format!(r#"{{"jsonrpc":"2.0","method":"notifications/overflow/{i}"}}"#);
            tokio::time::timeout(
                std::time::Duration::from_millis(500),
                McpProtocolClient::handle_message(&json, &pending, &tx, &queue_full),
            )
            .await
            .expect("over-capacity send must not block")
            .expect("handle_message returns Ok");
        }
        assert!(
            queue_full.load(Ordering::Relaxed),
            "over-filling should open a drop episode"
        );

        // Now run the consumer (as the manager's drain task does). It must fully
        // empty the channel. Close the sender so `recv()` ends once drained.
        drop(tx);
        let mut drained = 0usize;
        while rx.recv().await.is_some() {
            drained += 1;
        }
        assert_eq!(
            drained, NOTIFICATION_CHANNEL_CAPACITY,
            "the consumer must empty every buffered notification"
        );
    }

    /// #366: the drop `warn!` fires once per full/dropping EPISODE, not once per
    /// dropped notification. `note_dropped_notification` is the gate: it returns
    /// `true` only for the drop that OPENS an episode. With the old per-drop
    /// behavior (an unconditional `warn!`) every drop would log; this asserts the
    /// 2nd/3rd drops in an episode are suppressed and a fresh episode logs again.
    #[test]
    fn drop_warn_fires_once_per_saturation_episode() {
        let queue_full = AtomicBool::new(false);

        // First drop of an episode -> should log.
        assert!(
            McpProtocolClient::note_dropped_notification(&queue_full),
            "first drop opens the episode and should log"
        );
        // Repeats within the SAME episode -> suppressed.
        assert!(
            !McpProtocolClient::note_dropped_notification(&queue_full),
            "a second drop in the same episode must not log"
        );
        assert!(
            !McpProtocolClient::note_dropped_notification(&queue_full),
            "further drops in the same episode must not log"
        );

        // The episode ends when the queue drains (a notification is accepted).
        queue_full.store(false, Ordering::Relaxed);

        // A later saturation opens a NEW episode -> logs once more.
        assert!(
            McpProtocolClient::note_dropped_notification(&queue_full),
            "a new saturation episode should log again"
        );
        assert!(
            !McpProtocolClient::note_dropped_notification(&queue_full),
            "repeats in the new episode stay quiet"
        );
    }
}
