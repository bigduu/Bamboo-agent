use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tracing::{debug, trace, warn};

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

/// Bound the dual-era discovery probe so a legacy stdio server that silently
/// ignores pre-initialize requests does not consume the full operation timeout
/// (60 seconds by default) before Bamboo falls back to `initialize`.
const MODERN_DISCOVERY_PROBE_TIMEOUT_MS: u64 = 5_000;
const SUBSCRIPTION_ACK_METHOD: &str = "notifications/subscriptions/acknowledged";
const SUBSCRIPTION_ID_META_KEY: &str = "io.modelcontextprotocol/subscriptionId";

/// HTTP-envelope metadata derived from an MCP request.
///
/// Non-HTTP transports ignore this value. Streamable HTTP uses it to mirror
/// the modern protocol's body metadata into mandatory request headers.
#[derive(Debug, Clone, Default)]
pub struct McpTransportMetadata {
    /// JSON-RPC request ID, used to associate a Streamable HTTP response stream
    /// with the operation that owns it.
    pub request_id: Option<u64>,
    pub protocol_version: Option<String>,
    pub modern: bool,
    pub method: String,
    pub name: Option<String>,
    /// Raw `x-mcp-header` name/value pairs. The HTTP transport performs the
    /// required safe header-value encoding.
    pub tool_parameter_headers: Vec<(String, String)>,
}

/// Transport trait for MCP communication
#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send(&self, message: String) -> Result<()>;

    /// Send a message with protocol-derived envelope metadata.
    ///
    /// stdio and deprecated HTTP+SSE do not need the modern HTTP envelope, so
    /// their default implementation delegates to [`send`](Self::send).
    async fn send_with_metadata(
        &self,
        message: String,
        _metadata: McpTransportMetadata,
    ) -> Result<()> {
        self.send(message).await
    }

    /// Cancel an in-flight request after Bamboo stops waiting for its result.
    ///
    /// stdio cancellation is a JSON-RPC notification. Streamable HTTP
    /// overrides this hook and closes the request's response stream instead.
    async fn cancel_request(&self, request_id: u64, reason: &str) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/cancelled".to_string(),
            params: Some(serde_json::json!({
                "requestId": request_id,
                "reason": reason,
            })),
        };
        self.send(serde_json::to_string(&notification)?).await
    }

    /// Whether this transport can speak the stateless MCP 2026-07-28 protocol.
    fn supports_modern_protocol(&self) -> bool {
        true
    }

    /// Latest initialization-based protocol revision appropriate for fallback.
    fn latest_legacy_protocol_version(&self) -> &'static str {
        LATEST_LEGACY_PROTOCOL_VERSION
    }

    /// Whether tool `x-mcp-header` annotations must be validated and mirrored.
    fn requires_tool_parameter_headers(&self) -> bool {
        false
    }

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProtocolMode {
    Unnegotiated,
    Modern { version: String },
    Legacy { version: String },
}

#[derive(Debug, Clone)]
enum RequestProtocol {
    Modern { version: String },
    Legacy { version: Option<String> },
}

enum ModernDiscoveryError {
    /// No successful modern response was received, so a legacy probe is safe.
    Fallback(McpError),
    /// A modern response or a normative modern error pinned the protocol era.
    Pinned(McpError),
}

#[derive(Default)]
struct ToolSubscriptionState {
    id: Option<u64>,
    acknowledged: bool,
    ack_sender: Option<oneshot::Sender<Result<()>>>,
}

#[derive(Debug, Clone, Copy)]
enum ToolHeaderValueType {
    String,
    Integer,
    Boolean,
}

#[derive(Debug, Clone)]
struct ToolHeaderSpec {
    name: String,
    path: Vec<String>,
    value_type: ToolHeaderValueType,
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
    protocol_mode: RwLock<ProtocolMode>,
    tool_header_specs: RwLock<HashMap<String, Vec<ToolHeaderSpec>>>,
    /// Long-lived `subscriptions/listen` request used by modern servers to
    /// deliver `notifications/tools/list_changed`.
    tool_subscription: Arc<Mutex<ToolSubscriptionState>>,
    /// Whether the current modern discovery result requires a tool-change
    /// subscription. Shared with the inbound handler for stdio correlation.
    modern_tool_list_changes: Arc<AtomicBool>,
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
            protocol_mode: RwLock::new(ProtocolMode::Unnegotiated),
            tool_header_specs: RwLock::new(HashMap::new()),
            tool_subscription: Arc::new(Mutex::new(ToolSubscriptionState::default())),
            modern_tool_list_changes: Arc::new(AtomicBool::new(false)),
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

        self.pending_requests.write().await.clear();
        let mut subscription = self.tool_subscription.lock().await;
        if let Some(sender) = subscription.ack_sender.take() {
            let _ = sender.send(Err(McpError::Disconnected));
        }
        *subscription = ToolSubscriptionState::default();
        self.modern_tool_list_changes.store(false, Ordering::SeqCst);
        drop(subscription);

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
        let tool_subscription = self.tool_subscription.clone();
        let modern_tool_list_changes = self.modern_tool_list_changes.clone();

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
                    &tool_subscription,
                    &modern_tool_list_changes,
                )
                .await
                {
                    warn!("Failed to handle message: {}", e);
                }
            }
            let mut pending = pending_requests.write().await;
            for (_, request) in pending.drain() {
                let _ = request.sender.send(Err(McpError::Disconnected));
            }
            drop(pending);
            let mut subscription = tool_subscription.lock().await;
            if let Some(sender) = subscription.ack_sender.take() {
                let _ = sender.send(Err(McpError::Disconnected));
            }
            *subscription = ToolSubscriptionState::default();
            trace!("MCP message handler exited (channel closed)");
        });

        self.message_handler = Some(handler);
    }

    async fn handle_message(
        message: &str,
        pending_requests: &RwLock<std::collections::HashMap<u64, PendingRequest>>,
        notification_tx: &mpsc::Sender<JsonRpcNotification>,
        notification_queue_full: &AtomicBool,
        tool_subscription: &Mutex<ToolSubscriptionState>,
        modern_tool_list_changes: &AtomicBool,
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
            if Self::handle_subscription_notification(
                &notification,
                pending_requests,
                tool_subscription,
                modern_tool_list_changes,
            )
            .await?
            {
                return Ok(());
            }
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

    async fn handle_subscription_notification(
        notification: &JsonRpcNotification,
        pending_requests: &RwLock<std::collections::HashMap<u64, PendingRequest>>,
        tool_subscription: &Mutex<ToolSubscriptionState>,
        modern_tool_list_changes: &AtomicBool,
    ) -> Result<bool> {
        if notification.method == SUBSCRIPTION_ACK_METHOD {
            let received_id = Self::notification_subscription_id(notification);
            let accepts_tool_changes = notification
                .params
                .as_ref()
                .and_then(|params| params.get("notifications"))
                .and_then(|notifications| notifications.get("toolsListChanged"))
                .and_then(Value::as_bool)
                == Some(true);
            let mut subscription = tool_subscription.lock().await;
            let Some(expected_id) = subscription.id else {
                warn!("Ignoring MCP subscription acknowledgment with no active subscription");
                return Ok(true);
            };
            if received_id != Some(expected_id) {
                warn!(
                    "Ignoring MCP subscription acknowledgment with mismatched id (expected={:?}, received={:?})",
                    subscription.id, received_id
                );
                return Ok(true);
            }
            if !accepts_tool_changes {
                let error = McpError::Protocol(
                    "MCP subscription acknowledgment omitted requested toolsListChanged"
                        .to_string(),
                );
                if let Some(sender) = subscription.ack_sender.take() {
                    let _ = sender.send(Err(error));
                }
                return Ok(true);
            }
            subscription.acknowledged = true;
            if let Some(sender) = subscription.ack_sender.take() {
                let _ = sender.send(Ok(()));
            }
            return Ok(true);
        }

        if notification.method == "notifications/cancelled" {
            let request_id = notification
                .params
                .as_ref()
                .and_then(|params| params.get("requestId"))
                .and_then(Value::as_u64);
            let mut subscription = tool_subscription.lock().await;
            if let Some(id) = request_id.filter(|id| Some(*id) == subscription.id) {
                if let Some(sender) = subscription.ack_sender.take() {
                    let _ = sender.send(Err(McpError::Protocol(
                        "MCP server cancelled subscriptions/listen".to_string(),
                    )));
                }
                *subscription = ToolSubscriptionState::default();
                drop(subscription);
                if let Some(request) = pending_requests.write().await.remove(&id) {
                    let _ = request.sender.send(Err(McpError::Protocol(
                        "MCP server cancelled subscriptions/listen".to_string(),
                    )));
                }
                return Ok(true);
            }
        }

        if notification.method == "notifications/tools/list_changed"
            && modern_tool_list_changes.load(Ordering::SeqCst)
        {
            let received_id = Self::notification_subscription_id(notification);
            let subscription = tool_subscription.lock().await;
            if !subscription.acknowledged || received_id != subscription.id {
                warn!(
                    "Ignoring uncorrelated MCP tools/list_changed notification (active={:?}, acknowledged={}, received={:?})",
                    subscription.id, subscription.acknowledged, received_id
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn notification_subscription_id(notification: &JsonRpcNotification) -> Option<u64> {
        notification
            .params
            .as_ref()
            .and_then(|params| params.get("_meta"))
            .and_then(|meta| meta.get(SUBSCRIPTION_ID_META_KEY))
            .and_then(Value::as_u64)
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
        self.send_request_with_tool_headers(method, params, timeout_ms, Vec::new())
            .await
    }

    async fn send_request_with_tool_headers(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: u64,
        tool_parameter_headers: Vec<(String, String)>,
    ) -> Result<JsonRpcResponse> {
        let protocol = match self.protocol_mode.read().await.clone() {
            ProtocolMode::Modern { version } => RequestProtocol::Modern { version },
            ProtocolMode::Legacy { version } => RequestProtocol::Legacy {
                version: Some(version),
            },
            // Kept for low-level tests and diagnostics. Production requests
            // negotiate before entering the operation phase.
            ProtocolMode::Unnegotiated => RequestProtocol::Legacy { version: None },
        };
        self.send_request_using(method, params, timeout_ms, protocol, tool_parameter_headers)
            .await
    }

    async fn send_request_using(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: u64,
        protocol: RequestProtocol,
        tool_parameter_headers: Vec<(String, String)>,
    ) -> Result<JsonRpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let params = match &protocol {
            RequestProtocol::Modern { version } => {
                Some(Self::with_modern_request_metadata(params, version)?)
            }
            RequestProtocol::Legacy { .. } => params,
        };
        let name = Self::request_name(method, params.as_ref());
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

        let metadata = match protocol {
            RequestProtocol::Modern { version } => McpTransportMetadata {
                request_id: Some(id),
                protocol_version: Some(version),
                modern: true,
                method: method.to_string(),
                name,
                tool_parameter_headers,
            },
            RequestProtocol::Legacy { version } => McpTransportMetadata {
                request_id: Some(id),
                protocol_version: version,
                modern: false,
                method: method.to_string(),
                name,
                tool_parameter_headers: Vec::new(),
            },
        };

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);
        let transport = self.transport.read().await;
        match tokio::time::timeout_at(
            deadline,
            transport.send_with_metadata(request_json, metadata),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                // Avoid leaking pending requests on send failure.
                self.pending_requests.write().await.remove(&id);
                warn!(
                    "MCP JSON-RPC request send failed (id={}, method={}): {}",
                    id, method, error
                );
                return Err(error);
            }
            Err(_) => {
                drop(transport);
                self.cancel_timed_out_request(id, method, timeout_ms).await;
                return Err(McpError::Timeout(format!(
                    "Request {id} timed out after {timeout_ms}ms"
                )));
            }
        }
        drop(transport);

        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(Ok(response))) => {
                if let Some(error) = response.error {
                    Err(McpError::RemoteProtocol {
                        code: error.code,
                        message: error.message,
                        data: error.data,
                    })
                } else {
                    Ok(response)
                }
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(McpError::Disconnected),
            Err(_) => {
                self.cancel_timed_out_request(id, method, timeout_ms).await;
                Err(McpError::Timeout(format!(
                    "Request {id} timed out after {timeout_ms}ms"
                )))
            }
        }
    }

    async fn cancel_timed_out_request(&self, id: u64, method: &str, timeout_ms: u64) {
        self.pending_requests.write().await.remove(&id);
        let reason = format!("Request timed out after {timeout_ms}ms");
        let transport = self.transport.read().await;
        if let Err(error) = transport.cancel_request(id, &reason).await {
            warn!(
                "MCP request cancellation failed after timeout (id={}, method={}): {}",
                id, method, error
            );
        }
        drop(transport);
        warn!(
            "MCP JSON-RPC request timed out (id={}, method={}, timeout_ms={})",
            id, method, timeout_ms
        );
    }

    fn with_modern_request_metadata(params: Option<Value>, version: &str) -> Result<Value> {
        let mut params = params.unwrap_or_else(|| serde_json::json!({}));
        let object = params.as_object_mut().ok_or_else(|| {
            McpError::Protocol("MCP request params must be a JSON object".to_string())
        })?;

        let meta = object
            .entry("_meta")
            .or_insert_with(|| serde_json::json!({}));
        let meta = meta.as_object_mut().ok_or_else(|| {
            McpError::Protocol("MCP request _meta must be a JSON object".to_string())
        })?;
        meta.insert(
            "io.modelcontextprotocol/protocolVersion".to_string(),
            Value::String(version.to_string()),
        );
        meta.insert(
            "io.modelcontextprotocol/clientInfo".to_string(),
            serde_json::json!({
                "name": "bamboo-agent",
                "version": env!("CARGO_PKG_VERSION"),
            }),
        );
        meta.insert(
            "io.modelcontextprotocol/clientCapabilities".to_string(),
            serde_json::to_value(ClientCapabilities::default())?,
        );
        Ok(params)
    }

    fn request_name(method: &str, params: Option<&Value>) -> Option<String> {
        let key = match method {
            "tools/call" | "prompts/get" => "name",
            "resources/read" => "uri",
            _ => return None,
        };
        params?.get(key)?.as_str().map(ToString::to_string)
    }

    pub async fn initialize(&self, timeout_ms: u64) -> Result<McpInitializeResult> {
        let supports_modern = self.transport.read().await.supports_modern_protocol();
        if supports_modern {
            let probe_timeout_ms = timeout_ms.min(MODERN_DISCOVERY_PROBE_TIMEOUT_MS);
            match self.discover_modern(probe_timeout_ms).await {
                Ok(result) => return Ok(result),
                Err(ModernDiscoveryError::Pinned(error)) => return Err(error),
                Err(ModernDiscoveryError::Fallback(error)) => {
                    debug!(
                        "MCP server did not complete modern discovery; falling back to legacy initialization: {}",
                        error
                    );
                }
            }
        }

        self.initialize_legacy(timeout_ms).await
    }

    async fn discover_modern(
        &self,
        timeout_ms: u64,
    ) -> std::result::Result<McpInitializeResult, ModernDiscoveryError> {
        let response = self
            .send_request_using(
                "server/discover",
                Some(serde_json::json!({})),
                timeout_ms,
                RequestProtocol::Modern {
                    version: LATEST_PROTOCOL_VERSION.to_string(),
                },
                Vec::new(),
            )
            .await
            .map_err(|error| {
                if Self::is_recognized_modern_error(&error) {
                    ModernDiscoveryError::Pinned(error)
                } else {
                    ModernDiscoveryError::Fallback(error)
                }
            })?;

        // A successful `server/discover` response pins the server to the
        // modern era. Any malformed or incompatible result must surface as a
        // protocol error rather than being retried as legacy initialization.
        *self.protocol_mode.write().await = ProtocolMode::Modern {
            version: LATEST_PROTOCOL_VERSION.to_string(),
        };

        let result: McpDiscoverResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing discovery result".to_string()))
                .map_err(ModernDiscoveryError::Pinned)?,
        )
        .map_err(McpError::from)
        .map_err(ModernDiscoveryError::Pinned)?;

        Self::validate_modern_discovery(&result).map_err(ModernDiscoveryError::Pinned)?;

        let server_info = result.server_info().unwrap_or_else(|| Implementation {
            name: "unnamed-mcp-server".to_string(),
            version: "unknown".to_string(),
        });
        let supports_tool_list_changes = result
            .capabilities
            .tools
            .as_ref()
            .is_some_and(|tools| tools.list_changed);
        let normalized = McpInitializeResult {
            protocol_version: LATEST_PROTOCOL_VERSION.to_string(),
            capabilities: result.capabilities,
            server_info,
            instructions: result.instructions,
        };
        self.modern_tool_list_changes
            .store(supports_tool_list_changes, Ordering::SeqCst);
        if supports_tool_list_changes {
            self.start_tool_change_subscription(timeout_ms)
                .await
                .map_err(ModernDiscoveryError::Pinned)?;
        }
        Ok(normalized)
    }

    fn validate_modern_discovery(result: &McpDiscoverResult) -> Result<()> {
        Self::ensure_complete_result(Some(&result.result_type), true)?;
        Self::ensure_cacheable_result(Some(result.ttl_ms), Some(&result.cache_scope), true)?;
        if !result
            .supported_versions
            .iter()
            .any(|version| version == LATEST_PROTOCOL_VERSION)
        {
            return Err(McpError::Protocol(format!(
                "MCP server does not advertise Bamboo's supported modern version {}; server supports [{}]",
                LATEST_PROTOCOL_VERSION,
                result.supported_versions.join(", ")
            )));
        }
        Ok(())
    }

    async fn start_tool_change_subscription(&self, timeout_ms: u64) -> Result<()> {
        {
            let subscription = self.tool_subscription.lock().await;
            if subscription.id.is_some() {
                return if subscription.acknowledged {
                    Ok(())
                } else {
                    Err(McpError::Protocol(
                        "MCP tool-change subscription acknowledgment is still pending".to_string(),
                    ))
                };
            }
        }

        let version = match self.protocol_mode.read().await.clone() {
            ProtocolMode::Modern { version } => version,
            _ => {
                return Err(McpError::Protocol(
                    "Tool-change subscriptions require modern MCP".to_string(),
                ))
            }
        };
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (ack_tx, ack_rx) = oneshot::channel();
        {
            let mut subscription = self.tool_subscription.lock().await;
            subscription.id = Some(id);
            subscription.acknowledged = false;
            subscription.ack_sender = Some(ack_tx);
        }
        let params = Self::with_modern_request_metadata(
            Some(serde_json::json!({
                "notifications": {
                    "toolsListChanged": true
                }
            })),
            &version,
        )?;
        let request = JsonRpcRequest::new(id, "subscriptions/listen", Some(params));
        let request_json = serde_json::to_string(&request)?;

        // The request intentionally remains in flight for the lifetime of the
        // connection. Retain a pending entry so a graceful close is correlated
        // instead of logged as an unmatched response.
        let (tx, rx) = oneshot::channel();
        self.pending_requests
            .write()
            .await
            .insert(id, PendingRequest { sender: tx });

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);
        let transport = self.transport.read().await;
        let send_result = tokio::time::timeout_at(
            deadline,
            transport.send_with_metadata(
                request_json,
                McpTransportMetadata {
                    request_id: Some(id),
                    protocol_version: Some(version),
                    modern: true,
                    method: "subscriptions/listen".to_string(),
                    name: None,
                    tool_parameter_headers: Vec::new(),
                },
            ),
        )
        .await;
        drop(transport);
        match send_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.pending_requests.write().await.remove(&id);
                let mut subscription = self.tool_subscription.lock().await;
                if subscription.id == Some(id) {
                    *subscription = ToolSubscriptionState::default();
                }
                return Err(error);
            }
            Err(_) => {
                let error = McpError::Timeout(format!(
                    "MCP subscriptions/listen send timed out after {timeout_ms}ms"
                ));
                self.cancel_tool_subscription(id, "Subscription send timed out")
                    .await;
                return Err(error);
            }
        }

        // A graceful server-side close eventually resolves this request.
        // Clear the active state so the next health probe can reopen it.
        let subscription = self.tool_subscription.clone();
        tokio::spawn(async move {
            let result = rx.await;
            let mut state = subscription.lock().await;
            if state.id == Some(id) {
                if let Some(sender) = state.ack_sender.take() {
                    let _ = sender.send(Err(McpError::Protocol(
                        "MCP subscriptions/listen ended before acknowledgment".to_string(),
                    )));
                }
                *state = ToolSubscriptionState::default();
            }
            drop(state);
            match result {
                Ok(Ok(response)) if response.error.is_some() => {
                    warn!(
                        "MCP subscriptions/listen closed with an error: {:?}",
                        response.error
                    );
                }
                Ok(Err(error)) => {
                    debug!("MCP subscriptions/listen ended: {}", error);
                }
                _ => {}
            }
        });

        let acknowledgment = tokio::time::timeout_at(deadline, ack_rx).await;
        let error = match acknowledgment {
            Ok(Ok(Ok(()))) => {
                let subscription = self.tool_subscription.lock().await;
                if subscription.id == Some(id) && subscription.acknowledged {
                    return Ok(());
                }
                McpError::Protocol(
                    "MCP subscriptions/listen closed during acknowledgment".to_string(),
                )
            }
            Ok(Ok(Err(error))) => error,
            Ok(Err(_)) => McpError::Disconnected,
            Err(_) => McpError::Timeout(format!(
                "MCP subscriptions/listen acknowledgment timed out after {timeout_ms}ms"
            )),
        };
        self.cancel_tool_subscription(id, "Subscription acknowledgment failed")
            .await;
        Err(error)
    }

    async fn cancel_tool_subscription(&self, id: u64, reason: &str) {
        self.pending_requests.write().await.remove(&id);
        let mut subscription = self.tool_subscription.lock().await;
        if subscription.id == Some(id) {
            if let Some(sender) = subscription.ack_sender.take() {
                let _ = sender.send(Err(McpError::Protocol(reason.to_string())));
            }
            *subscription = ToolSubscriptionState::default();
        }
        drop(subscription);

        let transport = self.transport.read().await;
        if let Err(error) = transport.cancel_request(id, reason).await {
            warn!(
                "Failed to cancel MCP subscriptions/listen (id={}): {}",
                id, error
            );
        }
    }

    async fn initialize_legacy(&self, timeout_ms: u64) -> Result<McpInitializeResult> {
        let preferred_version = self.transport.read().await.latest_legacy_protocol_version();
        let request = McpInitializeRequest {
            protocol_version: preferred_version.to_string(),
            ..McpInitializeRequest::default()
        };
        let params = serde_json::to_value(request)?;

        let response = self
            .send_request_using(
                "initialize",
                Some(params),
                timeout_ms,
                RequestProtocol::Legacy { version: None },
                Vec::new(),
            )
            .await?;

        let result: McpInitializeResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
        )?;

        if !Self::supports_legacy_version(&result.protocol_version) {
            return Err(McpError::Protocol(format!(
                "MCP server selected unsupported legacy protocol version '{}'",
                result.protocol_version
            )));
        }

        // Send initialized notification
        let initialized = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        let transport = self.transport.read().await;
        transport
            .send_with_metadata(
                serde_json::to_string(&initialized)?,
                McpTransportMetadata {
                    request_id: None,
                    protocol_version: Some(result.protocol_version.clone()),
                    modern: false,
                    method: initialized.method.clone(),
                    name: None,
                    tool_parameter_headers: Vec::new(),
                },
            )
            .await?;
        drop(transport);

        *self.protocol_mode.write().await = ProtocolMode::Legacy {
            version: result.protocol_version.clone(),
        };
        self.modern_tool_list_changes.store(false, Ordering::SeqCst);

        Ok(result)
    }

    fn is_recognized_modern_error(error: &McpError) -> bool {
        matches!(
            error,
            McpError::RemoteProtocol {
                code: HEADER_MISMATCH_ERROR
                    | MISSING_REQUIRED_CLIENT_CAPABILITY_ERROR
                    | UNSUPPORTED_PROTOCOL_VERSION_ERROR,
                ..
            }
        )
    }

    fn supports_legacy_version(version: &str) -> bool {
        matches!(
            version,
            "2025-11-25" | "2025-06-18" | "2025-03-26" | "2024-11-05"
        )
    }

    async fn is_modern(&self) -> bool {
        matches!(
            &*self.protocol_mode.read().await,
            ProtocolMode::Modern { .. }
        )
    }

    pub async fn list_tools(&self, timeout_ms: u64) -> Result<Vec<McpTool>> {
        let modern = self.is_modern().await;
        let requires_parameter_headers = self
            .transport
            .read()
            .await
            .requires_tool_parameter_headers()
            && modern;
        let mut cursor = None;
        let mut seen_cursors = std::collections::HashSet::new();
        let mut tool_infos = Vec::new();

        loop {
            let params = cursor
                .as_ref()
                .map(|cursor| serde_json::json!({ "cursor": cursor }));
            let response = self.send_request("tools/list", params, timeout_ms).await?;
            let result: McpToolListResult = serde_json::from_value(
                response
                    .result
                    .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
            )?;

            Self::ensure_complete_result(result.result_type.as_deref(), modern)?;
            Self::ensure_cacheable_result(result.ttl_ms, result.cache_scope.as_deref(), modern)?;
            tool_infos.extend(result.tools);

            let Some(next_cursor) = result.next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(McpError::Protocol(format!(
                    "MCP tools/list repeated pagination cursor '{next_cursor}'"
                )));
            }
            cursor = Some(next_cursor);
        }

        let mut header_specs = HashMap::new();
        let mut tools = Vec::with_capacity(tool_infos.len());

        for tool in tool_infos {
            let output_schema = match tool.output_schema {
                Some(schema) if modern && !schema.is_object() => {
                    warn!(
                        "Ignoring modern MCP tool '{}' because outputSchema must be a JSON Schema object",
                        tool.name
                    );
                    continue;
                }
                schema => schema,
            };
            let parameters = match tool.input_schema {
                Some(parameters)
                    if !modern
                        || (parameters.is_object()
                            && parameters.get("type").and_then(Value::as_str)
                                == Some("object")) =>
                {
                    parameters
                }
                Some(_) | None if modern => {
                    warn!(
                        "Ignoring modern MCP tool '{}' because inputSchema must be an object schema with type 'object'",
                        tool.name
                    );
                    continue;
                }
                None => serde_json::json!({}),
                Some(parameters) => parameters,
            };
            if requires_parameter_headers {
                match Self::tool_header_specs(&parameters) {
                    Ok(specs) => {
                        header_specs.insert(tool.name.clone(), specs);
                    }
                    Err(reason) => {
                        warn!(
                            "Ignoring MCP tool '{}' because its x-mcp-header schema is invalid: {}",
                            tool.name, reason
                        );
                        continue;
                    }
                }
            }
            tools.push(McpTool {
                name: tool.name,
                description: tool.description,
                parameters,
                output_schema,
            });
        }
        *self.tool_header_specs.write().await = header_specs;
        Ok(tools)
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<McpCallResult> {
        let modern = self.is_modern().await;
        if modern && !arguments.is_object() {
            return Err(McpError::Protocol(
                "Modern MCP tool arguments must be a JSON object".to_string(),
            ));
        }
        let tool_parameter_headers = if modern {
            let specs = self
                .tool_header_specs
                .read()
                .await
                .get(name)
                .cloned()
                .unwrap_or_default();
            Self::extract_tool_parameter_headers(&specs, &arguments)?
        } else {
            Vec::new()
        };
        let request = McpToolCallRequest {
            name: name.to_string(),
            arguments: Some(arguments),
        };
        let params = serde_json::to_value(request)?;

        let response = self
            .send_request_with_tool_headers(
                "tools/call",
                Some(params),
                timeout_ms,
                tool_parameter_headers,
            )
            .await?;

        let result: McpToolCallResult = serde_json::from_value(
            response
                .result
                .ok_or_else(|| McpError::Protocol("Missing result".to_string()))?,
        )?;
        Self::ensure_complete_result(result.result_type.as_deref(), self.is_modern().await)?;

        Ok(McpCallResult {
            content: result.content,
            is_error: result.is_error,
            structured_content: result.structured_content,
        })
    }

    fn ensure_complete_result(result_type: Option<&str>, modern: bool) -> Result<()> {
        match result_type {
            Some("complete") => Ok(()),
            None if !modern => Ok(()),
            None => Err(McpError::Protocol(
                "Modern MCP result is missing required resultType".to_string(),
            )),
            Some("input_required") => Err(McpError::Protocol(
                "MCP request requires client input that Bamboo did not advertise".to_string(),
            )),
            Some(other) => Err(McpError::Protocol(format!(
                "Unsupported MCP resultType '{other}'"
            ))),
        }
    }

    fn ensure_cacheable_result(
        ttl_ms: Option<u64>,
        cache_scope: Option<&str>,
        modern: bool,
    ) -> Result<()> {
        if !modern {
            return Ok(());
        }
        if ttl_ms.is_none() {
            return Err(McpError::Protocol(
                "Modern MCP cacheable result is missing required ttlMs".to_string(),
            ));
        }
        match cache_scope {
            Some("public" | "private") => Ok(()),
            None => Err(McpError::Protocol(
                "Modern MCP cacheable result is missing required cacheScope".to_string(),
            )),
            Some(other) => Err(McpError::Protocol(format!(
                "Modern MCP cacheable result has invalid cacheScope '{other}'"
            ))),
        }
    }

    fn tool_header_specs(schema: &Value) -> std::result::Result<Vec<ToolHeaderSpec>, String> {
        let mut specs = Vec::new();
        let mut names = std::collections::HashSet::new();
        Self::collect_tool_header_specs(schema, &mut Vec::new(), &mut names, &mut specs)?;
        Ok(specs)
    }

    fn collect_tool_header_specs(
        schema: &Value,
        path: &mut Vec<String>,
        names: &mut std::collections::HashSet<String>,
        specs: &mut Vec<ToolHeaderSpec>,
    ) -> std::result::Result<(), String> {
        let Some(object) = schema.as_object() else {
            return Ok(());
        };

        if let Some(annotation) = object.get("x-mcp-header") {
            if path.is_empty() {
                return Err("x-mcp-header cannot annotate the schema root".to_string());
            }
            let name = annotation
                .as_str()
                .ok_or_else(|| "x-mcp-header must be a string".to_string())?;
            if !Self::is_http_token(name) {
                return Err(format!("'{name}' is not a valid HTTP field-name token"));
            }
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("duplicate x-mcp-header name '{name}'"));
            }
            let value_type = match object.get("type").and_then(Value::as_str) {
                Some("string") => ToolHeaderValueType::String,
                Some("integer") => ToolHeaderValueType::Integer,
                Some("boolean") => ToolHeaderValueType::Boolean,
                _ => {
                    return Err(format!(
                        "x-mcp-header '{name}' must annotate a string, integer, or boolean property"
                    ))
                }
            };
            specs.push(ToolHeaderSpec {
                name: name.to_string(),
                path: path.clone(),
                value_type,
            });
        }

        if let Some(properties) = object.get("properties") {
            let properties = properties
                .as_object()
                .ok_or_else(|| "JSON Schema properties must be an object".to_string())?;
            for (property, child_schema) in properties {
                path.push(property.clone());
                Self::collect_tool_header_specs(child_schema, path, names, specs)?;
                path.pop();
            }
        }

        for (keyword, value) in object {
            if keyword != "properties"
                && keyword != "x-mcp-header"
                && Self::contains_tool_header_annotation(value)
            {
                return Err(format!(
                    "x-mcp-header is not statically reachable through properties (found under {keyword})"
                ));
            }
        }

        Ok(())
    }

    fn contains_tool_header_annotation(value: &Value) -> bool {
        match value {
            Value::Object(object) => {
                object.contains_key("x-mcp-header")
                    || object.values().any(Self::contains_tool_header_annotation)
            }
            Value::Array(values) => values.iter().any(Self::contains_tool_header_annotation),
            _ => false,
        }
    }

    fn is_http_token(value: &str) -> bool {
        !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            })
    }

    fn extract_tool_parameter_headers(
        specs: &[ToolHeaderSpec],
        arguments: &Value,
    ) -> Result<Vec<(String, String)>> {
        const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
        const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

        let mut headers = Vec::with_capacity(specs.len());
        for spec in specs {
            let value = spec
                .path
                .iter()
                .try_fold(arguments, |current, segment| current.get(segment));
            let Some(value) = value.filter(|value| !value.is_null()) else {
                continue;
            };

            let encoded = match spec.value_type {
                ToolHeaderValueType::String => value.as_str().map(ToString::to_string),
                ToolHeaderValueType::Boolean => value.as_bool().map(|value| value.to_string()),
                ToolHeaderValueType::Integer => {
                    if let Some(value) = value.as_i64() {
                        (value >= MIN_SAFE_INTEGER && value <= MAX_SAFE_INTEGER as i64)
                            .then(|| value.to_string())
                    } else if let Some(value) = value.as_u64() {
                        (value <= MAX_SAFE_INTEGER).then(|| value.to_string())
                    } else if let Some(value) = value.as_f64() {
                        (value.is_finite()
                            && value.fract() == 0.0
                            && value >= MIN_SAFE_INTEGER as f64
                            && value <= MAX_SAFE_INTEGER as f64)
                            .then(|| {
                                if value == 0.0 {
                                    "0".to_string()
                                } else {
                                    format!("{value:.0}")
                                }
                            })
                    } else {
                        None
                    }
                }
            }
            .ok_or_else(|| {
                McpError::Protocol(format!(
                    "Tool argument '{}' cannot be mirrored into Mcp-Param-{} with its declared primitive type",
                    spec.path.join("."),
                    spec.name
                ))
            })?;
            headers.push((spec.name.clone(), encoded));
        }
        Ok(headers)
    }

    /// Health probe compatible with both protocol eras.
    ///
    /// MCP 2026-07-28 removed `ping`, so modern connections use the mandatory
    /// `server/discover` RPC. Legacy connections retain `ping`.
    pub async fn ping(&self, timeout_ms: u64) -> Result<()> {
        if self.is_modern().await {
            let response = self
                .send_request("server/discover", Some(serde_json::json!({})), timeout_ms)
                .await?;
            let result: McpDiscoverResult = serde_json::from_value(
                response
                    .result
                    .ok_or_else(|| McpError::Protocol("Missing discovery result".to_string()))?,
            )?;
            Self::validate_modern_discovery(&result)?;
            let supports_tool_list_changes = result
                .capabilities
                .tools
                .as_ref()
                .is_some_and(|tools| tools.list_changed);
            self.modern_tool_list_changes
                .store(supports_tool_list_changes, Ordering::SeqCst);
            if supports_tool_list_changes {
                self.start_tool_change_subscription(timeout_ms).await?;
            }
        } else {
            self.send_request("ping", None, timeout_ms).await?;
        }
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
    use std::collections::VecDeque;
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

    type CapturedMessages = Arc<TokioMutex<Vec<(Value, McpTransportMetadata)>>>;

    /// Deterministic request/response transport used to verify protocol-era
    /// negotiation and the exact modern wire envelope.
    struct ScriptedTransport {
        connected: bool,
        message_rx: TokioMutex<Option<mpsc::Receiver<String>>>,
        response_tx: mpsc::Sender<String>,
        responses: TokioMutex<VecDeque<Option<Value>>>,
        captured: CapturedMessages,
        supports_modern: bool,
        requires_tool_parameter_headers: bool,
        legacy_version: &'static str,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<Option<Value>>) -> (Self, CapturedMessages) {
            let (response_tx, message_rx) = mpsc::channel(100);
            let captured = Arc::new(TokioMutex::new(Vec::new()));
            (
                Self {
                    connected: false,
                    message_rx: TokioMutex::new(Some(message_rx)),
                    response_tx,
                    responses: TokioMutex::new(responses.into()),
                    captured: captured.clone(),
                    supports_modern: true,
                    requires_tool_parameter_headers: false,
                    legacy_version: LATEST_LEGACY_PROTOCOL_VERSION,
                },
                captured,
            )
        }

        fn with_tool_parameter_headers(mut self) -> Self {
            self.requires_tool_parameter_headers = true;
            self
        }

        fn success(result: Value) -> Option<Value> {
            Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 0,
                "result": result
            }))
        }

        fn error(code: i32, message: &str, data: Option<Value>) -> Option<Value> {
            Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 0,
                "error": {
                    "code": code,
                    "message": message,
                    "data": data
                }
            }))
        }

        fn discover_result(tools_list_changed: bool) -> Value {
            serde_json::json!({
                "resultType": "complete",
                "supportedVersions": [LATEST_PROTOCOL_VERSION],
                "capabilities": {
                    "tools": {
                        "listChanged": tools_list_changed
                    }
                },
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "modern-test-server",
                        "version": "1.0.0"
                    }
                },
                "ttlMs": 1000,
                "cacheScope": "private"
            })
        }

        fn subscription_ack(id: u64, tools_list_changed: bool) -> Option<Value> {
            Some(serde_json::json!({
                "jsonrpc": "2.0",
                "method": SUBSCRIPTION_ACK_METHOD,
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/subscriptionId": id
                    },
                    "notifications": {
                        "toolsListChanged": tools_list_changed
                    }
                }
            }))
        }
    }

    #[async_trait]
    impl McpTransport for ScriptedTransport {
        async fn connect(&mut self) -> Result<()> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.connected = false;
            Ok(())
        }

        async fn send(&self, message: String) -> Result<()> {
            self.send_with_metadata(message, McpTransportMetadata::default())
                .await
        }

        async fn send_with_metadata(
            &self,
            message: String,
            metadata: McpTransportMetadata,
        ) -> Result<()> {
            let request: Value = serde_json::from_str(&message)?;
            let id = request.get("id").cloned();
            self.captured.lock().await.push((request, metadata));

            if let Some(id) = id {
                let response = self.responses.lock().await.pop_front().ok_or_else(|| {
                    McpError::Transport("script has no response for request".to_string())
                })?;
                if let Some(mut response) = response {
                    if response.get("method").is_none() {
                        response["id"] = id;
                    }
                    self.response_tx
                        .send(response.to_string())
                        .await
                        .map_err(|_| McpError::Disconnected)?;
                }
            }
            Ok(())
        }

        fn supports_modern_protocol(&self) -> bool {
            self.supports_modern
        }

        fn latest_legacy_protocol_version(&self) -> &'static str {
            self.legacy_version
        }

        fn requires_tool_parameter_headers(&self) -> bool {
            self.requires_tool_parameter_headers
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

    #[tokio::test]
    async fn modern_negotiation_uses_discovery_and_per_request_metadata() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(false)),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "tools": [{
                    "name": "weather",
                    "description": "Get weather",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                }],
                "ttlMs": 1000,
                "cacheScope": "private"
            })),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");

        let initialized = client.initialize(1000).await.expect("modern discovery");
        assert_eq!(initialized.protocol_version, LATEST_PROTOCOL_VERSION);
        assert_eq!(initialized.server_info.name, "modern-test-server");
        assert!(matches!(
            &*client.protocol_mode.read().await,
            ProtocolMode::Modern { version } if version == LATEST_PROTOCOL_VERSION
        ));

        let tools = client.list_tools(1000).await.expect("modern tools/list");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "weather");

        let captured = captured.lock().await;
        assert_eq!(
            captured.len(),
            2,
            "modern negotiation must not send initialize/initialized"
        );
        assert_eq!(captured[0].0["method"], "server/discover");
        assert_eq!(captured[1].0["method"], "tools/list");
        for (request, metadata) in captured.iter() {
            assert!(metadata.modern);
            assert_eq!(
                metadata.protocol_version.as_deref(),
                Some(LATEST_PROTOCOL_VERSION)
            );
            assert_eq!(
                request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
                LATEST_PROTOCOL_VERSION
            );
            assert_eq!(
                request["params"]["_meta"]["io.modelcontextprotocol/clientInfo"]["name"],
                "bamboo-agent"
            );
            assert_eq!(
                request["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"],
                serde_json::json!({})
            );
        }
    }

    #[tokio::test]
    async fn legacy_server_falls_back_to_latest_initialization_revision() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::error(-32601, "Method not found", None),
            ScriptedTransport::success(serde_json::json!({
                "protocolVersion": LATEST_LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": {
                    "name": "legacy-test-server",
                    "version": "1.0.0"
                }
            })),
            ScriptedTransport::success(serde_json::json!({})),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");

        let initialized = client.initialize(1000).await.expect("legacy fallback");
        assert_eq!(initialized.protocol_version, LATEST_LEGACY_PROTOCOL_VERSION);
        assert!(matches!(
            &*client.protocol_mode.read().await,
            ProtocolMode::Legacy { version } if version == LATEST_LEGACY_PROTOCOL_VERSION
        ));
        client.ping(1000).await.expect("legacy ping");

        let captured = captured.lock().await;
        assert_eq!(captured.len(), 4);
        assert_eq!(captured[0].0["method"], "server/discover");
        assert!(captured[0].1.modern);

        assert_eq!(captured[1].0["method"], "initialize");
        assert_eq!(
            captured[1].0["params"]["protocolVersion"],
            LATEST_LEGACY_PROTOCOL_VERSION
        );
        assert!(!captured[1].1.modern);
        assert!(captured[1].1.protocol_version.is_none());

        assert_eq!(captured[2].0["method"], "notifications/initialized");
        assert_eq!(
            captured[2].1.protocol_version.as_deref(),
            Some(LATEST_LEGACY_PROTOCOL_VERSION)
        );

        assert_eq!(captured[3].0["method"], "ping");
        assert_eq!(
            captured[3].1.protocol_version.as_deref(),
            Some(LATEST_LEGACY_PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn recognized_modern_error_never_falls_back_to_initialize() {
        let (transport, captured) = ScriptedTransport::new(vec![ScriptedTransport::error(
            UNSUPPORTED_PROTOCOL_VERSION_ERROR,
            "Unsupported protocol version",
            Some(serde_json::json!({
                "supported": ["2099-01-01"]
            })),
        )]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");

        let error = client
            .initialize(1000)
            .await
            .expect_err("recognized modern error must surface");
        assert!(matches!(
            error,
            McpError::RemoteProtocol {
                code: UNSUPPORTED_PROTOCOL_VERSION_ERROR,
                ..
            }
        ));
        assert_eq!(captured.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn modern_tool_change_capability_opens_subscription_stream() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(true)),
            ScriptedTransport::subscription_ack(2, true),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");
        client.initialize(1000).await.expect("modern discovery");

        let captured = captured.lock().await;
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[1].0["method"], "subscriptions/listen");
        assert_eq!(
            captured[1].0["params"]["notifications"]["toolsListChanged"],
            true
        );
        assert_eq!(
            captured[1].0["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            LATEST_PROTOCOL_VERSION
        );
        let subscription = client.tool_subscription.lock().await;
        assert_eq!(subscription.id, Some(2));
        assert!(subscription.acknowledged);
        drop(subscription);
        assert!(client.pending_requests.read().await.contains_key(&2));
        drop(captured);

        client.disconnect().await.expect("disconnect");
        assert!(client.pending_requests.read().await.is_empty());
    }

    #[tokio::test]
    async fn subscription_is_not_active_until_requested_filter_is_acknowledged() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(true)),
            ScriptedTransport::subscription_ack(2, false),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");

        let error = client
            .initialize(1000)
            .await
            .expect_err("rejected subscription filter must fail initialization");
        assert!(error.to_string().contains("toolsListChanged"));
        assert!(client.tool_subscription.lock().await.id.is_none());
        assert!(client.pending_requests.read().await.is_empty());

        let captured = captured.lock().await;
        assert_eq!(captured[0].0["method"], "server/discover");
        assert_eq!(captured[1].0["method"], "subscriptions/listen");
        assert_eq!(captured[2].0["method"], "notifications/cancelled");
        assert_eq!(captured[2].0["params"]["requestId"], 2);
    }

    #[tokio::test]
    async fn malformed_modern_discovery_never_downgrades_to_legacy() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "supportedVersions": [LATEST_PROTOCOL_VERSION],
                "capabilities": {},
                "ttlMs": 1000
            })),
            ScriptedTransport::success(serde_json::json!({
                "protocolVersion": LATEST_LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": {"name": "must-not-run", "version": "1"}
            })),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");

        let error = client
            .initialize(1000)
            .await
            .expect_err("malformed modern result must surface");
        assert!(error.to_string().contains("cacheScope"));
        assert_eq!(
            captured.lock().await.len(),
            1,
            "successful server/discover response must pin the modern era"
        );
        assert!(matches!(
            &*client.protocol_mode.read().await,
            ProtocolMode::Modern { .. }
        ));
    }

    #[tokio::test]
    async fn modern_tool_change_notifications_require_acknowledged_matching_subscription() {
        use std::collections::HashMap;

        let pending: RwLock<HashMap<u64, PendingRequest>> = RwLock::new(HashMap::new());
        let (notification_tx, mut notification_rx) = mpsc::channel(4);
        let queue_full = AtomicBool::new(false);
        let subscription = Mutex::new(ToolSubscriptionState {
            id: Some(7),
            acknowledged: true,
            ack_sender: None,
        });
        let modern_tool_list_changes = AtomicBool::new(true);

        let mismatched = r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":8}}}"#;
        McpProtocolClient::handle_message(
            mismatched,
            &pending,
            &notification_tx,
            &queue_full,
            &subscription,
            &modern_tool_list_changes,
        )
        .await
        .expect("ignore mismatched notification");
        assert!(notification_rx.try_recv().is_err());

        let matched = r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":7}}}"#;
        McpProtocolClient::handle_message(
            matched,
            &pending,
            &notification_tx,
            &queue_full,
            &subscription,
            &modern_tool_list_changes,
        )
        .await
        .expect("accept correlated notification");
        assert_eq!(
            notification_rx
                .try_recv()
                .expect("correlated notification")
                .method,
            "notifications/tools/list_changed"
        );
    }

    #[tokio::test]
    async fn health_probe_reopens_a_closed_tool_change_subscription() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(true)),
            ScriptedTransport::subscription_ack(2, true),
            ScriptedTransport::success(ScriptedTransport::discover_result(true)),
            ScriptedTransport::subscription_ack(4, true),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");
        client.initialize(1000).await.expect("initial subscription");

        let close = r#"{"jsonrpc":"2.0","id":2,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":2}}}"#;
        McpProtocolClient::handle_message(
            close,
            &client.pending_requests,
            &client.notification_tx,
            &client.notification_queue_full,
            &client.tool_subscription,
            &client.modern_tool_list_changes,
        )
        .await
        .expect("route graceful close");
        tokio::task::yield_now().await;
        assert!(client.tool_subscription.lock().await.id.is_none());

        client.ping(1000).await.expect("health probe resubscribes");
        let subscription = client.tool_subscription.lock().await;
        assert_eq!(subscription.id, Some(4));
        assert!(subscription.acknowledged);
        drop(subscription);

        let captured = captured.lock().await;
        assert_eq!(captured.len(), 4);
        assert_eq!(captured[2].0["method"], "server/discover");
        assert_eq!(captured[3].0["method"], "subscriptions/listen");
    }

    #[tokio::test]
    async fn modern_tools_list_follows_all_pages_and_rejects_bad_cache_fields() {
        let (transport, captured) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(false)),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "tools": [{
                    "name": "first",
                    "description": "First page",
                    "inputSchema": {"type": "object"}
                }],
                "nextCursor": "page-2",
                "ttlMs": 1000,
                "cacheScope": "public"
            })),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "tools": [{
                    "name": "second",
                    "description": "Second page",
                    "inputSchema": {"type": "object"}
                }],
                "ttlMs": 1000,
                "cacheScope": "private"
            })),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");
        client.initialize(1000).await.expect("modern discovery");

        let tools = client.list_tools(1000).await.expect("paginated tools/list");
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );

        let captured = captured.lock().await;
        assert_eq!(captured.len(), 3);
        assert!(captured[1].0["params"].get("cursor").is_none());
        assert_eq!(captured[2].0["params"]["cursor"], "page-2");
        drop(captured);

        assert!(McpProtocolClient::ensure_cacheable_result(None, Some("private"), true).is_err());
        assert!(McpProtocolClient::ensure_cacheable_result(Some(1), None, true).is_err());
        assert!(McpProtocolClient::ensure_cacheable_result(Some(1), Some("shared"), true).is_err());
        assert!(
            McpProtocolClient::ensure_cacheable_result(None, None, false).is_ok(),
            "legacy results do not have cache fields"
        );
    }

    #[tokio::test]
    async fn modern_http_tool_headers_are_derived_from_valid_nested_properties() {
        let responses = vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(false)),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "tools": [{
                    "name": "route",
                    "description": "Route a request",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "routing": {
                                "type": "object",
                                "properties": {
                                    "region": {
                                        "type": "string",
                                        "x-mcp-header": "Region"
                                    },
                                    "shard": {
                                        "type": "integer",
                                        "x-mcp-header": "Shard"
                                    }
                                }
                            }
                        }
                    }
                }],
                "ttlMs": 1000,
                "cacheScope": "private"
            })),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "content": [{
                    "type": "text",
                    "text": "ok"
                }]
            })),
        ];
        let (transport, captured) = ScriptedTransport::new(responses);
        let transport = transport.with_tool_parameter_headers();
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");
        client.initialize(1000).await.expect("modern discovery");
        client.list_tools(1000).await.expect("tools/list");
        client
            .call_tool(
                "route",
                serde_json::json!({
                    "routing": {
                        "region": "华东",
                        "shard": 42.0
                    }
                }),
                1000,
            )
            .await
            .expect("tools/call");

        let captured = captured.lock().await;
        let (_, metadata) = &captured[2];
        assert_eq!(metadata.method, "tools/call");
        assert_eq!(metadata.name.as_deref(), Some("route"));
        let mut headers = metadata.tool_parameter_headers.clone();
        headers.sort();
        assert_eq!(
            headers,
            vec![
                ("Region".to_string(), "华东".to_string()),
                ("Shard".to_string(), "42".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn modern_tool_output_schema_and_all_content_blocks_are_preserved() {
        let (transport, _) = ScriptedTransport::new(vec![
            ScriptedTransport::success(ScriptedTransport::discover_result(false)),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "tools": [{
                    "name": "report",
                    "description": "Return a report",
                    "inputSchema": {"type": "object"},
                    "outputSchema": {
                        "type": "object",
                        "properties": {"count": {"type": "integer"}}
                    }
                }],
                "ttlMs": 1000,
                "cacheScope": "private"
            })),
            ScriptedTransport::success(serde_json::json!({
                "resultType": "complete",
                "content": [
                    {
                        "type": "audio",
                        "data": "UklGRg==",
                        "mimeType": "audio/wav"
                    },
                    {
                        "type": "resource_link",
                        "uri": "file:///report.json",
                        "name": "report",
                        "mimeType": "application/json",
                        "size": 42
                    }
                ],
                "structuredContent": null
            })),
        ]);
        let mut client = McpProtocolClient::new(Box::new(transport));
        client.connect().await.expect("connect");
        client.initialize(1000).await.expect("modern discovery");

        let tools = client.list_tools(1000).await.expect("tools/list");
        assert_eq!(
            tools[0].output_schema,
            Some(serde_json::json!({
                "type": "object",
                "properties": {"count": {"type": "integer"}}
            }))
        );

        let result = client
            .call_tool("report", serde_json::json!({}), 1000)
            .await
            .expect("tools/call");
        assert!(matches!(
            result.content[0],
            crate::types::McpContentItem::Audio { .. }
        ));
        assert!(matches!(
            result.content[1],
            crate::types::McpContentItem::ResourceLink { .. }
        ));
        assert_eq!(
            result.structured_content,
            crate::types::McpStructuredContent::Null
        );
    }

    #[test]
    fn invalid_tool_header_annotations_are_rejected() {
        let duplicate = serde_json::json!({
            "type": "object",
            "properties": {
                "a": {"type": "string", "x-mcp-header": "Tenant"},
                "b": {"type": "string", "x-mcp-header": "tenant"}
            }
        });
        assert!(McpProtocolClient::tool_header_specs(&duplicate)
            .expect_err("header names are case-insensitively unique")
            .contains("duplicate"));

        let unreachable = serde_json::json!({
            "type": "object",
            "allOf": [{
                "properties": {
                    "region": {"type": "string", "x-mcp-header": "Region"}
                }
            }]
        });
        assert!(McpProtocolClient::tool_header_specs(&unreachable)
            .expect_err("composition branch is not statically reachable")
            .contains("allOf"));

        let unsafe_integer_schema = serde_json::json!({
            "type": "object",
            "properties": {
                "shard": {"type": "integer", "x-mcp-header": "Shard"}
            }
        });
        let specs =
            McpProtocolClient::tool_header_specs(&unsafe_integer_schema).expect("valid schema");
        assert!(McpProtocolClient::extract_tool_parameter_headers(
            &specs,
            &serde_json::json!({"shard": 9_007_199_254_740_992_u64})
        )
        .is_err());
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
        let transport = MockTransport::new(); // Won't respond
        let messages = transport.messages_sent.clone();
        let client = McpProtocolClient::new(Box::new(transport));

        let result = client.send_request("test", None, 100).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Timeout(_) => {}
            _ => panic!("Expected Timeout error"),
        }
        let sent = messages.read().await;
        assert_eq!(sent.len(), 2, "request plus cancellation notification");
        let cancellation: Value = serde_json::from_str(&sent[1]).expect("cancellation JSON");
        assert_eq!(cancellation["method"], "notifications/cancelled");
        assert_eq!(cancellation["params"]["requestId"], 1);
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
        let subscription = Mutex::new(ToolSubscriptionState::default());
        let modern_tool_list_changes = AtomicBool::new(false);

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
            McpProtocolClient::handle_message(
                notif_json,
                &pending,
                &notif_tx,
                &queue_full,
                &subscription,
                &modern_tool_list_changes,
            ),
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
        McpProtocolClient::handle_message(
            resp_json,
            &pending,
            &notif_tx,
            &queue_full,
            &subscription,
            &modern_tool_list_changes,
        )
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
        let subscription = Mutex::new(ToolSubscriptionState::default());
        let modern_tool_list_changes = AtomicBool::new(false);

        // Fill exactly to capacity via the production send path — all accepted.
        for i in 0..NOTIFICATION_CHANNEL_CAPACITY {
            let json = format!(r#"{{"jsonrpc":"2.0","method":"notifications/message/{i}"}}"#);
            McpProtocolClient::handle_message(
                &json,
                &pending,
                &tx,
                &queue_full,
                &subscription,
                &modern_tool_list_changes,
            )
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
                McpProtocolClient::handle_message(
                    &json,
                    &pending,
                    &tx,
                    &queue_full,
                    &subscription,
                    &modern_tool_list_changes,
                ),
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
