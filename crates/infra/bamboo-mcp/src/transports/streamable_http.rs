//! MCP Streamable HTTP transport.
//!
//! Implements the stateless MCP `2026-07-28` POST transport while retaining
//! the session / GET / DELETE behavior needed by initialization-based servers
//! through `2025-11-25`.
//! See <https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http>

use async_trait::async_trait;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace, warn};

use crate::config::{HeaderConfig, StreamableHttpConfig};
use crate::error::{McpError, Result};
use crate::protocol::client::{McpCancellationMetadata, McpTransport, McpTransportMetadata};
use crate::protocol::models::{JsonRpcError, JsonRpcNotification};

const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_METHOD_HEADER: &str = "mcp-method";
const MCP_NAME_HEADER: &str = "mcp-name";
const ACCEPT_HEADER: &str = "application/json, text/event-stream";
const RESPONSE_STREAM_CLOSED_ERROR: i32 = -32000;

struct PostSseHandle {
    request_id: Option<u64>,
    handle: tokio::task::JoinHandle<()>,
}

pub struct StreamableHttpTransport {
    config: StreamableHttpConfig,
    client: Client,
    session_id: Arc<Mutex<Option<String>>>,
    legacy_protocol_version: Arc<Mutex<Option<String>>>,
    connected: Arc<AtomicBool>,
    // Dropped in disconnect() so the channel closes and the client handler
    // wakes without polling.
    message_tx: Option<mpsc::Sender<String>>,
    message_rx: Mutex<Option<mpsc::Receiver<String>>>,
    get_sse_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Per-POST SSE forwarder tasks. Stored so they can be aborted
    // deterministically in disconnect() (they otherwise exit only on the next
    // send-Err once the receiver is dropped).
    post_sse_handles: Mutex<Vec<PostSseHandle>>,
}

impl StreamableHttpTransport {
    pub fn new(config: StreamableHttpConfig) -> Self {
        Self::new_with_client(config, Client::new())
    }

    pub fn new_with_client(config: StreamableHttpConfig, client: Client) -> Self {
        let (message_tx, message_rx) = mpsc::channel(256);
        Self {
            config,
            client,
            session_id: Arc::new(Mutex::new(None)),
            legacy_protocol_version: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            message_tx: Some(message_tx),
            message_rx: Mutex::new(Some(message_rx)),
            get_sse_handle: Mutex::new(None),
            post_sse_handles: Mutex::new(Vec::new()),
        }
    }

    fn build_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static(ACCEPT_HEADER),
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        for HeaderConfig { name, value, .. } in &self.config.headers {
            let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| McpError::InvalidConfig(format!("Invalid header name: {}", e)))?;
            let header_value = value
                .parse()
                .map_err(|e| McpError::InvalidConfig(format!("Invalid header value: {}", e)))?;
            headers.insert(header_name, header_value);
        }

        Ok(headers)
    }

    fn apply_request_metadata(
        headers: &mut HeaderMap,
        metadata: &McpTransportMetadata,
    ) -> Result<()> {
        // Protocol-derived headers must reflect the JSON-RPC body exactly.
        // Remove configured values first so a stale Mcp-Param-* value cannot
        // survive when the corresponding tool argument is absent.
        let configured_protocol_headers: Vec<HeaderName> = headers
            .keys()
            .filter(|name| {
                let name = name.as_str();
                matches!(
                    name,
                    MCP_SESSION_ID_HEADER
                        | MCP_PROTOCOL_VERSION_HEADER
                        | MCP_METHOD_HEADER
                        | MCP_NAME_HEADER
                ) || name.starts_with("mcp-param-")
            })
            .cloned()
            .collect();
        for name in configured_protocol_headers {
            headers.remove(name);
        }

        if let Some(version) = metadata.protocol_version.as_deref() {
            headers.insert(
                MCP_PROTOCOL_VERSION_HEADER,
                HeaderValue::from_str(version).map_err(|error| {
                    McpError::Transport(format!("Invalid MCP protocol version header: {error}"))
                })?,
            );
        } else if metadata.modern {
            return Err(McpError::Protocol(
                "Modern Streamable HTTP request is missing a protocol version".to_string(),
            ));
        }

        if !metadata.modern {
            return Ok(());
        }

        headers.insert(
            MCP_METHOD_HEADER,
            HeaderValue::from_str(&metadata.method).map_err(|error| {
                McpError::Transport(format!("Invalid MCP method header: {error}"))
            })?,
        );
        if let Some(name) = metadata.name.as_deref() {
            headers.insert(
                MCP_NAME_HEADER,
                Self::encoded_header_value(name, "Mcp-Name")?,
            );
        }
        for (name, value) in &metadata.tool_parameter_headers {
            let header_name = HeaderName::from_bytes(format!("mcp-param-{name}").as_bytes())
                .map_err(|error| {
                    McpError::Protocol(format!("Invalid x-mcp-header name '{name}': {error}"))
                })?;
            headers.insert(
                header_name,
                Self::encoded_header_value(value, &format!("Mcp-Param-{name}"))?,
            );
        }
        Ok(())
    }

    fn encoded_header_value(value: &str, label: &str) -> Result<HeaderValue> {
        let bytes = value.as_bytes();
        let has_edge_whitespace = bytes
            .first()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
            || bytes
                .last()
                .is_some_and(|byte| matches!(byte, b' ' | b'\t'));
        let visible_ascii_or_tab = bytes.iter().all(|byte| matches!(byte, b'\t' | b' '..=b'~'));
        let matches_sentinel = value.starts_with("=?base64?") && value.ends_with("?=");
        let encoded = if visible_ascii_or_tab && !has_edge_whitespace && !matches_sentinel {
            value.to_string()
        } else {
            format!("=?base64?{}?=", BASE64_STANDARD.encode(bytes))
        };
        HeaderValue::from_str(&encoded)
            .map_err(|error| McpError::Transport(format!("Invalid {label} value: {error}")))
    }

    fn redact_url_for_log(url: &str) -> String {
        match reqwest::Url::parse(url) {
            Ok(mut parsed) => {
                parsed.set_query(None);
                parsed.set_fragment(None);
                parsed.to_string()
            }
            Err(_) => url.to_string(),
        }
    }

    /// POST a message to the MCP endpoint and route any response(s) to the
    /// message channel. Returns Ok(()) if the POST was accepted (202) or a
    /// response was successfully forwarded.
    async fn post_and_route_response(
        &self,
        message: String,
        metadata: &McpTransportMetadata,
    ) -> Result<()> {
        let mut headers = self.build_headers()?;
        Self::apply_request_metadata(&mut headers, metadata)?;

        if !metadata.modern {
            if let Some(sid) = self.session_id.lock().await.clone() {
                let value = HeaderValue::from_str(&sid)
                    .map_err(|e| McpError::Transport(format!("Invalid session id: {}", e)))?;
                headers.insert(MCP_SESSION_ID_HEADER, value);
            }
        }

        trace!(
            "MCP StreamableHTTP POST (url={}, bytes={})",
            Self::redact_url_for_log(&self.config.url),
            message.len()
        );

        let response = tokio::time::timeout(
            tokio::time::Duration::from_secs(60),
            self.client
                .post(&self.config.url)
                .headers(headers)
                .body(message)
                .send(),
        )
        .await
        .map_err(|_| McpError::Timeout("POST request timed out".to_string()))??;

        let status = response.status();

        // Modern MCP is sessionless. Only legacy exchanges may establish a
        // protocol-level session.
        if !metadata.modern {
            if let Some(sid) = response.headers().get(MCP_SESSION_ID_HEADER) {
                let sid_str = sid.to_str().map_err(|e| {
                    McpError::Transport(format!("Invalid session id header: {}", e))
                })?;
                let mut guard = self.session_id.lock().await;
                guard.get_or_insert_with(|| sid_str.to_string());
            }
        }

        if status == reqwest::StatusCode::ACCEPTED {
            // Server accepted notification/response, no body.
            trace!("MCP StreamableHTTP POST accepted (202)");
            return Ok(());
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            // Modern negotiation errors may omit the JSON-RPC id. Return the
            // error directly instead of routing it through `JsonRpcResponse`,
            // whose response correlation necessarily requires an id.
            if let Some(error) = Self::parse_json_rpc_error_response(&body) {
                return Err(McpError::HttpProtocol {
                    status: status.as_u16(),
                    code: error.code,
                    message: error.message,
                    data: error.data,
                });
            }
            return Err(McpError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if content_type.contains("text/event-stream") {
            // SSE response — parse events and forward each to channel.
            trace!("MCP StreamableHTTP POST response is SSE stream");
            let Some(tx) = self.message_tx.clone() else {
                return Ok(()); // disconnected
            };
            let connected = self.connected.clone();
            let request_id = metadata.request_id;

            // We need to consume the response body in a spawned task to avoid
            // blocking the caller. Events from this POST's SSE response are
            // forwarded to the channel so receive() can pick them up.
            let handle = tokio::spawn(async move {
                let mut stream = response.bytes_stream().eventsource();
                let mut saw_terminal_response = false;
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(evt) => {
                            if !evt.data.trim().is_empty() {
                                let is_terminal = request_id.is_some_and(|id| {
                                    StreamableHttpTransport::is_terminal_response_for(&evt.data, id)
                                });
                                if is_terminal {
                                    saw_terminal_response = true;
                                }
                                trace!(
                                    "MCP StreamableHTTP POST SSE event (event='{}', data_len={})",
                                    evt.event,
                                    evt.data.len()
                                );
                                if tx.send(evt.data).await.is_err() {
                                    break;
                                }
                                if is_terminal {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("MCP StreamableHTTP POST SSE error: {}", e);
                            break;
                        }
                    }
                }
                if !saw_terminal_response && connected.load(Ordering::SeqCst) {
                    if let Some(id) = request_id {
                        let synthetic = serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": RESPONSE_STREAM_CLOSED_ERROR,
                                "message": "Streamable HTTP response stream closed before a terminal response"
                            }
                        });
                        let _ = tx.send(synthetic.to_string()).await;
                    }
                }
            });

            // Track the forwarder so disconnect() can abort it deterministically.
            // Also prune any already-finished handles so the Vec doesn't grow
            // unbounded across many POSTs.
            let mut handles = self.post_sse_handles.lock().await;
            handles.retain(|entry| !entry.handle.is_finished());
            handles.push(PostSseHandle { request_id, handle });
        } else {
            // JSON response — forward the body directly.
            let body = response.text().await?;
            if !body.trim().is_empty() {
                trace!(
                    "MCP StreamableHTTP POST response is JSON (bytes={})",
                    body.len()
                );
                self.route_message(body).await?;
            }
        }

        Ok(())
    }

    async fn route_message(&self, body: String) -> Result<()> {
        let Some(tx) = self.message_tx.as_ref() else {
            return Err(McpError::Disconnected);
        };
        tx.send(body).await.map_err(|_| McpError::Disconnected)
    }

    fn parse_json_rpc_error_response(body: &str) -> Option<JsonRpcError> {
        let value = serde_json::from_str::<serde_json::Value>(body).ok()?;
        if value.get("jsonrpc").and_then(serde_json::Value::as_str) != Some("2.0") {
            return None;
        }
        serde_json::from_value(value.get("error")?.clone()).ok()
    }

    fn is_terminal_response_for(body: &str, request_id: u64) -> bool {
        serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .is_some_and(|value| {
                (value.get("id").and_then(serde_json::Value::as_u64) == Some(request_id)
                    && (value.get("result").is_some() || value.get("error").is_some()))
                    || (value.get("method").and_then(serde_json::Value::as_str)
                        == Some("notifications/cancelled")
                        && value
                            .get("params")
                            .and_then(|params| params.get("requestId"))
                            .and_then(serde_json::Value::as_u64)
                            == Some(request_id))
            })
    }

    /// Attempt to open a GET SSE stream for server-initiated messages.
    /// Per spec, the server MAY return 405 if it doesn't support this.
    async fn start_get_sse_stream(&self) {
        let mut headers = self.build_headers().unwrap_or_default();
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );

        if let Some(version) = self.legacy_protocol_version.lock().await.as_deref() {
            if let Ok(value) = HeaderValue::from_str(version) {
                headers.insert(MCP_PROTOCOL_VERSION_HEADER, value);
            }
        }

        // Add session id if available.
        {
            let sid = self.session_id.lock().await;
            if let Some(sid) = sid.as_ref() {
                if let Ok(value) = HeaderValue::from_str(sid) {
                    headers.insert(MCP_SESSION_ID_HEADER, value);
                }
            }
        }

        trace!(
            "MCP StreamableHTTP GET SSE stream (url={})",
            Self::redact_url_for_log(&self.config.url)
        );

        let response = match self
            .client
            .get(&self.config.url)
            .headers(headers)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("MCP StreamableHTTP GET SSE stream failed: {}", e);
                return;
            }
        };

        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            debug!("MCP StreamableHTTP server does not support GET SSE stream (405)");
            return;
        }

        if !response.status().is_success() {
            debug!(
                "MCP StreamableHTTP GET SSE stream returned: {}",
                response.status()
            );
            return;
        }

        // Extract session id from GET response if present.
        if let Some(sid) = response.headers().get(MCP_SESSION_ID_HEADER) {
            if let Ok(sid_str) = sid.to_str() {
                let mut guard = self.session_id.lock().await;
                guard.get_or_insert_with(|| sid_str.to_string());
            }
        }

        debug!("MCP StreamableHTTP GET SSE stream opened");

        let Some(tx) = self.message_tx.clone() else {
            return; // disconnected
        };
        let connected = self.connected.clone();

        let handle = tokio::spawn(async move {
            let mut stream = response.bytes_stream().eventsource();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(evt) => {
                        if !evt.data.trim().is_empty() {
                            trace!(
                                "MCP StreamableHTTP GET SSE event (event='{}', data_len={})",
                                evt.event,
                                evt.data.len()
                            );
                            if tx.send(evt.data).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("MCP StreamableHTTP GET SSE error: {}", e);
                        break;
                    }
                }
            }
            connected.store(false, Ordering::SeqCst);
        });

        let mut guard = self.get_sse_handle.lock().await;
        *guard = Some(handle);
    }
}

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    async fn connect(&mut self) -> Result<()> {
        debug!(
            "Connecting to MCP StreamableHTTP endpoint: {} (connect_timeout_ms={})",
            Self::redact_url_for_log(&self.config.url),
            self.config.connect_timeout_ms
        );

        // Streamable HTTP has no separate wire-level connect step. The protocol
        // client first probes with `server/discover`, or falls back to legacy
        // `initialize`.
        //
        // Recreate the message channel so a connect()-after-disconnect() works.
        // disconnect() drops `message_tx` and the receiver is take-once, so
        // without this a second connect() would leave POST responses silently
        // dropped (no sender) and the client message handler unstarted (no
        // receiver to take). This mirrors the stdio/SSE transports, which also
        // recreate their channel in connect().
        let (message_tx, message_rx) = mpsc::channel(256);
        self.message_tx = Some(message_tx);
        *self.message_rx.lock().await = Some(message_rx);

        // Clear any stale GET-SSE handle so the next send() re-opens the stream
        // against the fresh channel.
        {
            let mut guard = self.get_sse_handle.lock().await;
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
        {
            let mut handles = self.post_sse_handles.lock().await;
            for entry in handles.drain(..) {
                entry.handle.abort();
            }
        }

        self.connected.store(true, Ordering::SeqCst);

        debug!("MCP StreamableHTTP transport ready");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnecting MCP StreamableHTTP transport");

        self.connected.store(false, Ordering::SeqCst);

        // Drop the struct's message sender so the channel closes, waking the
        // client handler (if any) without polling.
        self.message_tx = None;

        // Cancel the GET SSE stream background task.
        {
            let mut guard = self.get_sse_handle.lock().await;
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }

        // Abort all per-POST SSE forwarder tasks for deterministic shutdown.
        // Dropping `message_tx` above would eventually make them exit on the
        // next send-Err, but aborting is immediate and leaves no leaked tasks.
        {
            let mut handles = self.post_sse_handles.lock().await;
            for entry in handles.drain(..) {
                entry.handle.abort();
            }
        }

        // Send DELETE to terminate session (best-effort).
        {
            let sid = self.session_id.lock().await;
            if let Some(session_id) = sid.as_ref() {
                let mut headers = self.build_headers()?;
                if let Ok(value) = HeaderValue::from_str(session_id) {
                    headers.insert(MCP_SESSION_ID_HEADER, value);
                }
                if let Some(version) = self.legacy_protocol_version.lock().await.as_deref() {
                    if let Ok(value) = HeaderValue::from_str(version) {
                        headers.insert(MCP_PROTOCOL_VERSION_HEADER, value);
                    }
                }

                trace!(
                    "MCP StreamableHTTP DELETE session (url={})",
                    Self::redact_url_for_log(&self.config.url)
                );
                let _ = self
                    .client
                    .delete(&self.config.url)
                    .headers(headers)
                    .send()
                    .await;
            }
        }

        // Clear session id.
        {
            let mut guard = self.session_id.lock().await;
            *guard = None;
        }
        {
            let mut guard = self.legacy_protocol_version.lock().await;
            *guard = None;
        }

        debug!("MCP StreamableHTTP transport disconnected");
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
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        let starts_legacy_operation_phase = !metadata.modern && metadata.protocol_version.is_some();
        if let Some(version) = (!metadata.modern)
            .then_some(metadata.protocol_version.as_ref())
            .flatten()
        {
            *self.legacy_protocol_version.lock().await = Some(version.clone());
        }

        self.post_and_route_response(message, &metadata).await?;

        // GET streams and protocol sessions were removed in 2026-07-28. Open
        // the standalone stream only after legacy initialization has completed
        // and the negotiated protocol version is available.
        if starts_legacy_operation_phase {
            let guard = self.get_sse_handle.lock().await;
            if guard.is_none() {
                drop(guard);
                self.start_get_sse_stream().await;
            }
        }

        Ok(())
    }

    async fn cancel_request(
        &self,
        request_id: u64,
        reason: &str,
        metadata: McpCancellationMetadata,
    ) -> Result<()> {
        if metadata.modern {
            let mut handles = self.post_sse_handles.lock().await;
            if let Some(index) = handles
                .iter()
                .position(|entry| entry.request_id == Some(request_id))
            {
                let entry = handles.remove(index);
                entry.handle.abort();
                trace!(
                    "Cancelled modern MCP StreamableHTTP request by closing response stream (id={})",
                    request_id
                );
            }
            return Ok(());
        }

        // Initialization-based Streamable HTTP revisions use the JSON-RPC
        // cancellation notification, just like stdio. They do not assign an
        // SSE response stream as the cancellation primitive.
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/cancelled".to_string(),
            params: Some(serde_json::json!({
                "requestId": request_id,
                "reason": reason,
            })),
        };
        self.send_with_metadata(
            serde_json::to_string(&notification)?,
            McpTransportMetadata {
                request_id: None,
                protocol_version: metadata.protocol_version,
                modern: false,
                method: notification.method,
                name: None,
                tool_parameter_headers: Vec::new(),
            },
        )
        .await
    }

    fn is_legacy_fallback_error(&self, error: &McpError) -> bool {
        matches!(
            error,
            McpError::HttpStatus {
                status: 400..=499,
                ..
            } | McpError::HttpProtocol {
                status: 400..=499,
                ..
            }
        )
    }

    fn requires_tool_parameter_headers(&self) -> bool {
        true
    }

    async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
        self.message_rx.lock().await.take()
    }

    async fn receive(&self) -> Result<Option<String>> {
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        let mut guard = self.message_rx.lock().await;
        match guard.as_mut() {
            None => Err(McpError::Disconnected),
            Some(rx) => {
                match tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await
                {
                    Ok(Some(message)) => {
                        trace!(
                            "MCP StreamableHTTP received message (bytes={})",
                            message.len()
                        );
                        Ok(Some(message))
                    }
                    Ok(None) => {
                        warn!("MCP StreamableHTTP message channel closed");
                        Err(McpError::Disconnected)
                    }
                    Err(_) => Ok(None),
                }
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

impl Drop for StreamableHttpTransport {
    /// Safety net: if the transport is dropped without an explicit
    /// `disconnect()`, abort the background forwarder tasks so they don't leak.
    /// The locks are uncontended at drop time (no other owner), so `try_lock`
    /// succeeds; if it ever didn't, the tasks still exit once `message_tx` is
    /// dropped with the struct.
    fn drop(&mut self) {
        if let Ok(mut guard) = self.get_sse_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
        if let Ok(mut handles) = self.post_sse_handles.try_lock() {
            for entry in handles.drain(..) {
                entry.handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn create_test_config() -> StreamableHttpConfig {
        StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![],
            connect_timeout_ms: 5000,
        }
    }

    async fn serve_http_once(status: &str, content_type: &str, body: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let status = status.to_string();
        let content_type = content_type.to_string();
        let body = body.to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test request");
            let mut request = vec![0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read test request");
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write test response");
        });
        format!("http://{address}/mcp")
    }

    async fn serve_http_once_and_capture(
        status: &str,
        content_type: &str,
        body: &str,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let status = status.to_string();
        let content_type = content_type.to_string();
        let body = body.to_string();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test request");
            let mut request = Vec::new();
            let expected_len = loop {
                let mut chunk = [0_u8; 2048];
                let read = socket.read(&mut chunk).await.expect("read test request");
                assert!(read > 0, "request closed before headers completed");
                request.extend_from_slice(&chunk[..read]);
                let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_len = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                break header_end + 4 + content_len;
            };
            while request.len() < expected_len {
                let mut chunk = [0_u8; 2048];
                let read = socket.read(&mut chunk).await.expect("read test body");
                assert!(read > 0, "request closed before body completed");
                request.extend_from_slice(&chunk[..read]);
            }
            let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write test response");
        });
        (format!("http://{address}/mcp"), request_rx)
    }

    #[test]
    fn test_transport_new() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);
        assert!(!transport.is_connected());
    }

    #[test]
    fn test_build_headers_basic() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);
        let headers = transport.build_headers().unwrap();

        assert_eq!(headers.get(reqwest::header::ACCEPT).unwrap(), ACCEPT_HEADER);
        assert_eq!(
            headers.get(reqwest::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_build_headers_with_custom() {
        let config = StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![HeaderConfig {
                name: "Authorization".to_string(),
                value: "Bearer token123".to_string(),
                value_encrypted: None,
                credential_ref: None,
            }],
            connect_timeout_ms: 5000,
        };
        let transport = StreamableHttpTransport::new(config);
        let headers = transport.build_headers().unwrap();

        assert!(headers.contains_key("authorization"));
    }

    #[test]
    fn test_build_headers_invalid_name() {
        let config = StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![HeaderConfig {
                name: "Invalid\nName".to_string(),
                value: "test".to_string(),
                value_encrypted: None,
                credential_ref: None,
            }],
            connect_timeout_ms: 5000,
        };
        let transport = StreamableHttpTransport::new(config);
        assert!(transport.build_headers().is_err());
    }

    #[test]
    fn modern_request_headers_are_body_derived_and_safely_encoded() {
        let config = StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![
                HeaderConfig {
                    name: "MCP-Protocol-Version".to_string(),
                    value: "stale".to_string(),
                    value_encrypted: None,
                    credential_ref: None,
                },
                HeaderConfig {
                    name: "Mcp-Method".to_string(),
                    value: "wrong/method".to_string(),
                    value_encrypted: None,
                    credential_ref: None,
                },
                HeaderConfig {
                    name: "Mcp-Name".to_string(),
                    value: "wrong-name".to_string(),
                    value_encrypted: None,
                    credential_ref: None,
                },
                HeaderConfig {
                    name: "Mcp-Param-Unused".to_string(),
                    value: "must-be-removed".to_string(),
                    value_encrypted: None,
                    credential_ref: None,
                },
                HeaderConfig {
                    name: "Authorization".to_string(),
                    value: "Bearer retained".to_string(),
                    value_encrypted: None,
                    credential_ref: None,
                },
            ],
            connect_timeout_ms: 5000,
        };
        let transport = StreamableHttpTransport::new(config);
        let mut headers = transport.build_headers().expect("configured headers");
        StreamableHttpTransport::apply_request_metadata(
            &mut headers,
            &McpTransportMetadata {
                request_id: Some(7),
                protocol_version: Some("2026-07-28".to_string()),
                modern: true,
                method: "tools/call".to_string(),
                name: Some("天气".to_string()),
                tool_parameter_headers: vec![
                    ("Region".to_string(), " 华东 ".to_string()),
                    ("Shard".to_string(), "42".to_string()),
                ],
            },
        )
        .expect("modern headers");

        assert_eq!(headers[MCP_PROTOCOL_VERSION_HEADER], "2026-07-28");
        assert_eq!(headers[MCP_METHOD_HEADER], "tools/call");
        assert_eq!(
            headers[MCP_NAME_HEADER],
            format!("=?base64?{}?=", BASE64_STANDARD.encode("天气"))
        );
        assert_eq!(
            headers["mcp-param-region"],
            format!("=?base64?{}?=", BASE64_STANDARD.encode(" 华东 "))
        );
        assert_eq!(headers["mcp-param-shard"], "42");
        assert!(!headers.contains_key("mcp-param-unused"));
        assert_eq!(headers["authorization"], "Bearer retained");
    }

    #[test]
    fn legacy_request_omits_modern_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(MCP_METHOD_HEADER, HeaderValue::from_static("stale"));
        headers.insert(MCP_NAME_HEADER, HeaderValue::from_static("stale"));
        headers.insert("mcp-param-region", HeaderValue::from_static("stale"));

        StreamableHttpTransport::apply_request_metadata(
            &mut headers,
            &McpTransportMetadata {
                request_id: Some(7),
                protocol_version: Some("2025-11-25".to_string()),
                modern: false,
                method: "tools/call".to_string(),
                name: Some("weather".to_string()),
                tool_parameter_headers: vec![("Region".to_string(), "west".to_string())],
            },
        )
        .expect("legacy headers");

        assert_eq!(headers[MCP_PROTOCOL_VERSION_HEADER], "2025-11-25");
        assert!(!headers.contains_key(MCP_METHOD_HEADER));
        assert!(!headers.contains_key(MCP_NAME_HEADER));
        assert!(!headers.contains_key("mcp-param-region"));
    }

    #[test]
    fn modern_header_encoding_escapes_sentinel_and_control_characters() {
        let sentinel = StreamableHttpTransport::encoded_header_value("=?base64?literal?=", "test")
            .expect("sentinel encoding");
        assert_eq!(
            sentinel,
            format!(
                "=?base64?{}?=",
                BASE64_STANDARD.encode("=?base64?literal?=")
            )
        );

        let newline = StreamableHttpTransport::encoded_header_value("line1\nline2", "test")
            .expect("control encoding");
        assert_eq!(
            newline,
            format!("=?base64?{}?=", BASE64_STANDARD.encode("line1\nline2"))
        );
    }

    #[test]
    fn parses_json_rpc_error_bodies_with_or_without_id() {
        let with_id = StreamableHttpTransport::parse_json_rpc_error_response(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32022,"message":"unsupported"}}"#,
        )
        .expect("error with id");
        assert_eq!(with_id.code, -32022);
        let without_id = StreamableHttpTransport::parse_json_rpc_error_response(
            r#"{"jsonrpc":"2.0","error":{"code":-32023,"message":"missing capability"}}"#,
        )
        .expect("error without id");
        assert_eq!(without_id.code, -32023);
        assert!(StreamableHttpTransport::parse_json_rpc_error_response(
            r#"{"jsonrpc":"2.0","id":1,"result":{}}"#
        )
        .is_none());
        assert!(
            StreamableHttpTransport::parse_json_rpc_error_response("legacy error page").is_none()
        );
    }

    #[tokio::test]
    async fn non_success_json_rpc_error_without_id_surfaces_directly() {
        let url = serve_http_once(
            "400 Bad Request",
            "application/json",
            r#"{"jsonrpc":"2.0","error":{"code":-32022,"message":"unsupported"}}"#,
        )
        .await;
        let transport = StreamableHttpTransport::new(StreamableHttpConfig {
            url,
            headers: Vec::new(),
            connect_timeout_ms: 1000,
        });
        let error = transport
            .post_and_route_response(
                r#"{"jsonrpc":"2.0","id":7,"method":"server/discover","params":{}}"#.to_string(),
                &McpTransportMetadata {
                    request_id: Some(7),
                    protocol_version: Some("2026-07-28".to_string()),
                    modern: true,
                    method: "server/discover".to_string(),
                    name: None,
                    tool_parameter_headers: Vec::new(),
                },
            )
            .await
            .expect_err("HTTP JSON-RPC error must surface");
        assert!(matches!(
            error,
            McpError::HttpProtocol {
                status: 400,
                code: -32022,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn non_json_http_errors_preserve_status_for_era_detection() {
        for (status_line, expected_status) in [
            ("400 Bad Request", 400_u16),
            ("404 Not Found", 404_u16),
            ("503 Service Unavailable", 503_u16),
        ] {
            let url = serve_http_once(status_line, "text/plain", "not JSON-RPC").await;
            let transport = StreamableHttpTransport::new(StreamableHttpConfig {
                url,
                headers: Vec::new(),
                connect_timeout_ms: 1000,
            });
            let error = transport
                .post_and_route_response(
                    r#"{"jsonrpc":"2.0","id":7,"method":"server/discover","params":{}}"#
                        .to_string(),
                    &McpTransportMetadata {
                        request_id: Some(7),
                        protocol_version: Some("2026-07-28".to_string()),
                        modern: true,
                        method: "server/discover".to_string(),
                        name: None,
                        tool_parameter_headers: Vec::new(),
                    },
                )
                .await
                .expect_err("HTTP status must surface");
            assert!(matches!(
                error,
                McpError::HttpStatus {
                    status,
                    ref body
                } if status == expected_status && body == "not JSON-RPC"
            ));
        }
    }

    #[test]
    fn only_http_4xx_is_a_legacy_fallback_signal() {
        let transport = StreamableHttpTransport::new(create_test_config());
        assert!(transport.is_legacy_fallback_error(&McpError::HttpStatus {
            status: 400,
            body: "legacy".to_string(),
        }));
        assert!(transport.is_legacy_fallback_error(&McpError::HttpStatus {
            status: 404,
            body: "legacy".to_string(),
        }));
        assert!(transport.is_legacy_fallback_error(&McpError::HttpProtocol {
            status: 400,
            code: -32601,
            message: "Method not found".to_string(),
            data: None,
        }));
        assert!(!transport.is_legacy_fallback_error(&McpError::HttpStatus {
            status: 503,
            body: "temporary failure".to_string(),
        }));
        assert!(!transport
            .is_legacy_fallback_error(&McpError::Timeout("modern probe timed out".to_string(),)));
        assert!(!transport
            .is_legacy_fallback_error(&McpError::Transport("connection refused".to_string(),)));
    }

    #[tokio::test]
    async fn abrupt_post_sse_eof_resolves_the_owning_request() {
        let url = serve_http_once("200 OK", "text/event-stream", "").await;
        let mut transport = StreamableHttpTransport::new(StreamableHttpConfig {
            url,
            headers: Vec::new(),
            connect_timeout_ms: 1000,
        });
        transport.connect().await.expect("connect");
        let mut receiver = transport
            .take_message_receiver()
            .await
            .expect("message receiver");
        transport
            .post_and_route_response(
                r#"{"jsonrpc":"2.0","id":7,"method":"subscriptions/listen","params":{}}"#
                    .to_string(),
                &McpTransportMetadata {
                    request_id: Some(7),
                    protocol_version: Some("2026-07-28".to_string()),
                    modern: true,
                    method: "subscriptions/listen".to_string(),
                    name: None,
                    tool_parameter_headers: Vec::new(),
                },
            )
            .await
            .expect("accepted SSE response");

        let routed = tokio::time::timeout(tokio::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("synthetic response timeout")
            .expect("synthetic response");
        let response: serde_json::Value =
            serde_json::from_str(&routed).expect("synthetic response JSON");
        assert_eq!(response["id"], 7);
        assert_eq!(response["error"]["code"], RESPONSE_STREAM_CLOSED_ERROR);
    }

    #[test]
    fn terminal_response_detection_requires_matching_request_id() {
        assert!(StreamableHttpTransport::is_terminal_response_for(
            r#"{"jsonrpc":"2.0","id":7,"result":{}}"#,
            7
        ));
        assert!(StreamableHttpTransport::is_terminal_response_for(
            r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"failed"}}"#,
            7
        ));
        assert!(StreamableHttpTransport::is_terminal_response_for(
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}"#,
            7
        ));
        assert!(!StreamableHttpTransport::is_terminal_response_for(
            r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{}}"#,
            7
        ));
        assert!(!StreamableHttpTransport::is_terminal_response_for(
            r#"{"jsonrpc":"2.0","id":8,"result":{}}"#,
            7
        ));
    }

    #[test]
    fn test_redact_url() {
        assert_eq!(
            StreamableHttpTransport::redact_url_for_log("http://example.com/mcp?token=secret"),
            "http://example.com/mcp"
        );
    }

    #[tokio::test]
    async fn test_send_disconnected() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);

        let result = transport.send("{}".to_string()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            e => panic!("Expected Disconnected, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_receive_disconnected() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);

        let result = transport.receive().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            e => panic!("Expected Disconnected, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_connect_disconnect() {
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);

        transport.connect().await.unwrap();
        assert!(transport.is_connected());

        transport.disconnect().await.unwrap();
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn test_receive_timeout() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);
        transport.connected.store(true, Ordering::SeqCst);

        let result = transport.receive().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_session_id_stored_on_response() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);
        transport.connected.store(true, Ordering::SeqCst);

        // Simulate a session id being stored.
        {
            let mut guard = transport.session_id.lock().await;
            *guard = Some("test-session-123".to_string());
        }

        let sid = transport.session_id.lock().await;
        assert_eq!(sid.as_deref(), Some("test-session-123"));
    }

    #[tokio::test]
    async fn test_connect_after_disconnect_recreates_channel() {
        // Reconnect must restore a usable channel: after disconnect() the
        // sender is dropped and the receiver is taken, so a naive transport
        // would silently drop POST responses and never start the handler.
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);

        transport.connect().await.unwrap();
        // The client takes the receiver once when starting its message handler.
        let rx = transport.take_message_receiver().await;
        assert!(rx.is_some(), "first connect should expose a receiver");
        drop(rx);

        transport.disconnect().await.unwrap();
        // After disconnect the sender is gone and the receiver was taken.
        assert!(transport.message_tx.is_none());
        assert!(transport.take_message_receiver().await.is_none());

        // Second connect() must recreate both ends.
        transport.connect().await.unwrap();
        assert!(
            transport.message_tx.is_some(),
            "reconnect should recreate the sender so POST responses are routed"
        );
        let rx2 = transport.take_message_receiver().await;
        assert!(
            rx2.is_some(),
            "reconnect should recreate the receiver so the handler starts"
        );

        // And the recreated channel actually carries a message.
        let tx = transport.message_tx.clone().unwrap();
        tx.send("ping".to_string()).await.unwrap();
        let mut rx2 = rx2.unwrap();
        assert_eq!(rx2.recv().await.as_deref(), Some("ping"));
    }

    #[tokio::test]
    async fn test_disconnect_aborts_post_sse_forwarders() {
        // A POST-SSE forwarder is a spawned task; disconnect() must abort it
        // deterministically rather than leaving it to exit on the next
        // send-Err. We simulate a forwarder with a task that would otherwise
        // run forever and flips a flag only when it actually exits.
        use std::sync::atomic::AtomicBool;

        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);
        transport.connect().await.unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let started_clone = started.clone();
        let handle = tokio::spawn(async move {
            started_clone.store(true, Ordering::SeqCst);
            // Run forever unless aborted.
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
            }
        });
        transport.post_sse_handles.lock().await.push(PostSseHandle {
            request_id: Some(1),
            handle,
        });

        // Let the task begin.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(started.load(Ordering::SeqCst), "forwarder task should run");

        transport.disconnect().await.unwrap();

        // After disconnect the handle vec is drained and the task is aborted.
        assert!(
            transport.post_sse_handles.lock().await.is_empty(),
            "disconnect should drain the forwarder handles"
        );
    }

    #[tokio::test]
    async fn test_disconnect_aborts_forwarder_handle_is_finished() {
        // Stronger assertion: capture a clone-free handle, confirm it reports
        // finished after disconnect aborts it.
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);
        transport.connect().await.unwrap();

        // Spawn a forever task, hold an abort handle to observe termination.
        let forever = tokio::spawn(async {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
            }
        });
        let abort_handle = forever.abort_handle();
        transport.post_sse_handles.lock().await.push(PostSseHandle {
            request_id: Some(1),
            handle: forever,
        });

        assert!(!abort_handle.is_finished(), "task should be running");

        transport.disconnect().await.unwrap();

        // Give the runtime a moment to process the abort.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(
            abort_handle.is_finished(),
            "disconnect should have aborted the forwarder task"
        );
    }

    #[tokio::test]
    async fn cancelling_request_aborts_only_its_response_stream() {
        let config = create_test_config();
        let transport = StreamableHttpTransport::new(config);
        let first = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let first_abort = first.abort_handle();
        let second = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let second_abort = second.abort_handle();
        {
            let mut handles = transport.post_sse_handles.lock().await;
            handles.push(PostSseHandle {
                request_id: Some(7),
                handle: first,
            });
            handles.push(PostSseHandle {
                request_id: Some(8),
                handle: second,
            });
        }

        transport
            .cancel_request(
                7,
                "test timeout",
                McpCancellationMetadata {
                    protocol_version: Some("2026-07-28".to_string()),
                    modern: true,
                },
            )
            .await
            .expect("cancel stream");
        tokio::task::yield_now().await;
        assert!(first_abort.is_finished());
        assert!(!second_abort.is_finished());
        assert_eq!(transport.post_sse_handles.lock().await.len(), 1);

        second_abort.abort();
    }

    #[tokio::test]
    async fn legacy_http_cancellation_sends_notification_instead_of_closing_stream() {
        let (url, request_rx) =
            serve_http_once_and_capture("202 Accepted", "application/json", "").await;
        let mut transport = StreamableHttpTransport::new(StreamableHttpConfig {
            url,
            headers: Vec::new(),
            connect_timeout_ms: 1000,
        });
        transport.connect().await.expect("connect");

        transport
            .cancel_request(
                19,
                "legacy timeout",
                McpCancellationMetadata {
                    protocol_version: Some("2025-11-25".to_string()),
                    modern: false,
                },
            )
            .await
            .expect("send legacy cancellation");

        let request = request_rx.await.expect("captured cancellation");
        let request_lower = request.to_ascii_lowercase();
        assert!(request_lower.contains("mcp-protocol-version: 2025-11-25"));
        let body = request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .expect("HTTP body");
        let notification: serde_json::Value =
            serde_json::from_str(body).expect("cancellation notification JSON");
        assert_eq!(notification["method"], "notifications/cancelled");
        assert_eq!(notification["params"]["requestId"], 19);
        assert_eq!(notification["params"]["reason"], "legacy timeout");
    }

    #[tokio::test]
    async fn test_drop_aborts_forwarder_handles() {
        // Dropping the transport without disconnect() must not leak forwarders.
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);
        transport.connect().await.unwrap();

        let forever = tokio::spawn(async {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
            }
        });
        let abort_handle = forever.abort_handle();
        transport.post_sse_handles.lock().await.push(PostSseHandle {
            request_id: Some(1),
            handle: forever,
        });

        assert!(!abort_handle.is_finished());

        drop(transport);

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(
            abort_handle.is_finished(),
            "dropping the transport should abort forwarder tasks"
        );
    }

    #[tokio::test]
    async fn test_message_channel_backpressure_blocks_never_drops() {
        // Guards the #23 invariant: the bounded channel must apply
        // backpressure (block the sender) rather than silently drop messages
        // once full. With capacity 256, the 257th send must not complete until
        // the receiver drains one.
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);
        transport.connect().await.unwrap();

        let tx = transport.message_tx.clone().unwrap();
        // Fill the channel to capacity (256).
        for i in 0..256 {
            tx.send(format!("msg-{i}")).await.unwrap();
        }

        // The next send must block (not drop). try_send should report Full.
        let pending = tx.try_send("overflow".to_string());
        assert!(
            matches!(pending, Err(mpsc::error::TrySendError::Full(_))),
            "channel at capacity must signal Full (backpressure), never drop"
        );

        // Drain one and confirm ordering preserved (no drops).
        let mut rx = transport.take_message_receiver().await.unwrap();
        assert_eq!(rx.recv().await.as_deref(), Some("msg-0"));
        // Now there's room: the blocked send can complete.
        tx.send("overflow".to_string()).await.unwrap();
    }

    #[tokio::test]
    async fn test_disconnect_clears_session() {
        let config = create_test_config();
        let mut transport = StreamableHttpTransport::new(config);
        transport.connect().await.unwrap();

        {
            let mut guard = transport.session_id.lock().await;
            *guard = Some("test-session".to_string());
        }

        transport.disconnect().await.unwrap();

        let sid = transport.session_id.lock().await;
        assert!(sid.is_none());
    }
}
