//! MCP Streamable HTTP transport (MCP protocol version 2025-03-26).
//!
//! Implements the "Streamable HTTP" transport where a single HTTP endpoint
//! handles POST (send), GET (server-initiated SSE), and DELETE (session termination).
//! See <https://modelcontextprotocol.io/specification/2025-03-26/basic/transports>

use async_trait::async_trait;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace, warn};

use crate::config::{HeaderConfig, StreamableHttpConfig};
use crate::error::{McpError, Result};
use crate::protocol::client::McpTransport;

const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const ACCEPT_HEADER: &str = "application/json, text/event-stream";

pub struct StreamableHttpTransport {
    config: StreamableHttpConfig,
    client: Client,
    session_id: Arc<Mutex<Option<String>>>,
    connected: Arc<AtomicBool>,
    // Dropped in disconnect() so the channel closes and the client handler
    // wakes without polling.
    message_tx: Option<mpsc::Sender<String>>,
    message_rx: Mutex<Option<mpsc::Receiver<String>>>,
    get_sse_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Per-POST SSE forwarder tasks. Stored so they can be aborted
    // deterministically in disconnect() (they otherwise exit only on the next
    // send-Err once the receiver is dropped).
    post_sse_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
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
            connected: Arc::new(AtomicBool::new(false)),
            message_tx: Some(message_tx),
            message_rx: Mutex::new(Some(message_rx)),
            get_sse_handle: Mutex::new(None),
            post_sse_handles: Mutex::new(Vec::new()),
        }
    }

    fn build_headers(&self, include_session_id: bool) -> Result<HeaderMap> {
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

        if include_session_id {
            // Session id is added per-request in send() after reading the lock.
        }

        Ok(headers)
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
        session_id: Option<String>,
    ) -> Result<()> {
        let mut headers = self.build_headers(true)?;

        if let Some(sid) = session_id {
            let value = HeaderValue::from_str(&sid)
                .map_err(|e| McpError::Transport(format!("Invalid session id: {}", e)))?;
            headers.insert(MCP_SESSION_ID_HEADER, value);
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

        // Extract session id from response if present.
        if let Some(sid) = response.headers().get(MCP_SESSION_ID_HEADER) {
            let sid_str = sid
                .to_str()
                .map_err(|e| McpError::Transport(format!("Invalid session id header: {}", e)))?;
            let mut guard = self.session_id.lock().await;
            guard.get_or_insert_with(|| sid_str.to_string());
        }

        if status == reqwest::StatusCode::ACCEPTED {
            // Server accepted notification/response, no body.
            trace!("MCP StreamableHTTP POST accepted (202)");
            return Ok(());
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(McpError::Transport(format!(
                "POST failed: {} - {}",
                status, body
            )));
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
            let url = self.config.url.clone();
            let connected = self.connected.clone();

            // We need to consume the response body in a spawned task to avoid
            // blocking the caller. Events from this POST's SSE response are
            // forwarded to the channel so receive() can pick them up.
            let handle = tokio::spawn(async move {
                let mut stream = response.bytes_stream().eventsource();
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(evt) => {
                            if !evt.data.trim().is_empty() {
                                trace!(
                                    "MCP StreamableHTTP POST SSE event (event='{}', data_len={})",
                                    evt.event,
                                    evt.data.len()
                                );
                                if tx.send(evt.data).await.is_err() {
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
                let _ = (url, connected); // suppress unused warnings
            });

            // Track the forwarder so disconnect() can abort it deterministically.
            // Also prune any already-finished handles so the Vec doesn't grow
            // unbounded across many POSTs.
            let mut handles = self.post_sse_handles.lock().await;
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        } else {
            // JSON response — forward the body directly.
            let body = response.text().await?;
            if !body.trim().is_empty() {
                trace!(
                    "MCP StreamableHTTP POST response is JSON (bytes={})",
                    body.len()
                );
                if let Some(tx) = self.message_tx.as_ref() {
                    if tx.send(body).await.is_err() {
                        warn!("MCP StreamableHTTP: message channel closed");
                    }
                }
            }
        }

        Ok(())
    }

    /// Attempt to open a GET SSE stream for server-initiated messages.
    /// Per spec, the server MAY return 405 if it doesn't support this.
    async fn start_get_sse_stream(&self) {
        let mut headers = self.build_headers(true).unwrap_or_default();
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );

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

        // Streamable HTTP doesn't have a separate "connect" step in the traditional
        // sense. The first request (initialize) will be sent via send(). Here we
        // just mark the transport as ready and optionally open a GET SSE stream.
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
            for handle in handles.drain(..) {
                handle.abort();
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
            for handle in handles.drain(..) {
                handle.abort();
            }
        }

        // Send DELETE to terminate session (best-effort).
        {
            let sid = self.session_id.lock().await;
            if let Some(session_id) = sid.as_ref() {
                let mut headers = self.build_headers(false)?;
                if let Ok(value) = HeaderValue::from_str(session_id) {
                    headers.insert(MCP_SESSION_ID_HEADER, value);
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

        debug!("MCP StreamableHTTP transport disconnected");
        Ok(())
    }

    async fn send(&self, message: String) -> Result<()> {
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        let session_id = self.session_id.lock().await.clone();

        self.post_and_route_response(message, session_id).await?;

        // After the first successful exchange, try to open the GET SSE stream
        // for server-initiated messages (if not already opened).
        {
            let guard = self.get_sse_handle.lock().await;
            if guard.is_none() {
                // Don't hold the lock while starting the stream.
                drop(guard);
                self.start_get_sse_stream().await;
            }
        }

        Ok(())
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
            for handle in handles.drain(..) {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> StreamableHttpConfig {
        StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![],
            connect_timeout_ms: 5000,
        }
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
        let headers = transport.build_headers(false).unwrap();

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
        let headers = transport.build_headers(false).unwrap();

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
        assert!(transport.build_headers(false).is_err());
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
        transport.post_sse_handles.lock().await.push(handle);

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
        transport.post_sse_handles.lock().await.push(forever);

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
        transport.post_sse_handles.lock().await.push(forever);

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
