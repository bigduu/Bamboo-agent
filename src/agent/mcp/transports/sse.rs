use async_trait::async_trait;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::{header::HeaderMap, Client};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, info, trace, warn};

use crate::agent::mcp::config::{HeaderConfig, SseConfig};
use crate::agent::mcp::error::{McpError, Result};
use crate::agent::mcp::protocol::client::McpTransport;

#[derive(Debug, Clone)]
struct PostResponse {
    status: reqwest::StatusCode,
    body: String,
}

#[async_trait]
trait PostClient: Send + Sync {
    async fn post_json(&self, url: String, headers: HeaderMap, body: String) -> Result<PostResponse>;
}

#[derive(Clone)]
struct ReqwestPostClient {
    client: Client,
}

#[async_trait]
impl PostClient for ReqwestPostClient {
    async fn post_json(&self, url: String, headers: HeaderMap, body: String) -> Result<PostResponse> {
        let response = self
            .client
            .post(&url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(body)
            .timeout(tokio::time::Duration::from_secs(60))
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            return Ok(PostResponse {
                status,
                body: String::new(),
            });
        }

        let body = response.text().await.unwrap_or_default();
        Ok(PostResponse { status, body })
    }
}

pub struct SseTransport {
    config: SseConfig,
    client: Client,
    post_client: Arc<dyn PostClient>,
    connected: Arc<AtomicBool>,
    message_tx: mpsc::Sender<String>,
    message_rx: Mutex<mpsc::Receiver<String>>,
    sse_handle: Option<tokio::task::JoinHandle<()>>,
    endpoint_url: Arc<Mutex<Option<String>>>,
    endpoint_notify: Arc<Notify>,
}

impl SseTransport {
    pub fn new(config: SseConfig) -> Self {
        Self::new_with_client(config, Client::new())
    }

    pub fn new_with_client(config: SseConfig, client: Client) -> Self {
        let (message_tx, message_rx) = mpsc::channel(100);
        let post_client: Arc<dyn PostClient> = Arc::new(ReqwestPostClient {
            client: client.clone(),
        });
        Self {
            config,
            client,
            post_client,
            connected: Arc::new(AtomicBool::new(false)),
            message_tx,
            message_rx: Mutex::new(message_rx),
            sse_handle: None,
            endpoint_url: Arc::new(Mutex::new(None)),
            endpoint_notify: Arc::new(Notify::new()),
        }
    }

    fn build_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            "text/event-stream".parse().unwrap(),
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

    fn redact_url_for_log(url: &str) -> String {
        // Avoid leaking secrets in query strings (e.g., session tokens).
        match reqwest::Url::parse(url) {
            Ok(mut parsed) => {
                parsed.set_query(None);
                parsed.set_fragment(None);
                parsed.to_string()
            }
            Err(_) => url.to_string(),
        }
    }

    fn resolve_endpoint(base: &str, endpoint: &str) -> Option<String> {
        // MCP SSE servers may send either an absolute URL or a relative path in the `endpoint` event.
        if endpoint.trim().is_empty() {
            return None;
        }

        if let Ok(url) = reqwest::Url::parse(endpoint) {
            return Some(url.to_string());
        }

        // Relative path - join against the base URL.
        if let Ok(base_url) = reqwest::Url::parse(base) {
            if let Ok(joined) = base_url.join(endpoint) {
                return Some(joined.to_string());
            }
        }

        None
    }

    fn fallback_post_url(&self) -> String {
        // If no endpoint was provided via SSE, fall back to common conventions:
        // - If the configured URL ends with `/sse`, assume POST is `.../message`.
        // - Otherwise (e.g. `/mcp`), many servers expect POST to the same URL.
        let base = self.config.url.trim_end_matches('/');
        if base.ends_with("/sse") {
            format!("{}/message", base.trim_end_matches("/sse"))
        } else {
            base.to_string()
        }
    }
}

#[async_trait]
impl McpTransport for SseTransport {
    async fn connect(&mut self) -> Result<()> {
        info!(
            "Connecting to MCP SSE endpoint: {} (connect_timeout_ms={})",
            Self::redact_url_for_log(&self.config.url),
            self.config.connect_timeout_ms
        );

        // Reset any previously learned endpoint.
        {
            let mut guard = self.endpoint_url.lock().await;
            *guard = None;
        }

        let headers = self.build_headers()?;
        // IMPORTANT: do NOT use reqwest's per-request `.timeout(...)` for SSE streams.
        // That timeout applies to *reading the response body*, and would terminate long-lived streams.
        // Instead, we enforce a timeout only for the initial handshake (waiting for response headers).
        let response = tokio::time::timeout(
            tokio::time::Duration::from_millis(self.config.connect_timeout_ms),
            self.client.get(&self.config.url).headers(headers).send(),
        )
        .await
        .map_err(|_| {
            McpError::Timeout(format!(
                "SSE connect timed out after {}ms",
                self.config.connect_timeout_ms
            ))
        })??;

        if !response.status().is_success() {
            // Best-effort body capture for diagnostics; non-success responses won't be streamed.
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(McpError::Connection(format!(
                "HTTP error: {} - {}",
                status, body
            )));
        }

        debug!(
            "MCP SSE connected (status={}, content_type={:?})",
            response.status(),
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
        );

        // Start SSE event handler
        let message_tx = self.message_tx.clone();
        let url = self.config.url.clone();
        let endpoint_url = self.endpoint_url.clone();
        let endpoint_notify = self.endpoint_notify.clone();
        let connected = self.connected.clone();

        let handle = tokio::spawn(async move {
            let mut stream = response.bytes_stream().eventsource();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        // Trace-level log: event types can be noisy (keepalive comments, etc.)
                        trace!(
                            "MCP SSE event received (event='{}', data_len={})",
                            event.event,
                            event.data.len()
                        );
                        if event.event == "endpoint" {
                            // Store the endpoint URL for POST requests
                            let resolved = SseTransport::resolve_endpoint(&url, &event.data)
                                .unwrap_or_else(|| event.data.clone());
                            {
                                let mut guard = endpoint_url.lock().await;
                                *guard = Some(resolved.clone());
                            }
                            endpoint_notify.notify_waiters();
                            debug!(
                                "MCP SSE provided endpoint: {}",
                                SseTransport::redact_url_for_log(&resolved)
                            );
                        } else if (event.event == "message" || event.event.is_empty())
                            && message_tx.send(event.data).await.is_err()
                        {
                            break;
                        } else if event.event != "message" && !event.event.is_empty() {
                            // Useful when debugging mismatched server implementations.
                            trace!("Ignoring MCP SSE event type: '{}'", event.event);
                        }
                    }
                    Err(e) => {
                        warn!("SSE stream error: {}", e);
                        break;
                    }
                }
            }
            connected.store(false, Ordering::SeqCst);
            warn!("SSE stream ended for {}", SseTransport::redact_url_for_log(&url));
        });

        self.sse_handle = Some(handle);
        self.connected.store(true, Ordering::SeqCst);

        info!("MCP SSE transport connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        info!("Disconnecting MCP SSE transport");

        self.connected.store(false, Ordering::SeqCst);

        if let Some(handle) = self.sse_handle.take() {
            handle.abort();
        }

        Ok(())
    }

    async fn send(&self, message: String) -> Result<()> {
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        // For SSE transport, we need to POST to the server-provided endpoint (preferred),
        // or use a best-effort fallback when the server doesn't send one.
        //
        // Some servers emit `event: endpoint` immediately after the SSE connection opens, but
        // because we process SSE events in a background task, the first JSON-RPC request can race
        // that event. We briefly wait for the endpoint to arrive to avoid posting to the wrong URL.
        let mut endpoint = self.endpoint_url.lock().await.clone();
        if endpoint.is_none() {
            // Short grace period; do not block for long since some servers never send an endpoint.
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_millis(250),
                self.endpoint_notify.notified(),
            )
            .await;
            endpoint = self.endpoint_url.lock().await.clone();
        }

        let used_endpoint_event_initial = endpoint.is_some();
        let mut post_url = endpoint.unwrap_or_else(|| self.fallback_post_url());

        let headers = self.build_headers()?;

        trace!(
            "MCP SSE POST send (url={}, used_endpoint_event={}, bytes={})",
            Self::redact_url_for_log(&post_url),
            used_endpoint_event_initial,
            message.len()
        );

        // Retry once if we raced the endpoint event and the fallback URL is wrong.
        // (Common symptom: 404/405 on the first `initialize` call, then subsequent calls work.)
        let mut attempt: u8 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let response = self
                .post_client
                .post_json(post_url.clone(), headers.clone(), message.clone())
                .await?;

            if response.status.is_success() {
                break;
            }

            let status = response.status;
            let body = response.body;

            if attempt == 1
                && !used_endpoint_event_initial
                && (status == reqwest::StatusCode::NOT_FOUND
                    || status == reqwest::StatusCode::METHOD_NOT_ALLOWED)
            {
                let endpoint_now = self.endpoint_url.lock().await.clone();
                if let Some(endpoint_now) = endpoint_now {
                    if endpoint_now != post_url {
                        debug!(
                            "MCP SSE POST retry due to {} (fallback_url={}, endpoint_url={})",
                            status,
                            Self::redact_url_for_log(&post_url),
                            Self::redact_url_for_log(&endpoint_now)
                        );
                        post_url = endpoint_now;
                        continue;
                    }
                }
            }

            return Err(McpError::Transport(format!(
                "POST failed: {} - {}",
                status, body
            )));
        }

        trace!("MCP SSE POST ok (attempt={})", attempt);

        trace!(
            "Sent message via POST to {}",
            Self::redact_url_for_log(&post_url)
        );
        Ok(())
    }

    async fn receive(&self) -> Result<Option<String>> {
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        let mut rx = self.message_rx.lock().await;
        match tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(message)) => {
                // Don't log raw message bodies at debug/info: they may contain tool args/secrets.
                trace!("Received SSE message (bytes={})", message.len());
                Ok(Some(message))
            }
            Ok(None) => {
                // Channel closed
                warn!("SSE message channel closed");
                Err(McpError::Disconnected)
            }
            Err(_) => {
                // Timeout, no message available
                Ok(None)
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use async_trait::async_trait;
    use tokio::sync::Mutex;
    use tokio::time::Duration;

    fn create_test_config() -> SseConfig {
        SseConfig {
            url: "http://localhost:8080/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 5000,
        }
    }

    #[test]
    fn test_sse_transport_new() {
        let config = create_test_config();
        let transport = SseTransport::new(config);
        assert!(!transport.is_connected());
        assert!(transport.sse_handle.is_none());
    }

    #[test]
    fn test_sse_build_headers_empty() {
        let config = create_test_config();
        let transport = SseTransport::new(config);

        let headers = transport.build_headers().unwrap();
        assert!(headers.contains_key(reqwest::header::ACCEPT));
        assert_eq!(
            headers.get(reqwest::header::ACCEPT).unwrap(),
            "text/event-stream"
        );
    }

    #[test]
    fn test_sse_build_headers_with_custom() {
        let config = SseConfig {
            url: "http://localhost:8080/sse".to_string(),
            headers: vec![HeaderConfig {
                name: "Authorization".to_string(),
                value: "Bearer token123".to_string(),
                value_encrypted: None,
            }],
            connect_timeout_ms: 5000,
        };
        let transport = SseTransport::new(config);

        let headers = transport.build_headers().unwrap();
        assert!(headers.contains_key("authorization"));
    }

    #[test]
    fn test_sse_build_headers_invalid_name() {
        let config = SseConfig {
            url: "http://localhost:8080/sse".to_string(),
            headers: vec![HeaderConfig {
                name: "Invalid Header Name\n".to_string(), // Invalid
                value: "test".to_string(),
                value_encrypted: None,
            }],
            connect_timeout_ms: 5000,
        };
        let transport = SseTransport::new(config);

        let result = transport.build_headers();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sse_send_disconnected() {
        let config = create_test_config();
        let transport = SseTransport::new(config);

        // Try to send without connecting
        let result = transport.send("test".to_string()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[tokio::test]
    async fn test_sse_receive_disconnected() {
        let config = create_test_config();
        let transport = SseTransport::new(config);

        // Try to receive without connecting
        let result = transport.receive().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::Disconnected => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[tokio::test]
    async fn test_sse_disconnect() {
        let config = create_test_config();
        let mut transport = SseTransport::new(config);

        // Even without actual connection, disconnect should work
        let result = transport.disconnect().await;
        assert!(result.is_ok());
        assert!(!transport.is_connected());
        assert!(transport.sse_handle.is_none());
    }

    #[test]
    fn test_sse_is_connected() {
        let config = create_test_config();
        let transport = SseTransport::new(config);

        assert!(!transport.is_connected());

        // Manually set connected flag
        transport.connected.store(true, Ordering::SeqCst);
        assert!(transport.is_connected());
    }

    #[tokio::test]
    async fn test_sse_connect_invalid_url() {
        let config = SseConfig {
            url: "http://invalid-host-12345:99999/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 1000,
        };

        let mut transport = SseTransport::new(config);
        let result = transport.connect().await;
        // Should fail with connection error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sse_receive_timeout() {
        let config = create_test_config();
        let transport = SseTransport::new(config);

        // Set connected flag manually to test receive without actual SSE stream
        transport.connected.store(true, Ordering::SeqCst);

        let result = transport.receive().await;
        // Should timeout and return Ok(None)
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_header_config() {
        let header = HeaderConfig {
            name: "Content-Type".to_string(),
            value: "application/json".to_string(),
            value_encrypted: None,
        };
        assert_eq!(header.name, "Content-Type");
        assert_eq!(header.value, "application/json");
    }

    #[test]
    fn test_sse_config_default_timeout() {
        let config = SseConfig {
            url: "http://localhost:8080/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 10000, // default
        };
        assert_eq!(config.connect_timeout_ms, 10000);
    }

    #[test]
    fn test_sse_config_custom_timeout() {
        let config = SseConfig {
            url: "http://localhost:8080/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 5000,
        };
        assert_eq!(config.connect_timeout_ms, 5000);
    }

    struct MockStep {
        expected_url: String,
        status: reqwest::StatusCode,
        body: String,
        set_endpoint_url: Option<String>,
    }

    struct MockPostClient {
        steps: Vec<MockStep>,
        step_idx: AtomicUsize,
        seen_urls: Arc<Mutex<Vec<String>>>,
        endpoint_url: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl PostClient for MockPostClient {
        async fn post_json(
            &self,
            url: String,
            _headers: HeaderMap,
            _body: String,
        ) -> Result<PostResponse> {
            self.seen_urls.lock().await.push(url.clone());

            let idx = self.step_idx.fetch_add(1, AtomicOrdering::SeqCst);
            let step = self.steps.get(idx).unwrap_or_else(|| {
                panic!("Unexpected POST call #{idx} to {url} (no mock step configured)")
            });

            assert_eq!(url, step.expected_url, "Unexpected POST URL at step {}", idx);

            if let Some(endpoint) = &step.set_endpoint_url {
                let mut guard = self.endpoint_url.lock().await;
                *guard = Some(endpoint.clone());
            }

            Ok(PostResponse {
                status: step.status,
                body: step.body.clone(),
            })
        }
    }

    #[tokio::test]
    async fn test_sse_send_retries_when_endpoint_set_after_first_404() {
        let config = SseConfig {
            url: "http://example.test/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 1000,
        };
        let mut transport = SseTransport::new_with_client(config, Client::new());
        transport.connected.store(true, Ordering::SeqCst);

        // Ensure the endpoint grace wait does not add a large delay in unit tests.
        let endpoint_notify = transport.endpoint_notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            endpoint_notify.notify_waiters();
        });

        let seen_urls = Arc::new(Mutex::new(Vec::new()));
        let fallback_url = "http://example.test/message".to_string();
        let endpoint_url = "http://example.test/messages/".to_string();

        transport.post_client = Arc::new(MockPostClient {
            steps: vec![
                MockStep {
                    expected_url: fallback_url.clone(),
                    status: reqwest::StatusCode::NOT_FOUND,
                    body: "Not Found".to_string(),
                    set_endpoint_url: Some(endpoint_url.clone()),
                },
                MockStep {
                    expected_url: endpoint_url.clone(),
                    status: reqwest::StatusCode::OK,
                    body: String::new(),
                    set_endpoint_url: None,
                },
            ],
            step_idx: AtomicUsize::new(0),
            seen_urls: seen_urls.clone(),
            endpoint_url: transport.endpoint_url.clone(),
        });

        transport.send("{}".to_string()).await.unwrap();

        assert_eq!(
            *seen_urls.lock().await,
            vec![fallback_url, endpoint_url]
        );
    }

    #[tokio::test]
    async fn test_sse_fallback_posts_to_same_url_for_mcp_path() {
        let config = SseConfig {
            url: "http://example.test/mcp".to_string(),
            headers: vec![],
            connect_timeout_ms: 1000,
        };
        let mut transport = SseTransport::new_with_client(config, Client::new());
        transport.connected.store(true, Ordering::SeqCst);

        // Ensure the endpoint grace wait does not add a large delay in unit tests.
        let endpoint_notify = transport.endpoint_notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            endpoint_notify.notify_waiters();
        });

        let seen_urls = Arc::new(Mutex::new(Vec::new()));
        let mcp_url = "http://example.test/mcp".to_string();

        transport.post_client = Arc::new(MockPostClient {
            steps: vec![MockStep {
                expected_url: mcp_url.clone(),
                status: reqwest::StatusCode::OK,
                body: String::new(),
                set_endpoint_url: None,
            }],
            step_idx: AtomicUsize::new(0),
            seen_urls: seen_urls.clone(),
            endpoint_url: transport.endpoint_url.clone(),
        });

        transport.send("{}".to_string()).await.unwrap();

        assert_eq!(*seen_urls.lock().await, vec![mcp_url]);
    }
}
