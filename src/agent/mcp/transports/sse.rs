use async_trait::async_trait;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::{Client, header::HeaderMap};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::config::{HeaderConfig, SseConfig};
use crate::error::{McpError, Result};
use crate::protocol::client::McpTransport;

pub struct SseTransport {
    config: SseConfig,
    client: Client,
    connected: AtomicBool,
    message_tx: mpsc::Sender<String>,
    message_rx: Mutex<mpsc::Receiver<String>>,
    sse_handle: Option<tokio::task::JoinHandle<()>>,
    endpoint_url: Mutex<Option<String>>,
}

impl SseTransport {
    pub fn new(config: SseConfig) -> Self {
        let (message_tx, message_rx) = mpsc::channel(100);
        Self {
            config,
            client: Client::new(),
            connected: AtomicBool::new(false),
            message_tx,
            message_rx: Mutex::new(message_rx),
            sse_handle: None,
            endpoint_url: Mutex::new(None),
        }
    }

    fn build_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            "text/event-stream".parse().unwrap(),
        );

        for HeaderConfig { name, value } in &self.config.headers {
            let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| McpError::InvalidConfig(format!("Invalid header name: {}", e)))?;
            let header_value = value.parse().map_err(|e| {
                McpError::InvalidConfig(format!("Invalid header value: {}", e))
            })?;
            headers.insert(header_name, header_value);
        }

        Ok(headers)
    }
}

#[async_trait]
impl McpTransport for SseTransport {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to MCP SSE endpoint: {}", self.config.url);

        let headers = self.build_headers()?;
        let response = self
            .client
            .get(&self.config.url)
            .headers(headers)
            .timeout(tokio::time::Duration::from_millis(self.config.connect_timeout_ms))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(McpError::Connection(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        // Start SSE event handler
        let message_tx = self.message_tx.clone();
        let url = self.config.url.clone();

        let handle = tokio::spawn(async move {
            let mut stream = response.bytes_stream().eventsource();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        debug!("SSE event: {}", event.event);
                        if event.event == "endpoint" {
                            // Store the endpoint URL for POST requests
                            debug!("Got endpoint: {}", event.data);
                        } else if event.event == "message" || event.event.is_empty() {
                            if message_tx.send(event.data).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("SSE stream error: {}", e);
                        break;
                    }
                }
            }
            warn!("SSE stream ended for {}", url);
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

        // For SSE transport, we need to POST to the endpoint
        let endpoint = self.endpoint_url.lock().await.clone();
        let post_url = endpoint.unwrap_or_else(|| {
            // If no endpoint was provided via SSE, use the base URL + "/message"
            format!("{}/message", self.config.url.trim_end_matches("/sse"))
        });

        let headers = self.build_headers()?;

        let response = self
            .client
            .post(&post_url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(message)
            .timeout(tokio::time::Duration::from_secs(60))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(McpError::Transport(format!(
                "POST failed: {} - {}",
                status, body
            )));
        }

        debug!("Sent message via POST to {}", post_url);
        Ok(())
    }

    async fn receive(&self) -> Result<Option<String>> {
        if !self.is_connected() {
            return Err(McpError::Disconnected);
        }

        let mut rx = self.message_rx.lock().await;
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            rx.recv(),
        )
        .await
        {
            Ok(Some(message)) => {
                debug!("Received SSE message: {}", message);
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

    // Note: Testing actual SSE connection and message handling would require
    // a mock server. These tests verify the basic structure and error handling.
}
