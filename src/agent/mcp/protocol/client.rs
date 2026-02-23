use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, warn};

use crate::error::{McpError, Result};
use crate::protocol::models::*;
use crate::types::{McpCallResult, McpTool};

/// Transport trait for MCP communication
#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send(&self, message: String) -> Result<()>;
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
    notification_rx: Arc<RwLock<mpsc::Receiver<JsonRpcNotification>>>,
}

impl McpProtocolClient {
    pub fn new(transport: Box<dyn McpTransport>) -> Self {
        let (notification_tx, notification_rx) = mpsc::channel(100);
        Self {
            transport: Arc::new(RwLock::new(transport)),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(std::collections::HashMap::new())),
            message_handler: None,
            notification_tx,
            notification_rx: Arc::new(RwLock::new(notification_rx)),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let mut transport = self.transport.write().await;
        transport.connect().await?;
        drop(transport);

        // Start message handler
        self.start_message_handler();

        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(handler) = self.message_handler.take() {
            handler.abort();
        }

        let mut transport = self.transport.write().await;
        transport.disconnect().await
    }

    fn start_message_handler(&mut self) {
        let transport = self.transport.clone();
        let pending_requests = self.pending_requests.clone();
        let notification_tx = self.notification_tx.clone();

        let handler = tokio::spawn(async move {
            loop {
                let transport = transport.read().await;
                if !transport.is_connected() {
                    break;
                }

                match transport.receive().await {
                    Ok(Some(message)) => {
                        debug!("Received message: {}", message);
                        if let Err(e) = Self::handle_message(
                            &message,
                            &pending_requests,
                            &notification_tx,
                        )
                        .await
                        {
                            warn!("Failed to handle message: {}", e);
                        }
                    }
                    Ok(None) => {
                        // No message available, continue
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        error!("Transport error: {}", e);
                        break;
                    }
                }
            }
        });

        self.message_handler = Some(handler);
    }

    async fn handle_message(
        message: &str,
        pending_requests: &RwLock<std::collections::HashMap<u64, PendingRequest>>,
        notification_tx: &mpsc::Sender<JsonRpcNotification>,
    ) -> Result<()> {
        // Try to parse as response
        if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(message) {
            let mut pending = pending_requests.write().await;
            if let Some(request) = pending.remove(&response.id) {
                let _ = request.sender.send(Ok(response));
            }
            return Ok(());
        }

        // Try to parse as notification
        if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(message) {
            let _ = notification_tx.send(notification).await;
            return Ok(());
        }

        Err(McpError::Protocol("Unknown message type".to_string()))
    }

    async fn send_request(&self,
        method: &str,
        params: Option<Value>,
        timeout_ms: u64,
    ) -> Result<JsonRpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let request = JsonRpcRequest::new(id, method, params);
        let request_json = serde_json::to_string(&request)?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(id, PendingRequest { sender: tx });
        }

        let transport = self.transport.read().await;
        transport.send(request_json).await?;
        drop(transport);

        match tokio::time::timeout(
            tokio::time::Duration::from_millis(timeout_ms),
            rx,
        )
        .await
        {
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
            response.result.ok_or_else(|| McpError::Protocol("Missing result".to_string()))?
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
        let response = self
            .send_request("tools/list", None, timeout_ms)
            .await?;

        let result: McpToolListResult = serde_json::from_value(
            response.result.ok_or_else(|| McpError::Protocol("Missing result".to_string()))?
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
            response.result.ok_or_else(|| McpError::Protocol("Missing result".to_string()))?
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

    pub async fn try_receive_notification(&self) -> Option<JsonRpcNotification> {
        let mut rx = self.notification_rx.write().await;
        rx.try_recv().ok()
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

    // Mock transport for testing
    struct MockTransport {
        connected: bool,
        messages_sent: Arc<RwLock<Vec<String>>>,
        messages_to_receive: Arc<RwLock<Vec<String>>>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                connected: false,
                messages_sent: Arc::new(RwLock::new(Vec::new())),
                messages_to_receive: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn with_response(message: String) -> Self {
            let messages = Arc::new(RwLock::new(vec![message]));
            Self {
                connected: false,
                messages_sent: Arc::new(RwLock::new(Vec::new())),
                messages_to_receive: messages,
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

        async fn receive(&self) -> Result<Option<String>> {
            let mut messages = self.messages_to_receive.write().await;
            if messages.is_empty() {
                Ok(None)
            } else {
                Ok(Some(messages.remove(0)))
            }
        }

        fn is_connected(&self) -> bool {
            self.connected
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
        let request = JsonRpcRequest::new(1, "test/method", Some(serde_json::json!({"key": "value"})));
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

    #[test]
    fn test_pending_request() {
        let (tx, rx) = oneshot::channel();
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
}
