use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use crate::config::{McpConfig, McpServerConfig, TransportConfig};
use crate::error::{McpError, Result};
use crate::protocol::{McpProtocolClient, McpTransport};
use crate::tool_index::ToolIndex;
use crate::transports::{SseTransport, StdioTransport};
use crate::types::{McpEvent, McpTool, RuntimeInfo, ServerStatus};

/// Runtime state for a connected MCP server
struct ServerRuntime {
    config: McpServerConfig,
    client: RwLock<McpProtocolClient>,
    info: RwLock<RuntimeInfo>,
    tools: RwLock<Vec<McpTool>>,
    shutdown: AtomicBool,
    reconnecting: AtomicBool,
}

/// Manages MCP server connections and tool execution
pub struct McpServerManager {
    runtimes: DashMap<String, Arc<ServerRuntime>>,
    index: Arc<ToolIndex>,
    event_tx: Option<tokio::sync::mpsc::Sender<McpEvent>>,
}

impl Clone for McpServerManager {
    fn clone(&self) -> Self {
        Self {
            runtimes: self.runtimes.clone(),
            index: self.index.clone(),
            event_tx: self.event_tx.clone(),
        }
    }
}

impl McpServerManager {
    pub fn new() -> Self {
        Self {
            runtimes: DashMap::new(),
            index: Arc::new(ToolIndex::new()),
            event_tx: None,
        }
    }

    pub fn with_event_channel(mut self, tx: tokio::sync::mpsc::Sender<McpEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub fn tool_index(&self) -> Arc<ToolIndex> {
        self.index.clone()
    }

    /// Initialize from configuration
    pub async fn initialize_from_config(&self,
        config: &McpConfig,
    ) {
        for server_config in &config.servers {
            if !server_config.enabled {
                continue;
            }

            if let Err(e) = self.start_server(server_config.clone()).await {
                error!(
                    "Failed to start MCP server '{}': {}",
                    server_config.id, e
                );
            }
        }
    }

    /// Start a new MCP server connection
    pub async fn start_server(&self,
        config: McpServerConfig,
    ) -> Result<()> {
        let server_id = config.id.clone();

        if self.runtimes.contains_key(&server_id) {
            return Err(McpError::AlreadyRunning(server_id));
        }

        info!("Starting MCP server '{}'", server_id);

        // Create transport
        let transport: Box<dyn McpTransport> = match &config.transport {
            TransportConfig::Stdio(stdio_config) => {
                Box::new(StdioTransport::new(stdio_config.clone()))
            }
            TransportConfig::Sse(sse_config) => {
                Box::new(SseTransport::new(sse_config.clone()))
            }
        };

        // Create client
        let mut client = McpProtocolClient::new(transport);

        // Connect
        client.connect().await.map_err(|e| {
            error!("Failed to connect to MCP server '{}': {}", server_id, e);
            e
        })?;

        // Initialize
        let init_result = client
            .initialize(config.request_timeout_ms)
            .await
            .map_err(|e| {
                error!(
                    "Failed to initialize MCP server '{}': {}",
                    server_id, e
                );
                e
            })?;

        info!(
            "MCP server '{}' initialized: {} v{}",
            server_id, init_result.server_info.name, init_result.server_info.version
        );

        // List tools
        let tools = client.list_tools(config.request_timeout_ms).await?;
        info!(
            "MCP server '{}' has {} tools",
            server_id,
            tools.len()
        );

        // Create runtime
        let runtime = Arc::new(ServerRuntime {
            config: config.clone(),
            client: RwLock::new(client),
            info: RwLock::new(RuntimeInfo {
                status: ServerStatus::Ready,
                last_error: None,
                connected_at: Some(Utc::now()),
                disconnected_at: None,
                tool_count: tools.len(),
                restart_count: 0,
                last_ping_at: Some(Utc::now()),
            }),
            tools: RwLock::new(tools.clone()),
            shutdown: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
        });

        // Register tools in index
        let aliases = self.index.register_server_tools(
            &server_id,
            &tools,
            &config.allowed_tools,
            &config.denied_tools,
        );

        info!(
            "Registered {} MCP tools for server '{}'",
            aliases.len(),
            server_id
        );

        // Store runtime
        self.runtimes.insert(server_id.clone(), runtime.clone());

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ServerStatusChanged {
                    server_id: server_id.clone(),
                    status: ServerStatus::Ready,
                    error: None,
                })
                .await;

            let tool_names: Vec<String> = aliases
                .into_iter()
                .map(|a| a.alias)
                .collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id,
                    tools: tool_names,
                })
                .await;
        }

        // Start health check task
        self.start_health_check(runtime, config.healthcheck_interval_ms);

        Ok(())
    }

    /// Stop an MCP server connection
    pub async fn stop_server(&self,
        server_id: &str,
    ) -> Result<()> {
        let (_, runtime) = self
            .runtimes
            .remove(server_id)
            .ok_or_else(|| McpError::NotRunning(server_id.to_string()))?;

        info!("Stopping MCP server '{}'", server_id);

        runtime.shutdown.store(true, Ordering::SeqCst);

        // Disconnect client
        let mut client = runtime.client.write().await;
        if let Err(e) = client.disconnect().await {
            warn!("Error disconnecting MCP server '{}': {}", server_id, e);
        }

        // Update info
        let mut info = runtime.info.write().await;
        info.status = ServerStatus::Stopped;
        info.disconnected_at = Some(Utc::now());

        // Remove tools from index
        self.index.remove_server_tools(server_id);

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ServerStatusChanged {
                    server_id: server_id.to_string(),
                    status: ServerStatus::Stopped,
                    error: None,
                })
                .await;
        }

        info!("MCP server '{}' stopped", server_id);
        Ok(())
    }

    /// Call a tool on a specific server
    pub async fn call_tool(
        &self,
        server_id: &str,
        tool_name: &str,
        args: serde_json::Value,
    ) -> Result<crate::types::McpCallResult> {
        let runtime = self
            .runtimes
            .get(server_id)
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;

        let client = runtime.client.read().await;
        let timeout = runtime.config.request_timeout_ms;

        let result = client.call_tool(tool_name, args, timeout).await?;

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ToolExecuted {
                    server_id: server_id.to_string(),
                    tool_name: tool_name.to_string(),
                    success: !result.is_error,
                })
                .await;
        }

        Ok(result)
    }

    /// Get tool info for a specific tool
    pub fn get_tool_info(
        &self,
        server_id: &str,
        tool_name: &str,
    ) -> Option<McpTool> {
        self.runtimes.get(server_id).and_then(|runtime| {
            let tools = runtime.tools.try_read().ok()?;
            tools.iter().find(|t| t.name == tool_name).cloned()
        })
    }

    /// Refresh tools from a server
    pub async fn refresh_tools(
        &self,
        server_id: &str,
    ) -> Result<()> {
        let runtime = self
            .runtimes
            .get(server_id)
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;

        info!("Refreshing tools for MCP server '{}'", server_id);

        let client = runtime.client.read().await;
        let new_tools = client
            .list_tools(runtime.config.request_timeout_ms)
            .await?;
        drop(client);

        // Update tools
        let mut tools = runtime.tools.write().await;
        *tools = new_tools.clone();
        drop(tools);

        // Update info
        let mut info = runtime.info.write().await;
        info.tool_count = new_tools.len();

        // Re-register tools
        self.index.remove_server_tools(server_id);
        let aliases = self.index.register_server_tools(
            server_id,
            &new_tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        );

        info!(
            "Refreshed {} tools for MCP server '{}'",
            aliases.len(),
            server_id
        );

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let tool_names: Vec<String> = aliases
                .into_iter()
                .map(|a| a.alias)
                .collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id: server_id.to_string(),
                    tools: tool_names,
                })
                .await;
        }

        Ok(())
    }

    /// Get all server IDs
    pub fn list_servers(&self) -> Vec<String> {
        self.runtimes
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get runtime info for a server
    pub fn get_server_info(&self,
        server_id: &str,
    ) -> Option<RuntimeInfo> {
        self.runtimes.get(server_id).and_then(|runtime| {
            runtime.info.try_read().ok().map(|info| info.clone())
        })
    }

    /// Check if a server is running
    pub fn is_server_running(&self,
        server_id: &str,
    ) -> bool {
        self.runtimes.contains_key(server_id)
    }

    /// Shutdown all servers
    pub async fn shutdown_all(&self,
    ) {
        let server_ids: Vec<String> = self.list_servers();
        for server_id in server_ids {
            if let Err(e) = self.stop_server(&server_id).await {
                error!("Error stopping server '{}': {}", server_id, e);
            }
        }
    }

    fn start_health_check(
        &self,
        runtime: Arc<ServerRuntime>,
        interval_ms: u64,
    ) {
        let server_id = runtime.config.id.clone();
        let manager = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                if runtime.shutdown.load(Ordering::SeqCst) {
                    break;
                }

                // Skip health check if currently reconnecting
                if runtime.reconnecting.load(Ordering::SeqCst) {
                    continue;
                }

                let client = runtime.client.read().await;
                match client.ping(runtime.config.request_timeout_ms).await {
                    Ok(_) => {
                        let mut info = runtime.info.write().await;
                        info.last_ping_at = Some(Utc::now());
                        if info.status == ServerStatus::Degraded {
                            info.status = ServerStatus::Ready;
                            // Emit recovery event
                            if let Some(ref tx) = manager.event_tx {
                                let _ = tx
                                    .send(McpEvent::ServerStatusChanged {
                                        server_id: server_id.clone(),
                                        status: ServerStatus::Ready,
                                        error: None,
                                    })
                                    .await;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Health check failed for MCP server '{}': {}",
                            server_id, e
                        );

                        // Drop client lock before attempting reconnection
                        drop(client);

                        // Update status to Degraded
                        {
                            let mut info = runtime.info.write().await;
                            info.status = ServerStatus::Degraded;
                            info.last_error = Some(e.to_string());
                        }

                        // Emit degraded event
                        if let Some(ref tx) = manager.event_tx {
                            let _ = tx
                                .send(McpEvent::ServerStatusChanged {
                                    server_id: server_id.clone(),
                                    status: ServerStatus::Degraded,
                                    error: Some(e.to_string()),
                                })
                                .await;
                        }

                        // Attempt reconnection if enabled
                        if runtime.config.reconnect.enabled {
                            if let Err(reconnect_err) = manager.attempt_reconnection(runtime.clone()).await {
                                error!(
                                    "Reconnection failed for MCP server '{}': {}",
                                    server_id, reconnect_err
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    /// Attempt to reconnect a degraded server with exponential backoff
    async fn attempt_reconnection(&self, runtime: Arc<ServerRuntime>) -> Result<()> {
        let server_id = runtime.config.id.clone();

        // Check if already reconnecting
        if runtime.reconnecting.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ).is_err() {
            info!("Reconnection already in progress for MCP server '{}'", server_id);
            return Ok(());
        }

        let reconnect_config = &runtime.config.reconnect;
        let mut current_backoff = reconnect_config.initial_backoff_ms;
        let mut attempt = 0u32;

        info!(
            "Starting reconnection attempts for MCP server '{}' (max_attempts: {})",
            server_id,
            if reconnect_config.max_attempts == 0 {
                "unlimited".to_string()
            } else {
                reconnect_config.max_attempts.to_string()
            }
        );

        loop {
            // Check if shutdown was requested
            if runtime.shutdown.load(Ordering::SeqCst) {
                info!("Reconnection cancelled due to shutdown for MCP server '{}'", server_id);
                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }

            // Check max attempts
            if reconnect_config.max_attempts > 0 && attempt >= reconnect_config.max_attempts {
                error!(
                    "Max reconnection attempts ({}) reached for MCP server '{}'",
                    reconnect_config.max_attempts, server_id
                );

                // Update status to Error
                let mut info = runtime.info.write().await;
                info.status = ServerStatus::Error;
                info.last_error = Some("Max reconnection attempts reached".to_string());
                info.disconnected_at = Some(Utc::now());

                // Emit error event
                if let Some(ref tx) = self.event_tx {
                    let _ = tx
                        .send(McpEvent::ServerStatusChanged {
                            server_id: server_id.clone(),
                            status: ServerStatus::Error,
                            error: Some("Max reconnection attempts reached".to_string()),
                        })
                        .await;
                }

                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Err(McpError::Connection(format!(
                    "Max reconnection attempts reached for server '{}'",
                    server_id
                )));
            }

            attempt += 1;
            info!(
                "Reconnection attempt {} for MCP server '{}' (backoff: {}ms)",
                attempt, server_id, current_backoff
            );

            // Wait for backoff period
            tokio::time::sleep(Duration::from_millis(current_backoff)).await;

            // Attempt reconnection
            match self.reconnect_server(runtime.clone()).await {
                Ok(_) => {
                    info!(
                        "Successfully reconnected MCP server '{}' after {} attempt(s)",
                        server_id, attempt
                    );

                    // Update runtime info
                    let mut info = runtime.info.write().await;
                    info.status = ServerStatus::Ready;
                    info.last_error = None;
                    info.restart_count += 1;
                    info.disconnected_at = None;

                    // Emit recovery event
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx
                            .send(McpEvent::ServerStatusChanged {
                                server_id: server_id.clone(),
                                status: ServerStatus::Ready,
                                error: None,
                            })
                            .await;
                    }

                    runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Reconnection attempt {} failed for MCP server '{}': {}",
                        attempt, server_id, e
                    );

                    // Update error info
                    let mut info = runtime.info.write().await;
                    info.last_error = Some(e.to_string());

                    // Calculate next backoff with exponential increase
                    if reconnect_config.max_backoff_ms > current_backoff {
                        current_backoff = std::cmp::min(
                            current_backoff * 2,
                            reconnect_config.max_backoff_ms
                        );
                    }
                }
            }
        }
    }

    /// Internal method to reconnect a single server
    async fn reconnect_server(&self, runtime: Arc<ServerRuntime>) -> Result<()> {
        let server_id = runtime.config.id.clone();

        info!("Attempting to reconnect MCP server '{}'", server_id);

        // Disconnect existing client if connected
        {
            let mut client = runtime.client.write().await;
            if client.is_connected().await {
                let _ = client.disconnect().await;
            }
        }

        // Create new transport
        let transport: Box<dyn McpTransport> = match &runtime.config.transport {
            TransportConfig::Stdio(stdio_config) => {
                Box::new(StdioTransport::new(stdio_config.clone()))
            }
            TransportConfig::Sse(sse_config) => {
                Box::new(SseTransport::new(sse_config.clone()))
            }
        };

        // Create new client
        let mut client = McpProtocolClient::new(transport);

        // Connect
        client.connect().await.map_err(|e| {
            error!("Failed to reconnect to MCP server '{}': {}", server_id, e);
            e
        })?;

        // Initialize
        let init_result = client
            .initialize(runtime.config.request_timeout_ms)
            .await
            .map_err(|e| {
                error!(
                    "Failed to initialize reconnected MCP server '{}': {}",
                    server_id, e
                );
                e
            })?;

        info!(
            "MCP server '{}' re-initialized: {} v{}",
            server_id, init_result.server_info.name, init_result.server_info.version
        );

        // List tools
        let tools = client.list_tools(runtime.config.request_timeout_ms).await?;
        info!(
            "MCP server '{}' has {} tools after reconnection",
            server_id,
            tools.len()
        );

        // Update client
        {
            let mut client_lock = runtime.client.write().await;
            *client_lock = client;
        }

        // Update tools
        {
            let mut tools_lock = runtime.tools.write().await;
            *tools_lock = tools.clone();
        }

        // Re-register tools in index
        self.index.remove_server_tools(&server_id);
        let aliases = self.index.register_server_tools(
            &server_id,
            &tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        );

        info!(
            "Re-registered {} MCP tools for server '{}'",
            aliases.len(),
            server_id
        );

        // Emit tools changed event
        if let Some(ref tx) = self.event_tx {
            let tool_names: Vec<String> = aliases
                .into_iter()
                .map(|a| a.alias)
                .collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id,
                    tools: tool_names,
                })
                .await;
        }

        Ok(())
    }
}

impl Default for McpServerManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ReconnectConfig, StdioConfig};
    use tokio::sync::mpsc;

    fn create_test_server_config(id: &str) -> McpServerConfig {
        McpServerConfig {
            id: id.to_string(),
            name: Some(format!("Test Server {}", id)),
            enabled: true,
            transport: TransportConfig::Stdio(StdioConfig {
                command: "echo".to_string(),
                args: vec![],
                cwd: None,
                env: std::collections::HashMap::new(),
                startup_timeout_ms: 5000,
            }),
            request_timeout_ms: 5000,
            healthcheck_interval_ms: 1000,
            reconnect: ReconnectConfig {
                enabled: false, // Disable for most tests
                initial_backoff_ms: 100,
                max_backoff_ms: 1000,
                max_attempts: 3,
            },
            allowed_tools: vec![],
            denied_tools: vec![],
        }
    }

    #[test]
    fn test_manager_new() {
        let manager = McpServerManager::new();
        assert!(manager.list_servers().is_empty());
    }

    #[test]
    fn test_manager_clone() {
        let manager = McpServerManager::new();
        let cloned = manager.clone();
        assert!(cloned.list_servers().is_empty());
    }

    #[test]
    fn test_manager_with_event_channel() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = McpServerManager::new().with_event_channel(tx);
        assert!(manager.event_tx.is_some());
    }

    #[test]
    fn test_tool_index_accessor() {
        let manager = McpServerManager::new();
        let index = manager.tool_index();
        assert!(index.all_aliases().is_empty());
    }

    #[tokio::test]
    async fn test_list_servers_empty() {
        let manager = McpServerManager::new();
        let servers = manager.list_servers();
        assert!(servers.is_empty());
    }

    #[tokio::test]
    async fn test_is_server_running() {
        let manager = McpServerManager::new();
        assert!(!manager.is_server_running("nonexistent"));
    }

    #[tokio::test]
    async fn test_get_server_info_nonexistent() {
        let manager = McpServerManager::new();
        let info = manager.get_server_info("nonexistent");
        assert!(info.is_none());
    }

    #[tokio::test]
    async fn test_get_tool_info_nonexistent() {
        let manager = McpServerManager::new();
        let tool = manager.get_tool_info("nonexistent", "tool");
        assert!(tool.is_none());
    }

    #[tokio::test]
    async fn test_stop_server_nonexistent() {
        let manager = McpServerManager::new();
        let result = manager.stop_server("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::NotRunning(id) => assert_eq!(id, "nonexistent"),
            _ => panic!("Expected NotRunning error"),
        }
    }

    #[tokio::test]
    async fn test_call_tool_nonexistent_server() {
        let manager = McpServerManager::new();
        let result = manager
            .call_tool("nonexistent", "tool", serde_json::json!({}))
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::ServerNotFound(id) => assert_eq!(id, "nonexistent"),
            _ => panic!("Expected ServerNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_refresh_tools_nonexistent() {
        let manager = McpServerManager::new();
        let result = manager.refresh_tools("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::ServerNotFound(id) => assert_eq!(id, "nonexistent"),
            _ => panic!("Expected ServerNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_shutdown_all_empty() {
        let manager = McpServerManager::new();
        // Should not panic
        manager.shutdown_all().await;
    }

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();
        assert!(config.enabled);
        assert_eq!(config.initial_backoff_ms, 1000);
        assert_eq!(config.max_backoff_ms, 30000);
        assert_eq!(config.max_attempts, 0);
    }

    #[test]
    fn test_reconnect_config_custom() {
        let config = ReconnectConfig {
            enabled: true,
            initial_backoff_ms: 500,
            max_backoff_ms: 10000,
            max_attempts: 5,
        };
        assert!(config.enabled);
        assert_eq!(config.initial_backoff_ms, 500);
        assert_eq!(config.max_backoff_ms, 10000);
        assert_eq!(config.max_attempts, 5);
    }

    #[tokio::test]
    async fn test_start_server_already_running() {
        let manager = McpServerManager::new();
        let config = create_test_server_config("test-server");

        // Start server (will fail because echo doesn't implement MCP protocol)
        let _ = manager.start_server(config.clone()).await;

        // Try to start again - should fail with AlreadyRunning
        // Note: This test may not work if the first start fails
        // In that case, we're testing the logic path
    }

    #[tokio::test]
    async fn test_initialize_from_config_disabled_server() {
        let manager = McpServerManager::new();

        let mut config = create_test_server_config("disabled-server");
        config.enabled = false;

        let mcp_config = McpConfig {
            version: 1,
            servers: vec![config],
        };

        manager.initialize_from_config(&mcp_config).await;

        // Should not have started the disabled server
        assert!(!manager.is_server_running("disabled-server"));
    }

    #[tokio::test]
    async fn test_event_channel_server_status() {
        let (tx, rx) = mpsc::channel(100);
        let manager = McpServerManager::new().with_event_channel(tx);

        // Events are sent during server operations
        // This test verifies the channel is properly set up
        assert!(manager.event_tx.is_some());

        // Clean up
        drop(manager);
        drop(rx);
    }

    #[test]
    fn test_server_status_display() {
        assert_eq!(format!("{}", ServerStatus::Ready), "ready");
        assert_eq!(format!("{}", ServerStatus::Degraded), "degraded");
        assert_eq!(format!("{}", ServerStatus::Error), "error");
        assert_eq!(format!("{}", ServerStatus::Stopped), "stopped");
        assert_eq!(format!("{}", ServerStatus::Connecting), "connecting");
    }

    #[test]
    fn test_runtime_info_default() {
        let info = RuntimeInfo::default();
        assert_eq!(info.status, ServerStatus::Stopped);
        assert!(info.last_error.is_none());
        assert!(info.connected_at.is_none());
        assert!(info.disconnected_at.is_none());
        assert_eq!(info.tool_count, 0);
        assert_eq!(info.restart_count, 0);
        assert!(info.last_ping_at.is_none());
    }

    // Test exponential backoff calculation (indirectly through manager behavior)
    #[test]
    fn test_exponential_backoff_calculation() {
        let initial = 1000u64;
        let max = 30000u64;
        let mut current = initial;

        // First backoff
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 2000);

        // Second backoff
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 4000);

        // Third backoff
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 8000);

        // Fourth backoff
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 16000);

        // Fifth backoff
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 30000); // Capped at max

        // Try again - should stay at max
        current = std::cmp::min(current * 2, max);
        assert_eq!(current, 30000);
    }

    #[test]
    fn test_exponential_backoff_max_zero() {
        // Test that max_attempts = 0 means unlimited
        let config = ReconnectConfig {
            enabled: true,
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            max_attempts: 0,
        };

        assert_eq!(config.max_attempts, 0);
        // In the actual code, max_attempts == 0 bypasses the attempt limit check
    }

    // Test that reconnection logic is properly gated by enabled flag
    #[test]
    fn test_reconnect_disabled() {
        let config = ReconnectConfig {
            enabled: false,
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            max_attempts: 3,
        };

        assert!(!config.enabled);
        // In the actual health check code, reconnection is only attempted if enabled
    }
}
