use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::agent::mcp::config::{McpConfig, McpServerConfig, TransportConfig};
use crate::agent::mcp::error::{McpError, Result};
use crate::agent::mcp::protocol::{McpProtocolClient, McpTransport};
use crate::agent::mcp::tool_index::ToolIndex;
use crate::agent::mcp::transports::{SseTransport, StdioTransport};
use crate::agent::mcp::types::{McpEvent, McpTool, RuntimeInfo, ServerStatus};
use crate::core::Config;

mod config_sync;
mod fingerprint;
mod lifecycle;
mod reconnect;

#[cfg(test)]
mod tests;

/// Runtime state for a connected MCP server.
struct ServerRuntime {
    config: McpServerConfig,
    client: RwLock<McpProtocolClient>,
    info: RwLock<RuntimeInfo>,
    tools: RwLock<Vec<McpTool>>,
    shutdown: AtomicBool,
    reconnecting: AtomicBool,
    // Fingerprint of the global proxy settings at the time this runtime was started.
    // Used to force-restart SSE transports when proxy settings change.
    proxy_fingerprint: Option<String>,
}

/// Manages MCP server connections and tool execution.
pub struct McpServerManager {
    runtimes: DashMap<String, Arc<ServerRuntime>>,
    index: Arc<ToolIndex>,
    event_tx: Option<tokio::sync::mpsc::Sender<McpEvent>>,
    config: Option<Arc<tokio::sync::RwLock<Config>>>,
}

impl Clone for McpServerManager {
    fn clone(&self) -> Self {
        Self {
            runtimes: self.runtimes.clone(),
            index: self.index.clone(),
            event_tx: self.event_tx.clone(),
            config: self.config.clone(),
        }
    }
}

impl McpServerManager {
    pub fn new() -> Self {
        Self {
            runtimes: DashMap::new(),
            index: Arc::new(ToolIndex::new()),
            event_tx: None,
            config: None,
        }
    }

    /// Create a manager that can respect global proxy settings when connecting SSE transports.
    pub fn new_with_config(config: Arc<tokio::sync::RwLock<Config>>) -> Self {
        Self {
            runtimes: DashMap::new(),
            index: Arc::new(ToolIndex::new()),
            event_tx: None,
            config: Some(config),
        }
    }

    pub fn with_event_channel(mut self, tx: tokio::sync::mpsc::Sender<McpEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub fn tool_index(&self) -> Arc<ToolIndex> {
        self.index.clone()
    }

    /// Get all server IDs.
    pub fn list_servers(&self) -> Vec<String> {
        self.runtimes
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get runtime info for a server.
    pub fn get_server_info(&self, server_id: &str) -> Option<RuntimeInfo> {
        self.runtimes
            .get(server_id)
            .and_then(|runtime| runtime.info.try_read().ok().map(|info| info.clone()))
    }

    /// Check if a server is running.
    pub fn is_server_running(&self, server_id: &str) -> bool {
        self.runtimes.contains_key(server_id)
    }

    /// Shutdown all servers.
    pub async fn shutdown_all(&self) {
        let server_ids: Vec<String> = self.list_servers();
        for server_id in server_ids {
            if let Err(e) = self.stop_server(&server_id).await {
                error!("Error stopping server '{}': {}", server_id, e);
            }
        }
    }
}

impl Default for McpServerManager {
    fn default() -> Self {
        Self::new()
    }
}
