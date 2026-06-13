use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{error, info, warn};

use crate::config::{McpConfig, McpServerConfig, TransportConfig};
use crate::error::{McpError, Result};
use crate::protocol::{McpProtocolClient, McpTransport};
use crate::tool_index::ToolIndex;
use crate::transports::{SseTransport, StdioTransport, StreamableHttpTransport};
use crate::types::{McpEvent, McpTool, RuntimeInfo, ServerStatus};
use bamboo_llm::Config;

mod config_sync;
mod fingerprint;
mod lifecycle;
mod reconnect;

#[cfg(test)]
mod tests;

const DEFAULT_MAX_CONCURRENT_CALLS_PER_SERVER: usize = 4;
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const DEFAULT_CIRCUIT_OPEN_MS: u64 = 5_000;
/// Consecutive failures (protocol errors OR `is_error` tool results) after which
/// the server is RECYCLED — disconnected (the child process is killed) and
/// reconnected (a fresh one is spawned). This catches servers that stay alive at
/// the protocol level (so health-check pings keep succeeding) yet have a wedged
/// capability that keeps returning errors — e.g. nova when its ScreenCaptureKit /
/// replayd pipeline is stuck. A single success resets the counter, so a tool that
/// merely errors occasionally never trips it.
const DEFAULT_RECONNECT_FAILURE_THRESHOLD: u32 = 3;

#[derive(Debug, Clone, Copy)]
struct McpQosConfig {
    max_concurrent_calls: usize,
    circuit_failure_threshold: u32,
    circuit_open_ms: u64,
    reconnect_failure_threshold: u32,
}

impl Default for McpQosConfig {
    fn default() -> Self {
        Self {
            max_concurrent_calls: DEFAULT_MAX_CONCURRENT_CALLS_PER_SERVER,
            circuit_failure_threshold: DEFAULT_CIRCUIT_FAILURE_THRESHOLD,
            circuit_open_ms: DEFAULT_CIRCUIT_OPEN_MS,
            reconnect_failure_threshold: DEFAULT_RECONNECT_FAILURE_THRESHOLD,
        }
    }
}

#[derive(Debug, Default)]
struct McpQosState {
    consecutive_failures: u32,
    circuit_open_until: Option<Instant>,
}

#[derive(Debug)]
struct McpServerQos {
    config: McpQosConfig,
    permits: Arc<Semaphore>,
    state: Mutex<McpQosState>,
}

impl McpServerQos {
    fn new(config: McpQosConfig) -> Self {
        let max_permits = config.max_concurrent_calls.max(1);
        Self {
            config,
            permits: Arc::new(Semaphore::new(max_permits)),
            state: Mutex::new(McpQosState::default()),
        }
    }

    async fn check_circuit(&self, server_id: &str, tool_name: &str) -> Result<()> {
        let mut state = self.state.lock().await;

        if let Some(open_until) = state.circuit_open_until {
            let now = Instant::now();
            if now < open_until {
                let remaining = open_until.saturating_duration_since(now).as_millis();
                return Err(McpError::ToolExecution(format!(
                    "MCP QoS circuit open for server '{}' (tool '{}'), retry in ~{}ms",
                    server_id, tool_name, remaining
                )));
            }

            state.circuit_open_until = None;
            state.consecutive_failures = 0;
        }

        Ok(())
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.permits.clone().acquire_owned().await.map_err(|error| {
            McpError::ToolExecution(format!("MCP QoS permit unavailable: {error}"))
        })
    }

    async fn record_success(&self) {
        let mut state = self.state.lock().await;
        state.consecutive_failures = 0;
        state.circuit_open_until = None;
    }

    /// Record a failed call (a protocol error OR an `is_error` tool result).
    /// Returns `true` when the server has failed enough consecutive times that it
    /// should be RECYCLED (disconnected + reconnected); the caller performs the
    /// reconnect. The counter is reset on `true` so a recycle isn't re-triggered
    /// on every subsequent call — a fresh run of failures is needed first.
    async fn record_failure(&self, server_id: &str, tool_name: &str, error: &McpError) -> bool {
        let mut state = self.state.lock().await;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);

        if state.consecutive_failures >= self.config.circuit_failure_threshold {
            state.circuit_open_until =
                Some(Instant::now() + StdDuration::from_millis(self.config.circuit_open_ms));
            warn!(
                "MCP QoS opening circuit for server '{}' after {} consecutive failures (tool '{}', last_error={})",
                server_id, state.consecutive_failures, tool_name, error
            );
        }

        if state.consecutive_failures >= self.config.reconnect_failure_threshold {
            warn!(
                "MCP server '{}' hit {} consecutive failures (tool '{}', last_error={}) — recycling (disconnect + reconnect)",
                server_id, state.consecutive_failures, tool_name, error
            );
            state.consecutive_failures = 0;
            state.circuit_open_until = None;
            return true;
        }

        false
    }
}

/// Runtime state for a connected MCP server.
struct ServerRuntime {
    config: McpServerConfig,
    client: RwLock<McpProtocolClient>,
    info: RwLock<RuntimeInfo>,
    tools: RwLock<Vec<McpTool>>,
    shutdown: AtomicBool,
    reconnecting: AtomicBool,
    qos: McpServerQos,
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

    /// `(server_id, instructions)` for every currently-ready server that returned
    /// `instructions` from `initialize`. Used to inject each connected server's
    /// own usage guidance into the system prompt — so guidance appears only while
    /// the server is actually loaded. Sorted by server id for a stable prompt.
    pub fn connected_server_instructions(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .runtimes
            .iter()
            .filter_map(|entry| {
                let info = entry.value().info.try_read().ok()?;
                if info.status != ServerStatus::Ready {
                    return None;
                }
                let instructions = info.instructions.clone()?;
                Some((entry.key().clone(), instructions))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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
