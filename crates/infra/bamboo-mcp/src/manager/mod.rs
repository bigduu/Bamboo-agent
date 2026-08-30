use chrono::Utc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{error, info, warn};

use crate::config::{McpConfig, McpServerConfig, TransportConfig};
use crate::error::{McpError, Result};
use crate::protocol::{McpProtocolClient, McpTransport};
#[cfg(test)]
use crate::tool_index::ServerToolCatalog;
use crate::tool_index::{ToolIndex, MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS};
use crate::transports::{SseTransport, StdioTransport, StreamableHttpTransport};
use crate::types::{McpEvent, McpTool, RuntimeInfo, ServerStatus};
use bamboo_llm::Config;

mod config_sync;
mod fingerprint;
pub(crate) mod generation;
mod lifecycle;
mod reconnect;

use generation::{
    ExpectedPublication, GenerationAuthority, McpRuntimeGeneration, ServerPublication,
    TransportRuntime,
};
pub use generation::{McpRuntimeSnapshot, PublicationId, ResolvedMcpCall, RuntimeId};

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

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PublishProbePhase {
    BeforeFenceAndSwap,
    AfterFencesBeforeSwap,
    AfterTransferAndSwapBeforeUnlock,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EventProbePhase {
    BeforeBatchValidation,
    AcceptedBatchBeforeFirstDelivery,
    BeforeOutputSend,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TaskProbePhase {
    PreparedAndGated,
    Transferred,
    Activated,
    Dropped,
}

#[cfg(test)]
type PublishProbe = Arc<dyn Fn(PublishProbePhase) + Send + Sync>;
#[cfg(test)]
type EventProbe = Arc<dyn Fn(EventProbePhase) + Send + Sync>;
#[cfg(test)]
type TaskProbe = Arc<dyn Fn(TaskProbePhase, usize) + Send + Sync>;
#[cfg(test)]
type CatalogPlanProbe = Arc<dyn Fn(&str, &mut ServerToolCatalog) + Send + Sync>;

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
pub(super) struct McpServerQos {
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
pub(super) struct ServerRuntime {
    pub(super) config: McpServerConfig,
    pub(super) info: tokio::sync::RwLock<RuntimeInfo>,
    pub(super) reconnecting: AtomicBool,
    pub(super) qos: McpServerQos,
    // Fingerprint of the global proxy settings at the time this runtime was started.
    // Used to force-restart SSE transports when proxy settings change.
    pub(super) proxy_fingerprint: Option<String>,
}

/// Fully bootstrapped candidate that has not yet been published into the
/// manager's runtime map or tool index. Configuration reconciliation prepares
/// every replacement first, so a later bootstrap failure cannot evict a
/// working runtime.
struct PreparedServerRuntime {
    publication: Option<Arc<ServerPublication>>,
    activation: Option<RuntimeActivation>,
}

impl PreparedServerRuntime {
    fn publication(&self) -> &Arc<ServerPublication> {
        self.publication
            .as_ref()
            .expect("staged publication is present until commit")
    }

    fn into_commit(mut self) -> PreparedRuntimeCommit {
        let publication = self
            .publication
            .take()
            .expect("staged publication commits once");
        let activation = self
            .activation
            .take()
            .expect("staged runtime activation commits once");
        PreparedRuntimeCommit {
            publication,
            activation: Some(activation),
            published: false,
        }
    }
}

/// Candidate ownership after all fallible publication planning is complete.
/// Until `mark_published`, Drop retires the candidate synchronously. After the
/// generation swap, all candidates are marked first and only then are their
/// pre-owned background tasks activated, so a panic cannot retire a runtime
/// already reachable from the published generation.
struct PreparedRuntimeCommit {
    publication: Arc<ServerPublication>,
    activation: Option<RuntimeActivation>,
    published: bool,
}

impl PreparedRuntimeCommit {
    #[cfg(test)]
    fn publication(&self) -> &Arc<ServerPublication> {
        &self.publication
    }

    fn mark_published(&mut self) {
        self.published = true;
    }

    fn activate(&mut self) {
        if let Some(activation) = self.activation.take() {
            activation.activate();
        }
    }

    #[cfg(test)]
    fn discard_activation(&mut self) {
        self.activation = None;
    }
}

impl Drop for PreparedRuntimeCommit {
    fn drop(&mut self) {
        if !self.published {
            self.publication.close_admission();
            self.publication.runtime.retire();
        }
    }
}

impl Drop for PreparedServerRuntime {
    fn drop(&mut self) {
        if let Some(publication) = self.publication.take() {
            publication.close_admission();
            publication.runtime.retire();
        }
        self.activation = None;
    }
}

/// Synchronous activation handle for background tasks that were spawned,
/// gated, and attached to a candidate runtime before its publication became
/// durable. Dropping the sender keeps those tasks behind the gate until the
/// candidate runtime retires and aborts them.
struct RuntimeActivation {
    gate: Option<watch::Sender<bool>>,
    #[cfg(test)]
    task_count: usize,
    #[cfg(test)]
    probe: Option<TaskProbe>,
}

impl RuntimeActivation {
    fn activate(mut self) {
        #[cfg(test)]
        self.observe(TaskProbePhase::Transferred);
        if let Some(gate) = self.gate.take() {
            gate.send_replace(true);
        }
        #[cfg(test)]
        self.observe(TaskProbePhase::Activated);
    }

    #[cfg(test)]
    fn observe(&self, phase: TaskProbePhase) {
        if let Some(probe) = &self.probe {
            probe(phase, self.task_count);
        }
    }
}

impl Drop for RuntimeActivation {
    fn drop(&mut self) {
        #[cfg(test)]
        if self.gate.is_some() {
            self.observe(TaskProbePhase::Dropped);
        }
    }
}

/// Manages MCP server connections and tool execution.
pub struct McpServerManager {
    authority: Arc<GenerationAuthority>,
    index: Arc<ToolIndex>,
    event_tx: Option<tokio::sync::mpsc::Sender<McpEvent>>,
    config: Option<Arc<tokio::sync::RwLock<Config>>>,
    event_sequence_lock: Arc<Mutex<()>>,
    reconcile_lock: Arc<Mutex<()>>,
    next_publication_id: Arc<AtomicU64>,
    next_runtime_id: Arc<AtomicU64>,
    #[cfg(test)]
    publish_probe: Option<PublishProbe>,
    #[cfg(test)]
    event_probe: Option<EventProbe>,
    #[cfg(test)]
    task_probe: Option<TaskProbe>,
    #[cfg(test)]
    catalog_plan_probe: Option<CatalogPlanProbe>,
}

impl Clone for McpServerManager {
    fn clone(&self) -> Self {
        Self {
            authority: self.authority.clone(),
            index: self.index.clone(),
            event_tx: self.event_tx.clone(),
            config: self.config.clone(),
            event_sequence_lock: self.event_sequence_lock.clone(),
            reconcile_lock: self.reconcile_lock.clone(),
            next_publication_id: self.next_publication_id.clone(),
            next_runtime_id: self.next_runtime_id.clone(),
            #[cfg(test)]
            publish_probe: self.publish_probe.clone(),
            #[cfg(test)]
            event_probe: self.event_probe.clone(),
            #[cfg(test)]
            task_probe: self.task_probe.clone(),
            #[cfg(test)]
            catalog_plan_probe: self.catalog_plan_probe.clone(),
        }
    }
}

impl McpServerManager {
    pub fn new() -> Self {
        let authority = GenerationAuthority::new(MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS);
        Self {
            index: Arc::new(ToolIndex::from_authority(authority.clone())),
            authority,
            event_tx: None,
            config: None,
            event_sequence_lock: Arc::new(Mutex::new(())),
            reconcile_lock: Arc::new(Mutex::new(())),
            next_publication_id: Arc::new(AtomicU64::new(1)),
            next_runtime_id: Arc::new(AtomicU64::new(1)),
            #[cfg(test)]
            publish_probe: None,
            #[cfg(test)]
            event_probe: None,
            #[cfg(test)]
            task_probe: None,
            #[cfg(test)]
            catalog_plan_probe: None,
        }
    }

    /// Create a manager that can respect global proxy settings when connecting SSE transports.
    pub fn new_with_config(config: Arc<tokio::sync::RwLock<Config>>) -> Self {
        let authority = GenerationAuthority::new(MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS);
        Self {
            index: Arc::new(ToolIndex::from_authority(authority.clone())),
            authority,
            event_tx: None,
            config: Some(config),
            event_sequence_lock: Arc::new(Mutex::new(())),
            reconcile_lock: Arc::new(Mutex::new(())),
            next_publication_id: Arc::new(AtomicU64::new(1)),
            next_runtime_id: Arc::new(AtomicU64::new(1)),
            #[cfg(test)]
            publish_probe: None,
            #[cfg(test)]
            event_probe: None,
            #[cfg(test)]
            task_probe: None,
            #[cfg(test)]
            catalog_plan_probe: None,
        }
    }

    pub fn with_event_channel(mut self, tx: tokio::sync::mpsc::Sender<McpEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub fn tool_index(&self) -> Arc<ToolIndex> {
        self.index.clone()
    }

    /// Freeze one coherent MCP generation for multi-field reads or exact
    /// execution resolution.
    pub fn snapshot(&self) -> McpRuntimeSnapshot {
        McpRuntimeSnapshot::new(self.authority.clone(), self.authority.generation())
    }

    pub(crate) fn has_same_authority(&self, index: &ToolIndex) -> bool {
        GenerationAuthority::same_authority(&self.authority, index.authority())
    }

    #[cfg(test)]
    fn observe_publish(&self, phase: PublishProbePhase) {
        if let Some(probe) = &self.publish_probe {
            probe(phase);
        }
    }

    #[cfg(test)]
    fn observe_event(&self, phase: EventProbePhase) {
        if let Some(probe) = &self.event_probe {
            probe(phase);
        }
    }

    fn allocate_publication_id(&self) -> Result<PublicationId> {
        self.next_publication_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map(PublicationId::new)
            .map_err(|_| McpError::PublicationIdentityExhausted)
    }

    fn allocate_runtime_id(&self) -> Result<RuntimeId> {
        self.next_runtime_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map(RuntimeId::new)
            .map_err(|_| McpError::RuntimeIdentityExhausted)
    }

    /// Get all server IDs.
    pub fn list_servers(&self) -> Vec<String> {
        self.snapshot().server_ids()
    }

    /// Get runtime info for a server.
    pub fn get_server_info(&self, server_id: &str) -> Option<RuntimeInfo> {
        self.snapshot().server_info(server_id)
    }

    /// `(server_id, instructions)` for every currently-ready server that returned
    /// `instructions` from `initialize`. Used to inject each connected server's
    /// own usage guidance into the system prompt — so guidance appears only while
    /// the server is actually loaded. Sorted by server id for a stable prompt.
    pub fn connected_server_instructions(&self) -> Vec<(String, String)> {
        self.snapshot().connected_server_instructions()
    }

    /// Check if a server is running.
    pub fn is_server_running(&self, server_id: &str) -> bool {
        self.snapshot().contains_server(server_id)
    }

    fn current_expected(&self, server_id: &str) -> Option<ExpectedPublication> {
        self.snapshot().expected_server(server_id)
    }

    fn current_expected_for_runtime(
        &self,
        runtime: &Arc<TransportRuntime>,
    ) -> Option<ExpectedPublication> {
        self.snapshot().expected_runtime(runtime)
    }

    fn is_current_publication(&self, expected: &ExpectedPublication) -> bool {
        self.current_expected(expected.server_id())
            .is_some_and(|current| Arc::ptr_eq(&current.publication, &expected.publication))
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
