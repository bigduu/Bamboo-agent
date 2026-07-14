//! `ServiceManager` — supervises long-running "service" plugins (issue #479,
//! prereq for epic #477's standalone connectors distributed as plugins).
//!
//! Sibling of `connect::ConnectManager` / `schedule_app::ScheduleManager`:
//! constructed once in `app_state::builder`, always alive, fully inert until
//! `start_service` is called. Neither existing precedent fits a resident
//! service process as-is:
//!
//! - `connect::ConnectManager` is too thin (no restart/health).
//! - `bamboo_mcp::manager::McpServerManager` is JSON-RPC-coupled
//!   (`ToolIndex`/QoS/circuit-breaker are meaningless for a service that
//!   speaks no MCP protocol at all).
//!
//! This module instead reuses the SHAPE of two `bamboo-mcp` patterns without
//! depending on that crate:
//!
//! - Spawn mechanics from `bamboo_mcp::transports::stdio` (`kill_on_drop`,
//!   `hide_window_for_tokio_command`, piped stdout/stderr → log lines via a
//!   dedicated reader task per stream — see [`lifecycle::spawn_stdio_logger`]).
//! - The health-check-drives-restart pattern from
//!   `bamboo_mcp::manager::lifecycle::start_health_check` /
//!   `manager::reconnect::attempt_reconnection`, generalized to
//!   [`bamboo_plugin::manifest::HealthCheckKind`]'s three kinds (process
//!   liveness is just "has the child exited", so only `Tcp`/`Http` need an
//!   actual polling task — see [`lifecycle::supervise_running_child`]) and to
//!   `bamboo_plugin::manifest::ShutdownSignal`'s graceful-then-hard-kill
//!   stop, mirroring `reconnect`'s exponential backoff.
//!
//! # Security: `env_clear()` before applying declared env
//!
//! Unlike `bamboo-mcp`'s stdio transport (which inherits bamboo-server's
//! FULL process environment — deliberately left alone, see that crate's
//! module docs), [`lifecycle::spawn_child`] calls `Command::env_clear()` and
//! then applies only a minimal `PATH`/`HOME`(+platform-runtime-essential)
//! allowlist before the manifest's declared `env` and
//! `BAMBOO_PLUGIN_SERVICE_CONFIG`. A service is the highest-trust plugin
//! artifact kind (a resident, unconstrained process) — see issue #479
//! security §2 — so it must not silently see every secret bamboo-server's own
//! environment happens to carry (API keys, tokens, etc).
//!
//! # `shutdown` vs `stop_token`
//!
//! Each [`ServiceRuntime`] carries a `shutdown: AtomicBool` (the issue's
//! explicit requirement) — the single source of truth `lifecycle` consults
//! after ANY wake to decide "was this an intentional stop" vs "should
//! `restart_policy` fire" — plus a `tokio_util::sync::CancellationToken` used
//! purely as the WAKE mechanism (interrupting an in-progress health-check
//! wait or restart backoff sleep). A bare `tokio::sync::Notify` was
//! considered and rejected: `notify_waiters` only wakes CURRENTLY-waiting
//! tasks, so a stop request arriving before the supervisor reaches its
//! `.notified().await` would be silently lost — `CancellationToken` has no
//! such race (once cancelled, `cancelled()` resolves immediately regardless
//! of call order).
//!
//! # Boot-time reconcile
//!
//! `app_state::builder` starts every ENABLED service from every plugin's
//! `installed.json` row in the background (mirrors
//! `app_state::init::init_mcp_manager`'s background MCP bootstrap) — a
//! service that is "supposed to run" (declared, enabled, plugin installed)
//! but has no live runtime (the previous `bamboo serve` process died) is
//! started fresh. See `app_state::builder`'s `boot_reconcile_services`.

mod lifecycle;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use bamboo_domain::mcp_config::ReconnectConfig;
use bamboo_plugin::manifest::{GracefulShutdown, HealthCheckSpec};

/// Resolved, ready-to-spawn configuration for one service — the output of
/// `ServiceManifestEntry::resolve` plus the owning plugin id and the
/// per-service user config path (see the module docs on
/// `BAMBOO_PLUGIN_SERVICE_CONFIG`). Built by
/// `plugin_installer::ServerPluginInstaller` (the only place with both
/// `AppState::app_data_dir` and a validated manifest).
#[derive(Debug, Clone)]
pub struct ServiceRuntimeConfig {
    pub id: String,
    pub plugin_id: String,
    pub name: Option<String>,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub health_check: HealthCheckSpec,
    pub restart_policy: ReconnectConfig,
    pub graceful_shutdown: GracefulShutdown,
    /// `<data_dir>/plugins/<plugin_id>/config.json`, passed to the child as
    /// `BAMBOO_PLUGIN_SERVICE_CONFIG`. bamboo only ever creates the PARENT
    /// directory (`plugin_installer::ServerPluginInstaller::resolve_service_config`)
    /// — it never writes or deletes this file itself, so a user/service's own
    /// config there survives an upgrade OR uninstall of the plugin bundle
    /// (bundles are plaintext; connectors carry tokens that must not be
    /// silently deleted when the bundle is — see issue #479 open question 2).
    pub user_config_path: PathBuf,
}

/// Lifecycle state of one supervised service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    Starting,
    Running,
    Degraded,
    Crashed,
    Restarting,
    Stopping,
    Stopped,
}

/// Point-in-time status of one service — what the plugin-list HTTP surface
/// (`InstalledPluginView::service_status`) and `bamboo plugin` status
/// commands read.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceStatusSnapshot {
    pub id: String,
    pub plugin_id: String,
    pub state: ServiceState,
    pub pid: Option<u32>,
    pub restart_count: u32,
    pub last_error: Option<String>,
}

/// Errors [`ServiceManager`]'s public API can return. Deliberately tiny
/// (unlike `bamboo_mcp::error::McpError`) — there is no protocol layer here
/// to report errors from.
#[derive(Debug, thiserror::Error)]
pub enum ServiceManagerError {
    #[error("service '{0}' is already running")]
    AlreadyRunning(String),
    #[error("service '{0}' is not running")]
    NotRunning(String),
}

/// Per-service runtime state, shared between the public [`ServiceManager`]
/// API and the supervisor task in [`lifecycle`].
pub(crate) struct ServiceRuntime {
    config: ServiceRuntimeConfig,
    state: RwLock<ServiceState>,
    /// `0` when no child is currently alive.
    pid: AtomicU32,
    /// Lifetime-cumulative count of restart attempts (exposed in status;
    /// never resets). Distinct from `lifecycle`'s internal
    /// consecutive-attempt counter, which DOES reset on every successful
    /// `Running` transition and drives backoff/`max_attempts`.
    restart_count: AtomicU32,
    last_error: RwLock<Option<String>>,
    /// Set by [`ServiceManager::stop_service`] BEFORE cancelling
    /// `stop_token`. The supervisor consults this (not which `select!`
    /// branch fired) after every wake to decide "intentional stop, don't
    /// restart" vs "unexpected exit, `restart_policy` may apply". See the
    /// module docs.
    shutdown: AtomicBool,
    stop_token: CancellationToken,
    supervisor: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

/// Owns every supervised service's runtime. Empty/inert until
/// `start_service` is called — mirrors `McpServerManager`'s
/// always-constructed-but-lazy lifecycle. Cheap to construct (`DashMap::new`,
/// no I/O), so `AppState` builds exactly one and shares it via `Arc`.
#[derive(Default)]
pub struct ServiceManager {
    runtimes: DashMap<String, Arc<ServiceRuntime>>,
}

impl ServiceManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start supervising `config.id`. Spawns the child (or fails fast and
    /// enters the restart-backoff loop immediately if the very first spawn
    /// fails — matching `plugin_installer`'s "best-effort start, ownership
    /// is still recorded" contract for MCP servers) and returns as soon as
    /// the supervisor task is scheduled — does NOT wait for the child to
    /// actually come up (the caller reads [`Self::status`] for that).
    pub async fn start_service(
        &self,
        config: ServiceRuntimeConfig,
    ) -> Result<(), ServiceManagerError> {
        let id = config.id.clone();
        let runtime = Arc::new(ServiceRuntime {
            config,
            state: RwLock::new(ServiceState::Starting),
            pid: AtomicU32::new(0),
            restart_count: AtomicU32::new(0),
            last_error: RwLock::new(None),
            shutdown: AtomicBool::new(false),
            stop_token: CancellationToken::new(),
            supervisor: RwLock::new(None),
        });
        // Atomic check-and-insert via the entry API: a plain
        // contains_key→insert would let two racing callers (boot reconcile —
        // spawned outside PLUGIN_OP_LOCK — vs a concurrent install of the
        // same plugin) both pass the check, the second overwriting the
        // first's runtime and orphaning its supervisor task + child process
        // beyond stop_service's reach.
        match self.runtimes.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                return Err(ServiceManagerError::AlreadyRunning(occupied.key().clone()));
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(runtime.clone());
            }
        }
        let handle = tokio::spawn(lifecycle::run_supervisor(runtime.clone()));
        *runtime.supervisor.write().await = Some(handle);
        Ok(())
    }

    /// Stop `id`: marks the runtime `shutdown` (so the supervisor never
    /// restarts it), cancels its stop token (waking it out of any
    /// health-check wait / backoff sleep / `child.wait()`), performs the
    /// graceful-signal → timeout → hard-kill sequence (see
    /// [`bamboo_plugin::manifest::GracefulShutdown`]), and awaits the
    /// supervisor task's exit before returning. Idempotent-adjacent: a
    /// second call on an already-removed id returns
    /// [`ServiceManagerError::NotRunning`] rather than panicking, so callers
    /// (uninstall/upgrade drop-diff, rollback) can treat it as best-effort
    /// exactly like `mcp_manager.stop_server`.
    pub async fn stop_service(&self, id: &str) -> Result<(), ServiceManagerError> {
        let Some((_, runtime)) = self.runtimes.remove(id) else {
            return Err(ServiceManagerError::NotRunning(id.to_string()));
        };
        runtime.shutdown.store(true, Ordering::SeqCst);
        runtime.stop_token.cancel();
        let handle = runtime.supervisor.write().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    pub fn is_running(&self, id: &str) -> bool {
        self.runtimes.contains_key(id)
    }

    pub async fn status(&self, id: &str) -> Option<ServiceStatusSnapshot> {
        let runtime = self.runtimes.get(id)?.clone();
        Some(lifecycle::snapshot(&runtime).await)
    }

    /// Snapshot of every currently-supervised service (regardless of which
    /// plugin owns it) — the raw material `handlers::agent::plugin`'s list
    /// handler groups back by `plugin_id` into
    /// `InstalledPluginView::service_status`.
    pub async fn list_status(&self) -> Vec<ServiceStatusSnapshot> {
        let runtimes: Vec<Arc<ServiceRuntime>> = self
            .runtimes
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut out = Vec::with_capacity(runtimes.len());
        for runtime in &runtimes {
            out.push(lifecycle::snapshot(runtime).await);
        }
        out
    }
}
