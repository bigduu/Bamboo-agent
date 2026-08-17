//! Unified application state management for the Bamboo server
//!
//! This module provides the central AppState struct that consolidates all
//! server state including sessions, storage, LLM providers, tools, and metrics.
//!
//! # Architecture
//!
//! The AppState uses a unified design that eliminates the proxy pattern where
//! web_service created an AgentAppState that called back via HTTP. Instead, it
//! provides direct access to all components.
//!
//! ```text
//! ┌────────────────────────────────────────────────────┐
//! │              AppState (Unified)                    │
//! │                                                    │
//! │  ┌──────────────┐      ┌──────────────┐          │
//! │  │   Config     │      │   Provider   │          │
//! │  │  (Hot-reload)│◄────►│   (LLM)      │          │
//! │  └──────────────┘      └──────────────┘          │
//! │                                                    │
//! │  ┌──────────────┐      ┌──────────────┐          │
//! │  │   Sessions   │      │   Storage    │          │
//! │  │  (In-memory) │      │  (Persistent)│          │
//! │  └──────────────┘      └──────────────┘          │
//! │                                                    │
//! │  ┌──────────────┐      ┌──────────────┐          │
//! │  │    Tools     │      │    Skills    │          │
//! │  │ (Builtin+MCP)│      │   Manager    │          │
//! │  └──────────────┘      └──────────────┘          │
//! │                                                    │
//! │  ┌──────────────┐      ┌──────────────┐          │
//! │  │     MCP      │      │   Metrics    │          │
//! │  │   Manager    │      │   Service    │          │
//! │  └──────────────┘      └──────────────┘          │
//! └────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Features
//!
//! - **Hot-reloadable configuration**: Config and provider can be reloaded at runtime
//! - **Direct provider access**: No HTTP proxy overhead
//! - **Session management**: In-memory session cache with persistent storage
//! - **Tool composition**: Combines built-in and MCP tools
//! - **Metrics collection**: Integrated metrics and event tracking
//!
//! # Usage Example
//!
//! ```rust,no_run
//! use bamboo_server::app_state::AppState;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Initialize app state
//!     let app_data_dir = PathBuf::from("/path/to/.bamboo");
//!     let state = AppState::new(app_data_dir)
//!         .await
//!         .expect("failed to initialize app state");
//!
//!     // Access components
//!     let provider = state.get_provider().await;
//!     let schemas = state.get_all_tool_schemas();
//!
//!     // Hot reload configuration
//!     state.reload_config().await;
//!     state.reload_provider().await.ok();
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use crate::error::AppError;
use crate::schedule_app::{ScheduleManager, ScheduleStore};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::AgentEvent;
use bamboo_agent_core::{tools::ToolSchema, Message};
use bamboo_engine::execution::spawn::SpawnScheduler;
use bamboo_infrastructure::process::registry::ProcessRegistry;
use bamboo_llm::Config;
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use bamboo_mcp::manager::McpServerManager;
use bamboo_metrics::metrics_service::MetricsService;
use bamboo_skills::SkillManager;
use bamboo_storage::LockedSessionStore;
use bamboo_storage::SessionStoreV2;

// Context functions moved to bamboo-agent-runtime::context
pub use bamboo_engine::context::{
    build_env_prompt_context, build_workspace_prompt_context, workspace_prompt_guidance,
    DEFAULT_BASE_PROMPT, ENV_CONTEXT_END_MARKER, ENV_CONTEXT_START_MARKER,
    WORKSPACE_CONTEXT_END_MARKER, WORKSPACE_CONTEXT_PREFIX, WORKSPACE_CONTEXT_START_MARKER,
};

/// Placeholder provider used when the configured provider cannot be initialized.
///
/// This keeps the server usable for configuration/UX flows while ensuring we fail fast
/// (instead of silently switching to a different provider or model).
struct UnconfiguredProvider {
    message: String,
}

#[async_trait]
impl LLMProvider for UnconfiguredProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        Err(LLMError::Auth(format!(
            "LLM provider is not configured: {}",
            self.message
        )))
    }

    async fn list_models(&self) -> bamboo_llm::provider::Result<Vec<String>> {
        Err(LLMError::Auth(format!(
            "LLM provider is not configured: {}",
            self.message
        )))
    }
}

// Re-export execution types from the runtime crate.
pub use bamboo_engine::execution::runner_state::{AgentRunner, AgentStatus};

/// Unified application state consolidating web_service and agent/server state
///
/// This struct holds all the state needed to run the Bamboo server, including
/// configuration, LLM providers, sessions, storage, tools, skills, and metrics.
///
/// # Design Goals
///
/// - **Direct access**: Components are directly accessible without HTTP proxies
/// - **Hot reload**: Configuration and providers can be reloaded at runtime
/// - **Thread safety**: Uses Arc<RwLock> for concurrent access
/// - **Persistence**: Integrates with JsonlStorage for session persistence
///
/// # Component Overview
///
/// | Component | Purpose | Thread-Safe |
/// |-----------|---------|--------------|
/// | `config` | Application configuration | Yes (RwLock) |
/// | `provider` | Hot-reloadable LLM provider | Yes (RwLock) |
/// | `sessions` | Active conversation sessions | Yes (RwLock) |
/// | `storage` | Persistent session storage | Yes (Arc) |
/// | `tools` | Tool execution (builtin + MCP) | Yes (Arc) |
/// | `skill_manager` | Skill registry and execution | Yes (Arc) |
/// | `mcp_manager` | MCP server lifecycle | Yes (Arc) |
/// | `metrics_service` | Usage metrics collection | Yes (Arc) |
/// | `agent_runners` | Active agent executions | Yes (RwLock) |
pub struct AppState {
    /// Application data directory (configured via `BAMBOO_DATA_DIR`; default `${HOME}/.bamboo`)
    pub app_data_dir: PathBuf,

    /// Hot-reloadable application configuration
    ///
    /// Can be reloaded from disk at runtime using `reload_config()`.
    pub config: Arc<RwLock<Config>>,

    /// Process-owned modular configuration authority. Production bootstrap
    /// always installs one after the recoverable legacy split; injected test
    /// states may omit it and retain the compatibility-only config path.
    pub config_facade: Option<Arc<bamboo_config::ConfigFacade>>,

    /// Serializes a config WRITE's whole [in-memory mutation + disk persist] with
    /// a `reload_config`'s [disk read + in-memory swap], so a reload can never
    /// observe an in-flight-but-not-yet-persisted update and clobber it with the
    /// stale disk copy (the residual of #41). It is NOT the `config` RwLock —
    /// using a separate mutex keeps config READERS (the hot agent-loop path)
    /// unblocked during a write's disk I/O. #126.
    pub config_io_lock: Arc<tokio::sync::Mutex<()>>,

    /// Server-owned live configuration watcher and its health envelope.
    /// The runtime handle keeps the directory watcher tasks alive.
    pub config_live_health: Arc<std::sync::RwLock<config_runtime::ConfigLiveHealth>>,
    /// MCP section health is independent from provider health so an invalid or
    /// degraded MCP candidate cannot make unrelated sections appear unhealthy.
    pub mcp_config_live_health: Arc<std::sync::RwLock<config_runtime::ConfigLiveHealth>>,
    #[allow(dead_code)]
    config_watcher: config_runtime::ConfigWatcherRuntime,
    /// Project shared-resource watcher. Held for the server lifetime.
    #[allow(dead_code)]
    pub(crate) project_resource_watcher: project_watcher::ProjectResourceWatcher,

    /// Encrypted credential authority exposed only through metadata/replace/clear APIs.
    pub credential_store: Arc<bamboo_config::CredentialStore>,

    /// Shared Remote Cluster Fabric deploy engine (one worker registry across the
    /// HTTP operator handlers and the `cluster` agent tool).
    pub fabric_deployer: Arc<bamboo_server_tools::FabricDeployer>,

    /// In-process mailbox bus (broker), when not externally configured. Held so
    /// it lives for the server's lifetime (dropping it aborts the bus). `None`
    /// when an external broker is configured or the bus couldn't bind. Never read
    /// — its only job is to keep the bus task alive until AppState drops.
    #[allow(dead_code)]
    embedded_broker: Option<builder::EmbeddedBroker>,

    /// The cluster health monitor sweep. Lives for the server's lifetime (dropping
    /// it aborts the sweep). `None` when the monitor is disabled
    /// (`health_interval_secs = 0`). Never read — held only to keep the task alive.
    #[allow(dead_code)]
    health_monitor: Option<builder::HealthMonitor>,

    /// Hot-reloadable LLM provider with direct access
    ///
    /// This eliminates the proxy pattern where we created an AgentAppState
    /// that called back to web_service via HTTP. Now we have direct provider access.
    pub provider: Arc<RwLock<Arc<dyn LLMProvider>>>,

    /// Stable handle that always delegates to the latest provider in `provider`.
    ///
    /// This avoids stale provider snapshots after runtime config updates.
    provider_handle: Arc<dyn LLMProvider>,

    /// Active conversation sessions (in-memory cache)
    ///
    /// Maps session IDs to Session objects. Persisted to storage
    /// via the `storage` field.
    pub sessions: bamboo_engine::SessionCache,

    /// Persistent storage backend for sessions (V2).
    ///
    /// Implemented as folder-per-session with a global `sessions.json` index.
    pub storage: Arc<dyn Storage>,

    /// Concrete session store implementation (for index/list/cleanup APIs).
    pub session_store: Arc<SessionStoreV2>,

    /// Durable, Bamboo-home-scoped idempotency receipts for root-session
    /// creation. Kept outside each target session directory so deleting a
    /// session cannot erase retry truth during the retention window.
    pub(crate) session_create_operations:
        Arc<session_create_operations::SessionCreateOperationStore>,

    /// Short-lived, process-local response receipts for `POST /chat` and
    /// `POST /execute`. Raw caller keys and request payloads are never stored.
    pub(crate) mutation_idempotency: Arc<mutation_idempotency::MutationIdempotencyStore>,

    /// Authoritative first-class Project registry and shared-resource paths.
    pub project_store: Arc<bamboo_projects::ProjectStore>,

    /// Redacted adapter used by HTTP creation paths and the agent runtime to
    /// resolve one authoritative Project/workspace identity.
    pub project_context_resolver: Arc<bamboo_engine::project_context::ProjectContextResolver>,

    /// Instance-scoped live workspace providers used for preview and
    /// post-persistence publication. The equivalent process-global providers
    /// remain first-registration-wins; retaining this pair prevents parallel
    /// test AppStates from resolving through a sibling state's config/root.
    pub(crate) workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,

    /// Per-session write serialisation + metadata-merge persistence layer.
    ///
    /// Wraps the same [`Storage`] as `self.storage`, adding per-session
    /// `Mutex` guards and authoritative-metadata-group merge semantics.
    /// Use `self.persistence.merge_save_runtime(...)` for any write that
    /// may race with a UI metadata update.
    pub persistence: Arc<LockedSessionStore>,

    /// Durable logical-session delivery plane. These are internal runtime
    /// capabilities; no public messaging endpoint is registered.
    pub session_inbox: Arc<dyn bamboo_domain::SessionInboxPort>,
    pub session_activation_router: Arc<bamboo_engine::SessionActivationRouter>,
    pub session_messenger: Arc<bamboo_engine::SessionMessenger>,

    /// Framework-owned session coordinator (cache + storage + persistence).
    /// The canonical load/save coordination lives here in `bamboo-engine`, not
    /// on `AppState`; the inherent `AppState::load_session`/`save_and_cache_session`
    /// methods now delegate to it. Holds clones of the same `Arc`s as the
    /// `sessions`/`storage`/`persistence` fields above.
    pub session_repo: bamboo_engine::SessionRepository,

    /// Background scheduler for async sub-session spawning.
    pub spawn_scheduler: Arc<SpawnScheduler>,

    /// Coordinates child completion notifications into parent resume.
    pub child_completion_coordinator: Arc<bamboo_engine::ChildCompletionCoordinator>,

    /// Spawner for the guardian adversarial-review child, injected into each run
    /// so the terminal gate can create a read-only reviewer (the engine runner
    /// cannot construct a child directly — see [`bamboo_engine::GuardianSpawner`]).
    /// Backed by a dedicated [`crate::tools::ChildSessionAdapter`].
    pub guardian_spawner: Arc<dyn bamboo_engine::GuardianSpawner>,

    /// Bash self-resume hook (issue #84 Phase 2b). Backed by the same
    /// [`ChildCompletionCoordinator`] that handles child-completion resumes —
    /// it polls the live shell registry and resumes a session once all its
    /// background bash shells finish.
    pub bash_resume_hook: Arc<dyn bamboo_engine::BashResumeHook>,

    /// Schedule store (timed tasks).
    pub schedule_store: Arc<ScheduleStore>,

    /// Background schedule manager that triggers scheduled runs.
    pub schedule_manager: Arc<ScheduleManager>,

    /// bamboo-connect manager (#452 / epic #447): owns every configured IM
    /// platform's long-poll/dispatch background task. Fully inert (zero
    /// tasks) when `config.connect.platforms` is empty. Held so its tasks
    /// live for the server's lifetime (`ConnectManager::drop` aborts them).
    pub connect_manager: Arc<crate::connect::ConnectManager>,

    /// Tool surface factory providing pre-built tool executors for each session type.
    ///
    /// Use `state.tools_for(ToolSurface::Root)` for root sessions,
    /// `state.tools_for(ToolSurface::Child)` for child sessions, etc.
    pub tool_factory: crate::tools::ToolSurfaceFactory,

    /// Shared tool-execution permission checker — the same `Arc` the tool
    /// executors use. Retained so request handlers can record session grants
    /// when the user approves a permission prompt (see the respond handler).
    pub permission_checker: Arc<dyn bamboo_tools::permission::PermissionChecker>,

    /// Durable, revisioned authority for permission policy. The checker is
    /// updated only after a successful commit to this section.
    pub permission_section: Arc<bamboo_tools::permission::PermissionSection>,

    /// Serializes the complete permission commit + live-checker publication.
    pub permission_io_lock: Arc<tokio::sync::Mutex<()>>,
    pub approval_registry:
        bamboo_engine::external_agents::approval_registry::SharedApprovalRegistry,

    /// Backend notification policy service (preferences + dedup + per-session
    /// relays). Classifies agent events into `AgentEvent::Notification` for
    /// clients to render; preferences are persisted server-side.
    pub notification_service: Arc<bamboo_notification::NotificationService>,

    /// Live SSE/WS client-subscriber counts per session (see
    /// [`watchers::SessionWatchers`]). Used to suppress a redundant desktop
    /// popup for categories the UI already surfaces while a client is
    /// actively watching a session.
    pub session_watchers: Arc<watchers::SessionWatchers>,

    /// Cancellation tokens for in-flight requests
    ///
    /// Maps request/session IDs to their cancellation tokens,
    /// allowing graceful shutdown of long-running operations.
    pub cancel_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,

    /// Cancels the supervised MCP proxy service (issue #47) on shutdown so the
    /// reconnect/backoff supervisor stops cleanly instead of looping forever
    /// after an intended stop. Unused when no broker is configured.
    pub mcp_proxy_shutdown: CancellationToken,

    /// Skill manager for prompt-based skill execution
    ///
    /// Manages the skill registry and handles skill lookup,
    /// validation, and execution.
    pub skill_manager: Arc<SkillManager>,

    /// Durable, recovered workflow-run boundary. It owns the production engine
    /// plus server-derived session/catalog trust adapters.
    pub workflow_runs: crate::workflow::WorkflowRunAccess,

    /// MCP server manager for external tool servers
    ///
    /// Handles lifecycle of Model Context Protocol servers,
    /// including initialization, tool discovery, and shutdown.
    pub mcp_manager: Arc<McpServerManager>,

    /// Supervises long-running "service" plugins (issue #479, prereq for
    /// epic #477). Always constructed, fully inert until a plugin install
    /// (or the boot-time reconcile) calls `start_service`. See
    /// `crate::service_manager`'s module docs.
    pub service_manager: Arc<crate::service_manager::ServiceManager>,

    /// Handle to the background boot-time service reconcile pass
    /// (`plugin_installer::boot_reconcile_services`, spawned fire-and-forget
    /// by `app_state::builder` — see its comment). It is deliberately NOT
    /// synchronized against `plugin_installer::PLUGIN_OP_LOCK`, so it can, in
    /// principle, race a `ServerPluginInstaller::install`/
    /// `stop_services_for_upgrade` call that lands on the SAME data dir
    /// while it is still in flight (e.g. immediately after construction).
    /// Production code never touches this field; it exists purely as a
    /// test-only synchronization point (see
    /// [`AppState::wait_for_boot_reconcile_services`]) so
    /// `plugin_installer::tests` can deterministically drain that one-shot
    /// pass before exercising service install/stop/upgrade, instead of
    /// racing it under CI scheduling jitter (issue #486).
    #[doc(hidden)]
    pub boot_reconcile_services_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Metrics collection and persistence service
    ///
    /// Tracks token usage, costs, and performance metrics
    /// across all sessions.
    pub metrics_service: Arc<MetricsService>,

    /// Active agent runners indexed by session ID
    ///
    /// Each runner manages event broadcasting and cancellation
    /// for an active agent execution.
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,

    /// Reference-counted execute handlers still preparing a runner, keyed by
    /// session. This server-scoped registry closes the durable pending-turn
    /// expiry race without leaking state across AppState instances/tests.
    pub(crate) execute_startups: Arc<std::sync::Mutex<HashMap<String, usize>>>,

    /// Session-scoped event streams (long-lived).
    ///
    /// Unlike `agent_runners`, these senders exist even when no agent execution is running.
    /// They are used for:
    /// - UI subscriptions to `/api/v1/events/{session_id}` (background tasks, etc.)
    /// - sub-session forwarding (child -> parent)
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,

    /// Account-scoped durable change feed (powers `GET /api/v1/stream`).
    ///
    /// Unlike `session_event_senders`, this is a single account-wide sink: all
    /// durable change events (message appended, session metadata, task updates,
    /// terminal status) across every session are sequenced, journaled to disk,
    /// and broadcast here for resumable multi-client sync.
    pub account_sink: Arc<bamboo_engine::events::AccountEventSink>,

    /// Registry for tracking external processes.
    pub process_registry: Arc<ProcessRegistry>,

    /// Optional metrics bus for event streaming
    ///
    /// When enabled, allows subscribing to metrics events
    /// in real-time.
    pub metrics_bus: Option<bamboo_metrics::bus::MetricsBus>,

    /// Unified agent execution runtime holding shared resources.
    pub agent: Arc<bamboo_engine::Agent>,

    /// Multi-provider registry (used when features.provider_model_ref is enabled).
    pub provider_registry: Arc<bamboo_llm::ProviderRegistry>,

    /// Provider/model router (used when features.provider_model_ref is enabled).
    pub provider_router: Arc<bamboo_llm::ProviderModelRouter>,

    /// Unified model catalog service (used when features.provider_model_ref is enabled).
    pub model_catalog: Arc<bamboo_llm::ModelCatalogService>,

    /// Tracks session ids whose auto-title generation is currently in flight.
    ///
    /// Used by [`crate::title_gen`] to dedupe concurrent invocations
    /// (e.g. multiple chat messages arriving while a regenerate-title request is running).
    pub title_gen_in_flight: Arc<dashmap::DashSet<String>>,

    /// v2-P2 (#181, slice 2): in-memory one-time pairing codes. A 6-digit numeric
    /// code (keyed by the code string) maps to an entry holding its expiry. Codes
    /// are PROCESS-EPHEMERAL — never persisted to `config.json`; a restart drops
    /// all outstanding codes by design. Keyed by `Instant`-based expiry; expired
    /// entries are purged opportunistically on insert/lookup.
    pub pairing_codes: Arc<dashmap::DashMap<String, crate::handlers::settings::PairingCodeEntry>>,

    /// v2-P2 (#181, slice 2): per-process brute-force guard for the public
    /// code-redemption path (`POST /v2/pair { code }`). A 6-digit code is only
    /// ~1M space, so a public redeem endpoint is brute-forceable without a guard.
    /// Tracks recent FAILED code-redemption attempts and a cooldown deadline.
    pub pairing_code_guard: Arc<crate::handlers::settings::PairingCodeGuard>,

    /// #190: per-client-IP brute-force guard for the public root-password
    /// endpoints (`POST /v1/bamboo/access/verify` and the root-password path of
    /// `POST /v2/pair`). Tracks recent FAILED root-password attempts per IP and a
    /// per-key cooldown; loopback/desktop requests are exempted by the handlers
    /// so the desktop can never lock itself out. PROCESS-EPHEMERAL — never
    /// persisted; a restart clears all counters.
    pub root_password_guard: Arc<crate::handlers::settings::RootPasswordGuard>,

    /// Process-ephemeral credentials for Codex children that route model calls
    /// through this server. Tokens are hashed in memory and revoked at the end
    /// of their owning actor activation.
    pub(crate) codex_run_tokens: Arc<crate::codex_run_tokens::CodexRunTokenRegistry>,
}

impl AppState {
    /// Try to claim the title-generation slot for `session_id`.
    /// Returns `true` on success, `false` if generation is already in flight.
    pub fn title_gen_acquire(&self, session_id: &str) -> bool {
        self.title_gen_in_flight.insert(session_id.to_string())
    }

    /// Release the title-generation slot for `session_id`. Idempotent.
    pub fn title_gen_release(&self, session_id: &str) {
        self.title_gen_in_flight.remove(session_id);
    }

    /// Test-only synchronization point (see
    /// [`boot_reconcile_services_handle`](Self::boot_reconcile_services_handle)'s
    /// doc comment): wait for the background boot-time service reconcile
    /// pass to finish. Idempotent — a second call (or a call after
    /// production code never having populated the handle) is a no-op.
    #[doc(hidden)]
    pub async fn wait_for_boot_reconcile_services(&self) {
        let handle = self.boot_reconcile_services_handle.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

mod agent_session_context;
mod builder;
mod config_runtime;
pub(crate) use config_runtime::ConfigLiveHealth;
pub(crate) use config_runtime::ConfigSectionMutationError;
pub(crate) use config_runtime::CredentialBackedResetCommit;
pub mod init;
pub mod parent_approval_reviewer;
mod persistence;
mod project_watcher;
mod provider_api;
pub mod resume_adapter;
pub mod runner_lifecycle;
// `pub` (not `pub(crate)`): `ScheduleContext::notification_relay` (a public
// field of the public `schedule_app::ScheduleContext`) is typed
// `session_events::NotificationRelayDeps`, so external callers that build a
// `ScheduleContext` by hand (e.g. integration tests) need to name it.
pub(crate) mod mutation_idempotency;
pub(crate) mod session_create_operations;
pub mod session_events;
mod session_loader;
mod tools;
pub mod watchers;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct ConfigUpdateEffects {
    pub reload_provider: bamboo_config::patch::ReloadMode,
    pub reconcile_mcp: bamboo_config::patch::ReloadMode,
}

impl Default for ConfigUpdateEffects {
    fn default() -> Self {
        Self {
            reload_provider: bamboo_config::patch::ReloadMode::None,
            reconcile_mcp: bamboo_config::patch::ReloadMode::None,
        }
    }
}
