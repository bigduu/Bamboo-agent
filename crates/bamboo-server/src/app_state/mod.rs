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
use crate::metrics_service::MetricsService;
use crate::schedules::{ScheduleManager, ScheduleStore};
use crate::spawn_scheduler::SpawnScheduler;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::AgentEvent;
use bamboo_agent_core::{tools::ToolSchema, Message};
use bamboo_engine::McpServerManager;
use bamboo_engine::SkillManager;
use bamboo_infrastructure::registry::ProcessRegistry;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::SessionStoreV2;
use bamboo_infrastructure::{LLMError, LLMProvider, LLMStream, LockedSessionStore};

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
    ) -> bamboo_infrastructure::provider::Result<LLMStream> {
        Err(LLMError::Auth(format!(
            "LLM provider is not configured: {}",
            self.message
        )))
    }

    async fn list_models(&self) -> bamboo_infrastructure::provider::Result<Vec<String>> {
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
    pub sessions: Arc<RwLock<HashMap<String, bamboo_agent_core::Session>>>,

    /// Persistent storage backend for sessions (V2).
    ///
    /// Implemented as folder-per-session with a global `sessions.json` index.
    pub storage: Arc<dyn Storage>,

    /// Concrete session store implementation (for index/list/cleanup APIs).
    pub session_store: Arc<SessionStoreV2>,

    /// Per-session write serialisation + metadata-merge persistence layer.
    ///
    /// Wraps the same [`Storage`] as `self.storage`, adding per-session
    /// `Mutex` guards and authoritative-metadata-group merge semantics.
    /// Use `self.persistence.merge_save_runtime(...)` for any write that
    /// may race with a UI metadata update.
    pub persistence: Arc<LockedSessionStore>,

    /// Background scheduler for async sub-session spawning.
    pub spawn_scheduler: Arc<SpawnScheduler>,

    /// Coordinates child completion notifications into parent resume.
    pub child_completion_coordinator: Arc<child_completion_coordinator::ChildCompletionCoordinator>,

    /// Schedule store (timed tasks).
    pub schedule_store: Arc<ScheduleStore>,

    /// Background schedule manager that triggers scheduled runs.
    pub schedule_manager: Arc<ScheduleManager>,

    /// Tool surface factory providing pre-built tool executors for each session type.
    ///
    /// Use `state.tools_for(ToolSurface::Root)` for root sessions,
    /// `state.tools_for(ToolSurface::Child)` for child sessions, etc.
    pub tool_factory: crate::tools::ToolSurfaceFactory,

    /// Subagent profile registry (role definitions for child sessions).
    ///
    /// Composed from built-in profiles + user/project/env overrides at
    /// startup. Currently provided as ambient state for future PRs that
    /// will plumb it into child-session creation and tool-surface filtering;
    /// the runtime does not read from it yet, so adding this field is a
    /// no-op for existing behaviour.
    pub subagent_profiles: Arc<bamboo_domain::subagent::SubagentProfileRegistry>,

    /// Cancellation tokens for in-flight requests
    ///
    /// Maps request/session IDs to their cancellation tokens,
    /// allowing graceful shutdown of long-running operations.
    pub cancel_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,

    /// Skill manager for prompt-based skill execution
    ///
    /// Manages the skill registry and handles skill lookup,
    /// validation, and execution.
    pub skill_manager: Arc<SkillManager>,

    /// MCP server manager for external tool servers
    ///
    /// Handles lifecycle of Model Context Protocol servers,
    /// including initialization, tool discovery, and shutdown.
    pub mcp_manager: Arc<McpServerManager>,

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

    /// Session-scoped event streams (long-lived).
    ///
    /// Unlike `agent_runners`, these senders exist even when no agent execution is running.
    /// They are used for:
    /// - UI subscriptions to `/api/v1/events/{session_id}` (background tasks, etc.)
    /// - sub-session forwarding (child -> parent)
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,

    /// Registry for tracking external processes.
    pub process_registry: Arc<ProcessRegistry>,

    /// Optional metrics bus for event streaming
    ///
    /// When enabled, allows subscribing to metrics events
    /// in real-time.
    pub metrics_bus: Option<bamboo_engine::metrics::bus::MetricsBus>,

    /// Unified agent execution runtime holding shared resources.
    pub agent: Arc<bamboo_engine::Agent>,

    /// Multi-provider registry (used when features.provider_model_ref is enabled).
    pub provider_registry: Arc<bamboo_infrastructure::ProviderRegistry>,

    /// Provider/model router (used when features.provider_model_ref is enabled).
    pub provider_router: Arc<bamboo_infrastructure::ProviderModelRouter>,

    /// Unified model catalog service (used when features.provider_model_ref is enabled).
    pub model_catalog: Arc<bamboo_infrastructure::ModelCatalogService>,

    /// Tracks session ids whose auto-title generation is currently in flight.
    ///
    /// Used by [`crate::title_gen`] to dedupe concurrent invocations
    /// (e.g. execute handler firing while a regenerate-title request is running).
    pub title_gen_in_flight: Arc<dashmap::DashSet<String>>,
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
}

mod builder;
pub mod child_completion_coordinator;
mod config_runtime;
pub mod init;
mod persistence;
mod provider_api;
pub mod resume_adapter;
pub mod runner_lifecycle;
pub(crate) mod session_events;
mod session_loader;
mod tools;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, Default)]
pub struct ConfigUpdateEffects {
    pub reload_provider: bool,
    pub reconcile_mcp: bool,
}
