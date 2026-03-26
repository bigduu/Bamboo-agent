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
//! use bamboo_agent::server::app_state::AppState;
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
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use crate::agent::core::storage::{SessionStoreV2, Storage};
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::AgentEvent;
use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
use crate::agent::mcp::McpServerManager;
use crate::agent::skill::{SkillManager, SkillStoreConfig};
use crate::core::Config;
use crate::process::ProcessRegistry;
use crate::server::error::AppError;
use crate::server::metrics_service::MetricsService;
use crate::server::schedules::manager::ScheduleContext;
use crate::server::schedules::{ScheduleManager, ScheduleStore};
use crate::server::spawn_scheduler::{SpawnContext, SpawnScheduler};

/// Default system prompt for agent interactions
pub const DEFAULT_BASE_PROMPT: &str =
    "You are Bodhi, a highly capable AI assistant.\n\nYou help users solve problems quickly and correctly. Be concise, practical, and proactive.\nIf requirements are unclear, ask focused clarifying questions before proceeding.\nUse Task for non-trivial multi-step task tracking.\nDo not proactively use SubSession/sub-agent delegation unless the user explicitly asks for sub sessions, sub agents, delegation, or parallel agent work.\n\nWhen making function calls using tools, always include a brief text explanation before or alongside the tool calls describing what you are about to do and why. Never silently call tools without any visible narration to the user.\nBefore ending a task, always call ask_user to confirm whether the user still has additional requests. Do not ask final confirmation in plain assistant text. Before that ask_user call, always output a detailed summary of the work completed, findings, changes made, and any relevant context. The summary should be thorough and informative, not just a one-liner.";

pub const WORKSPACE_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_START -->";
pub const WORKSPACE_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_END -->";
pub const WORKSPACE_CONTEXT_PREFIX: &str = "Workspace path: ";

/// Guidance for workspace-based interactions
pub fn workspace_prompt_guidance() -> String {
    let config_path =
        crate::core::paths::path_to_display_string(&crate::core::paths::config_json_path());
    format!(
        "If you need to inspect files, check the workspace first, then Bamboo data at {}. Bamboo configuration is stored in {} (equivalent to ${{BAMBOO_DATA_DIR}}/config.json).",
        crate::core::paths::bamboo_dir_display(),
        config_path
    )
}

pub fn build_workspace_prompt_context(workspace_path: &str) -> Option<String> {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return None;
    }

    Some(format!(
        "{WORKSPACE_CONTEXT_START_MARKER}\n{WORKSPACE_CONTEXT_PREFIX}{workspace_path}\n{}\n{WORKSPACE_CONTEXT_END_MARKER}",
        workspace_prompt_guidance()
    ))
}

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
    ) -> crate::agent::llm::provider::Result<LLMStream> {
        Err(LLMError::Auth(format!(
            "LLM provider is not configured: {}",
            self.message
        )))
    }

    async fn list_models(&self) -> crate::agent::llm::provider::Result<Vec<String>> {
        Err(LLMError::Auth(format!(
            "LLM provider is not configured: {}",
            self.message
        )))
    }
}

/// Status of an agent execution runner
///
/// Represents the lifecycle state of an agent run from initialization
/// through completion or error.
#[derive(Debug, Clone)]
pub enum AgentStatus {
    /// Agent is initialized but not yet running
    Pending,

    /// Agent is currently executing
    Running,

    /// Agent completed successfully
    Completed,

    /// Agent execution was cancelled by user
    Cancelled,

    /// Agent execution failed with an error message
    Error(String),
}

/// Runner that manages agent execution for a session
///
/// Each active agent run has an associated AgentRunner that coordinates
/// event broadcasting, cancellation, and status tracking.
///
/// # Event Broadcasting
///
/// Uses a broadcast channel to support multiple subscribers watching
/// the same agent run simultaneously.
///
/// # Cancellation
///
/// Provides a cancellation token that can be used to gracefully stop
/// an in-progress agent execution.
#[derive(Debug, Clone)]
pub struct AgentRunner {
    /// Broadcast sender for agent events
    ///
    /// Allows multiple clients to subscribe to agent events
    /// via `event_sender.subscribe()`.
    pub event_sender: broadcast::Sender<AgentEvent>,

    /// Cancellation token for graceful shutdown
    ///
    /// When triggered, the agent should stop execution at the
    /// next safe point.
    pub cancel_token: CancellationToken,

    /// Current status of the agent run
    pub status: AgentStatus,

    /// Timestamp when the run was started
    pub started_at: DateTime<Utc>,

    /// Timestamp when the run completed (if finished)
    pub completed_at: Option<DateTime<Utc>>,

    /// Last token budget event to replay for new subscribers
    ///
    /// When a new client subscribes to an ongoing run, this
    /// allows them to receive the most recent token usage info.
    pub last_budget_event: Option<AgentEvent>,
}

impl Default for AgentRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRunner {
    /// Create a new agent runner with default settings
    ///
    /// Initializes a broadcast channel with capacity for 1000 events,
    /// a fresh cancellation token, and Pending status.
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(1000);
        Self {
            event_sender,
            cancel_token: CancellationToken::new(),
            status: AgentStatus::Pending,
            started_at: Utc::now(),
            completed_at: None,
            last_budget_event: None,
        }
    }
}

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
    pub sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,

    /// Persistent storage backend for sessions (V2).
    ///
    /// Implemented as folder-per-session with a global `sessions.json` index.
    pub storage: Arc<dyn Storage>,

    /// Concrete session store implementation (for index/list/cleanup APIs).
    pub session_store: Arc<SessionStoreV2>,

    /// Background scheduler for async sub-session spawning.
    pub spawn_scheduler: Arc<SpawnScheduler>,

    /// Schedule store (timed tasks).
    pub schedule_store: Arc<ScheduleStore>,

    /// Background schedule manager that triggers scheduled runs.
    pub schedule_manager: Arc<ScheduleManager>,

    /// Composite tool executor (builtin + MCP tools)
    ///
    /// Combines built-in tools (file ops, code execution) with
    /// MCP-provided tools from configured servers.
    pub tools: Arc<dyn ToolExecutor>,

    /// Tool executor for child sessions (sub-sessions).
    ///
    /// This intentionally excludes `SubSession` from schemas so child sessions
    /// cannot recursively spawn more sessions. (Enforced in the tool too.)
    pub child_tools: Arc<dyn ToolExecutor>,

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

    /// Registry for tracking external processes (e.g., Claude Code CLI sessions)
    pub process_registry: Arc<ProcessRegistry>,

    /// Discovered Claude Code CLI binary path (if installed).
    ///
    /// This is resolved asynchronously after server startup to avoid blocking
    /// core endpoints like `/v1/bamboo/setup/status`.
    pub claude_cli_path: Arc<RwLock<Option<String>>>,

    /// Active Claude Code CLI runners indexed by Claude session ID
    ///
    /// These are streamed to clients via SSE under the `/v1/agent/...` endpoints.
    pub claude_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,

    /// Maps client-provided session ids (aliases) to real Claude UUID session ids.
    ///
    /// Claude Code requires session ids to be UUIDs, but some clients/tests use
    /// human-readable strings. We accept those as aliases and generate a UUID.
    pub claude_session_aliases: Arc<RwLock<HashMap<String, String>>>,

    /// Optional metrics bus for event streaming
    ///
    /// When enabled, allows subscribing to metrics events
    /// in real-time.
    pub metrics_bus: Option<crate::agent::metrics::MetricsBus>,
}

mod builder;
mod config_runtime;
mod persistence;
mod provider_api;
mod session_events;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, Default)]
pub struct ConfigUpdateEffects {
    pub reload_provider: bool,
    pub reconcile_mcp: bool,
}
