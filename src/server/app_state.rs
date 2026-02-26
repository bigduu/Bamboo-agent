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
//!     let state = AppState::new(app_data_dir).await;
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

use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::core::storage::JsonlStorage;
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::AgentEvent;
use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
use crate::agent::mcp::McpServerManager;
use crate::agent::skill::{SkillManager, SkillStoreConfig};
use crate::core::Config;
use crate::process::ProcessRegistry;
use crate::server::metrics_service::MetricsService;

/// Default system prompt for agent interactions
pub const DEFAULT_BASE_PROMPT: &str =
    "You are a helpful AI assistant with access to various tools and skills.";

/// Guidance for workspace-based interactions
pub const WORKSPACE_PROMPT_GUIDANCE: &str =
    "If you need to inspect files, check the workspace first, then ~/.bamboo.";

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
    /// Application data directory (typically ~/.bamboo)
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

    /// Persistent storage backend for sessions
    ///
    /// Uses JSONL format for append-only event logging.
    pub storage: JsonlStorage,

    /// Composite tool executor (builtin + MCP tools)
    ///
    /// Combines built-in tools (file ops, code execution) with
    /// MCP-provided tools from configured servers.
    pub tools: Arc<dyn ToolExecutor>,

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

    /// Registry for tracking external processes (e.g., Claude Code CLI sessions)
    pub process_registry: Arc<ProcessRegistry>,

    /// Discovered Claude Code CLI binary path (if installed)
    pub claude_cli_path: Option<String>,

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

impl AppState {
    /// Create unified app state with direct provider access
    ///
    /// This eliminates the proxy pattern where we created an AgentAppState
    /// that called back to web_service via HTTP. Now we have direct provider access.
    ///
    /// # Arguments
    ///
    /// * `bamboo_home_dir` - Bamboo home directory containing all application data.
    ///                        This is the root directory (e.g., ~/.bamboo) that contains:
    ///                        - config.json: Configuration file
    ///                        - sessions/: Conversation history
    ///                        - skills/: Skill definitions
    ///                        - workflows/: Workflow definitions
    ///                        - cache/: Cached data
    ///                        - runtime/: Runtime files
    ///
    /// # Returns
    ///
    /// A fully initialized AppState with all components ready for use.
    ///
    /// # Panics
    ///
    /// Panics if storage initialization fails (critical error).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_agent::server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo")).await;
    ///     let provider = state.get_provider().await;
    ///     let _models = provider.list_models().await.ok();
    /// }
    /// ```
    pub async fn new(bamboo_home_dir: PathBuf) -> Self {
        // Load config from the specified data directory
        let config = Config::from_data_dir(Some(bamboo_home_dir.clone()));

        // Create provider with direct access (no HTTP proxy)
        let provider =
            match crate::agent::llm::create_provider_with_dir(&config, bamboo_home_dir.clone())
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    // Keep the server usable for configuration/UI even when provider init fails,
                    // but do not silently fall back to a different provider.
                    log::error!("Failed to create provider: {}.", e);
                    Arc::new(UnconfiguredProvider {
                        message: e.to_string(),
                    })
                }
            };

        Self::new_with_provider(bamboo_home_dir, config, provider).await
    }

    /// Create unified app state with a specific provider
    ///
    /// Allows injecting a custom LLM provider instead of creating
    /// one from configuration. Useful for testing and custom deployments.
    ///
    /// # Arguments
    ///
    /// * `bamboo_home_dir` - Bamboo home directory containing all application data
    /// * `config` - Application configuration
    /// * `provider` - Pre-configured LLM provider implementation
    ///
    /// # Returns
    ///
    /// A fully initialized AppState with the provided provider.
    ///
    /// # Initialization Steps
    ///
    /// 1. Initialize JSONL storage in `{bamboo_home_dir}/sessions`
    /// 4. Load built-in tools
    /// 5. Initialize MCP manager and load configured servers
    /// 6. Create composite tool executor (builtin + MCP)
    /// 7. Initialize skill manager
    /// 8. Initialize metrics service with SQLite backend
    /// 9. Start runner cleanup task (removes completed runners after 5 minutes)
    ///
    /// # Panics
    ///
    /// Panics if storage or metrics initialization fails.
    pub async fn new_with_provider(
        bamboo_home_dir: PathBuf,
        config: Config,
        provider: Arc<dyn LLMProvider>,
    ) -> Self {
        let data_dir = bamboo_home_dir.clone();
        let sessions_dir = data_dir.join("sessions");

        log::info!("Initializing storage at: {:?}", sessions_dir);
        let storage = JsonlStorage::new(&sessions_dir);
        if let Err(e) = storage.init().await {
            log::error!("Failed to init storage at {:?}: {}", sessions_dir, e);
            panic!("Failed to init storage: {}", e);
        }
        log::info!("Storage initialized successfully at: {:?}", sessions_dir);

        // Discover Claude Code CLI once at startup. This is optional.
        // We do it early so we can conditionally register the `claude_code` tool.
        let claude_cli_path =
            tokio::task::spawn_blocking(|| crate::claude::try_find_claude_binary())
                .await
                .ok()
                .flatten();

        if let Some(ref path) = claude_cli_path {
            log::info!("Claude Code CLI enabled (found at: {})", path);
        } else {
            log::warn!("Claude Code CLI not found; Claude integration disabled");
        }

        let config = Arc::new(RwLock::new(config));

        // Initialize built-in tools (with optional Claude Code tool)
        let builtin_executor =
            crate::agent::tools::BuiltinToolExecutor::new_with_config(config.clone());
        if let Some(ref path) = claude_cli_path {
            if let Err(e) = builtin_executor.register_tool(
                crate::agent::tools::tools::ClaudeCodeTool::new(path.clone()),
            ) {
                log::warn!("Failed to register claude_code tool: {}", e);
            }
        }
        let builtin_tools: Arc<dyn ToolExecutor> = Arc::new(builtin_executor);

        // Initialize MCP manager
        let mcp_manager = Arc::new(McpServerManager::new());

        // Try to load MCP config and initialize servers
        let mcp_config = config.read().await.mcp.clone();
        mcp_manager.initialize_from_config(&mcp_config).await;

        // Create composite tool executor (builtin + MCP)
        let mcp_tools = Arc::new(crate::agent::mcp::McpToolExecutor::new(
            mcp_manager.clone(),
            mcp_manager.tool_index(),
        ));
        let tools: Arc<dyn ToolExecutor> = Arc::new(crate::agent::mcp::CompositeToolExecutor::new(
            builtin_tools,
            mcp_tools,
        ));

        // Initialize skill manager
        let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: data_dir.join("skills"),
        }));
        if let Err(error) = skill_manager.initialize().await {
            log::warn!("Failed to initialize skill manager: {}", error);
        }

        // Initialize metrics service
        let metrics_service = Arc::new(
            MetricsService::new(data_dir.join("metrics.db"))
                .await
                .unwrap_or_else(|error| {
                    log::error!("Failed to initialize metrics storage: {}", error);
                    panic!("Failed to init metrics storage: {}", error);
                }),
        );

        // Initialize agent runners with cleanup task
        let agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Start runner cleanup task
        {
            let runners = agent_runners.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;

                    let mut runners_guard = runners.write().await;
                    let now = Utc::now();

                    runners_guard.retain(|session_id, runner| {
                        let should_keep = match &runner.status {
                            AgentStatus::Running => true,
                            _ => {
                                let age = now.signed_duration_since(
                                    runner.completed_at.unwrap_or(runner.started_at),
                                );
                                age.num_seconds() < 300 // 5 minute TTL
                            }
                        };

                        if !should_keep {
                            log::debug!("[{}] Cleaning up completed runner", session_id);
                        }

                        should_keep
                    });
                }
            });
        }

        // Initialize Claude runners with cleanup task
        let claude_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let runners = claude_runners.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;

                    let mut runners_guard = runners.write().await;
                    let now = Utc::now();

                    runners_guard.retain(|session_id, runner| {
                        let should_keep = match &runner.status {
                            AgentStatus::Running => true,
                            _ => {
                                let age = now.signed_duration_since(
                                    runner.completed_at.unwrap_or(runner.started_at),
                                );
                                age.num_seconds() < 300 // 5 minute TTL
                            }
                        };

                        if !should_keep {
                            log::debug!("[claude:{}] Cleaning up completed runner", session_id);
                        }

                        should_keep
                    });
                }
            });
        }

        // Initialize process registry (external process lifecycle)
        let process_registry = Arc::new(ProcessRegistry::new());

        let provider_lock: Arc<RwLock<Arc<dyn LLMProvider>>> = Arc::new(RwLock::new(provider));
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(
            crate::server::reloadable_provider::ReloadableProvider::new(provider_lock.clone()),
        );

        Self {
            app_data_dir: bamboo_home_dir,
            config,
            provider: provider_lock,
            provider_handle,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            storage,
            tools,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            skill_manager,
            mcp_manager,
            metrics_service,
            agent_runners,
            process_registry,
            claude_cli_path,
            claude_runners,
            claude_session_aliases: Arc::new(RwLock::new(HashMap::new())),
            metrics_bus: None, // Will be set by server if needed
        }
    }

    /// Reload the provider based on current configuration
    ///
    /// Re-reads the configuration and creates a new LLM provider
    /// instance, allowing runtime switching of providers or models.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the provider was successfully reloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration cannot be read
    /// - Provider initialization fails (e.g., invalid API key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_agent::server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo")).await;
    ///
    ///     // User updated config file...
    ///     state.reload_provider().await.expect("Provider reload failed");
    /// }
    /// ```
    pub async fn reload_provider(&self) -> Result<(), crate::agent::llm::LLMError> {
        let config = self.config.read().await.clone();

        let configured_model = match config.provider.as_str() {
            "copilot" => config
                .providers
                .copilot
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "openai" => config
                .providers
                .openai
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "anthropic" => config
                .providers
                .anthropic
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            "gemini" => config
                .providers
                .gemini
                .as_ref()
                .and_then(|p| p.model.as_ref()),
            _ => None,
        };

        log::info!(
            "Reloading provider: type={}, model={:?}",
            config.provider,
            configured_model
        );

        let new_provider =
            crate::agent::llm::create_provider_with_dir(&config, self.app_data_dir.clone()).await?;

        let mut provider = self.provider.write().await;
        *provider = new_provider;

        log::info!("Provider reloaded successfully to: {}", config.provider);
        Ok(())
    }

    /// Reload the configuration from file
    ///
    /// Reads the configuration file again and updates the in-memory
    /// config. Note: This does NOT automatically reload the provider;
    /// call `reload_provider()` afterwards if needed.
    ///
    /// # Returns
    ///
    /// The newly loaded configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_agent::server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo")).await;
    ///
    ///     // Reload config from disk
    ///     let new_config = state.reload_config().await;
    ///
    ///     // Optionally reload provider with new config
    ///     state.reload_provider().await.ok();
    /// }
    /// ```
    pub async fn reload_config(&self) -> Config {
        let new_config = Config::from_data_dir(Some(self.app_data_dir.clone()));
        let mut config = self.config.write().await;
        *config = new_config.clone();
        new_config
    }

    /// Persist the current in-memory config to disk (`{app_data_dir}/config.json`).
    ///
    /// This is the single "exit" for configuration writes in the server runtime.
    pub async fn persist_config(&self) -> anyhow::Result<()> {
        let config = self.config.read().await.clone();
        let data_dir = self.app_data_dir.clone();
        tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
        Ok(())
    }

    /// Get a clone of the current provider
    ///
    /// Returns a thread-safe reference to the current LLM provider.
    /// This is the preferred way to access the provider for making requests.
    ///
    /// # Returns
    ///
    /// An Arc reference to the current provider implementation.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_agent::server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo")).await;
    ///     let provider = state.get_provider().await;
    ///
    ///     // Use provider to make LLM requests...
    /// }
    /// ```
    pub async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        // Important: return the reloadable handle, not a snapshot clone of the current provider.
        // This ensures config/provider switches take effect without restarting the server.
        self.provider_handle.clone()
    }

    /// Shutdown all MCP servers gracefully
    ///
    /// Sends shutdown signals to all running MCP server processes
    /// and waits for them to terminate cleanly.
    ///
    /// This should be called during application shutdown to ensure
    /// MCP servers are not left running as orphaned processes.
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        log::info!("Shutting down MCP servers...");
        self.mcp_manager.shutdown_all().await;
        log::info!("MCP servers shut down complete");
    }

    /// Save an agent event to persistent storage
    ///
    /// Appends the event to the session's event log in JSONL format.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    /// * `event` - Event to save
    #[allow(dead_code)]
    pub async fn save_event(&self, session_id: &str, event: &AgentEvent) {
        let _ = self.storage.append_event(session_id, event).await;
    }

    /// Save a complete session to persistent storage
    ///
    /// Writes the session metadata and all events to the storage backend.
    ///
    /// # Arguments
    ///
    /// * `session` - Session object to save
    pub async fn save_session(&self, session: &crate::agent::core::Session) {
        let _ = self.storage.save_session(session).await;
    }

    /// Get all tool schemas from the composite tool executor
    ///
    /// Returns schemas for both built-in tools and MCP-provided tools.
    /// These schemas are used to inform the LLM about available tools.
    ///
    /// # Returns
    ///
    /// Vector of tool schemas in Anthropic's tool definition format.
    pub fn get_all_tool_schemas(&self) -> Vec<crate::agent::core::tools::ToolSchema> {
        self.tools.list_tools()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_state_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf()).await;

        // Verify basic fields
        assert!(state.sessions.read().await.is_empty());
    }
}
