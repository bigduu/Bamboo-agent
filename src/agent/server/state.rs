//! Agent server state management for the Bamboo server
//!
//! This module provides the AppState implementation used specifically by
//! the agent server component. It manages sessions, LLM providers, tools,
//! skills, MCP servers, and metrics.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │        Agent AppState                        │
//! │                                              │
//! │  Sessions ◄────► Storage (JSONL)            │
//! │     ↓                                         │
//! │  Agent Runner (Event Broadcasting)          │
//! │     ↓                                         │
//! │  Tools (Builtin + MCP) + Skills             │
//! │     ↓                                         │
//! │  LLM Provider (OpenAI/Anthropic/Copilot)    │
//! │     ↓                                         │
//! │  Metrics Service (SQLite)                   │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! # Key Components
//!
//! - **Sessions**: In-memory cache of active conversations
//! - **Storage**: Persistent JSONL-based session storage
//! - **LLM Provider**: Pluggable LLM backend (OpenAI, Anthropic, Copilot)
//! - **Tools**: Composite executor combining built-in and MCP tools
//! - **Skills**: Prompt-based skill registry and executor
//! - **Metrics**: Usage tracking and cost calculation
//!
//! # Usage Example
//!
//! ```rust,no_run
//! use bamboo::agent::server::state::AppState;
//!
//! #[tokio::main]
//! async fn main() {
//!     let state = AppState::new_with_config(
//!         "openai",
//!         "https://api.openai.com/v1".to_string(),
//!         "gpt-4".to_string(),
//!         "sk-test".to_string(),
//!         None,
//!         false,
//!     ).await;
//!
//!     let schemas = state.get_all_tool_schemas();
//!     println!("Available tools: {}", schemas.len());
//! }
//! ```

use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{storage::JsonlStorage, AgentEvent, Session};
use crate::agent::llm::OpenAIProvider;
use crate::agent::mcp::{CompositeToolExecutor, McpServerManager};
use crate::agent::server::metrics_service::MetricsService;
use crate::agent::skill::{SkillManager, SkillStoreConfig};
use crate::agent::tools::BuiltinToolExecutor;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

/// Default system prompt for agent interactions
pub const DEFAULT_BASE_PROMPT: &str =
    "You are a helpful AI assistant with access to various tools and skills.";

/// Guidance for workspace-based interactions
pub const WORKSPACE_PROMPT_GUIDANCE: &str =
    "If you need to inspect files, check the workspace first, then ~/.bamboo.";

/// Status of an agent execution runner
///
/// Tracks the lifecycle state of an agent run from initialization
/// through completion or cancellation.
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
/// Provides event broadcasting to support multiple subscribers watching
/// the same agent run simultaneously. Each active agent run has an
/// associated AgentRunner instance.
///
/// # Features
///
/// - **Event Broadcasting**: Uses broadcast channel for multi-subscriber support
/// - **Cancellation**: Provides token for graceful shutdown
/// - **Status Tracking**: Monitors run progress and completion
/// - **Budget Replay**: Stores last token budget event for new subscribers
#[derive(Clone)]
pub struct AgentRunner {
    /// Broadcast sender for agent events
    ///
    /// Multiple clients can subscribe via `event_sender.subscribe()`
    /// to receive real-time updates.
    pub event_sender: broadcast::Sender<AgentEvent>,

    /// Cancellation token for graceful shutdown
    ///
    /// When triggered, the agent should stop at the next safe point.
    pub cancel_token: CancellationToken,

    /// Current execution status
    pub status: AgentStatus,

    /// Timestamp when the run was started
    pub started_at: DateTime<Utc>,

    /// Timestamp when the run completed (if finished)
    pub completed_at: Option<DateTime<Utc>>,

    /// Last token budget event to replay for new subscribers
    ///
    /// Ensures late-joining subscribers see current token usage.
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
    /// Initializes:
    /// - Broadcast channel with 1000-event capacity
    /// - Fresh cancellation token
    /// - Pending status
    /// - Current timestamp as start time
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

/// Application state for the agent server
///
/// Manages all the state needed for agent execution, including sessions,
/// LLM providers, tools, skills, MCP servers, and metrics collection.
///
/// # Component Overview
///
/// | Component | Purpose | Type |
/// |-----------|---------|------|
/// | `sessions` | Active conversation cache | Arc<RwLock<HashMap>> |
/// | `storage` | Persistent session storage | JsonlStorage |
/// | `llm` | LLM provider backend | Arc<dyn LLMProvider> |
/// | `tools` | Tool execution | Arc<dyn ToolExecutor> |
/// | `skill_manager` | Skill registry | Arc<SkillManager> |
/// | `mcp_manager` | MCP server lifecycle | Arc<McpServerManager> |
/// | `metrics_service` | Usage tracking | Arc<MetricsService> |
/// | `agent_runners` | Active executions | Arc<RwLock<HashMap>> |
///
/// # Thread Safety
///
/// All fields are wrapped in Arc for shared ownership. Mutable state
/// uses RwLock for concurrent read access with exclusive writes.
pub struct AppState {
    /// Active conversation sessions (in-memory cache)
    ///
    /// Maps session IDs to Session objects. Changes are persisted
    /// to storage via the `storage` field.
    pub sessions: Arc<RwLock<HashMap<String, Session>>>,

    /// Persistent storage backend for sessions
    ///
    /// Uses JSONL format for efficient append-only logging of events.
    pub storage: JsonlStorage,

    /// LLM provider for making completion requests
    ///
    /// Supports OpenAI, Anthropic, and Copilot providers.
    /// Configured during initialization based on config.
    pub llm: Arc<dyn crate::agent::llm::LLMProvider>,

    /// Composite tool executor (built-in + MCP tools)
    ///
    /// Combines built-in tools with MCP-provided tools into
    /// a unified interface.
    pub tools: Arc<dyn ToolExecutor>,

    /// Cancellation tokens for in-flight operations
    ///
    /// Maps identifiers to cancellation tokens for graceful shutdown.
    pub cancel_tokens: Arc<RwLock<HashMap<String, tokio_util::sync::CancellationToken>>>,

    /// Skill manager for prompt-based skills
    ///
    /// Handles skill discovery, validation, and execution.
    pub skill_manager: Arc<SkillManager>,

    /// MCP server manager for external tool servers
    ///
    /// Manages lifecycle of Model Context Protocol servers.
    pub mcp_manager: Arc<McpServerManager>,

    /// Metrics collection and persistence service
    ///
    /// Tracks usage, costs, and performance metrics in SQLite.
    pub metrics_service: Arc<MetricsService>,

    /// Default model name for LLM requests
    pub model_name: String,

    /// Agent runners with broadcast channels for multi-subscriber support
    ///
    /// Each runner manages event broadcasting for an active agent execution.
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
}

impl AppState {
    /// Create a new AppState with default settings
    ///
    /// Uses OpenAI provider with test settings. Primarily for testing.
    #[allow(dead_code)]
    pub async fn new() -> Self {
        Self::new_with_config(
            "openai",
            "http://localhost:12123".to_string(),
            "kimi-for-coding".to_string(),
            "sk-test".to_string(),
            None,
            false,
        )
        .await
    }

    /// Create AppState with specific provider configuration
    ///
    /// Initializes all components based on the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `provider` - Provider type ("openai", "copilot", etc.)
    /// * `llm_base_url` - Base URL for LLM API requests
    /// * `model` - Model identifier to use
    /// * `api_key` - API key for authentication
    /// * `app_data_dir` - Optional data directory (defaults to ~/.bamboo)
    /// * `_tauri_mode` - Unused, kept for API compatibility
    ///
    /// # Returns
    ///
    /// Fully initialized AppState ready for use.
    ///
    /// # Initialization Steps
    ///
    /// 1. Set up data directory (default: ~/.bamboo)
    /// 2. Migrate old session files if needed
    /// 3. Initialize JSONL storage
    /// 4. Create LLM provider (with Copilot auth if needed)
    /// 5. Initialize built-in tools
    /// 6. Load and initialize MCP servers from config
    /// 7. Create composite tool executor
    /// 8. Initialize skill manager
    /// 9. Initialize metrics service
    /// 10. Start runner cleanup task (5-minute TTL)
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Storage initialization fails
    /// - Metrics initialization fails
    /// - Copilot authentication fails (when using Copilot provider)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo::agent::server::state::AppState;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new_with_config(
    ///         "openai",
    ///         "https://api.openai.com/v1".to_string(),
    ///         "gpt-4".to_string(),
    ///         "sk-your-key".to_string(),
    ///         None,
    ///         false,
    ///     ).await;
    ///
    ///     println!("Initialized with {} tools", state.get_all_tool_schemas().len());
    /// }
    /// ```
    pub async fn new_with_config(
        provider: &str,
        llm_base_url: String,
        model: String,
        api_key: String,
        app_data_dir: Option<PathBuf>,
        _tauri_mode: bool,
    ) -> Self {
        // Use provided app_data_dir or default to ~/.bamboo
        let data_dir = app_data_dir.unwrap_or_else(bamboo_dir);
        let sessions_dir = data_dir.join("sessions");

        // Migrate session files from old location if needed
        // This is a temporary migration function that will be removed in v0.3.0
        #[allow(deprecated)]
        if let Err(e) = crate::core::migrate_session_files() {
            log::warn!("Failed to migrate session files: {}", e);
        }

        log::info!("Initializing storage at: {:?}", sessions_dir);
        let storage = JsonlStorage::new(&sessions_dir);
        if let Err(e) = storage.init().await {
            log::error!("Failed to init storage at {:?}: {}", sessions_dir, e);
            panic!("Failed to init storage: {}", e);
        }
        log::info!("Storage initialized successfully at: {:?}", sessions_dir);

        // Initialize LLM Provider based on provider type
        log::info!(
            "Creating LLM provider: {} with base URL: {} and model: {}",
            provider,
            llm_base_url,
            model
        );

        let llm: Arc<dyn crate::agent::llm::LLMProvider> = match provider {
            "copilot" => {
                log::info!("Using Copilot provider with Device Code authentication");

                // Create Copilot provider and authenticate
                let mut copilot_provider = if api_key != "sk-test" && !api_key.is_empty() {
                    // Use provided API key directly
                    crate::agent::llm::CopilotProvider::with_token(api_key)
                } else {
                    // Use device code flow
                    crate::agent::llm::CopilotProvider::new()
                };

                // Try silent auth first (cached token)
                if !copilot_provider.is_authenticated() {
                    match copilot_provider.try_authenticate_silent().await {
                        Ok(true) => {
                            log::info!("Authenticated with cached Copilot token");
                        }
                        Ok(false) => {
                            println!("\n⚠️  Copilot authentication required");
                            // Run interactive device code flow
                            if let Err(e) = copilot_provider.authenticate().await {
                                log::error!("Failed to authenticate with Copilot: {}", e);
                                panic!("Copilot authentication failed: {}. Please try again.", e);
                            }
                        }
                        Err(e) => {
                            log::error!("Authentication error: {}", e);
                            panic!("Copilot authentication error: {}", e);
                        }
                    }
                }

                Arc::new(copilot_provider)
            }
            _ => {
                log::info!("Using OpenAI provider");
                Arc::new(OpenAIProvider::new(api_key).with_base_url(llm_base_url))
            }
        };

        // Initialize built-in tools
        let builtin_tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());

        // Initialize MCP manager
        let mcp_manager = Arc::new(McpServerManager::new());

        // Try to load MCP config and initialize servers
        let mcp_config = load_mcp_config(&data_dir).await;
        mcp_manager.initialize_from_config(&mcp_config).await;

        // Create composite tool executor (builtin + MCP)
        let mcp_tools = Arc::new(crate::agent::mcp::McpToolExecutor::new(
            mcp_manager.clone(),
            mcp_manager.tool_index(),
        ));
        let tools: Arc<dyn ToolExecutor> =
            Arc::new(CompositeToolExecutor::new(builtin_tools, mcp_tools));

        let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: data_dir.join("skills"),
        }));
        if let Err(error) = skill_manager.initialize().await {
            log::warn!("Failed to initialize skill manager: {}", error);
        }

        let metrics_service = Arc::new(
            MetricsService::new(data_dir.join("metrics.db"))
                .await
                .unwrap_or_else(|error| {
                    log::error!("Failed to initialize metrics storage: {}", error);
                    panic!("Failed to init metrics storage: {}", error);
                }),
        );

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
                                age.num_seconds() < 300 // 5分钟 TTL
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

        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            storage,
            llm,
            tools,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            skill_manager,
            mcp_manager,
            metrics_service,
            model_name: model,
            agent_runners,
        }
    }

    /// Shutdown all MCP servers gracefully
    ///
    /// Sends shutdown signals to all running MCP server processes
    /// and waits for them to terminate cleanly.
    ///
    /// Should be called during application shutdown to prevent
    /// orphaned MCP server processes.
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        log::info!("Shutting down MCP servers...");
        self.mcp_manager.shutdown_all().await;
        log::info!("MCP servers shut down complete");
    }

    /// Save an agent event to persistent storage
    ///
    /// Appends the event to the session's JSONL event log.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    /// * `event` - Event to persist
    #[allow(dead_code)]
    pub async fn save_event(&self, session_id: &str, event: &AgentEvent) {
        let _ = self.storage.append_event(session_id, event).await;
    }

    /// Save a complete session to persistent storage
    ///
    /// Writes session metadata and all events to the JSONL backend.
    ///
    /// # Arguments
    ///
    /// * `session` - Session object to save
    pub async fn save_session(&self, session: &Session) {
        let _ = self.storage.save_session(session).await;
    }

    /// Get all tool schemas from the composite tool executor
    ///
    /// Returns schemas for both built-in tools (file operations, code execution, etc.)
    /// and MCP-provided tools. These schemas inform the LLM about available tools.
    ///
    /// # Returns
    ///
    /// Vector of tool schemas in Anthropic's tool definition format.
    pub fn get_all_tool_schemas(&self) -> Vec<crate::agent::core::tools::ToolSchema> {
        self.tools.list_tools()
    }
}

#[cfg(test)]
fn merge_base_and_enhancement(base_prompt: &str, enhance_prompt: Option<&str>) -> String {
    let mut merged = base_prompt.to_string();

    if let Some(enhancement) = enhance_prompt
        .map(str::trim)
        .filter(|enhancement| !enhancement.is_empty())
    {
        merged.push_str("\n\n");
        merged.push_str(enhancement);
    }

    merged
}

#[cfg(test)]
fn merge_workspace_context(base_prompt: &str, workspace_path: Option<&str>) -> String {
    let mut merged = base_prompt.to_string();

    if let Some(workspace_path) = workspace_path
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty())
    {
        merged.push_str("\n\nWorkspace path: ");
        merged.push_str(workspace_path);
        merged.push('\n');
        merged.push_str(WORKSPACE_PROMPT_GUIDANCE);
    }

    merged
}

/// Get the default Bamboo data directory path
///
/// Returns the path to ~/.bamboo on Unix/Linux/macOS or
/// %USERPROFILE%\.bamboo on Windows.
///
/// Falls back to the system temp directory if home cannot be determined.
fn bamboo_dir() -> PathBuf {
    crate::config::paths::bamboo_home()
}

/// Load MCP configuration from file
///
/// Reads MCP server configuration from `{app_data_root}/mcp.json`.
/// Returns default (empty) configuration if file doesn't exist or
/// cannot be parsed.
///
/// # Arguments
///
/// * `app_data_root` - Root directory containing mcp.json
///
/// # Returns
///
/// Loaded McpConfig, or default if loading fails.
async fn load_mcp_config(app_data_root: &std::path::Path) -> crate::agent::mcp::McpConfig {
    let config_path = app_data_root.join("mcp.json");

    if !config_path.exists() {
        log::info!(
            "No MCP config file found at {:?}, using default",
            config_path
        );
        return crate::agent::mcp::McpConfig::default();
    }

    match tokio::fs::read_to_string(&config_path).await {
        Ok(content) => match serde_json::from_str::<crate::agent::mcp::McpConfig>(&content) {
            Ok(config) => {
                log::info!("Loaded MCP config with {} servers", config.servers.len());
                config
            }
            Err(e) => {
                log::error!("Failed to parse MCP config: {}", e);
                crate::agent::mcp::McpConfig::default()
            }
        },
        Err(e) => {
            log::error!("Failed to read MCP config: {}", e);
            crate::agent::mcp::McpConfig::default()
        }
    }
}

/// Start SSE stream sender
#[cfg(test)]
pub fn spawn_sse_sender(
    mut rx: mpsc::Receiver<AgentEvent>,
    tx: mpsc::Sender<actix_web::web::Bytes>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let event_json = match serde_json::to_string(&event) {
                Ok(json) => json,
                Err(_) => continue,
            };

            let sse_data = format!("data: {}\n\n", event_json);
            let bytes = actix_web::web::Bytes::from(sse_data);

            if tx.send(bytes).await.is_err() {
                break;
            }

            // If Complete or Error event, end stream
            match &event {
                AgentEvent::Complete { .. } | AgentEvent::Error { .. } => {
                    break;
                }
                _ => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{merge_base_and_enhancement, merge_workspace_context};

    #[test]
    fn merge_base_and_enhancement_appends_non_empty_value() {
        let merged = merge_base_and_enhancement("Base prompt", Some("Extra instructions"));
        assert_eq!(merged, "Base prompt\n\nExtra instructions");
    }

    #[test]
    fn merge_base_and_enhancement_ignores_empty_value() {
        let merged = merge_base_and_enhancement("Base prompt", Some("   "));
        assert_eq!(merged, "Base prompt");
    }

    #[test]
    fn merge_workspace_context_appends_non_empty_workspace_path() {
        let merged = merge_workspace_context("Base prompt", Some("/tmp/workspace"));
        assert_eq!(
            merged,
            "Base prompt\n\nWorkspace path: /tmp/workspace\nIf you need to inspect files, check the workspace first, then ~/.bamboo."
        );
    }

    #[test]
    fn merge_workspace_context_ignores_empty_workspace_path() {
        let merged = merge_workspace_context("Base prompt", Some("  "));
        assert_eq!(merged, "Base prompt");
    }
}
