use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use crate::agent::core::storage::JsonlStorage;
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::AgentEvent;
use crate::agent::llm::LLMProvider;
use crate::agent::mcp::McpServerManager;
use crate::agent::server::metrics_service::MetricsService;
use crate::agent::skill::{SkillManager, SkillStoreConfig};
use crate::core::Config;

pub const DEFAULT_BASE_PROMPT: &str =
    "You are a helpful AI assistant with access to various tools and skills.";
pub const WORKSPACE_PROMPT_GUIDANCE: &str =
    "If you need to inspect files, check the workspace first, then ~/.bamboo.";

/// Runner that manages agent execution for a session
#[derive(Debug, Clone)]
pub enum AgentStatus {
    Pending,
    Running,
    Completed,
    Cancelled,
    Error(String),
}

/// Status of an agent runner
#[derive(Debug, Clone)]
pub struct AgentRunner {
    pub event_sender: broadcast::Sender<AgentEvent>,
    pub cancel_token: CancellationToken,
    pub status: AgentStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    /// Last token budget event to replay for new subscribers
    pub last_budget_event: Option<AgentEvent>,
}

impl Default for AgentRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRunner {
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
/// This eliminates the proxy pattern where web_service created an AgentAppState
/// that called back to itself via HTTP. Now we have direct provider access.
pub struct AppState {
    // From web_service::AppState
    pub app_data_dir: PathBuf,
    pub config: Arc<RwLock<Config>>,
    /// Hot-reloadable provider with direct access (no HTTP proxy)
    pub provider: Arc<RwLock<Arc<dyn LLMProvider>>>,

    // From agent::server::AppState
    pub sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,
    pub storage: JsonlStorage,
    /// Direct LLM provider (same as provider.read().await, but more convenient)
    pub llm: Arc<dyn LLMProvider>,
    pub tools: Arc<dyn ToolExecutor>,
    pub cancel_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    pub skill_manager: Arc<SkillManager>,
    pub mcp_manager: Arc<McpServerManager>,
    pub metrics_service: Arc<MetricsService>,
    pub model_name: String,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,

    // Unified metrics infrastructure
    pub metrics_bus: Option<crate::agent::metrics::MetricsBus>,
}

impl AppState {
    /// Create unified app state with direct provider access
    ///
    /// This eliminates the proxy pattern where we created an AgentAppState
    /// that called back to web_service via HTTP. Now we have direct provider access.
    pub async fn new(app_data_dir: PathBuf) -> Self {
        let config = Config::new();

        // Create provider with direct access (no HTTP proxy)
        let provider = match crate::agent::llm::create_provider_with_dir(&config, app_data_dir.clone()).await {
            Ok(p) => p,
            Err(e) => {
                log::error!("Failed to create provider: {}. Using OpenAI as fallback.", e);
                Arc::new(crate::agent::llm::OpenAIProvider::new("sk-test".to_string()))
            }
        };

        Self::new_with_provider(app_data_dir, config, provider).await
    }

    /// Create unified app state with a specific provider
    pub async fn new_with_provider(
        app_data_dir: PathBuf,
        config: Config,
        provider: Arc<dyn LLMProvider>,
    ) -> Self {
        let data_dir = app_data_dir.clone();
        let sessions_dir = data_dir.join("sessions");

        // Migrate session files from old location if needed
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

        // Initialize built-in tools
        let builtin_tools: Arc<dyn ToolExecutor> = Arc::new(crate::agent::tools::BuiltinToolExecutor::new());

        // Initialize MCP manager
        let mcp_manager = Arc::new(McpServerManager::new());

        // Try to load MCP config and initialize servers
        let mcp_config = Self::load_mcp_config(&data_dir).await;
        mcp_manager.initialize_from_config(&mcp_config).await;

        // Create composite tool executor (builtin + MCP)
        let mcp_tools = Arc::new(crate::agent::mcp::McpToolExecutor::new(
            mcp_manager.clone(),
            mcp_manager.tool_index(),
        ));
        let tools: Arc<dyn ToolExecutor> =
            Arc::new(crate::agent::mcp::CompositeToolExecutor::new(builtin_tools, mcp_tools));

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

        // Get model name from config
        let model_name = config
            .providers
            .anthropic
            .as_ref()
            .and_then(|p| p.model.as_ref())
            .cloned()
            .unwrap_or_else(|| "claude-3-5-sonnet-20241022".to_string());

        Self {
            app_data_dir,
            config: Arc::new(RwLock::new(config)),
            provider: Arc::new(RwLock::new(provider.clone())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            storage,
            llm: provider,
            tools,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            skill_manager,
            mcp_manager,
            metrics_service,
            model_name,
            agent_runners,
            metrics_bus: None, // Will be set by server if needed
        }
    }

    /// Reload the provider based on current configuration
    pub async fn reload_provider(&self) -> Result<(), crate::agent::llm::LLMError> {
        let config = self.config.read().await.clone();

        log::info!(
            "Reloading provider: type={}, model={:?}",
            config.provider,
            config
                .providers
                .anthropic
                .as_ref()
                .and_then(|p| p.model.as_ref())
        );

        let new_provider =
            crate::agent::llm::create_provider_with_dir(&config, self.app_data_dir.clone()).await?;

        let mut provider = self.provider.write().await;
        *provider = new_provider;

        log::info!("Provider reloaded successfully to: {}", config.provider);
        Ok(())
    }

    /// Reload the configuration from file
    pub async fn reload_config(&self) -> Config {
        let new_config = Config::new();
        let mut config = self.config.write().await;
        *config = new_config.clone();
        new_config
    }

    /// Get a clone of the current provider
    pub async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        self.provider.read().await.clone()
    }

    /// Shutdown all MCP servers gracefully
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        log::info!("Shutting down MCP servers...");
        self.mcp_manager.shutdown_all().await;
        log::info!("MCP servers shut down complete");
    }

    #[allow(dead_code)]
    pub async fn save_event(&self, session_id: &str, event: &AgentEvent) {
        let _ = self.storage.append_event(session_id, event).await;
    }

    pub async fn save_session(&self, session: &crate::agent::core::Session) {
        let _ = self.storage.save_session(session).await;
    }

    /// Get all tool schemas from the built-in tool executor
    pub fn get_all_tool_schemas(&self) -> Vec<crate::agent::core::tools::ToolSchema> {
        self.tools.list_tools()
    }

    /// Load MCP configuration from file
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_state_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf()).await;

        // Verify basic fields
        assert!(state.sessions.blocking_read().is_empty());
        assert!(!state.model_name.is_empty());
    }
}
