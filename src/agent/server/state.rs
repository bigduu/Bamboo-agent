use crate::metrics_service::MetricsService;
use agent_core::tools::ToolExecutor;
use agent_core::{storage::JsonlStorage, AgentEvent, Session};
use agent_llm::OpenAIProvider;
use agent_mcp::{CompositeToolExecutor, McpServerManager};
use agent_skill::{SkillManager, SkillStoreConfig};
use agent_tools::BuiltinToolExecutor;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_BASE_PROMPT: &str =
    "You are a helpful AI assistant with access to various tools and skills.";
pub const WORKSPACE_PROMPT_GUIDANCE: &str =
    "If you need to inspect files, check the workspace first, then ~/.bamboo.";

/// Status of an agent runner
#[derive(Debug, Clone)]
pub enum AgentStatus {
    Pending,
    Running,
    Completed,
    Cancelled,
    Error(String),
}

/// Runner that manages agent execution for a session
/// Supports multiple subscribers via broadcast channel
#[derive(Clone)]
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

pub struct AppState {
    pub sessions: Arc<RwLock<HashMap<String, Session>>>,
    pub storage: JsonlStorage,
    pub llm: Arc<dyn agent_llm::LLMProvider>,
    pub tools: Arc<dyn ToolExecutor>,
    pub cancel_tokens: Arc<RwLock<HashMap<String, tokio_util::sync::CancellationToken>>>,
    pub skill_manager: Arc<SkillManager>,
    pub mcp_manager: Arc<McpServerManager>,
    pub metrics_service: Arc<MetricsService>,
    pub model_name: String,
    /// Agent runners with broadcast channels for multi-subscriber support
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
}

impl AppState {
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
        if let Err(e) = chat_core::migrate_session_files() {
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

        let llm: Arc<dyn agent_llm::LLMProvider> = match provider {
            "copilot" => {
                log::info!("Using Copilot provider with Device Code authentication");

                // Create Copilot provider and authenticate
                let mut copilot_provider = if api_key != "sk-test" && !api_key.is_empty() {
                    // Use provided API key directly
                    agent_llm::CopilotProvider::with_token(api_key)
                } else {
                    // Use device code flow
                    agent_llm::CopilotProvider::new()
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
        let mcp_tools = Arc::new(agent_mcp::McpToolExecutor::new(
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
                                    runner.completed_at.unwrap_or(runner.started_at)
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

    pub async fn save_session(&self, session: &Session) {
        let _ = self.storage.save_session(session).await;
    }

    /// Get all tool schemas from the built-in tool executor
    pub fn get_all_tool_schemas(&self) -> Vec<agent_core::tools::ToolSchema> {
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

fn bamboo_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(".bamboo")
}

/// Load MCP configuration from file
async fn load_mcp_config(app_data_root: &std::path::Path) -> agent_mcp::McpConfig {
    let config_path = app_data_root.join("mcp.json");

    if !config_path.exists() {
        log::info!(
            "No MCP config file found at {:?}, using default",
            config_path
        );
        return agent_mcp::McpConfig::default();
    }

    match tokio::fs::read_to_string(&config_path).await {
        Ok(content) => match serde_json::from_str::<agent_mcp::McpConfig>(&content) {
            Ok(config) => {
                log::info!("Loaded MCP config with {} servers", config.servers.len());
                config
            }
            Err(e) => {
                log::error!("Failed to parse MCP config: {}", e);
                agent_mcp::McpConfig::default()
            }
        },
        Err(e) => {
            log::error!("Failed to read MCP config: {}", e);
            agent_mcp::McpConfig::default()
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
