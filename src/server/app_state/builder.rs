use super::*;

type PermissionChecker = dyn crate::agent::tools::permission::PermissionChecker;

impl AppState {
    /// Create unified app state with direct provider access
    ///
    /// This eliminates the proxy pattern where we created an AgentAppState
    /// that called back to web_service via HTTP. Now we have direct provider access.
    ///
    /// # Arguments
    ///
    /// * `bamboo_home_dir` - Bamboo home directory containing all application data.
    ///                        This is the root directory (e.g., `${HOME}/.bamboo`) that contains:
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
    ///     let state = AppState::new(PathBuf::from("/path/to/bamboo-data-dir")).await;
    ///     let provider = state.get_provider().await;
    ///     let _models = provider.list_models().await.ok();
    /// }
    /// ```
    pub async fn new(bamboo_home_dir: PathBuf) -> Self {
        // Ensure all helpers that rely on `core::paths::bamboo_dir()` see the same
        // directory as the server runtime.
        crate::core::paths::init_bamboo_dir(bamboo_home_dir.clone());

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
        let (session_store, storage) = init_storage_components(&data_dir).await;

        // In-memory session cache (shared across handlers and background jobs).
        let sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let config = Arc::new(RwLock::new(config));

        let claude_cli_path: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
        spawn_claude_cli_discovery_task(claude_cli_path.clone());

        let permission_checker = load_permission_checker(&bamboo_home_dir).await;
        let mcp_manager = init_mcp_manager(config.clone());

        let skill_manager = init_skill_manager(&data_dir).await;
        let base_tools = build_base_tools(
            config.clone(),
            permission_checker,
            mcp_manager.clone(),
            skill_manager.clone(),
            storage.clone(),
            sessions.clone(),
        );
        let metrics_service = init_metrics_service(&data_dir).await;

        let agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));
        spawn_runner_cleanup_task(agent_runners.clone(), None);

        let claude_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));
        spawn_runner_cleanup_task(claude_runners.clone(), Some("claude"));

        // Initialize process registry (external process lifecycle)
        let process_registry = Arc::new(ProcessRegistry::new());

        let (provider_lock, provider_handle) = build_provider_handles(provider);

        // Long-lived session event senders map (UI subscriptions + background tasks).
        let session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Child tools intentionally do not expose `Task` (no nested child spawns).
        let child_tools: Arc<dyn ToolExecutor> = base_tools.clone();

        // Initialize sub-session spawn scheduler (async background jobs).
        let spawn_scheduler = build_spawn_scheduler(
            session_store.clone(),
            storage.clone(),
            provider_handle.clone(),
            child_tools.clone(),
            config.clone(),
            skill_manager.clone(),
            metrics_service.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
        );

        let tools_with_task = build_tools_with_task(
            base_tools,
            session_store.clone(),
            storage.clone(),
            spawn_scheduler.clone(),
            session_event_senders.clone(),
        );

        let schedule_store = init_schedule_store(&data_dir).await;
        let schedule_manager = build_schedule_manager(
            schedule_store.clone(),
            session_store.clone(),
            storage.clone(),
            provider_handle.clone(),
            tools_with_task.clone(),
            skill_manager.clone(),
            metrics_service.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
            config.clone(),
        );

        let tools = build_root_tools(
            tools_with_task,
            schedule_store.clone(),
            schedule_manager.clone(),
            session_store.clone(),
            storage.clone(),
            spawn_scheduler.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
        );

        Self {
            app_data_dir: bamboo_home_dir,
            config,
            provider: provider_lock,
            provider_handle,
            sessions,
            storage,
            session_store,
            spawn_scheduler,
            schedule_store,
            schedule_manager,
            tools,
            child_tools,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            skill_manager,
            mcp_manager,
            metrics_service,
            agent_runners,
            session_event_senders,
            process_registry,
            claude_cli_path,
            claude_runners,
            claude_session_aliases: Arc::new(RwLock::new(HashMap::new())),
            metrics_bus: None, // Will be set by server if needed
        }
    }
}

async fn init_storage_components(data_dir: &PathBuf) -> (Arc<SessionStoreV2>, Arc<dyn Storage>) {
    log::info!("Initializing session store V2 at: {:?}", data_dir);
    let session_store = Arc::new(
        SessionStoreV2::new(data_dir.clone())
            .await
            .unwrap_or_else(|e| {
                log::error!("Failed to init SessionStoreV2 at {:?}: {}", data_dir, e);
                panic!("Failed to init SessionStoreV2: {}", e);
            }),
    );
    let storage: Arc<dyn Storage> = session_store.clone();
    log::info!(
        "Session store V2 initialized (index: {:?}, sessions: {:?})",
        session_store.index_path(),
        session_store.sessions_root_dir()
    );
    (session_store, storage)
}

async fn load_permission_checker(bamboo_home_dir: &PathBuf) -> Arc<PermissionChecker> {
    let storage = crate::agent::tools::permission::storage::PermissionStorage::new(bamboo_home_dir);
    let permission_config = match storage.load().await {
        Ok(Some(config)) => config,
        Ok(None) => {
            let cfg = crate::agent::tools::permission::PermissionConfig::new();
            cfg.set_enabled(false);
            cfg
        }
        Err(error) => {
            log::warn!("Failed to load permission config; defaulting to disabled: {error}");
            let cfg = crate::agent::tools::permission::PermissionConfig::new();
            cfg.set_enabled(false);
            cfg
        }
    };
    permission_config.cleanup_expired_grants();
    Arc::new(
        crate::agent::tools::permission::ConfigPermissionChecker::new(Arc::new(permission_config)),
    )
}

fn spawn_claude_cli_discovery_task(claude_cli_path: Arc<RwLock<Option<String>>>) {
    // Optional integration: discover Claude Code CLI in the background so server startup
    // is not blocked by PATH scanning / process invocations (e.g. `claude --version`).
    tokio::spawn(async move {
        let discovered = tokio::task::spawn_blocking(crate::claude::try_find_claude_binary)
            .await
            .ok()
            .flatten();

        if let Some(path) = discovered {
            *claude_cli_path.write().await = Some(path.clone());
            log::info!("Claude Code CLI discovered (found at: {})", path);
        } else {
            log::warn!("Claude Code CLI not found; Claude integration disabled");
        }
    });
}

fn init_mcp_manager(config: Arc<RwLock<Config>>) -> Arc<McpServerManager> {
    // Initialize MCP manager (needs access to config to respect proxy for SSE transports).
    let mcp_manager = Arc::new(McpServerManager::new_with_config(config.clone()));

    // Initialize MCP servers in the background so the HTTP API is responsive quickly.
    {
        let mcp_manager = mcp_manager.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mcp_config = config.read().await.mcp.clone();
            mcp_manager.initialize_from_config(&mcp_config).await;
        });
    }

    mcp_manager
}

fn build_base_tools(
    config: Arc<RwLock<Config>>,
    permission_checker: Arc<PermissionChecker>,
    mcp_manager: Arc<McpServerManager>,
    skill_manager: Arc<SkillManager>,
    storage: Arc<dyn Storage>,
    sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,
) -> Arc<dyn ToolExecutor> {
    // Initialize built-in tools with permission checks.
    // If no permission config has been persisted yet, keep checks disabled for backward
    // compatibility and opt-in behavior.
    let builtin_executor = Arc::new(
        crate::agent::tools::BuiltinToolExecutor::new_with_config_and_permissions(
            config,
            permission_checker,
        ),
    );
    let builtin_tools: Arc<dyn ToolExecutor> = builtin_executor;

    // Create composite tool executor (builtin + MCP)
    let mcp_tools = Arc::new(crate::agent::mcp::McpToolExecutor::new(
        mcp_manager.clone(),
        mcp_manager.tool_index(),
    ));

    let base: Arc<dyn ToolExecutor> = Arc::new(crate::agent::mcp::CompositeToolExecutor::new(
        builtin_tools,
        mcp_tools,
    ));

    let load_skill_tool = Arc::new(crate::server::tools::LoadSkillTool::new(
        skill_manager.clone(),
        sessions.clone(),
        storage.clone(),
    ));
    let with_load_skill: Arc<dyn ToolExecutor> = Arc::new(
        crate::server::tools::OverlayToolExecutor::new(base, load_skill_tool),
    );

    let read_skill_resource_tool = Arc::new(crate::server::tools::ReadSkillResourceTool::new(
        skill_manager,
        sessions,
        storage,
    ));
    Arc::new(crate::server::tools::OverlayToolExecutor::new(
        with_load_skill,
        read_skill_resource_tool,
    ))
}

async fn init_skill_manager(data_dir: &PathBuf) -> Arc<SkillManager> {
    // Initialize skill manager
    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: data_dir.join("skills"),
    }));
    if let Err(error) = skill_manager.initialize().await {
        log::warn!("Failed to initialize skill manager: {}", error);
    }
    skill_manager
}

async fn init_metrics_service(data_dir: &PathBuf) -> Arc<MetricsService> {
    // Initialize metrics service
    Arc::new(
        MetricsService::new(data_dir.join("metrics.db"))
            .await
            .unwrap_or_else(|error| {
                log::error!("Failed to initialize metrics storage: {}", error);
                panic!("Failed to init metrics storage: {}", error);
            }),
    )
}

fn spawn_runner_cleanup_task(
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    log_prefix: Option<&'static str>,
) {
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
                    if let Some(prefix) = log_prefix {
                        log::debug!("[{}:{}] Cleaning up completed runner", prefix, session_id);
                    } else {
                        log::debug!("[{}] Cleaning up completed runner", session_id);
                    }
                }

                should_keep
            });
        }
    });
}

fn build_provider_handles(
    provider: Arc<dyn LLMProvider>,
) -> (Arc<RwLock<Arc<dyn LLMProvider>>>, Arc<dyn LLMProvider>) {
    let provider_lock: Arc<RwLock<Arc<dyn LLMProvider>>> = Arc::new(RwLock::new(provider));
    let provider_handle: Arc<dyn LLMProvider> = Arc::new(
        crate::server::reloadable_provider::ReloadableProvider::new(provider_lock.clone()),
    );
    (provider_lock, provider_handle)
}

fn build_spawn_scheduler(
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    provider_handle: Arc<dyn LLMProvider>,
    child_tools: Arc<dyn ToolExecutor>,
    config: Arc<RwLock<Config>>,
    skill_manager: Arc<SkillManager>,
    metrics_service: Arc<MetricsService>,
    sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
) -> Arc<SpawnScheduler> {
    Arc::new(SpawnScheduler::new(SpawnContext {
        session_store,
        storage,
        provider: provider_handle,
        tools: child_tools,
        config,
        skill_manager,
        metrics_collector: metrics_service.collector(),
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
    }))
}

fn build_tools_with_task(
    base_tools: Arc<dyn ToolExecutor>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    spawn_scheduler: Arc<SpawnScheduler>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
) -> Arc<dyn ToolExecutor> {
    // Root tools include `Task` via a lightweight overlay executor.
    let spawn_tool = Arc::new(crate::server::tools::SpawnSessionTool::new(
        session_store,
        storage,
        spawn_scheduler,
        session_event_senders,
    ));

    Arc::new(crate::server::tools::OverlayToolExecutor::new(
        base_tools, spawn_tool,
    ))
}

async fn init_schedule_store(data_dir: &PathBuf) -> Arc<ScheduleStore> {
    // Initialize schedule store + manager (timed tasks).
    Arc::new(
        ScheduleStore::new(data_dir.clone())
            .await
            .unwrap_or_else(|e| {
                log::error!("Failed to init ScheduleStore at {:?}: {}", data_dir, e);
                panic!("Failed to init ScheduleStore: {}", e);
            }),
    )
}

fn build_schedule_manager(
    schedule_store: Arc<ScheduleStore>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    provider_handle: Arc<dyn LLMProvider>,
    tools_for_schedules: Arc<dyn ToolExecutor>,
    skill_manager: Arc<SkillManager>,
    metrics_service: Arc<MetricsService>,
    sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    config: Arc<RwLock<Config>>,
) -> Arc<ScheduleManager> {
    // Schedule jobs should not automatically inherit schedule-management tools; keep the tool
    // surface minimal for background automation unless explicitly needed later.
    Arc::new(ScheduleManager::new(ScheduleContext {
        schedule_store,
        session_store,
        storage,
        provider: provider_handle,
        tools: tools_for_schedules,
        skill_manager,
        metrics_collector: metrics_service.collector(),
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
        config,
    }))
}

fn build_root_tools(
    tools_with_task: Arc<dyn ToolExecutor>,
    schedule_store: Arc<ScheduleStore>,
    schedule_manager: Arc<ScheduleManager>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    spawn_scheduler: Arc<SpawnScheduler>,
    sessions: Arc<RwLock<HashMap<String, crate::agent::core::Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
) -> Arc<dyn ToolExecutor> {
    // Root sessions can manage schedules via `schedule_tasks`.
    // Background schedule runs intentionally use `tools_for_schedules` above and therefore
    // do not get this management tool by default.
    let schedule_tasks_tool = Arc::new(crate::server::tools::ScheduleTasksTool::new(
        schedule_store,
        schedule_manager,
        session_store.clone(),
        storage.clone(),
    ));
    let tools_with_schedule: Arc<dyn ToolExecutor> = Arc::new(
        crate::server::tools::OverlayToolExecutor::new(tools_with_task, schedule_tasks_tool),
    );

    let sub_session_manager_tool = Arc::new(crate::server::tools::SubSessionManagerTool::new(
        session_store.clone(),
        storage.clone(),
        spawn_scheduler,
        sessions,
        agent_runners,
        session_event_senders,
    ));
    let tools_with_sub_session_manager: Arc<dyn ToolExecutor> =
        Arc::new(crate::server::tools::OverlayToolExecutor::new(
            tools_with_schedule,
            sub_session_manager_tool,
        ));

    let session_inspector_tool = Arc::new(crate::server::tools::SessionInspectorTool::new(
        session_store,
        storage,
    ));

    Arc::new(crate::server::tools::OverlayToolExecutor::new(
        tools_with_sub_session_manager,
        session_inspector_tool,
    ))
}
