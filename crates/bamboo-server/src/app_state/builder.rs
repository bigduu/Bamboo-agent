use super::init::{
    build_provider_handles, build_schedule_manager, build_spawn_scheduler, init_mcp_manager,
    init_metrics_service, init_schedule_store, init_skill_manager, init_storage,
    load_permission_checker, spawn_runner_cleanup_task,
};
use super::tools::{build_base_tools, build_root_tools};
use super::*;

impl AppState {
    /// Create unified app state with direct provider access
    ///
    /// This eliminates the proxy pattern where we created an AgentAppState
    /// that called back to web_service via HTTP. Now we have direct provider access.
    ///
    /// # Arguments
    ///
    /// * `bamboo_home_dir` - Bamboo home directory containing all application data.
    ///   This is the root directory (e.g., `${HOME}/.bamboo`) that contains:
    ///   - config.json: Configuration file
    ///   - sessions/: Conversation history
    ///   - skills/: Skill definitions
    ///   - workflows/: Workflow definitions
    ///   - cache/: Cached data
    ///   - runtime/: Runtime files
    ///
    /// # Returns
    ///
    /// A fully initialized AppState with all components ready for use.
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/bamboo-data-dir"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///     let provider = state.get_provider().await;
    ///     let _models = provider.list_models().await.ok();
    /// }
    /// ```
    pub async fn new(bamboo_home_dir: PathBuf) -> Result<Self, AppError> {
        // Ensure all helpers that rely on `core::paths::bamboo_dir()` see the same
        // directory as the server runtime.
        bamboo_infrastructure::paths::init_bamboo_dir(bamboo_home_dir.clone());

        // Load config from the specified data directory
        let config = Config::from_data_dir(Some(bamboo_home_dir.clone()));

        // Create provider with direct access (no HTTP proxy)
        let provider =
            match bamboo_infrastructure::create_provider_with_dir(&config, bamboo_home_dir.clone())
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    // Keep the server usable for configuration/UI even when provider init fails,
                    // but do not silently fall back to a different provider.
                    tracing::error!("Failed to create provider: {}.", e);
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
    pub async fn new_with_provider(
        bamboo_home_dir: PathBuf,
        config: Config,
        provider: Arc<dyn LLMProvider>,
    ) -> Result<Self, AppError> {
        let data_dir = bamboo_home_dir.clone();
        let (session_store, storage) = init_storage(&data_dir).await?;

        // In-memory session cache (shared across handlers and background jobs).
        let sessions: Arc<RwLock<HashMap<String, bamboo_agent_core::Session>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let config = Arc::new(RwLock::new(config));

        let permission_checker = load_permission_checker(&bamboo_home_dir).await;
        let mcp_manager = init_mcp_manager(config.clone());
        let skill_manager = init_skill_manager(&data_dir).await;
        let metrics_service = init_metrics_service(&data_dir).await?;

        let agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));
        spawn_runner_cleanup_task(agent_runners.clone(), None);

        let process_registry = Arc::new(ProcessRegistry::new());
        let (provider_lock, provider_handle) = build_provider_handles(provider);

        // Initialize multi-provider registry (for features.provider_model_ref).
        let config_snapshot = config.read().await;
        let provider_registry = match bamboo_infrastructure::ProviderRegistry::from_config(
            &config_snapshot,
            bamboo_home_dir.clone(),
        )
        .await
        {
            Ok(registry) => Arc::new(registry),
            Err(e) => {
                tracing::error!("Failed to create provider registry: {}", e);
                Arc::new(
                    bamboo_infrastructure::ProviderRegistry::from_config(
                        &Config::default(),
                        bamboo_home_dir.clone(),
                    )
                    .await
                    .expect("Cannot create even an empty provider registry"),
                )
            }
        };
        drop(config_snapshot);

        let provider_router = Arc::new(bamboo_infrastructure::ProviderModelRouter::new(
            provider_registry.clone(),
        ));
        let model_catalog = Arc::new(bamboo_infrastructure::ModelCatalogService::new(
            provider_registry.clone(),
        ));

        let base_tools = build_base_tools(
            config.clone(),
            permission_checker,
            mcp_manager.clone(),
            skill_manager.clone(),
            storage.clone(),
            sessions.clone(),
            bamboo_home_dir.clone(),
        );

        // Long-lived session event senders map (UI subscriptions + background tasks).
        let session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Child tools intentionally do not expose `SubSession` (no nested child spawns).
        let child_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = base_tools.clone();

        // Unified agent runtime (shared resources for all execution paths).
        // default_tools = base_tools (builtin + MCP + memory + skills).
        // Root sessions use runtime default (no override);
        // child sessions override via ExecuteRequest with ToolSurface::Child.
        let agent = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .attachment_reader(session_store.clone())
                .skill_manager(skill_manager.clone())
                .metrics_collector(metrics_service.collector())
                .config(config.clone())
                .provider(provider_handle.clone())
                .default_tools(base_tools.clone())
                .build()
                .expect("agent runtime should be fully configured"),
        );

        // Initialize sub-session spawn scheduler (async background jobs).
        let spawn_scheduler = build_spawn_scheduler(
            agent.clone(),
            child_tools,
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
        );

        let tools_with_task = base_tools.clone();

        let schedule_store = init_schedule_store(&data_dir).await?;
        let schedule_manager = build_schedule_manager(
            schedule_store.clone(),
            agent.clone(),
            tools_with_task.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
            config.clone(),
        );

        crate::services::auto_dream::spawn_auto_dream_task(
            crate::services::auto_dream::AutoDreamContext {
                session_store: session_store.clone(),
                storage: storage.clone(),
                provider: provider_handle.clone(),
                config: config.clone(),
                provider_registry: provider_registry.clone(),
            },
        );

        let config_for_resolver = config.clone();
        let subagent_model_resolver: Option<Arc<dyn Fn(&str) -> Option<String> + Send + Sync>> = {
            let config_snap = config.read().await.clone();
            let registry = provider_registry.clone();
            if config_snap.features.provider_model_ref && config_snap.defaults.is_some() {
                Some(Arc::new(move |subagent_type: &str| -> Option<String> {
                    let config_snap = config_for_resolver.blocking_read().clone();
                    crate::model_config_helper::resolve_subagent_model(
                        &config_snap,
                        &config_snap.provider,
                        &registry,
                        subagent_type,
                    )
                    .map(|m| m.model_name)
                }))
            } else {
                None
            }
        };

        let tools = build_root_tools(
            tools_with_task.clone(),
            schedule_store.clone(),
            schedule_manager.clone(),
            session_store.clone(),
            storage.clone(),
            spawn_scheduler.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
            subagent_model_resolver,
        );

        let tool_factory =
            crate::tools::ToolSurfaceFactory::new(base_tools, tools_with_task, tools);

        Ok(Self {
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
            tool_factory,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            skill_manager,
            mcp_manager,
            metrics_service,
            agent_runners,
            session_event_senders,
            process_registry,
            metrics_bus: None, // Will be set by server if needed
            agent,
            provider_registry,
            provider_router,
            model_catalog,
        })
    }
}
