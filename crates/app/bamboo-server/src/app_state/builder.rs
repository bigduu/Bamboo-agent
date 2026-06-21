use super::init::{
    build_provider_handles, build_schedule_manager, build_spawn_scheduler, init_mcp_manager,
    init_metrics_service, init_schedule_store, init_skill_manager, init_storage,
    load_permission_checker, spawn_runner_cleanup_task,
};
use super::tools::{build_base_tools, build_root_tools};
use super::*;
use crate::tools::OptionalSubagentModelResolver;
use bamboo_agent_core::storage::Storage;

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
        bamboo_config::paths::init_bamboo_dir(bamboo_home_dir.clone());

        // Load config from the specified data directory
        let config = Config::from_data_dir(Some(bamboo_home_dir.clone()));

        let provider_registry =
            match bamboo_llm::ProviderRegistry::from_config(&config, bamboo_home_dir.clone()).await
            {
                Ok(registry) => Arc::new(registry),
                Err(e) => {
                    tracing::error!("Failed to create provider registry: {}", e);
                    Arc::new(
                        bamboo_llm::ProviderRegistry::from_config(
                            &Config::default(),
                            bamboo_home_dir.clone(),
                        )
                        .await
                        .expect("Cannot create even an empty provider registry"),
                    )
                }
            };

        let provider = provider_registry.get_default().unwrap_or_else(|| {
            let default_provider_name = provider_registry.default_provider_name();
            let message = if config.has_provider_instances() {
                format!(
                    "Default provider instance '{}' is not available or failed to initialize",
                    default_provider_name
                )
            } else {
                format!(
                    "Provider '{}' is not available or failed to initialize",
                    config.provider
                )
            };
            Arc::new(UnconfiguredProvider { message }) as Arc<dyn LLMProvider>
        });

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
        // Wire the configured-default-workspace resolver into agent-core. This keeps
        // the dependency arrow pointing down (agent-core no longer imports the
        // infrastructure config crate); the server owns config and provides it here.
        bamboo_agent_core::workspace_state::set_default_workspace_provider(|| {
            bamboo_llm::Config::from_data_dir(Some(bamboo_config::paths::bamboo_dir()))
                .get_default_work_area_path()
        });

        let data_dir = bamboo_home_dir.clone();
        let (session_store, storage) = init_storage(&data_dir).await?;
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));

        // In-memory session cache (shared across handlers and background jobs).
        let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());

        let config = Arc::new(RwLock::new(config));

        let permission_checker = load_permission_checker(&bamboo_home_dir).await;
        let notification_service = Arc::new(bamboo_notification::NotificationService::new(
            bamboo_home_dir.join("notification_preferences.json"),
        ));
        let mcp_manager = init_mcp_manager(config.clone());
        let skill_manager = init_skill_manager(&data_dir).await;
        let metrics_service = init_metrics_service(&data_dir).await?;

        let startup_sessions = {
            let entries = session_store.list_index_entries().await;
            let mut sessions = Vec::new();
            for entry in entries {
                if let Some(session) = session_store
                    .load_session(&entry.id)
                    .await
                    .map_err(AppError::StorageError)?
                {
                    sessions.push(session);
                }
            }
            sessions
        };
        metrics_service
            .reconcile_startup_sessions(startup_sessions, &[])
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reconcile stale metrics state on startup: {error}"
                ))
            })?;

        let agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>> =
            Arc::new(RwLock::new(HashMap::new()));
        spawn_runner_cleanup_task(agent_runners.clone(), None);

        let process_registry = Arc::new(ProcessRegistry::new());
        let (provider_lock, provider_handle) = build_provider_handles(provider);

        // Initialize multi-provider registry (for features.provider_model_ref).
        let config_snapshot = config.read().await;
        let provider_registry = match bamboo_llm::ProviderRegistry::from_config(
            &config_snapshot,
            bamboo_home_dir.clone(),
        )
        .await
        {
            Ok(registry) => Arc::new(registry),
            Err(e) => {
                tracing::error!("Failed to create provider registry: {}", e);
                Arc::new(
                    bamboo_llm::ProviderRegistry::from_config(
                        &Config::default(),
                        bamboo_home_dir.clone(),
                    )
                    .await
                    .expect("Cannot create even an empty provider registry"),
                )
            }
        };
        drop(config_snapshot);

        let provider_router = Arc::new(bamboo_llm::ProviderModelRouter::new(
            provider_registry.clone(),
        ));
        let model_catalog = Arc::new(bamboo_llm::ModelCatalogService::new(
            provider_registry.clone(),
        ));

        let base_tools = build_base_tools(
            config.clone(),
            permission_checker.clone(),
            mcp_manager.clone(),
            skill_manager.clone(),
            storage.clone(),
            persistence.clone(),
            sessions.clone(),
            bamboo_home_dir.clone(),
        );

        // Long-lived session event senders map (UI subscriptions + background tasks).
        let session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Account-scoped durable change feed. Opening the journal recovers the
        // max seq so the sequence counter stays monotonic across restarts.
        let account_sink = bamboo_engine::events::AccountEventSink::new(data_dir.join("events"))
            .map_err(|e| {
                AppError::InternalError(anyhow::anyhow!(
                    "failed to initialize account change-feed journal: {e}"
                ))
            })?;

        // Sub-agents are full agents with the full toolset (no per-role tool
        // trimming): the child tool surface is the plain base tools.
        let child_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = base_tools.clone();

        // Unified agent runtime (shared resources for all execution paths).
        // default_tools = base_tools (builtin + MCP + memory + skills) as a safe fallback.
        // Interactive execution paths pass an explicit tool surface override:
        // root sessions use ToolSurface::Root; child sessions use ToolSurface::Child.
        let agent = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .persistence(persistence.clone())
                .attachment_reader(session_store.clone())
                .skill_manager(skill_manager.clone())
                .metrics_collector(metrics_service.collector())
                .config(config.clone())
                .provider(provider_handle.clone())
                .default_tools(base_tools.clone())
                .build()
                .expect("agent runtime should be fully configured"),
        );

        let child_completion_coordinator =
            Arc::new(bamboo_engine::ChildCompletionCoordinator::new(
                storage.clone(),
                persistence.clone(),
                sessions.clone(),
                agent_runners.clone(),
                session_event_senders.clone(),
                agent.clone(),
                config.clone(),
                provider_registry.clone(),
                provider_router.clone(),
                data_dir.clone(),
                Some(account_sink.inbox()),
            ));

        // Initialize sub-session spawn scheduler (async background jobs).
        let config_snapshot = config.read().await.clone();

        // When a broker is configured, run the MCP proxy service under a
        // supervisor (issue #47): deployed workers forward their (host-bound)
        // MCP tool calls here, and we execute them against this orchestrator's
        // real MCP servers (single MCP host). The supervisor restarts the proxy
        // with bounded backoff after a transient WebSocket drop instead of
        // permanently disabling proxied tools for the worker's lifetime.
        let mcp_proxy_shutdown = tokio_util::sync::CancellationToken::new();
        if let Some(broker) = config_snapshot.subagents.broker.clone() {
            if !broker.endpoint.trim().is_empty() {
                let backend: std::sync::Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
                    std::sync::Arc::new(bamboo_mcp::executor::McpToolExecutor::new(
                        mcp_manager.clone(),
                        mcp_manager.tool_index(),
                    ));
                let shutdown = mcp_proxy_shutdown.clone();
                tokio::spawn(async move {
                    let me = bamboo_broker::AgentRef {
                        session_id: bamboo_broker::ORCHESTRATOR_ID.to_string(),
                        role: Some("orchestrator".into()),
                    };
                    bamboo_broker::serve_mcp_proxy_supervised(
                        &broker.endpoint,
                        me,
                        &broker.token,
                        backend,
                        shutdown,
                    )
                    .await;
                });
            }
        }
        let external_runner =
            bamboo_engine::external_agents::runtime::build_external_child_runner(&config_snapshot);
        let spawn_scheduler = build_spawn_scheduler(
            agent.clone(),
            child_tools,
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
            external_runner,
            Some(provider_router.clone()),
            Some(child_completion_coordinator.clone()),
            Some(data_dir.clone()),
            Some(account_sink.inbox()),
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
            persistence.clone(),
            config.clone(),
            provider_registry.clone(),
            Some(data_dir.clone()),
            Some(account_sink.inbox()),
        );

        bamboo_engine::auto_dream::spawn_auto_dream_task(
            bamboo_engine::auto_dream::AutoDreamContext {
                session_store: session_store.clone(),
                storage: storage.clone(),
                provider: provider_handle.clone(),
                config: config.clone(),
                provider_registry: provider_registry.clone(),
            },
        );

        // Background memory "gardener": opt-in blob remediation + near-duplicate
        // consolidation. No-op cost unless `memory.gardener_enabled` /
        // `memory.dedup_gardener_enabled` is set; an empty prefilter makes zero LLM calls.
        bamboo_engine::gardener::spawn_gardener_task(bamboo_engine::auto_dream::AutoDreamContext {
            session_store: session_store.clone(),
            storage: storage.clone(),
            provider: provider_handle.clone(),
            config: config.clone(),
            provider_registry: provider_registry.clone(),
        });

        let config_for_resolver = config.clone();
        let subagent_model_resolver: OptionalSubagentModelResolver = {
            let registry = provider_registry.clone();
            Some(Arc::new(
                move |subagent_type: String| -> futures::future::BoxFuture<
                    'static,
                    Option<bamboo_domain::ProviderModelRef>,
                > {
                    let config_for_resolver = config_for_resolver.clone();
                    let registry = registry.clone();
                    Box::pin(async move {
                        let config_snap = config_for_resolver.read().await.clone();
                        bamboo_engine::model_config_helper::resolve_subagent_model_ref(
                            &config_snap,
                            &config_snap.provider,
                            &registry,
                            &subagent_type,
                        )
                    })
                },
            ))
        };

        let tools = build_root_tools(
            tools_with_task.clone(),
            schedule_store.clone(),
            schedule_manager.clone(),
            session_store.clone(),
            storage.clone(),
            persistence.clone(),
            spawn_scheduler.clone(),
            sessions.clone(),
            agent_runners.clone(),
            session_event_senders.clone(),
            subagent_model_resolver,
            config.clone(),
            provider_registry.clone(),
            config_snapshot.subagents.broker.clone(),
        );

        child_completion_coordinator
            .set_root_tools(tools.clone())
            .await;

        let tool_factory =
            crate::tools::ToolSurfaceFactory::new(base_tools, tools_with_task, tools);

        let session_repo = bamboo_engine::SessionRepository::new(
            sessions.clone(),
            storage.clone(),
            persistence.clone(),
        );

        // Dedicated child-session adapter backing the guardian review spawner.
        // The guardian path passes an explicit model (no subagent_type routing)
        // and registers its parent wait at the terminal gate (not via the
        // adapter's coalescing slots), so a lightweight adapter with no resolver
        // and a fresh wait-slot map suffices. `Arc<ChildSessionAdapter>` doubles
        // as `Arc<dyn GuardianSpawner>`.
        let child_adapter = Arc::new(crate::tools::ChildSessionAdapter {
            session_store: session_store.clone(),
            storage: storage.clone(),
            persistence: persistence.clone(),
            scheduler: spawn_scheduler.clone(),
            sessions_cache: sessions.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
            subagent_model_resolver: None,
            config: config.clone(),
            parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        });
        let guardian_spawner: Arc<dyn bamboo_engine::GuardianSpawner> = child_adapter.clone();
        // Wire the spawner into the completion coordinator too, so a resumed run
        // can re-spawn a guardian to re-review a fix after a reject verdict.
        child_completion_coordinator
            .set_guardian_spawner(guardian_spawner.clone())
            .await;

        // The completion coordinator doubles as the bash self-resume hook
        // (issue #84 Phase 2b): it polls the live shell registry and resumes a
        // session once all its background bash shells finish.
        let bash_resume_hook: Arc<dyn bamboo_engine::BashResumeHook> =
            child_completion_coordinator.clone();

        Ok(Self {
            app_data_dir: bamboo_home_dir,
            config,
            provider: provider_lock,
            provider_handle,
            sessions,
            storage,
            session_store,
            session_repo,
            persistence,
            spawn_scheduler,
            child_completion_coordinator,
            guardian_spawner,
            bash_resume_hook,
            schedule_store,
            schedule_manager,
            tool_factory,
            permission_checker,
            notification_service,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            mcp_proxy_shutdown,
            skill_manager,
            mcp_manager,
            metrics_service,
            agent_runners,
            session_event_senders,
            account_sink,
            process_registry,
            metrics_bus: None, // Will be set by server if needed
            agent,
            provider_registry,
            provider_router,
            model_catalog,
            title_gen_in_flight: Arc::new(dashmap::DashSet::new()),
        })
    }
}
