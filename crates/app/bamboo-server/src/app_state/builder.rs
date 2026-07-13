use super::init::{
    build_connect_manager, build_provider_handles, build_schedule_manager, build_spawn_scheduler,
    init_mcp_manager, init_metrics_service, init_schedule_store, init_skill_manager, init_storage,
    load_permission_checker, spawn_session_map_cleanup_task,
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

        // Loud, unmissable startup signal: `plugin_trust.enforcement: off`
        // silently affects EVERY future `bamboo plugin install`/`update`
        // (URL sources skip the host allowlist, signature, and checksum
        // layers with no per-install flag needed — see
        // `bamboo_server::plugin_source`'s module docs), not just one
        // command invocation, so it gets its own warning here at boot in
        // addition to the per-install warning `fetch_manifest_bundle` logs
        // for each individual insecure install.
        if config.plugin_trust.enforcement_is_off() {
            tracing::warn!(
                "plugin_trust.enforcement is OFF — plugin installs from ANY URL are accepted \
                 without host/signature/checksum verification (config.json plugin_trust.enforcement)"
            );
        }

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
        let data_dir = bamboo_home_dir.clone();
        let (session_store, storage) = init_storage(&data_dir).await?;
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));

        // In-memory session cache (shared across handlers and background jobs).
        let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());

        // Embed the mailbox bus (broker) in-process unless an external one is
        // configured. Mutates `config.subagents.broker` to point at the loopback
        // bus BEFORE it is wrapped/read downstream, so ask_agent / deploy_agent /
        // cluster all wire to it — and a standalone `bamboo broker serve` is no
        // longer required for sub-agent dispatch. (Foundation for routing local
        // actors onto the bus.)
        let mut config = config;
        let embedded_broker = maybe_embed_broker(&mut config, &data_dir).await;

        let config = Arc::new(RwLock::new(config));

        // Wire the configured-default-workspace resolver into agent-core. This keeps
        // the dependency arrow pointing down (agent-core owns only the slot; the
        // server fills it). The closure reads the server's LIVE in-memory config —
        // not a fresh disk-reading Config::new(), which would diverge from the live
        // config and clobber the global env-var cache (#38). `try_read` never blocks
        // (the resolver is called from sync code, so a blocking read could deadlock);
        // on the rare write-lock contention it returns the last successfully-resolved
        // path so a session never transiently falls back to the process cwd.
        {
            let config_for_workspace = config.clone();
            let last_known: Arc<std::sync::Mutex<Option<PathBuf>>> =
                Arc::new(std::sync::Mutex::new(None));
            bamboo_agent_core::workspace_state::set_default_workspace_provider(Box::new(
                move || match config_for_workspace.try_read() {
                    Ok(cfg) => {
                        let path = cfg.get_default_work_area_path();
                        if let Ok(mut cache) = last_known.lock() {
                            *cache = path.clone();
                        }
                        path
                    }
                    Err(_) => last_known.lock().ok().and_then(|c| c.clone()),
                },
            ));
        }

        let permission_checker = load_permission_checker(&bamboo_home_dir).await;
        let notification_service = Arc::new(bamboo_notification::NotificationService::new(
            bamboo_home_dir.join("notification_preferences.json"),
        ));
        let session_watchers = super::watchers::SessionWatchers::new();
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
        // NOTE: the idle-eviction sweep for `agent_runners` is spawned below,
        // once `session_event_senders` also exists, so it can drop both maps'
        // entries for a completed session together (issue #346).

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

        // Long-lived session event senders map (UI subscriptions + background tasks).
        // Declared before `build_base_tools` (moved up from its original spot below)
        // because the `notify` tool overlaid there needs it to broadcast onto a
        // session's live channel — see `app_state::tools::build_base_tools`.
        let session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Shared bundle of always-on notification relay deps (see
        // `session_events::NotificationRelayDeps`). Built once and cloned into
        // every entry point that starts a relay directly at execution time —
        // the schedule manager, the root child-session adapter, and the
        // guardian child-session adapter below — so they can never drift.
        let notification_relay_deps = crate::app_state::session_events::NotificationRelayDeps {
            notification_service: notification_service.clone(),
            session_event_senders: session_event_senders.clone(),
            session_watchers: session_watchers.clone(),
            config: config.clone(),
        };

        let base_tools = build_base_tools(
            config.clone(),
            permission_checker.clone(),
            mcp_manager.clone(),
            skill_manager.clone(),
            storage.clone(),
            persistence.clone(),
            sessions.clone(),
            bamboo_home_dir.clone(),
            notification_service.clone(),
            session_event_senders.clone(),
            session_watchers.clone(),
        );

        // Idle-evict completed runners together with their paired session event
        // senders (issue #346). Spawned here (not next to `agent_runners`) so it
        // owns handles to both maps.
        spawn_session_map_cleanup_task(agent_runners.clone(), session_event_senders.clone(), None);

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
                    // No per-role tool allowlist is configured here yet, so the
                    // proxy is unrestricted (every role sees/can call all
                    // host-bound tools) — identical to the pre-#54 behavior.
                    // Wiring a config-driven allowlist in is a follow-up; the
                    // filtering mechanism (RoleToolAllowlist) is now in place.
                    let allowlist =
                        std::sync::Arc::new(bamboo_broker::RoleToolAllowlist::unrestricted());
                    bamboo_broker::serve_mcp_proxy_supervised(
                        &broker.endpoint,
                        me,
                        &broker.token,
                        backend,
                        allowlist,
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
            notification_relay_deps.clone(),
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

        // Config-write io-lock + the shared Remote Cluster Fabric deploy engine.
        // The engine is built once and shared by the HTTP handlers (via AppState)
        // and the `cluster` agent tool, so both use ONE worker registry.
        let config_io_lock = Arc::new(tokio::sync::Mutex::new(()));
        let fabric_registry: crate::tools::DeployedRegistry =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let fabric_bamboo_bin =
            std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));
        let fabric_deployer = Arc::new(bamboo_server_tools::FabricDeployer::new(
            config.clone(),
            config_io_lock.clone(),
            bamboo_home_dir.clone(),
            fabric_registry,
            fabric_bamboo_bin,
        ));
        // Cluster health monitor: periodically probe deployed workers on the bus and
        // flip node status live (Running↔Unreachable) + auto-recover. Server-scoped
        // — it runs under BOTH the embedded and an external broker (it reads the
        // broker endpoint lazily each tick), and is aborted when the server drops.
        let health_monitor = fabric_deployer
            .clone()
            .spawn_health_monitor()
            .await
            .map(HealthMonitor);

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
            fabric_deployer.clone(),
            notification_relay_deps.clone(),
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

        // bamboo-connect (#452 / epic #447): drives bamboo sessions from IM
        // platforms (Telegram first). Fully inert when `config.connect.platforms`
        // is empty — mirrors the schedule manager / notification relay's
        // always-constructed-but-only-active-if-configured lifecycle.
        let connect_manager = Arc::new(
            build_connect_manager(
                agent.clone(),
                tool_factory.get(crate::tools::ToolSurface::Root),
                session_repo.clone(),
                agent_runners.clone(),
                session_event_senders.clone(),
                Some(account_sink.inbox()),
                Some(data_dir.clone()),
                config.clone(),
                provider_registry.clone(),
                permission_checker.clone(),
            )
            .await,
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
            notification_relay: Some(notification_relay_deps.clone()),
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

        // Cluster-fabric reconcile: session-bound workers died with the previous
        // bamboo process (kill-on-drop child / in-memory russh session), so any
        // persisted `Running`/`Deploying` node state is stale on boot. Flip it to
        // `Unreachable` so the UI/agent see reality (a redeploy brings it back).
        reconcile_fabric_on_boot(&config, &bamboo_home_dir).await;

        Ok(Self {
            app_data_dir: bamboo_home_dir,
            config,
            config_io_lock,
            fabric_deployer,
            embedded_broker,
            health_monitor,
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
            connect_manager,
            tool_factory,
            permission_checker,
            notification_service,
            session_watchers,
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
            pairing_codes: Arc::new(dashmap::DashMap::new()),
            pairing_code_guard: Arc::new(crate::handlers::settings::PairingCodeGuard::default()),
            root_password_guard: Arc::new(crate::handlers::settings::RootPasswordGuard::default()),
            // remote-actor P2a (#181): empty in-memory agent registry.
        })
    }
}

/// A handle to the in-process mailbox bus (broker) so it can be shut down with
/// the server. Dropping it aborts the serve task.
pub struct EmbeddedBroker {
    task: tokio::task::JoinHandle<()>,
    gc_task: tokio::task::JoinHandle<()>,
}

impl Drop for EmbeddedBroker {
    fn drop(&mut self) {
        self.task.abort();
        self.gc_task.abort();
    }
}

/// Server-scoped handle to the cluster health monitor. Kept separate from
/// [`EmbeddedBroker`] so the monitor runs under BOTH the embedded broker and an
/// external (`broker.json`) one; aborts the sweep on drop.
pub struct HealthMonitor(tokio::task::JoinHandle<()>);

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Start an in-process broker on `127.0.0.1:<auto>` and point
/// `config.subagents.broker` (RUNTIME-ONLY, `#[serde(skip)]`) at it — UNLESS a
/// user-managed external broker is configured in `<data_dir>/broker.json` and
/// reachable (then use that). Returns `None` when an external broker is used or
/// the bind fails (sub-agent dispatch then degrades exactly as before).
///
/// The broker's endpoint deliberately lives in EITHER the in-memory config (for
/// the embedded case, regenerated each boot) or its own `broker.json` (for the
/// external case) — NEVER in `config.json`. That is what stops a prior run's
/// ephemeral auto-port from leaking into the user's config and being dialed dead
/// on the next boot (every sub-agent + the MCP proxy would hit "connect refused").
async fn maybe_embed_broker(
    config: &mut bamboo_llm::Config,
    data_dir: &std::path::Path,
) -> Option<EmbeddedBroker> {
    // A user-managed EXTERNAL broker (multi-host / shared standalone bus) lives in
    // its OWN file, `<data_dir>/broker.json` — separate from config.json. Honour
    // it ONLY if actually REACHABLE; a dead endpoint (standalone broker not up)
    // falls through to a fresh in-process broker so dispatch still works.
    if let Some(external) = load_external_broker(data_dir) {
        let endpoint = external.endpoint.trim().to_string();
        if broker_endpoint_reachable(&endpoint).await {
            tracing::info!(%endpoint, "using external broker from broker.json");
            config.subagents.broker = Some(external);
            return None;
        }
        tracing::warn!(
            %endpoint,
            "broker.json endpoint is unreachable — embedding a fresh in-process broker instead"
        );
    }

    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!("embedded broker: bind failed, sub-agent dispatch disabled: {e}");
            return None;
        }
    };
    let port = match listener.local_addr() {
        Ok(a) => a.port(),
        Err(e) => {
            tracing::warn!("embedded broker: local_addr failed: {e}");
            return None;
        }
    };
    let token = uuid::Uuid::new_v4().simple().to_string();
    let root = data_dir.join("broker");
    let core = Arc::new(bamboo_broker::BrokerCore::new(root));
    // Reclaim orphan mailbox dirs (one-shot parent links, killed pool workers)
    // every 5 min so `<data>/broker/mailboxes/` doesn't grow unbounded.
    let gc_task = core
        .clone()
        .spawn_mailbox_gc(std::time::Duration::from_secs(300));
    let server = Arc::new(bamboo_broker::BrokerServer::new(core, token.clone()));

    let task = tokio::spawn(async move {
        if let Err(e) = server.serve(listener).await {
            tracing::error!("embedded broker serve loop ended: {e}");
        }
    });

    // Set the endpoint IN MEMORY ONLY — `subagents.broker` is `#[serde(skip)]`, so
    // this ephemeral loopback port is regenerated every boot and never touches disk.
    config.subagents.broker = Some(bamboo_config::BrokerClientConfig {
        endpoint: format!("ws://127.0.0.1:{port}"),
        token,
        token_encrypted: None,
    });
    tracing::info!(port, "embedded mailbox bus (broker) started in-process");
    Some(EmbeddedBroker { task, gc_task })
}

/// Load a user-managed EXTERNAL broker from `<data_dir>/broker.json`, if present.
/// This file is the SEPARATE, persisted home for a standalone/remote broker —
/// deliberately NOT `config.json`, so the embedded broker's ephemeral runtime
/// port can never leak into the user's config (the stale-dead-port bug). An
/// absent file or a parse error yields `None` (embed a fresh in-process broker).
///
/// Format is a plain [`BrokerClientConfig`] JSON object, e.g.:
/// `{ "endpoint": "wss://broker.example:9600", "token": "…" }`.
fn load_external_broker(data_dir: &std::path::Path) -> Option<bamboo_config::BrokerClientConfig> {
    let path = data_dir.join("broker.json");
    let bytes = std::fs::read(&path).ok()?;
    match serde_json::from_slice::<bamboo_config::BrokerClientConfig>(&bytes) {
        Ok(cfg) if !cfg.endpoint.trim().is_empty() => Some(cfg),
        Ok(_) => {
            tracing::warn!(?path, "broker.json has an empty endpoint — ignoring");
            None
        }
        Err(e) => {
            tracing::warn!(?path, "broker.json present but unparseable: {e}");
            None
        }
    }
}

/// Best-effort TCP reachability probe of a `ws[s]://host:port[/path]` broker
/// endpoint. Used to tell a LIVE external/standalone broker (keep) apart from a
/// DEAD persisted endpoint (a prior run's embedded auto-port that leaked into
/// config.json — re-embed). A short timeout keeps boot fast when it's dead.
async fn broker_endpoint_reachable(endpoint: &str) -> bool {
    let host_port = endpoint
        .trim()
        .trim_start_matches("wss://")
        .trim_start_matches("ws://")
        .split('/')
        .next()
        .unwrap_or("");
    if host_port.is_empty() {
        return false;
    }
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            tokio::net::TcpStream::connect(host_port),
        )
        .await,
        Ok(Ok(_))
    )
}

/// On boot, mark stale cluster-fabric node state as `Unreachable`: workers
/// deployed by the previous process were session-bound (they died with it), so a
/// persisted `Running`/`Deploying` status no longer reflects reality. Best-effort
/// + persisted so the UI and `cluster status` don't show phantom-running nodes.
async fn reconcile_fabric_on_boot(
    config: &Arc<RwLock<bamboo_llm::Config>>,
    data_dir: &std::path::Path,
) {
    use bamboo_config::cluster_fabric::NodeStatus;

    let snapshot = {
        let mut cfg = config.write().await;
        let mut changed = 0usize;
        for node in &mut cfg.cluster_fabric.nodes {
            if let Some(state) = node.state.as_mut() {
                if matches!(state.status, NodeStatus::Running | NodeStatus::Deploying) {
                    state.status = NodeStatus::Unreachable;
                    state.last_error =
                        Some("orchestrator restarted; worker no longer tracked".to_string());
                    changed += 1;
                }
            }
        }
        if changed == 0 {
            return;
        }
        tracing::info!(
            reconciled = changed,
            "cluster-fabric: marked stale Running nodes Unreachable on boot"
        );
        cfg.clone()
    };

    if let Err(e) = snapshot.save_to_dir(data_dir.to_path_buf()) {
        tracing::warn!("cluster-fabric boot reconcile: failed to persist: {e}");
    }
}

#[cfg(test)]
mod broker_embed_tests {
    use super::{broker_endpoint_reachable, load_external_broker};

    #[test]
    fn load_external_broker_reads_broker_json_not_config() {
        let dir = tempfile::tempdir().unwrap();
        // Absent file ⇒ None (embed a fresh in-process broker).
        assert!(load_external_broker(dir.path()).is_none());

        // A well-formed broker.json ⇒ parsed external broker.
        std::fs::write(
            dir.path().join("broker.json"),
            r#"{ "endpoint": "wss://broker.example:9600", "token": "t" }"#,
        )
        .unwrap();
        let got = load_external_broker(dir.path()).expect("parsed");
        assert_eq!(got.endpoint, "wss://broker.example:9600");
        assert_eq!(got.token, "t");

        // Empty endpoint ⇒ ignored (treated as absent).
        std::fs::write(dir.path().join("broker.json"), r#"{ "endpoint": "  " }"#).unwrap();
        assert!(load_external_broker(dir.path()).is_none());

        // Garbage ⇒ ignored, never panics.
        std::fs::write(dir.path().join("broker.json"), "not json").unwrap();
        assert!(load_external_broker(dir.path()).is_none());
    }

    #[tokio::test]
    async fn reachability_probe_distinguishes_live_from_dead() {
        // Bound-then-dropped port: nothing listening ⇒ unreachable (a stale
        // persisted embedded endpoint must NOT be trusted).
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = l.local_addr().unwrap();
        drop(l);
        assert!(!broker_endpoint_reachable(&format!("ws://{dead}")).await);

        // Empty / malformed ⇒ never trusted.
        assert!(!broker_endpoint_reachable("").await);
        assert!(!broker_endpoint_reachable("ws://").await);

        // A LIVE listener (with a path) ⇒ reachable (a real external broker is kept).
        let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = live.local_addr().unwrap();
        assert!(broker_endpoint_reachable(&format!("ws://{addr}/stream")).await);
    }
}
