//! Tool assembly functions for building the layered tool surface.
//!
//! These functions compose the tool executor chain:
//! ```text
//! base_tools (builtin + MCP + memory + skills + compact_context)
//!   └─> root_tools (base + SubAgent + scheduler + session_history)
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::AgentEvent;
use bamboo_llm::Config;
use bamboo_mcp::manager::McpServerManager;
use bamboo_plugin_protocol::ToolEventPublisher;
use bamboo_skills::SkillManager;
use bamboo_storage::LockedSessionStore;
use bamboo_storage::SessionStoreV2;

use super::init::PermissionChecker;
use super::watchers::SessionWatchers;
use super::{AgentRunner, ScheduleManager, ScheduleStore, SpawnScheduler};

#[allow(clippy::too_many_arguments)]
pub(super) fn build_base_tools(
    config: Arc<RwLock<Config>>,
    permission_checker: Arc<PermissionChecker>,
    mcp_manager: Arc<McpServerManager>,
    skill_manager: Arc<SkillManager>,
    session_repo: bamboo_engine::SessionRepository,
    app_data_dir: PathBuf,
    notification_service: Arc<bamboo_notification::NotificationService>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_watchers: Arc<SessionWatchers>,
    ledger_schedule_bridge: Arc<crate::schedule_app::LateBoundLedgerBridge>,
    project_store: Arc<bamboo_projects::ProjectStore>,
    account_sink: Arc<bamboo_engine::events::AccountEventSink>,
    workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    tool_event_publisher: Arc<dyn ToolEventPublisher>,
) -> Arc<dyn ToolExecutor> {
    // Initialize built-in tools with permission checks.
    // If no permission config has been persisted yet, keep checks disabled for backward
    // compatibility and opt-in behavior.
    let builtin_executor = Arc::new(
        bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
            config.clone(),
            permission_checker.clone(),
        )
        .with_tool_event_publisher(tool_event_publisher),
    );
    let builtin_tools: Arc<dyn ToolExecutor> = builtin_executor;

    // Create composite tool executor (builtin + MCP)
    let mcp_tools = Arc::new(bamboo_mcp::executor::McpToolExecutor::new(
        mcp_manager.clone(),
        mcp_manager.tool_index(),
    ));

    let base: Arc<dyn ToolExecutor> = Arc::new(bamboo_mcp::executor::CompositeToolExecutor::new(
        builtin_tools,
        mcp_tools,
    ));

    // Replace the framework Workspace tool with the Project-aware server
    // overlay and expose the explicit Project registry tool. Overlay dispatch
    // still delegates permission decisions to the built-in executor.
    let workspace_tool = Arc::new(
        crate::tools::ProjectWorkspaceTool::new_with_workspace_resolver(
            session_repo.clone(),
            project_store.clone(),
            workspace_resolver.clone(),
        ),
    );
    let with_workspace: Arc<dyn ToolExecutor> =
        Arc::new(crate::tools::OverlayToolExecutor::new(base, workspace_tool));
    let project_tool = Arc::new(
        crate::tools::ProjectTool::new_with_workspace_resolver(
            session_repo.clone(),
            project_store.clone(),
            workspace_resolver,
        )
        .with_account_sink(account_sink),
    );
    let with_project: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_workspace,
        project_tool,
    ));

    let memory_tool = Arc::new(
        crate::tools::MemoryTool::new(session_repo.clone(), app_data_dir.clone())
            .with_project_store(project_store.clone()),
    );
    let with_memory: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_project,
        memory_tool,
    ));

    // `ledger` sits in the base layer (not root-only) so headless reminder
    // sessions fired by the schedule manager can read and transition the very
    // record that woke them. The schedule bridge is late-bound: the scheduler
    // is built after the tool chain, and the builder binds it once it's up.
    let ledger_tool = Arc::new(
        crate::tools::LedgerTool::new(session_repo.clone(), app_data_dir.clone())
            .with_schedule_bridge(ledger_schedule_bridge)
            .with_project_store(project_store.clone()),
    );
    let with_ledger: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_memory,
        ledger_tool,
    ));

    let load_skill_tool = Arc::new(
        crate::tools::LoadSkillTool::new(
            skill_manager.clone(),
            config.clone(),
            session_repo.clone(),
        )
        .with_project_store(project_store.clone())
        .with_permission_checked_context_registry(
            with_ledger.clone(),
            permission_checker.permission_config(),
        ),
    );
    let with_load_skill: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_ledger,
        load_skill_tool,
    ));

    let read_skill_resource_tool = Arc::new(
        crate::tools::ReadSkillResourceTool::new(skill_manager, config.clone(), session_repo)
            .with_project_store(project_store),
    );
    let with_skills: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_load_skill,
        read_skill_resource_tool,
    ));

    // compact_context is available to all sessions for manual compression.
    let compact_tool = Arc::new(crate::tools::CompactContextTool);
    let with_compact: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_skills,
        compact_tool,
    ));

    // notify is available to all sessions (including headless/scheduled runs
    // with no live subscriber — that's the whole point of proactively
    // alerting the owner) for proactively surfacing something outside the
    // chat transcript.
    let notify_dispatcher = Arc::new(crate::tools::ServerNotificationDispatcher::new(
        notification_service,
        session_event_senders,
        session_watchers,
        config,
    ));
    let notify_tool = Arc::new(crate::tools::NotifyTool::new(notify_dispatcher));
    Arc::new(crate::tools::OverlayToolExecutor::new(
        with_compact,
        notify_tool,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_root_tools(
    base_tools: Arc<dyn ToolExecutor>,
    schedule_store: Arc<ScheduleStore>,
    schedule_manager: Arc<ScheduleManager>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
    session_messenger: Arc<bamboo_engine::SessionMessenger>,
    spawn_scheduler: Arc<SpawnScheduler>,
    sessions: bamboo_engine::SessionCache,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<
        RwLock<HashMap<String, broadcast::Sender<bamboo_agent_core::AgentEvent>>>,
    >,
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    config: Arc<RwLock<Config>>,
    provider_registry: Arc<bamboo_llm::ProviderRegistry>,
    broker: Option<bamboo_config::BrokerClientConfig>,
    fabric_deployer: Arc<bamboo_server_tools::FabricDeployer>,
    project_store: Arc<bamboo_projects::ProjectStore>,
    workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Arc<dyn ToolExecutor> {
    // Shared adapter for the unified child session tool.
    let adapter = Arc::new(crate::tools::ChildSessionAdapter {
        session_store: session_store.clone(),
        storage: storage.clone(),
        persistence: persistence.clone(),
        session_messenger: Some(session_messenger),
        scheduler: spawn_scheduler,
        sessions_cache: sessions,
        agent_runners: agent_runners.clone(),
        session_event_senders,
        subagent_model_resolver,
        config: config.clone(),
        project_store: Some(project_store.clone()),
        workspace_resolver: workspace_resolver.clone(),
        parent_wait_slots: Arc::new(dashmap::DashMap::new()),
    });

    // Root sessions can create and manage child sessions via unified SubAgent tool.
    // The adapter satisfies both ports the tool depends on (`ChildSessionPort`
    // for session lifecycle, `SubagentResolutionPort` for subagent_type config).
    // The model catalog enables `action=list_models` + explicit `create.model`.
    let sub_agent_tool = Arc::new(
        crate::tools::SubAgentTool::new(adapter.clone(), adapter).with_model_catalog(Arc::new(
            crate::tools::RegistryModelCatalog::new(provider_registry),
        )),
    );
    let tools_with_sub_agent: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(base_tools, sub_agent_tool),
    );

    // Root sessions can manage schedules via `scheduler`.
    // Background schedule runs intentionally use `tools_for_schedules` above and therefore
    // do not get this management tool by default.
    let schedule_tasks_tool = Arc::new(crate::schedule_app::ScheduleTasksTool::new(
        schedule_store,
        schedule_manager,
        session_store.clone(),
        storage.clone(),
        config.clone(),
        project_store,
        workspace_resolver,
    ));
    let tools_with_schedule: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_sub_agent, schedule_tasks_tool),
    );

    let session_inspector_tool = Arc::new(crate::tools::SessionInspectorTool::new(
        session_store,
        storage,
    ));
    let tools_with_inspector: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_schedule, session_inspector_tool),
    );

    // When a broker is configured, root agents also get `ask_agent` (command
    // broker-deployed agents, query/steer) and `deploy_agent` (spin up new
    // workers themselves — local / Docker / SSH — wired to the same broker).
    match broker {
        Some(b) if !b.endpoint.trim().is_empty() => {
            let with_ask: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
                tools_with_inspector,
                Arc::new(crate::tools::AskAgentTool::new(
                    b.endpoint.clone(),
                    b.token.clone(),
                )),
            ));
            // deploy_agent shares the fabric deployer's registry, so its
            // list/stop covers cluster-deployed workers too (and vice versa).
            let bamboo_bin =
                std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));
            let with_deploy: Arc<dyn ToolExecutor> =
                Arc::new(crate::tools::OverlayToolExecutor::new(
                    with_ask,
                    Arc::new(crate::tools::DeployAgentTool::new(
                        b.endpoint,
                        b.token,
                        bamboo_bin,
                        fabric_deployer.registry(),
                        config.clone(),
                    )),
                ));
            // `cluster`: progressive-disclosure inventory (list/describe/status)
            // + dispatch (deploy/stop) via the SAME shared deploy engine.
            Arc::new(crate::tools::OverlayToolExecutor::new(
                with_deploy,
                Arc::new(crate::tools::ClusterTool::new(config, fabric_deployer)),
            ))
        }
        _ => tools_with_inspector,
    }
}
