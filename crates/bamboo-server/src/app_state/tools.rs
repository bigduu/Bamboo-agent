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
use bamboo_engine::McpServerManager;
use bamboo_engine::SkillManager;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::LockedSessionStore;
use bamboo_infrastructure::SessionStoreV2;

use super::init::PermissionChecker;
use super::{AgentRunner, ScheduleManager, ScheduleStore, SpawnScheduler};

#[allow(clippy::too_many_arguments)]
pub(super) fn build_base_tools(
    config: Arc<RwLock<Config>>,
    permission_checker: Arc<PermissionChecker>,
    mcp_manager: Arc<McpServerManager>,
    skill_manager: Arc<SkillManager>,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
    sessions: bamboo_engine::SessionCache,
    app_data_dir: PathBuf,
) -> Arc<dyn ToolExecutor> {
    // Initialize built-in tools with permission checks.
    // If no permission config has been persisted yet, keep checks disabled for backward
    // compatibility and opt-in behavior.
    let builtin_executor = Arc::new(
        bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
            config.clone(),
            permission_checker,
        ),
    );
    let builtin_tools: Arc<dyn ToolExecutor> = builtin_executor;

    // Create composite tool executor (builtin + MCP)
    let mcp_tools = Arc::new(bamboo_engine::McpToolExecutor::new(
        mcp_manager.clone(),
        mcp_manager.tool_index(),
    ));

    let base: Arc<dyn ToolExecutor> = Arc::new(bamboo_engine::CompositeToolExecutor::new(
        builtin_tools,
        mcp_tools,
    ));

    // One framework-owned session coordinator, shared by every tool that needs
    // to load/persist sessions — instead of each re-rolling the cache+storage dance.
    let session_repo = bamboo_engine::SessionRepository::new(
        sessions.clone(),
        storage.clone(),
        persistence.clone(),
    );

    let memory_tool = Arc::new(crate::tools::MemoryTool::new(
        session_repo.clone(),
        app_data_dir.clone(),
    ));
    let with_memory: Arc<dyn ToolExecutor> =
        Arc::new(crate::tools::OverlayToolExecutor::new(base, memory_tool));

    let load_skill_tool = Arc::new(crate::tools::LoadSkillTool::new(
        skill_manager.clone(),
        config.clone(),
        session_repo.clone(),
    ));
    let with_load_skill: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_memory,
        load_skill_tool,
    ));

    let read_skill_resource_tool = Arc::new(crate::tools::ReadSkillResourceTool::new(
        skill_manager,
        config,
        session_repo,
    ));
    let with_skills: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_load_skill,
        read_skill_resource_tool,
    ));

    // compact_context is available to all sessions for manual compression.
    let compact_tool = Arc::new(crate::tools::CompactContextTool);
    Arc::new(crate::tools::OverlayToolExecutor::new(
        with_skills,
        compact_tool,
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
    spawn_scheduler: Arc<SpawnScheduler>,
    sessions: bamboo_engine::SessionCache,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<
        RwLock<HashMap<String, broadcast::Sender<bamboo_agent_core::AgentEvent>>>,
    >,
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    config: Arc<RwLock<Config>>,
    subagent_profiles: Arc<bamboo_domain::subagent::SubagentProfileRegistry>,
) -> Arc<dyn ToolExecutor> {
    // Shared adapter for the unified child session tool. Cloning the
    // profile registry Arc is cheap and lets us hand the same registry
    // to the SubAgentTool below without going through the adapter.
    let profiles_for_tool = subagent_profiles.clone();
    let tool_names: Vec<String> = base_tools
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();
    let adapter = Arc::new(crate::tools::ChildSessionAdapter {
        session_store: session_store.clone(),
        storage: storage.clone(),
        persistence: persistence.clone(),
        scheduler: spawn_scheduler,
        sessions_cache: sessions,
        agent_runners: agent_runners.clone(),
        session_event_senders,
        subagent_model_resolver,
        config: config.clone(),
        subagent_profiles,
        tool_names,
        parent_wait_slots: Arc::new(dashmap::DashMap::new()),
    });

    // Root sessions can create and manage child sessions via unified SubAgent tool.
    // The adapter satisfies both ports the tool depends on (`ChildSessionPort`
    // for session lifecycle, `SubagentResolutionPort` for subagent_type config).
    let sub_agent_tool = Arc::new(crate::tools::SubAgentTool::new(
        adapter.clone(),
        adapter,
        profiles_for_tool,
    ));
    let tools_with_sub_agent: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(base_tools, sub_agent_tool),
    );

    // Root sessions can manage schedules via `scheduler`.
    // Background schedule runs intentionally use `tools_for_schedules` above and therefore
    // do not get this management tool by default.
    let schedule_tasks_tool = Arc::new(crate::tools::ScheduleTasksTool::new(
        schedule_store,
        schedule_manager,
        session_store.clone(),
        storage.clone(),
        config,
    ));
    let tools_with_schedule: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_sub_agent, schedule_tasks_tool),
    );

    let session_inspector_tool = Arc::new(crate::tools::SessionInspectorTool::new(
        session_store,
        storage,
    ));

    Arc::new(crate::tools::OverlayToolExecutor::new(
        tools_with_schedule,
        session_inspector_tool,
    ))
}
