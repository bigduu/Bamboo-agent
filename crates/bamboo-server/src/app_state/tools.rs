//! Tool assembly functions for building the layered tool surface.
//!
//! These functions compose the tool executor chain:
//! ```text
//! base_tools (builtin + MCP + memory + skills)
//!   └─> root_tools (base + SubSession + ScheduleTasks + SubSessionManager + SessionInspector)
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::Session;
use bamboo_engine::McpServerManager;
use bamboo_engine::SkillManager;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::SessionStoreV2;

use super::init::PermissionChecker;
use super::{AgentRunner, ScheduleManager, ScheduleStore, SpawnScheduler};

pub(super) fn build_base_tools(
    config: Arc<RwLock<Config>>,
    permission_checker: Arc<PermissionChecker>,
    mcp_manager: Arc<McpServerManager>,
    skill_manager: Arc<SkillManager>,
    storage: Arc<dyn Storage>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
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

    let memory_tool = Arc::new(crate::tools::MemoryTool::new(
        sessions.clone(),
        storage.clone(),
        app_data_dir.clone(),
    ));
    let with_memory: Arc<dyn ToolExecutor> =
        Arc::new(crate::tools::OverlayToolExecutor::new(base, memory_tool));

    let load_skill_tool = Arc::new(crate::tools::LoadSkillTool::new(
        skill_manager.clone(),
        config.clone(),
        sessions.clone(),
        storage.clone(),
    ));
    let with_load_skill: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_memory,
        load_skill_tool,
    ));

    let read_skill_resource_tool = Arc::new(crate::tools::ReadSkillResourceTool::new(
        skill_manager,
        config,
        sessions,
        storage,
    ));
    Arc::new(crate::tools::OverlayToolExecutor::new(
        with_load_skill,
        read_skill_resource_tool,
    ))
}

pub(super) fn build_root_tools(
    base_tools: Arc<dyn ToolExecutor>,
    schedule_store: Arc<ScheduleStore>,
    schedule_manager: Arc<ScheduleManager>,
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    spawn_scheduler: Arc<SpawnScheduler>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<
        RwLock<HashMap<String, broadcast::Sender<bamboo_agent_core::AgentEvent>>>,
    >,
) -> Arc<dyn ToolExecutor> {
    // Shared adapter for both child session tools.
    let adapter = Arc::new(crate::tools::ChildSessionAdapter {
        session_store: session_store.clone(),
        storage: storage.clone(),
        scheduler: spawn_scheduler,
        sessions_cache: sessions,
        agent_runners: agent_runners.clone(),
        session_event_senders: session_event_senders,
    });

    // Root sessions can spawn child sessions.
    let spawn_tool = Arc::new(crate::tools::SpawnSessionTool::new(adapter.clone()));
    let tools_with_spawn: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        base_tools, spawn_tool,
    ));

    // Root sessions can manage schedules via `scheduler`.
    // Background schedule runs intentionally use `tools_for_schedules` above and therefore
    // do not get this management tool by default.
    let schedule_tasks_tool = Arc::new(crate::tools::ScheduleTasksTool::new(
        schedule_store,
        schedule_manager,
        session_store.clone(),
        storage.clone(),
    ));
    let tools_with_schedule: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_spawn, schedule_tasks_tool),
    );

    let sub_session_manager_tool = Arc::new(crate::tools::SubSessionManagerTool::new(adapter));
    let tools_with_sub_session_manager: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_schedule, sub_session_manager_tool),
    );

    let session_inspector_tool = Arc::new(crate::tools::SessionInspectorTool::new(
        session_store,
        storage,
    ));

    Arc::new(crate::tools::OverlayToolExecutor::new(
        tools_with_sub_session_manager,
        session_inspector_tool,
    ))
}
