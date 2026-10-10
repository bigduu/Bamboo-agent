//! Tool assembly functions for building the layered tool surface.
//!
//! These functions compose the tool executor chain:
//! ```text
//! base_tools (builtin + MCP + memory + skills + context controls + legacy self-only
//!             session_history + exact self-only session_history_current)
//!   └─> root_tools (base + Plan + SubAgent + scheduler + full legacy session_history)
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
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    app_data_dir: PathBuf,
    notification_service: Arc<bamboo_notification::NotificationService>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_watchers: Arc<SessionWatchers>,
    ledger_schedule_bridge: Arc<crate::schedule_app::LateBoundLedgerBridge>,
    project_store: Arc<bamboo_projects::ProjectStore>,
    account_sink: Arc<bamboo_engine::events::AccountEventSink>,
    workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    tool_event_publisher: Arc<dyn ToolEventPublisher>,
    memory_store: bamboo_memory::memory_store::MemoryStore,
) -> (
    Arc<dyn ToolExecutor>,
    Arc<dyn bamboo_engine::external_agents::runtime::NativeToolCeilingSource>,
) {
    let ceiling_config = config.clone();
    let ceiling_projects = project_store.clone();
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
    let builtin_tools: Arc<dyn ToolExecutor> = builtin_executor.clone();

    // Create composite tool executor (builtin + MCP)
    let mcp_tools = Arc::new(bamboo_mcp::executor::McpToolExecutor::new(
        mcp_manager.clone(),
        mcp_manager.tool_index(),
    ));

    let base: Arc<dyn ToolExecutor> = Arc::new(bamboo_mcp::executor::CompositeToolExecutor::new(
        builtin_tools,
        mcp_tools,
    ));

    // Replace the built-in default-root session_note instance so note writes
    // and next-round prompt reads share this AppState's one concrete store.
    let session_note_tool = Arc::new(bamboo_tools::tools::SessionNoteTool::with_memory_store(
        memory_store.clone(),
    ));
    let base: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        base,
        session_note_tool,
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

    let memory_tool = Arc::new(crate::tools::MemoryTool::with_store(
        session_repo.clone(),
        memory_store,
    ));
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

    let with_skills = crate::tools::assemble_legacy_skill_tools(
        with_ledger.clone(),
        skill_manager,
        config.clone(),
        session_repo,
        Some(project_store),
        Some(crate::tools::LegacySkillContextRegistry {
            tools: with_ledger,
            permission_config: permission_checker.permission_config(),
        }),
    );

    // compact_context is available to all sessions for manual compression.
    let compact_tool = Arc::new(crate::tools::CompactContextTool);
    let with_compact: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_skills,
        compact_tool,
    ));

    // archive_context is a distinct summary-free retrieval-window control.
    let archive_tool = Arc::new(crate::tools::ArchiveContextTool);
    let with_context_controls: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(with_compact, archive_tool),
    );

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
    let with_notify: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_context_controls,
        notify_tool,
    ));

    // Preserve the legacy self-only name for Base/Child compatibility. Root
    // replaces only this exact name with the full viewer below.
    let self_history_tool = Arc::new(crate::tools::SessionInspectorTool::self_only(
        session_store.clone(),
        storage.clone(),
    ));
    let with_legacy_history: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(with_notify, self_history_tool),
    );

    // A distinct exact identity keeps the least-privilege schema Core under
    // Progressive and StickyFallback loading. It is not an alias for the broad
    // Root viewer and therefore cannot inherit cross-Session actions.
    let current_history_tool = Arc::new(crate::tools::SessionInspectorTool::current(
        session_store,
        storage,
    ));
    let base: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        with_legacy_history,
        current_history_tool,
    ));
    let ceiling = Arc::new(HostNativeToolCeiling {
        builtin: builtin_executor,
        base: base.clone(),
        config: ceiling_config,
        projects: ceiling_projects,
    });
    (base, ceiling)
}

/// Keep the actual instances and complete resolver assembled above. No Root
/// role filter, catalog reconstruction, or client-supplied list is authority.
struct HostNativeToolCeiling {
    builtin: Arc<bamboo_tools::BuiltinToolExecutor>,
    base: Arc<dyn ToolExecutor>,
    config: Arc<RwLock<Config>>,
    projects: Arc<bamboo_projects::ProjectStore>,
}

impl HostNativeToolCeiling {
    fn native_owner(&self, name: &str) -> bool {
        self.base.exact_tool_owner(name).is_some_and(|owner| {
            std::ptr::addr_eq(owner, self.builtin.as_ref() as &dyn ToolExecutor)
        }) && self.builtin.eligible_native_tool(name)
    }

    fn resolve(&self, reference: &str) -> Option<String> {
        bamboo_domain::resolve_tool_reference_name(reference, |name| {
            self.base.owns_exact_tool(name)
        })
    }
}

#[async_trait::async_trait]
impl bamboo_engine::external_agents::runtime::NativeToolCeilingSource for HostNativeToolCeiling {
    async fn observe(&self, session: &bamboo_domain::Session) -> Result<Vec<String>, String> {
        let invalid = || "native_tool_ceiling_project_unavailable".to_string();
        let typed = session
            .runtime_metadata
            .as_ref()
            .and_then(|meta| meta.project_id.as_deref());
        let legacy = session.metadata.get("project_id").map(String::as_str);
        let parse = |raw: &str| bamboo_domain::ProjectId::parse(raw.trim()).map_err(|_| invalid());
        let typed = typed.map(parse).transpose()?;
        let legacy = legacy.map(parse).transpose()?;
        if typed.is_some() && legacy.is_some() && typed != legacy {
            return Err(invalid());
        }
        if let Some(project_id) = typed.or(legacy) {
            let projects = self.projects.clone();
            tokio::task::spawn_blocking(move || {
                let manifest = projects
                    .get(&project_id)
                    .map_err(|_| "native_tool_ceiling_project_unavailable".to_string())?;
                if manifest.id != project_id
                    || manifest.status != bamboo_domain::ProjectStatus::Active
                {
                    return Err("native_tool_ceiling_project_unavailable".to_string());
                }
                Ok(())
            })
            .await
            .map_err(|_| invalid())??;
        }
        let disabled = self.config.read().await.disabled_tool_references();
        let disabled: std::collections::BTreeSet<_> = disabled
            .iter()
            .filter_map(|reference| self.resolve(reference))
            .collect();
        for name in bamboo_subagent::proto::NativeToolCeiling::NAMES {
            if self.base.owns_exact_tool(name) && self.base.exact_tool_owner(name).is_none() {
                return Err("native_tool_ceiling_owner_unknown".into());
            }
        }
        let profile =
            bamboo_engine::session_app::child_session::named_profile::named_profile_tool_names(
                session,
            )
            .map_err(|_| "named_profile_binding_invalid".to_string())?;
        let child_denied: std::collections::BTreeSet<_> = session
            .metadata
            .get("disabled_tools")
            .map(|raw| {
                serde_json::from_str::<Vec<String>>(raw)
                    .map_err(|_| "named_profile_parent_tools_invalid".to_string())
            })
            .transpose()?
            .unwrap_or_default()
            .iter()
            .filter_map(|name| self.resolve(name))
            .collect();
        let ticket_child = session
            .metadata
            .contains_key(bamboo_engine::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY);
        let ticket_packet = session
            .metadata
            .get("ticket.work_contract_ref.v1")
            .and_then(|raw| {
                serde_json::from_str::<
                        bamboo_engine::ticket_worker_plan::tickets::WorkContextPacket,
                    >(raw)
                    .ok()
            });
        Ok(bamboo_subagent::proto::NativeToolCeiling::NAMES
            .iter()
            .filter(|name| {
                if (!ticket_child && **name == "Task")
                    || (ticket_child
                        && **name != "Task"
                        && (!matches!(**name, "Read" | "Write")
                            || ticket_packet.as_ref().is_none_or(|packet| {
                                packet.workspace.is_none()
                                    || !packet.contract.allowed_tools.contains(**name)
                            })))
                {
                    return false;
                }
                self.native_owner(name)
                    && !disabled.contains(**name)
                    && !child_denied.contains(**name)
                    && profile
                        .as_ref()
                        .is_none_or(|tools| tools.iter().any(|tool| tool == **name))
            })
            .map(|name| (*name).to_string())
            .collect())
    }
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
    parent_request_replies: Arc<dyn bamboo_server_tools::ParentRequestReplyPort>,
) -> (
    Arc<dyn ToolExecutor>,
    Arc<dyn bamboo_agent_core::tools::Tool>,
) {
    // Shared adapter for the unified child session tool.
    let adapter = Arc::new(crate::tools::ChildSessionAdapter {
        session_store: session_store.clone(),
        storage: storage.clone(),
        persistence: persistence.clone(),
        session_messenger: Some(session_messenger.clone()),
        scheduler: spawn_scheduler,
        sessions_cache: sessions,
        agent_runners: agent_runners.clone(),
        session_event_senders,
        subagent_model_resolver,
        config: config.clone(),
        project_store: Some(project_store.clone()),
        workspace_resolver: workspace_resolver.clone(),
        parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        recovered_launches: Arc::new(dashmap::DashMap::new()),
    });

    // Root sessions can create and manage child sessions via unified SubAgent tool.
    // The adapter satisfies both ports the tool depends on (`ChildSessionPort`
    // for session lifecycle, `SubagentResolutionPort` for subagent_type config).
    // The model catalog enables `action=list_models` + explicit `create.model`.
    let sub_agent_tool = Arc::new(
        crate::tools::SubAgentTool::new(adapter.clone(), adapter.clone())
            .with_model_catalog(Arc::new(crate::tools::RegistryModelCatalog::new(
                provider_registry,
            )))
            .with_parent_request_replies(parent_request_replies),
    );
    let tools_with_sub_agent: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(base_tools, sub_agent_tool.clone()),
    );

    // Planning is delegated to one runtime-enforced read-only child. This keeps
    // the root session in its normal orchestrator posture and reuses the same
    // durable child/wait/completion path as `SubAgent`.
    let plan_tool = Arc::new(crate::tools::PlanTool::new(
        adapter.clone(),
        adapter.clone(),
    ));
    let tools_with_plan: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
        tools_with_sub_agent,
        plan_tool,
    ));

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
        crate::tools::OverlayToolExecutor::new(tools_with_plan, schedule_tasks_tool),
    );

    // Intentional same-name overlay replacement: Root keeps every privileged
    // cross-session action while Base/Child expose only current-Session reads.
    let session_inspector_tool = Arc::new(crate::tools::SessionInspectorTool::new(
        session_store.clone(),
        storage,
    ));
    let tools_with_inspector: Arc<dyn ToolExecutor> = Arc::new(
        crate::tools::OverlayToolExecutor::new(tools_with_schedule, session_inspector_tool),
    );
    let tools_with_control: Arc<dyn ToolExecutor> =
        Arc::new(crate::tools::OverlayToolExecutor::new(
            tools_with_inspector,
            Arc::new(bamboo_server_tools::SessionControlTool::new(
                session_messenger.clone(),
            )),
        ));

    // Keep these exact-name compatibility calls registered. The per-session
    // model catalog removes physical broker tools from Root/Child schemas.
    let tools: Arc<dyn ToolExecutor> = match broker {
        Some(b) if !b.endpoint.trim().is_empty() => {
            let with_ask: Arc<dyn ToolExecutor> = Arc::new(crate::tools::OverlayToolExecutor::new(
                tools_with_control,
                Arc::new(
                    crate::tools::AskAgentTool::new(b.endpoint.clone(), b.token.clone())
                        .with_deployments(fabric_deployer.registry(), session_store.clone())
                        .with_messenger(session_messenger),
                ),
            ));
            // deploy_agent shares the fabric deployer's registry, so its
            // list/stop covers cluster-deployed workers too (and vice versa).
            let bamboo_bin =
                std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));
            let with_deploy: Arc<dyn ToolExecutor> =
                Arc::new(crate::tools::OverlayToolExecutor::new(
                    with_ask,
                    Arc::new(
                        crate::tools::DeployAgentTool::new(
                            b.endpoint,
                            b.token,
                            bamboo_bin,
                            fabric_deployer.registry(),
                            config.clone(),
                        )
                        .with_actor_store(session_store)
                        .with_child_port(adapter),
                    ),
                ));
            // `cluster`: progressive-disclosure inventory (list/describe/status)
            // + dispatch (deploy/stop) via the SAME shared deploy engine.
            Arc::new(crate::tools::OverlayToolExecutor::new(
                with_deploy,
                Arc::new(crate::tools::ClusterTool::new(config, fabric_deployer)),
            ))
        }
        _ => tools_with_control,
    };
    (tools, sub_agent_tool)
}

#[cfg(test)]
mod native_ceiling_tests {
    use super::*;
    use bamboo_agent_core::{
        Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolSchema,
    };
    use bamboo_engine::external_agents::runtime::NativeToolCeilingSource;
    use serde_json::json;

    struct Foreign(&'static str);
    #[async_trait::async_trait]
    impl Tool for Foreign {
        fn name(&self) -> &str {
            self.0
        }
        fn description(&self) -> &str {
            "foreign owner"
        }
        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object"})
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
            panic!("ownership observation must not invoke")
        }
    }
    struct Unknown(Arc<dyn ToolExecutor>);
    #[async_trait::async_trait]
    impl ToolExecutor for Unknown {
        async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("must not dispatch")
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            self.0.list_tools()
        }
    }
    fn source(home: &std::path::Path) -> HostNativeToolCeiling {
        let builtin = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        HostNativeToolCeiling {
            base: builtin.clone(),
            builtin,
            config: Arc::new(RwLock::new(Config::default())),
            projects: Arc::new(bamboo_projects::ProjectStore::open(home).unwrap()),
        }
    }

    #[tokio::test]
    async fn ticket_ceiling_selects_only_host_builtin_task_and_legacy_keeps_five_tools() {
        let home = tempfile::tempdir().unwrap();
        let mut source = source(home.path());
        let mut child = bamboo_domain::Session::new_child("ticket-child", "root", "", "");
        assert_eq!(source.observe(&child).await.unwrap().len(), 5);
        child.metadata.insert(
            bamboo_engine::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
            "assignment".into(),
        );
        assert_eq!(source.observe(&child).await.unwrap(), ["Task"]);
        source.base = Arc::new(crate::tools::OverlayToolExecutor::new(
            source.base.clone(),
            Arc::new(Foreign("Task")),
        ));
        assert!(source.observe(&child).await.unwrap().is_empty());
        source.base = source.builtin.clone();
        source.config.write().await.tools.disabled = vec!["Task".into()];
        assert!(source.observe(&child).await.unwrap().is_empty());
    }
    #[tokio::test]
    async fn native_owner_uses_complete_composite_overlay_exact_before_alias_and_config() {
        let home = tempfile::tempdir().unwrap();
        let mut source = source(home.path());
        let secondary = Arc::new(
            bamboo_tools::BuiltinToolExecutorBuilder::new()
                .with_tool(Foreign("apply_patch"))
                .unwrap()
                .build(),
        );
        source.base = Arc::new(bamboo_mcp::executor::CompositeToolExecutor::new(
            source.builtin.clone(),
            secondary,
        ));
        source.config.write().await.tools.disabled =
            vec!["apply_patch".into(), "Bash".into(), "Write".into()];
        let session = bamboo_domain::Session::new("owner-root", "");
        assert_eq!(
            source.observe(&session).await.unwrap(),
            ["Edit", "Glob", "Read"]
        );
        source.base = Arc::new(crate::tools::OverlayToolExecutor::new(
            source.base.clone(),
            Arc::new(Foreign("apply_patch")),
        ));
        assert_eq!(
            source.observe(&session).await.unwrap(),
            ["Edit", "Glob", "Read"]
        );
        source.base = Arc::new(crate::tools::OverlayToolExecutor::new(
            source.base.clone(),
            Arc::new(Foreign("Edit")),
        ));
        assert_eq!(source.observe(&session).await.unwrap(), ["Glob", "Read"]);
        source.base = source.builtin.clone();
        // No actual apply_patch owner now: the same disabled reference aliases Edit.
        assert_eq!(source.observe(&session).await.unwrap(), ["Glob", "Read"]);
        source.config.write().await.tools.disabled.clear();
        source.base = Arc::new(Unknown(source.builtin.clone()));
        assert_eq!(
            source.observe(&session).await.unwrap_err(),
            "native_tool_ceiling_owner_unknown"
        );
    }
    #[tokio::test]
    async fn native_owner_rejects_spoofed_actual_registry_and_closed_project_identity() {
        let home = tempfile::tempdir().unwrap();
        let source = source(home.path());
        let mut session = bamboo_domain::Session::new("project-root", "");
        let project = source
            .projects
            .create("active without workspace", None)
            .unwrap();
        let mut foreign = source.projects.create("foreign fixture", None).unwrap();
        session.set_project_id_meta(project.id.as_str());
        assert_eq!(source.observe(&session).await.unwrap().len(), 5);
        session
            .metadata
            .insert("project_id".into(), "another-project".into());
        assert!(source.observe(&session).await.is_err());
        session.set_project_id_meta("malformed/project");
        assert!(source.observe(&session).await.is_err());
        session.set_project_id_meta("missing-project");
        assert!(source.observe(&session).await.is_err());
        session.set_project_id_meta(project.id.as_str());
        source
            .projects
            .archive(&project.id, project.revision)
            .unwrap();
        assert!(source.observe(&session).await.is_err());
        let foreign_id = foreign.id.clone();
        let path = source.projects.paths().manifest_path(&foreign_id);
        foreign.id = bamboo_domain::ProjectId::new();
        std::fs::write(&path, serde_json::to_vec(&foreign).unwrap()).unwrap();
        let backup = path.with_file_name("project.json.bak");
        if backup.exists() {
            std::fs::remove_file(backup).unwrap();
        }
        session.set_project_id_meta(foreign_id.as_str());
        assert!(source.observe(&session).await.is_err());
        std::fs::write(&path, "invalid manifest").unwrap();
        assert!(source.observe(&session).await.is_err());
        session.clear_project_id_meta();
        assert!(source.builtin.registry().unregister("Write"));
        source.builtin.register_tool(Foreign("Write")).unwrap();
        assert!(!source
            .observe(&session)
            .await
            .unwrap()
            .contains(&"Write".into()));
    }
}

#[cfg(test)]
mod legacy_skill_assembly_tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;
    use bamboo_agent_core::{Message, Session, Tool, ToolCall, ToolExecutionContext};
    use serde_json::json;

    fn call(name: &str, skill: &str) -> ToolCall {
        ToolCall {
            id: format!("assembly-{name}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: json!({"skill_id": skill, "resource_path": "references/proof.txt"})
                    .to_string(),
            },
        }
    }

    #[tokio::test]
    async fn legacy_assembly_real_server_project_context_and_policy_parity() {
        for provider_fails in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let policy = bamboo_tools::permission::PermissionConfig::new();
            policy.set_enabled(true);
            bamboo_tools::permission::storage::PermissionStorage::new(home.path())
                .save(&policy)
                .await
                .unwrap();
            let state = super::super::AppState::new_with_provider(
                home.path().to_path_buf(),
                Config::default(),
                Arc::new(super::super::UnconfiguredProvider {
                    message: "assembly test must not call a model".into(),
                }),
            )
            .await
            .unwrap();
            let project = state.project_store.create("Assembly", None).unwrap();
            let skill_dir = state
                .project_store
                .paths()
                .project_home(&project.id)
                .join("skills/assembly-project");
            std::fs::create_dir_all(skill_dir.join("references")).unwrap();
            std::fs::write(skill_dir.join("SKILL.md"), "---\nname: assembly-project\ndescription: Assembly project\nmetadata:\n  dynamic_context:\n    - id: proof\n      tool: Read\n      input: {path: provider.txt}\n      max_chars: 512\n      timeout_ms: 1000\n---\nPROJECT_INSTRUCTIONS\n").unwrap();
            std::fs::write(skill_dir.join("references/proof.txt"), "PROJECT_RESOURCE\n").unwrap();
            let workspace = home.path().join("workspace");
            std::fs::create_dir_all(&workspace).unwrap();
            if provider_fails {
                std::fs::File::create(workspace.join("provider.txt"))
                    .unwrap()
                    .set_len(64 * 1024 * 1024)
                    .unwrap();
            } else {
                std::fs::write(workspace.join("provider.txt"), "REAL_BASE_PROVIDER\n").unwrap();
            }
            let mut session = Session::new("assembly-server", "test-model");
            session.set_project_id_meta(project.id.to_string());
            session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
            session.metadata.insert(
                bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
                "[\"assembly-project\"]".into(),
            );
            session
                .metadata
                .insert("unrelated".into(), "retained".into());
            session.add_message(Message::user("use assembly-project"));
            state.session_repo.save(&mut session).await.unwrap();
            let tools = state.agent.default_tools();
            assert!(Arc::ptr_eq(
                tools,
                &state.tools_for(crate::tools::ToolSurface::Base)
            ));
            let schemas = tools.list_tools();
            assert!(schemas
                .windows(2)
                .all(|pair| pair[0].function.name < pair[1].function.name));
            for schema in [
                crate::tools::LoadSkillTool::new(
                    state.skill_manager.clone(),
                    state.config.clone(),
                    state.session_repo.clone(),
                )
                .to_schema(),
                crate::tools::ReadSkillResourceTool::new(
                    state.skill_manager.clone(),
                    state.config.clone(),
                    state.session_repo.clone(),
                )
                .to_schema(),
            ] {
                let matching: Vec<_> = schemas
                    .iter()
                    .filter(|s| s.function.name == schema.function.name)
                    .collect();
                assert_eq!(matching.len(), 1);
                assert_eq!(
                    serde_json::to_value(matching[0]).unwrap(),
                    serde_json::to_value(schema).unwrap()
                );
            }
            assert!(!tools.owns_exact_tool("skills_list"));
            assert!(!tools.owns_exact_tool("skills_read"));
            let load = call("load_skill", "assembly-project");
            let mut ctx = ToolExecutionContext::none(&load.id);
            ctx.session_id = Some(&session.id);
            ctx.root_session_id = Some(&session.root_session_id);
            ctx.bypass_permissions = true;
            let result = tools.execute_with_context(&load, ctx).await.unwrap();
            assert!(result.success);
            let receipt: serde_json::Value = serde_json::from_str(&result.result).unwrap();
            // Non-stopping degraded context retains the original active receipt.
            assert_eq!(receipt["activation_status"], "active");
            let saved = state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            let active: bamboo_skills::ActiveWorkflow =
                serde_json::from_str(&saved.metadata[bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY])
                    .unwrap();
            assert_eq!(active.dynamic_context.len(), 1);
            assert_eq!(
                active.dynamic_context[0].provenance,
                "registered_tool_permission_checked"
            );
            if provider_fails {
                assert_eq!(
                    active.dynamic_context[0].status,
                    bamboo_skills::WorkflowActivationStatus::Degraded
                );
                assert!(active.dynamic_context[0].content.is_empty());
                assert!(active.dynamic_context[0].diagnostic.is_some());
            } else {
                assert!(active.dynamic_context[0]
                    .content
                    .contains("REAL_BASE_PROVIDER"));
            }
            assert_eq!(saved.metadata["unrelated"], "retained");
            assert!(saved.metadata
                [bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_SUMMARY_METADATA_KEY]
                .contains("assembly-project"));
            let read = call("read_skill_resource", "assembly-project");
            let mut ctx = ToolExecutionContext::none(&read.id);
            ctx.session_id = Some(&session.id);
            let read_result = tools.execute_with_context(&read, ctx).await.unwrap();
            assert!(read_result.success && read_result.result.contains("PROJECT_RESOURCE"));
            let saved = state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            assert!(saved.metadata
                [bamboo_skills::runtime_metadata::LAST_RESOURCE_READ_SUMMARY_METADATA_KEY]
                .contains("references/proof.txt"));
            state.config.write().await.skills.disabled = vec!["assembly-project".into()];
            let mut ctx = ToolExecutionContext::none(&load.id);
            ctx.session_id = Some(&session.id);
            assert!(tools.execute_with_context(&load, ctx).await.is_err());
        }
    }
}
