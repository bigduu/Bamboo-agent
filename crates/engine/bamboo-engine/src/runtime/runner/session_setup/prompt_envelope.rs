use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Session,
};
use bamboo_domain::CapabilityLoadingMode;

/// Structured request envelope separating the stable instructions, the
/// session-stable prefix messages, and the per-round dynamic context blocks. The
/// engine reads these three runs straight into the canonical [`PromptIR`]; the
/// conversation window and the wire-specific projections live on the IR, not here.
#[derive(Debug, Clone, Default)]
#[cfg(test)]
pub struct PromptEnvelope {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
    pub dynamic_context_messages: Vec<Message>,
}

#[derive(Debug, Clone)]
pub struct StablePromptFrame {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
}

impl StablePromptFrame {
    pub fn new(
        stable_instructions: impl Into<String>,
        stable_prefix_messages: Vec<Message>,
    ) -> Self {
        Self {
            stable_instructions: stable_instructions.into(),
            stable_prefix_messages,
        }
    }
}

/// Tell the model which authoritative Session is executing this request.
///
/// The block is session-stable and joins the append-only context ledger after
/// the cross-session-invariant system/tool-guide prefix. The value is useful
/// for orientation only; tools derive authorization from `ToolCtx` instead.
pub(crate) fn build_session_identity_context_block(session: &Session) -> ContextBlock {
    // Render as a JSON string so even a legacy/imported identifier containing
    // whitespace or control characters remains inert data in the prompt.
    let encoded_session_id =
        serde_json::to_string(&session.id).expect("Session ID string is JSON serializable");
    ContextBlock::new(
        ContextBlockType::SessionIdentity,
        ContextBlockPriority::Critical,
        ContextBlockStability::SessionStable,
        "Current Session Identity",
        format!(
            "Current Session ID: {encoded_session_id}\nTools authorize the caller from trusted runtime context, not from this prompt value."
        ),
    )
}

/// A session-selected prompt enhancement for a root that coordinates child
/// work. Keep this independent of the current SubAgent action schema: the
/// runtime will continue to own the concrete delegation and inspection calls.
const ROOT_ORCHESTRATION_INTRO: &str = "Root delegation mode (explicitly selected for this Session):\n\
Own task boundaries, ordering, progress, integration, and the final response. Give ready, independently verifiable directions or slices to parallel children within available capacity and budget. Sequence dependent work, overlapping writes, and jobs competing for the same constrained compute.";

const ROOT_ORCHESTRATION_GUIDANCE: &str = "Give each child scope, non-goals, required evidence, and a budget. Explicitly authorize scoped nested delegation when useful.\n\
Inspect authoritative child progress and results when needed. Correct or revise the same child's assignment, then confirm the child received the change.\n\
If a finding exceeds the assignment, pause that expansion and seek the user's approval or a separate focused task before proceeding. Never infer broader authority from a child report.\n\
Verify child evidence, resolve conflicting results, and report completed work, remaining work, and risks with clear provenance.";

const ROOT_PLANNING_GUIDANCE: &str = "For uncertain or multi-step work, use SubAgent's read-only explorer role and review its findings before assigning implementation.";

const ULTRA_ROOT_GUIDANCE: &str = "Ultra Root thinking mode.\n\
Actively look for useful independent work. Launch ready independent SubAgent assignments in parallel within the available capacity and budget. While children run, dispatch additional ready assignments and advance your own independent work. Make authorized file edits directly when they are quicker or unblock the work. Sequence jobs competing for the same constrained compute.\n\
Use SubAgent's read-only explorer role to resolve uncertainty. Its findings are prerequisites only for the work that depends on them. Wait when a required child result blocks further useful work. Direct execution or one child can be enough for simple or sequential tasks.\n\
Use the highest reasoning tier available on the primary model.";

const SUPERVISOR_DELEGATION_GUIDANCE: &str = "Supervisor delegation-first policy for Standard and Ultra\n\
Own scope, planning, dispatch, progress, verification and final synthesis. Use bounded Session/Work/plan views here. Delegate execution, including simple implementation tasks, to authorized independent Sessions. Send workspace reads, searches, writes, images, shell and process operations through workers, including work that uses a Skill, Workflow, browser or external tool. Give each worker acceptance criteria and non-goals. Dispatch independent slices in parallel with disjoint writes. Sequence dependent slices and overlapping writes. This role takes precedence over general execution guidance.\n\
Use canonical Host views for the current application user's Projects and Sessions. Report an older per-Project restriction as a Host migration blocker. Track parallel workstreams with session_control directory and verify candidates with session_history list and bounded inspect. Respect newer Human engagement as a takeover. Follow up a user-engaged Session again only when the user asks. Use authoritative Run evidence for completion.\n\
Use SubAgent only for auxiliary planning, review or verification. Select the read-only explorer role for planning. Continue or correct the same child by ActorId. Explicitly authorize scoped nested delegation where needed. Use work_dispatch only for existing explicit Work. Keep the coordinating role with the Supervisor.\n\
Use discovery for a discoverable route. Tool exposure describes the catalog. Use Host admission to verify target scope and worker readiness. When no authorized route or worker fits, report the specific blocker. Inspect the existing assignment or receipt before retrying. Reuse the complete request and operation UUID on an uncertain response.";

pub(crate) fn build_root_orchestration_context_block(session: &Session) -> Option<ContextBlock> {
    if session.supervisor_incarnation_id().is_some() {
        let routing = bamboo_orchestration::managed_session::SUPERVISOR_SESSION_ROUTING_GUIDANCE;
        let handoff = bamboo_orchestration::managed_session::SUPERVISOR_HANDOFF_GUIDANCE;
        let mut guidance = format!("{SUPERVISOR_DELEGATION_GUIDANCE}\n\n{routing}\n\n{handoff}");
        if session.root_orchestration_only_enabled() {
            guidance.push_str("\n\nIn Ultra, prefer parallel auxiliary planning and verification. Actively look for independent assignments.");
        }
        return Some(ContextBlock::new(
            ContextBlockType::RootOrchestration,
            ContextBlockPriority::Critical,
            ContextBlockStability::SessionStable,
            "Supervisor Delegation Policy",
            guidance,
        ));
    }
    (session.root_orchestration_prompt_enabled() || session.root_orchestration_only_enabled()).then(
        || {
            let guidance = if session.root_orchestration_only_enabled() {
                format!("{ULTRA_ROOT_GUIDANCE}\n\n{ROOT_ORCHESTRATION_INTRO}\n{ROOT_ORCHESTRATION_GUIDANCE}")
            } else {
                format!("{ROOT_ORCHESTRATION_INTRO}\n{ROOT_PLANNING_GUIDANCE}\n{ROOT_ORCHESTRATION_GUIDANCE}")
            };
            ContextBlock::new(
                ContextBlockType::RootOrchestration,
                ContextBlockPriority::Critical,
                ContextBlockStability::SessionStable,
                "Root Delegation Mode",
                guidance,
            )
        },
    )
}

/// Render current tool exposure after ledger reconciliation. The caller adds
/// this read model to the request tail so it cannot replay as historical state.
pub(crate) fn build_supervisor_route_observation(
    session: &Session,
    tool_schemas: &[ToolSchema],
    loading_mode: CapabilityLoadingMode,
) -> Option<ContextBlock> {
    session.supervisor_incarnation_id()?;
    let callable = crate::runtime::runner::loop_execution::effective_callable_set_for_round(
        session,
        tool_schemas,
        loading_mode,
    );
    let route = |name| {
        tool_schemas.iter().find(|schema| {
            schema.function.name == name && session.allows_model_tool_execution(name)
        })
    };
    let exposure = |schema: Option<&ToolSchema>| match schema {
        Some(schema) if callable.contains_execution_name(&schema.function.name) => "callable",
        Some(_) => "discoverable",
        None => "unavailable",
    };
    let session_control = route("session_control");
    let control_action =
        |action| {
            exposure(session_control.filter(|schema| {
                super::tool_schemas::offers_session_control_action(schema, action)
            }))
        };
    let observation = serde_json::json!({
        "main_execution": {
            "spawn_session": exposure(route("spawn_session")),
            "session_control.create": control_action("create"),
            "session_control.followup": control_action("followup"),
            "session_message.send": exposure(route("session_message")),
            "work_dispatch": exposure(route("work_dispatch")),
        },
        "auxiliary": { "SubAgent": exposure(route("SubAgent")) },
    });
    Some(ContextBlock::new(
        ContextBlockType::RootOrchestration,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Supervisor Delegation Routes",
        format!("Current delegation route observation: {observation}"),
    ))
}

#[cfg(test)]
#[test]
fn root_orchestration_guidance_is_bounded_and_schema_independent() {
    let shared_len = ROOT_ORCHESTRATION_INTRO.len() + ROOT_ORCHESTRATION_GUIDANCE.len();
    assert!(shared_len + ROOT_PLANNING_GUIDANCE.len() + 2 <= 1_200);
    assert!(shared_len + ULTRA_ROOT_GUIDANCE.len() + 3 <= 1_800);
    assert!(ROOT_ORCHESTRATION_INTRO.contains("parallel children"));
    for concept in ["progress", "Correct", "scope", "approval", "evidence"] {
        assert!(
            ROOT_ORCHESTRATION_GUIDANCE.contains(concept),
            "missing {concept}"
        );
    }
    assert!(!ROOT_ORCHESTRATION_GUIDANCE.contains("\"action\""));
    assert!(!ROOT_ORCHESTRATION_GUIDANCE.contains("\"intent\""));
}

#[cfg(test)]
#[test]
fn orchestration_only_root_receives_guidance_without_prompt_only_selection() {
    let mut root = Session::new("mode-only-root", "model");
    assert!(!root.root_orchestration_prompt_enabled());
    assert!(build_root_orchestration_context_block(&root).is_none());

    root.set_root_orchestration_only(true)
        .expect("ordinary Root may select orchestration-only mode");
    let restored: Session =
        serde_json::from_str(&serde_json::to_string(&root).expect("serialize selected Root"))
            .expect("restore selected Root");
    assert!(!restored.root_orchestration_prompt_enabled());
    let guidance = build_root_orchestration_context_block(&restored)
        .expect("durable orchestration-only mode supplies delegation guidance");
    assert_eq!(guidance.block_type, ContextBlockType::RootOrchestration);
    assert!(guidance.content.contains("read-only explorer role"));
    assert!(guidance.content.contains("Ultra Root thinking mode"));
    assert!(guidance
        .content
        .contains("Use the highest reasoning tier available on the primary model"));
    for concept in [
        "Actively look for useful independent work",
        "Launch ready independent SubAgent assignments in parallel",
        "within the available capacity and budget",
        "While children run, dispatch additional ready assignments and advance your own independent work",
        "Make authorized file edits directly",
        "prerequisites only for the work that depends on them",
        "Wait when a required child result blocks further useful work",
        "one child can be enough for simple or sequential tasks",
    ] {
        assert!(guidance.content.contains(concept), "missing {concept}");
    }
    assert!(!guidance.content.contains(ROOT_PLANNING_GUIDANCE));

    let child = Session::new_child_of("child", &restored, "model", "worker");
    assert!(build_root_orchestration_context_block(&child).is_none());

    let mut disabled = restored;
    disabled
        .set_root_orchestration_only(false)
        .expect("Root may disable orchestration-only mode");
    assert!(build_root_orchestration_context_block(&disabled).is_none());
    disabled.set_root_orchestration_prompt_enabled(true);
    let prompt_only = build_root_orchestration_context_block(&disabled).unwrap();
    assert!(!prompt_only.content.contains("Ultra Root thinking mode"));
    assert!(prompt_only.content.contains(ROOT_PLANNING_GUIDANCE));
}

#[cfg(test)]
#[test]
fn supervisor_delegation_first_guidance_is_identity_derived_and_survives_mode_changes() {
    use bamboo_domain::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
    let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    for ultra in [false, true, false] {
        supervisor.set_root_orchestration_only(ultra).unwrap();
        let restored: Session =
            serde_json::from_value(serde_json::to_value(&supervisor).unwrap()).unwrap();
        let block = build_root_orchestration_context_block(&restored)
            .expect("trusted Supervisor always receives its delegation contract");
        assert_eq!(block.stability, ContextBlockStability::SessionStable);
        assert!(block.content.len() <= 3_900);
        assert_eq!(
            block
                .content
                .contains("In Ultra, prefer parallel auxiliary"),
            ultra
        );
        assert!(!block
            .content
            .contains("Make authorized file edits directly"));
        assert!(block.content.contains("before retrying"));
        for concept in [
            "Supervisor",
            "Delegate execution",
            "bounded Session/Work/plan views",
            "current application user",
            "unavailable",
            "blocker",
            "same child",
            "nested delegation",
            "assigned acceptance criteria",
            "reject the exact current submission",
            "Verify the corrected result",
            "wait for explicit User acceptance of the exact current submission",
            "Dispatch independent slices in parallel with disjoint writes",
            "parallel workstreams",
            "session_control directory",
            "Follow up a user-engaged Session again only when the user asks",
            "Use authoritative Run evidence for completion",
        ] {
            assert!(block.content.contains(concept), "missing {concept}");
        }
        assert!(!block.content.contains("dedicated Read/Grep/Glob"));
        assert!(!block.content.contains("owner must explicitly attach"));
        let child = Session::new_child_of("child", &restored, "model", "worker");
        assert!(build_root_orchestration_context_block(&child).is_none());
    }
    let mut ordinary = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    ordinary.metadata.insert("role".into(), "supervisor".into());
    assert!(build_root_orchestration_context_block(&ordinary).is_none());
}

#[cfg(test)]
#[test]
fn supervisor_delegation_first_reports_each_main_and_auxiliary_route() {
    use bamboo_domain::{
        CapabilityLoadingMode, SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID,
    };
    let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    let tools = crate::session_app::approval_replay::framework_fixture::FixtureTools::new(&[
        "Read",
        "spawn_session",
        "session_control",
        "SubAgent",
        "vendor::SubAgent",
        "work_dispatch",
    ]);
    let config = crate::runtime::config::AgentLoopConfig {
        disabled_tools: ["session_control".into()].into(),
        ..Default::default()
    };
    for ultra in [false, true] {
        supervisor.set_root_orchestration_only(ultra).unwrap();
        let catalog = super::tool_schemas::resolve_available_tool_schemas_for_session(
            &config,
            tools.as_ref(),
            &supervisor,
        );
        let project_catalog = super::tool_schemas::resolve_available_tool_schemas_for_session(
            &Default::default(),
            tools.as_ref(),
            &supervisor,
        );
        let observe = |schemas: &[ToolSchema], mode| {
            let block = build_supervisor_route_observation(&supervisor, schemas, mode).unwrap();
            assert_eq!(block.stability, ContextBlockStability::RoundDynamic);
            let json = block.content.split_once(": ").unwrap().1;
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        };
        for mode in [
            CapabilityLoadingMode::LegacyFullCatalog,
            CapabilityLoadingMode::Progressive,
            CapabilityLoadingMode::StickyFallback,
        ] {
            let deferred_tool = if mode == CapabilityLoadingMode::LegacyFullCatalog {
                "callable"
            } else {
                "discoverable"
            };
            let full = observe(catalog.as_slice(), mode);
            assert_eq!(full["main_execution"]["work_dispatch"], "callable");
            assert_eq!(
                full["main_execution"]["session_control.create"],
                "unavailable"
            );
            assert_eq!(full["main_execution"]["spawn_session"], deferred_tool);
            assert_eq!(full["auxiliary"]["SubAgent"], deferred_tool);

            let standalone = catalog
                .iter()
                .find(|schema| schema.function.name == "spawn_session")
                .unwrap();
            let standalone = observe(std::slice::from_ref(standalone), mode);
            assert_eq!(standalone["main_execution"]["spawn_session"], deferred_tool);
            assert_eq!(standalone["auxiliary"]["SubAgent"], "unavailable");

            let child = catalog
                .iter()
                .find(|schema| schema.function.name == "SubAgent")
                .unwrap();
            let auxiliary = observe(std::slice::from_ref(child), mode);
            assert_eq!(auxiliary["auxiliary"]["SubAgent"], deferred_tool);
            assert!(auxiliary["main_execution"]
                .as_object()
                .unwrap()
                .values()
                .all(|state| state == "unavailable"));

            let blocked = observe(&[], mode);
            assert!(blocked["main_execution"]
                .as_object()
                .unwrap()
                .values()
                .all(|state| state == "unavailable"));
            assert_eq!(blocked["auxiliary"]["SubAgent"], "unavailable");

            let project = observe(&project_catalog, mode);
            assert_eq!(
                project["main_execution"]["session_control.create"],
                deferred_tool
            );
            assert_eq!(
                project["main_execution"]["session_control.followup"],
                "unavailable"
            );
        }
    }
    let ordinary = Session::new("ordinary", "model");
    assert!(
        build_supervisor_route_observation(&ordinary, &[], CapabilityLoadingMode::Progressive)
            .is_none()
    );
}

/// Build the single provider-visible Workspace block from authoritative
/// session metadata. Project identity is included only in its redacted,
/// path-free form so the active workspace path appears exactly once.
pub(crate) fn build_workspace_context_block(session: &Session) -> Option<ContextBlock> {
    let workspace = super::prompt_setup::workspace_context_from_session(session)?;
    let project = session
        .metadata
        .get(crate::project_context::PROJECT_CONTEXT_RENDERED_KEY)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let content = project
        .into_iter()
        .chain(std::iter::once(workspace.trim()))
        .collect::<Vec<_>>()
        .join("\n\n");

    Some(ContextBlock::new(
        ContextBlockType::Workspace,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Project & Workspace",
        content,
    ))
}

/// Build repository instructions from the authoritative workspace metadata,
/// never from a marker parsed out of System text.
pub(crate) fn build_instruction_overlay_context_block(session: &Session) -> Option<ContextBlock> {
    let workspace = session.workspace_path_meta()?;
    let content =
        crate::runtime::context::instruction::build_instruction_prompt_context(workspace.trim())?;
    Some(ContextBlock::new(
        ContextBlockType::InstructionOverlay,
        ContextBlockPriority::Critical,
        ContextBlockStability::RoundDynamic,
        "Project Instructions",
        content,
    ))
}

#[cfg(test)]
pub(crate) fn render_context_block_message(block: &ContextBlock) -> Message {
    block.render_runtime_context_message()
}

/// Assemble a [`PromptEnvelope`] from the stable frame and the per-round dynamic
/// context blocks. The conversation window is threaded directly into the IR by the
/// caller, so it is not stored here.
#[cfg(test)]
pub(crate) fn assemble_prompt_envelope(
    stable: StablePromptFrame,
    dynamic_blocks: Vec<ContextBlock>,
) -> PromptEnvelope {
    let dynamic_context_messages: Vec<Message> = dynamic_blocks
        .iter()
        .map(render_context_block_message)
        .collect();

    PromptEnvelope {
        stable_instructions: stable.stable_instructions,
        stable_prefix_messages: stable.stable_prefix_messages,
        dynamic_context_messages,
    }
}

pub(crate) fn build_task_list_context_block(session: &Session) -> Option<ContextBlock> {
    let content = session.format_task_list_for_prompt();
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }

    let title = session
        .task_list
        .as_ref()
        .map(|task_list| format!("Current Task List: {}", task_list.title.trim()))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Current Task List".to_string());

    Some(ContextBlock::new(
        ContextBlockType::TaskSnapshot,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        title,
        trimmed,
    ))
}

/// Rebuild the existing durable WorkflowRuntime block through the legacy adapter.
pub(crate) fn build_active_workflow_context_block(session: &Session) -> Option<ContextBlock> {
    super::legacy_instruction::active_context_block(session)
}

/// Build the per-round session-goal block directly from the active goal.
///
/// Placed by the caller in the volatile tail (alongside task/memory/plan) so the
/// goal — which changes per session/round — never sits in the cached system
/// prefix. Replaces the old `inject_goal_into_system_message` path, which leaked
/// the goal into the `base` system block. Returns `None` when there is no goal.
pub(crate) fn build_goal_context_block(goal: Option<&str>) -> Option<ContextBlock> {
    let objective = goal.map(str::trim).filter(|value| !value.is_empty())?;
    Some(ContextBlock::new(
        ContextBlockType::GoalState,
        ContextBlockPriority::Critical,
        ContextBlockStability::RoundDynamic,
        "Session Goal",
        crate::runtime::runner::prompt_context::render_goal_section(objective),
    ))
}

/// Build context injected by lifecycle hooks as a volatile block. The
/// source strings live in the current run's structured runtime state, so they
/// never mutate or invalidate the cached base system prompt.
pub(crate) fn build_agent_hook_context_block(session: &Session) -> Option<ContextBlock> {
    let mut contexts = session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.hook_contexts.clone())
        .unwrap_or_default();
    let plugin_contexts: Vec<String> = session
        .metadata
        .get("runtime.plugin_hook_contexts")
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default();
    contexts.extend(plugin_contexts);
    let content = contexts
        .iter()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    if content.is_empty() {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::AgentHookContext,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Session Hook Context",
        content,
    ))
}

/// Build the per-round plan-mode block directly from session state (the active
/// `PlanModeState`), replacing the legacy inject-into-system + reparse path.
/// Returns `None` when plan mode is inactive.
pub(crate) fn build_plan_mode_context_block(session: &Session) -> Option<ContextBlock> {
    let text = crate::runtime::runner::prompt_context::render_plan_mode_section(session)?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::PlanModeState,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Plan Mode State",
        trimmed,
    ))
}

/// Build the per-round durable plan-execution block directly from session state
/// plus persisted plan artifacts, replacing the legacy inject + reparse path.
/// Returns `None` when plan mode is inactive.
pub(crate) fn build_plan_runtime_context_block(
    session: &Session,
    app_data_dir: Option<&std::path::Path>,
) -> Option<ContextBlock> {
    let text =
        crate::runtime::runner::prompt_context::render_plan_runtime_section(session, app_data_dir)?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::PlanRuntimeState,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Durable Plan Execution Context",
        trimmed,
    ))
}

/// Build the volatile external-memory block from the session field that the async
/// refresh populates (external memory is the one ASYNC volatile producer).
/// Returns `None` when there is no external memory this round.
pub(crate) fn build_external_memory_context_block(session: &Session) -> Option<ContextBlock> {
    let content = crate::runtime::runner::prompt_context::render_external_memory_section(session)?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::ExternalMemory,
        ContextBlockPriority::Medium,
        ContextBlockStability::RoundDynamic,
        "External Memory (Persistent)",
        trimmed,
    ))
}

/// Per-round redacted Project resource inventory.
///
/// This intentionally rides the volatile tail instead of the system field:
/// watcher-driven `resource_revision` changes must not invalidate the
/// cacheable Project identity prefix.
pub(crate) fn build_project_resources_context_block(session: &Session) -> Option<ContextBlock> {
    let content = session
        .metadata
        .get(crate::project_context::PROJECT_RESOURCES_RENDERED_KEY)?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::ProjectResources,
        ContextBlockPriority::Medium,
        ContextBlockStability::RoundDynamic,
        "Project Shared Resources (redacted)",
        trimmed,
    ))
}

fn history_boundary_content(
    archive_boundaries: usize,
    archived_messages: usize,
    retained_recent_user_turns: usize,
) -> String {
    format!(
        "Earlier Session messages are stored exactly but omitted from the active model context.\n\
         Archive boundaries: {archive_boundaries}. Archived messages: {archived_messages}. \
         Recent complete user turns retained at the latest boundary: {retained_recent_user_turns}.\n\
         Use session_history_current with search_current to locate exact evidence, then \
         read_around or read_current for bounded raw history. Raw Session history is the \
         transcript authority. Memory is selective and may be stale; do not guess."
    )
}

fn history_boundary_block(
    archive_boundaries: usize,
    archived_messages: usize,
    retained_recent_user_turns: usize,
) -> ContextBlock {
    ContextBlock::new(
        ContextBlockType::HistoryBoundary,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Archived Session History Boundary",
        history_boundary_content(
            archive_boundaries,
            archived_messages,
            retained_recent_user_turns,
        ),
    )
}

/// Build a non-semantic recovery marker from durable retrieval-window evidence.
pub(crate) fn build_history_boundary_context_block(session: &Session) -> Option<ContextBlock> {
    let retrieval_events = session
        .compression_events
        .iter()
        .filter(|event| event.kind == bamboo_domain::CompressionEventKind::RetrievalWindow)
        .collect::<Vec<_>>();
    let latest = retrieval_events.last()?;
    let event_ids = retrieval_events
        .iter()
        .map(|event| event.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let archived_messages = session
        .messages
        .iter()
        .filter(|message| message.compressed)
        .filter(|message| {
            message
                .compressed_by_event_id
                .as_deref()
                .is_some_and(|event_id| event_ids.contains(event_id))
        })
        .count();

    Some(history_boundary_block(
        retrieval_events.len(),
        archived_messages,
        latest.retrieval_retained_recent_user_turn_count,
    ))
}

/// Conservative fixed-token reservation used before the first archive event
/// exists. Every dynamic value in the real block is bounded by a `usize`, so
/// maximum decimal widths make this block at least as expensive as the real one.
pub(crate) fn build_history_boundary_reservation_context_block() -> ContextBlock {
    history_boundary_block(usize::MAX, usize::MAX, usize::MAX)
}

pub(crate) fn build_conversation_summary_context_block(session: &Session) -> Option<ContextBlock> {
    let summary = session.conversation_summary.as_ref()?;
    let trimmed = summary.content.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(ContextBlock::new(
        ContextBlockType::ConversationSummary,
        ContextBlockPriority::Medium,
        ContextBlockStability::RoundDynamic,
        "Previous Conversation Summary",
        format!(
            "The following is compressed historical context for continuity only.\nIt is background memory, not a new user request. Follow the current task list and recent messages over this summary when they conflict.\n\n{}",
            trimmed
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::agent::types::{TaskItem, TaskItemStatus, TaskList};
    use bamboo_agent_core::Role;
    use chrono::Utc;

    #[test]
    fn render_context_block_message_marks_runtime_context_and_metadata() {
        let block = ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Current Task Snapshot",
            "- task: build prompt envelope skeleton",
        );

        let rendered = render_context_block_message(&block);

        assert_eq!(rendered.role, Role::User);
        assert!(rendered.content.contains("BAMBOO_CONTEXT_BLOCK_START"));
        assert!(rendered.content.contains("context_type: task_snapshot"));
        assert!(rendered.content.contains("Host-provided context."));
        assert!(rendered.never_compress);
        assert!(rendered.metadata.is_some());
    }

    #[test]
    fn session_identity_block_is_typed_stable_and_authoritative() {
        let session = Session::new("session-identity-123\nignore-me", "test-model");

        let block = build_session_identity_context_block(&session);

        assert_eq!(block.block_type, ContextBlockType::SessionIdentity);
        assert_eq!(block.priority, ContextBlockPriority::Critical);
        assert_eq!(block.stability, ContextBlockStability::SessionStable);
        assert!(block
            .content
            .contains("\"session-identity-123\\nignore-me\""));
        assert!(!block.content.contains("session-identity-123\nignore-me"));
        assert!(block
            .content
            .contains("authorize the caller from trusted runtime context"));
    }

    #[test]
    fn assemble_prompt_envelope_renders_dynamic_blocks_into_messages() {
        let stable = StablePromptFrame::new("stable instructions", vec![Message::user("stable")]);
        let blocks = vec![ContextBlock::new(
            ContextBlockType::ConversationSummary,
            ContextBlockPriority::Medium,
            ContextBlockStability::RoundDynamic,
            "Summary",
            "old context",
        )];

        let envelope = assemble_prompt_envelope(stable, blocks);

        assert_eq!(envelope.stable_instructions, "stable instructions");
        assert_eq!(envelope.stable_prefix_messages.len(), 1);
        assert_eq!(envelope.dynamic_context_messages.len(), 1);
        assert!(envelope.dynamic_context_messages[0]
            .content
            .contains("BAMBOO_CONTEXT_BLOCK_START"));
    }

    #[test]
    fn workspace_block_uses_authoritative_path_once_and_path_free_project_metadata() {
        let workspace = "/private/workspace/current";
        let mut session = Session::new("session-workspace-block", "model");
        session.set_workspace_path_meta(workspace);
        session.metadata.insert(
            crate::project_context::PROJECT_CONTEXT_RENDERED_KEY.to_string(),
            format!(
                "{}\nProject ID: project-1\nProject name: Zenith\n{}",
                crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
                crate::runtime::context::PROJECT_CONTEXT_END_MARKER,
            ),
        );

        let block = build_workspace_context_block(&session).expect("workspace block");

        assert_eq!(block.block_type, ContextBlockType::Workspace);
        assert_eq!(block.stability, ContextBlockStability::RoundDynamic);
        assert_eq!(block.content.matches(workspace).count(), 1);
        assert!(block.content.contains("Project ID: project-1"));
        assert!(!block.content.contains("Project path:"));
        assert!(!block.content.contains("Project home"));
    }

    #[test]
    fn build_task_list_context_block_uses_formatted_prompt_content() {
        let mut session = Session::new("session-task-block", "model");
        session.task_list = Some(TaskList {
            session_id: session.id.clone(),
            title: "Agent Tasks".to_string(),
            items: vec![TaskItem {
                id: "task-1".to_string(),
                description: "Implement prompt envelope".to_string(),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let block = build_task_list_context_block(&session).expect("task block should exist");

        assert_eq!(block.block_type, ContextBlockType::TaskSnapshot);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert!(block.content.contains("Current Task List"));
        assert!(block.content.contains("Implement prompt envelope"));
    }

    #[test]
    fn build_external_memory_context_block_reads_session_field() {
        let mut session = Session::new("session-external-memory-block", "model");
        session.metadata.insert(
            crate::runtime::runner::prompt_context::EXTERNAL_MEMORY_RENDERED_KEY.to_string(),
            "## External Memory (Persistent)\n\nSession note body".to_string(),
        );

        let block = build_external_memory_context_block(&session)
            .expect("external memory block should exist");

        assert_eq!(block.block_type, ContextBlockType::ExternalMemory);
        assert_eq!(block.priority, ContextBlockPriority::Medium);
        assert!(block.content.contains("## External Memory (Persistent)"));
        assert!(block.content.contains("Session note body"));
        // No external memory field → no block.
        assert!(build_external_memory_context_block(&Session::new("s2", "model")).is_none());
    }

    #[test]
    fn project_resources_context_block_is_round_dynamic_and_redacted() {
        let mut session = Session::new("project-resources", "model");
        session.metadata.insert(
            crate::project_context::PROJECT_RESOURCES_RENDERED_KEY.to_string(),
            "Project ID: project-1\nResource revision: 3\n- Skills: status=available, items=2"
                .to_string(),
        );
        let block =
            build_project_resources_context_block(&session).expect("project resource block");
        assert_eq!(block.block_type, ContextBlockType::ProjectResources);
        assert_eq!(block.stability, ContextBlockStability::RoundDynamic);
        assert!(block.content.contains("Resource revision: 3"));
        assert!(!block.content.contains("secret"));
    }

    #[test]
    fn build_agent_hook_context_block_reads_runtime_state_as_volatile_context() {
        let mut session = Session::new("session-hook-block", "model");
        let mut state = bamboo_domain::AgentRuntimeState::new("run-hook");
        state.hook_contexts = vec!["first hook context".to_string(), "second".to_string()];
        session.agent_runtime_state = Some(state);

        let block = build_agent_hook_context_block(&session).expect("hook block should exist");

        assert_eq!(block.block_type, ContextBlockType::AgentHookContext);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert_eq!(block.stability, ContextBlockStability::RoundDynamic);
        assert!(block.content.contains("first hook context"));
        assert!(block.content.contains("second"));
        assert!(build_agent_hook_context_block(&Session::new("empty", "model")).is_none());
    }

    #[test]
    fn build_plan_mode_context_block_renders_from_session_state() {
        use bamboo_domain::session::runtime_state::{
            AgentRuntimeState, PlanModeState, PlanModeStatus,
        };
        let mut session = Session::new("session-plan-mode-block", "model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        });

        let block = build_plan_mode_context_block(&session).expect("plan mode block should exist");

        assert_eq!(block.block_type, ContextBlockType::PlanModeState);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert!(block.content.contains("PLAN MODE ACTIVE"));
        // Inactive plan mode → no block.
        assert!(build_plan_mode_context_block(&Session::new("s2", "model")).is_none());
    }

    #[test]
    fn build_plan_runtime_context_block_renders_from_session_state() {
        use bamboo_domain::session::runtime_state::{
            AgentRuntimeState, PlanModeState, PlanModeStatus,
        };
        let mut session = Session::new("session-plan-runtime-block", "model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Designing,
        });

        let block = build_plan_runtime_context_block(&session, None)
            .expect("plan runtime block should exist");

        assert_eq!(block.block_type, ContextBlockType::PlanRuntimeState);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert!(block.content.contains("DURABLE PLAN EXECUTION CONTEXT"));
        // Inactive plan mode → no block.
        assert!(build_plan_runtime_context_block(&Session::new("s2", "model"), None).is_none());
    }

    #[test]
    fn build_conversation_summary_context_block_wraps_summary_content() {
        let mut session = Session::new("session-summary-block", "model");
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Older work was compressed.",
            3,
            120,
        ));

        let block =
            build_conversation_summary_context_block(&session).expect("summary block should exist");

        assert_eq!(block.block_type, ContextBlockType::ConversationSummary);
        assert_eq!(block.priority, ContextBlockPriority::Medium);
        assert!(block.content.contains("compressed historical context"));
        assert!(block.content.contains("Older work was compressed."));
    }

    #[test]
    fn history_boundary_rehydrates_without_raw_history_content() {
        let mut session = Session::new("session-history-boundary", "model");
        let mut event = bamboo_domain::CompressionEvent::new(
            1,
            1,
            82.0,
            58.0,
            0,
            bamboo_domain::CompressionTriggerType::Auto,
            0.0,
            None,
            0,
        );
        event.kind = bamboo_domain::CompressionEventKind::RetrievalWindow;
        event.retrieval_retained_recent_user_turn_count = 3;
        let event_id = event.id.clone();
        let mut archived = Message::user("private archived transcript body");
        archived.compressed = true;
        archived.compressed_by_event_id = Some(event_id);
        session.messages.push(archived);
        session.compression_events.push(event);

        let block = build_history_boundary_context_block(&session).expect("history boundary");
        assert_eq!(block.block_type, ContextBlockType::HistoryBoundary);
        assert_eq!(block.stability, ContextBlockStability::RoundDynamic);
        assert!(block.content.contains("Archived messages: 1"));
        assert!(block.content.contains("session_history_current"));
        assert!(block
            .content
            .contains("Memory is selective and may be stale"));
        assert!(!block.content.contains("private archived transcript body"));

        let bytes = serde_json::to_vec(&session).unwrap();
        let restored: Session = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            build_history_boundary_context_block(&restored)
                .expect("boundary after reload")
                .content,
            block.content
        );
        assert!(
            build_history_boundary_reservation_context_block()
                .content
                .len()
                >= block.content.len()
        );
    }

    #[test]
    fn history_boundary_is_absent_without_retrieval_event() {
        assert!(build_history_boundary_context_block(&Session::new("plain", "model")).is_none());
    }
}

#[cfg(test)]
#[test]
fn supervisor_task_routing_policy_keeps_children_auxiliary_and_scope_explicit() {
    use bamboo_domain::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
    let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
    supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    for ultra in [false, true] {
        supervisor.set_root_orchestration_only(ultra).unwrap();
        let guidance = build_root_orchestration_context_block(&supervisor)
            .unwrap()
            .content;
        for concept in [
            "Act only on requested assignments",
            "their own fresh Host notifications",
            "same verified assignment",
            "new independent Session for a separate assignment or requested context isolation",
            "intended Project and workspace",
            "Project is optional",
            "omit both project_id and workspace_path",
            "the user identifies it for this assignment",
            "Ask one focused question if a required workspace is unclear",
            "session_history list",
            "bounded inspect",
            "to authorized independent Sessions",
            "auxiliary planning",
            "Use session_control followup",
            "current application user",
            "When no authorized route or worker fits, report the specific blocker",
        ] {
            assert!(guidance.contains(concept), "missing {concept}");
        }
        assert!(!guidance.contains("explicitly attach"));
        assert!(
            guidance.find("session_history list").unwrap()
                < guidance.find("auxiliary planning").unwrap()
        );
        let child = Session::new_child_of("policy-child", &supervisor, "model", "Child");
        assert!(build_root_orchestration_context_block(&child).is_none());
    }
}
