use bamboo_agent_core::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Session,
};

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

/// Rebuild the exact active instruction workflow from its durable LKG snapshot.
/// This is a dedicated host context block, never a synthetic user message in
/// session history and never a catalog/live-filesystem re-resolution.
pub(crate) fn build_active_workflow_context_block(session: &Session) -> Option<ContextBlock> {
    let durable = session
        .metadata
        .get(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_skills::DurableWorkflowActivation>(raw).ok())
        .filter(|durable| {
            durable.active.status == bamboo_skills::WorkflowActivationStatus::Active
        })?;
    let entry = durable.snapshot.skills.get(&durable.active.id)?;
    if durable.snapshot.skills.len() != 1
        || entry.revision != durable.active.revision
        || entry.catalog_entry.source != durable.active.source
        || entry.catalog_entry.kind != bamboo_skills::WorkflowKind::Instruction
    {
        return None;
    }
    let dynamic = durable
        .active
        .dynamic_context
        .iter()
        .map(|block| {
            serde_json::json!({
                "provider_id": block.provider_id,
                "provenance": block.provenance,
                "status": block.status,
                "content": block.content,
                "diagnostic": block.diagnostic,
            })
        })
        .collect::<Vec<_>>();
    Some(
        ContextBlock::new(
            ContextBlockType::WorkflowRuntime,
            ContextBlockPriority::Critical,
            ContextBlockStability::SessionStable,
            format!("Active Workflow: {}@{}", durable.active.id, durable.active.revision),
            format!(
                "workflow_id: {}\nsource: {:?}\nrevision: {}\nargs: {}\ncontext_fingerprint: {}\n\n### Instructions\n{}\n\n### Dynamic Context\n{}",
                durable.active.id,
                durable.active.source,
                durable.active.revision,
                durable.active.args,
                durable
                    .active
                    .context_fingerprint
                    .as_deref()
                    .unwrap_or("unavailable"),
                entry.definition.prompt,
                serde_json::to_string(&dynamic).unwrap_or_else(|_| "[]".to_string()),
            ),
        )
        .with_metadata(Some(serde_json::json!({
            "workflow_id": durable.active.id,
            "source": durable.active.source,
            "revision": durable.active.revision,
            "context_fingerprint": durable.active.context_fingerprint,
        }))),
    )
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

/// Build context injected by `SessionStart` hooks as a volatile block. The
/// source strings live in the current run's structured runtime state, so they
/// never mutate or invalidate the cached base system prompt.
pub(crate) fn build_agent_hook_context_block(session: &Session) -> Option<ContextBlock> {
    let contexts = &session.agent_runtime_state.as_ref()?.hook_contexts;
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
        assert!(rendered.content.contains("It is not a new user request."));
        assert!(rendered.never_compress);
        assert!(rendered.metadata.is_some());
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
}
