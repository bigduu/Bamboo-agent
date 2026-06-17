use bamboo_agent_core::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Session,
};

/// Structured request envelope separating the stable instructions, the
/// session-stable prefix messages, and the per-round dynamic context blocks. The
/// engine reads these three runs straight into the canonical [`PromptIR`]; the
/// conversation window and the wire-specific projections live on the IR, not here.
#[derive(Debug, Clone, Default)]
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

pub(crate) fn render_context_block_message(block: &ContextBlock) -> Message {
    block.render_runtime_context_message()
}

/// Assemble a [`PromptEnvelope`] from the stable frame and the per-round dynamic
/// context blocks. The conversation window is threaded directly into the IR by the
/// caller, so it is not stored here.
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
