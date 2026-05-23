use bamboo_agent_core::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Session,
};
use serde::{Deserialize, Serialize};

/// Structured request envelope used to separate stable instructions,
/// dynamic runtime context, and conversation history before provider-specific
/// serialization.
#[derive(Debug, Clone, Default)]
pub struct PromptEnvelope {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
    pub dynamic_context_messages: Vec<Message>,
    pub conversation_messages: Vec<Message>,
    pub observability: PromptEnvelopeObservability,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptEnvelopeObservability {
    pub stable_instructions_chars: usize,
    pub stable_prefix_message_count: usize,
    pub dynamic_context_message_count: usize,
    pub conversation_message_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_context_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub included_block_types: Vec<ContextBlockType>,
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

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
const PLAN_MODE_START_MARKER: &str = "<!-- BAMBOO_PLAN_MODE_START -->";
const PLAN_MODE_END_MARKER: &str = "<!-- BAMBOO_PLAN_MODE_END -->";
const PLAN_RUNTIME_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->";
const PLAN_RUNTIME_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->";

pub(crate) fn render_context_block_message(block: &ContextBlock) -> Message {
    block.render_runtime_context_message()
}

/// Assemble a [`PromptEnvelope`] from stable frame, context blocks and the
/// final conversation message window.
pub(crate) fn assemble_prompt_envelope(
    stable: StablePromptFrame,
    dynamic_blocks: Vec<ContextBlock>,
    conversation_messages: Vec<Message>,
) -> PromptEnvelope {
    let dynamic_context_messages: Vec<Message> = dynamic_blocks
        .iter()
        .map(render_context_block_message)
        .collect();

    let observability = PromptEnvelopeObservability {
        stable_instructions_chars: stable.stable_instructions.len(),
        stable_prefix_message_count: stable.stable_prefix_messages.len(),
        dynamic_context_message_count: dynamic_context_messages.len(),
        conversation_message_count: conversation_messages.len(),
        stable_prefix_hash: None,
        dynamic_context_hash: None,
        included_block_types: dynamic_blocks.iter().map(|block| block.block_type).collect(),
    };

    PromptEnvelope {
        stable_instructions: stable.stable_instructions,
        stable_prefix_messages: stable.stable_prefix_messages,
        dynamic_context_messages,
        conversation_messages,
        observability,
    }
}

/// Convert a [`PromptEnvelope`] into a Chat Completions compatible message list.
///
/// This helper is intentionally lightweight for the skeleton phase and will be
/// expanded when the execution path is switched over to use envelopes.
pub(crate) fn envelope_to_chat_messages(envelope: &PromptEnvelope) -> Vec<Message> {
    let mut messages = Vec::new();

    if !envelope.stable_instructions.trim().is_empty() {
        messages.push(Message::system(envelope.stable_instructions.trim().to_string()));
    }

    messages.extend(envelope.stable_prefix_messages.clone());
    messages.extend(envelope.dynamic_context_messages.clone());
    messages.extend(envelope.conversation_messages.clone());

    messages
}

#[derive(Debug, Clone, Default)]
pub struct ResponsesPromptView {
    pub instructions: Option<String>,
    pub input_messages: Vec<Message>,
}

fn extract_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let section_end = section_start + end_rel_idx;
    let section = prompt[start_idx..section_end + end_marker.len()].trim();
    (!section.is_empty()).then(|| section.to_string())
}

fn strip_wrapped_markers(section: &str, start_marker: &str, end_marker: &str) -> String {
    section
        .trim()
        .strip_prefix(start_marker)
        .map(str::trim_start)
        .and_then(|value| value.strip_suffix(end_marker).map(str::trim_end))
        .unwrap_or(section)
        .trim()
        .to_string()
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

#[cfg(test)]
pub(crate) fn build_external_memory_context_block(session: &Session) -> Option<ContextBlock> {
    build_external_memory_context_block_from_messages(&session.messages)
}

pub(crate) fn build_external_memory_context_block_from_messages(
    messages: &[Message],
) -> Option<ContextBlock> {
    let system_message = messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))?;
    let content = extract_wrapped_section(
        &system_message.content,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )?;
    let trimmed = strip_wrapped_markers(
        &content,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    );
    if trimmed.trim().is_empty() {
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

pub(crate) fn build_plan_mode_context_block_from_messages(
    messages: &[Message],
) -> Option<ContextBlock> {
    let system_message = messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))?;
    let content = extract_wrapped_section(
        &system_message.content,
        PLAN_MODE_START_MARKER,
        PLAN_MODE_END_MARKER,
    )?;
    let trimmed = strip_wrapped_markers(&content, PLAN_MODE_START_MARKER, PLAN_MODE_END_MARKER);
    if trimmed.trim().is_empty() {
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

pub(crate) fn build_plan_runtime_context_block_from_messages(
    messages: &[Message],
) -> Option<ContextBlock> {
    let system_message = messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))?;
    let content = extract_wrapped_section(
        &system_message.content,
        PLAN_RUNTIME_CONTEXT_START_MARKER,
        PLAN_RUNTIME_CONTEXT_END_MARKER,
    )?;
    let trimmed = strip_wrapped_markers(
        &content,
        PLAN_RUNTIME_CONTEXT_START_MARKER,
        PLAN_RUNTIME_CONTEXT_END_MARKER,
    );
    if trimmed.trim().is_empty() {
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

/// Convert a [`PromptEnvelope`] into a Responses-style view.
pub(crate) fn envelope_to_responses_view(envelope: &PromptEnvelope) -> ResponsesPromptView {
    let instructions = envelope
        .stable_instructions
        .trim()
        .is_empty()
        .then_some(String::new())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let trimmed = envelope.stable_instructions.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });

    let mut input_messages = Vec::new();
    input_messages.extend(envelope.stable_prefix_messages.clone());
    input_messages.extend(envelope.dynamic_context_messages.clone());
    input_messages.extend(envelope.conversation_messages.clone());

    ResponsesPromptView {
        instructions,
        input_messages,
    }
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
        assert!(rendered
            .content
            .contains("It is not a new user request."));
        assert!(rendered.never_compress);
        assert!(rendered.metadata.is_some());
    }

    #[test]
    fn assemble_prompt_envelope_tracks_counts_and_block_types() {
        let stable = StablePromptFrame::new("stable instructions", vec![Message::user("stable")]);
        let blocks = vec![ContextBlock::new(
            ContextBlockType::ConversationSummary,
            ContextBlockPriority::Medium,
            ContextBlockStability::RoundDynamic,
            "Summary",
            "old context",
        )];
        let conversation = vec![Message::assistant("answer", None)];

        let envelope = assemble_prompt_envelope(stable, blocks, conversation);

        assert_eq!(envelope.observability.stable_prefix_message_count, 1);
        assert_eq!(envelope.observability.dynamic_context_message_count, 1);
        assert_eq!(envelope.observability.conversation_message_count, 1);
        assert_eq!(
            envelope.observability.included_block_types,
            vec![ContextBlockType::ConversationSummary]
        );
    }

    #[test]
    fn envelope_to_chat_messages_includes_single_stable_system_prefix() {
        let stable = StablePromptFrame::new("stable instructions", vec![Message::user("stable")]);
        let blocks = vec![ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Task",
            "do the thing",
        )];
        let envelope = assemble_prompt_envelope(stable, blocks, vec![Message::user("latest user")]);

        let messages = envelope_to_chat_messages(&envelope);

        assert!(matches!(messages.first().map(|m| &m.role), Some(Role::System)));
        assert_eq!(messages.len(), 4);
    }

    #[test]
    fn envelope_to_responses_view_uses_instructions_without_system_message() {
        let stable = StablePromptFrame::new("stable instructions", vec![Message::user("stable")]);
        let envelope = assemble_prompt_envelope(
            stable,
            vec![ContextBlock::new(
                ContextBlockType::TaskSnapshot,
                ContextBlockPriority::High,
                ContextBlockStability::RoundDynamic,
                "Task",
                "do the thing",
            )],
            vec![Message::user("latest user")],
        );

        let view = envelope_to_responses_view(&envelope);

        assert_eq!(view.instructions.as_deref(), Some("stable instructions"));
        assert_eq!(view.input_messages.len(), 3);
        assert!(!matches!(
            view.input_messages.first().map(|m| &m.role),
            Some(Role::System)
        ));
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
    fn build_external_memory_context_block_extracts_wrapped_system_section() {
        let mut session = Session::new("session-external-memory-block", "model");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nSession note body\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let block = build_external_memory_context_block(&session)
            .expect("external memory block should exist");

        assert_eq!(block.block_type, ContextBlockType::ExternalMemory);
        assert_eq!(block.priority, ContextBlockPriority::Medium);
        assert!(block.content.contains("## External Memory (Persistent)"));
        assert!(block.content.contains("Session note body"));
        assert!(!block.content.contains("BAMBOO_EXTERNAL_MEMORY_START"));
    }

    #[test]
    fn build_plan_mode_context_block_extracts_wrapped_system_section() {
        let messages = vec![Message::system(
            "Base prompt\n\n<!-- BAMBOO_PLAN_MODE_START -->\n=== PLAN MODE ACTIVE ===\n\nEXPLORE\n<!-- BAMBOO_PLAN_MODE_END -->",
        )];

        let block = build_plan_mode_context_block_from_messages(&messages)
            .expect("plan mode block should exist");

        assert_eq!(block.block_type, ContextBlockType::PlanModeState);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert!(block.content.contains("PLAN MODE ACTIVE"));
        assert!(!block.content.contains("BAMBOO_PLAN_MODE_START"));
    }

    #[test]
    fn build_plan_runtime_context_block_extracts_wrapped_system_section() {
        let messages = vec![Message::system(
            "Base prompt\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\n=== DURABLE PLAN EXECUTION CONTEXT ===\n\nResume rule\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->",
        )];

        let block = build_plan_runtime_context_block_from_messages(&messages)
            .expect("plan runtime block should exist");

        assert_eq!(block.block_type, ContextBlockType::PlanRuntimeState);
        assert_eq!(block.priority, ContextBlockPriority::High);
        assert!(block.content.contains("DURABLE PLAN EXECUTION CONTEXT"));
        assert!(!block.content.contains("BAMBOO_PLAN_RUNTIME_CONTEXT_START"));
    }

    #[test]
    fn build_conversation_summary_context_block_wraps_summary_content() {
        let mut session = Session::new("session-summary-block", "model");
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Older work was compressed.",
            3,
            120,
        ));

        let block = build_conversation_summary_context_block(&session)
            .expect("summary block should exist");

        assert_eq!(block.block_type, ContextBlockType::ConversationSummary);
        assert_eq!(block.priority, ContextBlockPriority::Medium);
        assert!(block.content.contains("compressed historical context"));
        assert!(block.content.contains("Older work was compressed."));
    }
}
