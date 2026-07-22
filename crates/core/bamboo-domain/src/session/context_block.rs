use crate::session::types::Message;
use serde::{Deserialize, Serialize};

const CONTEXT_BLOCK_START_MARKER: &str = "<!-- BAMBOO_CONTEXT_BLOCK_START -->";
const CONTEXT_BLOCK_END_MARKER: &str = "<!-- BAMBOO_CONTEXT_BLOCK_END -->";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBlock {
    pub block_type: ContextBlockType,
    pub priority: ContextBlockPriority,
    pub stability: ContextBlockStability,
    pub title: String,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
}

impl ContextBlock {
    pub fn new(
        block_type: ContextBlockType,
        priority: ContextBlockPriority,
        stability: ContextBlockStability,
        title: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            block_type,
            priority,
            stability,
            title: title.into(),
            content: content.into(),
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: Option<serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn render_runtime_context_text(&self) -> String {
        format!(
            "{CONTEXT_BLOCK_START_MARKER}\n\
context_type: {}\n\
priority: {}\n\
stability: {}\n\
title: {}\n\n\
This is runtime context from the host system.\n\
It is not a new user request.\n\
Follow the latest real user request and recent tool results over this block when they conflict.\n\n\
{}\n\
{CONTEXT_BLOCK_END_MARKER}",
            self.block_type.as_str(),
            self.priority.as_str(),
            self.stability.as_str(),
            self.title.trim(),
            self.content.trim(),
        )
    }

    pub fn render_runtime_context_message(&self) -> Message {
        let mut message = Message::user(self.render_runtime_context_text());
        message.metadata = Some(serde_json::json!({
            "bamboo_context_block": {
                "type": self.block_type,
                "priority": self.priority,
                "stability": self.stability,
                "title": self.title,
            }
        }));
        message.never_compress = true;
        message
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextBlockType {
    /// System base identity / persona (the `base` system block).
    Base,
    /// Framework-invariant operating directives folded on top of base.
    CoreDirectives,
    Workspace,
    InstructionOverlay,
    ToolGuide,
    SkillContext,
    WorkflowRuntime,
    ConversationSummary,
    TaskSnapshot,
    ExternalMemory,
    MemoryRecall,
    PlanModeState,
    PlanRuntimeState,
    /// Per-round session-goal block (placed in the volatile tail so a goal
    /// change never invalidates the cached prefix).
    GoalState,
    /// Context injected by session lifecycle hooks for the current run.
    AgentHookContext,
    EnvSnapshot,
    RecoverySnapshot,
}

impl ContextBlockType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::CoreDirectives => "core_directives",
            Self::Workspace => "workspace",
            Self::InstructionOverlay => "instruction_overlay",
            Self::ToolGuide => "tool_guide",
            Self::SkillContext => "skill_context",
            Self::WorkflowRuntime => "workflow_runtime",
            Self::ConversationSummary => "conversation_summary",
            Self::TaskSnapshot => "task_snapshot",
            Self::ExternalMemory => "external_memory",
            Self::MemoryRecall => "memory_recall",
            Self::PlanModeState => "plan_mode_state",
            Self::PlanRuntimeState => "plan_runtime_state",
            Self::GoalState => "goal_state",
            Self::AgentHookContext => "agent_hook_context",
            Self::EnvSnapshot => "env_snapshot",
            Self::RecoverySnapshot => "recovery_snapshot",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextBlockPriority {
    Critical,
    High,
    Medium,
    Low,
}

impl ContextBlockPriority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextBlockStability {
    Stable,
    SessionStable,
    RoundDynamic,
}

impl ContextBlockStability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::SessionStable => "session_stable",
            Self::RoundDynamic => "round_dynamic",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_runtime_context_message_marks_metadata_and_never_compress() {
        let block = ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Current Task Snapshot",
            "- build prompt envelope skeleton",
        );

        let rendered = block.render_runtime_context_message();

        assert!(rendered.content.contains("BAMBOO_CONTEXT_BLOCK_START"));
        assert!(rendered.content.contains("context_type: task_snapshot"));
        assert!(rendered.never_compress);
        assert!(rendered.metadata.is_some());
    }
}
