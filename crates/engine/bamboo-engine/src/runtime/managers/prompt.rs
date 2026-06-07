use async_trait::async_trait;
use bamboo_agent_core::Session;

use crate::runtime::config::AgentLoopConfig;

/// Result of prompt assembly.
#[derive(Debug, Clone)]
pub struct PromptAssemblyOutput {
    pub effective_system_prompt: String,
    pub skill_context: String,
    pub tool_guide_context: String,
}

/// Manages system prompt assembly.
///
/// Responsible for composing the effective system prompt from base prompt,
/// workspace context, instruction context, env context, skill context,
/// tool guide, and external memory sections.
#[async_trait]
pub trait PromptManager: Send + Sync {
    /// Assemble the full system prompt for the given session and config.
    async fn assemble_prompt(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
    ) -> PromptAssemblyOutput;

    /// Refresh external memory injection into an existing system message.
    async fn refresh_external_memory(&self, session: &mut Session, config: &AgentLoopConfig);

    /// Refresh the task list section in the system prompt.
    fn refresh_task_list(&self, session: &mut Session);
}
