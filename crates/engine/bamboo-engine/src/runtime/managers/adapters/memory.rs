use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::Session;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::memory::MemoryManager;

/// Default memory manager that delegates to existing runner functions.
pub struct DefaultMemoryManager;

#[async_trait]
impl MemoryManager for DefaultMemoryManager {
    async fn recall_memories(&self, session: &mut Session, config: &AgentLoopConfig) -> bool {
        let msg_count_before = session.messages.len();
        crate::runtime::runner::prompt_context::refresh_external_memory_context(
            session,
            config.prompt_memory_flags,
            None,
            config.project_context_resolver.as_deref(),
            config.app_data_dir.as_deref(),
        )
        .await;
        session.messages.len() > msg_count_before
    }

    async fn maybe_compress(
        &self,
        _session: &mut Session,
        _config: &AgentLoopConfig,
        _phase: &str,
        _session_id: &str,
        _model_name: &str,
        _tool_schemas: &[ToolSchema],
    ) -> bool {
        // Actual compression happens via the LLM manager's mid-turn path.
        false
    }
}
