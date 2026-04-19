use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::Session;

use crate::runtime::config::AgentLoopConfig;

/// Manages external memory recall, compression, and summarization.
#[async_trait]
pub trait MemoryManager: Send + Sync {
    /// Recall relevant memories for the current context.
    /// Returns `true` if any memories were injected.
    async fn recall_memories(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
    ) -> bool;

    /// Check if compression should be applied and apply it.
    /// Returns `true` if compression was performed.
    #[allow(clippy::too_many_arguments)]
    async fn maybe_compress(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
        phase: &str,
        session_id: &str,
        model_name: &str,
        tool_schemas: &[ToolSchema],
    ) -> bool;
}
