use async_trait::async_trait;
use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, Session};

/// Result of a mini-loop fast decision.
#[derive(Debug, Clone)]
pub struct MiniLoopDecision {
    pub answer: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

/// Executor for cheap model fast decisions.
///
/// Used for task evaluation, compression decisions, routing,
/// retry classification, and other lightweight LLM-powered judgments
/// that should not consume the main model's budget or streaming path.
#[async_trait]
pub trait MiniLoopExecutor: Send + Sync {
    /// Execute a fast decision using a cheap model.
    async fn decide(
        &self,
        session: &Session,
        prompt: &str,
        context: &str,
    ) -> Result<MiniLoopDecision, AgentError>;

    /// Evaluate task progress using a fast model.
    async fn evaluate_task(
        &self,
        session: &Session,
        tool_calls: &[ToolCall],
        round: usize,
    ) -> Result<MiniLoopDecision, AgentError>;
}
