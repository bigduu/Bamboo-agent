use async_trait::async_trait;
use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, Session};

use crate::runtime::managers::mini_loop::{MiniLoopDecision, MiniLoopExecutor};

/// Default mini-loop executor — placeholder that returns empty decisions.
///
/// In production, the agent loop uses direct LLM calls for task evaluation.
/// This adapter provides a no-op fallback when no custom executor is configured.
pub struct DefaultMiniLoopExecutor;

#[async_trait]
impl MiniLoopExecutor for DefaultMiniLoopExecutor {
    async fn decide(
        &self,
        _session: &Session,
        prompt: &str,
        _context: &str,
    ) -> Result<MiniLoopDecision, AgentError> {
        // No-op: return the prompt as the answer.
        Ok(MiniLoopDecision {
            answer: prompt.to_string(),
            prompt_tokens: 0,
            completion_tokens: 0,
        })
    }

    async fn evaluate_task(
        &self,
        _session: &Session,
        _tool_calls: &[ToolCall],
        _round: usize,
    ) -> Result<MiniLoopDecision, AgentError> {
        // No-op: return empty decision.
        Ok(MiniLoopDecision {
            answer: String::new(),
            prompt_tokens: 0,
            completion_tokens: 0,
        })
    }
}
