use crate::metrics::TokenUsage;
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;

/// Output from a single LLM round execution.
#[derive(Debug, Clone)]
pub struct LlmRoundOutput {
    pub content: String,
    pub reasoning_content: String,
    pub tool_calls: Vec<bamboo_agent_core::tools::ToolCall>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub response_id: Option<String>,
    pub round_usage: TokenUsage,
}

/// Manages LLM interaction: message preparation, stream handling, retry/fallback.
#[async_trait]
pub trait LlmManager: Send + Sync {
    /// Execute a single LLM round.
    #[allow(clippy::too_many_arguments)]
    async fn execute_round(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
        event_tx: &mpsc::Sender<AgentEvent>,
        cancel_token: &CancellationToken,
        session_id: &str,
        model_name: &str,
        tool_schemas: &[ToolSchema],
    ) -> Result<LlmRoundOutput, AgentError>;

    /// Attempt overflow recovery by compressing context.
    async fn attempt_overflow_recovery(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
        session_id: &str,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) -> Result<bool, AgentError>;
}
