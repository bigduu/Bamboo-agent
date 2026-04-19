use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_infrastructure::LLMProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::llm::{LlmManager, LlmRoundOutput};

/// Default LLM manager that delegates to existing runner functions.
pub struct DefaultLlmManager {
    llm: Arc<dyn LLMProvider>,
}

impl DefaultLlmManager {
    pub fn new(llm: Arc<dyn LLMProvider>) -> Self {
        Self { llm }
    }
}

#[async_trait]
impl LlmManager for DefaultLlmManager {
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
    ) -> Result<LlmRoundOutput, AgentError> {
        let result = crate::runtime::runner::round_lifecycle::execute_llm_round(
            session,
            config,
            &self.llm,
            event_tx,
            cancel_token,
            session_id,
            model_name,
            tool_schemas,
        )
        .await?;

        let (content, reasoning_content, tool_calls) = {
            let stream = &result.stream_output;
            (
                stream.content.clone(),
                stream.reasoning_content.clone(),
                stream.tool_calls.clone(),
            )
        };

        Ok(LlmRoundOutput {
            content,
            reasoning_content,
            tool_calls,
            prompt_tokens: result.prompt_tokens,
            completion_tokens: result.completion_tokens,
            response_id: None,
            round_usage: result.round_usage,
        })
    }

    async fn attempt_overflow_recovery(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
        session_id: &str,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) -> Result<bool, AgentError> {
        let model_name = config
            .model_name
            .as_deref()
            .unwrap_or("unknown");

        crate::runtime::runner::round_lifecycle::force_overflow_context_recovery(
            session,
            config,
            model_name,
            session_id,
            &self.llm,
            Some(event_tx),
        )
        .await
    }
}
