use async_trait::async_trait;
use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentError, Session};
use bamboo_domain::{Message, Role};
use bamboo_infrastructure::LLMProvider;

use crate::runtime::managers::mini_loop::{MiniLoopDecision, MiniLoopExecutor};

/// Production `MiniLoopExecutor` backed by a real LLM provider.
///
/// Uses a fast/cheap model for lightweight decisions such as task complexity
/// classification, compression decisions, retry classification, and routing.
pub struct LLMMiniLoopExecutor {
    provider: std::sync::Arc<dyn LLMProvider>,
    model: String,
}

impl LLMMiniLoopExecutor {
    pub fn new(provider: std::sync::Arc<dyn LLMProvider>, model: String) -> Self {
        Self { provider, model }
    }
}

#[async_trait]
impl MiniLoopExecutor for LLMMiniLoopExecutor {
    async fn decide(
        &self,
        _session: &Session,
        prompt: &str,
        context: &str,
    ) -> Result<MiniLoopDecision, AgentError> {
        let user_content = if context.is_empty() {
            prompt.to_string()
        } else {
            format!("Context:\n{}\n\n{}", context, prompt)
        };

        let now = chrono::Utc::now();
        let messages = vec![
            Message {
                id: String::new(),
                role: Role::System,
                content: "You are a task complexity classifier. Respond with exactly one word: simple, standard, or complex.".to_string(),
                reasoning: None,
                content_parts: None,
                image_ocr: None,
                phase: None,
                tool_calls: None,
                tool_call_id: None,
                tool_success: None,
                compressed: false,
                compressed_by_event_id: None,
                never_compress: false,
                compression_level: 0,
                created_at: now,
                metadata: None,
            },
            Message {
                id: String::new(),
                role: Role::User,
                content: user_content,
                reasoning: None,
                content_parts: None,
                image_ocr: None,
                phase: None,
                tool_calls: None,
                tool_call_id: None,
                tool_success: None,
                compressed: false,
                compressed_by_event_id: None,
                never_compress: false,
                compression_level: 0,
                created_at: now,
                metadata: None,
            },
        ];

        let options = bamboo_infrastructure::llm::provider::LLMRequestOptions {
            request_purpose: Some("mini_loop".to_string()),
            ..Default::default()
        };
        let stream = self
            .provider
            .chat_stream_with_options(&messages, &[], Some(50), &self.model, Some(&options))
            .await
            .map_err(|e| AgentError::LLM(e.to_string()))?;

        let output = crate::runtime::stream::handler::consume_llm_stream_silent(
            stream,
            &tokio_util::sync::CancellationToken::new(),
            "mini-loop",
        )
        .await
        .map_err(|e| AgentError::LLM(e.to_string()))?;

        Ok(MiniLoopDecision {
            answer: output.content.trim().to_string(),
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
        // Task evaluation is handled by the dedicated task_evaluation module.
        // This implementation returns an empty decision as a fallback.
        Ok(MiniLoopDecision {
            answer: String::new(),
            prompt_tokens: 0,
            completion_tokens: 0,
        })
    }
}
