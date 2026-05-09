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
            session_id: None,
            reasoning_effort: None,
            parallel_tool_calls: None,
            responses: None,
            request_purpose: Some("mini_loop".to_string()),
        };
        let stream = self
            .provider
            .chat_stream_with_options(&messages, &[], Some(256), &self.model, Some(&options))
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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure::llm::provider::LLMRequestOptions;
    use bamboo_domain::ReasoningEffort;
    use bamboo_infrastructure::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use bamboo_agent_core::tools::ToolSchema;
    use futures::stream;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct CaptureProvider {
        captured_max_tokens: Mutex<Vec<Option<u32>>>,
        captured_reasoning: Mutex<Vec<Option<ReasoningEffort>>>,
    }

    #[async_trait]
    impl LLMProvider for CaptureProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token("simple".to_string())),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            self.captured_max_tokens
                .lock()
                .expect("lock should not be poisoned")
                .push(max_output_tokens);
            self.captured_reasoning
                .lock()
                .expect("lock should not be poisoned")
                .push(options.and_then(|o| o.reasoning_effort));
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }
    }

    #[tokio::test]
    async fn mini_loop_sends_no_reasoning_effort() {
        let provider = Arc::new(CaptureProvider::default());
        let executor = LLMMiniLoopExecutor::new(provider.clone(), "fast-model".to_string());
        let session = Session::new("test", "model");

        let decision = executor
            .decide(&session, "classify this task", "")
            .await
            .expect("decide should succeed");
        assert_eq!(decision.answer, "simple");

        let captured_reasoning = provider
            .captured_reasoning
            .lock()
            .expect("lock should not be poisoned");
        assert_eq!(
            captured_reasoning.as_slice(),
            [None],
            "mini_loop should not request reasoning to avoid thinking budget consuming output tokens"
        );
    }

    #[tokio::test]
    async fn mini_loop_max_tokens_accommodates_provider_default_reasoning() {
        let provider = Arc::new(CaptureProvider::default());
        let executor = LLMMiniLoopExecutor::new(provider.clone(), "fast-model".to_string());
        let session = Session::new("test", "model");

        let _ = executor
            .decide(&session, "classify this task", "")
            .await
            .expect("decide should succeed");

        let captured = provider
            .captured_max_tokens
            .lock()
            .expect("lock should not be poisoned");
        let max_tokens = captured[0].expect("max_output_tokens should be set");
        // Even if the provider has a default_reasoning_effort of High (4096 thinking budget),
        // 256 tokens is enough for the one-word response while leaving room for thinking.
        assert!(
            max_tokens >= 256,
            "max_output_tokens ({}) should be at least 256 to accommodate potential provider default reasoning",
            max_tokens
        );
    }
}
