use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::execute_llm_stream;
use crate::agent::core::budget::{PreparedContext, TokenUsageBreakdown};
use crate::agent::core::{AgentEvent, Message, Session};
use crate::agent::llm::{LLMChunk, LLMProvider, LLMStream};

struct MockLlmProvider {
    chunks: Vec<LLMChunk>,
}

#[async_trait]
impl LLMProvider for MockLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[crate::agent::core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> crate::agent::llm::provider::Result<LLMStream> {
        let items = self
            .chunks
            .clone()
            .into_iter()
            .map(Ok::<LLMChunk, crate::agent::llm::provider::LLMError>);
        Ok(Box::pin(stream::iter(items)))
    }
}

#[tokio::test]
async fn execute_llm_stream_sets_session_usage_and_emits_budget_event() {
    let mut session = Session::new("session-stream-1", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);

    let prepared_context = PreparedContext {
        messages: vec![Message::system("system")],
        token_usage: TokenUsageBreakdown {
            system_tokens: 10,
            summary_tokens: 0,
            window_tokens: 12,
            total_tokens: 22,
            budget_limit: 100,
        },
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
    };

    let llm: Arc<dyn LLMProvider> = Arc::new(MockLlmProvider {
        chunks: vec![LLMChunk::Token("hi".to_string()), LLMChunk::Done],
    });

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &llm,
        &event_tx,
        &CancellationToken::new(),
        &prepared_context,
        &[],
        128,
        "test-model",
        "session-stream-1",
    )
    .await
    .expect("execute llm stream");

    assert_eq!(stream_output.content, "hi");
    assert!(session.token_usage.is_some());

    let first = event_rx.recv().await.expect("budget event expected");
    assert!(matches!(first, AgentEvent::TokenBudgetUpdated { .. }));

    let second = event_rx.recv().await.expect("token event expected");
    assert!(matches!(second, AgentEvent::Token { .. }));
}
