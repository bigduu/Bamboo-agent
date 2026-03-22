use std::sync::Arc;

use super::prepare_round_context;
use crate::agent::core::budget::{BudgetStrategy, TokenBudget};
use crate::agent::core::{Message, Role, Session};
use crate::agent::llm::models::{ContentPart, ImageUrl};
use crate::agent::llm::provider::{LLMProvider, LLMStream};
use crate::agent::loop_module::config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};

/// A no-op LLM provider for tests that returns an empty stream.
struct NoopLlmProvider;

#[async_trait::async_trait]
impl LLMProvider for NoopLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[crate::agent::core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> crate::agent::llm::provider::Result<LLMStream> {
        Ok(Box::pin(futures::stream::empty()))
    }
}

fn noop_llm() -> Arc<dyn LLMProvider> {
    Arc::new(NoopLlmProvider)
}

#[tokio::test]
async fn prepare_round_context_applies_placeholder_fallback_only_to_prepared_context() {
    let mut session = Session::new("session-cp-1", "test-model");
    session.messages.push(Message::user_with_parts(
        "看图",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        }],
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        image_fallback: Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Placeholder,
            vision_model: None,
        }),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-1",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    let prepared_user = prepared
        .prepared_context
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("prepared user message should exist");

    assert!(prepared_user.content_parts.is_none());
    assert!(prepared_user.content.contains("[Image omitted:"));

    let persisted_user = session
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("persisted user message should exist");
    assert!(persisted_user.content_parts.is_some());
}

#[tokio::test]
async fn prepare_round_context_records_compression_events_and_marks_messages() {
    let mut session = Session::new("session-cp-2", "test-model");
    session.token_budget = Some(TokenBudget::new(
        600,
        200,
        BudgetStrategy::Window { size: 50 },
    ));
    session.messages.push(Message::system("System prompt"));
    for index in 0..20 {
        session
            .messages
            .push(Message::user(format!("Old user message {}", index)));
        session.messages.push(Message::assistant(
            format!("Old assistant response {}", index),
            None,
        ));
    }

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let _prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-2",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "Expected at least one compression event"
    );
    let archived_count = session.messages.iter().filter(|m| m.compressed).count();
    assert!(archived_count > 0, "Expected archived messages");
    assert!(
        session
            .messages
            .iter()
            .filter(|m| m.compressed)
            .all(|m| m.compressed_by_event_id.is_some()),
        "Archived messages should reference a compression event"
    );
}
