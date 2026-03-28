use std::sync::Arc;

use super::prepare_round_context;
use crate::agent::core::budget::{BudgetStrategy, TokenBudget};
use crate::agent::core::tools::{FunctionCall, ToolCall};
use crate::agent::core::TokenBudgetUsage;
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
async fn prepare_round_context_truncates_prepared_context_without_persisting_compression_state() {
    let mut session = Session::new("session-cp-2", "test-model");
    session.token_budget = Some(TokenBudget::new(
        360,
        80,
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
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-2",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    assert!(prepared.prepared_context.truncation_occurred);
    assert!(prepared.prepared_context.segments_removed > 0);
    assert!(
        !prepared.prepared_context.compressed_message_ids.is_empty(),
        "Prepared context should identify messages that were truncated from this request"
    );
    assert!(
        session.compression_events.is_empty(),
        "Persistent compression state should only be created by the explicit compress_context tool"
    );
    assert_eq!(
        session.messages.iter().filter(|m| m.compressed).count(),
        0,
        "prepare_round_context should not automatically mark persisted messages compressed"
    );
}

#[tokio::test]
async fn prepare_round_context_drops_orphan_tool_results_only_from_prepared_context() {
    let mut session = Session::new("session-cp-3", "test-model");
    session.messages.push(Message::user("Run tool"));
    session.messages.push(Message::assistant(
        "Calling tool",
        Some(vec![ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "memory_note".to_string(),
                arguments: "{}".to_string(),
            },
        }]),
    ));
    session
        .messages
        .push(Message::tool_result("call_1", "ok result"));
    session
        .messages
        .push(Message::tool_result("call_orphan", "orphan result"));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-3",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    let orphan_in_prepared =
        prepared.prepared_context.messages.iter().any(|m| {
            matches!(m.role, Role::Tool) && m.tool_call_id.as_deref() == Some("call_orphan")
        });
    assert!(
        !orphan_in_prepared,
        "orphan tool result should be removed from LLM context"
    );

    let orphan_in_persisted = session
        .messages
        .iter()
        .any(|m| matches!(m.role, Role::Tool) && m.tool_call_id.as_deref() == Some("call_orphan"));
    assert!(
        orphan_in_persisted,
        "persisted session history must remain unchanged"
    );
}

#[tokio::test]
async fn prepare_round_context_prunes_unresolved_tool_calls_from_prepared_context() {
    let mut session = Session::new("session-cp-4", "test-model");
    session.messages.push(Message::user("Run tool"));
    session.messages.push(Message::assistant(
        "This text should stay",
        Some(vec![ToolCall {
            id: "call_missing".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "memory_note".to_string(),
                arguments: "{}".to_string(),
            },
        }]),
    ));
    session.messages.push(Message::user("continue"));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-4",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    let unresolved_tool_call_in_prepared = prepared.prepared_context.messages.iter().any(|m| {
        m.tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.id == "call_missing"))
    });
    assert!(
        !unresolved_tool_call_in_prepared,
        "unresolved tool call should be pruned from prepared LLM context"
    );

    let assistant_text_kept = prepared
        .prepared_context
        .messages
        .iter()
        .any(|m| matches!(m.role, Role::Assistant) && m.content == "This text should stay");
    assert!(assistant_text_kept, "assistant text should be preserved");

    let unresolved_tool_call_in_persisted = session.messages.iter().any(|m| {
        m.tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.id == "call_missing"))
    });
    assert!(
        unresolved_tool_call_in_persisted,
        "persisted history must remain unchanged"
    );
}

#[tokio::test]
async fn prepare_round_context_forces_compression_when_usage_crosses_ninety_eight_percent() {
    let mut session = Session::new("session-cp-force", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 1200,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 970,
        total_tokens: 1078,
        max_context_tokens: 1200,
        budget_limit: 1100,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force",
        &[],
        &llm,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "forced fallback should persist a compression event when usage is >= 98%"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "forced fallback should mark older messages compressed"
    );
    assert_eq!(
        session
            .metadata
            .get("context_compression_tool_enabled")
            .map(String::as_str),
        Some("false")
    );
    assert!(
        prepared.prepared_context.token_usage.usage_percentage() < 98.0,
        "prepared context should be recomputed after forced compression"
    );
}
