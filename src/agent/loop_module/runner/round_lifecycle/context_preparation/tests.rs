use std::sync::{Arc, Mutex};

use super::{maybe_apply_host_context_compression, prepare_round_context};
use crate::agent::core::budget::{BudgetStrategy, TokenBudget};
use crate::agent::core::tools::{FunctionCall, ToolCall};
use crate::agent::core::TokenBudgetUsage;
use crate::agent::core::{Message, Role, Session};
use crate::agent::llm::models::{ContentPart, ImageUrl};
use crate::agent::llm::provider::{LLMProvider, LLMStream};
use crate::agent::llm::{LLMChunk, LLMError};
use crate::agent::loop_module::config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
use futures::stream;

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

struct RecordingLlmProvider {
    models: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl LLMProvider for RecordingLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[crate::agent::core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> crate::agent::llm::provider::Result<LLMStream> {
        self.models
            .lock()
            .expect("recorded model list lock should not be poisoned")
            .push(model.to_string());

        Ok(Box::pin(stream::iter(vec![
            Ok::<LLMChunk, LLMError>(LLMChunk::Token("summary".to_string())),
            Ok::<LLMChunk, LLMError>(LLMChunk::Done),
        ])))
    }
}

fn recording_llm() -> (Arc<dyn LLMProvider>, Arc<Mutex<Vec<String>>>) {
    let models = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(RecordingLlmProvider {
        models: Arc::clone(&models),
    });
    (llm, models)
}

#[tokio::test]
async fn maybe_apply_host_context_compression_uses_fast_model_for_summary_request() {
    let mut session = Session::new("session-cp-fast-model", "main-model");
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
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
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
        window_tokens: 900,
        total_tokens: 1000,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("main-model".to_string()),
        fast_model_name: Some("fast-model".to_string()),
        ..Default::default()
    };
    let (llm, models) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model",
        "session-cp-fast-model",
        &[],
        &llm,
        None,
        "pre-turn",
    )
    .await
    .expect("host compression should run with fast model");

    assert!(applied, "expected pre-turn compression to be applied");

    let models = models
        .lock()
        .expect("recorded model list lock should not be poisoned");
    assert_eq!(models.as_slice(), ["fast-model"]);
}

#[tokio::test]
async fn force_overflow_context_recovery_degrades_tool_guide_before_skill_context() {
    let mut session = Session::new("session-cp-overflow-degrade", "test-model");
    session.messages.push(Message::system(
        "Base prompt\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\nskill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->".to_string(),
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-cp-overflow-degrade",
        &llm,
        None,
    )
    .await
    .expect("overflow degradation should complete");

    assert!(applied);
    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, Role::System))
        .map(|message| message.content.clone())
        .unwrap_or_default();
    assert!(system_prompt.contains("BAMBOO_SKILL_CONTEXT_START"));
    assert!(!system_prompt.contains("BAMBOO_TOOL_GUIDE_START"));
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
        None,
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
async fn prepare_round_context_auto_compresses_when_hard_limit_truncation_pressure_is_high() {
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
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "high pressure hard-limit truncation should trigger host auto-compression persistence"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "host auto-compression should mark historical messages compressed"
    );
    assert!(
        prepared.prepared_context.token_usage.summary_tokens > 0,
        "prepared context should reserve summary tokens after host auto-compression"
    );
    assert!(
        prepared
            .prepared_context
            .messages
            .iter()
            .any(|m| m.content.contains("CONVERSATION_SUMMARY_START")),
        "prepared context should include the persisted compression summary"
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
        None,
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
        None,
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
        max_output_tokens: 0,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
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
        window_tokens: 1078,
        total_tokens: 1178,
        max_context_tokens: 1200,
        budget_limit: 1200,
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
        None,
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
    assert!(
        prepared.prepared_context.token_usage.usage_percentage() < 98.0,
        "prepared context should be recomputed after forced compression"
    );
}

#[tokio::test]
async fn maybe_apply_host_context_compression_supports_mid_turn_phase() {
    let mut session = Session::new("session-cp-mid-turn", "test-model");
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
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
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
        window_tokens: 850,
        total_tokens: 950,
        max_context_tokens: 1200,
        budget_limit: 1000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-cp-mid-turn",
        &[],
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("mid-turn host compression should run");

    assert!(applied, "expected mid-turn compression to be applied");
    assert!(
        !session.compression_events.is_empty(),
        "mid-turn compression should persist a compression event"
    );
}

#[tokio::test]
async fn prepare_round_context_auto_compresses_when_context_window_usage_crosses_trigger() {
    // Host auto-compression now uses a single rule:
    // usage(context_window) >= compression_trigger_percent.
    // Here total_tokens/context_window = 1000/1200 = 83.3% with trigger=80, so it should run.
    let mut session = Session::new("session-cp-force-context-only", "test-model");
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
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
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
    // context_window = 1200
    // total_tokens/context_window = 1000/1200 = 83.3% >= 80%
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 900,
        total_tokens: 1000,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let _prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force-context-only",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "host auto compression should run when context-window usage (83.3%) crosses trigger (80%)"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "messages should be compressed when host auto compression runs"
    );
}

#[tokio::test]
async fn prepare_round_context_skips_host_auto_compression_below_trigger() {
    let mut session = Session::new("session-cp-force-context-low", "test-model");
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
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..4 {
        session
            .messages
            .push(Message::user(format!("User message {} short text", index)));
        session.messages.push(Message::assistant(
            format!("Assistant response {} short text", index),
            None,
        ));
    }
    // context_window = 1200, usage = 62.5%; history content is also intentionally
    // kept short so estimated usage stays below trigger (80%).
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 650,
        total_tokens: 750,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: true,
        segments_removed: 4,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let _prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force-context-low",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        session.compression_events.is_empty(),
        "host auto compression should stay off below trigger (80%)"
    );
    assert!(
        !session.messages.iter().any(|m| m.compressed),
        "messages should stay uncompressed below host auto-compression trigger"
    );
}

#[tokio::test]
async fn force_overflow_context_recovery_can_bypass_regular_trigger_gate() {
    let mut session = Session::new("session-cp-overflow-force", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 1200,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 95,
        compression_target_percent: 50,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
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
        window_tokens: 780,
        total_tokens: 880,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-cp-overflow-force",
        &llm,
        None,
    )
    .await
    .expect("forced overflow recovery should complete");

    assert!(
        applied,
        "forced overflow recovery should bypass the normal trigger gate"
    );
    assert!(!session.compression_events.is_empty());
    assert!(session.messages.iter().any(|m| m.compressed));
}

/// Integration test: multi-round compress → build pressure → re-expose → compress again.
///
/// This verifies the full cycle including the anchor_index==0 fix and
/// token_usage preservation after compression.
#[tokio::test]
async fn multi_round_compression_cycle() {
    use crate::agent::core::budget::{
        apply_compression_plan, build_forced_compression_plan_with_summary,
        estimate_context_compression_exposure,
    };

    let budget = TokenBudget {
        max_context_tokens: 2000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 50,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
    };
    let mut session = Session::new("multi-round-compress", "test-model");
    session.token_budget = Some(budget.clone());
    session.add_message(Message::system("You are a helpful assistant"));

    // ---- Round 1: build pressure ----
    for idx in 0..8 {
        session.add_message(Message::user(format!(
            "User question {idx} {}",
            "alpha beta gamma delta ".repeat(10)
        )));
        session.add_message(Message::assistant(
            format!(
                "Assistant response {idx} {}",
                "analyzing files checks plans ".repeat(10)
            ),
            None,
        ));
    }

    // Simulate persisted usage from prepare_hybrid_context
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 50,
        summary_tokens: 0,
        window_tokens: 1700,
        total_tokens: 1750,
        max_context_tokens: 2000,
        budget_limit: 2000, // context_window
        truncation_occurred: true,
        segments_removed: 3,
        prompt_cached_tool_outputs: 0,
    });

    let exposure1 = estimate_context_compression_exposure(
        &session,
        "test-model",
        session.token_budget.as_ref(),
    );
    assert!(
        exposure1.should_expose_tool,
        "should expose tool on first pressure: usage={:.1}%",
        exposure1.active_usage_percent
    );

    // ---- Compress round 1 ----
    let plan1 = build_forced_compression_plan_with_summary(
        &session,
        "test-model",
        session.token_budget.as_ref(),
        "Summary of rounds 0-7: user asked many questions, assistant analyzed files.".to_string(),
    )
    .expect("first compression plan should succeed");

    let compressed1 = apply_compression_plan(&mut session, plan1);
    assert!(compressed1 > 0, "first compression should archive messages");

    // token_usage should NOT be None after compression
    assert!(
        session.token_usage.is_some(),
        "token_usage should be preserved (re-estimated) after compression"
    );
    let usage_after_1 = session.token_usage.as_ref().unwrap();
    assert!(
        usage_after_1.budget_limit > 0,
        "budget_limit should be preserved after compression"
    );

    // ---- Round 2: build more pressure after first compression ----
    // Only one User message remains (anchor_index == 0 scenario)
    let user_count_after_1 = session
        .messages
        .iter()
        .filter(|m| !m.compressed && matches!(m.role, Role::User))
        .count();
    // Could be 1 or more depending on anchor — just verify compression happened
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "some messages should be compressed after round 1"
    );

    // Add more messages to build pressure again
    for idx in 0..6 {
        session.add_message(Message::user(format!(
            "Follow-up {idx} {}",
            "more content to fill budget ".repeat(12)
        )));
        session.add_message(Message::assistant(
            format!(
                "Reply {idx} {}",
                "detailed analysis and next steps ".repeat(12)
            ),
            None,
        ));
    }

    // Simulate updated persisted usage
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 50,
        summary_tokens: 100,
        window_tokens: 1650,
        total_tokens: 1800,
        max_context_tokens: 2000,
        budget_limit: 2000,
        truncation_occurred: true,
        segments_removed: 2,
        prompt_cached_tool_outputs: 0,
    });

    // ---- Compress round 2 (anchor_index == 0 or small) ----
    let plan2 = build_forced_compression_plan_with_summary(
        &session,
        "test-model",
        session.token_budget.as_ref(),
        format!(
            "Updated summary: rounds 0-7 summarized earlier (user_count_after_first={}). Follow-up rounds 8-13 added.",
            user_count_after_1
        ),
    )
    .expect("second compression plan should succeed (anchor_index fix)");

    let compressed2 = apply_compression_plan(&mut session, plan2);
    assert!(
        compressed2 > 0,
        "second compression should archive more messages"
    );
    assert!(
        session.compression_events.len() >= 2,
        "should have at least 2 compression events"
    );
    assert!(
        session.token_usage.is_some(),
        "token_usage should be preserved after second compression"
    );
}
