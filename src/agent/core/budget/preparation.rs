//! Context preparation for budget management.
//!
//! Implements the hybrid context preparation algorithm that enforces token
//! budgets while preserving tool-call chain atomicity.

use crate::agent::core::agent::types::{Role, Session};
use crate::agent::core::budget::counter::TokenCounter;
use crate::agent::core::budget::segmenter::{MessageSegment, MessageSegmenter};
use crate::agent::core::budget::types::{
    BudgetError, BudgetStrategy, PreparedContext, TokenBudget, TokenUsageBreakdown,
};

/// Prepare context for LLM call with budget enforcement.
///
/// This function implements the hybrid token budget strategy:
/// 1. Extract and always include system messages
/// 2. Validate system prompt fits within budget
/// 3. Segment remaining messages (keeping tool chains atomic)
/// 4. Select recent segments from the end until budget is filled
/// 5. Return prepared messages (NOT mutating session)
///
/// # Arguments
///
/// * `session` - The session containing all messages
/// * `budget` - Token budget configuration
/// * `counter` - Token counter implementation
///
/// # Returns
///
/// * `Ok(PreparedContext)` - Truncated messages and usage info
/// * `Err(BudgetError)` - If system prompt is too large
///
/// # Example
///
/// ```ignore
/// use crate::agent::core::budget::{TokenBudget, HeuristicTokenCounter, prepare_hybrid_context};
///
/// let budget = TokenBudget::for_model(128_000);
/// let counter = HeuristicTokenCounter::default();
/// let prepared = prepare_hybrid_context(&session, &budget, &counter)?;
///
/// // Use prepared.messages for LLM call
/// // Full session.messages is preserved in storage
/// ```
pub fn prepare_hybrid_context(
    session: &Session,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
) -> Result<PreparedContext, BudgetError> {
    let segmenter = MessageSegmenter::new();
    let active_messages: Vec<_> = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .cloned()
        .collect();

    // 1. Extract system messages (always included) - takes ownership, no clone needed
    let (system_messages, mut segments) = segmenter.segment_with_system(active_messages);

    // 2. Count system tokens
    let system_tokens = counter.count_messages(&system_messages);

    // 3. Check if system prompt alone exceeds budget
    let hard_available = budget.available_input_tokens();
    if system_tokens > hard_available {
        return Err(BudgetError::SystemPromptTooLarge {
            system_tokens,
            available_tokens: hard_available,
        });
    }

    // 4. Calculate remaining budget after system messages.
    // Use proactive threshold so compression can occur before the hard limit.
    let proactive_limit = budget.compression_trigger_input_tokens();
    let target_limit = budget.compression_target_input_tokens();
    let proactive_remaining_budget = proactive_limit.saturating_sub(system_tokens);

    // 5. Count tokens for each segment
    for segment in &mut segments {
        segment.token_estimate = counter.count_messages(&segment.messages);
    }

    let pre_window_tokens: u32 = segments.iter().fold(0u32, |acc, segment| {
        acc.saturating_add(segment.token_estimate)
    });
    let compression_needed = pre_window_tokens > proactive_remaining_budget;
    let remaining_budget = if compression_needed {
        target_limit.saturating_sub(system_tokens)
    } else {
        proactive_remaining_budget
    };
    if compression_needed {
        let pre_total_tokens = system_tokens.saturating_add(pre_window_tokens);
        let pre_usage_pct = if hard_available == 0 {
            0.0
        } else {
            (pre_total_tokens as f64 / hard_available as f64) * 100.0
        };
        let target_effective_pct = if hard_available == 0 {
            0
        } else {
            target_limit
                .saturating_mul(100)
                .saturating_div(hard_available)
        };
        tracing::info!(
            "[{}] Context compression needed: pre_total={} (system={}, window={}), proactive_limit={} (trigger={}%), target_limit={} (target_config={}%, target_effective={}%), hard_limit={}, usage={:.1}%",
            session.id,
            pre_total_tokens,
            system_tokens,
            pre_window_tokens,
            proactive_limit,
            budget.compression_trigger_percent,
            target_limit,
            budget.compression_target_percent,
            target_effective_pct,
            hard_available,
            pre_usage_pct
        );
    }

    // 6. Select segments from the end until budget is filled
    let selection = select_segments_within_budget(segments, remaining_budget, &budget.strategy);
    let mut selected_segments = selection.selected;
    let removed_count = selection.removed.len();
    let removed_messages_count: usize = selection.removed.iter().map(|s| s.messages.len()).sum();
    let removed_tool_segments_count = selection
        .removed
        .iter()
        .filter(|segment| segment.is_tool_chain)
        .count();
    let removed_tokens: u32 = selection.removed.iter().fold(0u32, |acc, segment| {
        acc.saturating_add(segment.token_estimate)
    });
    let compressed_message_ids: Vec<String> = selection
        .removed
        .iter()
        .flat_map(|segment| segment.messages.iter())
        .map(|message| message.id.clone())
        .collect();

    // 7. Build final message list
    let mut prepared_messages = system_messages;

    // Inject conversation summary between system messages and the window.
    // This preserves context from earlier (compressed) parts of the conversation.
    let summary_tokens = if let Some(ref summary) = session.conversation_summary {
        let summary_message = crate::agent::core::agent::types::Message::user(format!(
            "<!-- CONVERSATION_SUMMARY_START -->\n\
             ## Previous Conversation Summary\n\
             The following is a summary of earlier conversation that was removed \
             due to context window limits. Use it to maintain continuity.\n\n\
             {}\n\
             <!-- CONVERSATION_SUMMARY_END -->",
            summary.content
        ));
        let tokens = counter.count_messages(&[summary_message.clone()]);
        prepared_messages.push(summary_message);
        tokens
    } else {
        0
    };

    // Add selected segments - use take to avoid cloning
    for segment in &mut selected_segments {
        prepared_messages.append(&mut segment.messages);
    }

    // 8. Calculate final token usage
    let window_tokens: u32 = selected_segments
        .iter()
        .fold(0u32, |acc, s| acc.saturating_add(s.token_estimate));
    let kept_messages_count: usize = selected_segments.iter().map(|s| s.messages.len()).sum();

    let total_tokens = system_tokens
        .saturating_add(summary_tokens)
        .saturating_add(window_tokens);

    let token_usage = TokenUsageBreakdown {
        system_tokens,
        summary_tokens,
        window_tokens,
        total_tokens,
        budget_limit: hard_available,
    };

    let truncation_occurred = removed_count > 0;
    if truncation_occurred {
        let applied_limit = if compression_needed {
            target_limit
        } else {
            proactive_limit
        };
        tracing::info!(
            "[{}] Context compression result: removed_segments={}, removed_messages={}, removed_tool_segments={}, removed_tokens={}, kept_segments={}, kept_messages={}, final_total={} / {} ({:.1}%), applied_limit={}",
            session.id,
            removed_count,
            removed_messages_count,
            removed_tool_segments_count,
            removed_tokens,
            selected_segments.len(),
            kept_messages_count,
            total_tokens,
            hard_available,
            token_usage.usage_percentage(),
            applied_limit
        );
    }

    Ok(PreparedContext {
        messages: prepared_messages,
        token_usage,
        truncation_occurred,
        segments_removed: removed_count,
        compressed_message_ids,
    })
}

/// Select message segments within the remaining budget.
///
/// Takes segments from the end (most recent) until budget is filled.
/// Respects tool-chain atomicity - never splits tool calls from their results.
fn select_segments_within_budget(
    segments: Vec<MessageSegment>,
    remaining_budget: u32,
    _strategy: &BudgetStrategy,
) -> SegmentSelectionResult {
    let total_tokens = segments.iter().fold(0u32, |acc, segment| {
        acc.saturating_add(segment.token_estimate)
    });
    if total_tokens <= remaining_budget {
        return SegmentSelectionResult {
            selected: segments,
            removed: Vec::new(),
        };
    }

    let mut keep_flags = vec![true; segments.len()];
    let mut protected_flags = vec![false; segments.len()];
    let mut current_tokens = total_tokens;

    // Prefer preserving the original question and latest textual outcome.
    if let Some(first_user_index) = segments.iter().position(segment_contains_user) {
        protected_flags[first_user_index] = true;
    }
    if let Some(last_user_index) = segments.iter().rposition(segment_contains_user) {
        protected_flags[last_user_index] = true;
    }
    if let Some(last_assistant_text_index) =
        segments.iter().rposition(segment_contains_assistant_text)
    {
        protected_flags[last_assistant_text_index] = true;
    }

    // Phase 1: drop oldest tool chains first (usually intermediate execution traces).
    for index in 0..segments.len() {
        if current_tokens <= remaining_budget {
            break;
        }
        if !keep_flags[index] || protected_flags[index] {
            continue;
        }
        if segments[index].is_tool_chain {
            keep_flags[index] = false;
            current_tokens = current_tokens.saturating_sub(segments[index].token_estimate);
        }
    }

    // Phase 2: if still over, drop oldest non-tool segments except protected anchors.
    for index in 0..segments.len() {
        if current_tokens <= remaining_budget {
            break;
        }
        if !keep_flags[index] || protected_flags[index] {
            continue;
        }
        if !segments[index].is_tool_chain {
            keep_flags[index] = false;
            current_tokens = current_tokens.saturating_sub(segments[index].token_estimate);
        }
    }

    // Phase 3: remove any remaining non-protected segments.
    for index in 0..segments.len() {
        if current_tokens <= remaining_budget {
            break;
        }
        if !keep_flags[index] || protected_flags[index] {
            continue;
        }
        keep_flags[index] = false;
        current_tokens = current_tokens.saturating_sub(segments[index].token_estimate);
    }

    // Phase 4 (fallback): if anchors still don't fit, remove protected segments from oldest first.
    for index in 0..segments.len() {
        if current_tokens <= remaining_budget {
            break;
        }
        if !keep_flags[index] || !protected_flags[index] {
            continue;
        }
        keep_flags[index] = false;
        current_tokens = current_tokens.saturating_sub(segments[index].token_estimate);
    }

    let mut selected = Vec::new();
    let mut removed = Vec::new();
    for (index, segment) in segments.into_iter().enumerate() {
        if keep_flags[index] {
            selected.push(segment);
        } else {
            removed.push(segment);
        }
    }

    SegmentSelectionResult { selected, removed }
}

struct SegmentSelectionResult {
    selected: Vec<MessageSegment>,
    removed: Vec<MessageSegment>,
}

fn segment_contains_user(segment: &MessageSegment) -> bool {
    segment
        .messages
        .iter()
        .any(|message| message.role == Role::User)
}

fn segment_contains_assistant_text(segment: &MessageSegment) -> bool {
    segment.messages.iter().any(|message| {
        message.role == Role::Assistant
            && !message.content.trim().is_empty()
            && message
                .tool_calls
                .as_ref()
                .map_or(true, |calls| calls.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::agent::types::{Message, Role};
    use crate::agent::core::budget::counter::HeuristicTokenCounter;
    use crate::agent::core::tools::{FunctionCall, ToolCall};

    fn create_tool_call(id: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "test".to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    fn make_session_with_messages(messages: Vec<Message>) -> Session {
        let mut session = Session::new("test-session", "test-model");
        session.messages = messages;
        session
    }

    #[test]
    fn returns_all_messages_when_within_budget() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
            Message::assistant("Hi there", None),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(!prepared.truncation_occurred);
        assert_eq!(prepared.messages.len(), 3);
        assert_eq!(prepared.segments_removed, 0);
    }

    #[test]
    fn always_includes_system_messages() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let messages = vec![
            Message::system("System prompt"),
            Message::user("User message"),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(prepared
            .messages
            .iter()
            .any(|m| m.role == crate::agent::core::agent::types::Role::System));
    }

    #[test]
    fn truncates_when_budget_exceeded() {
        let counter = HeuristicTokenCounter::default();

        // Small budget to force truncation
        let budget = TokenBudget::new(500, 200, BudgetStrategy::Window { size: 50 });

        // Create many messages to exceed budget
        let mut messages = vec![Message::system("System")];
        for i in 0..50 {
            messages.push(Message::user(format!(
                "Message number {} with some content",
                i
            )));
            messages.push(Message::assistant(format!("Response {}", i), None));
        }

        let session = make_session_with_messages(messages.clone());

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(prepared.truncation_occurred, "Should have truncated");
        assert!(
            prepared.messages.len() < messages.len(),
            "Should have fewer messages"
        );
        assert!(
            prepared.segments_removed > 0,
            "Should have removed some segments"
        );
    }

    #[test]
    fn preserves_recent_messages_when_truncating() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::new(500, 200, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system("System"),
            Message::user("Oldest message"),
            Message::assistant("Old response", None),
            Message::user("Recent message"),
            Message::assistant("Recent response", None),
        ];

        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // Recent messages should be preserved
        let last_user = prepared
            .messages
            .iter()
            .rev()
            .find(|m| m.role == crate::agent::core::agent::types::Role::User);
        assert!(last_user.is_some());
        assert!(last_user.unwrap().content.contains("Recent"));
    }

    #[test]
    fn preserves_tool_call_chains() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::new(500, 200, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system("System"),
            Message::user("Search"),
            Message::assistant("I'll search", Some(vec![create_tool_call("call_1")])),
            Message::tool_result("call_1", "Results"),
        ];

        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // If tool call is included, tool result must also be included
        let has_tool_call = prepared.messages.iter().any(|m| {
            m.tool_calls
                .as_ref()
                .map_or(false, |tc| tc.iter().any(|c| c.id == "call_1"))
        });
        let has_tool_result = prepared
            .messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some("call_1"));

        // Either both are present or neither is
        assert_eq!(
            has_tool_call, has_tool_result,
            "Tool call and result must stay together"
        );
    }

    #[test]
    fn errors_on_system_prompt_too_large() {
        let counter = HeuristicTokenCounter::default();

        // Tiny budget
        let budget = TokenBudget::new(100, 50, BudgetStrategy::default());

        // Huge system prompt
        let huge_system = "x".repeat(1000); // Way more than 50 tokens
        let messages = vec![Message::system(huge_system)];
        let session = make_session_with_messages(messages);

        let result = prepare_hybrid_context(&session, &budget, &counter);

        assert!(matches!(
            result,
            Err(BudgetError::SystemPromptTooLarge { .. })
        ));
    }

    #[test]
    fn calculates_token_usage_correctly() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let messages = vec![
            Message::system("System"),
            Message::user("Hello"),
            Message::assistant("Hi", None),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // Verify usage breakdown sums correctly
        let expected_total = prepared.token_usage.system_tokens
            + prepared.token_usage.summary_tokens
            + prepared.token_usage.window_tokens;

        assert_eq!(prepared.token_usage.total_tokens, expected_total);
        assert!(prepared.token_usage.total_tokens <= prepared.token_usage.budget_limit);
    }

    #[test]
    fn handles_empty_session() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let session = Session::new("empty", "test-model");
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(!prepared.truncation_occurred);
        assert!(prepared.messages.is_empty());
        assert_eq!(prepared.token_usage.total_tokens, 0);
    }

    #[test]
    fn handles_session_with_only_system() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let messages = vec![Message::system("System prompt")];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(!prepared.truncation_occurred);
        assert_eq!(prepared.messages.len(), 1);
        assert!(prepared.token_usage.system_tokens > 0);
        assert_eq!(prepared.token_usage.window_tokens, 0);
    }

    #[test]
    fn enforces_budget_limit_never_exceeds() {
        // Test that prepared context never exceeds budget limit
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::new(300, 100, BudgetStrategy::Window { size: 50 });

        // Create messages that would exceed budget
        let messages = vec![
            Message::system("System prompt here"),
            Message::user("First user message with some content"),
            Message::assistant("First assistant response with more content here", None),
            Message::user("Second user message with some content"),
            Message::assistant("Second assistant response with more content here", None),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // Total should never exceed budget limit
        assert!(
            prepared.token_usage.total_tokens <= prepared.token_usage.budget_limit,
            "Total tokens {} should not exceed budget limit {}",
            prepared.token_usage.total_tokens,
            prepared.token_usage.budget_limit
        );
    }

    #[test]
    fn skips_oversized_segments() {
        // Test that segments exceeding remaining budget are skipped
        let counter = HeuristicTokenCounter::default();
        // Tight budget: large message should not fit, but small message should.
        let budget = TokenBudget::new(400, 100, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system("System"),
            // Create a very large message that exceeds budget
            Message::user(&"x".repeat(1000)),
            Message::user("Small message"),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // The large message should be skipped, small message should be included
        let has_large_message = prepared.messages.iter().any(|m| m.content.len() > 100);
        let has_small_message = prepared
            .messages
            .iter()
            .any(|m| m.content.contains("Small"));

        assert!(!has_large_message, "Oversized segment should be skipped");
        assert!(
            has_small_message,
            "Small message within budget should be included"
        );
    }

    #[test]
    fn handles_zero_remaining_budget() {
        // Test behavior when remaining budget is 0 (system fills entire budget)
        let counter = HeuristicTokenCounter::default();
        // Budget that only fits system message - small safety margin leaves 0 for window
        let budget = TokenBudget::new(100, 50, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system("System prompt that uses most of the budget"),
            Message::user("User message"),
        ];
        let session = make_session_with_messages(messages);

        // Should fail with SystemPromptTooLarge since 22 tokens > 0 available
        let result = prepare_hybrid_context(&session, &budget, &counter);
        assert!(matches!(
            result,
            Err(BudgetError::SystemPromptTooLarge { .. })
        ));
    }

    #[test]
    fn handles_small_budget_with_fitting_system() {
        // Test with budget large enough for system but tight for additional messages
        let counter = HeuristicTokenCounter::default();
        // Budget: 200 total, 50 for output, 100 safety = 50 for system+window
        let budget = TokenBudget::new(200, 50, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system(
                "This is a longer system prompt that uses up more of the available budget space",
            ),
            Message::user("User message"),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // System message should always be present
        let has_system = prepared
            .messages
            .iter()
            .any(|m| m.role == crate::agent::core::agent::types::Role::System);
        assert!(has_system, "System message should always be included");

        // Budget should be enforced
        assert!(
            prepared.token_usage.total_tokens <= prepared.token_usage.budget_limit,
            "Total tokens should not exceed budget limit"
        );
    }

    #[test]
    fn excludes_precompressed_messages_from_llm_context() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let mut archived = Message::user("Archived context");
        archived.compressed = true;
        archived.compressed_by_event_id = Some("evt-1".to_string());

        let messages = vec![
            Message::system("System"),
            archived,
            Message::user("Active message"),
            Message::assistant("Active response", None),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();
        assert!(
            prepared
                .messages
                .iter()
                .all(|message| !message.content.contains("Archived context")),
            "Compressed messages must be excluded from LLM context"
        );
    }

    #[test]
    fn returns_newly_compressed_message_ids_when_truncated() {
        let counter = HeuristicTokenCounter::default();
        let budget = TokenBudget::new(500, 200, BudgetStrategy::Window { size: 50 });

        let mut messages = vec![Message::system("System")];
        for i in 0..24 {
            messages.push(Message::user(format!("Older message {}", i)));
            messages.push(Message::assistant(format!("Older response {}", i), None));
        }

        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(prepared.truncation_occurred);
        assert!(
            !prepared.compressed_message_ids.is_empty(),
            "Truncation should return IDs for archived messages"
        );
    }

    #[test]
    fn prefers_purging_intermediate_tool_traces_under_budget_pressure() {
        let counter = HeuristicTokenCounter::default();
        let mut budget =
            TokenBudget::with_safety_margin(800, 200, BudgetStrategy::Window { size: 50 }, 100);
        budget.compression_trigger_percent = 70;

        let messages = vec![
            Message::system("System"),
            Message::user("How do we migrate database schema safely?"),
            Message::assistant(
                "Running analysis step 1",
                Some(vec![create_tool_call("call_1")]),
            ),
            Message::tool_result("call_1", "intermediate-tool-output-1 ".repeat(180)),
            Message::assistant(
                "Running analysis step 2",
                Some(vec![create_tool_call("call_2")]),
            ),
            Message::tool_result("call_2", "intermediate-tool-output-2 ".repeat(180)),
            Message::assistant(
                "Final answer: use an online migration with backfill and cutover.",
                None,
            ),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();
        let has_question = prepared.messages.iter().any(|message| {
            message.role == Role::User && message.content.contains("migrate database schema")
        });
        let has_final_answer = prepared.messages.iter().any(|message| {
            message.role == Role::Assistant
                && message
                    .tool_calls
                    .as_ref()
                    .map_or(true, |calls| calls.is_empty())
                && message.content.contains("Final answer")
        });
        let tool_results_kept = prepared
            .messages
            .iter()
            .filter(|message| message.role == Role::Tool)
            .count();

        assert!(prepared.truncation_occurred);
        assert!(has_question, "Original user question should be preserved");
        assert!(
            has_final_answer,
            "Final assistant conclusion should be preserved"
        );
        assert!(
            tool_results_kept < 2,
            "At least one intermediate tool result should be purged"
        );
    }

    #[test]
    fn proactive_trigger_compresses_before_hard_limit() {
        let counter = HeuristicTokenCounter::default();
        let mut proactive_budget =
            TokenBudget::with_safety_margin(400, 100, BudgetStrategy::Window { size: 50 }, 100);
        proactive_budget.compression_trigger_percent = 50;

        let mut hard_only_budget = proactive_budget.clone();
        hard_only_budget.compression_trigger_percent = 100;

        let messages = vec![
            Message::system("System"),
            Message::user("Message A with enough content to consume noticeable token budget."),
            Message::assistant(
                "Response A with enough content to consume noticeable token budget.",
                None,
            ),
            Message::user("Message B with enough content to consume noticeable token budget."),
            Message::assistant(
                "Response B with enough content to consume noticeable token budget.",
                None,
            ),
            Message::user("Message C with enough content to consume noticeable token budget."),
            Message::assistant(
                "Response C with enough content to consume noticeable token budget.",
                None,
            ),
        ];
        let session = make_session_with_messages(messages);

        let proactive = prepare_hybrid_context(&session, &proactive_budget, &counter).unwrap();
        let hard_only = prepare_hybrid_context(&session, &hard_only_budget, &counter).unwrap();

        assert!(
            proactive.truncation_occurred,
            "Proactive trigger should truncate before hard budget is exceeded"
        );
        assert!(
            !hard_only.truncation_occurred,
            "Hard-limit-only budget should keep this context"
        );
    }

    #[test]
    fn compression_reduces_context_to_target_threshold() {
        let counter = HeuristicTokenCounter::default();
        let mut budget =
            TokenBudget::with_safety_margin(900, 200, BudgetStrategy::Window { size: 80 }, 100);
        budget.compression_trigger_percent = 80;
        budget.compression_target_percent = 50;

        let mut messages = vec![Message::system("System prompt")];
        for i in 0..20 {
            messages.push(Message::user(format!(
                "Question {} with enough content to pressure token usage in the context window.",
                i
            )));
            messages.push(Message::assistant(
                format!(
                    "Answer {} with enough content to pressure token usage in the context window.",
                    i
                ),
                None,
            ));
        }

        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();
        let keeps_latest_goal = prepared
            .messages
            .iter()
            .any(|message| message.role == Role::User && message.content.contains("Question 19"));

        assert!(prepared.truncation_occurred);
        assert!(
            prepared.token_usage.total_tokens <= budget.compression_target_input_tokens(),
            "Post-compression context should be at or below target threshold"
        );
        assert!(
            keeps_latest_goal,
            "Latest user goal/request should survive compression"
        );
    }
}
