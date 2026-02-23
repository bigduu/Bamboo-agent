//! Context preparation for budget management.
//!
//! Implements the hybrid context preparation algorithm that enforces token
//! budgets while preserving tool-call chain atomicity.

use crate::agent::core::agent::types::Session;
use crate::agent::core::budget::counter::TokenCounter;
use crate::agent::core::budget::segmenter::MessageSegmenter;
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

    // 1. Extract system messages (always included) - takes ownership, no clone needed
    let (system_messages, mut segments) = segmenter.segment_with_system(session.messages.clone());

    // 2. Count system tokens
    let system_tokens = counter.count_messages(&system_messages);

    // 3. Check if system prompt alone exceeds budget
    let available = budget.available_input_tokens();
    if system_tokens > available {
        return Err(BudgetError::SystemPromptTooLarge {
            system_tokens,
            available_tokens: available,
        });
    }

    // 4. Calculate remaining budget after system messages
    let remaining_budget = available.saturating_sub(system_tokens);

    // 5. Count tokens for each segment
    for segment in &mut segments {
        segment.token_estimate = counter.count_messages(&segment.messages);
    }

    // 6. Select segments from the end until budget is filled
    let (mut selected_segments, removed_count) =
        select_segments_within_budget(segments, remaining_budget, &budget.strategy);

    // 7. Build final message list
    let mut prepared_messages = system_messages;

    // Add summary if available (Phase 2 - future)
    // For now, summary is always 0
    let summary_tokens = 0;

    // Add selected segments - use take to avoid cloning
    for segment in &mut selected_segments {
        prepared_messages.append(&mut segment.messages);
    }

    // 8. Calculate final token usage
    let window_tokens: u32 = selected_segments
        .iter()
        .fold(0u32, |acc, s| acc.saturating_add(s.token_estimate));

    let total_tokens = system_tokens
        .saturating_add(summary_tokens)
        .saturating_add(window_tokens);

    let token_usage = TokenUsageBreakdown {
        system_tokens,
        summary_tokens,
        window_tokens,
        total_tokens,
        budget_limit: available,
    };

    let truncation_occurred = removed_count > 0;

    Ok(PreparedContext {
        messages: prepared_messages,
        token_usage,
        truncation_occurred,
        segments_removed: removed_count,
    })
}

/// Select message segments within the remaining budget.
///
/// Takes segments from the end (most recent) until budget is filled.
/// Respects tool-chain atomicity - never splits tool calls from their results.
fn select_segments_within_budget(
    segments: Vec<crate::agent::core::budget::segmenter::MessageSegment>,
    remaining_budget: u32,
    _strategy: &BudgetStrategy,
) -> (
    Vec<crate::agent::core::budget::segmenter::MessageSegment>,
    usize,
) {
    let total_segments = segments.len();
    let mut selected: Vec<crate::agent::core::budget::segmenter::MessageSegment> = Vec::new();
    let mut current_tokens: u32 = 0;

    // Iterate from the end (most recent) backwards
    for segment in segments.into_iter().rev() {
        let segment_tokens = segment.token_estimate;

        // Check if this segment alone exceeds the budget
        if segment_tokens > remaining_budget {
            if selected.is_empty() {
                // Edge case: single message exceeds entire budget
                // Log warning but skip it (don't include oversized segments)
                tracing::warn!(
                    "Single segment ({tokens} tokens) exceeds remaining budget ({budget} tokens), skipping",
                    tokens = segment_tokens,
                    budget = remaining_budget
                );
                // Skip this segment and continue to try to find smaller ones
                continue;
            } else {
                // Budget filled, stop selecting
                break;
            }
        }

        // Check if adding this segment would exceed budget
        if current_tokens.saturating_add(segment_tokens) > remaining_budget {
            break;
        }

        current_tokens = current_tokens.saturating_add(segment_tokens);
        selected.push(segment);
    }

    // Reverse to restore chronological order
    selected.reverse();

    let removed_count = total_segments - selected.len();

    (selected, removed_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::agent::types::Message;
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
        // Very small budget
        let budget = TokenBudget::new(200, 50, BudgetStrategy::Window { size: 50 });

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
}
