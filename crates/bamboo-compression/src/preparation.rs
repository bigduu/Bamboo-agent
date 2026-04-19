//! Context preparation for budget management.
//!
//! Implements the hybrid context preparation algorithm that enforces token
//! budgets while preserving tool-call chain atomicity.

use bamboo_agent_core::{Role, Session};
use crate::counter::TokenCounter;
use crate::segmenter::{MessageSegment, MessageSegmenter};
use crate::types::{
    BudgetError, BudgetStrategy, PreparedContext, TokenBudget, TokenUsageBreakdown,
};
use std::collections::{HashMap, HashSet};

const PROMPT_CACHE_MAX_MIN_TOOL_OUTPUT_CHARS: usize = 200_000;
const PROMPT_CACHE_MAX_EXCERPT_CHARS: usize = 20_000;
const PROMPT_CACHE_MAX_RECENT_USER_TURNS: usize = 64;
const PROMPT_CACHE_MAX_RECENT_TOOL_CHAINS: usize = 64;
const PROMPT_CACHE_MARKER: &str = "[cached_tool_output]";

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
/// use crate::{TokenBudget, TiktokenTokenCounter, prepare_hybrid_context};
///
/// let budget = TokenBudget::for_model(128_000);
/// let counter = TiktokenTokenCounter::default();
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
    let summary_message = session.conversation_summary.as_ref().map(|summary| {
        crate::compression_tooling::compression_summary_message(
            &summary.content,
        )
    });
    let summary_tokens = summary_message
        .as_ref()
        .map(|message| counter.count_messages(std::slice::from_ref(message)))
        .unwrap_or(0);
    let active_messages: Vec<_> = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .cloned()
        .collect();
    let prompt_cache_result = maybe_compact_old_tool_outputs_for_prompt(
        session,
        active_messages,
        budget,
        counter,
        summary_tokens,
    );
    let active_messages = prompt_cache_result.messages;

    // 1. Extract system messages (always included) - takes ownership, no clone needed
    let (system_messages, mut segments) = segmenter.segment_with_system(active_messages);

    // 2. Count system tokens
    let system_tokens = counter.count_messages(&system_messages);

    // 3. Check if system prompt alone exceeds context window
    let hard_limit = budget.max_context_tokens;
    if system_tokens > hard_limit {
        return Err(BudgetError::SystemPromptTooLarge {
            system_tokens,
            available_tokens: hard_limit,
        });
    }

    // 4. Calculate remaining budget after system messages and any existing
    // summary. Automatic trimming here is hard-limit safety fitting only.
    // Proactive trigger/target thresholds are handled by the host-side
    // compression pipeline before this stage.
    let hard_remaining_budget = hard_limit
        .saturating_sub(system_tokens)
        .saturating_sub(summary_tokens);

    // 5. Count tokens for each segment
    for segment in &mut segments {
        segment.token_estimate = counter.count_messages(&segment.messages);
    }

    let pre_window_tokens: u32 = segments.iter().fold(0u32, |acc, segment| {
        acc.saturating_add(segment.token_estimate)
    });
    let hard_fit_needed = pre_window_tokens > hard_remaining_budget;
    let remaining_budget = hard_remaining_budget;
    if hard_fit_needed {
        let pre_total_tokens = system_tokens
            .saturating_add(summary_tokens)
            .saturating_add(pre_window_tokens);
        let pre_usage_pct = if hard_limit == 0 {
            0.0
        } else {
            (pre_total_tokens as f64 / hard_limit as f64) * 100.0
        };
        tracing::info!(
            "[{}] Context hard-limit fit needed: pre_total={} (system={}, summary={}, window={}), hard_limit={}, usage={:.1}%",
            session.id,
            pre_total_tokens,
            system_tokens,
            summary_tokens,
            pre_window_tokens,
            hard_limit,
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
    if let Some(summary_message) = summary_message {
        prepared_messages.push(summary_message);
    }

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
        budget_limit: hard_limit,
    };

    let truncation_occurred = removed_count > 0;
    if truncation_occurred {
        tracing::info!(
            "[{}] Context hard-limit fit result: removed_segments={}, removed_messages={}, removed_tool_segments={}, removed_tokens={}, kept_segments={}, kept_messages={}, final_total={} / {} ({:.1}%)",
            session.id,
            removed_count,
            removed_messages_count,
            removed_tool_segments_count,
            removed_tokens,
            selected_segments.len(),
            kept_messages_count,
            total_tokens,
            hard_limit,
            token_usage.usage_percentage()
        );
    }

    Ok(PreparedContext {
        messages: prepared_messages,
        token_usage,
        truncation_occurred,
        segments_removed: removed_count,
        compressed_message_ids,
        prompt_cached_tool_outputs: prompt_cache_result.compacted_tool_outputs,
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
            if segment_contains_skill_tool_chain(&segments[index]) {
                continue;
            }
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

struct PromptCacheCompactionResult {
    messages: Vec<bamboo_agent_core::Message>,
    compacted_tool_outputs: usize,
}

#[derive(Clone, Copy)]
struct PromptCachePolicy {
    min_tool_output_chars: usize,
    head_chars: usize,
    tail_chars: usize,
    recent_user_turns: usize,
    recent_tool_chains: usize,
}

struct PromptCacheCandidate {
    index: usize,
    cached_summary: String,
    old_tokens: u32,
    new_tokens: u32,
    saved_tokens: u32,
}

fn maybe_compact_old_tool_outputs_for_prompt(
    session: &Session,
    mut active_messages: Vec<bamboo_agent_core::Message>,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
    summary_tokens: u32,
) -> PromptCacheCompactionResult {
    if active_messages.is_empty() {
        return PromptCacheCompactionResult {
            messages: active_messages,
            compacted_tool_outputs: 0,
        };
    }
    let policy = prompt_cache_policy_from_budget(budget);

    let context_window = budget.max_context_tokens;
    if context_window == 0 {
        return PromptCacheCompactionResult {
            messages: active_messages,
            compacted_tool_outputs: 0,
        };
    }

    // Keep the configured number of latest user turns unmodified when
    // compacting older tool traces. conclusion_with_options tool calls do not create
    // Role::User messages, so they are naturally excluded from this
    // turn boundary.
    let Some(protected_turn_start) =
        recent_user_turn_start_index(&active_messages, policy.recent_user_turns)
    else {
        return PromptCacheCompactionResult {
            messages: active_messages,
            compacted_tool_outputs: 0,
        };
    };

    let trigger_limit = budget.compression_trigger_context_tokens();
    let mut total_tokens = counter
        .count_messages(&active_messages)
        .saturating_add(summary_tokens);

    if total_tokens <= trigger_limit {
        return PromptCacheCompactionResult {
            messages: active_messages,
            compacted_tool_outputs: 0,
        };
    }

    let usage_before = (total_tokens as f64 / context_window as f64) * 100.0;
    let tool_call_names = tool_call_name_index(&active_messages);
    let protected_recent_calls =
        collect_recent_tool_call_ids(&active_messages, policy.recent_tool_chains);
    let mut candidates = build_prompt_cache_candidates(
        &active_messages,
        protected_turn_start,
        &protected_recent_calls,
        &tool_call_names,
        policy,
        counter,
    );

    if candidates.is_empty() {
        return PromptCacheCompactionResult {
            messages: active_messages,
            compacted_tool_outputs: 0,
        };
    }

    // Compact highest-yield candidates first so we minimize how many messages
    // are rewritten while still getting below the trigger threshold.
    candidates.sort_by(|a, b| {
        b.saved_tokens
            .cmp(&a.saved_tokens)
            .then_with(|| a.index.cmp(&b.index))
    });

    let mut compacted_count = 0usize;
    let mut saved_tokens_total = 0u32;

    for candidate in candidates {
        if total_tokens <= trigger_limit {
            break;
        }
        let message = &mut active_messages[candidate.index];
        message.content = candidate.cached_summary;

        compacted_count += 1;
        saved_tokens_total = saved_tokens_total.saturating_add(candidate.saved_tokens);
        total_tokens = total_tokens
            .saturating_sub(candidate.old_tokens)
            .saturating_add(candidate.new_tokens);
    }

    if compacted_count > 0 {
        let usage_after = (total_tokens as f64 / context_window as f64) * 100.0;
        tracing::info!(
            "[{}] Prompt-side tool output cache applied: compacted_messages={}, saved_tokens={}, usage_before={:.1}%, usage_after={:.1}%, trigger={}%",
            session.id,
            compacted_count,
            saved_tokens_total,
            usage_before,
            usage_after,
            budget.compression_trigger_percent
        );
    }

    PromptCacheCompactionResult {
        messages: active_messages,
        compacted_tool_outputs: compacted_count,
    }
}

fn build_prompt_cache_candidates(
    messages: &[bamboo_agent_core::Message],
    protected_turn_start: usize,
    protected_recent_calls: &HashSet<String>,
    tool_call_names: &HashMap<String, String>,
    policy: PromptCachePolicy,
    counter: &dyn TokenCounter,
) -> Vec<PromptCacheCandidate> {
    let mut candidates = Vec::new();

    for (index, message) in messages.iter().enumerate() {
        if index >= protected_turn_start || message.role != Role::Tool {
            continue;
        }

        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        if protected_recent_calls.contains(tool_call_id) {
            continue;
        }

        let Some(tool_name) = tool_call_names.get(tool_call_id) else {
            continue;
        };
        if !is_cacheable_tool_name(tool_name) {
            continue;
        }

        let original_char_count = message.content.chars().count();
        if original_char_count < policy.min_tool_output_chars {
            continue;
        }

        let cached_summary = build_cached_tool_output_summary(
            tool_name,
            tool_call_id,
            &message.content,
            policy.head_chars,
            policy.tail_chars,
        );
        if cached_summary.chars().count() >= original_char_count {
            continue;
        }

        let old_tokens = counter.count_message(message);
        if old_tokens == 0 {
            continue;
        }

        let mut preview_message = message.clone();
        preview_message.content = cached_summary.clone();
        let new_tokens = counter.count_message(&preview_message);
        if new_tokens >= old_tokens {
            continue;
        }

        candidates.push(PromptCacheCandidate {
            index,
            cached_summary,
            old_tokens,
            new_tokens,
            saved_tokens: old_tokens.saturating_sub(new_tokens),
        });
    }

    candidates
}

fn recent_user_turn_start_index(
    messages: &[bamboo_agent_core::Message],
    keep_recent_turns: usize,
) -> Option<usize> {
    if keep_recent_turns == 0 {
        return Some(0);
    }
    let user_indices: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| (message.role == Role::User).then_some(index))
        .collect();
    if user_indices.len() < keep_recent_turns {
        return None;
    }
    Some(user_indices[user_indices.len() - keep_recent_turns])
}

fn tool_call_name_index(messages: &[bamboo_agent_core::Message]) -> HashMap<String, String> {
    let mut index = HashMap::new();
    for message in messages {
        if message.role != Role::Assistant {
            continue;
        }
        let Some(tool_calls) = message.tool_calls.as_ref() else {
            continue;
        };
        for call in tool_calls {
            if call.id.trim().is_empty() {
                continue;
            }
            index.insert(call.id.clone(), call.function.name.clone());
        }
    }
    index
}

fn collect_recent_tool_call_ids(
    messages: &[bamboo_agent_core::Message],
    keep_recent_calls: usize,
) -> HashSet<String> {
    let mut result = HashSet::new();
    if keep_recent_calls == 0 {
        return result;
    }

    for message in messages.iter().rev() {
        if message.role != Role::Assistant {
            continue;
        }
        let Some(tool_calls) = message.tool_calls.as_ref() else {
            continue;
        };
        for call in tool_calls.iter().rev() {
            if !call.id.trim().is_empty() {
                result.insert(call.id.clone());
            }
            if result.len() >= keep_recent_calls {
                return result;
            }
        }
    }

    result
}

fn is_cacheable_tool_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Read" | "Grep" | "Bash" | "BashOutput" | "WebFetch"
    )
}

fn prompt_cache_policy_from_budget(budget: &TokenBudget) -> PromptCachePolicy {
    let min_tool_output_chars = (budget.prompt_cache_min_tool_output_chars as usize)
        .min(PROMPT_CACHE_MAX_MIN_TOOL_OUTPUT_CHARS);
    let head_chars = (budget.prompt_cache_head_chars as usize).min(PROMPT_CACHE_MAX_EXCERPT_CHARS);
    let tail_chars = (budget.prompt_cache_tail_chars as usize).min(PROMPT_CACHE_MAX_EXCERPT_CHARS);
    let recent_user_turns =
        (budget.prompt_cache_recent_user_turns as usize).min(PROMPT_CACHE_MAX_RECENT_USER_TURNS);
    let recent_tool_chains =
        (budget.prompt_cache_recent_tool_chains as usize).min(PROMPT_CACHE_MAX_RECENT_TOOL_CHAINS);

    PromptCachePolicy {
        min_tool_output_chars,
        head_chars,
        tail_chars,
        recent_user_turns,
        recent_tool_chains,
    }
}

fn build_cached_tool_output_summary(
    tool_name: &str,
    tool_call_id: &str,
    content: &str,
    head_chars: usize,
    tail_chars: usize,
) -> String {
    let head = take_first_chars(content, head_chars);
    let tail = take_last_chars(content, tail_chars);
    let line_count = content.lines().count();
    let char_count = content.chars().count();

    format!(
        "{PROMPT_CACHE_MARKER}\n\
tool: {tool_name}\n\
tool_call_id: {tool_call_id}\n\
original_chars: {char_count}\n\
original_lines: {line_count}\n\
head_excerpt:\n\
{head}\n\
tail_excerpt:\n\
{tail}\n\
note: Full output remains in session history and UI; this compact summary is used for context efficiency."
    )
}

fn take_first_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn take_last_chars(value: &str, max_chars: usize) -> String {
    let mut tail: Vec<char> = value.chars().rev().take(max_chars).collect();
    tail.reverse();
    tail.into_iter().collect()
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

fn segment_contains_skill_tool_chain(segment: &MessageSegment) -> bool {
    segment.messages.iter().any(|message| {
        if message.role != Role::Assistant {
            return false;
        }
        message.tool_calls.as_ref().is_some_and(|calls| {
            calls.iter().any(|call| {
                matches!(
                    call.function.name.as_str(),
                    "load_skill" | "read_skill_resource"
                )
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{ConversationSummary, Message, Role};
    use crate::compression_summary_message;
    use crate::counter::TiktokenTokenCounter;
    use crate::counter::TokenCounter;
    use bamboo_agent_core::{FunctionCall, ToolCall};
    use std::collections::HashMap;

    struct DeterministicCounter {
        message_tokens: HashMap<String, u32>,
        text_tokens: HashMap<String, u32>,
        default_message_tokens: u32,
    }

    impl DeterministicCounter {
        fn new(default_message_tokens: u32) -> Self {
            Self {
                message_tokens: HashMap::new(),
                text_tokens: HashMap::new(),
                default_message_tokens,
            }
        }

        fn with_message_token(mut self, content: impl Into<String>, tokens: u32) -> Self {
            self.message_tokens.insert(content.into(), tokens);
            self
        }
    }

    impl TokenCounter for DeterministicCounter {
        fn count_message(&self, message: &Message) -> u32 {
            self.message_tokens
                .get(&message.content)
                .copied()
                .unwrap_or(self.default_message_tokens)
        }

        fn count_text(&self, text: &str) -> u32 {
            self.text_tokens.get(text).copied().unwrap_or(0)
        }
    }

    fn create_tool_call(id: &str) -> ToolCall {
        create_named_tool_call(id, "test")
    }

    fn create_named_tool_call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
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
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();
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
            .any(|m| m.role == bamboo_agent_core::Role::System));
    }

    #[test]
    fn truncates_when_budget_exceeded() {
        let counter = TiktokenTokenCounter::default();

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
        let counter = TiktokenTokenCounter::default();
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
            .find(|m| m.role == bamboo_agent_core::Role::User);
        assert!(last_user.is_some());
        assert!(last_user.unwrap().content.contains("Recent"));
    }

    #[test]
    fn preserves_tool_call_chains() {
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();

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
        let counter = TiktokenTokenCounter::default();
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
    fn summary_tokens_contribute_to_hard_limit_fitting() {
        let summary_text = "summary-budget-test";
        let summary_message = compression_summary_message(summary_text);
        let counter = DeterministicCounter::new(1)
            .with_message_token("System", 10)
            .with_message_token("Older user", 20)
            .with_message_token("Older assistant", 20)
            .with_message_token("Recent user", 20)
            .with_message_token("Recent assistant", 20)
            .with_message_token("Latest user", 20)
            .with_message_token(summary_message.content.clone(), 30);

        let mut budget = TokenBudget::with_safety_margin(
            130,
            50,
            BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            0,
        );
        budget.compression_trigger_percent = 80;
        budget.compression_target_percent = 50;

        let mut session = make_session_with_messages(vec![
            Message::system("System"),
            Message::user("Older user"),
            Message::assistant("Older assistant", None),
            Message::user("Recent user"),
            Message::assistant("Recent assistant", None),
            Message::user("Latest user"),
        ]);
        session.conversation_summary = Some(ConversationSummary::new(summary_text, 2, 30));

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();
        let hard_limit = budget.max_context_tokens;

        assert!(
            prepared.truncation_occurred,
            "summary reserve should contribute to hard-limit fitting when it pushes total context over the model context window"
        );
        assert_eq!(prepared.token_usage.summary_tokens, 30);
        assert!(
            prepared.token_usage.total_tokens <= hard_limit,
            "total tokens {} should stay within context window {} when summary is included",
            prepared.token_usage.total_tokens,
            hard_limit
        );
        assert!(
            10 + 30 + 100 > hard_limit,
            "test setup should exceed the hard limit only when summary tokens are counted"
        );
        assert!(
            prepared
                .messages
                .iter()
                .any(|message| message.content.contains(summary_text)),
            "prepared context should include the conversation summary"
        );
    }

    #[test]
    fn handles_empty_session() {
        let counter = TiktokenTokenCounter::default();
        let budget = TokenBudget::for_model(128_000);

        let session = Session::new("empty", "test-model");
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        assert!(!prepared.truncation_occurred);
        assert!(prepared.messages.is_empty());
        assert_eq!(prepared.token_usage.total_tokens, 0);
    }

    #[test]
    fn handles_session_with_only_system() {
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();
        // Tight budget: large message should not fit, but small message should.
        let budget = TokenBudget::new(100, 50, BudgetStrategy::Window { size: 50 });

        // Use diverse text — BPE tokenizers compress repeated chars (e.g. "xxx...")
        // into very few tokens, so varied words produce a reliably large token count.
        let large_text: String = (0..50)
            .map(|i| format!("This is sentence number {i} with various words. "))
            .collect();
        let messages = vec![
            Message::system("System"),
            Message::user(&large_text),
            Message::user("Small message"),
        ];
        let session = make_session_with_messages(messages);

        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        // The large message should be skipped, small message should be included
        let has_large_message = prepared.messages.iter().any(|m| m.content.contains("sentence number"));
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
        // Test behavior when system prompt alone exceeds a tiny context window.
        let counter = TiktokenTokenCounter::default();
        let budget = TokenBudget::new(10, 50, BudgetStrategy::Window { size: 50 });

        let messages = vec![
            Message::system("System prompt that uses most of the budget"),
            Message::user("User message"),
        ];
        let session = make_session_with_messages(messages);

        // Should fail with SystemPromptTooLarge since system prompt exceeds
        // context window.
        let result = prepare_hybrid_context(&session, &budget, &counter);
        assert!(matches!(
            result,
            Err(BudgetError::SystemPromptTooLarge { .. })
        ));
    }

    #[test]
    fn handles_small_budget_with_fitting_system() {
        // Test with budget large enough for system but tight for additional messages
        let counter = TiktokenTokenCounter::default();
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
            .any(|m| m.role == bamboo_agent_core::Role::System);
        assert!(has_system, "System message should always be included");

        // Budget should be enforced
        assert!(
            prepared.token_usage.total_tokens <= prepared.token_usage.budget_limit,
            "Total tokens should not exceed budget limit"
        );
    }

    #[test]
    fn excludes_precompressed_messages_from_llm_context() {
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();
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
        let counter = TiktokenTokenCounter::default();
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
    fn phase_one_preserves_skill_tool_chains_before_other_tool_chains() {
        let mut skill_segment = MessageSegment::from_message(Message::assistant(
            "Loading skill instructions",
            Some(vec![create_named_tool_call("call_skill", "load_skill")]),
        ));
        skill_segment.messages.push(Message::tool_result(
            "call_skill",
            "skill instructions payload",
        ));
        skill_segment.token_estimate = 120;

        let mut other_segment = MessageSegment::from_message(Message::assistant(
            "Running non-skill tool",
            Some(vec![create_named_tool_call("call_other", "Grep")]),
        ));
        other_segment
            .messages
            .push(Message::tool_result("call_other", "other output payload"));
        other_segment.token_estimate = 120;

        let selection = select_segments_within_budget(
            vec![skill_segment, other_segment],
            120,
            &BudgetStrategy::Window { size: 50 },
        );

        let selected_has_skill = selection.selected.iter().any(|segment| {
            segment
                .messages
                .iter()
                .any(|message| message.tool_call_id.as_deref() == Some("call_skill"))
        });
        let selected_has_other = selection.selected.iter().any(|segment| {
            segment
                .messages
                .iter()
                .any(|message| message.tool_call_id.as_deref() == Some("call_other"))
        });

        assert!(selected_has_skill, "load_skill chain should be preserved");
        assert!(
            !selected_has_other,
            "non-skill tool chain should be removed first in phase one"
        );
    }

    #[test]
    fn trigger_percent_does_not_force_auto_truncation_before_hard_limit() {
        let counter = TiktokenTokenCounter::default();
        let mut trigger_fifty_budget =
            TokenBudget::with_safety_margin(400, 100, BudgetStrategy::Window { size: 50 }, 100);
        trigger_fifty_budget.compression_trigger_percent = 50;

        let mut trigger_hundred_budget = trigger_fifty_budget.clone();
        trigger_hundred_budget.compression_trigger_percent = 100;

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

        let trigger_fifty =
            prepare_hybrid_context(&session, &trigger_fifty_budget, &counter).unwrap();
        let trigger_hundred =
            prepare_hybrid_context(&session, &trigger_hundred_budget, &counter).unwrap();

        assert!(
            !trigger_fifty.truncation_occurred,
            "Crossing the configured tool exposure trigger should not auto-truncate context before the hard limit"
        );
        assert!(
            !trigger_hundred.truncation_occurred,
            "Hard-limit-only budget should also keep this context"
        );
        assert_eq!(trigger_fifty.messages.len(), trigger_hundred.messages.len());
        assert_eq!(
            trigger_fifty.token_usage.total_tokens,
            trigger_hundred.token_usage.total_tokens
        );
    }

    #[test]
    fn prompt_cache_compacts_old_tool_output_after_trigger_and_preserves_recent_turns() {
        let old_tool_output = "old-read-output ".repeat(120);
        let recent_tool_output = "recent-read-output ".repeat(120);

        let counter = DeterministicCounter::new(6)
            .with_message_token("System", 20)
            .with_message_token("User turn 1", 15)
            .with_message_token("Reading old files", 10)
            .with_message_token(old_tool_output.clone(), 220)
            .with_message_token("Old analysis", 10)
            .with_message_token("Need confirmation", 10)
            .with_message_token("User selected: OK", 8)
            .with_message_token("User turn 2", 15)
            .with_message_token("Reading recent files", 10)
            .with_message_token(recent_tool_output.clone(), 220)
            .with_message_token("Recent analysis", 10)
            .with_message_token("User turn 3", 15)
            .with_message_token("Current conclusion", 10);

        let mut budget =
            TokenBudget::with_safety_margin(650, 100, BudgetStrategy::Window { size: 60 }, 0);
        budget.compression_trigger_percent = 80;

        let messages = vec![
            Message::system("System"),
            Message::user("User turn 1"),
            Message::assistant(
                "Reading old files",
                Some(vec![create_named_tool_call("call_old", "Read")]),
            ),
            Message::tool_result("call_old", old_tool_output.clone()),
            Message::assistant("Old analysis", None),
            Message::assistant(
                "Need confirmation",
                Some(vec![create_named_tool_call(
                    "call_ask",
                    "conclusion_with_options",
                )]),
            ),
            Message::tool_result("call_ask", "User selected: OK"),
            Message::user("User turn 2"),
            Message::assistant(
                "Reading recent files",
                Some(vec![create_named_tool_call("call_recent", "Read")]),
            ),
            Message::tool_result("call_recent", recent_tool_output.clone()),
            Message::assistant("Recent analysis", None),
            Message::user("User turn 3"),
            Message::assistant("Current conclusion", None),
        ];
        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        let old_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_old"))
            .expect("old tool output should stay in prepared context");
        assert!(
            old_tool.content.contains(PROMPT_CACHE_MARKER),
            "older tool output should be replaced with cached summary once trigger is exceeded"
        );

        let recent_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_recent"))
            .expect("recent tool output should stay in prepared context");
        assert!(
            recent_tool.content.contains("recent-read-output"),
            "recent-turn tool output should remain unmodified"
        );
    }

    #[test]
    fn prompt_cache_turn_boundary_is_based_on_user_messages_not_conclusion_with_options_calls() {
        let turn_one_tool_output = "turn-one-output ".repeat(120);
        let turn_two_tool_output = "turn-two-output ".repeat(120);

        let counter = DeterministicCounter::new(6)
            .with_message_token("System", 20)
            .with_message_token("First request", 15)
            .with_message_token("Turn one read", 10)
            .with_message_token(turn_one_tool_output.clone(), 220)
            .with_message_token("Need confirmation", 10)
            .with_message_token("User selected: Need changes", 8)
            .with_message_token("Second request", 15)
            .with_message_token("Turn two read", 10)
            .with_message_token(turn_two_tool_output.clone(), 220)
            .with_message_token("Third request", 15)
            .with_message_token("Done", 10);

        let mut budget =
            TokenBudget::with_safety_margin(680, 100, BudgetStrategy::Window { size: 60 }, 0);
        budget.compression_trigger_percent = 80;

        let messages = vec![
            Message::system("System"),
            Message::user("First request"),
            Message::assistant(
                "Turn one read",
                Some(vec![create_named_tool_call("call_turn_one", "Read")]),
            ),
            Message::tool_result("call_turn_one", turn_one_tool_output.clone()),
            Message::assistant(
                "Need confirmation",
                Some(vec![create_named_tool_call(
                    "call_ask",
                    "conclusion_with_options",
                )]),
            ),
            Message::tool_result("call_ask", "User selected: Need changes"),
            Message::user("Second request"),
            Message::assistant(
                "Turn two read",
                Some(vec![create_named_tool_call("call_turn_two", "Read")]),
            ),
            Message::tool_result("call_turn_two", turn_two_tool_output.clone()),
            Message::user("Third request"),
            Message::assistant("Done", None),
        ];
        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        let turn_one_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_turn_one"))
            .expect("turn one tool output should stay in prepared context");
        assert!(
            turn_one_tool.content.contains(PROMPT_CACHE_MARKER),
            "older turn should be cache-compacted when trigger is exceeded"
        );

        let turn_two_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_turn_two"))
            .expect("turn two tool output should stay in prepared context");
        assert!(
            turn_two_tool.content.contains("turn-two-output"),
            "latest user turn should stay untouched; conclusion_with_options chain must not count as a separate user turn"
        );
    }

    #[test]
    fn prompt_cache_prioritizes_highest_token_savings_first() {
        let small_tool_output = "small-output ".repeat(120);
        let large_tool_output = "large-output ".repeat(240);

        let counter = DeterministicCounter::new(6)
            .with_message_token("System", 20)
            .with_message_token("Turn one", 15)
            .with_message_token("Read smaller file", 10)
            .with_message_token(small_tool_output.clone(), 120)
            .with_message_token("Read larger file", 10)
            .with_message_token(large_tool_output.clone(), 280)
            .with_message_token("Early analysis", 10)
            .with_message_token("Turn two", 15)
            .with_message_token("Read recent files 1", 10)
            .with_message_token("recent-output-1", 8)
            .with_message_token("Second turn analysis", 10)
            .with_message_token("Turn three", 15)
            .with_message_token("Read recent files 2", 10)
            .with_message_token("recent-output-2", 8)
            .with_message_token("Current conclusion", 10);

        let mut budget =
            TokenBudget::with_safety_margin(650, 100, BudgetStrategy::Window { size: 60 }, 0);
        budget.compression_trigger_percent = 80;

        let messages = vec![
            Message::system("System"),
            Message::user("Turn one"),
            Message::assistant(
                "Read smaller file",
                Some(vec![create_named_tool_call("call_small", "Read")]),
            ),
            Message::tool_result("call_small", small_tool_output.clone()),
            Message::assistant(
                "Read larger file",
                Some(vec![create_named_tool_call("call_large", "Read")]),
            ),
            Message::tool_result("call_large", large_tool_output.clone()),
            Message::assistant("Early analysis", None),
            Message::user("Turn two"),
            Message::assistant(
                "Read recent files 1",
                Some(vec![create_named_tool_call("call_recent_one", "Read")]),
            ),
            Message::tool_result("call_recent_one", "recent-output-1"),
            Message::assistant("Second turn analysis", None),
            Message::user("Turn three"),
            Message::assistant(
                "Read recent files 2",
                Some(vec![create_named_tool_call("call_recent_two", "Read")]),
            ),
            Message::tool_result("call_recent_two", "recent-output-2"),
            Message::assistant("Current conclusion", None),
        ];

        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        let small_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_small"))
            .expect("small tool output should stay in prepared context");
        assert!(
            small_tool.content.contains("small-output"),
            "smaller candidate should remain untouched when larger candidate can satisfy trigger"
        );

        let large_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_large"))
            .expect("large tool output should stay in prepared context");
        assert!(
            large_tool.content.contains(PROMPT_CACHE_MARKER),
            "largest savings candidate should be compacted first"
        );

        assert_eq!(
            prepared.prompt_cached_tool_outputs, 1,
            "only one compaction should be required when highest-savings candidate is selected first"
        );
    }

    #[test]
    fn prompt_cache_respects_budget_min_tool_output_chars_setting() {
        let old_tool_output = "old-read-output ".repeat(120);
        let recent_tool_output = "recent-read-output ".repeat(120);

        let counter = DeterministicCounter::new(6)
            .with_message_token("System", 20)
            .with_message_token("User turn 1", 15)
            .with_message_token("Reading old files", 10)
            .with_message_token(old_tool_output.clone(), 220)
            .with_message_token("Old analysis", 10)
            .with_message_token("User turn 2", 15)
            .with_message_token("Reading recent files", 10)
            .with_message_token(recent_tool_output.clone(), 220)
            .with_message_token("Recent analysis", 10)
            .with_message_token("User turn 3", 15)
            .with_message_token("Current conclusion", 10);

        let mut budget =
            TokenBudget::with_safety_margin(700, 100, BudgetStrategy::Window { size: 60 }, 0);
        budget.compression_trigger_percent = 80;
        budget.prompt_cache_min_tool_output_chars = 10_000;

        let messages = vec![
            Message::system("System"),
            Message::user("User turn 1"),
            Message::assistant(
                "Reading old files",
                Some(vec![create_named_tool_call("call_old", "Read")]),
            ),
            Message::tool_result("call_old", old_tool_output.clone()),
            Message::assistant("Old analysis", None),
            Message::user("User turn 2"),
            Message::assistant(
                "Reading recent files",
                Some(vec![create_named_tool_call("call_recent", "Read")]),
            ),
            Message::tool_result("call_recent", recent_tool_output.clone()),
            Message::assistant("Recent analysis", None),
            Message::user("User turn 3"),
            Message::assistant("Current conclusion", None),
        ];

        let session = make_session_with_messages(messages);
        let prepared = prepare_hybrid_context(&session, &budget, &counter).unwrap();

        let old_tool = prepared
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("call_old"))
            .expect("old tool output should stay in prepared context");
        assert!(
            old_tool.content.contains("old-read-output"),
            "raising min_tool_output_chars should suppress prompt-side cache compaction"
        );
        assert_eq!(prepared.prompt_cached_tool_outputs, 0);
    }

    #[test]
    fn hard_limit_fit_stays_within_budget_limit_and_keeps_latest_goal() {
        let counter = TiktokenTokenCounter::default();
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
            prepared.token_usage.total_tokens <= prepared.token_usage.budget_limit,
            "Hard-limit fitting should keep total tokens within the model context window"
        );
        assert!(
            keeps_latest_goal,
            "Latest user goal/request should survive hard-limit fitting"
        );
    }
}
