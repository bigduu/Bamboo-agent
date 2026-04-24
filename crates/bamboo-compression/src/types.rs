//! Core types for token budget management.
//!
//! TokenBudget, BudgetStrategy, TokenUsageBreakdown, and TokenBudgetUsage
//! are re-exported from bamboo-domain-session.
//! This file keeps PreparedContext and BudgetError in the facade for Phase 1.

// Re-exported from domain crate
pub use bamboo_domain::budget_types::{
    BudgetStrategy, TokenBudget, TokenBudgetUsage, TokenUsageBreakdown,
};

use thiserror::Error;

/// Result of context preparation with budget enforcement.
#[derive(Debug, Clone)]
pub struct PreparedContext {
    /// Messages prepared for LLM (may be truncated)
    pub messages: Vec<bamboo_domain::Message>,
    /// Token usage breakdown
    pub token_usage: TokenUsageBreakdown,
    /// Whether truncation occurred
    pub truncation_occurred: bool,
    /// Number of message segments removed
    pub segments_removed: usize,
    /// Message IDs newly archived by this preparation pass.
    pub compressed_message_ids: Vec<String>,
    /// Number of long tool outputs replaced with prompt-side cached summaries.
    pub prompt_cached_tool_outputs: usize,
}

/// Errors that can occur during budget management.
#[derive(Debug, Error)]
pub enum BudgetError {
    #[error("System prompt ({system_tokens} tokens) exceeds available budget ({available_tokens} tokens)")]
    SystemPromptTooLarge {
        system_tokens: u32,
        available_tokens: u32,
    },

    #[error("Single message ({message_tokens} tokens) exceeds available budget ({available_tokens} tokens). Consider splitting the message or attaching as a file.")]
    SingleMessageTooLarge {
        message_tokens: u32,
        available_tokens: u32,
    },

    #[error("Failed to count tokens: {0}")]
    TokenCountError(String),

    #[error("Failed to segment messages: {0}")]
    SegmentationError(String),
}

#[cfg(test)]
mod tests {
    use super::{BudgetStrategy, TokenBudget};

    #[test]
    fn compression_trigger_defaults_to_eighty_five_percent() {
        let budget = TokenBudget::for_model(128_000);
        assert_eq!(budget.compression_trigger_percent, 85);
    }

    #[test]
    fn compression_target_defaults_to_forty_percent() {
        let budget = TokenBudget::for_model(128_000);
        assert_eq!(budget.compression_target_percent, 40);
    }

    #[test]
    fn prompt_cache_defaults_match_current_compaction_policy() {
        let budget = TokenBudget::for_model(128_000);
        assert_eq!(budget.prompt_cache_min_tool_output_chars, 1_200);
        assert_eq!(budget.prompt_cache_head_chars, 280);
        assert_eq!(budget.prompt_cache_tail_chars, 180);
        assert_eq!(budget.prompt_cache_recent_user_turns, 2);
        assert_eq!(budget.prompt_cache_recent_tool_chains, 2);
    }

    #[test]
    fn compression_trigger_context_tokens_respects_percent() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.working_reserve_tokens = 0; // use legacy percentage mode
        budget.compression_trigger_percent = 50;
        assert_eq!(budget.compression_trigger_context_tokens(), 500);
    }

    #[test]
    fn compression_target_context_tokens_respects_percent() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.compression_target_percent = 50;
        assert_eq!(budget.compression_target_context_tokens(), 500);
    }

    #[test]
    fn compression_target_percent_is_clamped_to_supported_range() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.compression_target_percent = 10;
        assert_eq!(budget.compression_target_context_tokens(), 200);
    }

    #[test]
    fn compression_target_always_stays_below_trigger_limit() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.working_reserve_tokens = 0; // use legacy percentage mode
        budget.compression_trigger_percent = 30;
        budget.compression_target_percent = 50;
        assert_eq!(budget.compression_target_context_tokens(), 299);
    }

    #[test]
    fn trigger_percent_zero_means_disabled() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.working_reserve_tokens = 0; // use legacy percentage mode
        budget.compression_trigger_percent = 0;
        assert_eq!(
            budget.compression_trigger_context_tokens(),
            budget.max_context_tokens
        );
    }

    #[test]
    fn fixed_reserve_trigger_subtracts_reserve_from_context() {
        let budget = TokenBudget::with_safety_margin(200_000, 4_096, BudgetStrategy::default(), 1000);
        // default working_reserve_tokens = 50_000
        assert_eq!(budget.compression_trigger_context_tokens(), 150_000);
    }

    #[test]
    fn fixed_reserve_trigger_for_small_context() {
        let budget = TokenBudget::with_safety_margin(100_000, 4_096, BudgetStrategy::default(), 1000);
        // 100K >= 50K * 2, so fixed reserve: 100K - 50K = 50K
        assert_eq!(budget.compression_trigger_context_tokens(), 50_000);
    }

    #[test]
    fn fixed_reserve_fallback_for_tiny_context() {
        let budget = TokenBudget::with_safety_margin(60_000, 4_096, BudgetStrategy::default(), 1000);
        // 60K < 50K * 2 = 100K, so fallback to 75%
        assert_eq!(budget.compression_trigger_context_tokens(), 45_000);
    }

    #[test]
    fn working_reserve_zero_uses_legacy_percentage() {
        let mut budget =
            TokenBudget::with_safety_margin(200_000, 4_096, BudgetStrategy::default(), 1000);
        budget.working_reserve_tokens = 0;
        budget.compression_trigger_percent = 85;
        assert_eq!(budget.compression_trigger_context_tokens(), 170_000);
    }
}
