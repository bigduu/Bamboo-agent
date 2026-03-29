//! Core types for token budget management.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default safety margin as a percentage of context window (1%).
const DEFAULT_SAFETY_MARGIN_PERCENT: f64 = 0.01;
/// Minimum safety margin in tokens.
const MIN_SAFETY_MARGIN: u32 = 100;
/// Maximum safety margin in tokens.
const MAX_SAFETY_MARGIN: u32 = 2000;
/// Default percentage of context window where proactive compression starts.
const DEFAULT_COMPRESSION_TRIGGER_PERCENT: u8 = 85;
/// Default percentage of context window to target after compression.
const DEFAULT_COMPRESSION_TARGET_PERCENT: u8 = 40;
/// Minimum allowed compression target percent.
const MIN_COMPRESSION_TARGET_PERCENT: u32 = 20;
/// Maximum allowed compression target percent.
const MAX_COMPRESSION_TARGET_PERCENT: u32 = 50;
/// Default minimum tool output length (chars) before prompt-side cache compaction is considered.
const DEFAULT_PROMPT_CACHE_MIN_TOOL_OUTPUT_CHARS: u32 = 1_200;
/// Default number of leading characters preserved in cached tool output summaries.
const DEFAULT_PROMPT_CACHE_HEAD_CHARS: u32 = 280;
/// Default number of trailing characters preserved in cached tool output summaries.
const DEFAULT_PROMPT_CACHE_TAIL_CHARS: u32 = 180;
/// Default number of latest user turns protected from prompt-side cache compaction.
const DEFAULT_PROMPT_CACHE_RECENT_USER_TURNS: u8 = 2;
/// Default number of latest tool call chains protected from prompt-side cache compaction.
const DEFAULT_PROMPT_CACHE_RECENT_TOOL_CHAINS: u8 = 2;

/// Token budget configuration for a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBudget {
    /// Maximum context window size for the model (input + output)
    pub max_context_tokens: u32,
    /// Maximum tokens reserved for model output
    pub max_output_tokens: u32,
    /// Budget enforcement strategy
    pub strategy: BudgetStrategy,
    /// Safety margin for tokenizer estimation errors
    #[serde(default = "default_safety_margin")]
    pub safety_margin: u32,
    /// Proactive compression trigger threshold as a percentage of context window tokens.
    ///
    /// Example: `85` means start compressing once context reaches 85% of context window,
    /// rather than waiting until the hard limit is exceeded.
    #[serde(default = "default_compression_trigger_percent")]
    pub compression_trigger_percent: u8,
    /// Compression target threshold as a percentage of context window tokens.
    ///
    /// Once compression is triggered, context is reduced to this target percentage
    /// to avoid repeatedly compressing around the trigger line. Values are clamped
    /// to the supported range `[20, 50]`.
    #[serde(default = "default_compression_target_percent")]
    pub compression_target_percent: u8,
    /// Minimum tool output character length required before prompt-side cache compaction.
    #[serde(default = "default_prompt_cache_min_tool_output_chars")]
    pub prompt_cache_min_tool_output_chars: u32,
    /// Leading excerpt length (chars) kept in cached tool output summaries.
    #[serde(default = "default_prompt_cache_head_chars")]
    pub prompt_cache_head_chars: u32,
    /// Trailing excerpt length (chars) kept in cached tool output summaries.
    #[serde(default = "default_prompt_cache_tail_chars")]
    pub prompt_cache_tail_chars: u32,
    /// Number of latest user turns protected from prompt-side cache compaction.
    #[serde(default = "default_prompt_cache_recent_user_turns")]
    pub prompt_cache_recent_user_turns: u8,
    /// Number of latest tool call chains protected from prompt-side cache compaction.
    #[serde(default = "default_prompt_cache_recent_tool_chains")]
    pub prompt_cache_recent_tool_chains: u8,
}

fn default_safety_margin() -> u32 {
    // Default for deserialization when field is missing
    1000
}

fn default_compression_trigger_percent() -> u8 {
    DEFAULT_COMPRESSION_TRIGGER_PERCENT
}

fn default_compression_target_percent() -> u8 {
    DEFAULT_COMPRESSION_TARGET_PERCENT
}

fn default_prompt_cache_min_tool_output_chars() -> u32 {
    DEFAULT_PROMPT_CACHE_MIN_TOOL_OUTPUT_CHARS
}

fn default_prompt_cache_head_chars() -> u32 {
    DEFAULT_PROMPT_CACHE_HEAD_CHARS
}

fn default_prompt_cache_tail_chars() -> u32 {
    DEFAULT_PROMPT_CACHE_TAIL_CHARS
}

fn default_prompt_cache_recent_user_turns() -> u8 {
    DEFAULT_PROMPT_CACHE_RECENT_USER_TURNS
}

fn default_prompt_cache_recent_tool_chains() -> u8 {
    DEFAULT_PROMPT_CACHE_RECENT_TOOL_CHAINS
}

impl TokenBudget {
    /// Create a new token budget with the specified parameters.
    pub fn new(max_context_tokens: u32, max_output_tokens: u32, strategy: BudgetStrategy) -> Self {
        let safety_margin = calculate_safety_margin(max_context_tokens);
        Self {
            max_context_tokens,
            max_output_tokens,
            strategy,
            safety_margin,
            compression_trigger_percent: default_compression_trigger_percent(),
            compression_target_percent: default_compression_target_percent(),
            prompt_cache_min_tool_output_chars: default_prompt_cache_min_tool_output_chars(),
            prompt_cache_head_chars: default_prompt_cache_head_chars(),
            prompt_cache_tail_chars: default_prompt_cache_tail_chars(),
            prompt_cache_recent_user_turns: default_prompt_cache_recent_user_turns(),
            prompt_cache_recent_tool_chains: default_prompt_cache_recent_tool_chains(),
        }
    }

    /// Create a new token budget with explicit safety margin.
    pub fn with_safety_margin(
        max_context_tokens: u32,
        max_output_tokens: u32,
        strategy: BudgetStrategy,
        safety_margin: u32,
    ) -> Self {
        Self {
            max_context_tokens,
            max_output_tokens,
            strategy,
            safety_margin,
            compression_trigger_percent: default_compression_trigger_percent(),
            compression_target_percent: default_compression_target_percent(),
            prompt_cache_min_tool_output_chars: default_prompt_cache_min_tool_output_chars(),
            prompt_cache_head_chars: default_prompt_cache_head_chars(),
            prompt_cache_tail_chars: default_prompt_cache_tail_chars(),
            prompt_cache_recent_user_turns: default_prompt_cache_recent_user_turns(),
            prompt_cache_recent_tool_chains: default_prompt_cache_recent_tool_chains(),
        }
    }

    /// Calculate proactive compression limit for context window tokens.
    ///
    /// Returns a value <= `max_context_tokens`.
    /// `compression_trigger_percent = 100` disables proactive compression (hard-limit only).
    pub fn compression_trigger_context_tokens(&self) -> u32 {
        let context_window = self.max_context_tokens;
        if context_window == 0 {
            return 0;
        }

        let percent = normalize_trigger_percent(self.compression_trigger_percent);
        let trigger = context_window.saturating_mul(percent).saturating_div(100);
        trigger.clamp(1, context_window)
    }

    /// Calculate compression target limit for context window tokens.
    ///
    /// This value is used only once proactive compression is triggered and is
    /// guaranteed to be strictly below the trigger limit (when possible).
    pub fn compression_target_context_tokens(&self) -> u32 {
        let context_window = self.max_context_tokens;
        if context_window == 0 {
            return 0;
        }

        let trigger = self.compression_trigger_context_tokens();
        let percent = normalize_target_percent(self.compression_target_percent);
        let mut target = context_window
            .saturating_mul(percent)
            .saturating_div(100)
            .clamp(1, context_window);

        if target >= trigger {
            target = if trigger > 1 { trigger - 1 } else { 1 };
        }

        target.clamp(1, context_window)
    }

    /// Create a default budget for a model with the given context window.
    pub fn for_model(max_context_tokens: u32) -> Self {
        // Reserve ~25% for output by default, but clamp to reasonable maximum
        let max_output_tokens = calculate_max_output_tokens(max_context_tokens);
        Self::new(
            max_context_tokens,
            max_output_tokens,
            BudgetStrategy::default(),
        )
    }
}

/// Calculate safety margin as a percentage of context window.
/// Returns a value between MIN_SAFETY_MARGIN and MAX_SAFETY_MARGIN.
fn calculate_safety_margin(max_context_tokens: u32) -> u32 {
    let margin = (max_context_tokens as f64 * DEFAULT_SAFETY_MARGIN_PERCENT) as u32;
    margin.clamp(MIN_SAFETY_MARGIN, MAX_SAFETY_MARGIN)
}

fn normalize_trigger_percent(value: u8) -> u32 {
    match value {
        0 => 100, // Treat 0 as disabled rather than "always compress".
        1..=100 => value as u32,
        _ => 100,
    }
}

fn normalize_target_percent(value: u8) -> u32 {
    let raw = match value {
        0 => DEFAULT_COMPRESSION_TARGET_PERCENT as u32,
        1..=100 => value as u32,
        _ => DEFAULT_COMPRESSION_TARGET_PERCENT as u32,
    };
    raw.clamp(
        MIN_COMPRESSION_TARGET_PERCENT,
        MAX_COMPRESSION_TARGET_PERCENT,
    )
}

/// Calculate max output tokens, reserving ~25% but clamping to reasonable limits.
fn calculate_max_output_tokens(max_context_tokens: u32) -> u32 {
    // Reserve 25% for output, but cap at 16k (most providers limit output)
    const MAX_OUTPUT_CAP: u32 = 16_384;
    let output_tokens = (max_context_tokens as f64 * 0.25) as u32;
    output_tokens.min(MAX_OUTPUT_CAP)
}

impl Default for TokenBudget {
    fn default() -> Self {
        // Default to GPT-4o-mini context window (128k)
        Self::for_model(128_000)
    }
}

/// Strategy for managing token budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetStrategy {
    /// Simple window: keep the N most recent messages
    Window {
        /// Number of recent messages to keep
        size: usize,
    },
    /// Hybrid: keep recent window + optional summarization
    Hybrid {
        /// Number of recent message segments to keep
        window_size: usize,
        /// Whether to enable conversation summarization
        enable_summarization: bool,
    },
}

impl Default for BudgetStrategy {
    fn default() -> Self {
        Self::Hybrid {
            window_size: 20, // ~10-15 turns
            enable_summarization: true,
        }
    }
}

/// Result of context preparation with budget enforcement.
#[derive(Debug, Clone)]
pub struct PreparedContext {
    /// Messages prepared for LLM (may be truncated)
    pub messages: Vec<crate::agent::core::agent::types::Message>,
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

/// Detailed token usage breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageBreakdown {
    /// Tokens used by system message(s)
    pub system_tokens: u32,
    /// Tokens used by conversation summary (if any)
    pub summary_tokens: u32,
    /// Tokens used by recent message window
    pub window_tokens: u32,
    /// Total tokens in prepared context
    pub total_tokens: u32,
    /// Context window limit used as the denominator for usage percentages.
    pub budget_limit: u32,
}

impl TokenUsageBreakdown {
    /// Calculate percentage of budget used.
    pub fn usage_percentage(&self) -> f64 {
        if self.budget_limit == 0 {
            return 0.0;
        }
        (self.total_tokens as f64 / self.budget_limit as f64) * 100.0
    }
}

/// Errors that can occur during budget management.
#[derive(Debug, Error)]
pub enum BudgetError {
    /// System prompt exceeds budget
    #[error("System prompt ({system_tokens} tokens) exceeds available budget ({available_tokens} tokens)")]
    SystemPromptTooLarge {
        system_tokens: u32,
        available_tokens: u32,
    },

    /// Single message exceeds budget
    #[error("Single message ({message_tokens} tokens) exceeds available budget ({available_tokens} tokens). Consider splitting the message or attaching as a file.")]
    SingleMessageTooLarge {
        message_tokens: u32,
        available_tokens: u32,
    },

    /// Token counting error
    #[error("Failed to count tokens: {0}")]
    TokenCountError(String),

    /// Message segmentation error
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
        // context window = 1000, clamped target 20% => 200
        budget.compression_target_percent = 10;
        assert_eq!(budget.compression_target_context_tokens(), 200);
    }

    #[test]
    fn compression_target_always_stays_below_trigger_limit() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        // context window = 1000, trigger 30% => 300, target 50% would be 500,
        // so it should be clamped below trigger.
        budget.compression_trigger_percent = 30;
        budget.compression_target_percent = 50;
        assert_eq!(budget.compression_target_context_tokens(), 299);
    }

    #[test]
    fn trigger_percent_zero_means_disabled() {
        let mut budget =
            TokenBudget::with_safety_margin(1000, 200, BudgetStrategy::Window { size: 20 }, 100);
        budget.compression_trigger_percent = 0;
        assert_eq!(
            budget.compression_trigger_context_tokens(),
            budget.max_context_tokens
        );
    }
}
