//! Core types for token budget management.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default safety margin as a percentage of context window (1%).
const DEFAULT_SAFETY_MARGIN_PERCENT: f64 = 0.01;
/// Minimum safety margin in tokens.
const MIN_SAFETY_MARGIN: u32 = 100;
/// Maximum safety margin in tokens.
const MAX_SAFETY_MARGIN: u32 = 2000;

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
}

fn default_safety_margin() -> u32 {
    // Default for deserialization when field is missing
    1000
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
        }
    }

    /// Calculate available tokens for input (context window minus output reserve and safety margin).
    pub fn available_input_tokens(&self) -> u32 {
        self.max_context_tokens
            .saturating_sub(self.max_output_tokens)
            .saturating_sub(self.safety_margin)
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
    /// Budget limit for input tokens
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
