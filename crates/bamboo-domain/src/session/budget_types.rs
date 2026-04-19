//! Budget types needed by the session domain.

use serde::{Deserialize, Serialize};

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
    #[serde(default = "default_compression_trigger_percent")]
    pub compression_trigger_percent: u8,
    /// Compression target threshold as a percentage of context window tokens.
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
    1000
}

fn default_compression_trigger_percent() -> u8 {
    85
}

fn default_compression_target_percent() -> u8 {
    40
}

fn default_prompt_cache_min_tool_output_chars() -> u32 {
    1_200
}

fn default_prompt_cache_head_chars() -> u32 {
    280
}

fn default_prompt_cache_tail_chars() -> u32 {
    180
}

fn default_prompt_cache_recent_user_turns() -> u8 {
    2
}

fn default_prompt_cache_recent_tool_chains() -> u8 {
    2
}

impl TokenBudget {
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

    pub fn compression_trigger_context_tokens(&self) -> u32 {
        let context_window = self.max_context_tokens;
        if context_window == 0 {
            return 0;
        }

        let percent = normalize_trigger_percent(self.compression_trigger_percent);
        let trigger = context_window.saturating_mul(percent).saturating_div(100);
        trigger.clamp(1, context_window)
    }

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

    pub fn for_model(max_context_tokens: u32) -> Self {
        let max_output_tokens = calculate_max_output_tokens(max_context_tokens);
        Self::new(max_context_tokens, max_output_tokens, BudgetStrategy::default())
    }
}

fn calculate_safety_margin(max_context_tokens: u32) -> u32 {
    let margin = (max_context_tokens as f64 * 0.01) as u32;
    margin.clamp(100, 2000)
}

fn normalize_trigger_percent(value: u8) -> u32 {
    match value {
        0 => 100,
        1..=100 => value as u32,
        _ => 100,
    }
}

fn normalize_target_percent(value: u8) -> u32 {
    let raw = match value {
        0 => 40u32,
        1..=100 => value as u32,
        _ => 40u32,
    };
    raw.clamp(20, 50)
}

fn calculate_max_output_tokens(max_context_tokens: u32) -> u32 {
    const MAX_OUTPUT_CAP: u32 = 16_384;
    let output_tokens = (max_context_tokens as f64 * 0.25) as u32;
    output_tokens.min(MAX_OUTPUT_CAP)
}

impl Default for TokenBudget {
    fn default() -> Self {
        Self::for_model(128_000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetStrategy {
    Window { size: usize },
    Hybrid {
        window_size: usize,
        enable_summarization: bool,
    },
}

impl Default for BudgetStrategy {
    fn default() -> Self {
        Self::Hybrid {
            window_size: 20,
            enable_summarization: true,
        }
    }
}

/// Detailed token usage breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageBreakdown {
    pub system_tokens: u32,
    pub summary_tokens: u32,
    pub window_tokens: u32,
    pub total_tokens: u32,
    pub budget_limit: u32,
}

impl TokenUsageBreakdown {
    pub fn usage_percentage(&self) -> f64 {
        if self.budget_limit == 0 {
            return 0.0;
        }
        (self.total_tokens as f64 / self.budget_limit as f64) * 100.0
    }
}

/// Token budget usage information sent to frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBudgetUsage {
    pub system_tokens: u32,
    pub summary_tokens: u32,
    pub window_tokens: u32,
    pub total_tokens: u32,
    #[serde(default)]
    pub max_context_tokens: u32,
    pub budget_limit: u32,
    pub truncation_occurred: bool,
    pub segments_removed: usize,
    #[serde(default)]
    pub prompt_cached_tool_outputs: usize,
}
