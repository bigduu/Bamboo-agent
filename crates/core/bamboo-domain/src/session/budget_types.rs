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
    ///
    /// Legacy field — only used when `working_reserve_tokens == 0`.
    #[serde(default = "default_compression_trigger_percent")]
    pub compression_trigger_percent: u8,
    /// Compression target threshold as a percentage of context window tokens.
    #[serde(default = "default_compression_target_percent")]
    pub compression_target_percent: u8,
    /// Fixed number of tokens reserved for model reasoning and output.
    ///
    /// Compression triggers when `(max_context_tokens - working_reserve_tokens)` is
    /// exceeded. This provides consistent reasoning space regardless of model context
    /// window size (Claude Code uses ~50K).
    ///
    /// When `working_reserve_tokens == 0`, falls back to percentage-based triggering
    /// via `compression_trigger_percent` for backward compatibility.
    #[serde(default = "default_working_reserve_tokens")]
    pub working_reserve_tokens: u32,
    /// Fallback percentage trigger when the context window is too small for the fixed
    /// reserve (i.e. `max_context_tokens < working_reserve_tokens * 2`).
    #[serde(default = "default_fallback_trigger_percent")]
    pub fallback_trigger_percent: u8,
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
    /// Maximum tokens allowed per single tool output. 0 = use byte-based fallback only.
    #[serde(default)]
    pub max_tool_output_tokens: u32,
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

fn default_working_reserve_tokens() -> u32 {
    50_000
}

fn default_fallback_trigger_percent() -> u8 {
    75
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
            working_reserve_tokens: default_working_reserve_tokens(),
            fallback_trigger_percent: default_fallback_trigger_percent(),
            prompt_cache_min_tool_output_chars: default_prompt_cache_min_tool_output_chars(),
            prompt_cache_head_chars: default_prompt_cache_head_chars(),
            prompt_cache_tail_chars: default_prompt_cache_tail_chars(),
            prompt_cache_recent_user_turns: default_prompt_cache_recent_user_turns(),
            prompt_cache_recent_tool_chains: default_prompt_cache_recent_tool_chains(),
            max_tool_output_tokens: 0,
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
            working_reserve_tokens: default_working_reserve_tokens(),
            fallback_trigger_percent: default_fallback_trigger_percent(),
            prompt_cache_min_tool_output_chars: default_prompt_cache_min_tool_output_chars(),
            prompt_cache_head_chars: default_prompt_cache_head_chars(),
            prompt_cache_tail_chars: default_prompt_cache_tail_chars(),
            prompt_cache_recent_user_turns: default_prompt_cache_recent_user_turns(),
            prompt_cache_recent_tool_chains: default_prompt_cache_recent_tool_chains(),
            max_tool_output_tokens: 0,
        }
    }

    pub fn compression_trigger_context_tokens(&self) -> u32 {
        let context_window = self.max_context_tokens;
        if context_window == 0 {
            return 0;
        }

        // Primary: fixed reserve mode — trigger when (context - reserve) is exceeded.
        if self.working_reserve_tokens > 0 && context_window >= self.working_reserve_tokens * 2 {
            return context_window.saturating_sub(self.working_reserve_tokens);
        }

        // Fallback 1: small context window — use fallback_trigger_percent.
        if self.working_reserve_tokens > 0 {
            let percent = normalize_trigger_percent(self.fallback_trigger_percent);
            return context_window
                .saturating_mul(percent)
                .saturating_div(100)
                .clamp(1, context_window);
        }

        // Fallback 2: working_reserve_tokens == 0 — legacy percentage mode.
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
        Self::new(
            max_context_tokens,
            max_output_tokens,
            BudgetStrategy::default(),
        )
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
    Window {
        size: usize,
    },
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
    #[serde(default)]
    pub prompt_cached_tool_tokens_saved: u32,
    #[serde(default)]
    pub thinking_tokens: u32,
    #[serde(default)]
    pub cache_read_input_tokens: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_reserve_trigger_exact_boundary() {
        // context_window == working_reserve_tokens * 2 → should use fixed reserve
        let budget = TokenBudget {
            max_context_tokens: 100_000,
            working_reserve_tokens: 50_000,
            ..TokenBudget::for_model(100_000)
        };
        // 100K - 50K = 50K
        assert_eq!(budget.compression_trigger_context_tokens(), 50_000);
    }

    #[test]
    fn fixed_reserve_trigger_just_below_boundary() {
        // context_window < working_reserve_tokens * 2 → falls back to percentage
        let mut budget = TokenBudget::for_model(99_000);
        budget.working_reserve_tokens = 50_000;
        budget.fallback_trigger_percent = 75;
        // 99K < 100K → fallback: 99K * 75% = 74,250
        assert_eq!(budget.compression_trigger_context_tokens(), 74_250);
    }

    #[test]
    fn fixed_reserve_trigger_one_above_boundary() {
        // context_window == working_reserve_tokens * 2 + 1 → uses fixed reserve
        let mut budget = TokenBudget::for_model(100_001);
        budget.working_reserve_tokens = 50_000;
        // 100001 - 50000 = 50001
        assert_eq!(budget.compression_trigger_context_tokens(), 50_001);
    }

    #[test]
    fn zero_context_window_returns_zero_trigger() {
        let budget = TokenBudget {
            max_context_tokens: 0,
            working_reserve_tokens: 50_000,
            ..TokenBudget::for_model(1)
        };
        assert_eq!(budget.compression_trigger_context_tokens(), 0);
    }

    #[test]
    fn legacy_percentage_mode_when_reserve_is_zero() {
        let mut budget = TokenBudget::for_model(200_000);
        budget.working_reserve_tokens = 0;
        budget.compression_trigger_percent = 85;
        // 200K * 85% = 170K
        assert_eq!(budget.compression_trigger_context_tokens(), 170_000);
    }

    #[test]
    fn large_context_window_triggers_very_late_with_fixed_reserve() {
        // 1M context window, 50K reserve → trigger at 950K (95%)
        let mut budget = TokenBudget::for_model(1_000_000);
        budget.working_reserve_tokens = 50_000;
        assert_eq!(budget.compression_trigger_context_tokens(), 950_000);
    }

    #[test]
    fn compression_target_always_below_trigger() {
        let mut budget = TokenBudget::for_model(200_000);
        budget.working_reserve_tokens = 50_000;
        budget.compression_target_percent = 40;
        let trigger = budget.compression_trigger_context_tokens();
        let target = budget.compression_target_context_tokens();
        assert!(
            target < trigger,
            "target ({target}) must be < trigger ({trigger})"
        );
    }

    #[test]
    fn serde_roundtrip_preserves_fixed_reserve_fields() {
        let budget = TokenBudget {
            max_context_tokens: 200_000,
            max_output_tokens: 8192,
            strategy: BudgetStrategy::default(),
            working_reserve_tokens: 50_000,
            fallback_trigger_percent: 75,
            ..TokenBudget::for_model(200_000)
        };
        let json = serde_json::to_string(&budget).unwrap();
        let deserialized: TokenBudget = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.working_reserve_tokens, 50_000);
        assert_eq!(deserialized.fallback_trigger_percent, 75);
    }

    #[test]
    fn serde_backward_compat_deserializes_without_new_fields() {
        // Old config without working_reserve_tokens / fallback_trigger_percent
        let json = r#"{
            "max_context_tokens": 200000,
            "max_output_tokens": 8192,
            "strategy": {"hybrid": {"window_size": 20, "enable_summarization": true}},
            "safety_margin": 1000,
            "compression_trigger_percent": 85,
            "compression_target_percent": 40
        }"#;
        let budget: TokenBudget = serde_json::from_str(json).unwrap();
        // New fields should get defaults
        assert_eq!(budget.working_reserve_tokens, 50_000);
        assert_eq!(budget.fallback_trigger_percent, 75);
    }

    #[test]
    fn usage_percentage_handles_zero_budget() {
        let breakdown = TokenUsageBreakdown {
            system_tokens: 100,
            summary_tokens: 50,
            window_tokens: 200,
            total_tokens: 350,
            budget_limit: 0,
        };
        assert_eq!(breakdown.usage_percentage(), 0.0);
    }

    #[test]
    fn usage_percentage_computes_correctly() {
        let breakdown = TokenUsageBreakdown {
            system_tokens: 1000,
            summary_tokens: 500,
            window_tokens: 5000,
            total_tokens: 6500,
            budget_limit: 10_000,
        };
        let pct = breakdown.usage_percentage();
        assert!((pct - 65.0).abs() < 0.01, "expected 65.0%, got {pct}%");
    }

    #[test]
    fn safety_margin_clamped_to_min_100() {
        let margin = calculate_safety_margin(100);
        assert_eq!(margin, 100);
    }

    #[test]
    fn safety_margin_clamped_to_max_2000() {
        let margin = calculate_safety_margin(1_000_000);
        assert_eq!(margin, 2000);
    }

    #[test]
    fn normalize_trigger_percent_zero_means_hundred() {
        assert_eq!(normalize_trigger_percent(0), 100);
    }

    #[test]
    fn normalize_trigger_percent_passthrough() {
        assert_eq!(normalize_trigger_percent(85), 85);
        assert_eq!(normalize_trigger_percent(50), 50);
    }

    #[test]
    fn normalize_target_percent_clamps_to_range() {
        assert_eq!(normalize_target_percent(10), 20); // below min
        assert_eq!(normalize_target_percent(40), 40); // in range
        assert_eq!(normalize_target_percent(80), 50); // above max
    }

    #[test]
    fn budget_default_uses_128k() {
        let budget = TokenBudget::default();
        assert_eq!(budget.max_context_tokens, 128_000);
    }
}
