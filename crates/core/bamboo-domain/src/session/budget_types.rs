//! Budget types needed by the session domain.

use serde::{Deserialize, Serialize};

/// Token budget configuration for a conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Combined with the request input ceiling and dynamically reserved working space.
    #[serde(default = "default_compression_trigger_percent")]
    pub compression_trigger_percent: u8,
    /// Compression target threshold as a percentage of context window tokens.
    #[serde(default = "default_compression_target_percent")]
    pub compression_target_percent: u8,
    /// Optional legacy reserve measured from the total context window.
    ///
    /// A non-zero value can tighten the dynamically calculated trigger, but
    /// never postpone a configured percentage or consume reserved output space.
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
    80
}

fn default_compression_target_percent() -> u8 {
    25
}

fn default_working_reserve_tokens() -> u32 {
    0
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

/// Effective compression range for one resolved model budget.
///
/// This is derived runtime data, not a second persisted policy. Targets retain
/// a whole context-window percentage so retrieval receipts keep their existing
/// exact arithmetic. A zero trigger/target means no positive range is feasible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionThresholds {
    pub input_limit: u32,
    pub headroom: u32,
    pub trigger_tokens: u32,
    pub target_tokens: u32,
    pub target_usage_percent: u8,
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

    /// Maximum estimated input tokens that may be sent in one model request.
    ///
    /// `max_context_tokens` is the provider's total input + output window, so
    /// prompt preparation must reserve both the configured output allowance and
    /// the tokenizer-estimation safety margin before selecting input messages.
    pub fn max_request_input_tokens(&self) -> u32 {
        self.max_context_tokens
            .saturating_sub(self.max_output_tokens)
            .saturating_sub(self.safety_margin)
    }

    /// Resolve one range shared by summarization, archival and prompt fitting.
    ///
    /// Besides output and tokenizer reserves already removed from the input
    /// ceiling, leave up to another fifth of that input ceiling for work between
    /// checks. The reserve scales with the actual output/safety allowances;
    /// fixed legacy settings may only move the trigger earlier.
    pub fn compression_thresholds(&self) -> CompressionThresholds {
        let window = self.max_context_tokens;
        let input_limit = self.max_request_input_tokens();
        let headroom = self
            .max_output_tokens
            .max(self.safety_margin)
            .max(1)
            .min((input_limit / 5).max(1));
        let unavailable = CompressionThresholds {
            input_limit,
            headroom,
            ..CompressionThresholds::default()
        };
        if window == 0 || input_limit <= 1 {
            return unavailable;
        }
        let percentage_tokens =
            |percent: u32| ((u64::from(window) * u64::from(percent)) / 100) as u32;
        let mut trigger =
            percentage_tokens(normalize_trigger_percent(self.compression_trigger_percent))
                .min(input_limit.saturating_sub(headroom));
        if self.working_reserve_tokens > 0 {
            let legacy_trigger = if window >= self.working_reserve_tokens.saturating_mul(2) {
                window.saturating_sub(self.working_reserve_tokens)
            } else {
                percentage_tokens(normalize_trigger_percent(self.fallback_trigger_percent))
            };
            trigger = trigger.min(legacy_trigger);
        }
        if trigger <= 1 {
            return unavailable;
        }
        let largest_target_percent = (u64::from(trigger - 1) * 100 / u64::from(window)) as u32;
        let target_usage_percent = normalize_target_percent(self.compression_target_percent)
            .min(largest_target_percent) as u8;
        let target_tokens = percentage_tokens(u32::from(target_usage_percent));
        if target_usage_percent == 0 || target_tokens == 0 {
            return unavailable;
        }
        CompressionThresholds {
            input_limit,
            headroom,
            trigger_tokens: trigger,
            target_tokens,
            target_usage_percent,
        }
    }

    pub fn compression_trigger_context_tokens(&self) -> u32 {
        self.compression_thresholds().trigger_tokens
    }

    pub fn compression_target_context_tokens(&self) -> u32 {
        self.compression_thresholds().target_tokens
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
        0 => 25u32,
        1..=100 => value as u32,
        _ => 25u32,
    };
    raw.clamp(1, 99)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Provider-reported prompt usage for one completed LLM call.
///
/// `input_tokens` is the normalized fresh (non-cached) input. Together with
/// cache reads and cache creation it forms the disjoint prompt total used for
/// an exact prefix-cache hit rate.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderPromptUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    /// True when a prompt-preparation/compression snapshot is carrying the
    /// previous completed call forward while the next provider result is not
    /// available yet.
    #[serde(default)]
    pub retained_from_previous_call: bool,
}

impl ProviderPromptUsage {
    pub fn total_input_tokens(&self) -> u64 {
        self.input_tokens
            .saturating_add(self.cache_creation_input_tokens)
            .saturating_add(self.cache_read_input_tokens)
    }

    pub fn cache_hit_rate(&self) -> Option<f64> {
        let total = self.total_input_tokens();
        (total > 0).then(|| self.cache_read_input_tokens as f64 / total as f64)
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
    /// Exact provider-side prompt/cache split for the latest completed call.
    /// `None` means this snapshot was prepared before the provider completed,
    /// or the provider did not report usable prompt usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_prompt_usage: Option<ProviderPromptUsage>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_compression_range_reserves_real_output_and_scales_with_window() {
        for (window, output, safety, trigger, target) in [
            (1_000_000, 32_000, 10_000, 800_000, 250_000),
            (200_000, 32_000, 2_000, 134_000, 50_000),
            (32_000, 8_000, 1_000, 18_400, 8_000),
        ] {
            let budget =
                TokenBudget::with_safety_margin(window, output, BudgetStrategy::default(), safety);
            assert_eq!(budget.compression_trigger_context_tokens(), trigger);
            assert_eq!(budget.compression_target_context_tokens(), target);
            assert!(target < trigger);
            assert!(trigger < budget.max_request_input_tokens());
        }
    }

    #[test]
    fn dynamic_compression_range_never_promotes_a_tight_explicit_trigger() {
        let mut budget =
            TokenBudget::with_safety_margin(1_000_000, 32_000, BudgetStrategy::default(), 10_000);
        budget.compression_trigger_percent = 35;
        budget.compression_target_percent = 10;
        budget.working_reserve_tokens = 50_000;
        assert_eq!(budget.compression_trigger_context_tokens(), 350_000);
        assert_eq!(budget.compression_target_context_tokens(), 100_000);
    }

    #[test]
    fn dynamic_compression_range_retains_a_feasible_higher_explicit_target() {
        let mut budget =
            TokenBudget::with_safety_margin(1_000_000, 32_000, BudgetStrategy::default(), 10_000);
        budget.compression_trigger_percent = 90;
        budget.compression_target_percent = 75;
        let thresholds = budget.compression_thresholds();
        assert_eq!(thresholds.trigger_tokens, 900_000);
        assert_eq!(thresholds.target_tokens, 750_000);
        assert_eq!(thresholds.target_usage_percent, 75);
    }

    #[test]
    fn dynamic_compression_range_marks_an_unrepresentable_input_ceiling_unavailable() {
        let budget = TokenBudget::with_safety_margin(1_000, 990, BudgetStrategy::default(), 0);
        let thresholds = budget.compression_thresholds();
        assert_eq!(thresholds.input_limit, 10);
        assert_eq!(thresholds.trigger_tokens, 0);
        assert_eq!(thresholds.target_tokens, 0);
        assert_eq!(thresholds.target_usage_percent, 0);
    }

    #[test]
    fn dynamic_compression_range_handles_the_full_u32_window_without_overflow() {
        let budget =
            TokenBudget::with_safety_margin(u32::MAX, u32::MAX / 2, BudgetStrategy::default(), 1);
        let thresholds = budget.compression_thresholds();
        assert!(thresholds.target_tokens > 1_000_000_000);
        assert!(thresholds.target_tokens < thresholds.trigger_tokens);
        assert!(thresholds.trigger_tokens < thresholds.input_limit);
    }

    #[test]
    fn fixed_reserve_trigger_exact_boundary() {
        // An explicit reserve can tighten the percentage-based dynamic trigger.
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
        // The legacy fallback percentage can only tighten the dynamic ceiling.
        let mut budget = TokenBudget::for_model(99_000);
        budget.working_reserve_tokens = 50_000;
        budget.fallback_trigger_percent = 75;
        // Real output headroom is tighter than either legacy percentage.
        assert_eq!(budget.compression_trigger_context_tokens(), 65_301);
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
        // Output and safety headroom cap the configured 85% trigger.
        assert_eq!(budget.compression_trigger_context_tokens(), 165_232);
    }

    #[test]
    fn legacy_reserve_cannot_postpone_the_dynamic_range() {
        // A legacy 50K reserve cannot postpone the common 80% trigger.
        let mut budget = TokenBudget::for_model(1_000_000);
        budget.working_reserve_tokens = 50_000;
        assert_eq!(budget.compression_trigger_context_tokens(), 800_000);
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
    fn request_input_limit_reserves_output_and_safety_margin() {
        let budget =
            TokenBudget::with_safety_margin(100_000, 30_000, BudgetStrategy::default(), 2_000);

        assert_eq!(budget.max_request_input_tokens(), 68_000);
    }

    #[test]
    fn request_input_limit_saturates_when_reserves_exceed_context() {
        let budget =
            TokenBudget::with_safety_margin(10_000, 9_500, BudgetStrategy::default(), 1_000);

        assert_eq!(budget.max_request_input_tokens(), 0);
        assert_eq!(budget.compression_trigger_context_tokens(), 0);
        assert_eq!(budget.compression_target_context_tokens(), 0);
    }

    #[test]
    fn compression_trigger_never_consumes_reserved_output_capacity() {
        let mut budget =
            TokenBudget::with_safety_margin(200_000, 80_000, BudgetStrategy::default(), 5_000);
        budget.working_reserve_tokens = 50_000;

        assert_eq!(budget.max_request_input_tokens(), 115_000);
        assert_eq!(budget.compression_trigger_context_tokens(), 92_000);
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
        assert_eq!(budget.working_reserve_tokens, 0);
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
        assert_eq!(normalize_target_percent(10), 10); // explicit lower target
        assert_eq!(normalize_target_percent(40), 40); // in range
        assert_eq!(normalize_target_percent(80), 80); // explicit higher target
    }

    #[test]
    fn budget_default_uses_128k() {
        let budget = TokenBudget::default();
        assert_eq!(budget.max_context_tokens, 128_000);
    }
}
