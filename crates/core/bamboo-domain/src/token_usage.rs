//! Shared token usage value object used across domain, application, and infrastructure layers.

use serde::{Deserialize, Serialize};

/// Largest token counter that can be represented losslessly by the signed
/// 64-bit integer columns used by durable metrics stores.
///
/// Keeping this boundary beside the shared value object lets runtime budgets
/// and persistence apply one policy instead of allowing `u64 as i64` wraparound
/// or storage-specific divergence.
pub const MAX_DURABLE_TOKEN_COUNT: u64 = i64::MAX as u64;

/// Token consumption statistics for a single LLM call or aggregated period.
///
/// This is a stable, cross-layer value object. Every crate that needs to
/// represent "how many tokens were used" should use this type (or re-export it)
/// instead of defining a local duplicate.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    /// Tokens in the LLM request (prompt / input).
    pub prompt_tokens: u64,
    /// Tokens in the LLM response (completion / output).
    pub completion_tokens: u64,
    /// Total tokens consumed (normally prompt + completion).
    pub total_tokens: u64,
}

impl TokenUsage {
    /// Accumulate another usage snapshot into this one.
    pub fn add_assign(&mut self, other: TokenUsage) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(other.prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(other.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
    }

    /// Recompute `total_tokens` from the two component fields.
    pub fn recompute_total(&mut self) {
        self.total_tokens = self.prompt_tokens.saturating_add(self.completion_tokens);
    }

    /// Normalize all counters to the lossless signed-64 durable range.
    ///
    /// Components saturate independently and the total is recomputed from the
    /// saturated components, then saturated to the same boundary. This makes
    /// the policy deterministic even if an upstream supplied an inconsistent
    /// total and keeps round, session, and runtime views reconcilable.
    pub fn clamped_for_durable_metrics(mut self) -> Self {
        self.prompt_tokens = self.prompt_tokens.min(MAX_DURABLE_TOKEN_COUNT);
        self.completion_tokens = self.completion_tokens.min(MAX_DURABLE_TOKEN_COUNT);
        self.total_tokens = self
            .prompt_tokens
            .saturating_add(self.completion_tokens)
            .min(MAX_DURABLE_TOKEN_COUNT);
        self
    }

    /// Accumulate usage with the same saturation policy used by durable stores.
    pub fn add_assign_durable(&mut self, other: TokenUsage) {
        self.prompt_tokens = self
            .prompt_tokens
            .saturating_add(other.prompt_tokens)
            .min(MAX_DURABLE_TOKEN_COUNT);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(other.completion_tokens)
            .min(MAX_DURABLE_TOKEN_COUNT);
        self.total_tokens = self
            .prompt_tokens
            .saturating_add(self.completion_tokens)
            .min(MAX_DURABLE_TOKEN_COUNT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero() {
        let usage = TokenUsage::default();
        assert_eq!(usage.prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 0);
        assert_eq!(usage.total_tokens, 0);
    }

    #[test]
    fn add_assign_accumulates() {
        let mut usage1 = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };
        let usage2 = TokenUsage {
            prompt_tokens: 200,
            completion_tokens: 100,
            total_tokens: 300,
        };
        usage1.add_assign(usage2);
        assert_eq!(usage1.prompt_tokens, 300);
        assert_eq!(usage1.completion_tokens, 150);
        assert_eq!(usage1.total_tokens, 450);
    }

    #[test]
    fn recompute_total_uses_saturating_add() {
        let mut usage = TokenUsage {
            prompt_tokens: u64::MAX - 5,
            completion_tokens: u64::MAX - 9,
            total_tokens: 0,
        };
        usage.recompute_total();
        assert_eq!(usage.total_tokens, u64::MAX);
    }

    #[test]
    fn durable_metrics_policy_clamps_components_and_recomputes_total() {
        let usage = TokenUsage {
            prompt_tokens: u64::MAX,
            completion_tokens: u64::MAX,
            total_tokens: 1,
        }
        .clamped_for_durable_metrics();

        assert_eq!(usage.prompt_tokens, MAX_DURABLE_TOKEN_COUNT);
        assert_eq!(usage.completion_tokens, MAX_DURABLE_TOKEN_COUNT);
        assert_eq!(usage.total_tokens, MAX_DURABLE_TOKEN_COUNT);
    }

    #[test]
    fn durable_accumulation_saturates_without_wraparound() {
        let mut usage = TokenUsage {
            prompt_tokens: MAX_DURABLE_TOKEN_COUNT - 2,
            completion_tokens: MAX_DURABLE_TOKEN_COUNT - 3,
            total_tokens: MAX_DURABLE_TOKEN_COUNT,
        };
        usage.add_assign_durable(TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
        });

        assert_eq!(usage.prompt_tokens, MAX_DURABLE_TOKEN_COUNT);
        assert_eq!(usage.completion_tokens, MAX_DURABLE_TOKEN_COUNT);
        assert_eq!(usage.total_tokens, MAX_DURABLE_TOKEN_COUNT);
    }
}
