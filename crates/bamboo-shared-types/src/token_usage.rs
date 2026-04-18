//! Shared token usage value object used across domain, application, and infrastructure layers.

use serde::{Deserialize, Serialize};

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
        self.completion_tokens = self.completion_tokens.saturating_add(other.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
    }

    /// Recompute `total_tokens` from the two component fields.
    pub fn recompute_total(&mut self) {
        self.total_tokens = self.prompt_tokens.saturating_add(self.completion_tokens);
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
}
