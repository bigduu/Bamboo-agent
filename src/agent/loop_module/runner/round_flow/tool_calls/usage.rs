use crate::agent::metrics::TokenUsage;

pub(super) fn accumulate_round_usage(round_usage: &mut TokenUsage, delta: TokenUsage) {
    round_usage.prompt_tokens = round_usage
        .prompt_tokens
        .saturating_add(delta.prompt_tokens);
    round_usage.completion_tokens = round_usage
        .completion_tokens
        .saturating_add(delta.completion_tokens);
    round_usage.total_tokens = round_usage
        .prompt_tokens
        .saturating_add(round_usage.completion_tokens);
}

#[cfg(test)]
mod tests {
    use super::accumulate_round_usage;
    use crate::agent::metrics::TokenUsage;

    #[test]
    fn accumulate_round_usage_saturates_components_and_recomputes_total() {
        let mut usage = TokenUsage {
            prompt_tokens: u64::MAX - 5,
            completion_tokens: u64::MAX - 9,
            total_tokens: 0,
        };
        let delta = TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
        };

        accumulate_round_usage(&mut usage, delta);

        assert_eq!(usage.prompt_tokens, u64::MAX);
        assert_eq!(usage.completion_tokens, u64::MAX);
        assert_eq!(usage.total_tokens, u64::MAX);
    }
}
