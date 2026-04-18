use bamboo_application_memory::budget::{HeuristicTokenCounter, TokenCounter};
use bamboo_application_agent::Message;
use bamboo_application_metrics::types::TokenUsage as MetricsTokenUsage;

pub(super) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_messages(messages))
}

pub(super) fn estimate_completion_tokens(output_text: &str) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_text(output_text))
}

pub(super) fn build_estimated_usage(
    prompt_tokens: u64,
    completion_tokens: u64,
) -> MetricsTokenUsage {
    MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    }
}
