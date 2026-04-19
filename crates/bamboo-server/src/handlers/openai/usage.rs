use bamboo_agent_core::Message;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use bamboo_engine::metrics::types::TokenUsage as MetricsTokenUsage;

pub(super) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_messages(messages))
}

pub(super) fn estimate_text_tokens(text: &str) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_text(text))
}

pub(super) fn estimate_completion_tokens(output_text: &str) -> u64 {
    let counter = TiktokenTokenCounter::default();
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
