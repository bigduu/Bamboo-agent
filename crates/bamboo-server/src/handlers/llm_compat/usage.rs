//! Token-usage estimation shared by the LLM-compatibility surfaces
//! (anthropic / openai / gemini). Previously triplicated as a per-surface
//! `usage.rs`; unified here since the bodies were byte-identical.

use bamboo_agent_core::Message;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use bamboo_engine::metrics::types::TokenUsage as MetricsTokenUsage;

pub(crate) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_messages(messages))
}

pub(crate) fn estimate_text_tokens(text: &str) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_text(text))
}

pub(crate) fn estimate_completion_tokens(output_text: &str) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_text(output_text))
}

pub(crate) fn build_estimated_usage(
    prompt_tokens: u64,
    completion_tokens: u64,
) -> MetricsTokenUsage {
    MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    }
}
