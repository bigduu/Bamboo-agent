use crate::agent::core::budget::{HeuristicTokenCounter, TokenCounter};
use crate::agent::core::tools::ToolCall;
use crate::agent::core::Message;

pub(super) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_messages(messages))
}

pub(super) fn estimate_completion_tokens(content: &str, tool_calls: &[ToolCall]) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    let mut completion_surface = content.to_string();

    for call in tool_calls {
        if !completion_surface.is_empty() {
            completion_surface.push('\n');
        }
        completion_surface.push_str(&call.function.name);
        completion_surface.push('\n');
        completion_surface.push_str(&call.function.arguments);
    }

    u64::from(counter.count_text(&completion_surface))
}
