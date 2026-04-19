use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::Message;

pub(super) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_messages(messages))
}

pub(super) fn estimate_completion_tokens(content: &str, tool_calls: &[ToolCall]) -> u64 {
    let counter = TiktokenTokenCounter::default();
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

#[cfg(test)]
mod tests {
    use super::{estimate_completion_tokens, estimate_prompt_tokens};
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_agent_core::Message;

    #[test]
    fn estimate_prompt_tokens_counts_messages() {
        let messages = vec![
            Message::system("You are an assistant."),
            Message::user("Explain this code."),
        ];
        let tokens = estimate_prompt_tokens(&messages);
        assert!(tokens > 0);
    }

    #[test]
    fn estimate_completion_tokens_includes_tool_call_payload() {
        let call = ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: r#"{"file_path":"/tmp/a.rs"}"#.to_string(),
            },
        };

        let content_only = estimate_completion_tokens("done", &[]);
        let with_tool = estimate_completion_tokens("done", &[call]);

        assert!(with_tool >= content_only);
    }
}
