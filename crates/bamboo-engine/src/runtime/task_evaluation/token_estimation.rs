use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::Message;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};

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
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_agent_core::Message;

    fn make_tool_call(name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: "call-123".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    #[test]
    fn test_estimate_prompt_tokens_empty() {
        let messages: Vec<Message> = vec![];
        let tokens = estimate_prompt_tokens(&messages);
        assert_eq!(tokens, 0);
    }

    #[test]
    fn test_estimate_prompt_tokens_single_message() {
        let messages = vec![Message::user("Hello, world!")];
        let tokens = estimate_prompt_tokens(&messages);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_prompt_tokens_multiple_messages() {
        let messages = vec![
            Message::user("Hello"),
            Message::assistant("Hi there", None),
            Message::user("How are you?"),
        ];
        let tokens = estimate_prompt_tokens(&messages);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_completion_tokens_empty() {
        let tokens = estimate_completion_tokens("", &[]);
        assert_eq!(tokens, 0);
    }

    #[test]
    fn test_estimate_completion_tokens_text_only() {
        let tokens = estimate_completion_tokens("Hello, world!", &[]);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_completion_tokens_tool_calls_only() {
        let tool_calls = vec![make_tool_call("read_file", r#"{"path":"/test.txt"}"#)];
        let tokens = estimate_completion_tokens("", &tool_calls);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_completion_tokens_mixed() {
        let tool_calls = vec![make_tool_call("bash", r#"{"command":"ls"}"#)];
        let tokens = estimate_completion_tokens("Running command", &tool_calls);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_completion_tokens_multiple_tools() {
        let tool_calls = vec![
            make_tool_call("read_file", r#"{"path":"a.txt"}"#),
            make_tool_call("bash", r#"{"command":"pwd"}"#),
        ];
        let tokens = estimate_completion_tokens("Processing", &tool_calls);
        assert!(tokens > 0);
    }

    #[test]
    fn test_estimate_completion_tokens_large_content() {
        let large_content = "word ".repeat(1000);
        let tokens = estimate_completion_tokens(&large_content, &[]);
        assert!(tokens > 100);
    }

    #[test]
    fn test_estimate_completion_tokens_complex_args() {
        let complex_args = serde_json::json!({
            "nested": {"array": [1, 2, 3], "string": "test"},
            "number": 42
        })
        .to_string();
        let tool_calls = vec![make_tool_call("complex_tool", &complex_args)];
        let tokens = estimate_completion_tokens("", &tool_calls);
        assert!(tokens > 0);
    }
}
