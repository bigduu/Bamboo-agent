use crate::agent::core::budget::{HeuristicTokenCounter, TokenCounter};
use crate::agent::core::tools::{ToolCall, ToolSchema};
use crate::agent::core::Message;

pub(super) fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = HeuristicTokenCounter::with_defaults();
    u64::from(counter.count_messages(messages))
}

/// Estimate tokens consumed by tool schemas (function definitions sent to the LLM).
///
/// Each tool schema is serialized to JSON and counted. This estimate is used to
/// reserve budget space before context preparation, ensuring messages are properly
/// compressed even when many tools are registered.
pub(super) fn estimate_tool_schemas_tokens(tool_schemas: &[ToolSchema]) -> u32 {
    if tool_schemas.is_empty() {
        return 0;
    }

    let counter = HeuristicTokenCounter::with_defaults();
    let mut total: u32 = 0;

    for schema in tool_schemas {
        // Estimate: name + description + JSON-serialized parameters
        let name_tokens = counter.count_text(&schema.function.name);
        let desc_tokens = counter.count_text(&schema.function.description);
        let params_json = serde_json::to_string(&schema.function.parameters).unwrap_or_default();
        let params_tokens = counter.count_text(&params_json);

        // Per-tool overhead for schema structure (type, function wrapper, etc.)
        const PER_TOOL_OVERHEAD: u32 = 10;

        total = total.saturating_add(
            name_tokens
                .saturating_add(desc_tokens)
                .saturating_add(params_tokens)
                .saturating_add(PER_TOOL_OVERHEAD),
        );
    }

    // Add global overhead for the tools array wrapper
    const GLOBAL_TOOLS_OVERHEAD: u32 = 20;
    total.saturating_add(GLOBAL_TOOLS_OVERHEAD)
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

#[cfg(test)]
mod tests {
    use super::{estimate_completion_tokens, estimate_prompt_tokens, estimate_tool_schemas_tokens};
    use crate::agent::core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
    use crate::agent::core::Message;

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

    fn make_tool_schema(name: &str, description: &str) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: name.to_string(),
                description: description.to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path"}
                    },
                    "required": ["path"]
                }),
            },
        }
    }

    #[test]
    fn estimate_tool_schemas_tokens_returns_zero_for_empty() {
        assert_eq!(estimate_tool_schemas_tokens(&[]), 0);
    }

    #[test]
    fn estimate_tool_schemas_tokens_counts_single_tool() {
        let schemas = vec![make_tool_schema("Read", "Read file contents")];
        let tokens = estimate_tool_schemas_tokens(&schemas);
        // Should be > 0 and reasonable (name + description + parameters JSON + overhead)
        assert!(
            tokens > 20,
            "Expected > 20 tokens for a single tool, got {}",
            tokens
        );
        assert!(
            tokens < 500,
            "Expected < 500 tokens for a single tool, got {}",
            tokens
        );
    }

    #[test]
    fn estimate_tool_schemas_tokens_scales_with_tool_count() {
        let one = vec![make_tool_schema("Read", "Read file contents")];
        let many: Vec<_> = (0..10)
            .map(|i| {
                make_tool_schema(
                    &format!("Tool{}", i),
                    &format!("Description for tool {}", i),
                )
            })
            .collect();

        let one_tokens = estimate_tool_schemas_tokens(&one);
        let many_tokens = estimate_tool_schemas_tokens(&many);

        assert!(
            many_tokens > one_tokens * 5,
            "10 tools ({}) should use significantly more tokens than 1 tool ({})",
            many_tokens,
            one_tokens
        );
    }
}
