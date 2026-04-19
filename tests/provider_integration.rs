//! LLM Provider integration tests
//!
//! Tests for LLM provider integrations and protocol conversions.

#[cfg(test)]
mod tests {
    #[test]
    fn test_provider_types_exist() {
        use bamboo_infrastructure::AVAILABLE_PROVIDERS;
        use std::collections::HashSet;

        let providers = AVAILABLE_PROVIDERS;
        let provider_set: HashSet<&str> = providers.iter().copied().collect();

        assert!(provider_set.contains("openai"));
        assert!(provider_set.contains("anthropic"));
        assert!(provider_set.contains("gemini"));
        assert!(provider_set.contains("copilot"));
        assert_eq!(provider_set.len(), providers.len());
    }

    #[test]
    fn test_message_conversion() {
        use bamboo_infrastructure::api::models::ChatMessage as OpenAIChatMessage;
        use bamboo_infrastructure::protocol::ToProvider;
        use bamboo_agent::agent::Message;

        let msg = Message::user("Test message");
        let openai_msg: OpenAIChatMessage = msg.to_provider().unwrap();

        // Verify we can convert internal messages to the OpenAI-compatible API model.
        assert_eq!(
            openai_msg.role,
            bamboo_infrastructure::api::models::Role::User
        );
    }

    #[test]
    fn test_llm_chunk_types() {
        use bamboo_infrastructure::types::LLMChunk;

        // Test token chunk
        let token_chunk = LLMChunk::Token("Hello".to_string());
        match token_chunk {
            LLMChunk::Token(text) => assert_eq!(text, "Hello"),
            _ => panic!("Expected Token variant"),
        }

        // Test tool calls chunk
        let tool_calls = vec![];
        let tool_chunk = LLMChunk::ToolCalls(tool_calls.clone());
        match tool_chunk {
            LLMChunk::ToolCalls(calls) => assert_eq!(calls.len(), 0),
            _ => panic!("Expected ToolCalls variant"),
        }

        // Test done chunk
        let done_chunk = LLMChunk::Done;
        assert!(matches!(done_chunk, LLMChunk::Done));
    }

    #[test]
    fn test_tool_schema_creation() {
        use bamboo_agent_core::tools::FunctionSchema;
        use bamboo_agent_core::tools::ToolSchema;
        use serde_json::json;

        let tool = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "read_file".to_string(),
                description: "Read a file from disk".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "File path to read"
                        }
                    },
                    "required": ["path"]
                }),
            },
        };

        // Verify tool structure
        assert_eq!(tool.function.name, "read_file");
        assert!(tool.function.parameters.is_object());

        let params = tool.function.parameters.as_object().unwrap();
        assert!(params.contains_key("properties"));
        assert!(params.contains_key("required"));
    }

    #[test]
    fn test_available_providers() {
        // Verify that AVAILABLE_PROVIDERS is accessible
        let providers = bamboo_infrastructure::AVAILABLE_PROVIDERS;

        // Should include at least these providers
        assert!(providers.contains(&"openai"));
        assert!(providers.contains(&"anthropic"));
        assert!(providers.contains(&"gemini"));
        assert!(providers.contains(&"copilot"));
    }

    #[test]
    fn test_protocol_enums() {
        let openai_type =
            std::any::type_name::<bamboo_infrastructure::protocol::OpenAIProtocol>();
        let anthropic_type =
            std::any::type_name::<bamboo_infrastructure::protocol::AnthropicProtocol>();
        let gemini_type =
            std::any::type_name::<bamboo_infrastructure::protocol::GeminiProtocol>();

        assert!(openai_type.ends_with("OpenAIProtocol"));
        assert!(anthropic_type.ends_with("AnthropicProtocol"));
        assert!(gemini_type.ends_with("GeminiProtocol"));
    }

    #[test]
    fn test_llm_error_types() {
        use bamboo_infrastructure::LLMError;

        // Test error variants - using proper construction methods
        let api_error = LLMError::Api("Rate limit exceeded".to_string());
        let auth_error = LLMError::Auth("Invalid API key".to_string());
        let stream_error = LLMError::Stream("Stream closed".to_string());

        // Verify error messages
        assert!(api_error.to_string().contains("API"));
        assert!(auth_error.to_string().contains("Authentication"));
        assert!(stream_error.to_string().contains("Stream"));
    }
}
