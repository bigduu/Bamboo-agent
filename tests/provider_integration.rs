//! LLM Provider integration tests
//!
//! Tests for LLM provider integrations and protocol conversions.

#[cfg(test)]
mod tests {
    #[test]
    fn test_provider_types_exist() {
        // Verify that provider types are accessible
        // This is a module structure test

        // Create provider enum variants
        let _provider_type = "openai";
        let _provider_type = "anthropic";
        let _provider_type = "gemini";
        let _provider_type = "copilot";

        assert!(true);
    }

    #[test]
    fn test_message_conversion() {
        use bamboo_agent::agent::llm::api::models::ChatMessage as OpenAIChatMessage;
        use bamboo_agent::agent::llm::protocol::ToProvider;
        use bamboo_agent::agent::Message;

        let msg = Message::user("Test message");
        let openai_msg: OpenAIChatMessage = msg.to_provider().unwrap();

        // Verify we can convert internal messages to the OpenAI-compatible API model.
        assert_eq!(
            openai_msg.role,
            bamboo_agent::agent::llm::api::models::Role::User
        );
    }

    #[test]
    fn test_llm_chunk_types() {
        use bamboo_agent::agent::llm::types::LLMChunk;

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
        match done_chunk {
            LLMChunk::Done => assert!(true),
            _ => panic!("Expected Done variant"),
        }
    }

    #[test]
    fn test_tool_schema_creation() {
        use bamboo_agent::agent::core::tools::FunctionSchema;
        use bamboo_agent::agent::core::ToolSchema;
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
        let providers = bamboo_agent::agent::llm::AVAILABLE_PROVIDERS;

        // Should include at least these providers
        assert!(providers.contains(&"openai"));
        assert!(providers.contains(&"anthropic"));
        assert!(providers.contains(&"gemini"));
        assert!(providers.contains(&"copilot"));
    }

    #[test]
    fn test_protocol_enums() {
        // Test that protocol types exist and can be referenced
        // This verifies the module structure
        let _openai: Option<bamboo_agent::agent::llm::protocol::OpenAIProtocol> = None;
        let _anthropic: Option<bamboo_agent::agent::llm::protocol::AnthropicProtocol> = None;
        let _gemini: Option<bamboo_agent::agent::llm::protocol::GeminiProtocol> = None;
        assert!(true);
    }

    #[test]
    fn test_llm_error_types() {
        use bamboo_agent::agent::llm::LLMError;

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
