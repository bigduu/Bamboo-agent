#[cfg(test)]
mod tests {
    use crate::agent::core::agent::{Message, Role, Session};
    use crate::agent::core::tools::{
        FunctionCall, FunctionSchema, ToolCall, ToolResult, ToolSchema,
    };

    #[test]
    fn test_session_creation() {
        let session = Session::new("test-123", "test-model");
        assert_eq!(session.id, "test-123");
        assert!(session.messages.is_empty());
    }

    #[test]
    fn test_message_creation() {
        let msg = Message::user("Hello");
        assert_eq!(msg.content, "Hello");
        assert!(matches!(msg.role, Role::User));
        assert!(!msg.id.is_empty());
    }

    #[test]
    fn test_session_add_message() {
        let mut session = Session::new("test", "test-model");
        let msg = Message::user("Test message");
        session.add_message(msg);

        assert_eq!(session.messages.len(), 1);
        assert_eq!(session.messages[0].content, "Test message");
    }

    #[test]
    fn test_session_add_message_truncates_oversized_tool_output() {
        let mut session = Session::new("test", "test-model");
        let oversized = "x".repeat(300 * 1024);
        session.add_message(Message::tool_result("call-1", oversized.clone()));

        assert_eq!(session.messages.len(), 1);
        assert!(matches!(session.messages[0].role, Role::Tool));
        assert!(session.messages[0].content.len() < oversized.len());
        assert!(session.messages[0]
            .content
            .contains("[... tool output truncated ...]"));
    }

    #[test]
    fn test_compact_oversized_tool_messages_compacts_existing_history() {
        let mut session = Session::new("test", "test-model");
        let oversized = "y".repeat(320 * 1024);
        let mut tool = Message::tool_result("call-legacy", "");
        tool.content = oversized.clone();
        session.messages.push(tool);

        let compacted = session.compact_oversized_tool_messages();
        assert_eq!(compacted, 1);
        assert!(session.messages[0].content.len() < oversized.len());
        assert!(session.messages[0]
            .content
            .contains("[... tool output truncated ...]"));
    }

    #[test]
    fn test_tool_call_creation() {
        let tool_call = ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "test_tool".to_string(),
                arguments: r#"{"key": "value"}"#.to_string(),
            },
        };

        assert_eq!(tool_call.id, "call-1");
        assert_eq!(tool_call.function.name, "test_tool");
    }

    #[test]
    fn test_tool_result_creation() {
        let result = ToolResult {
            success: true,
            result: "Success output".to_string(),
            display_preference: Some("text".to_string()),
        };

        assert!(result.success);
        assert_eq!(result.result, "Success output");
    }

    #[test]
    fn test_tool_schema_creation() {
        let schema = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "test".to_string(),
                description: "Test tool".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {}
                }),
            },
        };

        assert_eq!(schema.function.name, "test");
    }

    #[test]
    fn test_assistant_message_with_tool_calls() {
        let tool_calls = vec![ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "get_weather".to_string(),
                arguments: r#"{"city": "Beijing"}"#.to_string(),
            },
        }];

        let msg = Message::assistant("", Some(tool_calls));
        assert!(msg.tool_calls.is_some());
        assert_eq!(msg.tool_calls.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_tool_result_message() {
        let msg = Message::tool_result("call-1", "Sunny, 25°C");
        assert!(matches!(msg.role, Role::Tool));
        assert_eq!(msg.tool_call_id, Some("call-1".to_string()));
        assert_eq!(msg.content, "Sunny, 25°C");
    }

    #[test]
    fn test_tool_message_serialization() {
        let msg = Message::tool_result("call_yyaeEH9yC4MEL0kc5fWJwOZv", "User selected: option1");
        let json = serde_json::to_string(&msg).unwrap();
        println!("Serialized tool message: {}", json);

        // Verify the JSON structure
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["role"], "tool");
        assert_eq!(parsed["content"], "User selected: option1");
        assert_eq!(parsed["tool_call_id"], "call_yyaeEH9yC4MEL0kc5fWJwOZv");

        // Ensure tool_call_id field exists with correct name
        assert!(
            parsed.get("tool_call_id").is_some(),
            "tool_call_id field should exist"
        );
    }

    #[test]
    fn test_assistant_with_tool_calls_serialization() {
        let tool_calls = vec![ToolCall {
            id: "call_yyaeEH9yC4MEL0kc5fWJwOZv".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "ask_user".to_string(),
                arguments: r#"{"question": "test"}"#.to_string(),
            },
        }];

        let msg = Message::assistant("", Some(tool_calls));
        let json = serde_json::to_string(&msg).unwrap();
        println!("Serialized assistant message: {}", json);

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["role"], "assistant");
        assert!(
            parsed.get("tool_calls").is_some(),
            "tool_calls field should exist"
        );
    }

    #[test]
    fn test_session_metadata_serialization() {
        let mut session = Session::new("test-metadata", "test-model");
        session
            .metadata
            .insert("model".to_string(), "gpt-5".to_string());
        session
            .metadata
            .insert("key".to_string(), "value".to_string());

        let json = serde_json::to_string(&session).unwrap();
        println!("Serialized session with metadata: {}", json);

        // Verify metadata is present in JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed.get("metadata").is_some(),
            "metadata field should exist"
        );
        assert_eq!(parsed["metadata"]["model"], "gpt-5");
        assert_eq!(parsed["metadata"]["key"], "value");

        // Deserialize and verify
        let deserialized: Session = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.metadata.get("model"),
            Some(&"gpt-5".to_string())
        );
        assert_eq!(deserialized.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_session_metadata_empty_not_serialized() {
        let session = Session::new("test-no-metadata", "test-model");
        // metadata is empty by default

        let json = serde_json::to_string(&session).unwrap();
        println!("Serialized session without metadata: {}", json);

        // Verify metadata is not present when empty (skip_serializing_if)
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed.get("metadata").is_none(),
            "metadata field should be skipped when empty"
        );
    }

    #[test]
    fn test_session_metadata_backward_compatibility() {
        // Test that old sessions without metadata can still be deserialized
        let old_session_json = r#"{
            "id": "old-session",
            "messages": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;

        let session: Session = serde_json::from_str(old_session_json).unwrap();
        assert_eq!(session.id, "old-session");
        assert!(session.metadata.is_empty());
    }

    #[test]
    fn test_session_with_model_field() {
        let mut session = Session::new("test-session", "gpt-4o-mini");
        assert_eq!(session.model, "gpt-4o-mini");

        // Model can be updated (e.g., user switches models mid-session).
        session.model = "gpt-5".to_string();
        assert_eq!(session.model, "gpt-5");
    }

    #[test]
    fn test_session_model_serialization() {
        let session = Session::new("test-session", "gpt-5");

        let json = serde_json::to_string(&session).unwrap();
        println!("Serialized session with model: {}", json);

        // Verify model is present in JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("model").is_some(), "model field should exist");
        assert_eq!(parsed["model"], "gpt-5");

        // Deserialize and verify
        let deserialized: Session = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.model, "gpt-5");
    }

    #[test]
    fn test_session_model_backward_compatibility() {
        // Test that old sessions without model field can still be deserialized
        let old_session_json = r#"{
            "id": "old-session",
            "messages": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;

        let session: Session = serde_json::from_str(old_session_json).unwrap();
        assert_eq!(session.id, "old-session");
        assert_eq!(session.model, "");
    }

    #[test]
    fn test_session_model_and_metadata_together() {
        // Test that model and metadata can coexist
        let mut session = Session::new("test-session", "gpt-4o");
        session
            .metadata
            .insert("temperature".to_string(), "0.7".to_string());
        session
            .metadata
            .insert("max_tokens".to_string(), "4096".to_string());

        let json = serde_json::to_string(&session).unwrap();
        println!("Serialized session with model and metadata: {}", json);

        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["model"], "gpt-4o");
        assert_eq!(parsed["metadata"]["temperature"], "0.7");
        assert_eq!(parsed["metadata"]["max_tokens"], "4096");

        let deserialized: Session = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.model, "gpt-4o");
        assert_eq!(
            deserialized.metadata.get("temperature"),
            Some(&"0.7".to_string())
        );
    }
}
