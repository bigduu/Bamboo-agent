//! OpenAI protocol conversion implementation.

use crate::api::models::{
    ChatMessage as OpenAIChatMessage, Content as OpenAIContent,
    ContentPart as OpenAIContentPart, Role as OpenAIRole, Tool, ToolCall as OpenAIToolCall,
};
use crate::protocol::{FromProvider, ProtocolResult, ToProvider};
use agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use agent_core::{Message, Role};

/// OpenAI protocol converter.
pub struct OpenAIProtocol;

// ============================================================================
// OpenAI → Internal (FromProvider)
// ============================================================================

impl FromProvider<OpenAIChatMessage> for Message {
    fn from_provider(msg: OpenAIChatMessage) -> ProtocolResult<Self> {
        let role = convert_openai_role_to_internal(&msg.role);

        let content = match msg.content {
            OpenAIContent::Text(text) => text,
            OpenAIContent::Parts(parts) => {
                // Extract text from parts, ignore images for now
                parts
                    .into_iter()
                    .filter_map(|part| match part {
                        OpenAIContentPart::Text { text } => Some(text),
                        OpenAIContentPart::ImageUrl { .. } => None,
                    })
                    .collect::<Vec<_>>()
                    .join("")
            }
        };

        let tool_calls = msg
            .tool_calls
            .map(|calls| {
                calls
                    .into_iter()
                    .map(ToolCall::from_provider)
                    .collect()
            })
            .transpose()?;

        Ok(Message {
            id: String::new(), // Will be generated if needed
            role,
            content,
            tool_calls,
            tool_call_id: msg.tool_call_id,
            created_at: chrono::Utc::now(),
        })
    }
}

impl FromProvider<OpenAIToolCall> for ToolCall {
    fn from_provider(tc: OpenAIToolCall) -> ProtocolResult<Self> {
        Ok(ToolCall {
            id: tc.id,
            tool_type: tc.tool_type,
            function: FunctionCall {
                name: tc.function.name,
                arguments: tc.function.arguments,
            },
        })
    }
}

impl FromProvider<Tool> for ToolSchema {
    fn from_provider(tool: Tool) -> ProtocolResult<Self> {
        Ok(ToolSchema {
            schema_type: tool.tool_type,
            function: FunctionSchema {
                name: tool.function.name,
                description: tool.function.description.unwrap_or_default(),
                parameters: tool.function.parameters,
            },
        })
    }
}

// ============================================================================
// Internal → OpenAI (ToProvider)
// ============================================================================

impl ToProvider<OpenAIChatMessage> for Message {
    fn to_provider(&self) -> ProtocolResult<OpenAIChatMessage> {
        let role = convert_internal_role_to_openai(&self.role);

        let content = OpenAIContent::Text(self.content.clone());

        let tool_calls = self
            .tool_calls
            .as_ref()
            .map(|calls| calls.iter().map(|tc| tc.to_provider()).collect())
            .transpose()?;

        Ok(OpenAIChatMessage {
            role,
            content,
            tool_calls,
            tool_call_id: self.tool_call_id.clone(),
        })
    }
}

impl ToProvider<OpenAIToolCall> for ToolCall {
    fn to_provider(&self) -> ProtocolResult<OpenAIToolCall> {
        Ok(OpenAIToolCall {
            id: self.id.clone(),
            tool_type: self.tool_type.clone(),
            function: crate::api::models::FunctionCall {
                name: self.function.name.clone(),
                arguments: self.function.arguments.clone(),
            },
        })
    }
}

impl ToProvider<Tool> for ToolSchema {
    fn to_provider(&self) -> ProtocolResult<Tool> {
        Ok(Tool {
            tool_type: self.schema_type.clone(),
            function: crate::api::models::FunctionDefinition {
                name: self.function.name.clone(),
                description: Some(self.function.description.clone()),
                parameters: self.function.parameters.clone(),
            },
        })
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn convert_openai_role_to_internal(role: &OpenAIRole) -> Role {
    match role {
        OpenAIRole::System => Role::System,
        OpenAIRole::User => Role::User,
        OpenAIRole::Assistant => Role::Assistant,
        OpenAIRole::Tool => Role::Tool,
    }
}

fn convert_internal_role_to_openai(role: &Role) -> OpenAIRole {
    match role {
        Role::System => OpenAIRole::System,
        Role::User => OpenAIRole::User,
        Role::Assistant => OpenAIRole::Assistant,
        Role::Tool => OpenAIRole::Tool,
    }
}

// ============================================================================
// Extension trait for ergonomic conversion
// ============================================================================

/// Extension trait for converting types with .into_internal() and .to_openai() (test-only)
#[cfg(test)]
pub trait OpenAIExt: Sized {
    fn into_internal(self) -> ProtocolResult<Message>;
    fn to_openai(&self) -> ProtocolResult<OpenAIChatMessage>;
}

#[cfg(test)]
impl OpenAIExt for OpenAIChatMessage {
    fn into_internal(self) -> ProtocolResult<Message> {
        Message::from_provider(self)
    }

    fn to_openai(&self) -> ProtocolResult<OpenAIChatMessage> {
        Ok(self.clone())
    }
}

#[cfg(test)]
impl OpenAIExt for Message {
    fn into_internal(self) -> ProtocolResult<Message> {
        Ok(self)
    }

    fn to_openai(&self) -> ProtocolResult<OpenAIChatMessage> {
        self.to_provider()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::{FunctionCall as OpenAIFunctionCall, Role as OpenAIRole};
    use agent_core::tools::FunctionCall;
    use agent_core::Role;

    #[test]
    fn test_openai_to_internal_simple_message() {
        let openai_msg = OpenAIChatMessage {
            role: OpenAIRole::User,
            content: OpenAIContent::Text("Hello".to_string()),
            tool_calls: None,
            tool_call_id: None,
        };

        let internal_msg: Message = Message::from_provider(openai_msg).unwrap();

        assert_eq!(internal_msg.role, Role::User);
        assert_eq!(internal_msg.content, "Hello");
        assert!(internal_msg.tool_calls.is_none());
    }

    #[test]
    fn test_internal_to_openai_simple_message() {
        let internal_msg = Message::user("Hello");

        let openai_msg: OpenAIChatMessage = internal_msg.to_provider().unwrap();

        assert_eq!(openai_msg.role, OpenAIRole::User);
        assert!(matches!(openai_msg.content, OpenAIContent::Text(ref t) if t == "Hello"));
        assert!(openai_msg.tool_calls.is_none());
    }

    #[test]
    fn test_openai_to_internal_with_tool_call() {
        let openai_msg = OpenAIChatMessage {
            role: OpenAIRole::Assistant,
            content: OpenAIContent::Text(String::new()),
            tool_calls: Some(vec![OpenAIToolCall {
                id: "call_1".to_string(),
                tool_type: "function".to_string(),
                function: OpenAIFunctionCall {
                    name: "search".to_string(),
                    arguments: r#"{"q":"test"}"#.to_string(),
                },
            }]),
            tool_call_id: None,
        };

        let internal_msg: Message = Message::from_provider(openai_msg).unwrap();

        assert_eq!(internal_msg.role, Role::Assistant);
        assert!(internal_msg.tool_calls.is_some());
        let tool_calls = internal_msg.tool_calls.unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_1");
        assert_eq!(tool_calls[0].function.name, "search");
    }

    #[test]
    fn test_internal_to_openai_with_tool_call() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let internal_msg = Message::assistant("", Some(vec![tool_call]));

        let openai_msg: OpenAIChatMessage = internal_msg.to_provider().unwrap();

        assert_eq!(openai_msg.role, OpenAIRole::Assistant);
        assert!(openai_msg.tool_calls.is_some());
        let tool_calls = openai_msg.tool_calls.unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_1");
        assert_eq!(tool_calls[0].function.name, "search");
        assert_eq!(tool_calls[0].function.arguments, r#"{"q":"test"}"#);
    }

    #[test]
    fn test_roundtrip_conversion() {
        let original = Message::user("Hello, world!");

        // Internal → OpenAI
        let openai_msg: OpenAIChatMessage = original.to_provider().unwrap();

        // OpenAI → Internal
        let roundtrip: Message = Message::from_provider(openai_msg).unwrap();

        assert_eq!(roundtrip.role, original.role);
        assert_eq!(roundtrip.content, original.content);
    }

    #[test]
    fn test_tool_schema_conversion() {
        let openai_tool = Tool {
            tool_type: "function".to_string(),
            function: crate::api::models::FunctionDefinition {
                name: "search".to_string(),
                description: Some("Search the web".to_string()),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "q": { "type": "string" }
                    }
                }),
            },
        };

        // OpenAI → Internal
        let internal_schema: ToolSchema = ToolSchema::from_provider(openai_tool.clone()).unwrap();
        assert_eq!(internal_schema.function.name, "search");

        // Internal → OpenAI
        let roundtrip: Tool = internal_schema.to_provider().unwrap();
        assert_eq!(roundtrip.function.name, "search");
        assert_eq!(roundtrip.function.description, Some("Search the web".to_string()));
    }

    #[test]
    fn test_extension_trait() {
        let openai_msg = OpenAIChatMessage {
            role: OpenAIRole::User,
            content: OpenAIContent::Text("Test".to_string()),
            tool_calls: None,
            tool_call_id: None,
        };

        // Using extension trait
        let internal = openai_msg.into_internal().unwrap();
        assert_eq!(internal.content, "Test");
    }
}
