//! Anthropic protocol conversion implementation.

use crate::providers::anthropic::api_types::*;
use crate::protocol::{FromProvider, ProtocolError, ProtocolResult, ToProvider};
use agent_core::tools::{FunctionSchema, ToolSchema};
use agent_core::{Message, Role};
use serde_json::Value;

#[cfg(test)]
use agent_core::tools::{FunctionCall, ToolCall};

/// Anthropic protocol converter.
pub struct AnthropicProtocol;

// ============================================================================
// Anthropic → Internal (FromProvider)
// ============================================================================

impl FromProvider<AnthropicMessage> for Message {
    fn from_provider(msg: AnthropicMessage) -> ProtocolResult<Self> {
        let role = convert_anthropic_role_to_internal(&msg.role);

        let content = match msg.content {
            AnthropicContent::Text(text) => text,
            AnthropicContent::Blocks(blocks) => {
                extract_text_from_anthropic_blocks(blocks)?
            }
        };

        Ok(Message {
            id: String::new(),
            role,
            content,
            tool_calls: None, // Anthropic messages don't have tool_calls at this level
            tool_call_id: None,
            created_at: chrono::Utc::now(),
        })
    }
}

impl FromProvider<AnthropicTool> for ToolSchema {
    fn from_provider(tool: AnthropicTool) -> ProtocolResult<Self> {
        Ok(ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: tool.name,
                description: tool.description.unwrap_or_default(),
                parameters: tool.input_schema,
            },
        })
    }
}

// ============================================================================
// Internal → Anthropic (ToProvider)
// ============================================================================

/// Converts internal messages to Anthropic request format.
///
/// Note: Anthropic has a special structure where system messages are
/// extracted to a top-level field, not included in the messages array.
pub struct AnthropicRequest {
    pub system: Option<String>,
    pub messages: Vec<AnthropicMessage>,
}

impl ToProvider<AnthropicRequest> for Vec<Message> {
    fn to_provider(&self) -> ProtocolResult<AnthropicRequest> {
        let mut system_parts = Vec::new();
        let mut anthropic_messages = Vec::new();

        for msg in self {
            match msg.role {
                Role::System => {
                    system_parts.push(msg.content.clone());
                }
                _ => {
                    anthropic_messages.push(msg.to_provider()?);
                }
            }
        }

        let system = if system_parts.is_empty() {
            None
        } else {
            Some(system_parts.join("\n\n"))
        };

        Ok(AnthropicRequest {
            system,
            messages: anthropic_messages,
        })
    }
}

impl ToProvider<AnthropicMessage> for Message {
    fn to_provider(&self) -> ProtocolResult<AnthropicMessage> {
        let role = convert_internal_role_to_anthropic(&self.role);

        let content = match self.role {
            Role::System => {
                // System messages are handled at the request level
                AnthropicContent::Text(self.content.clone())
            }
            Role::User => {
                AnthropicContent::Blocks(vec![AnthropicContentBlock::Text {
                    text: self.content.clone(),
                }])
            }
            Role::Assistant => {
                let mut blocks: Vec<AnthropicContentBlock> = Vec::new();

                // Add text content
                if !self.content.is_empty() {
                    blocks.push(AnthropicContentBlock::Text {
                        text: self.content.clone(),
                    });
                }

                // Add tool calls as tool_use blocks
                if let Some(tool_calls) = &self.tool_calls {
                    for tc in tool_calls {
                        let input: Value = serde_json::from_str(&tc.function.arguments)
                            .unwrap_or_else(|_| Value::String(tc.function.arguments.clone()));

                        blocks.push(AnthropicContentBlock::ToolUse {
                            id: tc.id.clone(),
                            name: tc.function.name.clone(),
                            input,
                        });
                    }
                }

                AnthropicContent::Blocks(blocks)
            }
            Role::Tool => {
                // Tool messages become tool_result blocks wrapped in a user message
                let tool_use_id = self
                    .tool_call_id
                    .clone()
                    .ok_or_else(|| ProtocolError::MissingField("tool_call_id".to_string()))?;

                AnthropicContent::Blocks(vec![AnthropicContentBlock::ToolResult {
                    tool_use_id,
                    content: Value::String(self.content.clone()),
                }])
            }
        };

        Ok(AnthropicMessage { role, content })
    }
}

impl ToProvider<AnthropicTool> for ToolSchema {
    fn to_provider(&self) -> ProtocolResult<AnthropicTool> {
        Ok(AnthropicTool {
            name: self.function.name.clone(),
            description: Some(self.function.description.clone()),
            input_schema: self.function.parameters.clone(),
        })
    }
}

// ============================================================================
// Response conversion (for proxy/adapter use cases)
// ============================================================================

/// Convert Anthropic response to internal format (for API proxy scenarios)
#[cfg(test)]
pub struct AnthropicResponseConverter;

#[cfg(test)]
impl AnthropicResponseConverter {
    /// Convert Anthropic messages response to internal message format
    pub fn convert_response(response: AnthropicMessagesResponse) -> ProtocolResult<Message> {
        // Extract text content from response blocks
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for block in response.content {
            match block {
                AnthropicResponseContentBlock::Text { text } => {
                    text_parts.push(text);
                }
                AnthropicResponseContentBlock::ToolUse { id, name, input } => {
                    tool_calls.push(ToolCall {
                        id,
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name,
                            arguments: serde_json::to_string(&input)
                                .unwrap_or_else(|_| String::new()),
                        },
                    });
                }
            }
        }

        let content = text_parts.join("");
        let tool_calls = if tool_calls.is_empty() {
            None
        } else {
            Some(tool_calls)
        };

        Ok(Message {
            id: response.id,
            role: Role::Assistant,
            content,
            tool_calls,
            tool_call_id: None,
            created_at: chrono::Utc::now(),
        })
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn convert_anthropic_role_to_internal(role: &AnthropicRole) -> Role {
    match role {
        AnthropicRole::User => Role::User,
        AnthropicRole::Assistant => Role::Assistant,
        AnthropicRole::System => Role::System,
    }
}

fn convert_internal_role_to_anthropic(role: &Role) -> AnthropicRole {
    match role {
        Role::User => AnthropicRole::User,
        Role::Assistant => AnthropicRole::Assistant,
        // Note: System messages are handled specially in Anthropic
        Role::System => AnthropicRole::User,
        // Tool messages become user messages with tool_result blocks
        Role::Tool => AnthropicRole::User,
    }
}

fn extract_text_from_anthropic_blocks(blocks: Vec<AnthropicContentBlock>) -> ProtocolResult<String> {
    let mut texts = Vec::new();

    for block in blocks {
        match block {
            AnthropicContentBlock::Text { text } => texts.push(text),
            AnthropicContentBlock::ToolUse { .. } => {
                // Tool calls are handled separately
            }
            AnthropicContentBlock::ToolResult { content, .. } => {
                // Extract text from tool result
                match content {
                    Value::String(s) => texts.push(s),
                    Value::Array(arr) => {
                        for item in arr {
                            if let Some(obj) = item.as_object() {
                                if let Some(text) = obj
                                    .get("text")
                                    .and_then(|v| v.as_str())
                                {
                                    texts.push(text.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(texts.join("\n"))
}

// ============================================================================
// Extension trait for ergonomic conversion
// ============================================================================

/// Extension trait for Anthropic conversion (test-only)
#[cfg(test)]
pub trait AnthropicExt: Sized {
    fn into_internal(self) -> ProtocolResult<Message>;
    fn to_anthropic(&self) -> ProtocolResult<AnthropicMessage>;
}

#[cfg(test)]
impl AnthropicExt for AnthropicMessage {
    fn into_internal(self) -> ProtocolResult<Message> {
        Message::from_provider(self)
    }

    fn to_anthropic(&self) -> ProtocolResult<AnthropicMessage> {
        // Already an Anthropic message, just clone it
        // In practice, you'd deserialize and re-serialize
        unimplemented!("Use clone for now")
    }
}

#[cfg(test)]
impl AnthropicExt for Message {
    fn into_internal(self) -> ProtocolResult<Message> {
        Ok(self)
    }

    fn to_anthropic(&self) -> ProtocolResult<AnthropicMessage> {
        self.to_provider()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anthropic_to_internal_text_message() {
        let anthropic_msg = AnthropicMessage {
            role: AnthropicRole::User,
            content: AnthropicContent::Text("Hello".to_string()),
        };

        let internal: Message = Message::from_provider(anthropic_msg).unwrap();

        assert_eq!(internal.role, Role::User);
        assert_eq!(internal.content, "Hello");
    }

    #[test]
    fn test_internal_to_anthropic_user_message() {
        let internal = Message::user("Hello");

        let anthropic: AnthropicMessage = internal.to_anthropic().unwrap();

        assert_eq!(anthropic.role, AnthropicRole::User);
        match anthropic.content {
            AnthropicContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert!(matches!(blocks[0], AnthropicContentBlock::Text { text: ref t } if t == "Hello"));
            }
            _ => panic!("Expected Blocks content"),
        }
    }

    #[test]
    fn test_internal_to_anthropic_system_message_extraction() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
        ];

        let request: AnthropicRequest = messages.to_provider().unwrap();

        assert_eq!(request.system, Some("You are helpful".to_string()));
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, AnthropicRole::User);
    }

    #[test]
    fn test_internal_to_anthropic_with_tool_call() {
        let tool_call = ToolCall {
            id: "toolu_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let internal = Message::assistant("Let me search", Some(vec![tool_call]));

        let anthropic: AnthropicMessage = internal.to_anthropic().unwrap();

        match anthropic.content {
            AnthropicContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 2);
                assert!(matches!(blocks[0], AnthropicContentBlock::Text { .. }));
                assert!(matches!(blocks[1], AnthropicContentBlock::ToolUse { ref id, ref name, .. } if id == "toolu_1" && name == "search"));
            }
            _ => panic!("Expected Blocks content"),
        }
    }

    #[test]
    fn test_tool_message_to_anthropic() {
        let internal = Message::tool_result("toolu_1", "Result here");

        let anthropic: AnthropicMessage = internal.to_anthropic().unwrap();

        assert_eq!(anthropic.role, AnthropicRole::User);
        match anthropic.content {
            AnthropicContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert!(matches!(blocks[0], AnthropicContentBlock::ToolResult { ref tool_use_id, .. } if tool_use_id == "toolu_1"));
            }
            _ => panic!("Expected Blocks content"),
        }
    }

    #[test]
    fn test_tool_schema_conversion() {
        let anthropic_tool = AnthropicTool {
            name: "search".to_string(),
            description: Some("Search the web".to_string()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "q": { "type": "string" }
                }
            }),
        };

        // Anthropic → Internal
        let internal_schema: ToolSchema = ToolSchema::from_provider(anthropic_tool.clone()).unwrap();
        assert_eq!(internal_schema.function.name, "search");

        // Internal → Anthropic
        let roundtrip: AnthropicTool = internal_schema.to_provider().unwrap();
        assert_eq!(roundtrip.name, "search");
        assert_eq!(roundtrip.description, Some("Search the web".to_string()));
    }

    #[test]
    fn test_anthropic_response_to_internal() {
        let response = AnthropicMessagesResponse {
            id: "msg_1".to_string(),
            response_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![
                AnthropicResponseContentBlock::Text {
                    text: "Hello, world!".to_string(),
                },
            ],
            model: "claude-3-sonnet".to_string(),
            stop_reason: "end_turn".to_string(),
            stop_sequence: None,
            usage: AnthropicUsage {
                input_tokens: 10,
                output_tokens: 5,
            },
        };

        let internal = AnthropicResponseConverter::convert_response(response).unwrap();

        assert_eq!(internal.role, Role::Assistant);
        assert_eq!(internal.content, "Hello, world!");
        assert!(internal.tool_calls.is_none());
    }

    #[test]
    fn test_anthropic_response_with_tool_use() {
        let response = AnthropicMessagesResponse {
            id: "msg_1".to_string(),
            response_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![
                AnthropicResponseContentBlock::Text {
                    text: "Let me help you search.".to_string(),
                },
                AnthropicResponseContentBlock::ToolUse {
                    id: "toolu_1".to_string(),
                    name: "search".to_string(),
                    input: serde_json::json!({"q": "test"}),
                },
            ],
            model: "claude-3-sonnet".to_string(),
            stop_reason: "tool_use".to_string(),
            stop_sequence: None,
            usage: AnthropicUsage {
                input_tokens: 10,
                output_tokens: 5,
            },
        };

        let internal = AnthropicResponseConverter::convert_response(response).unwrap();

        assert_eq!(internal.content, "Let me help you search.");
        assert!(internal.tool_calls.is_some());
        let tool_calls = internal.tool_calls.unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "toolu_1");
        assert_eq!(tool_calls[0].function.name, "search");
    }
}
