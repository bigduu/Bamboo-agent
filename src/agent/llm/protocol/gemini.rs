//! Google Gemini protocol conversion implementation.
//!
//! Gemini API has a unique format:
//! - Messages are called "contents"
//! - Role is "user" or "model" (not "assistant")
//! - Content is an array of "parts"
//! - System instructions are separate from messages
//!
//! # Example Gemini Request
//! ```json
//! {
//!   "contents": [
//!     {
//!       "role": "user",
//!       "parts": [{"text": "Hello"}]
//!     }
//!   ],
//!   "systemInstruction": {
//!     "parts": [{"text": "You are helpful"}]
//!   },
//!   "tools": [...]
//! }
//! ```

use crate::protocol::{FromProvider, ProtocolError, ProtocolResult, ToProvider};
use agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use agent_core::{Message, Role};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Gemini protocol converter.
pub struct GeminiProtocol;

// ============================================================================
// Gemini API Types
// ============================================================================

/// Gemini request format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiRequest {
    /// Conversation history
    pub contents: Vec<GeminiContent>,
    /// System instructions (extracted from system messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_instruction: Option<GeminiContent>,
    /// Available tools
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<GeminiTool>>,
    /// Generation config (temperature, max_tokens, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<Value>,
}

/// Gemini message/content format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiContent {
    /// "user" or "model" (not "assistant")
    pub role: String,
    /// Array of content parts
    pub parts: Vec<GeminiPart>,
}

/// Gemini content part
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiPart {
    /// Text content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Function call (for model responses)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<GeminiFunctionCall>,
    /// Function response (for user/tool messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_response: Option<GeminiFunctionResponse>,
}

/// Gemini function call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiFunctionCall {
    pub name: String,
    pub args: Value,
}

/// Gemini function response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiFunctionResponse {
    pub name: String,
    pub response: Value,
}

/// Gemini tool definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiTool {
    pub function_declarations: Vec<GeminiFunctionDeclaration>,
}

/// Gemini function declaration (tool schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiFunctionDeclaration {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: Value,
}

/// Gemini response format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiResponse {
    pub candidates: Vec<GeminiCandidate>,
}

/// Gemini response candidate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiCandidate {
    pub content: GeminiContent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

// ============================================================================
// Gemini → Internal (FromProvider)
// ============================================================================

impl FromProvider<GeminiContent> for Message {
    fn from_provider(content: GeminiContent) -> ProtocolResult<Self> {
        let role = match content.role.as_str() {
            "user" => Role::User,
            "model" => Role::Assistant,
            "system" => Role::System,
            _ => return Err(ProtocolError::InvalidRole(content.role)),
        };

        // Extract text and tool calls from parts
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for part in content.parts {
            if let Some(text) = part.text {
                text_parts.push(text);
            }

            if let Some(func_call) = part.function_call {
                tool_calls.push(ToolCall {
                    id: format!("gemini_{}", uuid::Uuid::new_v4()), // Gemini doesn't have IDs
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: func_call.name,
                        arguments: serde_json::to_string(&func_call.args)
                            .unwrap_or_default(),
                    },
                });
            }

            if let Some(func_response) = part.function_response {
                // Tool response becomes a tool message
                return Ok(Message::tool_result(
                    format!("gemini_tool_{}", func_response.name),
                    serde_json::to_string(&func_response.response)
                        .unwrap_or_default(),
                ));
            }
        }

        let content_text = text_parts.join("");

        Ok(Message {
            id: String::new(),
            role,
            content: content_text,
            tool_calls: if tool_calls.is_empty() { None } else { Some(tool_calls) },
            tool_call_id: None,
            created_at: chrono::Utc::now(),
        })
    }
}

impl FromProvider<GeminiTool> for ToolSchema {
    fn from_provider(tool: GeminiTool) -> ProtocolResult<Self> {
        // Gemini tools can have multiple function declarations
        // We'll convert the first one
        let func = tool
            .function_declarations
            .into_iter()
            .next()
            .ok_or_else(|| ProtocolError::InvalidToolCall("Empty tool declarations".to_string()))?;

        Ok(ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: func.name,
                description: func.description.unwrap_or_default(),
                parameters: func.parameters,
            },
        })
    }
}

// ============================================================================
// Internal → Gemini (ToProvider)
// ============================================================================

/// Convert internal messages to Gemini request format.
///
/// Note: Gemini extracts system messages to `system_instruction` field.
pub struct GeminiRequestBuilder;

impl ToProvider<GeminiRequest> for Vec<Message> {
    fn to_provider(&self) -> ProtocolResult<GeminiRequest> {
        let mut system_instruction = None;
        let mut contents = Vec::new();

        for msg in self {
            match msg.role {
                Role::System => {
                    // System messages become system_instruction
                    system_instruction = Some(GeminiContent {
                        role: "system".to_string(),
                        parts: vec![GeminiPart {
                            text: Some(msg.content.clone()),
                            function_call: None,
                            function_response: None,
                        }],
                    });
                }
                _ => {
                    contents.push(msg.to_provider()?);
                }
            }
        }

        Ok(GeminiRequest {
            contents,
            system_instruction,
            tools: None,
            generation_config: None,
        })
    }
}

impl ToProvider<GeminiContent> for Message {
    fn to_provider(&self) -> ProtocolResult<GeminiContent> {
        // Handle tool messages specially
        if self.role == Role::Tool {
            let tool_name = self
                .tool_call_id
                .clone()
                .ok_or_else(|| ProtocolError::MissingField("tool_call_id".to_string()))?;

            return Ok(GeminiContent {
                role: "user".to_string(),
                parts: vec![GeminiPart {
                    text: None,
                    function_call: None,
                    function_response: Some(GeminiFunctionResponse {
                        name: tool_name,
                        response: serde_json::from_str(&self.content)
                            .unwrap_or_else(|_| Value::String(self.content.clone())),
                    }),
                }],
            });
        }

        let role = match self.role {
            Role::User => "user",
            Role::Assistant => "model",
            Role::System => "system",
            Role::Tool => "user", // Already handled above, but kept for completeness
        };

        let mut parts = Vec::new();

        // Add text content
        if !self.content.is_empty() {
            parts.push(GeminiPart {
                text: Some(self.content.clone()),
                function_call: None,
                function_response: None,
            });
        }

        // Add tool calls as function_call parts
        if let Some(tool_calls) = &self.tool_calls {
            for tc in tool_calls {
                let args: Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));

                parts.push(GeminiPart {
                    text: None,
                    function_call: Some(GeminiFunctionCall {
                        name: tc.function.name.clone(),
                        args,
                    }),
                    function_response: None,
                });
            }
        }

        // Ensure at least one part
        if parts.is_empty() {
            parts.push(GeminiPart {
                text: Some(String::new()),
                function_call: None,
                function_response: None,
            });
        }

        Ok(GeminiContent { role: role.to_string(), parts })
    }
}

impl ToProvider<GeminiTool> for ToolSchema {
    fn to_provider(&self) -> ProtocolResult<GeminiTool> {
        Ok(GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: self.function.name.clone(),
                description: Some(self.function.description.clone()),
                parameters: self.function.parameters.clone(),
            }],
        })
    }
}

// ============================================================================
// Batch conversion for tools
// ============================================================================

impl ToProvider<Vec<GeminiTool>> for Vec<ToolSchema> {
    fn to_provider(&self) -> ProtocolResult<Vec<GeminiTool>> {
        // Gemini groups all function declarations into a single tool
        let declarations: Vec<GeminiFunctionDeclaration> = self
            .iter()
            .map(|schema| GeminiFunctionDeclaration {
                name: schema.function.name.clone(),
                description: Some(schema.function.description.clone()),
                parameters: schema.function.parameters.clone(),
            })
            .collect();

        if declarations.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![GeminiTool {
                function_declarations: declarations,
            }])
        }
    }
}

// ============================================================================
// Extension trait for ergonomic conversion
// ============================================================================

/// Extension trait for Gemini conversion
pub trait GeminiExt: Sized {
    fn into_internal(self) -> ProtocolResult<Message>;
    fn to_gemini(&self) -> ProtocolResult<GeminiContent>;
}

impl GeminiExt for GeminiContent {
    fn into_internal(self) -> ProtocolResult<Message> {
        Message::from_provider(self)
    }

    fn to_gemini(&self) -> ProtocolResult<GeminiContent> {
        Ok(self.clone())
    }
}

impl GeminiExt for Message {
    fn into_internal(self) -> ProtocolResult<Message> {
        Ok(self)
    }

    fn to_gemini(&self) -> ProtocolResult<GeminiContent> {
        self.to_provider()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gemini_to_internal_user_message() {
        let gemini = GeminiContent {
            role: "user".to_string(),
            parts: vec![GeminiPart {
                text: Some("Hello".to_string()),
                function_call: None,
                function_response: None,
            }],
        };

        let internal: Message = Message::from_provider(gemini).unwrap();

        assert_eq!(internal.role, Role::User);
        assert_eq!(internal.content, "Hello");
        assert!(internal.tool_calls.is_none());
    }

    #[test]
    fn test_internal_to_gemini_user_message() {
        let internal = Message::user("Hello");

        let gemini: GeminiContent = internal.to_provider().unwrap();

        assert_eq!(gemini.role, "user");
        assert_eq!(gemini.parts.len(), 1);
        assert_eq!(gemini.parts[0].text, Some("Hello".to_string()));
    }

    #[test]
    fn test_gemini_to_internal_model_message() {
        let gemini = GeminiContent {
            role: "model".to_string(),
            parts: vec![GeminiPart {
                text: Some("Hello there!".to_string()),
                function_call: None,
                function_response: None,
            }],
        };

        let internal: Message = Message::from_provider(gemini).unwrap();

        assert_eq!(internal.role, Role::Assistant);
        assert_eq!(internal.content, "Hello there!");
    }

    #[test]
    fn test_internal_to_gemini_with_tool_call() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let internal = Message::assistant("Let me search", Some(vec![tool_call]));

        let gemini: GeminiContent = internal.to_provider().unwrap();

        assert_eq!(gemini.role, "model");
        assert_eq!(gemini.parts.len(), 2);
        assert_eq!(gemini.parts[0].text, Some("Let me search".to_string()));
        assert!(gemini.parts[1].function_call.is_some());

        let func_call = gemini.parts[1].function_call.as_ref().unwrap();
        assert_eq!(func_call.name, "search");
        assert_eq!(func_call.args, serde_json::json!({"q": "test"}));
    }

    #[test]
    fn test_gemini_to_internal_with_tool_call() {
        let gemini = GeminiContent {
            role: "model".to_string(),
            parts: vec![GeminiPart {
                text: None,
                function_call: Some(GeminiFunctionCall {
                    name: "search".to_string(),
                    args: serde_json::json!({"q": "test"}),
                }),
                function_response: None,
            }],
        };

        let internal: Message = Message::from_provider(gemini).unwrap();

        assert_eq!(internal.role, Role::Assistant);
        assert!(internal.tool_calls.is_some());

        let tool_calls = internal.tool_calls.unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "search");
    }

    #[test]
    fn test_system_message_extraction() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
        ];

        let request: GeminiRequest = messages.to_provider().unwrap();

        assert!(request.system_instruction.is_some());
        let sys = request.system_instruction.unwrap();
        assert_eq!(sys.role, "system");
        assert_eq!(sys.parts[0].text, Some("You are helpful".to_string()));

        assert_eq!(request.contents.len(), 1);
        assert_eq!(request.contents[0].role, "user");
    }

    #[test]
    fn test_tool_response_conversion() {
        let internal = Message::tool_result("search_tool", r#"{"result": "ok"}"#);

        let gemini: GeminiContent = internal.to_provider().unwrap();

        assert_eq!(gemini.role, "user");
        assert!(gemini.parts[0].function_response.is_some());

        let func_resp = gemini.parts[0].function_response.as_ref().unwrap();
        assert_eq!(func_resp.name, "search_tool");
    }

    #[test]
    fn test_tool_schema_conversion() {
        let gemini_tool = GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: "search".to_string(),
                description: Some("Search the web".to_string()),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "q": { "type": "string" }
                    }
                }),
            }],
        };

        // Gemini → Internal
        let internal_schema: ToolSchema = ToolSchema::from_provider(gemini_tool.clone()).unwrap();
        assert_eq!(internal_schema.function.name, "search");

        // Internal → Gemini
        let roundtrip: GeminiTool = internal_schema.to_provider().unwrap();
        assert_eq!(roundtrip.function_declarations.len(), 1);
        assert_eq!(roundtrip.function_declarations[0].name, "search");
    }

    #[test]
    fn test_multiple_tools_grouped() {
        let tools = vec![
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "search".to_string(),
                    description: "Search".to_string(),
                    parameters: serde_json::json!({"type": "object"}),
                },
            },
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "read".to_string(),
                    description: "Read file".to_string(),
                    parameters: serde_json::json!({"type": "object"}),
                },
            },
        ];

        let gemini_tools: Vec<GeminiTool> = tools.to_provider().unwrap();

        // Gemini groups all tools into one
        assert_eq!(gemini_tools.len(), 1);
        assert_eq!(gemini_tools[0].function_declarations.len(), 2);
        assert_eq!(gemini_tools[0].function_declarations[0].name, "search");
        assert_eq!(gemini_tools[0].function_declarations[1].name, "read");
    }

    #[test]
    fn test_roundtrip_conversion() {
        let original = Message::user("Hello, world!");

        // Internal → Gemini
        let gemini: GeminiContent = original.to_provider().unwrap();

        // Gemini → Internal
        let roundtrip: Message = Message::from_provider(gemini).unwrap();

        assert_eq!(roundtrip.role, original.role);
        assert_eq!(roundtrip.content, original.content);
    }

    #[test]
    fn test_invalid_role_error() {
        let gemini = GeminiContent {
            role: "invalid_role".to_string(),
            parts: vec![GeminiPart {
                text: Some("test".to_string()),
                function_call: None,
                function_response: None,
            }],
        };

        let result: ProtocolResult<Message> = Message::from_provider(gemini);
        assert!(matches!(result, Err(ProtocolError::InvalidRole(_))));
    }

    #[test]
    fn test_empty_parts_has_default() {
        let internal = Message::assistant("", None);

        let gemini: GeminiContent = internal.to_provider().unwrap();

        // Should have at least one part with empty text
        assert_eq!(gemini.parts.len(), 1);
        assert_eq!(gemini.parts[0].text, Some(String::new()));
    }
}
