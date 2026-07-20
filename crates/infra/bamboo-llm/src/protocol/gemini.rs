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
use bamboo_domain::{FunctionCall, ToolCall};
use bamboo_domain::{FunctionSchema, ToolSchema};
use bamboo_domain::{Message, Role};
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
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "systemInstruction",
        alias = "system_instruction"
    )]
    pub system_instruction: Option<GeminiContent>,
    /// Available tools
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<GeminiTool>>,
    /// Generation config (temperature, max_tokens, etc.)
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "generationConfig",
        alias = "generation_config"
    )]
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
    /// Inline base64 image content.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "inlineData",
        alias = "inline_data"
    )]
    pub inline_data: Option<GeminiInlineData>,
    /// File/URL-based image reference.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "fileData",
        alias = "file_data"
    )]
    pub file_data: Option<GeminiFileData>,
    /// Function call (for model responses)
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "functionCall",
        alias = "function_call"
    )]
    pub function_call: Option<GeminiFunctionCall>,
    /// Function response (for user/tool messages)
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "functionResponse",
        alias = "function_response"
    )]
    pub function_response: Option<GeminiFunctionResponse>,
}

/// Gemini inline image payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiInlineData {
    #[serde(rename = "mimeType", alias = "mime_type")]
    pub mime_type: String,
    pub data: String,
}

/// Gemini file image payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiFileData {
    #[serde(rename = "fileUri", alias = "file_uri")]
    pub file_uri: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "mimeType",
        alias = "mime_type"
    )]
    pub mime_type: Option<String>,
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

fn normalize_function_response(content: &str) -> Value {
    match serde_json::from_str(content) {
        Ok(Value::Object(response)) => Value::Object(response),
        Ok(response) => serde_json::json!({ "result": response }),
        Err(_) => serde_json::json!({ "result": content }),
    }
}

/// Gemini tool definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiTool {
    #[serde(rename = "functionDeclarations", alias = "function_declarations")]
    pub function_declarations: Vec<GeminiFunctionDeclaration>,
}

/// Gemini function declaration (tool schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiFunctionDeclaration {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Full JSON Schema for function parameters, sent as `parametersJsonSchema`.
    /// This field supports the complete JSON Schema spec (including
    /// `additionalProperties`, `anyOf`, `$ref`, etc.) unlike the older
    /// `parameters` field which only accepts an OpenAPI 3.0.3 subset.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "parametersJsonSchema",
        alias = "parameters_json_schema"
    )]
    pub parameters_json_schema: Option<Value>,
    /// Legacy OpenAPI 3.0.3 `parameters` field. Accepted on deserialization
    /// for backwards compatibility but not serialized by the internal →
    /// Gemini direction.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "parameters",
        alias = "parameters"
    )]
    pub parameters: Option<Value>,
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

        // Extract text/image content and tool calls from parts.
        let mut text_parts = Vec::new();
        let mut content_parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut has_image_parts = false;

        for part in content.parts {
            if let Some(text) = part.text {
                text_parts.push(text.clone());
                content_parts.push(bamboo_domain::MessagePart::Text { text });
            }

            if let Some(inline_data) = part.inline_data {
                if let Some(url) = inline_data_to_data_url(&inline_data) {
                    has_image_parts = true;
                    content_parts.push(bamboo_domain::MessagePart::ImageUrl {
                        image_url: bamboo_domain::ImageUrlRef { url, detail: None },
                    });
                }
            }

            if let Some(file_data) = part.file_data {
                let file_uri = file_data.file_uri.trim();
                if !file_uri.is_empty() {
                    has_image_parts = true;
                    content_parts.push(bamboo_domain::MessagePart::ImageUrl {
                        image_url: bamboo_domain::ImageUrlRef {
                            url: file_uri.to_string(),
                            detail: None,
                        },
                    });
                }
            }

            if let Some(func_call) = part.function_call {
                tool_calls.push(ToolCall {
                    id: format!("gemini_{}", uuid::Uuid::new_v4()), // Gemini doesn't have IDs
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: func_call.name,
                        arguments: serde_json::to_string(&func_call.args).unwrap_or_default(),
                    },
                });
            }

            if let Some(func_response) = part.function_response {
                // Tool response becomes a tool message
                return Ok(Message::tool_result(
                    format!("gemini_tool_{}", func_response.name),
                    serde_json::to_string(&func_response.response).unwrap_or_default(),
                ));
            }
        }

        let content_text = text_parts.join("");

        Ok(Message {
            id: String::new(),
            role,
            content: content_text,
            reasoning: None,
            reasoning_signature: None,
            content_parts: has_image_parts.then_some(content_parts),
            image_ocr: None,
            phase: None,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: chrono::Utc::now(),
            metadata: None,
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

        let parameters = func
            .parameters_json_schema
            .or(func.parameters)
            .unwrap_or(Value::Null);

        Ok(ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: func.name,
                description: func.description.unwrap_or_default(),
                parameters,
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
        let mut system_texts = Vec::new();
        let mut contents = Vec::new();

        for msg in self {
            match msg.role {
                Role::System => {
                    let trimmed = msg.content.trim();
                    if !trimmed.is_empty() {
                        system_texts.push(trimmed.to_string());
                    }
                }
                _ => {
                    contents.push(msg.to_provider()?);
                }
            }
        }

        let system_instruction = if system_texts.is_empty() {
            None
        } else {
            Some(GeminiContent {
                role: "system".to_string(),
                parts: vec![GeminiPart {
                    text: Some(system_texts.join("\n\n")),
                    inline_data: None,
                    file_data: None,
                    function_call: None,
                    function_response: None,
                }],
            })
        };

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
                    inline_data: None,
                    file_data: None,
                    function_call: None,
                    function_response: Some(GeminiFunctionResponse {
                        name: tool_name,
                        // Gemini's functionResponse.response field is a protobuf
                        // Struct, so scalar, array, and plain-text tool results
                        // must be wrapped in an object before serialization.
                        response: normalize_function_response(&self.content),
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

        // Preserve multimodal parts (text + images) when available.
        if let Some(content_parts) = self.content_parts.as_ref() {
            for part in content_parts {
                if let Some(gemini_part) = message_content_part_to_gemini_part(part) {
                    parts.push(gemini_part);
                }
            }
        }

        // Fall back to text projection if there are no explicit parts.
        if parts.is_empty() && !self.content.is_empty() {
            parts.push(GeminiPart {
                text: Some(self.content.clone()),
                inline_data: None,
                file_data: None,
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
                    inline_data: None,
                    file_data: None,
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
                inline_data: None,
                file_data: None,
                function_call: None,
                function_response: None,
            });
        }

        Ok(GeminiContent {
            role: role.to_string(),
            parts,
        })
    }
}

impl ToProvider<GeminiTool> for ToolSchema {
    fn to_provider(&self) -> ProtocolResult<GeminiTool> {
        Ok(GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: self.function.name.clone(),
                description: Some(self.function.description.clone()),
                parameters_json_schema: Some(self.function.parameters.clone()),
                parameters: None,
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
                parameters_json_schema: Some(schema.function.parameters.clone()),
                parameters: None,
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

fn message_content_part_to_gemini_part(part: &bamboo_domain::MessagePart) -> Option<GeminiPart> {
    match part {
        bamboo_domain::MessagePart::Text { text } => Some(GeminiPart {
            text: Some(text.clone()),
            inline_data: None,
            file_data: None,
            function_call: None,
            function_response: None,
        }),
        bamboo_domain::MessagePart::ImageUrl { image_url } => {
            image_url_to_gemini_part(&image_url.url)
        }
    }
}

fn image_url_to_gemini_part(url: &str) -> Option<GeminiPart> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((mime_type, data)) = parse_data_url_base64(trimmed) {
        return Some(GeminiPart {
            text: None,
            inline_data: Some(GeminiInlineData { mime_type, data }),
            file_data: None,
            function_call: None,
            function_response: None,
        });
    }

    Some(GeminiPart {
        text: None,
        inline_data: None,
        file_data: Some(GeminiFileData {
            file_uri: trimmed.to_string(),
            mime_type: None,
        }),
        function_call: None,
        function_response: None,
    })
}

fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let data = data.trim();
    if data.is_empty() {
        return None;
    }

    let mut mime_type = "application/octet-stream";
    let mut is_base64 = false;
    for (idx, seg) in meta.split(';').enumerate() {
        let segment = seg.trim();
        if idx == 0 && !segment.is_empty() && !segment.eq_ignore_ascii_case("base64") {
            mime_type = segment;
        }
        if segment.eq_ignore_ascii_case("base64") {
            is_base64 = true;
        }
    }

    if !is_base64 {
        return None;
    }

    Some((mime_type.to_string(), data.to_string()))
}

fn inline_data_to_data_url(inline: &GeminiInlineData) -> Option<String> {
    let mime_type = inline.mime_type.trim();
    let data = inline.data.trim();
    if mime_type.is_empty() || data.is_empty() {
        return None;
    }
    Some(format!("data:{mime_type};base64,{data}"))
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
    use crate::models::{ContentPart, ImageUrl};
    use bamboo_domain::MessagePart;

    #[test]
    fn test_gemini_to_internal_user_message() {
        let gemini = GeminiContent {
            role: "user".to_string(),
            parts: vec![GeminiPart {
                text: Some("Hello".to_string()),
                inline_data: None,
                file_data: None,
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
    fn test_internal_to_gemini_with_data_url_image_part() {
        let internal = Message::user_with_parts(
            "describe",
            vec![
                ContentPart::Text {
                    text: "describe".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/png;base64,AAAA".to_string(),
                        detail: None,
                    },
                },
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        );

        let gemini: GeminiContent = internal.to_provider().unwrap();

        assert_eq!(gemini.parts.len(), 2);
        assert_eq!(gemini.parts[0].text, Some("describe".to_string()));
        let inline = gemini.parts[1]
            .inline_data
            .as_ref()
            .expect("inlineData should be present");
        assert_eq!(inline.mime_type, "image/png");
        assert_eq!(inline.data, "AAAA");
        assert!(gemini.parts[1].file_data.is_none());
    }

    #[test]
    fn test_gemini_to_internal_model_message() {
        let gemini = GeminiContent {
            role: "model".to_string(),
            parts: vec![GeminiPart {
                text: Some("Hello there!".to_string()),
                inline_data: None,
                file_data: None,
                function_call: None,
                function_response: None,
            }],
        };

        let internal: Message = Message::from_provider(gemini).unwrap();

        assert_eq!(internal.role, Role::Assistant);
        assert_eq!(internal.content, "Hello there!");
    }

    #[test]
    fn test_gemini_to_internal_with_inline_data_image() {
        let gemini = GeminiContent {
            role: "user".to_string(),
            parts: vec![GeminiPart {
                text: Some("look".to_string()),
                inline_data: Some(GeminiInlineData {
                    mime_type: "image/png".to_string(),
                    data: "BBBB".to_string(),
                }),
                file_data: None,
                function_call: None,
                function_response: None,
            }],
        };

        let internal: Message = Message::from_provider(gemini).unwrap();
        assert_eq!(internal.content, "look");
        let parts = internal
            .content_parts
            .as_ref()
            .expect("content_parts should preserve image");
        assert!(parts.iter().any(|part| {
            matches!(
                part,
                MessagePart::ImageUrl { image_url }
                if image_url.url == "data:image/png;base64,BBBB"
            )
        }));
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
                inline_data: None,
                file_data: None,
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
        let messages = vec![Message::system("You are helpful"), Message::user("Hello")];

        let request: GeminiRequest = messages.to_provider().unwrap();

        assert!(request.system_instruction.is_some());
        let sys = request.system_instruction.unwrap();
        assert_eq!(sys.role, "system");
        assert_eq!(sys.parts[0].text, Some("You are helpful".to_string()));

        assert_eq!(request.contents.len(), 1);
        assert_eq!(request.contents[0].role, "user");
    }

    #[test]
    fn test_multiple_system_messages_are_joined() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::system("Use tools when needed"),
            Message::user("Hello"),
        ];

        let request: GeminiRequest = messages.to_provider().unwrap();

        let sys = request
            .system_instruction
            .expect("system instruction should be present");
        assert_eq!(sys.role, "system");
        assert_eq!(
            sys.parts[0].text.as_deref(),
            Some("You are helpful\n\nUse tools when needed")
        );
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
        assert_eq!(func_resp.response, serde_json::json!({ "result": "ok" }));
    }

    #[test]
    fn test_plain_text_tool_response_is_wrapped_in_object() {
        let internal = Message::tool_result("read_file", "plain text output");

        let gemini: GeminiContent = internal.to_provider().unwrap();

        let func_resp = gemini.parts[0].function_response.as_ref().unwrap();
        assert_eq!(
            func_resp.response,
            serde_json::json!({ "result": "plain text output" })
        );
    }

    #[test]
    fn test_non_object_json_tool_response_is_wrapped_in_object() {
        let internal = Message::tool_result("list_items", r#"["first", "second"]"#);

        let gemini: GeminiContent = internal.to_provider().unwrap();

        let func_resp = gemini.parts[0].function_response.as_ref().unwrap();
        assert_eq!(
            func_resp.response,
            serde_json::json!({ "result": ["first", "second"] })
        );
    }

    #[test]
    fn test_tool_schema_conversion() {
        let gemini_tool = GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: "search".to_string(),
                description: Some("Search the web".to_string()),
                parameters_json_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "q": { "type": "string" }
                    }
                })),
                parameters: None,
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
                inline_data: None,
                file_data: None,
                function_call: None,
                function_response: None,
            }],
        };

        let result: ProtocolResult<Message> = Message::from_provider(gemini);
        assert!(matches!(result, Err(ProtocolError::InvalidRole(_))));
    }

    #[test]
    fn test_to_provider_uses_parameters_json_schema_field() {
        let tool = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "bash".to_string(),
                description: "Run a command".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "command": { "type": "string" }
                    },
                    "required": ["command"],
                    "additionalProperties": false
                }),
            },
        };

        let gemini_tool: GeminiTool = tool.to_provider().unwrap();
        let decl = &gemini_tool.function_declarations[0];

        // Must use parametersJsonSchema, NOT the legacy parameters field
        assert!(
            decl.parameters_json_schema.is_some(),
            "parameters_json_schema should be set"
        );
        assert!(
            decl.parameters.is_none(),
            "legacy parameters field should be None"
        );

        // additionalProperties should be preserved (Gemini accepts it in this field)
        let schema = decl.parameters_json_schema.as_ref().unwrap();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["command"]["type"], "string");
    }

    #[test]
    fn test_to_provider_serializes_as_parameters_json_schema() {
        let tool = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "read".to_string(),
                description: "Read a file".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
            },
        };

        let gemini_tool: GeminiTool = tool.to_provider().unwrap();
        let json = serde_json::to_string(&gemini_tool).unwrap();

        assert!(
            json.contains("parametersJsonSchema"),
            "serialized JSON should use 'parametersJsonSchema', got: {json}"
        );
        assert!(
            !json.contains("\"parameters\":"),
            "legacy 'parameters' field should not appear in output, got: {json}"
        );
        assert!(
            json.contains("additionalProperties"),
            "additionalProperties should be preserved in parametersJsonSchema, got: {json}"
        );
    }

    #[test]
    fn test_batch_to_provider_uses_parameters_json_schema() {
        let tools = vec![
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "bash".to_string(),
                    description: "Run".to_string(),
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": { "command": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
            },
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "read".to_string(),
                    description: "Read".to_string(),
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" },
                            "options": {
                                "type": "object",
                                "properties": {
                                    "encoding": { "type": "string" }
                                },
                                "additionalProperties": false
                            }
                        },
                        "additionalProperties": false
                    }),
                },
            },
        ];

        let gemini_tools: Vec<GeminiTool> = tools.to_provider().unwrap();
        let serialized = serde_json::to_string(&gemini_tools).unwrap();

        assert!(
            serialized.contains("parametersJsonSchema"),
            "should use parametersJsonSchema, got: {serialized}"
        );
        assert!(
            serialized.contains("additionalProperties"),
            "additionalProperties should be preserved, got: {serialized}"
        );
    }

    #[test]
    fn test_from_provider_prefers_parameters_json_schema_over_parameters() {
        let tool_with_both = GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: "search".to_string(),
                description: Some("Search".to_string()),
                parameters_json_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } }
                })),
                parameters: Some(serde_json::json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } }
                })),
            }],
        };

        let schema: ToolSchema = ToolSchema::from_provider(tool_with_both).unwrap();
        // Should pick parametersJsonSchema
        assert_eq!(
            schema.function.parameters["properties"]["q"]["type"],
            "string"
        );
    }

    #[test]
    fn test_from_provider_falls_back_to_legacy_parameters() {
        let legacy_tool = GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: "legacy".to_string(),
                description: Some("Legacy tool".to_string()),
                parameters_json_schema: None,
                parameters: Some(serde_json::json!({
                    "type": "object",
                    "properties": { "x": { "type": "integer" } }
                })),
            }],
        };

        let schema: ToolSchema = ToolSchema::from_provider(legacy_tool).unwrap();
        assert_eq!(
            schema.function.parameters["properties"]["x"]["type"],
            "integer"
        );
    }

    #[test]
    fn test_from_provider_handles_empty_parameters() {
        let tool_no_params = GeminiTool {
            function_declarations: vec![GeminiFunctionDeclaration {
                name: "ping".to_string(),
                description: Some("Ping".to_string()),
                parameters_json_schema: None,
                parameters: None,
            }],
        };

        let schema: ToolSchema = ToolSchema::from_provider(tool_no_params).unwrap();
        assert_eq!(schema.function.name, "ping");
        assert!(schema.function.parameters.is_null());
    }

    #[test]
    fn test_tool_roundtrip_preserves_additional_properties() {
        let tool = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "edit".to_string(),
                description: "Edit a file".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["path"],
                    "additionalProperties": false
                }),
            },
        };

        // Internal → Gemini
        let gemini: GeminiTool = tool.to_provider().unwrap();

        // Gemini → Internal
        let roundtrip: ToolSchema = ToolSchema::from_provider(gemini).unwrap();

        assert_eq!(roundtrip.function.name, "edit");
        assert_eq!(roundtrip.function.parameters["additionalProperties"], false);
        assert_eq!(
            roundtrip.function.parameters["required"],
            serde_json::json!(["path"])
        );
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
