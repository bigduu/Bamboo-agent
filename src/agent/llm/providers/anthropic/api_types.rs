//! Anthropic API types for request/response handling.
//!
//! These types represent the Anthropic Messages API format and are used
//! for converting between Anthropic and OpenAI-compatible formats.
//!
//! # Overview
//!
//! This module provides type definitions for interacting with Anthropic's API,
//! including both the modern Messages API and the legacy Complete API.
//!
//! # API Types
//!
//! - [`AnthropicMessagesRequest`] / [`AnthropicMessagesResponse`] - Modern Messages API
//! - [`AnthropicCompleteRequest`] / [`AnthropicCompleteResponse`] - Legacy Complete API
//! - [`AnthropicTool`] / [`AnthropicToolChoice`] - Tool/function calling support
//! - [`AnthropicErrorEnvelope`] / [`AnthropicErrorDetail`] - Error handling

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Request format for the Anthropic Messages API.
///
/// This struct represents the complete request structure for Anthropic's
/// modern Messages API, which supports conversational interactions with
/// Claude models.
///
/// # Fields
///
/// - `model`: The model identifier (e.g., "claude-3-opus-20240229")
/// - `messages`: The conversation history as a sequence of messages
/// - `system`: Optional system prompt to set the assistant's behavior
/// - `max_tokens`: Maximum number of tokens to generate
/// - `temperature`: Sampling temperature for randomness (0.0-1.0)
/// - `top_p`: Nucleus sampling parameter
/// - `top_k`: Top-k sampling parameter
/// - `stop_sequences`: Custom sequences that halt generation
/// - `stream`: Whether to stream the response
/// - `tools`: Available tools for function calling
/// - `tool_choice`: How to select which tool to use
/// - `extra`: Additional parameters not explicitly defined
///
/// # Example
///
/// ```rust
/// use bamboo::agent::llm::providers::anthropic::api_types::*;
///
/// let request = AnthropicMessagesRequest {
///     model: "claude-3-opus-20240229".to_string(),
///     messages: vec![/* ... */],
///     max_tokens: Some(1024),
///     temperature: Some(0.7),
///     // ... other fields
///     ..Default::default()
/// };
/// ```
#[derive(Deserialize)]
pub struct AnthropicMessagesRequest {
    /// The model identifier to use for generation.
    ///
    /// Examples: "claude-3-opus-20240229", "claude-3-sonnet-20240229"
    pub model: String,

    /// The conversation history as a sequence of messages.
    ///
    /// Messages alternate between user and assistant roles, starting with user.
    pub messages: Vec<AnthropicMessage>,

    /// Optional system prompt to set the assistant's behavior and context.
    ///
    /// Can be either a simple string or structured blocks.
    #[serde(default)]
    pub system: Option<AnthropicSystem>,

    /// Maximum number of tokens to generate in the response.
    ///
    /// If not specified, the API will use a default value based on the model.
    #[serde(default)]
    pub max_tokens: Option<u32>,

    /// Sampling temperature for controlling randomness (0.0 to 1.0).
    ///
    /// Higher values make output more random, lower values make it more deterministic.
    /// Recommended: 0.7 for creative tasks, 0.3 for analytical tasks.
    #[serde(default)]
    pub temperature: Option<f32>,

    /// Nucleus sampling parameter (0.0 to 1.0).
    ///
    /// Controls diversity via nucleus sampling. The model considers tokens
    /// with top_p probability mass.
    #[serde(default)]
    pub top_p: Option<f32>,

    /// Top-k sampling parameter.
    ///
    /// Limits the model to only consider the top k most likely tokens.
    #[serde(default)]
    pub top_k: Option<u32>,

    /// Custom sequences that cause the model to stop generating.
    ///
    /// When any of these sequences are encountered, generation stops.
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,

    /// Whether to stream the response incrementally.
    ///
    /// When true, the API returns tokens as they're generated.
    #[serde(default)]
    pub stream: Option<bool>,

    /// Available tools/functions that the model can call.
    ///
    /// Each tool defines a name, description, and input schema.
    #[serde(default)]
    pub tools: Option<Vec<AnthropicTool>>,

    /// Strategy for choosing which tool to use (if any).
    ///
    /// Can be "auto", "any", or a specific tool name.
    #[serde(default)]
    pub tool_choice: Option<AnthropicToolChoice>,

    /// Additional parameters not explicitly defined in this struct.
    ///
    /// Allows for forward compatibility with new API parameters.
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// A single message in an Anthropic conversation.
///
/// Each message has a role (user, assistant, or system) and content
/// that can be either simple text or structured blocks.
///
/// # Roles
///
/// - `User`: Input from the human user
/// - `Assistant`: Responses from Claude
/// - `System`: System-level instructions (typically in the system field instead)
///
/// # Content Format
///
/// Content can be either a simple text string or an array of content blocks,
/// allowing for rich multi-part messages including text, tool calls, and tool results.
#[derive(Deserialize)]
pub struct AnthropicMessage {
    /// The role of the message author.
    ///
    /// Indicates who sent this message in the conversation.
    pub role: AnthropicRole,

    /// The content of the message.
    ///
    /// Can be either plain text or structured content blocks.
    pub content: AnthropicContent,
}

/// Role types for Anthropic messages.
///
/// Specifies the author of a message in the conversation.
/// Roles are serialized as lowercase strings in the API.
///
/// # Variants
///
/// - `User`: Messages from the human user
/// - `Assistant`: Messages from Claude
/// - `System`: System-level instructions
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicRole {
    /// Message from the human user.
    User,

    /// Message from the Claude assistant.
    Assistant,

    /// System-level instruction message.
    System,
}

/// Content format for Anthropic messages.
///
/// Anthropic messages can contain either simple text or structured blocks.
/// This enum allows deserialization to handle both formats transparently.
///
/// # Variants
///
/// - `Text`: Simple text string (e.g., "Hello, world!")
/// - `Blocks`: Array of structured content blocks for complex messages
///
/// # Deserialization
///
/// Uses `#[serde(untagged)]` to automatically detect which variant to use
/// based on whether the input is a string or an array.
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicContent {
    /// Simple text content.
    ///
    /// Used for basic text-only messages.
    Text(String),

    /// Structured content blocks.
    ///
    /// Used for multi-part messages including text, tool calls, and tool results.
    Blocks(Vec<AnthropicContentBlock>),
}

/// Content block types in Anthropic messages.
///
/// Represents different types of content that can appear in a message,
/// including text, tool use requests, and tool execution results.
///
/// # Block Types
///
/// - `Text`: Regular text content
/// - `ToolUse`: Request to invoke a tool with specific parameters
/// - `ToolResult`: Result from a previously invoked tool
///
/// # Serialization
///
/// Uses `#[serde(tag = "type")]` to include a "type" field in the JSON
/// to identify which variant is being used.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicContentBlock {
    /// Text content block.
    Text {
        /// The text content of the block.
        text: String,
    },

    /// Tool invocation request block.
    ///
    /// Represents the model's request to call a specific tool with given parameters.
    ToolUse {
        /// Unique identifier for this tool use instance.
        id: String,

        /// Name of the tool to invoke.
        name: String,

        /// Input parameters for the tool call, as JSON.
        input: Value,
    },

    /// Tool execution result block.
    ///
    /// Contains the output from a previously invoked tool.
    ToolResult {
        /// ID of the tool use request this result corresponds to.
        tool_use_id: String,

        /// The result of the tool execution, as JSON.
        content: Value,
    },
}

/// System message format for setting assistant behavior.
///
/// System prompts can be provided as either a simple text string
/// or as structured blocks for more complex instructions.
///
/// # Variants
///
/// - `Text`: Simple text system prompt
/// - `Blocks`: Structured system prompt blocks
///
/// # Usage
///
/// System messages are typically used to:
/// - Define the assistant's role and behavior
/// - Provide context and instructions
/// - Set output format requirements
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicSystem {
    /// Simple text system prompt.
    Text(String),

    /// Structured system prompt blocks.
    Blocks(Vec<AnthropicSystemBlock>),
}

/// System prompt block type.
///
/// Currently supports only text blocks, but structured to allow
/// future expansion for other system block types.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicSystemBlock {
    /// Text content in the system prompt.
    Text {
        /// The text content of the system block.
        text: String,
    },
}

/// Tool definition for Anthropic function calling.
///
/// Defines a tool (function) that the model can choose to call during generation.
/// Tools extend the model's capabilities by allowing it to execute code,
/// query APIs, or perform other actions.
///
/// # Fields
///
/// - `name`: Unique identifier for the tool
/// - `description`: Human-readable explanation of what the tool does
/// - `input_schema`: JSON Schema defining the tool's parameters
///
/// # Example
///
/// ```json
/// {
///   "name": "get_weather",
///   "description": "Get the current weather in a location",
///   "input_schema": {
///     "type": "object",
///     "properties": {
///       "location": {"type": "string"}
///     },
///     "required": ["location"]
///   }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicTool {
    /// The name of the tool.
    ///
    /// Must be unique within the tools array. Used by the model to reference this tool.
    pub name: String,

    /// Human-readable description of what the tool does.
    ///
    /// Helps the model understand when and how to use this tool.
    #[serde(default)]
    pub description: Option<String>,

    /// JSON Schema defining the tool's input parameters.
    ///
    /// Specifies the expected structure, types, and constraints for tool inputs.
    pub input_schema: Value,
}

/// Tool selection strategy for Anthropic function calling.
///
/// Controls how the model chooses which tool (if any) to call.
///
/// # Variants
///
/// - `String`: Simple string value like "auto" or "any"
/// - `Tool`: Specific tool selection with name
///
/// # Options
///
/// When using the `String` variant:
/// - `"auto"`: Model decides whether to use a tool
/// - `"any"`: Model must use one of the provided tools
/// - `"none"`: Model should not use any tools
///
/// When using the `Tool` variant:
/// - Forces the model to use a specific tool by name
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicToolChoice {
    /// Simple string-based tool choice strategy.
    ///
    /// Values: "auto", "any", or "none"
    String(String),

    /// Specific tool selection.
    ///
    /// Forces the model to use the specified tool.
    Tool {
        /// Must be "tool" to indicate specific tool selection.
        #[serde(rename = "type")]
        tool_type: String,

        /// Name of the tool to use.
        name: String,
    },
}

/// Request format for the legacy Anthropic Complete API.
///
/// This struct represents the request structure for Anthropic's legacy
/// text completion API. This API is deprecated in favor of the Messages API
/// but is still supported for backward compatibility.
///
/// # Differences from Messages API
///
/// - Uses a single `prompt` string instead of message array
/// - Uses `max_tokens_to_sample` instead of `max_tokens`
/// - Does not support tool calling or multi-turn conversations
///
/// # Fields
///
/// - `model`: The model identifier
/// - `prompt`: The text prompt to complete
/// - `max_tokens_to_sample`: Maximum tokens to generate
/// - `stop_sequences`: Sequences that halt generation
/// - `temperature`, `top_p`, `top_k`: Sampling parameters
/// - `stream`: Whether to stream the response
/// - `extra`: Additional parameters
#[derive(Deserialize)]
pub struct AnthropicCompleteRequest {
    /// The model identifier to use for completion.
    pub model: String,

    /// The text prompt to complete.
    ///
    /// The model will generate text that continues from this prompt.
    pub prompt: String,

    /// Maximum number of tokens to generate in the completion.
    pub max_tokens_to_sample: u32,

    /// Custom sequences that cause the model to stop generating.
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,

    /// Sampling temperature for controlling randomness (0.0 to 1.0).
    #[serde(default)]
    pub temperature: Option<f32>,

    /// Nucleus sampling parameter (0.0 to 1.0).
    #[serde(default)]
    pub top_p: Option<f32>,

    /// Top-k sampling parameter.
    #[serde(default)]
    pub top_k: Option<u32>,

    /// Whether to stream the response incrementally.
    #[serde(default)]
    pub stream: Option<bool>,

    /// Additional parameters not explicitly defined.
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Response format for the Anthropic Messages API.
///
/// Represents the complete response from the Messages API, including
/// generated content, metadata, and token usage information.
///
/// # Fields
///
/// - `id`: Unique identifier for this response
/// - `response_type`: Always "message" for Messages API responses
/// - `role`: Always "assistant" for responses
/// - `content`: Array of content blocks (text and/or tool use)
/// - `model`: The model that generated this response
/// - `stop_reason`: Why generation stopped (e.g., "end_turn", "max_tokens")
/// - `stop_sequence`: The stop sequence that halted generation, if any
/// - `usage`: Token usage statistics
///
/// # Content Blocks
///
/// The response can contain multiple content blocks:
/// - Text blocks with generated text
/// - Tool use blocks with function call requests
#[derive(Serialize)]
pub struct AnthropicMessagesResponse {
    /// Unique identifier for this response.
    ///
    /// Can be used for logging and debugging.
    pub id: String,

    /// The type of response (always "message").
    #[serde(rename = "type")]
    pub response_type: String,

    /// The role of the response author (always "assistant").
    pub role: String,

    /// The generated content as an array of blocks.
    ///
    /// May contain text and/or tool use blocks.
    pub content: Vec<AnthropicResponseContentBlock>,

    /// The model identifier used for generation.
    pub model: String,

    /// The reason why generation stopped.
    ///
    /// Common values:
    /// - "end_turn": Natural end of response
    /// - "max_tokens": Hit token limit
    /// - "stop_sequence": Encountered a stop sequence
    /// - "tool_use": Model is requesting to call a tool
    pub stop_reason: String,

    /// The stop sequence that ended generation, if applicable.
    ///
    /// Only present if generation stopped due to a stop sequence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,

    /// Token usage statistics for this request.
    pub usage: AnthropicUsage,
}

/// Content block types in Anthropic responses.
///
/// Represents the different types of content that can appear in a
/// response from the Messages API.
///
/// # Block Types
///
/// - `Text`: Generated text content
/// - `ToolUse`: Request to invoke a tool
///
/// # Serialization
///
/// Uses `#[serde(tag = "type")]` to include a "type" field for identification.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicResponseContentBlock {
    /// Text content block in the response.
    Text {
        /// The generated text content.
        text: String,
    },

    /// Tool use request in the response.
    ///
    /// Indicates that the model wants to invoke a tool with specific parameters.
    ToolUse {
        /// Unique identifier for this tool use instance.
        ///
        /// Used to correlate with tool results in subsequent messages.
        id: String,

        /// Name of the tool to invoke.
        name: String,

        /// Input parameters for the tool call, as JSON.
        input: Value,
    },
}

/// Token usage statistics for an Anthropic API request.
///
/// Provides information about the number of tokens consumed by the request,
/// useful for monitoring costs and staying within limits.
///
/// # Fields
///
/// - `input_tokens`: Number of tokens in the request (prompt + messages)
/// - `output_tokens`: Number of tokens in the response
///
/// # Cost Calculation
///
/// Token costs vary by model. Check Anthropic's pricing documentation
/// for current rates per 1K tokens.
#[derive(Serialize)]
pub struct AnthropicUsage {
    /// Number of tokens in the request.
    ///
    /// Includes all input: system prompt, messages, and tool definitions.
    pub input_tokens: u32,

    /// Number of tokens in the response.
    ///
    /// Includes all generated content and tool use blocks.
    pub output_tokens: u32,
}

/// Response format for the legacy Anthropic Complete API.
///
/// Represents the response from the legacy text completion API.
/// This API is deprecated in favor of the Messages API.
///
/// # Differences from Messages API
///
/// - Returns a single `completion` string instead of content blocks
/// - Does not support tool calling
/// - Simpler structure for basic text completion
///
/// # Fields
///
/// - `response_type`: Always "completion"
/// - `completion`: The generated text completion
/// - `model`: The model that generated this completion
/// - `stop_reason`: Why generation stopped
#[derive(Serialize)]
pub struct AnthropicCompleteResponse {
    /// The type of response (always "completion").
    #[serde(rename = "type")]
    pub response_type: String,

    /// The generated text completion.
    ///
    /// This is the full text that continues from the input prompt.
    pub completion: String,

    /// The model identifier used for generation.
    pub model: String,

    /// The reason why generation stopped.
    ///
    /// Common values: "stop_sequence", "max_tokens"
    pub stop_reason: String,
}

/// Error response envelope from the Anthropic API.
///
/// Wraps error details in a structured format when the API encounters an error.
/// This is the top-level error structure returned by Anthropic's API.
///
/// # Structure
///
/// The error response has a "type" field indicating it's an error,
/// and an "error" object containing the detailed error information.
///
/// # Common Error Types
///
/// - `invalid_request_error`: Malformed or invalid request
/// - `authentication_error`: Invalid or missing API key
/// - `permission_error`: Insufficient permissions
/// - `not_found_error`: Resource not found
/// - `rate_limit_error`: Too many requests
/// - `api_error`: Internal server error
/// - `overloaded_error`: Service temporarily overloaded
#[derive(Serialize)]
pub struct AnthropicErrorEnvelope {
    /// The type of error envelope (typically "error").
    #[serde(rename = "type")]
    pub error_type: String,

    /// Detailed error information.
    pub error: AnthropicErrorDetail,
}

/// Detailed error information from the Anthropic API.
///
/// Contains specific error type and human-readable message explaining
/// what went wrong with the request.
///
/// # Fields
///
/// - `error_type`: Machine-readable error code
/// - `message`: Human-readable error description
///
/// # Example
///
/// ```json
/// {
///   "type": "invalid_request_error",
///   "message": "max_tokens is required"
/// }
/// ```
#[derive(Serialize)]
pub struct AnthropicErrorDetail {
    /// Machine-readable error type code.
    ///
    /// Can be used for programmatic error handling.
    #[serde(rename = "type")]
    pub error_type: String,

    /// Human-readable error message.
    ///
    /// Provides details about what went wrong.
    pub message: String,
}

/// Response from the Anthropic models list endpoint.
///
/// Contains a paginated list of available models that can be used
/// with the Anthropic API.
///
/// # Pagination
///
/// - `has_more`: Indicates if there are more models available
/// - `first_id`: ID of the first model in the list
/// - `last_id`: ID of the last model in the list
///
/// # Usage
///
/// Use this to discover available models and their IDs for use in requests.
#[derive(Serialize)]
pub struct AnthropicListModelsResponse {
    /// Array of available models.
    pub data: Vec<AnthropicModel>,

    /// Whether more models are available beyond this page.
    pub has_more: bool,

    /// ID of the first model in this page.
    pub first_id: Option<String>,

    /// ID of the last model in this page.
    pub last_id: Option<String>,
}

/// Information about an Anthropic model.
///
/// Contains metadata about a specific model available through the API.
///
/// # Fields
///
/// - `model_type`: Always "model"
/// - `id`: The model identifier to use in API requests
/// - `display_name`: Human-readable model name
/// - `created_at`: When the model was created
///
/// # Example
///
/// ```json
/// {
///   "type": "model",
///   "id": "claude-3-opus-20240229",
///   "display_name": "Claude 3 Opus",
///   "created_at": "2024-02-29T00:00:00Z"
/// }
/// ```
#[derive(Serialize)]
pub struct AnthropicModel {
    /// The type of resource (always "model").
    #[serde(rename = "type")]
    pub model_type: String,

    /// The model identifier.
    ///
    /// Use this value in the `model` field of API requests.
    pub id: String,

    /// Human-readable display name for the model.
    ///
    /// Examples: "Claude 3 Opus", "Claude 3 Sonnet"
    pub display_name: String,

    /// ISO 8601 timestamp of when the model was created.
    pub created_at: String,
}
