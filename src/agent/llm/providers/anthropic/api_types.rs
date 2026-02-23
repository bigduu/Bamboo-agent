//! Anthropic API types for request/response handling.
//!
//! These types represent the Anthropic Messages API format and are used
//! for converting between Anthropic and OpenAI-compatible formats.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Anthropic Messages API request format
#[derive(Deserialize)]
pub struct AnthropicMessagesRequest {
    pub model: String,
    pub messages: Vec<AnthropicMessage>,
    #[serde(default)]
    pub system: Option<AnthropicSystem>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub top_k: Option<u32>,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub tools: Option<Vec<AnthropicTool>>,
    #[serde(default)]
    pub tool_choice: Option<AnthropicToolChoice>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Anthropic message format
#[derive(Deserialize)]
pub struct AnthropicMessage {
    pub role: AnthropicRole,
    pub content: AnthropicContent,
}

/// Anthropic role types
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicRole {
    User,
    Assistant,
    System,
}

/// Anthropic content format (text or blocks)
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicContent {
    Text(String),
    Blocks(Vec<AnthropicContentBlock>),
}

/// Anthropic content block types
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: Value,
    },
}

/// Anthropic system message format
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicSystem {
    Text(String),
    Blocks(Vec<AnthropicSystemBlock>),
}

/// Anthropic system block
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicSystemBlock {
    Text { text: String },
}

/// Anthropic tool definition
#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicTool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub input_schema: Value,
}

/// Anthropic tool choice options
#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnthropicToolChoice {
    String(String),
    Tool {
        #[serde(rename = "type")]
        tool_type: String,
        name: String,
    },
}

/// Anthropic legacy complete API request format
#[derive(Deserialize)]
pub struct AnthropicCompleteRequest {
    pub model: String,
    pub prompt: String,
    pub max_tokens_to_sample: u32,
    #[serde(default)]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub top_k: Option<u32>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Anthropic Messages API response format
#[derive(Serialize)]
pub struct AnthropicMessagesResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub response_type: String,
    pub role: String,
    pub content: Vec<AnthropicResponseContentBlock>,
    pub model: String,
    pub stop_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
    pub usage: AnthropicUsage,
}

/// Anthropic response content block
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicResponseContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
}

/// Anthropic token usage
#[derive(Serialize)]
pub struct AnthropicUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Anthropic legacy complete API response format
#[derive(Serialize)]
pub struct AnthropicCompleteResponse {
    #[serde(rename = "type")]
    pub response_type: String,
    pub completion: String,
    pub model: String,
    pub stop_reason: String,
}

/// Anthropic error response envelope
#[derive(Serialize)]
pub struct AnthropicErrorEnvelope {
    #[serde(rename = "type")]
    pub error_type: String,
    pub error: AnthropicErrorDetail,
}

/// Anthropic error detail
#[derive(Serialize)]
pub struct AnthropicErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

/// Anthropic model list response
#[derive(Serialize)]
pub struct AnthropicListModelsResponse {
    pub data: Vec<AnthropicModel>,
    pub has_more: bool,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
}

/// Anthropic model info
#[derive(Serialize)]
pub struct AnthropicModel {
    #[serde(rename = "type")]
    pub model_type: String,
    pub id: String,
    pub display_name: String,
    pub created_at: String,
}
