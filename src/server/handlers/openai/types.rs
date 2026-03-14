use crate::agent::llm::api::models::Tool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize)]
pub(super) struct ListModelsResponse {
    pub(super) success: bool,
    pub(super) object: String,
    pub(super) data: Vec<Model>,
}

#[derive(Serialize)]
pub(super) struct Model {
    pub(super) id: String,
    pub(super) object: String,
    pub(super) created: u64,
    pub(super) owned_by: String,
    pub(super) supported_endpoint_types: Vec<String>,
}

// ============================================================================
// OpenAI Responses API (v1/responses) - Minimal compatibility layer
// ============================================================================

/// OpenAI Responses API request (subset).
///
/// We intentionally accept a subset of the spec to support common SDK usage:
/// - `model`
/// - `input` (string or array of message-like objects)
/// - `instructions` (mapped to a system message)
/// - `tools` (OpenAI tool schema; reuses existing OpenAI-compatible tool model)
/// - `stream`
/// - `max_output_tokens` (mapped to provider max tokens)
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ResponsesCreateRequest {
    pub(super) model: String,
    #[serde(default)]
    pub(super) input: serde_json::Value,
    #[serde(default)]
    pub(super) instructions: Option<String>,
    #[serde(default)]
    pub(super) tools: Option<Vec<Tool>>,
    #[serde(default)]
    pub(super) stream: Option<bool>,
    #[serde(default)]
    pub(super) max_output_tokens: Option<u32>,
    #[serde(flatten)]
    pub(super) parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_tokens: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesTextContent {
    #[serde(rename = "type")]
    pub(super) content_type: String, // "output_text"
    pub(super) text: String,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesMessageOutputItem {
    pub(super) id: String,
    #[serde(rename = "type")]
    pub(super) item_type: String, // "message"
    pub(super) role: String, // "assistant"
    pub(super) content: Vec<ResponsesTextContent>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesFunctionCallOutputItem {
    pub(super) id: String,
    #[serde(rename = "type")]
    pub(super) item_type: String, // "function_call"
    pub(super) call_id: String,
    pub(super) name: String,
    pub(super) arguments: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub(super) enum ResponsesOutputItem {
    Message(ResponsesMessageOutputItem),
    FunctionCall(ResponsesFunctionCallOutputItem),
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesCreateResponse {
    pub(super) id: String,
    pub(super) object: String, // "response"
    pub(super) created_at: u64,
    pub(super) model: String,
    pub(super) status: String, // "completed" | "in_progress"
    pub(super) output: Vec<ResponsesOutputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) usage: Option<ResponsesUsage>,
}

#[derive(Debug, Serialize)]
pub(super) struct ResponsesStreamEvent<T> {
    #[serde(rename = "type")]
    pub(super) event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delta: Option<String>,
}
