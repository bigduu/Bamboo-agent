//! Core type definitions for the tool system.

use serde::{Deserialize, Serialize};

/// An image produced by a tool (e.g. an MCP `screenshot`), carried alongside the
/// textual result so it can be forwarded to vision-capable models. `data` is raw
/// base64 (no `data:` prefix).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ToolResultImage {
    pub mime_type: String,
    pub data: String,
}

/// Represents the result of executing a tool.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolResult {
    pub success: bool,
    pub result: String,
    pub display_preference: Option<String>,
    /// Images returned by the tool (e.g. screenshots). Empty for the vast
    /// majority of tools; populated only by image-producing tools like MCP
    /// `screenshot`. Defaults to empty so existing serialized results load.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ToolResultImage>,
}

impl ToolResult {
    /// Construct a text-only result (no images). Convenience for the common case.
    pub fn text(success: bool, result: impl Into<String>) -> Self {
        Self {
            success,
            result: result.into(),
            display_preference: None,
            images: Vec::new(),
        }
    }
}

/// Schema definition for a tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub function: FunctionSchema,
}

/// Function metadata for tool schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSchema {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}
