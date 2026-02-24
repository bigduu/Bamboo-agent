//! Core type definitions for the tool system.
//!
//! This module defines the fundamental types used throughout Bamboo's tool system,
//! including tool calls, results, and schema definitions.
//!
//! # Types
//!
//! - [`ToolCall`] - A request to execute a tool
//! - [`FunctionCall`] - Function invocation details within a tool call
//! - [`ToolResult`] - The outcome of executing a tool
//! - [`ToolSchema`] - Schema definition for a tool
//! - [`FunctionSchema`] - Function metadata including parameters

use serde::{Deserialize, Serialize};

/// Represents a tool call request from the LLM.
///
/// A `ToolCall` contains the information needed to invoke a tool,
/// including a unique identifier and the function to call.
///
/// # Fields
///
/// * `id` - Unique identifier for this tool call
/// * `tool_type` - Type of tool (usually "function")
/// * `function` - The function call details
///
/// # Example
///
/// ```rust,ignore
/// let call = ToolCall {
///     id: "call_123".to_string(),
///     tool_type: "function".to_string(),
///     function: FunctionCall {
///         name: "read_file".to_string(),
///         arguments: r#"{"path": "/src/main.rs"}"#.to_string(),
///     },
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    /// Unique identifier for this tool call
    pub id: String,
    /// Tool type (typically "function")
    #[serde(rename = "type")]
    pub tool_type: String,
    /// The function to invoke
    pub function: FunctionCall,
}

/// Represents a function call within a tool invocation.
///
/// Contains the function name and JSON-encoded arguments.
///
/// # Fields
///
/// * `name` - Name of the function to call
/// * `arguments` - JSON string of function arguments
///
/// # Example
///
/// ```rust,ignore
/// let function = FunctionCall {
///     name: "write_file".to_string(),
///     arguments: r#"{"path": "test.txt", "content": "Hello"}"#.to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionCall {
    /// Name of the function to invoke
    pub name: String,
    /// JSON-encoded function arguments
    pub arguments: String,
}

/// Represents the result of executing a tool.
///
/// Contains the execution outcome, result data, and optional display preferences.
///
/// # Fields
///
/// * `success` - Whether the tool execution succeeded
/// * `result` - The result data (usually JSON or text)
/// * `display_preference` - Optional hint for how to display the result
///
/// # Example
///
/// ```rust,ignore
/// let result = ToolResult {
///     success: true,
///     result: "File contents here".to_string(),
///     display_preference: Some("code".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    /// Whether execution succeeded
    pub success: bool,
    /// Result data or error message
    pub result: String,
    /// Optional display hint (e.g., "code", "text", "json")
    pub display_preference: Option<String>,
}

/// Schema definition for a tool.
///
/// Describes a tool's interface for LLM function calling.
///
/// # Fields
///
/// * `schema_type` - Type of schema (usually "function")
/// * `function` - Function metadata and parameters
///
/// # Example
///
/// ```rust,ignore
/// let schema = ToolSchema {
///     schema_type: "function".to_string(),
///     function: FunctionSchema {
///         name: "read_file".to_string(),
///         description: "Read file contents".to_string(),
///         parameters: json!({
///             "type": "object",
///             "properties": {
///                 "path": {"type": "string"}
///             }
///         }),
///     },
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
    /// Schema type (typically "function")
    #[serde(rename = "type")]
    pub schema_type: String,
    /// Function definition
    pub function: FunctionSchema,
}

/// Function metadata for tool schema definition.
///
/// Describes a function's name, purpose, and parameters for LLM consumption.
///
/// # Fields
///
/// * `name` - Function name
/// * `description` - Human-readable description
/// * `parameters` - JSON Schema for function parameters
///
/// # Example
///
/// ```rust,ignore
/// let function = FunctionSchema {
///     name: "execute_command".to_string(),
///     description: "Execute a shell command".to_string(),
///     parameters: json!({
///         "type": "object",
///         "properties": {
///             "command": {"type": "string", "description": "Command to run"}
///         },
///         "required": ["command"]
///     }),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSchema {
    /// Function name
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// JSON Schema for parameters
    pub parameters: serde_json::Value,
}
