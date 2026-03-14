use serde::{Deserialize, Serialize};

/// Request payload for tool execution.
///
/// # Fields
///
/// * `tool_name` - Name of the tool to execute (e.g., "read_file", "execute_command")
/// * `parameters` - List of tool parameters as key-value pairs
///
/// # Example
///
/// ```json
/// {
///   "tool_name": "read_file",
///   "parameters": [
///     {"name": "path", "value": "/path/to/file"}
///   ]
/// }
/// ```
#[derive(Deserialize)]
pub struct ToolExecutionRequest {
    /// Tool name to execute.
    pub tool_name: String,
    /// Tool parameters as key-value pairs.
    pub parameters: Vec<ToolParameter>,
    /// Optional session context for tools that require read/write safety gates.
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Single tool parameter.
#[derive(Deserialize)]
pub struct ToolParameter {
    /// Parameter name.
    pub name: String,
    /// Parameter value (string, will be parsed as JSON if possible).
    pub value: String,
}

/// Response wrapper for tool execution result.
#[derive(Serialize)]
pub struct ToolExecutionResponse {
    /// JSON-encoded result payload.
    pub result: String,
}

/// Internal tool execution result payload.
#[derive(Serialize)]
pub struct ToolExecutionResultPayload {
    /// Name of the executed tool.
    pub tool_name: String,
    /// Tool execution result (usually JSON string).
    pub result: String,
    /// Display preference hint.
    pub display_preference: String,
}
