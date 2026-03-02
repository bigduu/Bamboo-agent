//! Tool execution API controller.
//!
//! This module provides HTTP endpoints for directly executing agent tools
//! without running the full agent loop. This is useful for testing tools
//! or using Bamboo's built-in utilities standalone.
//!
//! # Endpoint
//!
//! `POST /api/v1/tools/execute`
//!
//! # Available Tools
//!
//! - **File Operations**: `read_file`, `write_file`, `list_directory`, `file_exists`, `get_file_info`
//! - **Git Operations**: `git_status`, `git_diff`
//! - **Command Execution**: `execute_command`
//! - **Workspace**: `set_workspace`, `get_current_dir`
//!
//! # Example
//!
//! ```bash
//! curl -X POST http://localhost:9562/api/v1/tools/execute \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "tool_name": "read_file",
//!     "parameters": [
//!       {"name": "path", "value": "/path/to/file.txt"}
//!     ]
//!   }'
//! ```

use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent::core::tools::{FunctionCall, ToolCall};
use crate::agent::tools::normalize_tool_ref;

use crate::server::app_state::AppState;
use crate::server::error::AppError;

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
    /// Tool name to execute
    pub tool_name: String,
    /// Tool parameters as key-value pairs
    pub parameters: Vec<ToolParameter>,
}

/// Single tool parameter.
#[derive(Deserialize)]
pub struct ToolParameter {
    /// Parameter name
    pub name: String,
    /// Parameter value (string, will be parsed as JSON if possible)
    pub value: String,
}

/// Response wrapper for tool execution result.
#[derive(Serialize)]
pub struct ToolExecutionResponse {
    /// JSON-encoded result payload
    pub result: String,
}

/// Internal tool execution result payload.
#[derive(Serialize)]
pub struct ToolExecutionResultPayload {
    /// Name of the executed tool
    pub tool_name: String,
    /// Tool execution result (usually JSON string)
    pub result: String,
    /// Display preference hint (always "Default")
    pub display_preference: String,
}

/// Execute a tool directly without agent loop.
///
/// This endpoint allows direct execution of Bamboo's built-in tools
/// for testing or standalone use cases.
///
/// # HTTP Method
///
/// `POST /api/v1/tools/execute`
///
/// # Request Body
///
/// JSON-encoded [`ToolExecutionRequest`]
///
/// # Response
///
/// - `200 OK` - Tool executed successfully, returns [`ToolExecutionResponse`]
/// - `400 Bad Request` - Invalid request or parameters
/// - `404 Not Found` - Tool not found
/// - `500 Internal Server Error` - Tool execution failed
///
/// # Available Tools
///
/// - `read_file` - Read file contents
/// - `write_file` - Write file contents
/// - `execute_command` - Execute shell command
/// - `list_directory` - List directory contents
/// - `file_exists` - Check if file exists
/// - `get_file_info` - Get file metadata
/// - `git_status` - Get git repository status
/// - `git_diff` - Get git diff
/// - And more...
///
/// # Parameter Parsing
///
/// Parameters values are automatically parsed as JSON if possible.
/// If parsing fails, they're treated as plain strings.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:9562/api/v1/tools/execute \
///   -H "Content-Type: application/json" \
///   -d '{
///     "tool_name": "read_file",
///     "parameters": [
///       {"name": "path", "value": "/path/to/file.txt"}
///     ]
///   }'
/// ```
pub async fn execute_tool(
    app_state: web::Data<AppState>,
    payload: web::Json<ToolExecutionRequest>,
) -> Result<HttpResponse, AppError> {
    let request = payload.into_inner();
    let normalized = normalize_tool_ref(&request.tool_name)
        .ok_or_else(|| AppError::ToolNotFound(request.tool_name.clone()))?;

    let mut args = serde_json::Map::new();
    for param in request.parameters {
        let parsed = serde_json::from_str(&param.value).unwrap_or(Value::String(param.value));
        args.insert(param.name, parsed);
    }

    let call = ToolCall {
        id: "tool_call".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: normalized,
            arguments: serde_json::to_string(&args).map_err(AppError::SerializationError)?,
        },
    };

    let result = app_state
        .tools
        .execute(&call)
        .await
        .map_err(|err| AppError::ToolExecutionError(err.to_string()))?;

    let result_payload = ToolExecutionResultPayload {
        tool_name: request.tool_name,
        result: result.result,
        display_preference: "Default".to_string(),
    };
    let response = ToolExecutionResponse {
        result: serde_json::to_string(&result_payload).map_err(AppError::SerializationError)?,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Configure tool execution routes.
///
/// This function registers the tool execution endpoint with the Actix-web
/// service configuration.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/tools/execute", web::post().to(execute_tool));
}
