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

mod models;
mod request;
mod response;

#[cfg(test)]
mod tests;

use actix_web::{web, HttpResponse};

use bamboo_application_agent::tools::ToolExecutionContext;

use crate::app_state::AppState;
use crate::error::AppError;

pub use models::{
    ToolExecutionRequest, ToolExecutionResponse, ToolExecutionResultPayload, ToolParameter,
};

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
    let ToolExecutionRequest {
        tool_name,
        parameters,
        session_id,
    } = payload.into_inner();

    let canonical_tool_name = request::canonical_tool_name_or_error(&tool_name)?;
    let session_id = request::trimmed_session_id(session_id.as_deref());
    request::validate_session_context_requirement(&canonical_tool_name, session_id)?;
    let args = request::parse_arguments(parameters);
    let call = request::build_tool_call(canonical_tool_name, args)?;

    let root_tools = app_state.tools_for(crate::tools::ToolSurface::Root);
    let available_tool_schemas = root_tools.list_tools();
    let result = root_tools
        .execute_with_context(
            &call,
            ToolExecutionContext {
                session_id,
                tool_call_id: &call.id,
                event_tx: None,
                available_tool_schemas: Some(available_tool_schemas.as_slice()),
            },
        )
        .await
        .map_err(|err| AppError::ToolExecutionError(err.to_string()))?;

    let response = response::build_execution_response(tool_name, result)?;
    Ok(HttpResponse::Ok().json(response))
}

/// Configure tool execution routes.
///
/// This function registers the tool execution endpoint with the Actix-web
/// service configuration.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/tools/execute", web::post().to(execute_tool));
}
