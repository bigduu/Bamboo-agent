use crate::{agent::core::tools::ToolResult, server::error::AppError};

use super::models::{ToolExecutionResponse, ToolExecutionResultPayload};

pub(super) fn build_execution_response(
    requested_tool_name: String,
    result: ToolResult,
) -> Result<ToolExecutionResponse, AppError> {
    let display_preference = result
        .display_preference
        .unwrap_or_else(|| "Default".to_string());
    let payload = ToolExecutionResultPayload {
        tool_name: requested_tool_name,
        result: result.result,
        display_preference,
    };

    Ok(ToolExecutionResponse {
        result: serde_json::to_string(&payload).map_err(AppError::SerializationError)?,
    })
}
