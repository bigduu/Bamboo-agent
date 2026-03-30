use crate::{
    agent::{
        core::tools::{FunctionCall, ToolCall},
        tools::normalize_tool_ref,
    },
    server::error::AppError,
};
use serde_json::Value;

use super::models::ToolParameter;

pub(super) fn canonical_tool_name_or_error(tool_name: &str) -> Result<String, AppError> {
    normalize_tool_ref(tool_name).ok_or_else(|| AppError::ToolNotFound(tool_name.to_string()))
}

pub(super) fn trimmed_session_id(session_id: Option<&str>) -> Option<&str> {
    session_id.map(str::trim).filter(|value| !value.is_empty())
}

pub(super) fn validate_session_context_requirement(
    tool_name: &str,
    session_id: Option<&str>,
) -> Result<(), AppError> {
    if requires_session_context(tool_name) && session_id.is_none() {
        return Err(AppError::BadRequest(format!(
            "Tool '{}' requires session_id for safe execution",
            tool_name
        )));
    }
    Ok(())
}

pub(super) fn parse_arguments(parameters: Vec<ToolParameter>) -> serde_json::Map<String, Value> {
    let mut args = serde_json::Map::new();
    for param in parameters {
        let parsed = serde_json::from_str(&param.value).unwrap_or(Value::String(param.value));
        args.insert(param.name, parsed);
    }
    args
}

pub(super) fn build_tool_call(
    canonical_tool_name: String,
    args: serde_json::Map<String, Value>,
) -> Result<ToolCall, AppError> {
    Ok(ToolCall {
        id: "tool_call".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: canonical_tool_name,
            arguments: serde_json::to_string(&args).map_err(AppError::SerializationError)?,
        },
    })
}

pub(super) fn requires_session_context(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Write"
            | "Edit"
            | "SetWorkspace"
            | "Workspace"
            | "Task"
            | "SubSession"
            | "memory_note"
            | "scheduler"
            | "schedule_tasks"
            | "sub_session_manager"
            | "recall"
            | "session_inspector"
    )
}
