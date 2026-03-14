use crate::agent::core::tools::ToolCall;

pub(super) fn normalized_tool_name(tool_call: &ToolCall) -> Option<String> {
    crate::agent::tools::normalize_tool_ref(&tool_call.function.name)
}

pub(super) fn is_workspace_update_tool(tool_call: &ToolCall) -> Option<bool> {
    let normalized_tool_name = normalized_tool_name(tool_call)?;
    Some(
        normalized_tool_name == "SetWorkspace"
            || normalized_tool_name == "Write"
            || normalized_tool_name == "Edit"
            || normalized_tool_name == "NotebookEdit",
    )
}

pub(super) fn is_write_or_edit_tool_name(tool_name: String) -> bool {
    tool_name == "Write" || tool_name == "Edit" || tool_name == "NotebookEdit"
}

pub(super) fn extract_target_file_path_from_tool_call(tool_call: &ToolCall) -> Option<String> {
    let normalized_tool_name = normalized_tool_name(tool_call)?;
    let argument_key = match normalized_tool_name.as_str() {
        "Write" | "Edit" => "file_path",
        "NotebookEdit" => "notebook_path",
        _ => return None,
    };

    let args: serde_json::Value = serde_json::from_str(&tool_call.function.arguments).ok()?;
    args.get(argument_key)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}
