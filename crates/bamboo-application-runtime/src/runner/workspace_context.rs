//! Workspace context helpers used by the agent loop runner.

mod prompt;
mod workspace_update;

pub(super) fn apply_workspace_path_to_session(
    session: &mut bamboo_application_agent::Session,
    workspace_path: &str,
) {
    prompt::apply_workspace_path_to_session(session, workspace_path);
}

pub(super) fn extract_workspace_path_from_tool_result(
    tool_call: &bamboo_application_agent::tools::ToolCall,
    result: &bamboo_application_agent::tools::ToolResult,
) -> Option<String> {
    workspace_update::extract_workspace_path_from_tool_result(tool_call, result)
}

pub(super) fn should_apply_workspace_update(
    session: &bamboo_application_agent::Session,
    tool_call: &bamboo_application_agent::tools::ToolCall,
) -> bool {
    workspace_update::should_apply_workspace_update(session, tool_call)
}

#[cfg(test)]
mod tests;
