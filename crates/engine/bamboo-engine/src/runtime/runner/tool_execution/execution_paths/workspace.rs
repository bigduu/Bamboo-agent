use super::super::super::workspace_context;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::Session;

pub(super) fn maybe_apply_workspace_update(
    session: &mut Session,
    tool_call: &ToolCall,
    result: &ToolResult,
    session_id: &str,
) {
    let Some(update) =
        workspace_context::extract_workspace_path_from_tool_result(tool_call, result)
    else {
        return;
    };
    // The Project-aware Workspace tool checks ownership against its resolved
    // destination before committing the switch. Preserve its acknowledged result.
    workspace_context::apply_workspace_path_to_session(
        session,
        &update.path,
        update.binding_status,
    );
    tracing::info!(
        session_id,
        tool = %tool_call.function.name,
        workspace_changed = true,
        "updated session workspace metadata"
    );
}

#[cfg(test)]
mod tests;
