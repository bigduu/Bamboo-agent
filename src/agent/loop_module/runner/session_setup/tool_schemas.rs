use crate::agent::core::tools::{ToolExecutor, ToolSchema};
use crate::agent::loop_module::config::AgentLoopConfig;

pub(crate) fn resolve_available_tool_schemas_for_session(
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
) -> Vec<ToolSchema> {
    let mut tool_schemas = config.tool_registry.list_tools();
    if tool_schemas.is_empty() {
        tool_schemas = tools.list_tools();
    }

    tool_schemas.extend(config.additional_tool_schemas.clone());
    tool_schemas.sort_by(|left, right| left.function.name.cmp(&right.function.name));
    tool_schemas.dedup_by(|left, right| left.function.name == right.function.name);
    if !config.disabled_tools.is_empty() {
        tool_schemas.retain(|schema| !config.disabled_tools.contains(&schema.function.name));
    }

    tool_schemas
}
