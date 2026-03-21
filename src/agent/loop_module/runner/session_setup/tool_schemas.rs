use crate::agent::core::tools::{ToolExecutor, ToolSchema};
use crate::agent::core::Session;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::tools::tools::summarize_context::SUMMARIZE_CONTEXT_TOOL_NAME;

/// Minimum context usage percentage (0-100) at which the `summarize_context`
/// tool becomes visible to the LLM. Below this threshold, the tool schema is
/// omitted to save tokens.
const SUMMARIZE_CONTEXT_VISIBILITY_THRESHOLD: f64 = 80.0;

pub(super) fn resolve_available_tool_schemas(
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    session: &Session,
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

    // Conditionally hide `summarize_context` when context usage is below the
    // visibility threshold. This saves token budget in normal operation since
    // the tool schema itself consumes context tokens.
    if !should_show_summarize_context(session) {
        tool_schemas.retain(|schema| schema.function.name != SUMMARIZE_CONTEXT_TOOL_NAME);
    }

    tool_schemas
}

/// Determine whether the `summarize_context` tool should be visible.
///
/// The tool is shown when the session's last recorded token usage exceeds
/// the visibility threshold percentage, indicating context pressure.
fn should_show_summarize_context(session: &Session) -> bool {
    let Some(ref usage) = session.token_usage else {
        return false;
    };

    if usage.budget_limit == 0 {
        return false;
    }

    let usage_percent = (usage.total_tokens as f64 / usage.budget_limit as f64) * 100.0;
    usage_percent >= SUMMARIZE_CONTEXT_VISIBILITY_THRESHOLD
}
