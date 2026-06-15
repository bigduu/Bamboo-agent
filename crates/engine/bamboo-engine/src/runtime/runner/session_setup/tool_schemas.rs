use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{ToolExecutor, ToolSchema};
use bamboo_agent_core::Session;
use bamboo_tools::exposure::{
    activated_discoverable_tools, canonical_tool_name, discoverable_tool_short_description,
    is_core_tool,
};

const COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY: &str =
    "copilot_conclusion_with_options_enhancement_enabled";
const CONCLUSION_WITH_OPTIONS_ENHANCED_DESCRIPTION: &str = "Ask the user a question with options and wait for the user to select or enter a custom answer. If you are wrapping up a task turn, asking the user to choose next steps, or handing off execution, you must call this tool instead of ending with plain assistant text. For completion confirmation, include a `conclusion` object with both `summary` and `mermaid.graph`, and include `OK` as one of the options.";

fn is_copilot_conclusion_with_options_enhancement_enabled(session: &Session) -> bool {
    session
        .metadata
        .get(COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY)
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

fn apply_session_tool_schema_overrides(session: &Session, tool_schemas: &mut [ToolSchema]) {
    if !is_copilot_conclusion_with_options_enhancement_enabled(session) {
        return;
    }

    if let Some(schema) = tool_schemas.iter_mut().find(|schema| {
        schema
            .function
            .name
            .eq_ignore_ascii_case("conclusion_with_options")
    }) {
        schema.function.description = CONCLUSION_WITH_OPTIONS_ENHANCED_DESCRIPTION.to_string();
    }
}

pub(crate) fn resolve_available_tool_schemas_for_session(
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

    // The `update_goal` self-report tool is only meaningful while the autonomous
    // goal loop is active; hide it from every ordinary session so it never
    // tempts the model when no goal is set.
    if !config.goal_loop_active() {
        tool_schemas.retain(|schema| {
            schema.function.name != bamboo_tools::tools::goal::UPDATE_GOAL_TOOL_NAME
        });
    }

    let activated = activated_discoverable_tools(session);

    // Replace descriptions for inactive discoverable tools with short summaries.
    // All tools remain available to the LLM; activation only controls the
    // depth of guidance (short vs full) shown in the tool guide.
    for schema in &mut tool_schemas {
        let canonical = canonical_tool_name(&schema.function.name);
        if !is_core_tool(&canonical) && !activated.contains(&canonical) {
            if let Some(short) = discoverable_tool_short_description(&canonical) {
                schema.function.description =
                    format!("[Discoverable — not fully activated] {}", short);
            }
        }
    }

    apply_session_tool_schema_overrides(session, &mut tool_schemas);

    tool_schemas
}
