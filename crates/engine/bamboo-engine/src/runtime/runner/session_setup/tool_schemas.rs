use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{ToolExecutor, ToolSchema};
use bamboo_agent_core::Session;
use bamboo_domain::{
    resolve_tool_reference_name, CapabilityLoadingClass, CapabilityLoadingMode,
    ClassifiedToolIdentity, ClassifiedToolSchema, EffectiveCallableSet,
};
use bamboo_skills::runtime_metadata::{
    LOADED_SKILL_IDS_METADATA_KEY, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
    SKILL_RUNTIME_SELECTION_SOURCE_KEY,
};
use bamboo_tools::exposure::{activated_discoverable_tools, expandable_tool_short_description};

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
    let catalog = resolve_classified_tool_catalog_for_session(config, tools, session);
    let effective = EffectiveCallableSet::from_catalog(
        &catalog,
        CapabilityLoadingMode::LegacyFullCatalog,
        std::iter::empty::<&str>(),
    );
    catalog
        .into_iter()
        .filter(|entry| effective.contains_execution_name(entry.execution_name()))
        .map(ClassifiedToolSchema::into_schema)
        .collect()
}

/// Resolve the provider-neutral logical catalog for one round.
///
/// Legacy providers project every model-visible Deferred entry from this
/// catalog. Native/fallback progressive-loading adapters later consume the same
/// classification and may project only initially visible entries. HostOnly
/// entries remain represented for host compatibility but never cross the model
/// catalog projection above.
pub(crate) fn resolve_classified_tool_catalog_for_session(
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    session: &Session,
) -> Vec<ClassifiedToolSchema> {
    let mut tool_schemas = config.tool_registry.list_tools();
    if tool_schemas.is_empty() {
        tool_schemas = tools.list_tools();
    }

    tool_schemas.extend(config.additional_tool_schemas.clone());
    tool_schemas.sort_by(|left, right| left.function.name.cmp(&right.function.name));
    tool_schemas.dedup_by(|left, right| left.function.name == right.function.name);
    // Resolve the disabled set LIVE each round (#136): when a resolver is wired
    // (server path) a tool disabled/re-enabled mid-run takes effect on the next
    // round, because this list is rebuilt unfiltered every round; with no resolver
    // (SDK/tests) this is the frozen per-run snapshot (#44), unchanged.
    let (disabled_tools, _disabled_skill_ids) = config.resolve_disabled_filters();
    // The `update_goal` self-report tool is only meaningful while the autonomous
    // goal loop is active; hide it from every ordinary session so it never
    // tempts the model when no goal is set.
    if !config.goal_loop_active() {
        tool_schemas.retain(|schema| {
            schema.function.name != bamboo_tools::tools::goal::UPDATE_GOAL_TOOL_NAME
        });
    }

    // Once a single explicitly selected workflow reaches a terminal activation
    // result, stop advertising load_skill so the model-issued attempt occurs
    // exactly once. A typed degraded result is terminal too: the main session
    // continues without workflow instructions instead of retrying forever.
    // Automatic catalogs keep the tool available until the model chooses a
    // candidate.
    let loaded_skill_ids = session
        .metadata
        .get(LOADED_SKILL_IDS_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    let selected_skill_ids = session
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    let explicit_selection = session
        .metadata
        .get(SKILL_RUNTIME_SELECTION_SOURCE_KEY)
        .is_some_and(|source| source == "explicit");
    let explicit_activation_is_current = explicit_selection
        && !loaded_skill_ids.is_empty()
        && loaded_skill_ids == selected_skill_ids;
    let explicit_activation_degraded = explicit_selection
        && session
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY);
    if explicit_activation_is_current || explicit_activation_degraded {
        tool_schemas.retain(|schema| schema.function.name != "load_skill");
    }

    let activated = activated_discoverable_tools(session);

    // Legacy providers keep Deferred schemas visible during migration;
    // activation only controls the depth of the existing tool-guide summaries.
    for schema in &mut tool_schemas {
        let Some(identity) = ClassifiedToolIdentity::from_schema_name(&schema.function.name) else {
            continue;
        };
        let guide_name = identity.alias_fallback_name();
        if identity.loading_class() == CapabilityLoadingClass::Deferred
            && !activated.contains(guide_name)
        {
            if let Some(short) = expandable_tool_short_description(guide_name) {
                schema.function.description =
                    format!("[Discoverable — not fully activated] {}", short);
            }
        }
    }

    apply_session_tool_schema_overrides(session, &mut tool_schemas);

    let mut by_execution_name = std::collections::BTreeMap::<String, ClassifiedToolSchema>::new();
    for entry in tool_schemas
        .into_iter()
        .filter_map(ClassifiedToolSchema::new)
    {
        let key = entry.execution_name().to_string();
        match by_execution_name.entry(key) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(entry);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    let disabled_execution_names = disabled_tools
        .iter()
        .filter_map(|reference| {
            resolve_tool_reference_name(reference, |name| by_execution_name.contains_key(name))
        })
        .collect::<std::collections::BTreeSet<_>>();
    by_execution_name.retain(|name, _| !disabled_execution_names.contains(name));

    let mut catalog = by_execution_name.into_values().collect::<Vec<_>>();
    catalog.sort_by(|left, right| {
        left.schema()
            .function
            .name
            .cmp(&right.schema().function.name)
    });

    catalog
}

#[cfg(test)]
mod live_disabled_tests {
    use super::*;
    use bamboo_agent_core::tools::{
        FunctionSchema, ToolCall, ToolError, ToolExecutionContext, ToolResult,
    };
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct TwoTools;
    #[async_trait::async_trait]
    impl ToolExecutor for TwoTools {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            unreachable!("not invoked in this test")
        }
        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.execute(call).await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            ["alpha_tool", "beta_tool", "load_skill"]
                .into_iter()
                .map(|name| ToolSchema {
                    schema_type: "function".into(),
                    function: FunctionSchema {
                        name: name.into(),
                        description: String::new(),
                        parameters: serde_json::json!({ "type": "object" }),
                    },
                })
                .collect()
        }
    }

    fn offered(config: &AgentLoopConfig, tools: &TwoTools, session: &Session, name: &str) -> bool {
        resolve_available_tool_schemas_for_session(config, tools, session)
            .iter()
            .any(|s| s.function.name == name)
    }

    #[test]
    fn live_disabled_resolver_filters_tools_on_the_next_round() {
        // A resolver whose disabled set flips mid-run: round 1 nothing disabled,
        // round 2 "beta_tool" disabled — mirrors a user disabling a tool mid-run.
        let disabled = Arc::new(AtomicBool::new(false));
        let d = disabled.clone();
        let mut config = AgentLoopConfig::default();
        config.disabled_filter_resolver = Some(Arc::new(move || {
            let tools = if d.load(Ordering::SeqCst) {
                BTreeSet::from(["beta_tool".to_string()])
            } else {
                BTreeSet::new()
            };
            (tools, BTreeSet::new())
        }));
        let session = Session::new("s", "m");
        let tools = TwoTools;

        // Round 1: nothing disabled -> beta_tool is offered.
        assert!(offered(&config, &tools, &session, "beta_tool"));

        // Disable beta_tool mid-run (NO new execution).
        disabled.store(true, Ordering::SeqCst);

        // Round 2 (same run): the live disable took effect -> beta_tool gone,
        // alpha_tool still offered. Re-enable would restore it (list rebuilt fresh).
        assert!(!offered(&config, &tools, &session, "beta_tool"));
        assert!(offered(&config, &tools, &session, "alpha_tool"));
    }

    #[test]
    fn explicit_degraded_activation_hides_load_skill_after_one_attempt() {
        let config = AgentLoopConfig::default();
        let tools = TwoTools;
        let mut session = Session::new("degraded", "m");
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
            "explicit".to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            r#"["review"]"#.to_string(),
        );

        assert!(offered(&config, &tools, &session, "load_skill"));
        session.metadata.insert(
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
            r#"{"code":"provider_failed"}"#.to_string(),
        );
        assert!(!offered(&config, &tools, &session, "load_skill"));
        assert!(!super::super::skill_context::explicit_activation_pending(
            &session
        ));
    }
}
