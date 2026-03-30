use async_trait::async_trait;

use super::tool_schemas::resolve_available_tool_schemas_for_session;
use crate::agent::core::tools::{FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema};
use crate::agent::core::{Message, Session};

const COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY: &str =
    "copilot_conclusion_with_options_enhancement_enabled";
const ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT: &str =
    "If you are wrapping up a task turn, asking the user to choose next steps, or handing off execution, you must call this tool instead of ending with plain assistant text.";

struct StaticToolExecutor {
    schemas: Vec<ToolSchema>,
}

#[async_trait]
impl ToolExecutor for StaticToolExecutor {
    async fn execute(
        &self,
        _call: &ToolCall,
    ) -> crate::agent::core::tools::executor::Result<ToolResult> {
        Ok(ToolResult {
            success: true,
            result: "ok".to_string(),
            display_preference: None,
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }
}

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: name.to_string(),
            description: format!("{name} tool"),
            parameters: serde_json::json!({ "type": "object", "properties": {} }),
        },
    }
}

#[test]
fn resolve_available_tool_schemas_uses_executor_when_registry_empty() {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("z_tool"), schema("a_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_dedupes_and_merges_additional_entries() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool"), schema("a_tool")],
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "b_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_excludes_disabled_tools() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool")],
        disabled_tools: ["a_tool".to_string(), "b_tool".to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_excludes_canonicalized_disabled_tool_aliases() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        disabled_tools: ["Bash".to_string(), "Read".to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("Bash"), schema("Read"), schema("Write")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Write"]);
}

#[test]
fn resolve_available_tool_schemas_hides_discoverable_tools_by_default() {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved.iter().map(|item| item.function.name.as_str()).collect();

    assert_eq!(names, vec!["Read"]);
}

#[test]
fn resolve_available_tool_schemas_includes_activated_discoverable_tools() {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let mut session = Session::new("session-1", "model");
    crate::agent::tools::exposure::activate_discoverable_tools(
        &mut session,
        ["Sleep", "scheduler"],
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved.iter().map(|item| item.function.name.as_str()).collect();

    assert_eq!(names, vec!["Read", "Sleep", "scheduler"]);
}

#[test]
fn resolve_available_tool_schemas_does_not_mutate_session_metadata() {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Write"), schema("recall")],
    };
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::system("sys"));
    session
        .metadata
        .insert("existing".to_string(), "value".to_string());

    let resolved =
        super::tool_schemas::resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Write"]);
    assert_eq!(
        session.metadata.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(session.metadata.len(), 1);
}

#[test]
fn resolve_available_tool_schemas_keeps_conclusion_with_options_description_neutral_when_flag_disabled(
) {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("conclusion_with_options")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let conclusion_with_options_schema = resolved
        .iter()
        .find(|schema| schema.function.name == "conclusion_with_options")
        .expect("conclusion_with_options schema should exist");

    assert_eq!(
        conclusion_with_options_schema.function.description,
        "conclusion_with_options tool"
    );
    assert!(!conclusion_with_options_schema
        .function
        .description
        .contains(ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT));
}

#[test]
fn resolve_available_tool_schemas_strengthens_conclusion_with_options_description_when_flag_enabled(
) {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("conclusion_with_options")],
    };
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY.to_string(),
        "true".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let conclusion_with_options_schema = resolved
        .iter()
        .find(|schema| schema.function.name == "conclusion_with_options")
        .expect("conclusion_with_options schema should exist");

    assert!(conclusion_with_options_schema
        .function
        .description
        .contains(ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT));
    assert!(conclusion_with_options_schema
        .function
        .description
        .contains("conclusion"));
    assert!(conclusion_with_options_schema
        .function
        .description
        .contains("OK"));
}

#[test]
fn apply_system_prompt_contexts_persists_runtime_prompt_metadata() {
    let mut session = Session::new("session-1", "model");
    session.add_message(Message::system("Base prompt"));
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();

    super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        "## Skill System\nSkill details",
        "## Tool Usage Guidelines\nGuide details",
    );

    assert_eq!(
        session
            .metadata
            .get("runtime_prompt_composer_version")
            .map(String::as_str),
        Some("bamboo.runtime-system-prompt.v1")
    );
    assert!(session.metadata.contains_key("runtime_prompt_fingerprint"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_flags"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_lengths"));
}

#[test]
fn apply_system_prompt_contexts_updates_runtime_fingerprint_when_context_changes() {
    let mut session = Session::new("session-1", "model");
    session.add_message(Message::system("Base prompt"));
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();

    super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        "## Skill System\nSkill A",
        "## Tool Usage Guidelines\nGuide A",
    );
    let first = session
        .metadata
        .get("runtime_prompt_fingerprint")
        .cloned()
        .expect("first fingerprint");

    super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        "## Skill System\nSkill B",
        "## Tool Usage Guidelines\nGuide A",
    );
    let second = session
        .metadata
        .get("runtime_prompt_fingerprint")
        .cloned()
        .expect("second fingerprint");

    assert_ne!(first, second);
}
