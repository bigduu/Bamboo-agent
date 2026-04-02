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
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

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
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

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
    let config_with_env = crate::core::Config {
        env_vars: vec![crate::core::EnvVarEntry {
            name: "TEST_TOOL_TOKEN".to_string(),
            value: "hidden-value".to_string(),
            secret: true,
            value_encrypted: None,
            description: Some("Runtime test token".to_string()),
        }],
        ..crate::core::Config::default()
    };
    config_with_env.publish_env_vars();

    let mut session = Session::new("session-1", "model");
    let env_context = crate::server::app_state::build_env_prompt_context().unwrap_or_default();
    session.add_message(Message::system(format!(
        "Base prompt\n\n{}\nWorkspace path: /tmp/workspace\n{}\n{}\n\n{}",
        crate::server::app_state::WORKSPACE_CONTEXT_START_MARKER,
        crate::server::app_state::WORKSPACE_CONTEXT_END_MARKER,
        crate::server::app_state::workspace_prompt_guidance(),
        env_context,
    )));
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let skill_context = "## Skill System\nSkill details";
    let tool_guide_context = "## Tool Usage Guidelines\nGuide details";

    let report = super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        skill_context,
        tool_guide_context,
    );

    assert_eq!(report.version, "bamboo.runtime-system-prompt.v3");
    assert_eq!(report.sections.len(), 5);
    assert_eq!(
        session
            .metadata
            .get("runtime_prompt_composer_version")
            .map(String::as_str),
        Some("bamboo.runtime-system-prompt.v3")
    );
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_flags"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_component_lengths"));
    assert!(session
        .metadata
        .contains_key("runtime_prompt_section_layout"));

    let base_prompt = report
        .section("base_prompt")
        .expect("base prompt section should exist");
    let workspace_context = report
        .section("workspace_context")
        .expect("workspace section should exist");
    let env_context = report
        .section("env_context")
        .expect("env section should exist");
    assert!(workspace_context
        .content
        .contains("Workspace path: /tmp/workspace"));
    assert!(env_context
        .content
        .contains("environment variables were explicitly configured by the user inside Bodhi"));
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};workspace_context:environment_workspace:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
    );
    assert_eq!(
        session
            .metadata
            .get("runtime_prompt_section_layout")
            .map(String::as_str),
        Some(expected_layout.as_str())
    );
}

#[test]
fn prompt_assembly_report_component_values_match_sections() {
    use super::prompt_setup::{PromptAssemblyReport, PromptLayer, PromptSection};

    let config_with_env = crate::core::Config {
        env_vars: vec![crate::core::EnvVarEntry {
            name: "TEST_TOOL_TOKEN".to_string(),
            value: "hidden-value".to_string(),
            secret: true,
            value_encrypted: None,
            description: Some("Runtime test token".to_string()),
        }],
        ..crate::core::Config::default()
    };
    config_with_env.publish_env_vars();

    let base_prompt = "Base prompt";
    let workspace_context = format!(
        "{}\nWorkspace path: /tmp/workspace\n{}\n{}",
        crate::server::app_state::WORKSPACE_CONTEXT_START_MARKER,
        crate::server::app_state::WORKSPACE_CONTEXT_END_MARKER,
        crate::server::app_state::workspace_prompt_guidance(),
    );
    let env_context = crate::server::app_state::build_env_prompt_context().unwrap_or_default();
    let skill_context = "## Skill System\nSkill details";
    let tool_guide_context = "## Tool Usage Guidelines\nGuide details";
    let sections = vec![
        PromptSection::new("base_prompt", PromptLayer::CoreStatic, false, base_prompt),
        PromptSection::new(
            "workspace_context",
            PromptLayer::EnvironmentWorkspace,
            true,
            workspace_context.as_str(),
        ),
        PromptSection::new(
            "env_context",
            PromptLayer::EnvironmentConfiguration,
            true,
            env_context.as_str(),
        ),
        PromptSection::new(
            "skill_context",
            PromptLayer::SkillMetadata,
            true,
            skill_context,
        ),
        PromptSection::new(
            "tool_guide_context",
            PromptLayer::CapabilityTool,
            true,
            tool_guide_context,
        ),
    ];
    let final_prompt = format!(
        "{}\n\n{}\n\n{}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n{}\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n{}\n<!-- BAMBOO_TOOL_GUIDE_END -->",
        base_prompt, workspace_context, env_context, skill_context, tool_guide_context
    );

    let report = PromptAssemblyReport::from_sections(sections, &final_prompt);

    let expected_lengths = format!(
        "base={};workspace={};env={};skill={};tool_guide={};external_memory={};task_list={};final={}",
        base_prompt.len(),
        workspace_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
        0,
        0,
        final_prompt.len(),
    );
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};workspace_context:environment_workspace:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
    );

    assert_eq!(
        report.component_flags_value(),
        "workspace=1;env=1;skill=1;tool_guide=1;external_memory=0;task_list=0"
    );
    assert_eq!(report.component_lengths_value(), expected_lengths);
    assert_eq!(report.section_layout_value(), expected_layout);
}
