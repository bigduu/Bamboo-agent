use async_trait::async_trait;

use super::tool_schemas::resolve_available_tool_schemas_for_session;
use bamboo_agent_core::tools::{FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema};
use bamboo_agent_core::{Message, Session};

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
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
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
    let config = crate::runtime::config::AgentLoopConfig::default();
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
    let config = crate::runtime::config::AgentLoopConfig {
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
    let config = crate::runtime::config::AgentLoopConfig {
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
    let config = crate::runtime::config::AgentLoopConfig {
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
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let session = Session::new("session-1", "model");

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Read", "Sleep", "scheduler"]);

    // Inactive discoverable tools get shortened descriptions
    let sleep = resolved
        .iter()
        .find(|s| s.function.name == "Sleep")
        .unwrap();
    assert!(sleep.function.description.contains("Discoverable"));
    let scheduler = resolved
        .iter()
        .find(|s| s.function.name == "scheduler")
        .unwrap();
    assert!(scheduler.function.description.contains("Discoverable"));
}

#[test]
fn resolve_available_tool_schemas_includes_activated_discoverable_tools() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("Read"), schema("Sleep"), schema("scheduler")],
    };
    let mut session = Session::new("session-1", "model");
    bamboo_tools::exposure::activate_discoverable_tools(&mut session, ["Sleep", "scheduler"]);

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Read", "Sleep", "scheduler"]);

    // Activated discoverable tools keep full descriptions
    let sleep = resolved
        .iter()
        .find(|s| s.function.name == "Sleep")
        .unwrap();
    assert!(!sleep.function.description.contains("Discoverable"));
}

#[test]
fn resolve_available_tool_schemas_does_not_mutate_session_metadata() {
    let config = crate::runtime::config::AgentLoopConfig::default();
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

    // All tools are available; inactive discoverable ones get shortened descriptions
    assert_eq!(names, vec!["Write", "recall"]);
    let recall = resolved
        .iter()
        .find(|s| s.function.name == "recall")
        .unwrap();
    assert!(recall.function.description.contains("Discoverable"));
    assert_eq!(
        session.metadata.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(session.metadata.len(), 1);
}

#[test]
fn resolve_available_tool_schemas_keeps_conclusion_with_options_description_neutral_when_flag_disabled(
) {
    let config = crate::runtime::config::AgentLoopConfig::default();
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
    let config = crate::runtime::config::AgentLoopConfig::default();
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
fn apply_system_prompt_contexts_persists_shared_prompt_snapshot() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let config = bamboo_infrastructure::Config::default();
    config.publish_env_vars();

    let loop_config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some("Base prompt".to_string()),
        ..Default::default()
    };
    let mut session = Session::new("snapshot-session", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.metadata.insert(
        "workspace_path".to_string(),
        "/tmp/snapshot-workspace".to_string(),
    );
    session.add_message(Message::system("Base prompt"));

    let _report = super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &loop_config,
        "## Skill System\nSkill details",
        "## Tool Usage Guidelines\nTool details",
    );

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(snapshot.base_system_prompt, "Base prompt");
    assert!(snapshot
        .skill_context
        .as_deref()
        .unwrap_or_default()
        .contains("Skill details"));
    assert!(snapshot
        .tool_guide_context
        .as_deref()
        .unwrap_or_default()
        .contains("Tool details"));
    assert!(snapshot.effective_system_prompt.contains("Base prompt"));
    assert!(snapshot.prompt_memory_observability.is_none());
}

#[test]
fn refresh_prompt_snapshot_from_session_preserves_multi_topic_memory_split_fields() {
    let mut session = Session::new("snapshot-memory-topics", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Cross-session Dream Notebook (read-only)\n````md\nDream note content\n````\n\n### Session Memory Topic: `backend-api`\n````md\n/users and /orders finalized\n````\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream note content")
    );
    let merged = snapshot
        .session_memory_note
        .as_deref()
        .expect("session memory note should be merged from topic blocks");
    assert!(merged.contains("### Session Memory Topic: `backend-api`"));
    assert!(merged.contains("/users and /orders finalized"));
    assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
    assert!(merged.contains("CTA wording approved"));
}

#[test]
fn refresh_prompt_snapshot_from_session_supports_global_dream_fallback_heading() {
    let mut session = Session::new("snapshot-memory-fallback-dream", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.session_memory_note.as_deref(),
        Some("Session note content")
    );
}

#[test]
fn refresh_prompt_snapshot_from_session_extracts_fine_grained_external_memory_fields() {
    let mut session = Session::new("snapshot-memory-fine-grained", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Relevant Durable Memories\nTurn-specific historical memories shortlisted for the latest user request.\n- [active][project] Release rule\n  Summary: Use the release checklist.\n\n### Project Durable Memory Index\n````md\n# Bamboo Memory Index\n- memory entry\n````\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    assert!(snapshot
        .relevant_durable_memories
        .as_deref()
        .is_some_and(|value| value.contains("Release rule")));
    assert_eq!(
        snapshot.project_memory_index.as_deref(),
        Some("# Bamboo Memory Index\n- memory entry")
    );
    assert_eq!(
        snapshot.global_dream_fallback.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.dream_notebook.as_deref(),
        Some("Dream fallback content")
    );
    assert_eq!(
        snapshot.session_memory_note.as_deref(),
        Some("Session note content")
    );
}

#[test]
fn refresh_prompt_snapshot_from_session_restores_prompt_memory_observability_from_metadata() {
    let mut session = Session::new("snapshot-memory-observability", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.metadata.insert(
        "runtime_prompt_memory_observability".to_string(),
        serde_json::to_string(&bamboo_agent_core::PromptMemoryObservability {
            project_prompt_injection_enabled: true,
            relevant_recall_enabled: false,
            relevant_recall_rerank_enabled: false,
            project_first_dream_enabled: false,
            latest_user_query_present: true,
            resolved_project_key: Some("project-key".to_string()),
            session_notes_status: "loaded".to_string(),
            project_memory_index_status: "loaded".to_string(),
            relevant_memory_status: "disabled".to_string(),
            project_dream_status: "disabled".to_string(),
            global_dream_fallback_status: "forced_loaded".to_string(),
            dream_source: "global_fallback".to_string(),
            session_topic_count: 1,
            truncated_session_topic_count: 0,
            relevant_memory_count: 0,
            session_note_section_chars: 10,
            project_memory_index_section_chars: 20,
            relevant_memory_section_chars: 0,
            project_dream_section_chars: 0,
            global_dream_fallback_section_chars: 40,
            context_pressure_warning_chars: 0,
            external_memory_section_chars: 120,
        })
        .expect("observability should serialize"),
    );
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    let observability = snapshot
        .prompt_memory_observability
        .expect("observability should be restored");
    assert!(!observability.relevant_recall_enabled);
    assert_eq!(observability.global_dream_fallback_status, "forced_loaded");
    assert_eq!(observability.dream_source, "global_fallback");
}

#[test]
fn refresh_prompt_snapshot_from_session_ignores_topic_truncation_note_outside_code_block() {
    let mut session = Session::new("snapshot-memory-topic-note", "gpt-test");
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session.add_message(Message::system(
        "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Session Memory Topic: `backend-api`\n````md\n/users and /orders finalized\n````\n_(showing 12 of 120 chars — use action=read topic=backend-api to see full content)_\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    ));

    super::prompt_setup::refresh_prompt_snapshot_from_session(&mut session);

    let snapshot = super::prompt_setup::read_prompt_snapshot_metadata(&session)
        .expect("runtime prompt snapshot should exist");
    let merged = snapshot
        .session_memory_note
        .as_deref()
        .expect("session memory note should be merged from topic blocks");
    assert!(merged.contains("### Session Memory Topic: `backend-api`"));
    assert!(merged.contains("/users and /orders finalized"));
    assert!(!merged.contains("showing 12 of 120 chars"));
    assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
    assert!(merged.contains("CTA wording approved"));
}

#[test]
fn apply_system_prompt_contexts_persists_runtime_prompt_metadata() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let config_with_env = bamboo_infrastructure::Config {
        env_vars: vec![bamboo_infrastructure::EnvVarEntry {
            name: "TEST_TOOL_TOKEN".to_string(),
            value: "hidden-value".to_string(),
            secret: true,
            value_encrypted: None,
            description: Some("Runtime test token".to_string()),
        }],
        ..bamboo_infrastructure::Config::default()
    };
    config_with_env.publish_env_vars();

    let root = tempfile::tempdir().expect("temp dir");
    let workspace = root.path().join("project");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    std::fs::write(root.path().join("AGENTS.md"), "Workspace policy").expect("agents file");

    let mut session = Session::new("session-1", "model");
    let env_context = crate::runtime::context::build_env_prompt_context().unwrap_or_default();
    session.add_message(Message::system(format!(
        "Base prompt\n\n{}\nWorkspace path: {}\n{}\n{}\n\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
        env_context,
    )));
    let config = crate::runtime::config::AgentLoopConfig::default();
    let skill_context = "## Skill System\nSkill details";
    let tool_guide_context = "## Tool Usage Guidelines\nGuide details";

    let report = super::prompt_setup::apply_system_prompt_contexts(
        &mut session,
        &config,
        skill_context,
        tool_guide_context,
    );

    assert_eq!(report.version, "bamboo.runtime-system-prompt.v3");
    assert_eq!(report.sections.len(), 6);
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
    let instruction_context = report
        .section("instruction_context")
        .expect("instruction section should exist");
    let env_context = report
        .section("env_context")
        .expect("env section should exist");
    assert!(workspace_context
        .content
        .contains(&format!("Workspace path: {}", workspace.display())));
    assert!(instruction_context.content.contains("Workspace policy"));
    assert!(env_context
        .content
        .contains("environment variables were explicitly configured by the user inside Bodhi"));
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};workspace_context:environment_workspace:dynamic:1:{};instruction_context:environment_instruction:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        instruction_context.len(),
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

    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let config_with_env = bamboo_infrastructure::Config {
        env_vars: vec![bamboo_infrastructure::EnvVarEntry {
            name: "TEST_TOOL_TOKEN".to_string(),
            value: "hidden-value".to_string(),
            secret: true,
            value_encrypted: None,
            description: Some("Runtime test token".to_string()),
        }],
        ..bamboo_infrastructure::Config::default()
    };
    config_with_env.publish_env_vars();

    let base_prompt = "Base prompt";
    let workspace_context = format!(
        "{}\nWorkspace path: /tmp/workspace\n{}\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );
    let instruction_context = format!(
        "{}\n## AGENTS.md\nSource: /tmp/AGENTS.md\n\nWorkspace policy\n{}",
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER,
        crate::runtime::context::instruction::INSTRUCTION_CONTEXT_END_MARKER,
    );
    let env_context = crate::runtime::context::build_env_prompt_context().unwrap_or_default();
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
            "instruction_context",
            PromptLayer::EnvironmentInstruction,
            true,
            instruction_context.as_str(),
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
        "{}\n\n{}\n\n{}\n\n{}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n{}\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n{}\n<!-- BAMBOO_TOOL_GUIDE_END -->",
        base_prompt, workspace_context, instruction_context, env_context, skill_context, tool_guide_context
    );

    let report = PromptAssemblyReport::from_sections(sections, &final_prompt);

    let expected_lengths = format!(
        "base={};workspace={};instruction={};env={};skill={};tool_guide={};external_memory={};task_list={};final={}",
        base_prompt.len(),
        workspace_context.len(),
        instruction_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
        0,
        0,
        final_prompt.len(),
    );
    let expected_layout = format!(
        "base_prompt:core_static:static:1:{};workspace_context:environment_workspace:dynamic:1:{};instruction_context:environment_instruction:dynamic:1:{};env_context:environment_configuration:dynamic:1:{};skill_context:skill_metadata:dynamic:1:{};tool_guide_context:capability_tool:dynamic:1:{}",
        base_prompt.len(),
        workspace_context.len(),
        instruction_context.len(),
        env_context.len(),
        skill_context.len(),
        tool_guide_context.len(),
    );

    assert_eq!(
        report.component_flags_value(),
        "workspace=1;instruction=1;env=1;skill=1;tool_guide=1;external_memory=0;task_list=0"
    );
    assert_eq!(report.component_lengths_value(), expected_lengths);
    assert_eq!(report.section_layout_value(), expected_layout);
}
