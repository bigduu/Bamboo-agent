use async_trait::async_trait;

use super::prompt_envelope::StablePromptFrame;
use super::prompt_setup::build_stable_prompt_frame_with_sections;
use super::tool_schemas::resolve_available_tool_schemas_for_session;
use bamboo_agent_core::agent::types::{TaskItem, TaskItemStatus, TaskList};
use bamboo_agent_core::tools::{FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema};
use bamboo_agent_core::{Message, Session};
use bamboo_domain::RuntimeSessionPersistence;
use bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY;
use bamboo_skills::{SkillManager, SkillStoreConfig};
use chrono::Utc;
use std::sync::{Arc, Mutex};

const COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY: &str =
    "copilot_conclusion_with_options_enhancement_enabled";
const ASK_USER_ENHANCED_DESCRIPTION_FRAGMENT: &str =
    "If you are wrapping up a task turn, asking the user to choose next steps, or handing off execution, you must call this tool instead of ending with plain assistant text.";

struct StaticToolExecutor {
    schemas: Vec<ToolSchema>,
}

#[derive(Default)]
struct RecordingToolExecutor {
    calls: Mutex<Vec<ToolCall>>,
    schemas: Vec<ToolSchema>,
}

#[derive(Default)]
struct RecordingPersistence {
    sessions: Mutex<Vec<Session>>,
}

#[async_trait]
impl RuntimeSessionPersistence for RecordingPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.sessions
            .lock()
            .expect("recording lock")
            .push(session.clone());
        Ok(())
    }
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
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }
}

#[async_trait]
impl ToolExecutor for RecordingToolExecutor {
    async fn execute(
        &self,
        call: &ToolCall,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        self.calls
            .lock()
            .expect("recording tool lock")
            .push(call.clone());
        Ok(ToolResult {
            success: true,
            result: serde_json::json!({
                "skill_id": "review",
                "instructions": "REPORT_ONLY_ACTIONABLE_FINDINGS"
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
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

#[tokio::test]
async fn session_setup_publishes_current_skill_allowlist_before_tool_execution() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: Vec::new(),
    };
    let mut session = Session::new("selection-publish", "model");

    super::prepare_session_for_loop(
        &mut session,
        "Review the current changes",
        &config,
        &tools,
        None,
        "selection-publish",
        &crate::runtime::runner::logging::DebugLogger::new(false),
    )
    .await;

    let current_ids = session
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .expect("current runtime selection metadata");
    assert!(current_ids.contains("review"));
    assert!(!current_ids.contains("plan"));
    let saved = persistence.sessions.lock().expect("recording lock");
    let published = saved.last().expect("selection published");
    let ids = published
        .metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY)
        .expect("runtime selection metadata");
    assert!(ids.contains("review"));
    assert!(!ids.contains("plan"));
}

#[tokio::test]
async fn session_setup_preloads_one_explicit_skill_before_model_execution() {
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize skills");
    let persistence = Arc::new(RecordingPersistence::default());
    let config = crate::runtime::config::AgentLoopConfig {
        skill_manager: Some(manager),
        selected_skill_ids: Some(vec!["review".to_string()]),
        persistence: Some(persistence.clone()),
        ..Default::default()
    };
    let tools = RecordingToolExecutor {
        calls: Mutex::new(Vec::new()),
        schemas: vec![schema("load_skill")],
    };
    let mut session = Session::new("explicit-review", "model");

    super::prepare_session_for_loop(
        &mut session,
        "Review this change",
        &config,
        &tools,
        None,
        "explicit-review",
        &crate::runtime::runner::logging::DebugLogger::new(false),
    )
    .await;

    let calls = tools.calls.lock().expect("recording tool lock");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].function.name, "load_skill");
    assert!(calls[0].function.arguments.contains("review"));
    assert_eq!(
        session
            .metadata
            .get("skill_runtime_loaded_skill_ids")
            .map(String::as_str),
        Some("[\"review\"]")
    );
    assert_eq!(
        session
            .metadata
            .get("skill_runtime_last_loaded_skill_id")
            .map(String::as_str),
        Some("review")
    );
    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        .expect("system prompt")
        .content
        .as_str();
    assert!(system_prompt.contains("## Explicit Workflow Activated"));
    assert!(system_prompt.contains("REPORT_ONLY_ACTIONABLE_FINDINGS"));
    assert!(!system_prompt.contains("Select EXACTLY ONE skill"));

    let saved = persistence.sessions.lock().expect("recording lock");
    assert!(saved.iter().any(|saved_session| {
        saved_session
            .metadata
            .get("skill_runtime_loaded_skill_ids")
            .is_some_and(|ids| ids == "[\"review\"]")
    }));
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
fn resolve_available_tool_schemas_hides_load_skill_after_activation() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("load_skill"), schema("read_skill_resource")],
    };
    let mut session = Session::new("session-loaded-skill", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "explicit".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names = resolved
        .iter()
        .map(|schema| schema.function.name.as_str())
        .collect::<Vec<_>>();

    assert!(!names.contains(&"load_skill"));
    assert!(names.contains(&"read_skill_resource"));
}

#[test]
fn resolve_available_tool_schemas_keeps_load_skill_for_new_automatic_selection() {
    let config = crate::runtime::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("load_skill"), schema("read_skill_resource")],
    };
    let mut session = Session::new("session-auto-after-loaded-skill", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        "[\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selected_skill_ids".to_string(),
        "[\"debug\",\"review\"]".to_string(),
    );
    session.metadata.insert(
        "skill_runtime_selection_source".to_string(),
        "auto".to_string(),
    );

    let resolved = resolve_available_tool_schemas_for_session(&config, &tools, &session);
    let names = resolved
        .iter()
        .map(|schema| schema.function.name.as_str())
        .collect::<Vec<_>>();

    assert!(names.contains(&"load_skill"));
    assert!(names.contains(&"read_skill_resource"));
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
        schemas: vec![schema("Write"), schema("session_history")],
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
    assert_eq!(names, vec!["Write", "session_history"]);
    let session_history = resolved
        .iter()
        .find(|s| s.function.name == "session_history")
        .unwrap();
    assert!(session_history
        .function
        .description
        .contains("Discoverable"));
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
    let config = bamboo_llm::Config::default();
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
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_TOOL_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        description: Some("Runtime test token".to_string()),
    }];
    config_with_env.publish_env_vars();

    let root = tempfile::tempdir().expect("temp dir");
    let workspace = root.path().join("project");
    std::fs::create_dir_all(root.path().join(".git")).expect("git marker");
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
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_TOOL_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        description: Some("Runtime test token".to_string()),
    }];
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

#[test]
fn build_stable_prompt_frame_includes_base_and_stable_contexts() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let mut config_with_env = bamboo_llm::Config::default();
    config_with_env.env_vars = vec![bamboo_config::EnvVarEntry {
        name: "TEST_PROMPT_ENVELOPE_TOKEN".to_string(),
        value: "hidden-value".to_string(),
        secret: true,
        value_encrypted: None,
        description: Some("Prompt envelope token".to_string()),
    }];
    config_with_env.publish_env_vars();

    let workspace = std::env::temp_dir().join("bamboo-prompt-envelope-workspace");
    let system_prompt = format!(
        "Base system\n\n{}\nWorkspace path: {}\n{}\n{}",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );

    let config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some(system_prompt),
        ..Default::default()
    };
    let mut session = Session::new("session-stable-frame-1", "model");
    session.metadata.insert(
        "skill.context".to_string(),
        "## Skill\nUse the skill".to_string(),
    );

    let stable = build_stable_prompt_frame_with_sections(
        &session,
        &config,
        &[],
        &std::collections::BTreeSet::new(),
    )
    .0;

    assert!(stable.stable_instructions.contains("Base system"));
    assert!(stable.stable_instructions.contains("Workspace path:"));
    assert!(stable
        .stable_instructions
        .contains("environment variables were explicitly configured by the user inside Bodhi"));
    // Framework-invariant directives ride on top of even a fully custom override
    // base (`config.system_prompt`), so they are present regardless of the user's
    // base prompt.
    assert!(stable
        .stable_instructions
        .contains("Investigate before you conclude"));
    assert!(stable.stable_instructions.contains("Verify your own work"));
    assert!(stable.stable_prefix_messages.is_empty());
}

#[test]
fn build_stable_prompt_frame_strips_round_dynamic_prompt_blocks() {
    let _lock = crate::runtime::tests::env_cache_lock_acquire();
    let workspace = std::env::temp_dir().join("bamboo-prompt-envelope-workspace-dynamic");
    let system_prompt = format!(
        "Base system\n\n{}\nWorkspace path: {}\n{}\n{}\n\n<!-- BAMBOO_TASK_LIST_START -->\n## Current Task List: Agent Tasks\n[/] task-1: do the thing\n<!-- BAMBOO_TASK_LIST_END -->\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\nExternal memory snapshot\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPLAN MODE ACTIVE\n<!-- BAMBOO_PLAN_MODE_END -->\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\nPlan runtime snapshot\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->",
        crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER,
        workspace.display(),
        crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER,
        crate::runtime::context::workspace_prompt_guidance(),
    );

    let config = crate::runtime::config::AgentLoopConfig {
        system_prompt: Some(system_prompt),
        ..Default::default()
    };
    let mut session = Session::new("session-stable-frame-2", "model");
    session.metadata.insert(
        "skill.context".to_string(),
        "## Skill\nUse the skill".to_string(),
    );
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: "do the thing".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "Older work was compressed.",
        2,
        80,
    ));

    let stable = build_stable_prompt_frame_with_sections(
        &session,
        &config,
        &[],
        &std::collections::BTreeSet::new(),
    )
    .0;

    assert!(stable.stable_instructions.contains("Base system"));
    assert!(stable.stable_instructions.contains("Workspace path:"));
    assert!(!stable.stable_instructions.contains("Current Task List"));
    assert!(!stable
        .stable_instructions
        .contains("External memory snapshot"));
    assert!(!stable.stable_instructions.contains("PLAN MODE ACTIVE"));
    assert!(!stable.stable_instructions.contains("Plan runtime snapshot"));
}

#[test]
fn stable_prompt_frame_carries_instructions_and_prefix_messages() {
    // The stable frame is what feeds the IR's system field + StablePrefix run; the
    // Responses-input/chat projections are derived by the IR's lowering methods, not
    // a per-envelope converter.
    let stable =
        StablePromptFrame::new("Stable instructions", vec![Message::user("stable prefix")]);
    assert_eq!(stable.stable_instructions, "Stable instructions");
    assert_eq!(stable.stable_prefix_messages.len(), 1);
    assert_eq!(stable.stable_prefix_messages[0].content, "stable prefix");
}
