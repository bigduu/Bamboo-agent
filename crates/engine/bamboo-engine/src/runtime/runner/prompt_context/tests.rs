use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;

use super::merge_system_prompt_with_contexts;
use super::system_sections::{strip_existing_skill_context, strip_existing_tool_guide_context};
use bamboo_agent_core::Message;
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};

#[derive(Clone)]
struct StaticResponseProvider {
    response: String,
    requested_models: Arc<Mutex<Vec<String>>>,
}

impl StaticResponseProvider {
    fn new(response: impl Into<String>) -> Self {
        Self {
            response: response.into(),
            requested_models: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl LLMProvider for StaticResponseProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.requested_models
            .lock()
            .expect("lock poisoned")
            .push(model.to_string());
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token(self.response.clone())),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn publish_global_dream(store: &bamboo_memory::memory_store::MemoryStore, content: &str) {
    let generation = store
        .current_scope_generation(bamboo_memory::memory_store::MemoryScope::Global, None)
        .await
        .expect("read global generation");
    store
        .publish_dream_snapshot(
            bamboo_memory::memory_store::MemoryScope::Global,
            None,
            &generation,
            content,
        )
        .await
        .expect("publish global dream");
}

async fn publish_project_dream(
    store: &bamboo_memory::memory_store::MemoryStore,
    project_key: &str,
    content: &str,
) {
    let generation = store
        .current_scope_generation(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key),
        )
        .await
        .expect("read project generation");
    store
        .publish_dream_snapshot(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key),
            &generation,
            content,
        )
        .await
        .expect("publish project dream");
}

#[test]
fn latest_user_query_skips_hidden_and_runtime_resume_messages() {
    let mut session = bamboo_agent_core::Session::new("recall-real-user", "test-model");
    session.add_message(bamboo_agent_core::Message::user("real release question"));

    let mut hidden = bamboo_agent_core::Message::user("hidden child completion");
    hidden.metadata = Some(serde_json::json!({
        "hidden_from_ui": true,
        "runtime_kind": "child_completion_resume"
    }));
    session.add_message(hidden);

    let mut runtime_kind_only = bamboo_agent_core::Message::user("internal retry notice");
    runtime_kind_only.metadata = Some(serde_json::json!({"runtime_kind": "retry_resume"}));
    session.add_message(runtime_kind_only);

    assert_eq!(
        super::external_memory::latest_user_query_text(&session).as_deref(),
        Some("real release question")
    );
}

#[test]
fn latest_user_query_returns_none_for_internal_or_blank_user_messages_only() {
    let mut session = bamboo_agent_core::Session::new("recall-no-real-user", "test-model");
    session.add_message(bamboo_agent_core::Message::user("   "));
    let mut hidden = bamboo_agent_core::Message::user("hidden resume");
    hidden.metadata = Some(serde_json::json!({"hidden_from_ui": true}));
    session.add_message(hidden);

    assert!(super::external_memory::latest_user_query_text(&session).is_none());
}

#[test]
fn merge_system_prompt_with_contexts_appends_both_contexts() {
    let merged = merge_system_prompt_with_contexts(
        "You are a helpful assistant.",
        "\n\n## Skill System\n\n### Available Skills\nDetails",
        "## Tool Usage Guidelines\n\n### File Reading Tools\nDetails",
    );
    assert!(merged.starts_with("You are a helpful assistant."));
    assert!(merged.contains("<!-- BAMBOO_SKILL_CONTEXT_START -->"));
    assert!(merged.contains("## Skill System"));
    assert!(merged.contains("<!-- BAMBOO_TOOL_GUIDE_START -->"));
    assert!(merged.contains("## Tool Usage Guidelines"));
}

#[test]
fn merge_system_prompt_with_contexts_handles_empty_base_prompt() {
    let merged = merge_system_prompt_with_contexts(
        "",
        "\n\n## Skill System\n\n### Available Skills",
        "## Tool Usage Guidelines\n\n### File Reading Tools",
    );
    assert_eq!(
        merged,
        "<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\n### Available Skills\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\n### File Reading Tools\n<!-- BAMBOO_TOOL_GUIDE_END -->"
    );
}

#[test]
fn strip_existing_skill_context_removes_previous_section() {
    let stripped = strip_existing_skill_context(
        "Base prompt\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\n### Available Skills\nInstructions\n<!-- BAMBOO_SKILL_CONTEXT_END -->",
    );
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_skill_context_does_not_remove_user_heading_without_markers() {
    let original = "Base prompt\n\n## Skill System\nThis heading belongs to user prompt.";
    let stripped = strip_existing_skill_context(original);
    assert_eq!(stripped, original);
}

#[test]
fn strip_existing_tool_guide_context_removes_previous_section() {
    let stripped = strip_existing_tool_guide_context(
        "Base prompt\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\n### File Reading Tools\nInstructions\n<!-- BAMBOO_TOOL_GUIDE_END -->",
    );
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_tool_guide_context_does_not_remove_user_heading_without_markers() {
    let original = "Base prompt\n\n## Tool Usage Guidelines\nUser custom section.";
    let stripped = strip_existing_tool_guide_context(original);
    assert_eq!(stripped, original);
}

#[tokio::test]
async fn external_memory_includes_global_dream_fallback_and_session_note_when_project_unknown() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    publish_global_dream(
        &store,
        "# Bamboo Dream Notebook\n\nDurable cross-session insight",
    )
    .await;
    store
        .write_session_topic("session-dream-test", "default", "Session durable note")
        .await
        .expect("save session note");

    let mut session = bamboo_agent_core::Session::new("session-dream-test", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("Global Dream Summary (fallback)"));
    assert!(system_prompt.contains("Durable cross-session insight"));
    assert!(system_prompt.contains("Session Memory Note"));
    assert!(system_prompt.contains("Session durable note"));
    assert!(!system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("the `memory` tool only for durable project/global knowledge"));
    assert!(system_prompt.contains("prefer `memory` action=query first"));
}

#[tokio::test]
async fn external_memory_includes_ledger_agenda_when_records_are_open() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let jiandu_root = temp_dir.path().join("jiandu");
    let bamboo_root = temp_dir.path().join("bamboo");
    let store = bamboo_memory::memory_store::MemoryStore::new(&jiandu_root);

    // Bamboo's Ledger remains separate from Jiandu memory.
    let ledger = bamboo_memory::ledger_store::LedgerStore::new(&bamboo_root);
    let mut overdue = bamboo_domain::ledger::LedgerRecord::new(
        "rec_overdue",
        bamboo_domain::ledger::RecordKind::Todo,
        "Send the quarterly report",
    );
    overdue.time.due_at = Some(chrono::Utc::now() - chrono::Duration::days(1));
    ledger
        .write_record(overdue, None)
        .await
        .expect("write overdue record");
    let undated = bamboo_domain::ledger::LedgerRecord::new(
        "rec_open",
        bamboo_domain::ledger::RecordKind::Todo,
        "Clean the garage",
    );
    ledger
        .write_record(undated, None)
        .await
        .expect("write undated record");

    let mut session = bamboo_agent_core::Session::new("session-ledger-test", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));

    super::refresh_external_memory_context_with_stores(
        &mut session,
        &store,
        &bamboo_root,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");
    assert!(system_prompt.contains("### Ledger Agenda (prospective records)"));
    assert!(system_prompt.contains("[OVERDUE] `rec_overdue`"));
    assert!(system_prompt.contains("Send the quarterly report"));
    assert!(system_prompt.contains("[OPEN] `rec_open`"));
    assert!(system_prompt.contains("record it with the `ledger` tool"));

    // Flag off → the section disappears even with open records.
    let mut flags = crate::runtime::config::PromptMemoryFlags::default();
    flags.ledger_agenda = false;
    super::refresh_external_memory_context_with_stores(
        &mut session,
        &store,
        &bamboo_root,
        flags,
        None,
    )
    .await;
    let without = super::render_external_memory_section(&session)
        .expect("external memory section should still render");
    assert!(!without.contains("### Ledger Agenda"));
    assert!(!jiandu_root.join("ledger").exists());
}

#[tokio::test]
async fn external_memory_omits_ledger_agenda_when_ledger_is_empty() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let jiandu_root = temp_dir.path().join("jiandu");
    let bamboo_root = temp_dir.path().join("bamboo");
    let store = bamboo_memory::memory_store::MemoryStore::new(&jiandu_root);
    store
        .write_session_topic("session-empty-ledger", "default", "note")
        .await
        .expect("save session note");

    let mut session = bamboo_agent_core::Session::new("session-empty-ledger", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));

    super::refresh_external_memory_context_with_stores(
        &mut session,
        &store,
        &bamboo_root,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");
    assert!(!system_prompt.contains("### Ledger Agenda"));
    assert!(!jiandu_root.join("ledger").exists());
}

#[tokio::test]
async fn external_memory_includes_project_memory_index_and_omits_global_dream_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-index").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-alpha");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Release freeze begins next week",
            "Merge freeze begins on Tuesday for mobile release cut.",
            &["release".to_string(), "freeze".to_string()],
            Some("session-project-memory"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save project memory");
    publish_global_dream(
        &store,
        "# Bamboo Dream Notebook\n\nGlobal fallback that should not appear",
    )
    .await;

    let mut session = bamboo_agent_core::Session::new("session-project-memory", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("Release freeze begins next week"));
    assert!(!system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(!system_prompt.contains("Global fallback that should not appear"));
    assert!(system_prompt.contains("### Session Memory Note (markdown)"));
}

#[tokio::test]
async fn external_memory_excludes_other_project_memory_index_content() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id_a = bamboo_domain::ProjectId::parse("project-prompt-a").expect("project id");
    let project_id_b = bamboo_domain::ProjectId::parse("project-prompt-b").expect("project id");
    let project_memory_a = store.for_project(&project_id_a);
    let project_memory_b = store.for_project(&project_id_b);
    let workspace_a = temp_dir.path().join("workspace-project-a");
    let workspace_b = temp_dir.path().join("workspace-project-b");
    std::fs::create_dir_all(&workspace_a).expect("workspace a dir");
    std::fs::create_dir_all(&workspace_b).expect("workspace b dir");
    let project_key_a = project_id_a.to_string();
    let project_key_b = project_id_b.to_string();

    project_memory_a
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key_a.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Project A release rule",
            "Only Project A changes may ship this week.",
            &["release".to_string(), "project-a".to_string()],
            Some("session-project-a"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save project A memory");
    project_memory_b
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key_b.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Project B deployment rule",
            "Project B uses a separate deployment checklist.",
            &["deploy".to_string(), "project-b".to_string()],
            Some("session-project-b"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save project B memory");

    let mut session = bamboo_agent_core::Session::new("session-project-a", "test-model");
    session.set_project_id_meta(project_id_a.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace_a.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("Project A release rule"));
    assert!(system_prompt.contains("Only Project A changes may ship this week."));
    assert!(!system_prompt.contains("Project B deployment rule"));
    assert!(!system_prompt.contains("Project B uses a separate deployment checklist."));
}

#[tokio::test]
async fn external_memory_malformed_project_id_does_not_read_canonical_project_memory() {
    struct EmptyProjectSource;

    #[async_trait]
    impl crate::project_context::ProjectContextSource for EmptyProjectSource {
        async fn find_project(
            &self,
            _project_id: &bamboo_domain::ProjectId,
        ) -> Result<
            Option<crate::project_context::ProjectDescriptor>,
            crate::project_context::ProjectContextError,
        > {
            Ok(None)
        }
    }

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-secret").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_id.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Legacy secret",
            "MUST NOT ENTER PROMPT THROUGH MALFORMED PROJECT ID",
            &[],
            Some("malformed-prompt-memory"),
            "test",
            false,
            None,
        )
        .await
        .expect("seed canonical Project memory");
    let mut session = bamboo_agent_core::Session::new("malformed-prompt-memory", "test-model");
    session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
    session.set_project_id_meta("../malformed".to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    let resolver =
        crate::project_context::ProjectContextResolver::new(Arc::new(EmptyProjectSource));

    super::refresh_external_memory_context_with_store_and_resolver(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
        Some(&resolver),
    )
    .await;

    let rendered = super::render_external_memory_section(&session).unwrap_or_default();
    assert!(!rendered.contains("Legacy secret"));
    assert!(!rendered.contains("MUST NOT ENTER PROMPT THROUGH MALFORMED PROJECT ID"));
}

#[tokio::test]
async fn external_memory_truncates_project_memory_index_without_path_access() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id =
        bamboo_domain::ProjectId::parse("project-prompt-truncated").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-beta");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();
    for index in 0..12 {
        project_memory
            .write_memory(
                bamboo_memory::memory_store::MemoryScope::Project,
                Some(project_key.as_str()),
                bamboo_memory::memory_store::DurableMemoryType::Project,
                &format!("Architectural note {index}"),
                &format!("Durable project context {index}: {}", "x".repeat(320)),
                &["architecture".to_string()],
                Some("session-project-memory-truncated"),
                "test-model",
                false,
                None,
            )
            .await
            .expect("write project memory");
    }

    let mut session =
        bamboo_agent_core::Session::new("session-project-memory-truncated", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("showing "));
    assert!(system_prompt.contains("Architectural note"));
}

#[test]
fn memory_freshness_note_marks_old_index_entries() {
    let note = bamboo_memory::memory_store::render_memory_freshness_note(
        "2026-03-01T00:00:00Z",
        bamboo_memory::memory_store::FreshnessKind::Index,
    )
    .expect("old memory index should carry a freshness warning");
    assert!(note.contains("memory index entry"));
    assert!(note.contains("verify"));
}

#[tokio::test]
async fn external_memory_truncates_multi_topic_content_and_is_idempotent() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    store
        .write_session_topic("session-memory-many", "alpha", &"a".repeat(5_000))
        .await
        .expect("save alpha");
    store
        .write_session_topic("session-memory-many", "beta", &"b".repeat(5_000))
        .await
        .expect("save beta");

    let mut session = bamboo_agent_core::Session::new("session-memory-many", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;
    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert_eq!(
        system_prompt
            .matches("## External Memory (Persistent)")
            .count(),
        1
    );
    assert!(system_prompt.contains("### Session Memory Topic: `alpha`"));
    assert!(system_prompt.contains("### Session Memory Topic: `beta`"));
    assert!(system_prompt.contains("showing "));
    assert!(system_prompt.contains("use action=read topic=alpha"));
}

#[tokio::test]
async fn external_memory_renders_relevant_memory_section_for_project_hits() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-recall").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-recall-project");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "User prefers concise answers",
            "Keep responses concise and avoid unnecessary recap.",
            &["concise".to_string(), "style".to_string()],
            Some("session-recall-project"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save relevant project memory");

    let mut session = bamboo_agent_core::Session::new("session-recall-project", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user(
        "请记住我更喜欢 concise answers 并减少 recap",
    ));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("User prefers concise answers"));
    assert!(system_prompt.contains("Summary: Keep responses concise"));
    assert!(system_prompt.contains("[active][project]"));
}

#[tokio::test]
async fn external_memory_adds_stale_guidance_for_old_relevant_memory_hits() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-stale").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-recall-stale");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    let doc = project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Release freeze policy",
            "Mobile release freeze starts Tuesday and needs verification.",
            &["release".to_string(), "freeze".to_string()],
            Some("session-recall-stale"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save stale-eligible project memory");

    let raw = std::fs::read_to_string(&doc.path).expect("read stored memory doc");
    let old_timestamp = "2026-03-01T00:00:00Z";
    let rewritten = raw
        .replace(&doc.frontmatter.updated_at, old_timestamp)
        .replace(&doc.frontmatter.created_at, old_timestamp);
    std::fs::write(&doc.path, rewritten).expect("rewrite timestamps");
    project_memory
        .rebuild_scope(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
        )
        .await
        .expect("rebuild scope after timestamp rewrite");

    let mut session = bamboo_agent_core::Session::new("session-recall-stale", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user("release freeze policy"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("Release freeze policy"));
    assert!(
        system_prompt.contains("Historical memory")
            || system_prompt.contains("Older historical memory")
    );
    assert!(system_prompt.contains("verify against current"));
}

#[tokio::test]
async fn external_memory_omits_relevant_memory_section_when_no_match_exists() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-recall-none");
    std::fs::create_dir_all(&workspace).expect("workspace dir");

    let mut session = bamboo_agent_core::Session::new("session-recall-none", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user(
        "this query should not match anything relevant",
    ));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(!system_prompt.contains("### Relevant Durable Memories"));
}

#[tokio::test]
async fn external_memory_limits_relevant_memories_to_top_k() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-topk").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-recall-topk");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    for idx in 0..4 {
        project_memory
            .write_memory(
                bamboo_memory::memory_store::MemoryScope::Project,
                Some(project_key.as_str()),
                bamboo_memory::memory_store::DurableMemoryType::Project,
                &format!("Release freeze note {idx}"),
                &format!("release freeze detail {idx}"),
                &["release".to_string(), "freeze".to_string()],
                Some("session-recall-topk"),
                "main-model",
                false,
                None,
            )
            .await
            .expect("save project memory");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let mut session = bamboo_agent_core::Session::new("session-recall-topk", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user("release freeze"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert_eq!(system_prompt.matches("Summary:").count(), 3);
}

#[tokio::test]
async fn external_memory_uses_global_relevant_memory_fallback_only_when_project_has_no_hits() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id =
        bamboo_domain::ProjectId::parse("project-prompt-fallback").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-recall-fallback");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Unrelated project note",
            "This should not match the fallback query.",
            &["project".to_string()],
            Some("session-recall-fallback"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save unrelated project memory");
    store
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Global,
            None,
            bamboo_memory::memory_store::DurableMemoryType::Reference,
            "Global release guidance",
            "Use the release train checklist before shipping.",
            &["release".to_string(), "checklist".to_string()],
            Some("session-recall-fallback"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save global fallback memory");

    let mut session = bamboo_agent_core::Session::new("session-recall-fallback", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user("release checklist"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("Global release guidance"));
    assert!(system_prompt.contains("[active][global]"));
    assert!(!system_prompt.contains("Unrelated project note (score"));
}

#[tokio::test]
async fn external_memory_prefers_project_dream_over_global_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-dream").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-project-dream");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    publish_project_dream(
        &project_memory,
        project_key.as_str(),
        "# Bamboo Dream Notebook\n\nProject dream context",
    )
    .await;
    publish_global_dream(&store, "# Bamboo Dream Notebook\n\nGlobal dream fallback").await;

    let mut session = bamboo_agent_core::Session::new("session-project-dream", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Project Dream Summary"));
    assert!(system_prompt.contains("Project dream context"));
    assert!(!system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(!system_prompt.contains("Global dream fallback"));
}

#[tokio::test]
async fn external_memory_uses_global_dream_fallback_when_project_dream_and_index_are_missing() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-global-dream-fallback");
    std::fs::create_dir_all(&workspace).expect("workspace dir");

    publish_global_dream(&store, "# Bamboo Dream Notebook\n\nGlobal fallback dream").await;

    let mut session =
        bamboo_agent_core::Session::new("session-global-dream-fallback", "test-model");
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags::default(),
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(system_prompt.contains("Global fallback dream"));
    assert!(!system_prompt.contains("### Project Dream Summary"));
}

#[tokio::test]
async fn external_memory_omits_project_index_when_project_prompt_injection_disabled() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id =
        bamboo_domain::ProjectId::parse("project-prompt-disabled").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-no-project-index");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Project release rule",
            "Use the strict release checklist.",
            &["release".to_string()],
            Some("session-no-project-index"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save project memory");
    publish_project_dream(
        &project_memory,
        project_key.as_str(),
        "# Bamboo Dream Notebook\n\nProject dream remains available",
    )
    .await;

    let mut session = bamboo_agent_core::Session::new("session-no-project-index", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags {
            project_prompt_injection: false,
            ..crate::runtime::config::PromptMemoryFlags::default()
        },
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(!system_prompt.contains("### Project Durable Memory Index"));
    assert!(!system_prompt.contains("Project release rule"));

    let observability = session
        .metadata
        .get("runtime_prompt_memory_observability")
        .and_then(|raw| {
            serde_json::from_str::<bamboo_agent_core::PromptMemoryObservability>(raw).ok()
        })
        .expect("observability should be recorded");
    assert!(!observability.project_prompt_injection_enabled);
    assert!(!observability.relevant_recall_rerank_enabled);
    assert_eq!(observability.project_memory_index_status, "disabled");
    assert_eq!(observability.project_dream_status, "loaded");
    assert_eq!(
        observability.global_dream_fallback_status,
        "skipped_project_memory_or_dream_present"
    );
    assert_eq!(observability.dream_source, "project");
}

#[tokio::test]
async fn external_memory_omits_relevant_recall_and_uses_global_dream_when_project_first_disabled() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id =
        bamboo_domain::ProjectId::parse("project-prompt-global-mode").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-global-dream-mode");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "User prefers concise answers",
            "Keep answers concise.",
            &["concise".to_string()],
            Some("session-global-dream-mode"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save relevant project memory");
    publish_project_dream(
        &project_memory,
        project_key.as_str(),
        "# Bamboo Dream Notebook\n\nProject dream context",
    )
    .await;
    publish_global_dream(&store, "# Bamboo Dream Notebook\n\nGlobal dream fallback").await;

    let mut session = bamboo_agent_core::Session::new("session-global-dream-mode", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user("concise answers"));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags {
            relevant_recall: false,
            project_first_dream: false,
            ..crate::runtime::config::PromptMemoryFlags::default()
        },
        None,
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    assert!(!system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(system_prompt.contains("Global dream fallback"));
    assert!(!system_prompt.contains("### Project Dream Summary"));
    assert!(!system_prompt.contains("Project dream context"));

    let observability = session
        .metadata
        .get("runtime_prompt_memory_observability")
        .and_then(|raw| {
            serde_json::from_str::<bamboo_agent_core::PromptMemoryObservability>(raw).ok()
        })
        .expect("observability should be recorded");
    assert!(!observability.relevant_recall_enabled);
    assert!(!observability.relevant_recall_rerank_enabled);
    assert!(!observability.project_first_dream_enabled);
    assert_eq!(observability.relevant_memory_status, "disabled");
    assert_eq!(observability.global_dream_fallback_status, "forced_loaded");
    assert_eq!(observability.dream_source, "global_fallback");
}

#[tokio::test]
async fn external_memory_uses_model_rerank_for_relevant_memories_when_enabled() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id = bamboo_domain::ProjectId::parse("project-prompt-rerank").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-rerank-recall");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    let lexical_first = project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "Release freeze checklist",
            "Generic release freeze checklist for shipping work.",
            &["release".to_string(), "freeze".to_string()],
            Some("session-rerank-recall"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save lexical-first memory");
    let reranked_first = project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "Mobile launch blocker",
            "This durable note captures the release freeze decision for the mobile app and should be preferred for mobile freeze requests.",
            &["mobile".to_string(), "launch".to_string()],
            Some("session-rerank-recall"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save reranked-first memory");

    let provider = StaticResponseProvider::new(format!(
        "{{\"ids\":[\"{}\",\"{}\"]}}",
        reranked_first.frontmatter.id, lexical_first.frontmatter.id
    ));
    let requested_models = provider.requested_models.clone();
    let runtime_context = super::PromptMemoryRuntimeContext {
        llm: Arc::new(provider),
        background_model_name: Some("rerank-fast-model".to_string()),
    };

    let mut session = bamboo_agent_core::Session::new("session-rerank-recall", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user(
        "release freeze for mobile launch",
    ));

    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags {
            relevant_recall_rerank: true,
            ..crate::runtime::config::PromptMemoryFlags::default()
        },
        Some(&runtime_context),
    )
    .await;

    let system_prompt = super::render_external_memory_section(&session)
        .expect("external memory section should be rendered");

    let reranked_pos = system_prompt
        .find("Mobile launch blocker")
        .expect("reranked memory should be rendered");
    let lexical_pos = system_prompt
        .find("Release freeze checklist")
        .expect("lexical memory should be rendered");
    assert!(reranked_pos < lexical_pos);

    let observability = session
        .metadata
        .get("runtime_prompt_memory_observability")
        .and_then(|raw| {
            serde_json::from_str::<bamboo_agent_core::PromptMemoryObservability>(raw).ok()
        })
        .expect("observability should be recorded");
    assert!(observability.relevant_recall_enabled);
    assert!(observability.relevant_recall_rerank_enabled);
    assert_eq!(observability.relevant_memory_status, "reranked");
    assert_eq!(
        requested_models.lock().expect("lock poisoned").as_slice(),
        ["rerank-fast-model"]
    );
}

#[tokio::test]
async fn external_memory_uses_latest_background_model_on_repeated_refresh() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let project_id =
        bamboo_domain::ProjectId::parse("project-prompt-rerank-reload").expect("project id");
    let project_memory = store.for_project(&project_id);
    let workspace = temp_dir.path().join("workspace-rerank-reload");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = project_id.to_string();

    let lexical_first = project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "Release freeze checklist",
            "Generic release freeze checklist for shipping work.",
            &["release".to_string(), "freeze".to_string()],
            Some("session-rerank-reload"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save lexical-first memory");
    let reranked_first = project_memory
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Feedback,
            "Mobile launch blocker",
            "This durable note captures the release freeze decision for the mobile app and should be preferred for mobile freeze requests.",
            &["mobile".to_string(), "launch".to_string()],
            Some("session-rerank-reload"),
            "main-model",
            false,
            None,
        )
        .await
        .expect("save reranked-first memory");

    let provider = StaticResponseProvider::new(format!(
        "{{\"ids\":[\"{}\",\"{}\"]}}",
        reranked_first.frontmatter.id, lexical_first.frontmatter.id
    ));
    let requested_models = provider.requested_models.clone();

    let mut session = bamboo_agent_core::Session::new("session-rerank-reload", "test-model");
    session.set_project_id_meta(project_id.to_string());
    session.add_message(bamboo_agent_core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(bamboo_agent_core::Message::user(
        "release freeze for mobile launch",
    ));

    let runtime_context_v1 = super::PromptMemoryRuntimeContext {
        llm: Arc::new(provider.clone()),
        background_model_name: Some("bg-1".to_string()),
    };
    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags {
            relevant_recall_rerank: true,
            ..crate::runtime::config::PromptMemoryFlags::default()
        },
        Some(&runtime_context_v1),
    )
    .await;

    let runtime_context_v2 = super::PromptMemoryRuntimeContext {
        llm: Arc::new(provider.clone()),
        background_model_name: Some("bg-2".to_string()),
    };
    super::refresh_external_memory_context_with_store(
        &mut session,
        &store,
        crate::runtime::config::PromptMemoryFlags {
            relevant_recall_rerank: true,
            ..crate::runtime::config::PromptMemoryFlags::default()
        },
        Some(&runtime_context_v2),
    )
    .await;

    assert_eq!(
        requested_models.lock().expect("lock poisoned").as_slice(),
        ["bg-1", "bg-2"]
    );
}
