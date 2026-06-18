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
    store
        .write_dream_view("# Bamboo Dream Notebook\n\nDurable cross-session insight")
        .await
        .expect("save dream notebook");
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
async fn external_memory_includes_project_memory_index_and_omits_global_dream_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-alpha");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
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
        )
        .await
        .expect("save project memory");
    store
        .write_dream_view("# Bamboo Dream Notebook\n\nGlobal fallback that should not appear")
        .await
        .expect("save dream notebook");

    let mut session = bamboo_agent_core::Session::new("session-project-memory", "test-model");
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
    let workspace_a = temp_dir.path().join("workspace-project-a");
    let workspace_b = temp_dir.path().join("workspace-project-b");
    std::fs::create_dir_all(&workspace_a).expect("workspace a dir");
    std::fs::create_dir_all(&workspace_b).expect("workspace b dir");
    let project_key_a = bamboo_memory::memory_store::project_key_from_path(&workspace_a);
    let project_key_b = bamboo_memory::memory_store::project_key_from_path(&workspace_b);

    store
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
        )
        .await
        .expect("save project A memory");
    store
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
        )
        .await
        .expect("save project B memory");

    let mut session = bamboo_agent_core::Session::new("session-project-a", "test-model");
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
async fn external_memory_truncates_project_memory_index_and_adds_freshness_note() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = bamboo_memory::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-beta");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);
    let views_dir = store.resolver().views_dir(
        bamboo_memory::memory_store::MemoryScope::Project,
        Some(project_key.as_str()),
    );
    std::fs::create_dir_all(&views_dir).expect("views dir");
    let large_view = format!(
        "# Bamboo Memory Index (Project: {project_key})\n\n- `mem_old` Architectural note [project / active] updated 2026-03-01T00:00:00Z\n  - {}\n{}",
        "Older repo-state observation that needs verification.",
        "x".repeat(4_000)
    );
    std::fs::write(
        views_dir.join(bamboo_memory::memory_store::MEMORY_VIEW_FILE),
        large_view,
    )
    .expect("write memory view");

    let mut session =
        bamboo_agent_core::Session::new("session-project-memory-truncated", "test-model");
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
    assert!(
        system_prompt.contains("Historical memory index entry")
            || system_prompt.contains("Older memory index entry")
    );
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
    let workspace = temp_dir.path().join("workspace-recall-project");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
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
        )
        .await
        .expect("save relevant project memory");

    let mut session = bamboo_agent_core::Session::new("session-recall-project", "test-model");
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
    let workspace = temp_dir.path().join("workspace-recall-stale");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    let doc = store
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
        )
        .await
        .expect("save stale-eligible project memory");

    let raw = std::fs::read_to_string(&doc.path).expect("read stored memory doc");
    let old_timestamp = "2026-03-01T00:00:00Z";
    let rewritten = raw
        .replace(&doc.frontmatter.updated_at, old_timestamp)
        .replace(&doc.frontmatter.created_at, old_timestamp);
    std::fs::write(&doc.path, rewritten).expect("rewrite timestamps");
    store
        .rebuild_scope(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
        )
        .await
        .expect("rebuild scope after timestamp rewrite");

    let mut session = bamboo_agent_core::Session::new("session-recall-stale", "test-model");
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
    let workspace = temp_dir.path().join("workspace-recall-topk");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    for idx in 0..4 {
        store
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
            )
            .await
            .expect("save project memory");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let mut session = bamboo_agent_core::Session::new("session-recall-topk", "test-model");
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
    let workspace = temp_dir.path().join("workspace-recall-fallback");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
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
        )
        .await
        .expect("save global fallback memory");

    let mut session = bamboo_agent_core::Session::new("session-recall-fallback", "test-model");
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
    let workspace = temp_dir.path().join("workspace-project-dream");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
        .write_project_dream_view(
            project_key.as_str(),
            "# Bamboo Dream Notebook\n\nProject dream context",
        )
        .await
        .expect("write project dream");
    store
        .write_dream_view("# Bamboo Dream Notebook\n\nGlobal dream fallback")
        .await
        .expect("write global dream");

    let mut session = bamboo_agent_core::Session::new("session-project-dream", "test-model");
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

    store
        .write_dream_view("# Bamboo Dream Notebook\n\nGlobal fallback dream")
        .await
        .expect("write global dream");

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
    let workspace = temp_dir.path().join("workspace-no-project-index");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
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
        )
        .await
        .expect("save project memory");

    let mut session = bamboo_agent_core::Session::new("session-no-project-index", "test-model");
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
    let workspace = temp_dir.path().join("workspace-global-dream-mode");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    store
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
        )
        .await
        .expect("save relevant project memory");
    store
        .write_project_dream_view(
            project_key.as_str(),
            "# Bamboo Dream Notebook\n\nProject dream context",
        )
        .await
        .expect("write project dream");
    store
        .write_dream_view("# Bamboo Dream Notebook\n\nGlobal dream fallback")
        .await
        .expect("write global dream");

    let mut session = bamboo_agent_core::Session::new("session-global-dream-mode", "test-model");
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
    let workspace = temp_dir.path().join("workspace-rerank-recall");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    let lexical_first = store
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
        )
        .await
        .expect("save lexical-first memory");
    let reranked_first = store
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
    let workspace = temp_dir.path().join("workspace-rerank-reload");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

    let lexical_first = store
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
        )
        .await
        .expect("save lexical-first memory");
    let reranked_first = store
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
        )
        .await
        .expect("save reranked-first memory");

    let provider = StaticResponseProvider::new(format!(
        "{{\"ids\":[\"{}\",\"{}\"]}}",
        reranked_first.frontmatter.id, lexical_first.frontmatter.id
    ));
    let requested_models = provider.requested_models.clone();

    let mut session = bamboo_agent_core::Session::new("session-rerank-reload", "test-model");
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
