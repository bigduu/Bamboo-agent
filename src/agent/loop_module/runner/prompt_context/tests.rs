use super::merge_system_prompt_with_contexts;
use super::system_sections::{strip_existing_skill_context, strip_existing_tool_guide_context};

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
async fn inject_external_memory_includes_global_dream_fallback_and_session_note_when_project_unknown(
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    store
        .write_dream_view("# Bamboo Dream Notebook\n\nDurable cross-session insight")
        .await
        .expect("save dream notebook");
    store
        .write_session_topic("session-dream-test", "default", "Session durable note")
        .await
        .expect("save session note");

    let mut session = crate::agent::core::Session::new("session-dream-test", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("Global Dream Summary (fallback)"));
    assert!(system_prompt.contains("Durable cross-session insight"));
    assert!(system_prompt.contains("Session Memory Note"));
    assert!(system_prompt.contains("Session durable note"));
    assert!(!system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("Use the `memory` tool for durable project/global knowledge"));
    assert!(system_prompt.contains("prefer `memory` action=query first"));
}

#[tokio::test]
async fn inject_external_memory_includes_project_memory_index_and_omits_global_dream_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-alpha");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);

    store
        .write_memory(
            crate::agent::core::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            crate::agent::core::memory_store::DurableMemoryType::Project,
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

    let mut session = crate::agent::core::Session::new("session-project-memory", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("Release freeze begins next week"));
    assert!(!system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(!system_prompt.contains("Global fallback that should not appear"));
    assert!(system_prompt.contains("### Session Memory Note (markdown)"));
}

#[tokio::test]
async fn inject_external_memory_excludes_other_project_memory_index_content() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace_a = temp_dir.path().join("workspace-project-a");
    let workspace_b = temp_dir.path().join("workspace-project-b");
    std::fs::create_dir_all(&workspace_a).expect("workspace a dir");
    std::fs::create_dir_all(&workspace_b).expect("workspace b dir");
    let project_key_a = crate::agent::core::memory_store::project_key_from_path(&workspace_a);
    let project_key_b = crate::agent::core::memory_store::project_key_from_path(&workspace_b);

    store
        .write_memory(
            crate::agent::core::memory_store::MemoryScope::Project,
            Some(project_key_a.as_str()),
            crate::agent::core::memory_store::DurableMemoryType::Project,
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
            crate::agent::core::memory_store::MemoryScope::Project,
            Some(project_key_b.as_str()),
            crate::agent::core::memory_store::DurableMemoryType::Project,
            "Project B deployment rule",
            "Project B uses a separate deployment checklist.",
            &["deploy".to_string(), "project-b".to_string()],
            Some("session-project-b"),
            "main-model",
            false,
        )
        .await
        .expect("save project B memory");

    let mut session = crate::agent::core::Session::new("session-project-a", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace_a.to_string_lossy().to_string(),
    );

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("Project A release rule"));
    assert!(system_prompt.contains("Only Project A changes may ship this week."));
    assert!(!system_prompt.contains("Project B deployment rule"));
    assert!(!system_prompt.contains("Project B uses a separate deployment checklist."));
}

#[tokio::test]
async fn inject_external_memory_truncates_project_memory_index_and_adds_freshness_note() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-beta");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);
    let views_dir = store.resolver().views_dir(
        crate::agent::core::memory_store::MemoryScope::Project,
        Some(project_key.as_str()),
    );
    std::fs::create_dir_all(&views_dir).expect("views dir");
    let large_view = format!(
        "# Bamboo Memory Index (Project: {project_key})\n\n- `mem_old` Architectural note [project / active] updated 2026-03-01T00:00:00Z\n  - {}\n{}",
        "Older repo-state observation that needs verification.",
        "x".repeat(4_000)
    );
    std::fs::write(
        views_dir.join(crate::agent::core::memory_store::MEMORY_VIEW_FILE),
        large_view,
    )
    .expect("write memory view");

    let mut session =
        crate::agent::core::Session::new("session-project-memory-truncated", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Project Durable Memory Index"));
    assert!(system_prompt.contains("showing "));
    assert!(
        system_prompt.contains("Historical memory index entry")
            || system_prompt.contains("Older memory index entry")
    );
}

#[tokio::test]
async fn inject_external_memory_truncates_multi_topic_content_and_is_idempotent() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    store
        .write_session_topic("session-memory-many", "alpha", &"a".repeat(5_000))
        .await
        .expect("save alpha");
    store
        .write_session_topic("session-memory-many", "beta", &"b".repeat(5_000))
        .await
        .expect("save beta");

    let mut session = crate::agent::core::Session::new("session-memory-many", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;
    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

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
async fn inject_external_memory_renders_relevant_memory_section_for_project_hits() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-recall-project");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);

    store
        .write_memory(
            crate::agent::core::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            crate::agent::core::memory_store::DurableMemoryType::Feedback,
            "User prefers concise answers",
            "Keep responses concise and avoid unnecessary recap.",
            &["concise".to_string(), "style".to_string()],
            Some("session-recall-project"),
            "main-model",
            false,
        )
        .await
        .expect("save relevant project memory");

    let mut session = crate::agent::core::Session::new("session-recall-project", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(crate::agent::core::Message::user(
        "请记住我更喜欢 concise answers 并减少 recap",
    ));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("User prefers concise answers"));
    assert!(system_prompt.contains("Summary: Keep responses concise"));
    assert!(system_prompt.contains("[active][project]"));
}

#[tokio::test]
async fn inject_external_memory_omits_relevant_memory_section_when_no_match_exists() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-recall-none");
    std::fs::create_dir_all(&workspace).expect("workspace dir");

    let mut session = crate::agent::core::Session::new("session-recall-none", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(crate::agent::core::Message::user(
        "this query should not match anything relevant",
    ));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(!system_prompt.contains("### Relevant Durable Memories"));
}

#[tokio::test]
async fn inject_external_memory_limits_relevant_memories_to_top_k() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-recall-topk");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);

    for idx in 0..4 {
        store
            .write_memory(
                crate::agent::core::memory_store::MemoryScope::Project,
                Some(project_key.as_str()),
                crate::agent::core::memory_store::DurableMemoryType::Project,
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

    let mut session = crate::agent::core::Session::new("session-recall-topk", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(crate::agent::core::Message::user("release freeze"));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert_eq!(system_prompt.matches("Summary:").count(), 3);
}

#[tokio::test]
async fn inject_external_memory_uses_global_relevant_memory_fallback_only_when_project_has_no_hits()
{
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-recall-fallback");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);

    store
        .write_memory(
            crate::agent::core::memory_store::MemoryScope::Project,
            Some(project_key.as_str()),
            crate::agent::core::memory_store::DurableMemoryType::Project,
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
            crate::agent::core::memory_store::MemoryScope::Global,
            None,
            crate::agent::core::memory_store::DurableMemoryType::Reference,
            "Global release guidance",
            "Use the release train checklist before shipping.",
            &["release".to_string(), "checklist".to_string()],
            Some("session-recall-fallback"),
            "main-model",
            false,
        )
        .await
        .expect("save global fallback memory");

    let mut session = crate::agent::core::Session::new("session-recall-fallback", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );
    session.add_message(crate::agent::core::Message::user("release checklist"));

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Relevant Durable Memories"));
    assert!(system_prompt.contains("Global release guidance"));
    assert!(system_prompt.contains("[active][global]"));
    assert!(!system_prompt.contains("Unrelated project note (score"));
}

#[tokio::test]
async fn inject_external_memory_prefers_project_dream_over_global_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-project-dream");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    let project_key = crate::agent::core::memory_store::project_key_from_path(&workspace);

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

    let mut session = crate::agent::core::Session::new("session-project-dream", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Project Dream Summary"));
    assert!(system_prompt.contains("Project dream context"));
    assert!(!system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(!system_prompt.contains("Global dream fallback"));
}

#[tokio::test]
async fn inject_external_memory_uses_global_dream_fallback_when_project_dream_and_index_are_missing(
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = crate::agent::core::memory_store::MemoryStore::new(temp_dir.path());
    let workspace = temp_dir.path().join("workspace-global-dream-fallback");
    std::fs::create_dir_all(&workspace).expect("workspace dir");

    store
        .write_dream_view("# Bamboo Dream Notebook\n\nGlobal fallback dream")
        .await
        .expect("write global dream");

    let mut session =
        crate::agent::core::Session::new("session-global-dream-fallback", "test-model");
    session.add_message(crate::agent::core::Message::system("Base prompt"));
    session.metadata.insert(
        "workspace_path".to_string(),
        workspace.to_string_lossy().to_string(),
    );

    super::inject_external_memory_into_system_message_with_store(&mut session, &store).await;

    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
        .map(|message| message.content.clone())
        .expect("system prompt should exist");

    assert!(system_prompt.contains("### Global Dream Summary (fallback)"));
    assert!(system_prompt.contains("Global fallback dream"));
    assert!(!system_prompt.contains("### Project Dream Summary"));
}
