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
async fn inject_external_memory_includes_dream_notebook_and_session_note() {
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

    assert!(system_prompt.contains("Cross-session Dream Notebook"));
    assert!(system_prompt.contains("Durable cross-session insight"));
    assert!(system_prompt.contains("Session Memory Note"));
    assert!(system_prompt.contains("Session durable note"));
    assert!(system_prompt.contains("Use the `memory` tool for durable project/global knowledge"));
    assert!(system_prompt.contains("prefer `memory` action=query first"));
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

    assert_eq!(system_prompt.matches("## External Memory (Persistent)").count(), 1);
    assert!(system_prompt.contains("### Session Memory Topic: `alpha`"));
    assert!(system_prompt.contains("### Session Memory Topic: `beta`"));
    assert!(system_prompt.contains("showing "));
    assert!(system_prompt.contains("use action=read topic=alpha"));
}
