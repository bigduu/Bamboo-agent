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
    assert!(merged.contains("## Skill System"));
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
        "## Skill System\n\n### Available Skills\n\n## Tool Usage Guidelines\n\n### File Reading Tools"
    );
}

#[test]
fn strip_existing_skill_context_removes_previous_section() {
    let stripped = strip_existing_skill_context(
        "Base prompt\n\n## Skill System\n\n### Available Skills\nInstructions",
    );
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_skill_context_removes_legacy_section_marker() {
    let stripped =
        strip_existing_skill_context("Base prompt\n\n## Available Skills\n\n### One\nInstructions");
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_tool_guide_context_removes_previous_section() {
    let stripped = strip_existing_tool_guide_context(
        "Base prompt\n\n## Tool Usage Guidelines\n\n### File Reading Tools\nInstructions",
    );
    assert_eq!(stripped, "Base prompt");
}
