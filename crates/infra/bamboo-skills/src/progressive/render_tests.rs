// Copyright 2025 OpenAI. Licensed under Apache-2.0.
// Allocator fixtures adapted from Codex ext/skills/src/render_tests.rs at
// 7f892275e31002f0422477c6219189284560e689; see third_party/codex/NOTICE.
// Ordering and unavailable-alias boundary cases are Bamboo regression fixtures.
use super::*;
use std::num::NonZeroUsize;

// Bamboo-authored byte-boundary fixtures for the pinned narrow UTF-8 helper.
#[test]
fn explicit_skill_utf8_limits_are_bytes_and_preserve_complete_characters() {
    for (input, limit, expected, cut) in [
        ("", 0, "", false),
        ("abc", 0, "", true),
        ("abc", 3, "abc", false),
        ("é中🙂z", 1, "", true),
        ("é中🙂z", 2, "é", true),
        ("é中🙂z", 4, "é", true),
        ("é中🙂z", 5, "é中", true),
        ("é中🙂z", 8, "é中", true),
        ("é中🙂z", 9, "é中🙂", true),
        ("é中🙂z", 10, "é中🙂z", false),
        ("é中🙂z", usize::MAX, "é中🙂z", false),
    ] {
        assert_eq!(truncate_skill_utf8_bytes(input, limit), (expected, cut));
    }
    let exact = "🙂".repeat(2_000);
    assert_eq!(
        truncate_skill_utf8_bytes(&exact, EXPLICIT_SKILL_PROMPT_BYTES),
        (exact.as_str(), false)
    );
    let longer = format!("{exact}é");
    assert_eq!(
        truncate_skill_utf8_bytes(&longer, EXPLICIT_SKILL_PROMPT_BYTES),
        (exact.as_str(), true)
    );
    let crossing = format!("{}中", "a".repeat(7_999));
    assert_eq!(
        truncate_skill_utf8_bytes(&crossing, EXPLICIT_SKILL_PROMPT_BYTES)
            .0
            .len(),
        7_999
    );
}

fn entry(id: &str, description: &str, short: Option<&str>) -> SkillCatalogMetadata {
    SkillCatalogMetadata {
        package: id.into(),
        name: id.into(),
        description: description.into(),
        short_description: short.map(str::to_owned),
        main_resource: format!("/skills/{id}/SKILL.md"),
        source: crate::WorkflowSource::User,
        revision: 1,
        identity: id.into(),
        root: "/skills".into(),
        explicit: true,
        automatic: true,
    }
}

#[test]
fn catalog_budget_uses_context_percentage_or_character_fallback() {
    assert_eq!(
        skill_metadata_budget(Some(100_000), None),
        SkillMetadataBudget::Tokens(2_000)
    );
    assert_eq!(
        skill_metadata_budget(Some(400_000), None),
        SkillMetadataBudget::Tokens(8_000)
    );
    for window in [None, Some(0), Some(-1)] {
        assert_eq!(
            skill_metadata_budget(window, None),
            SkillMetadataBudget::Characters(8_000)
        );
    }
    assert_eq!(
        skill_metadata_budget(Some(100_000), NonZeroUsize::new(5_000)),
        SkillMetadataBudget::Tokens(5_000)
    );
    assert_eq!(
        skill_metadata_budget(None, NonZeroUsize::new(50_000)),
        SkillMetadataBudget::Tokens(10_000)
    );
}

#[test]
fn description_selection_follows_render_policy() {
    let entries = vec![
        entry("shortened", "full description", Some("short description")),
        entry("fallback", "fallback description", None),
    ];
    let core = render_skill_catalog(
        &entries,
        SkillCatalogRenderPolicy::CoreCompatible,
        SkillMetadataBudget::Characters(8_000),
    );
    let extension = render_skill_catalog(
        &entries,
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Characters(8_000),
    );
    assert!(core.text.contains("full description"));
    assert!(!core.text.contains("short description"));
    assert!(extension.text.contains("short description"));
    assert!(extension.text.contains("fallback description"));
}

#[test]
fn path_aliases_retain_every_skill_under_budget_pressure() {
    let root = "/Users/test/.codex/plugins/cache/example/hash1234567890/skills-with-a-very-long-shared-prefix";
    let entries = (0..12)
        .map(|index| {
            let mut item = entry(&format!("shared-root-skill-{index}"), "Description.", None);
            item.root = root.into();
            item.main_resource = format!("{root}/skill-{index}/SKILL.md");
            item
        })
        .collect::<Vec<_>>();
    let rendered = render_skill_catalog(
        &entries,
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Characters(800),
    );
    assert_eq!(rendered.included_count, 12);
    assert!(rendered.text.contains("### Skill roots"));
    assert!(rendered.text.contains("r0/skill-11/SKILL.md"));
    assert!(rendered.text.chars().count() <= 800);
}

#[test]
fn omission_notice_is_charged_and_round_robin_preserves_cjk() {
    let entries = (0..20)
        .map(|index| entry(&format!("skill-{index:02}"), &"界".repeat(1_024), None))
        .collect::<Vec<_>>();
    let rendered = render_skill_catalog(
        &entries,
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Tokens(100),
    );
    assert!(rendered.omitted_count > 0);
    assert!(rendered.text.contains("additional skills omitted"));
    assert!(SkillMetadataBudget::Tokens(100).cost(&rendered.text) <= 100);
    let core = render_skill_catalog(
        &entries,
        SkillCatalogRenderPolicy::CoreCompatible,
        SkillMetadataBudget::Tokens(100),
    );
    assert!(!core.text.contains("additional skills omitted"));
    let pair = vec![
        entry("one", &"界".repeat(1_024), None),
        entry("two", &"界".repeat(1_024), None),
    ];
    let characters = render_skill_catalog(
        &pair,
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Characters(200),
    );
    let tokens = render_skill_catalog(
        &pair,
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Tokens(50),
    );
    assert_eq!(characters.included_count, 2);
    for line in characters
        .text
        .lines()
        .filter(|line| line.contains("file:"))
    {
        assert!(line.contains('界'));
    }
    assert!(characters.text.chars().count() <= 200);
    assert!(characters.text.len() > tokens.text.len());
    let long = render_skill_catalog(
        &[entry("long", &"界".repeat(1_025), None)],
        SkillCatalogRenderPolicy::ExtensionCompatible,
        SkillMetadataBudget::Characters(8_000),
    );
    assert!(long.text.contains(&format!("{}...", "界".repeat(1_021))));
}

#[test]
fn core_metadata_order_is_deterministic_by_source_name_and_locator() {
    let mut entries = vec![
        entry("same", "", None),
        entry("same", "", None),
        entry("z-project", "", None),
        entry("a-user", "", None),
        entry("z-builtin", "", None),
    ];
    entries[0].main_resource = "/skills/z/SKILL.md".into();
    entries[1].main_resource = "/skills/a/SKILL.md".into();
    entries[2].source = crate::WorkflowSource::Project;
    entries[4].source = crate::WorkflowSource::Builtin;
    let render = |items: &[SkillCatalogMetadata]| {
        render_skill_catalog(
            items,
            SkillCatalogRenderPolicy::CoreCompatible,
            SkillMetadataBudget::Characters(8_000),
        )
    };
    let first = render(&entries);
    entries.reverse();
    assert_eq!(first.text, render(&entries).text);
    let lines = first.text.lines().collect::<Vec<_>>();
    assert!(lines[0].starts_with("- z-builtin:"));
    assert!(lines[1].starts_with("- z-project:"));
    assert!(lines[2].starts_with("- a-user:"));
    assert!(lines[3].contains("/skills/a/SKILL.md"));
    assert!(lines[4].contains("/skills/z/SKILL.md"));
}

#[test]
fn unavailable_alias_candidate_preserves_plain_omission_notice() {
    let root = format!("/{}", "r".repeat(300));
    let mut item = entry("long-root", "nonempty description", None);
    item.root = root.clone();
    item.main_resource = format!("{root}/long-root/SKILL.md");
    let budget = SkillMetadataBudget::Characters(80);
    assert!(budget.cost(&format!("### Skill roots\n- `r0` = `{root}`\n")) >= budget.limit());
    assert!(
        budget.cost("- 1 additional skill omitted from this bounded skills list.\n")
            <= budget.limit()
    );
    let rendered = render_skill_catalog(
        &[item],
        SkillCatalogRenderPolicy::ExtensionCompatible,
        budget,
    );
    assert_eq!(rendered.included_count, 0);
    assert_eq!(rendered.omitted_count, 1);
    assert_eq!(
        rendered.text,
        "- 1 additional skill omitted from this bounded skills list.\n"
    );
    assert!(budget.cost(&rendered.text) <= budget.limit());
}
