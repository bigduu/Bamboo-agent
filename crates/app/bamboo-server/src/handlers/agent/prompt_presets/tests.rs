use std::collections::HashSet;

use super::storage::{
    ensure_unique_preset_id, normalize_content, normalize_name, normalize_optional_description,
    sanitize_store, slugify_name,
};
use super::types::{PromptPresetStore, StoredPromptPreset, DEFAULT_PRESET_ID};

#[test]
fn slugify_name_returns_ascii_and_non_empty_value() {
    assert_eq!(slugify_name("My Prompt"), "my_prompt");
    assert!(slugify_name("测试提示词").starts_with("prompt_"));
}

#[test]
fn ensure_unique_preset_id_adds_suffix_when_needed() {
    let mut existing = HashSet::new();
    existing.insert("custom_prompt".to_string());
    existing.insert("custom_prompt_2".to_string());

    let resolved = ensure_unique_preset_id("custom_prompt", &existing);
    assert_eq!(resolved, "custom_prompt_3");
}

#[test]
fn ensure_unique_preset_id_respects_max_length_after_suffix() {
    let base = "a".repeat(80);
    let mut existing = HashSet::new();
    existing.insert(base.clone());

    let resolved = ensure_unique_preset_id(&base, &existing);
    assert_eq!(resolved.len(), 80);
    assert!(resolved.ends_with("_2"));
}

#[test]
fn sanitize_store_removes_invalid_and_reserved_items() {
    let mut store = PromptPresetStore {
        prompts: vec![
            StoredPromptPreset {
                id: DEFAULT_PRESET_ID.to_string(),
                name: "Reserved".to_string(),
                description: None,
                content: "Should be removed".to_string(),
            },
            StoredPromptPreset {
                id: "valid_prompt".to_string(),
                name: "  Valid Prompt  ".to_string(),
                description: Some("  Desc  ".to_string()),
                content: "  Content  ".to_string(),
            },
            StoredPromptPreset {
                id: "invalid-id".to_string(),
                name: "Invalid ID".to_string(),
                description: None,
                content: "Content".to_string(),
            },
            StoredPromptPreset {
                id: "valid_prompt".to_string(),
                name: "Duplicate".to_string(),
                description: None,
                content: "Duplicate".to_string(),
            },
        ],
    };

    sanitize_store(&mut store);

    assert_eq!(store.prompts.len(), 1);
    assert_eq!(store.prompts[0].id, "valid_prompt");
    assert_eq!(store.prompts[0].name, "Valid Prompt");
    assert_eq!(store.prompts[0].description.as_deref(), Some("Desc"));
    assert_eq!(store.prompts[0].content, "Content");
}

#[test]
fn normalize_helpers_trim_and_filter_empty_values() {
    assert_eq!(normalize_name("  name ").as_deref(), Some("name"));
    assert_eq!(normalize_name("   "), None);

    assert_eq!(normalize_content("  content ").as_deref(), Some("content"));
    assert_eq!(normalize_content("  "), None);

    assert_eq!(
        normalize_optional_description(Some("  desc ")).as_deref(),
        Some("desc")
    );
    assert_eq!(normalize_optional_description(Some("   ")), None);
    assert_eq!(normalize_optional_description(None), None);
}
