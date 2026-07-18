use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tokio::fs;
use uuid::Uuid;

use crate::error::AppError;

use super::types::{PromptPresetStore, StoredPromptPreset, DEFAULT_PRESET_ID};

const STORE_FILE_NAME: &str = "prompt-presets.json";
const MAX_PRESET_ID_LEN: usize = 80;

pub(crate) fn store_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join(STORE_FILE_NAME)
}

pub(crate) async fn load_store(path: &Path) -> Result<PromptPresetStore, AppError> {
    if !path.exists() {
        return Ok(PromptPresetStore::default());
    }

    let raw = fs::read_to_string(path).await?;
    if raw.trim().is_empty() {
        return Ok(PromptPresetStore::default());
    }

    let mut store: PromptPresetStore = serde_json::from_str(&raw)?;
    sanitize_store(&mut store);
    Ok(store)
}

pub(crate) async fn load_stored_presets(
    app_data_dir: &Path,
) -> Result<Vec<StoredPromptPreset>, AppError> {
    Ok(load_store(&store_file_path(app_data_dir)).await?.prompts)
}

/// # Non-atomic write (known, deferred gap)
///
/// This writes `path` in place (`fs::write`), so a hard kill mid-write can
/// truncate `prompt-presets.json`. `bamboo_plugin::registry::InstalledPlugins::save`
/// (`installed.json`) fixed the identical gap by writing to a sibling
/// `<path>.tmp` then `rename`-ing over the target (atomic on the same
/// filesystem) — this function should get the same treatment, but that's
/// deferred here since it's pre-existing behaviour (this crate's plugin
/// installer now just calls it a lot more often than before, via
/// `register_prompts`/`remove_prompt_preset`), not new code on this branch.
pub(crate) async fn save_store(path: &Path, store: &PromptPresetStore) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let serialized = serde_json::to_string_pretty(store)?;
    fs::write(path, serialized).await?;
    Ok(())
}

pub(super) fn read_default_prompt_content() -> String {
    bamboo_engine::prompt_defaults::read_global_default_system_prompt_template()
}

pub(super) fn normalize_name(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(super) fn normalize_content(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(super) fn normalize_optional_description(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub(super) fn validate_preset_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_PRESET_ID_LEN
        && id
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

pub(super) fn slugify_name(name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    let mut prev_sep = false;

    for ch in name.trim().chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_lowercase() || lower.is_ascii_digit() {
            slug.push(lower);
            prev_sep = false;
        } else if !prev_sep {
            slug.push('_');
            prev_sep = true;
        }
    }

    let trimmed = slug.trim_matches('_');
    if trimmed.is_empty() {
        format!("prompt_{}", &Uuid::new_v4().simple().to_string()[..8])
    } else {
        trimmed.chars().take(MAX_PRESET_ID_LEN).collect()
    }
}

pub(crate) fn ensure_unique_preset_id(base_id: &str, existing: &HashSet<String>) -> String {
    let mut normalized_base = base_id.trim().to_string();
    if normalized_base.len() > MAX_PRESET_ID_LEN {
        normalized_base = normalized_base.chars().take(MAX_PRESET_ID_LEN).collect();
    }

    if !existing.contains(&normalized_base) {
        return normalized_base;
    }

    let mut index = 2usize;
    loop {
        let suffix = format!("_{index}");
        let max_base_len = MAX_PRESET_ID_LEN.saturating_sub(suffix.len());
        let base_prefix: String = normalized_base.chars().take(max_base_len).collect();
        let candidate = format!("{base_prefix}{suffix}");
        if !existing.contains(&candidate) {
            return candidate;
        }
        index += 1;
    }
}

pub(super) fn sanitize_store(store: &mut PromptPresetStore) {
    let mut seen = HashSet::new();
    store.prompts.retain(|preset| {
        if preset.id == DEFAULT_PRESET_ID || !validate_preset_id(&preset.id) {
            return false;
        }
        if seen.contains(&preset.id) {
            return false;
        }
        if normalize_name(&preset.name).is_none() || normalize_content(&preset.content).is_none() {
            return false;
        }

        seen.insert(preset.id.clone());
        true
    });

    for preset in &mut store.prompts {
        if let Some(name) = normalize_name(&preset.name) {
            preset.name = name;
        }
        if let Some(content) = normalize_content(&preset.content) {
            preset.content = content;
        }
        preset.description = normalize_optional_description(preset.description.as_deref());
    }
}

pub(super) fn to_custom_item(preset: &StoredPromptPreset) -> super::types::PromptPresetItem {
    super::types::PromptPresetItem {
        id: preset.id.clone(),
        name: preset.name.clone(),
        description: preset.description.clone(),
        content: preset.content.clone(),
        is_default: false,
    }
}
