use std::collections::{BTreeMap, HashMap};

use crate::store::SkillActivationDescriptor;

pub const SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY: &str = "skill_runtime_selected_skill_ids";
pub const SKILL_RUNTIME_SELECTION_SOURCE_KEY: &str = "skill_runtime_selection_source";
pub const SKILL_RUNTIME_SELECTION_TRACE_KEY: &str = "skill_runtime_selection_trace";
pub const SKILL_RUNTIME_SELECTION_COUNT_KEY: &str = "skill_runtime_selection_count";
pub const SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY: &str = "skill_runtime_selected_skill_mode";
pub const SKILL_RUNTIME_ACTIVATION_GENERATION_KEY: &str = "skill_runtime_activation_generation";
pub const SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY: &str =
    "skill_runtime_selected_skill_revisions";
pub const SKILL_RUNTIME_ACTIVATION_ERROR_KEY: &str = "skill_runtime_activation_error";
pub const SKILL_RUNTIME_PINNED_SNAPSHOT_KEY: &str = "skill_runtime_pinned_snapshot_v1";
pub const SKILL_RUNTIME_SELECTED_CATALOG_KEY: &str = "skill_runtime_selected_catalog_v1";

pub const SELECTED_SKILL_IDS_METADATA_KEY: &str = "selected_skill_ids";
pub const SELECTED_SKILL_MODE_METADATA_KEY: &str = "skill_mode";

pub const LOADED_SKILL_IDS_METADATA_KEY: &str = "skill_runtime_loaded_skill_ids";
pub const LAST_LOADED_SKILL_ID_METADATA_KEY: &str = "skill_runtime_last_loaded_skill_id";
pub const LAST_LOADED_SKILL_SUMMARY_METADATA_KEY: &str = "skill_runtime_last_load_summary";
pub const LAST_RESOURCE_READ_SUMMARY_METADATA_KEY: &str =
    "skill_runtime_last_resource_read_summary";

/// Validate runner-owned immutable activation metadata against the snapshot
/// retained in memory. `Ok(false)` is reserved for legacy/direct callers that
/// have no generation marker and may establish a pin lazily.
pub fn validate_pinned_activation_metadata(
    metadata: &HashMap<String, String>,
    descriptor: Option<&SkillActivationDescriptor>,
    required_skill_id: Option<&str>,
) -> Result<bool, String> {
    let Some(raw_generation) = metadata.get(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY) else {
        return Ok(false);
    };
    let expected_generation = raw_generation.parse::<u64>().map_err(|_| {
        "Invalid pinned workflow activation generation; retry as a new activation".to_string()
    })?;
    let descriptor = descriptor.ok_or_else(|| {
        "Pinned workflow activation is unavailable; retry as a new activation".to_string()
    })?;
    let expected_revisions = metadata
        .get(SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY)
        .and_then(|raw| serde_json::from_str::<BTreeMap<String, u64>>(raw).ok())
        .ok_or_else(|| {
            "Pinned workflow activation revisions are missing or invalid; retry as a new activation"
                .to_string()
        })?;
    let required_skill_is_pinned = required_skill_id
        .map(|skill_id| descriptor.skill_revisions.contains_key(skill_id))
        .unwrap_or(true);
    if descriptor.catalog_revision != expected_generation
        || descriptor.skill_revisions != expected_revisions
        || !required_skill_is_pinned
    {
        return Err(
            "Pinned workflow activation metadata does not match the immutable snapshot; retry as a new activation"
                .to_string(),
        );
    }
    Ok(true)
}
