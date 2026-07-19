//! Skill access control — authorization checks for skill loading and resource reads.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY, SELECTED_SKILL_IDS_METADATA_KEY,
    SELECTED_SKILL_MODE_METADATA_KEY, SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
};
use crate::selection::parse_selected_skill_ids_metadata;
pub use crate::session_port::SkillSessionPort;

/// Error type for skill access control operations.
#[derive(Debug, thiserror::Error)]
pub enum SkillAccessError {
    #[error("{0}")]
    NotAllowed(String),
    #[error("{0}")]
    NotLoaded(String),
    #[error("{0}")]
    SessionRequired(String),
    #[error("{0}")]
    SessionNotFound(String),
    #[error("{0}")]
    PersistenceError(String),
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

pub fn parse_loaded_skill_ids(raw: &str) -> HashSet<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return HashSet::new();
    }

    if let Ok(ids) = serde_json::from_str::<Vec<String>>(trimmed) {
        return ids
            .into_iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();
    }

    trimmed
        .split(',')
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect()
}

pub fn serialize_loaded_skill_ids(ids: &HashSet<String>) -> String {
    let sorted: BTreeSet<String> = ids
        .iter()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect();
    serde_json::to_string(&sorted.into_iter().collect::<Vec<String>>()).unwrap_or("[]".to_string())
}

pub fn extract_skill_allowlist(metadata: &HashMap<String, String>) -> Option<HashSet<String>> {
    if let Some(raw) = metadata.get(SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY) {
        return Some(parse_loaded_skill_ids(raw));
    }

    metadata
        .get(SELECTED_SKILL_IDS_METADATA_KEY)
        .and_then(|raw| parse_selected_skill_ids_metadata(raw))
        .map(|ids| ids.into_iter().collect())
}

pub fn extract_skill_mode(metadata: &HashMap<String, String>) -> Option<String> {
    let mode = metadata
        .get(SELECTED_SKILL_MODE_METADATA_KEY)
        .or_else(|| metadata.get("mode"))?;
    let trimmed = mode.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn extract_loaded_ids_from_metadata(metadata: &HashMap<String, String>) -> HashSet<String> {
    metadata
        .get(LOADED_SKILL_IDS_METADATA_KEY)
        .map(|raw| parse_loaded_skill_ids(raw))
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Access control functions
// ---------------------------------------------------------------------------

pub async fn ensure_skill_allowed(
    port: &dyn SkillSessionPort,
    skill_id: &str,
    session_id: Option<&str>,
) -> Result<(), SkillAccessError> {
    let disabled = port.disabled_skill_ids().await;
    if disabled.contains(skill_id) {
        return Err(SkillAccessError::NotAllowed(format!(
            "Skill '{skill_id}' is globally disabled in Bamboo settings"
        )));
    }

    let Some(session_id) = session_id else {
        return Ok(());
    };

    let Some(metadata) = port.load_session_metadata(session_id).await else {
        return Err(SkillAccessError::SessionNotFound(format!(
            "Session '{session_id}' was not found while verifying skill selection"
        )));
    };

    let Some(allowlist) = extract_skill_allowlist(&metadata) else {
        return Err(SkillAccessError::NotAllowed(format!(
            "Skill '{skill_id}' cannot load because this request has no published skill selection"
        )));
    };

    if allowlist.contains(skill_id) {
        return Ok(());
    }

    Err(SkillAccessError::NotAllowed(format!(
        "Skill '{skill_id}' is not selected for this request"
    )))
}

pub async fn ensure_skill_loaded(
    port: &dyn SkillSessionPort,
    skill_id: &str,
    session_id: Option<&str>,
) -> Result<(), SkillAccessError> {
    let Some(session_id) = session_id else {
        return Err(SkillAccessError::SessionRequired(
            "read_skill_resource requires a session_id in tool context".to_string(),
        ));
    };

    let Some(metadata) = port.load_session_metadata(session_id).await else {
        return Err(SkillAccessError::SessionNotFound(format!(
            "Session '{session_id}' was not found while verifying loaded skill state"
        )));
    };

    let loaded_ids = extract_loaded_ids_from_metadata(&metadata);

    if loaded_ids.contains(skill_id) {
        return Ok(());
    }

    Err(SkillAccessError::NotLoaded(format!(
        "Skill '{skill_id}' has not been loaded in this session. Call load_skill first."
    )))
}

pub async fn mark_skill_loaded(
    port: &dyn SkillSessionPort,
    skill_id: &str,
    session_id: Option<&str>,
) -> Result<(), SkillAccessError> {
    let Some(session_id) = session_id else {
        return Err(SkillAccessError::SessionRequired(
            "load_skill requires a session_id in tool context".to_string(),
        ));
    };

    let metadata = port
        .load_session_metadata(session_id)
        .await
        .ok_or_else(|| {
            SkillAccessError::SessionNotFound(format!(
                "Session '{session_id}' not found while persisting loaded skill state"
            ))
        })?;

    let mut loaded_ids = extract_loaded_ids_from_metadata(&metadata);
    loaded_ids.insert(skill_id.to_string());

    let serialized_ids = serialize_loaded_skill_ids(&loaded_ids);
    let summary = serde_json::json!({
        "skill_id": skill_id,
        "loaded_ids": loaded_ids.iter().cloned().collect::<BTreeSet<_>>(),
        "selected_skill_mode": metadata.get(SELECTED_SKILL_MODE_METADATA_KEY).cloned(),
        "loaded_count": loaded_ids.len()
    })
    .to_string();

    let updates = vec![
        (
            LOADED_SKILL_IDS_METADATA_KEY.to_string(),
            Some(serialized_ids),
        ),
        (
            LAST_LOADED_SKILL_ID_METADATA_KEY.to_string(),
            Some(skill_id.to_string()),
        ),
        (
            LAST_LOADED_SKILL_SUMMARY_METADATA_KEY.to_string(),
            Some(summary),
        ),
    ];

    port.save_metadata_updates(session_id, &updates)
        .await
        .map_err(SkillAccessError::PersistenceError)?;

    Ok(())
}

pub async fn selected_skill_allowlist(
    port: &dyn SkillSessionPort,
    session_id: Option<&str>,
) -> Option<HashSet<String>> {
    let session_id = session_id?;
    let metadata = port.load_session_metadata(session_id).await?;
    extract_skill_allowlist(&metadata)
}

pub async fn selected_skill_mode(
    port: &dyn SkillSessionPort,
    session_id: Option<&str>,
) -> Option<String> {
    let session_id = session_id?;
    let metadata = port.load_session_metadata(session_id).await?;
    extract_skill_mode(&metadata)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_loaded_skill_ids_supports_json_and_csv() {
        let from_json = parse_loaded_skill_ids(r#"["skill-b","skill-a","skill-a"]"#);
        assert_eq!(from_json.len(), 2);
        assert!(from_json.contains("skill-a"));
        assert!(from_json.contains("skill-b"));

        let from_csv = parse_loaded_skill_ids("skill-c, skill-d , skill-c");
        assert_eq!(from_csv.len(), 2);
        assert!(from_csv.contains("skill-c"));
        assert!(from_csv.contains("skill-d"));
    }

    #[test]
    fn serialize_loaded_skill_ids_is_stable_and_sorted() {
        let mut ids = HashSet::new();
        ids.insert("skill-b".to_string());
        ids.insert("skill-a".to_string());

        assert_eq!(serialize_loaded_skill_ids(&ids), r#"["skill-a","skill-b"]"#);
    }

    #[test]
    fn extract_skill_allowlist_parses_metadata_json() {
        let mut metadata = HashMap::new();
        metadata.insert(
            "selected_skill_ids".to_string(),
            r#"["pdf","skill-creator"]"#.to_string(),
        );

        let allowlist = extract_skill_allowlist(&metadata).unwrap();
        assert!(allowlist.contains("pdf"));
        assert!(allowlist.contains("skill-creator"));
    }

    #[test]
    fn runtime_advertised_ids_override_requested_ids_and_preserve_empty_allowlist() {
        let mut metadata = HashMap::new();
        metadata.insert(
            SELECTED_SKILL_IDS_METADATA_KEY.to_string(),
            r#"["plan"]"#.to_string(),
        );
        metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            r#"["review"]"#.to_string(),
        );
        let allowlist = extract_skill_allowlist(&metadata).expect("runtime allowlist");
        assert_eq!(allowlist, HashSet::from(["review".to_string()]));

        metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            "[]".to_string(),
        );
        assert!(extract_skill_allowlist(&metadata)
            .expect("empty runtime allowlist")
            .is_empty());
    }

    #[test]
    fn extract_skill_mode_prefers_skill_mode_key() {
        let mut metadata = HashMap::new();
        metadata.insert("mode".to_string(), "ask".to_string());
        metadata.insert("skill_mode".to_string(), "code".to_string());

        assert_eq!(extract_skill_mode(&metadata).as_deref(), Some("code"));
    }
}
