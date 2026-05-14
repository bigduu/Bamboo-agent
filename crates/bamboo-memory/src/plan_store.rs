//! Plan artifact persistence for plan mode sessions.
//!
//! Stores plan artifacts in `${BAMBOO_DATA_DIR}/plan/{session_slug}/`:
//! - `plan.md` — human-readable plan markdown
//! - `state.json` — machine-readable plan runtime state
//! - `cursor.json` — machine-readable execution/resume cursor
//! - `sections.json` — structured plan section index for precise resume
//!
//! For backward compatibility, reads also fall back to the legacy flat file
//! layout `${BAMBOO_DATA_DIR}/plan/{session_slug}.md` when present.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const PLAN_FILE_NAME: &str = "plan.md";
const PLAN_STATE_FILE_NAME: &str = "state.json";
const PLAN_CURSOR_FILE_NAME: &str = "cursor.json";
const PLAN_SECTIONS_FILE_NAME: &str = "sections.json";
const PLAN_ARTIFACT_VERSION: u32 = 1;

/// Error type for plan store operations.
#[derive(Debug, thiserror::Error)]
pub enum PlanStoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Plan directory not accessible: {0}")]
    DirectoryNotAccessible(String),
}

/// Durable machine-readable state for a plan mode session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanStateArtifact {
    pub version: u32,
    pub session_id: String,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_hint: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
}

impl PlanStateArtifact {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            version: PLAN_ARTIFACT_VERSION,
            session_id: session_id.into(),
            updated_at: Utc::now(),
            status: None,
            active_task_id: None,
            active_step_id: None,
            next_step_id: None,
            active_section_id: None,
            next_section_id: None,
            last_completed_task_id: None,
            last_completed_section_id: None,
            round_hint: None,
            plan_hash: None,
        }
    }
}

/// Durable machine-readable execution cursor for resuming plan execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanCursorArtifact {
    pub version: u32,
    pub session_id: String,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_task_ordinal: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_task_ordinal: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_checkpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_hint: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_id_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension_hook_point: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_boundary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_note: Option<String>,
}

impl PlanCursorArtifact {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            version: PLAN_ARTIFACT_VERSION,
            session_id: session_id.into(),
            updated_at: Utc::now(),
            cursor_type: None,
            current_task_id: None,
            current_task_ordinal: None,
            current_step_id: None,
            current_section_id: None,
            next_task_id: None,
            next_task_ordinal: None,
            next_section_id: None,
            last_completed_task_id: None,
            last_completed_section_id: None,
            last_completed_checkpoint: None,
            round_hint: None,
            round_id_hint: None,
            suspension_hook_point: None,
            tool_call_boundary: None,
            resume_note: None,
        }
    }
}

/// Indexed section metadata extracted from persisted plan markdown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanSectionArtifact {
    pub version: u32,
    pub session_id: String,
    pub updated_at: DateTime<Utc>,
    pub sections: Vec<PlanSection>,
}

impl PlanSectionArtifact {
    pub fn new(session_id: impl Into<String>, sections: Vec<PlanSection>) -> Self {
        Self {
            version: PLAN_ARTIFACT_VERSION,
            session_id: session_id.into(),
            updated_at: Utc::now(),
            sections,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanSection {
    pub id: String,
    pub heading: String,
    pub level: u8,
    pub line_start: usize,
    pub line_end: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub anchor_terms: Vec<String>,
}

/// Stores and retrieves plan artifacts for sessions.
#[derive(Debug, Clone)]
pub struct PlanStore {
    plans_dir: PathBuf,
}

impl PlanStore {
    /// Create a new plan store with the given base data directory.
    ///
    /// Artifacts are stored in `{data_dir}/plan/`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, PlanStoreError> {
        let plans_dir = data_dir.as_ref().join("plan");
        fs::create_dir_all(&plans_dir).map_err(|e| {
            PlanStoreError::DirectoryNotAccessible(format!(
                "Failed to create plan directory at {}: {}",
                plans_dir.display(),
                e
            ))
        })?;
        Ok(Self { plans_dir })
    }

    /// Generate a slug from a session ID suitable for use as a filename.
    ///
    /// Uses the first 8 characters of the session ID plus a hash of the remainder
    /// to produce a short, deterministic, filesystem-safe identifier.
    fn session_slug(session_id: &str) -> String {
        let clean = session_id
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect::<String>();

        if clean.len() <= 16 {
            return clean;
        }

        let prefix: String = clean.chars().take(8).collect();
        let suffix: String = clean
            .chars()
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!(
            "{}-{}-{:x}",
            prefix,
            suffix,
            seahash::hash(clean.as_bytes())
        )
    }

    fn session_dir_path_internal(&self, session_id: &str) -> PathBuf {
        self.plans_dir.join(Self::session_slug(session_id))
    }

    fn preferred_plan_file_path(&self, session_id: &str) -> PathBuf {
        self.session_dir_path_internal(session_id)
            .join(PLAN_FILE_NAME)
    }

    fn legacy_plan_file_path(&self, session_id: &str) -> PathBuf {
        self.plans_dir
            .join(format!("{}.md", Self::session_slug(session_id)))
    }

    fn resolved_plan_file_path_internal(&self, session_id: &str) -> PathBuf {
        let preferred = self.preferred_plan_file_path(session_id);
        if preferred.exists() {
            preferred
        } else {
            let legacy = self.legacy_plan_file_path(session_id);
            if legacy.exists() {
                legacy
            } else {
                preferred
            }
        }
    }

    fn ensure_session_dir(&self, session_id: &str) -> Result<PathBuf, PlanStoreError> {
        let dir = self.session_dir_path_internal(session_id);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn unique_temp_path(path: &Path) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("artifact");
        path.with_file_name(format!(".{file_name}.{nanos}.{}.tmp", std::process::id()))
    }

    fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), PlanStoreError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = Self::unique_temp_path(path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp_path, path)?;

        if let Some(dir) = path.parent().and_then(|parent| File::open(parent).ok()) {
            let _ = dir.sync_all();
        }

        Ok(())
    }

    fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), PlanStoreError> {
        let bytes = serde_json::to_vec_pretty(value)?;
        Self::atomic_write_bytes(path, &bytes)
    }

    fn read_json_artifact<T: for<'de> Deserialize<'de>>(
        &self,
        path: &Path,
    ) -> Result<Option<T>, PlanStoreError> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&raw)?))
    }

    fn normalize_section_token(token: &str) -> String {
        let mut normalized = String::new();
        let mut last_was_dash = false;

        for ch in token.chars() {
            if ch.is_ascii_alphanumeric() {
                normalized.push(ch.to_ascii_lowercase());
                last_was_dash = false;
            } else if (ch.is_whitespace() || matches!(ch, '-' | '_' | ':' | '.')) && !last_was_dash
            {
                normalized.push('-');
                last_was_dash = true;
            }
        }

        normalized = normalized.trim_matches('-').to_string();
        if normalized.is_empty() {
            "section".to_string()
        } else {
            normalized
        }
    }

    fn extract_inline_anchor_terms(line: &str, heading: &str) -> Vec<String> {
        let mut anchors = Vec::new();
        let heading_trimmed = heading.trim();
        if !heading_trimmed.is_empty() {
            anchors.push(heading_trimmed.to_string());
        }

        let trimmed = line.trim();
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim();
            let value = value.trim();
            if matches!(
                key,
                "- task_id"
                    | "task_id"
                    | "- current_step_id"
                    | "current_step_id"
                    | "- step_id"
                    | "step_id"
            ) && !value.is_empty()
            {
                anchors.push(value.to_string());
            }
        }

        anchors.sort();
        anchors.dedup();
        anchors
    }

    fn heading_level_and_title(line: &str) -> Option<(u8, String)> {
        let trimmed = line.trim_start();
        let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
        if hashes == 0 {
            return None;
        }
        let title = trimmed[hashes..].trim();
        if title.is_empty() {
            return None;
        }
        Some((hashes as u8, title.to_string()))
    }

    fn index_plan_sections(session_id: &str, content: &str) -> PlanSectionArtifact {
        let lines: Vec<&str> = content.lines().collect();
        let mut sections = Vec::new();
        let mut heading_indices = Vec::new();

        for (index, line) in lines.iter().enumerate() {
            if let Some((level, heading)) = Self::heading_level_and_title(line) {
                heading_indices.push((index, level, heading));
            }
        }

        for (position, (line_start, level, heading)) in heading_indices.iter().enumerate() {
            let line_end = heading_indices
                .get(position + 1)
                .map(|(next_start, _, _)| next_start.saturating_sub(1))
                .unwrap_or_else(|| lines.len().saturating_sub(1));

            let parent_id = heading_indices[..position]
                .iter()
                .rev()
                .find(|(_, candidate_level, _)| *candidate_level < *level)
                .map(|(_, _, candidate_heading)| Self::normalize_section_token(candidate_heading));

            let mut anchor_terms = vec![heading.clone()];
            for line in lines[*line_start..=line_end].iter().take(10) {
                anchor_terms.extend(Self::extract_inline_anchor_terms(line, heading));
            }
            anchor_terms.sort();
            anchor_terms.dedup();

            sections.push(PlanSection {
                id: Self::normalize_section_token(heading),
                heading: heading.clone(),
                level: *level,
                line_start: *line_start,
                line_end,
                parent_id,
                anchor_terms,
            });
        }

        PlanSectionArtifact::new(session_id, sections)
    }

    /// Path to the session artifact directory for a given session.
    pub fn session_dir_path(&self, session_id: &str) -> PathBuf {
        self.session_dir_path_internal(session_id)
    }

    /// Path to the plan markdown file for a given session.
    ///
    /// Returns the currently resolved path. If a legacy flat-file artifact exists and
    /// the new directory-based artifact does not, this returns the legacy path.
    pub fn plan_file_path(&self, session_id: &str) -> PathBuf {
        self.resolved_plan_file_path_internal(session_id)
    }

    /// Path to the plan machine state artifact for a given session.
    pub fn state_file_path(&self, session_id: &str) -> PathBuf {
        self.session_dir_path_internal(session_id)
            .join(PLAN_STATE_FILE_NAME)
    }

    /// Path to the plan cursor artifact for a given session.
    pub fn cursor_file_path(&self, session_id: &str) -> PathBuf {
        self.session_dir_path_internal(session_id)
            .join(PLAN_CURSOR_FILE_NAME)
    }

    /// Path to the indexed sections artifact for a given session.
    pub fn sections_file_path(&self, session_id: &str) -> PathBuf {
        self.session_dir_path_internal(session_id)
            .join(PLAN_SECTIONS_FILE_NAME)
    }

    /// Write a plan markdown artifact for the given session.
    ///
    /// Overwrites any existing directory-based artifact for this session and refreshes
    /// the section index artifact derived from the markdown.
    pub fn write_plan(
        &self,
        session_id: &str,
        content: impl AsRef<str>,
    ) -> Result<PathBuf, PlanStoreError> {
        self.ensure_session_dir(session_id)?;
        let content = content.as_ref();
        let path = self.preferred_plan_file_path(session_id);
        Self::atomic_write_bytes(&path, content.as_bytes())?;

        let sections = Self::index_plan_sections(session_id, content);
        let sections_path = self.sections_file_path(session_id);
        Self::atomic_write_json(&sections_path, &sections)?;

        let legacy_path = self.legacy_plan_file_path(session_id);
        if legacy_path.exists() {
            let _ = fs::remove_file(legacy_path);
        }

        Ok(path)
    }

    /// Read the plan markdown artifact for the given session, if it exists.
    pub fn read_plan(&self, session_id: &str) -> Option<String> {
        let path = self.resolved_plan_file_path_internal(session_id);
        fs::read_to_string(&path).ok()
    }

    /// Check whether a plan artifact exists for the given session.
    pub fn plan_exists(&self, session_id: &str) -> bool {
        self.resolved_plan_file_path_internal(session_id).exists()
    }

    /// Write the plan machine state artifact for the given session.
    pub fn write_state(
        &self,
        session_id: &str,
        state: &PlanStateArtifact,
    ) -> Result<PathBuf, PlanStoreError> {
        self.ensure_session_dir(session_id)?;
        let path = self.state_file_path(session_id);
        Self::atomic_write_json(&path, state)?;
        Ok(path)
    }

    /// Read the plan machine state artifact for the given session, if it exists.
    pub fn read_state(
        &self,
        session_id: &str,
    ) -> Result<Option<PlanStateArtifact>, PlanStoreError> {
        self.read_json_artifact(&self.state_file_path(session_id))
    }

    /// Write the plan cursor artifact for the given session.
    pub fn write_cursor(
        &self,
        session_id: &str,
        cursor: &PlanCursorArtifact,
    ) -> Result<PathBuf, PlanStoreError> {
        self.ensure_session_dir(session_id)?;
        let path = self.cursor_file_path(session_id);
        Self::atomic_write_json(&path, cursor)?;
        Ok(path)
    }

    /// Read the plan cursor artifact for the given session, if it exists.
    pub fn read_cursor(
        &self,
        session_id: &str,
    ) -> Result<Option<PlanCursorArtifact>, PlanStoreError> {
        self.read_json_artifact(&self.cursor_file_path(session_id))
    }

    /// Read the indexed plan sections artifact for the given session, if it exists.
    pub fn read_sections(
        &self,
        session_id: &str,
    ) -> Result<Option<PlanSectionArtifact>, PlanStoreError> {
        self.read_json_artifact(&self.sections_file_path(session_id))
    }

    /// Delete all plan artifacts for the given session.
    pub fn delete_plan(&self, session_id: &str) -> Result<(), PlanStoreError> {
        let legacy_path = self.legacy_plan_file_path(session_id);
        if legacy_path.exists() {
            fs::remove_file(&legacy_path)?;
        }

        let session_dir = self.session_dir_path_internal(session_id);
        if session_dir.exists() {
            fs::remove_dir_all(session_dir)?;
        }

        Ok(())
    }

    /// Get the root storage directory path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, PlanStore) {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = PlanStore::new(temp_dir.path()).unwrap();
        (temp_dir, store)
    }

    #[test]
    fn session_slug_produces_short_identifier() {
        let id = "sess-abc123-def456-ghi789";
        let slug = PlanStore::session_slug(id);
        assert!(!slug.is_empty());
        assert!(!slug.contains('/'));
        assert!(!slug.contains('\\'));
    }

    #[test]
    fn write_and_read_plan() {
        let (_tmp, store) = temp_store();
        let session_id = "test-session-001";
        let content = "# Implementation Plan\n\n1. Step one\n2. Step two\n";

        let path = store.write_plan(session_id, content).unwrap();
        assert!(path.exists());
        assert!(store.plan_exists(session_id));

        let read = store.read_plan(session_id).unwrap();
        assert_eq!(read, content);
    }

    #[test]
    fn read_nonexistent_plan_returns_none() {
        let (_tmp, store) = temp_store();
        assert!(store.read_plan("nonexistent-session").is_none());
        assert!(!store.plan_exists("nonexistent-session"));
    }

    #[test]
    fn write_plan_overwrites_existing() {
        let (_tmp, store) = temp_store();
        let session_id = "test-session-002";

        store.write_plan(session_id, "Plan v1").unwrap();
        store.write_plan(session_id, "Plan v2").unwrap();

        let read = store.read_plan(session_id).unwrap();
        assert_eq!(read, "Plan v2");
    }

    #[test]
    fn delete_plan_removes_artifact_directory() {
        let (_tmp, store) = temp_store();
        let session_id = "test-session-003";

        store.write_plan(session_id, "Plan to delete").unwrap();
        store
            .write_state(session_id, &PlanStateArtifact::new(session_id))
            .unwrap();
        store
            .write_cursor(session_id, &PlanCursorArtifact::new(session_id))
            .unwrap();
        assert!(store.plan_exists(session_id));
        assert!(store.session_dir_path(session_id).exists());

        store.delete_plan(session_id).unwrap();
        assert!(!store.plan_exists(session_id));
        assert!(!store.session_dir_path(session_id).exists());
    }

    #[test]
    fn delete_nonexistent_plan_is_noop() {
        let (_tmp, store) = temp_store();
        store.delete_plan("never-created").unwrap();
    }

    #[test]
    fn plan_file_path_is_under_session_artifact_dir() {
        let (_tmp, store) = temp_store();
        let path = store.plan_file_path("some-session");
        assert!(path.starts_with(&store.plans_dir));
        assert_eq!(
            path.file_name().and_then(|n| n.to_str()),
            Some(PLAN_FILE_NAME)
        );
        assert_eq!(path.parent().unwrap().parent().unwrap(), store.plans_dir());
    }

    #[test]
    fn session_slug_handles_short_id() {
        let id = "short";
        let slug = PlanStore::session_slug(id);
        assert_eq!(slug, "short");
    }

    #[test]
    fn session_slug_strips_special_chars() {
        let id = "sess/abc\\def:ghi";
        let slug = PlanStore::session_slug(id);
        assert!(!slug.contains('/'));
        assert!(!slug.contains('\\'));
        assert!(!slug.contains(':'));
    }

    #[test]
    fn write_and_read_state_artifact() {
        let (_tmp, store) = temp_store();
        let session_id = "state-session-1";
        let mut state = PlanStateArtifact::new(session_id);
        state.status = Some("awaiting_approval".to_string());
        state.active_task_id = Some("task-1".to_string());
        state.active_section_id = Some("task-1".to_string());
        state.last_completed_task_id = Some("task-0".to_string());
        state.round_hint = Some(3);

        let path = store.write_state(session_id, &state).unwrap();
        assert!(path.exists());

        let read = store.read_state(session_id).unwrap().unwrap();
        assert_eq!(read, state);
    }

    #[test]
    fn write_and_read_cursor_artifact() {
        let (_tmp, store) = temp_store();
        let session_id = "cursor-session-1";
        let mut cursor = PlanCursorArtifact::new(session_id);
        cursor.cursor_type = Some("task_item".to_string());
        cursor.current_task_id = Some("task-2".to_string());
        cursor.current_task_ordinal = Some(2);
        cursor.current_section_id = Some("task-2".to_string());
        cursor.next_task_id = Some("task-3".to_string());
        cursor.next_task_ordinal = Some(3);
        cursor.last_completed_task_id = Some("task-1".to_string());
        cursor.round_hint = Some(4);
        cursor.round_id_hint = Some("round-4".to_string());
        cursor.suspension_hook_point = Some("AfterToolExecution".to_string());
        cursor.tool_call_boundary = Some("ExitPlanMode".to_string());
        cursor.resume_note = Some("Continue from task-2".to_string());

        let path = store.write_cursor(session_id, &cursor).unwrap();
        assert!(path.exists());

        let read = store.read_cursor(session_id).unwrap().unwrap();
        assert_eq!(read, cursor);
    }

    #[test]
    fn read_plan_falls_back_to_legacy_flat_file() {
        let (_tmp, store) = temp_store();
        let session_id = "legacy-session-1";
        let legacy_path = store.legacy_plan_file_path(session_id);
        fs::write(&legacy_path, "Legacy plan").unwrap();

        assert_eq!(store.read_plan(session_id).as_deref(), Some("Legacy plan"));
        assert_eq!(store.plan_file_path(session_id), legacy_path);
    }

    #[test]
    fn machine_state_writes_do_not_leave_temp_files_behind() {
        let (_tmp, store) = temp_store();
        let session_id = "temp-cleanup-session";
        let mut state = PlanStateArtifact::new(session_id);
        state.status = Some("designing".to_string());
        store.write_state(session_id, &state).unwrap();

        let entries = fs::read_dir(store.session_dir_path(session_id))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(entries.iter().all(|name| !name.ends_with(".tmp")));
        assert!(entries.iter().any(|name| name == PLAN_STATE_FILE_NAME));
    }

    #[test]
    fn write_plan_generates_section_index_artifact() {
        let (_tmp, store) = temp_store();
        let session_id = "sectioned-session";
        let plan = "# Plan\n\n## task-alpha\n- task_id: task-alpha\n- do alpha\n\n### step-alpha-1\n- current_step_id: step-alpha-1\n- detail\n\n## task-bravo\n- task_id: task-bravo\n- do bravo\n";

        store.write_plan(session_id, plan).unwrap();
        let sections = store
            .read_sections(session_id)
            .unwrap()
            .expect("sections should exist");

        assert!(sections.sections.len() >= 3);
        let task_alpha = sections
            .sections
            .iter()
            .find(|section| section.id == "task-alpha")
            .expect("task-alpha section");
        assert_eq!(task_alpha.heading, "task-alpha");
        assert!(task_alpha
            .anchor_terms
            .iter()
            .any(|term| term == "task-alpha"));

        let step_alpha = sections
            .sections
            .iter()
            .find(|section| section.id == "step-alpha-1")
            .expect("step-alpha-1 section");
        assert_eq!(step_alpha.parent_id.as_deref(), Some("task-alpha"));
        assert!(step_alpha
            .anchor_terms
            .iter()
            .any(|term| term == "step-alpha-1"));
    }
}
