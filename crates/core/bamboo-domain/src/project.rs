//! Stable Project identity and persistence DTOs.
//!
//! A Project is deliberately not derived from a workspace path. Workspaces are
//! mutable execution contexts, while [`ProjectId`] is the opaque durable key
//! used by sessions and Project-shared resources.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const PROJECT_MANIFEST_SCHEMA_VERSION: u32 = 2;
pub const PROJECT_INDEX_SCHEMA_VERSION: u32 = 2;

fn initial_project_revision() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid project id: {0}")]
pub struct InvalidProjectId(pub String);

/// Opaque, path-safe, stable Project identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectId(String);

impl ProjectId {
    pub const MAX_LEN: usize = 64;

    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidProjectId> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= Self::MAX_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
        if valid {
            Ok(Self(value))
        } else {
            Err(InvalidProjectId(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Default for ProjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for ProjectId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for ProjectId {
    type Err = InvalidProjectId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ProjectId {
    type Error = InvalidProjectId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ProjectId> for String {
    fn from(value: ProjectId) -> Self {
        value.into_string()
    }
}

impl Serialize for ProjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    #[default]
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectPathStatus {
    Configured,
    NeedsSelection,
    #[default]
    NeedsConfiguration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<String>,
}

/// Authoritative `${BAMBOO_DATA_DIR}/projects/<id>/project.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectManifest {
    pub schema_version: u32,
    pub id: ProjectId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub status: ProjectStatus,
    /// Canonical user source/work folder used when an assigned session has no
    /// explicit or persisted workspace. This is distinct from Bamboo's private
    /// `${BAMBOO_DATA_DIR}/projects/<id>` Project home.
    ///
    /// Legacy v1 manifests may remain unconfigured (`None`) until the user
    /// selects a path. New active Projects must be created with this field.
    #[serde(default)]
    pub project_path: Option<String>,
    /// Explicit migration/configuration state. Consumers must not infer a
    /// primary path from `workspace_bindings` ordering or labels.
    #[serde(default)]
    pub project_path_status: ProjectPathStatus,
    /// Additional registered workspaces/worktrees. `project_path` is itself an
    /// authoritative registered root and is not duplicated in this collection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspace_bindings: Vec<WorkspaceBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legacy_project_keys: Vec<String>,
    /// CAS token for metadata and workspace binding updates.
    #[serde(default = "initial_project_revision")]
    pub revision: u64,
    /// Revision of the Project-shared resource inventory.
    #[serde(default = "initial_project_revision")]
    pub resource_revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ProjectManifest {
    pub fn new(
        id: ProjectId,
        name: impl Into<String>,
        description: Option<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: PROJECT_MANIFEST_SCHEMA_VERSION,
            id,
            name: name.into(),
            description,
            status: ProjectStatus::Active,
            project_path: None,
            project_path_status: ProjectPathStatus::NeedsConfiguration,
            workspace_bindings: Vec::new(),
            legacy_project_keys: Vec::new(),
            revision: 1,
            resource_revision: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Every workspace root owned by this Project, with the primary
    /// `project_path` first when configured.
    pub fn workspace_roots(&self) -> impl Iterator<Item = &str> {
        self.project_path.iter().map(String::as_str).chain(
            self.workspace_bindings
                .iter()
                .map(|binding| binding.path.as_str()),
        )
    }

    pub fn workspace_count(&self) -> usize {
        usize::from(self.project_path.is_some()) + self.workspace_bindings.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIndexEntry {
    pub id: ProjectId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: ProjectStatus,
    #[serde(default)]
    pub project_path: Option<String>,
    #[serde(default)]
    pub project_path_status: ProjectPathStatus,
    pub revision: u64,
    pub resource_revision: u64,
    pub workspace_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&ProjectManifest> for ProjectIndexEntry {
    fn from(manifest: &ProjectManifest) -> Self {
        Self {
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            description: manifest.description.clone(),
            status: manifest.status,
            project_path: manifest.project_path.clone(),
            project_path_status: manifest.project_path_status,
            revision: manifest.revision,
            resource_revision: manifest.resource_revision,
            workspace_count: manifest.workspace_count(),
            created_at: manifest.created_at,
            updated_at: manifest.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIndex {
    pub schema_version: u32,
    pub revision: u64,
    pub updated_at: DateTime<Utc>,
    pub projects: BTreeMap<ProjectId, ProjectIndexEntry>,
}

impl ProjectIndex {
    pub fn empty(now: DateTime<Utc>) -> Self {
        Self {
            schema_version: PROJECT_INDEX_SCHEMA_VERSION,
            revision: 0,
            updated_at: now,
            projects: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectResourceKind {
    Settings,
    Skills,
    Commands,
    Memory,
    Artifacts,
    State,
}

/// Redacted inventory entry. It intentionally contains no file contents,
/// environment values, headers, credential references, or secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResourceEntry {
    pub kind: ProjectResourceKind,
    pub present: bool,
    pub item_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResourceSummary {
    pub project_id: ProjectId,
    pub resource_revision: u64,
    pub resources: Vec<ProjectResourceEntry>,
}

/// Pre-resolved legacy session input for the migration dry-run seam.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacySessionProjectInput {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legacy_project_keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyProjectMatchBasis {
    ExactCanonicalBinding,
    GitCommonDir,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProjectAssignment {
    pub session_id: String,
    pub project_id: ProjectId,
    pub basis: LegacyProjectMatchBasis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProjectSuggestion {
    pub basis: LegacyProjectMatchBasis,
    pub session_ids: Vec<String>,
    pub workspace_paths: Vec<String>,
    pub legacy_project_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProjectUnassigned {
    pub session_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProjectDryRunReport {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignments: Vec<LegacyProjectAssignment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<LegacyProjectSuggestion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unassigned: Vec<LegacyProjectUnassigned>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyMemoryMigrationPhase {
    Copying,
    Verified,
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyMemoryFileDisposition {
    Pending,
    Staged,
    Copied,
    ExistingIdentical,
    TargetConflict,
    /// The source remains untouched as a read-only legacy record, but is not
    /// copied into Project primary storage because canonical validation failed.
    SkippedInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMemoryMigrationFile {
    /// Slash-separated relative path below the legacy/project memory roots.
    pub relative_path: String,
    pub size: u64,
    pub sha256: String,
    pub disposition: LegacyMemoryFileDisposition,
    /// Redacted validation diagnostic. This describes why an individual
    /// source record was isolated without embedding its contents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

/// Durable status returned by the actual copy -> verify -> commit migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMemoryMigrationReport {
    pub project_id: ProjectId,
    pub legacy_project_key: String,
    pub transaction_id: String,
    pub phase: LegacyMemoryMigrationPhase,
    pub files: Vec<LegacyMemoryMigrationFile>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,
}

/// Read-compatibility alias. The Project-home root always has precedence; the
/// legacy scope is read-only and only fills entries absent from the new root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMemoryReadAlias {
    pub legacy_project_key: String,
    pub read_only: bool,
    pub project_home_precedence: bool,
    pub source_available: bool,
    pub migration_committed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_id_is_opaque_and_path_safe() {
        for valid in ["01JABCDEF0123456789ABCDEFG", "uuid-like_value-1"] {
            let id: ProjectId = valid.parse().unwrap();
            assert_eq!(id.as_str(), valid);
            let encoded = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<ProjectId>(&encoded).unwrap(), id);
        }

        for invalid in ["", ".", "..", "../escape", "a/b", r"a\b", "with space"] {
            assert!(invalid.parse::<ProjectId>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn manifest_additive_fields_have_safe_defaults() {
        let value = serde_json::json!({
            "schema_version": 1,
            "id": "01JABCDEF0123456789ABCDEFG",
            "name": "Zenith",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        });
        let manifest: ProjectManifest = serde_json::from_value(value).unwrap();
        assert_eq!(manifest.status, ProjectStatus::Active);
        assert!(manifest.project_path.is_none());
        assert_eq!(
            manifest.project_path_status,
            ProjectPathStatus::NeedsConfiguration
        );
        assert!(manifest.workspace_bindings.is_empty());
        assert!(manifest.legacy_project_keys.is_empty());
        assert_eq!(manifest.revision, 1);
        assert_eq!(manifest.resource_revision, 1);
    }
}
