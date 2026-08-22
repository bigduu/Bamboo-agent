//! Metadata-only workflow catalog layered on the canonical SkillStore discovery.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::store::storage::SkillDirectorySource;
use crate::types::SkillDefinition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowKind {
    Instruction,
    Orchestration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowSource {
    Builtin,
    Project,
    Workspace,
    User,
    Plugin,
}

impl WorkflowSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Project => "project",
            Self::Workspace => "workspace",
            Self::User => "user",
            Self::Plugin => "plugin",
        }
    }
}

impl From<SkillDirectorySource> for WorkflowSource {
    fn from(value: SkillDirectorySource) -> Self {
        match value {
            SkillDirectorySource::Builtin => Self::Builtin,
            SkillDirectorySource::Project => Self::Project,
            SkillDirectorySource::Workspace => Self::Workspace,
            SkillDirectorySource::Global => Self::User,
            // `~/.agents/skills` is another user-level discovery root. Keep its lower
            // internal precedence without leaking an implementation-specific fifth public
            // source into the four-source catalog contract.
            SkillDirectorySource::Agents => Self::User,
            SkillDirectorySource::Plugin => Self::Plugin,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkflowMigrationStatus {
    /// A read-only legacy source is catalog-visible and can be migrated.
    Available,
    /// The winning Skill bundle records a completed non-destructive migration.
    Migrated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowedWorkflowCandidate {
    pub source: WorkflowSource,
    pub status: WorkflowStatus,
    #[serde(default)]
    pub legacy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_status: Option<LegacyWorkflowMigrationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Public catalog item. It intentionally contains no instructions or resource paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCatalogEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub kind: WorkflowKind,
    pub source: WorkflowSource,
    pub revision: u64,
    /// Restart-stable SHA-256 identity of the exact definition, public
    /// metadata and immutable resource bytes represented by this winner.
    #[serde(default)]
    pub content_digest: String,
    pub version: String,
    pub invocation_policy: serde_json::Value,
    pub argument_schema: serde_json::Value,
    pub status: WorkflowStatus,
    #[serde(default)]
    pub legacy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_status: Option<LegacyWorkflowMigrationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub winner: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shadowed_candidates: Vec<ShadowedWorkflowCandidate>,
}

impl WorkflowCatalogEntry {
    /// Only orchestration bundles and explicit legacy workflow adapters belong
    /// to the public Workflow identity. Plain instruction Skills stay in the
    /// Skill catalog even though the runtime shares discovery internals.
    pub fn is_public_workflow(&self) -> bool {
        self.kind == WorkflowKind::Orchestration || self.legacy
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCatalogSnapshot {
    pub revision: u64,
    pub entries: Vec<WorkflowCatalogEntry>,
}

impl WorkflowCatalogSnapshot {
    pub fn public_workflows(mut self) -> Self {
        self.entries
            .retain(WorkflowCatalogEntry::is_public_workflow);
        self
    }
}

/// Restart-stable content identity for one exact catalog winner. Publication
/// counters and diagnostics are excluded; execution metadata, the parsed
/// definition and every immutable resource byte are included.
pub fn workflow_catalog_content_digest<'a, I>(
    entry: &WorkflowCatalogEntry,
    definition: Option<&SkillDefinition>,
    resources: I,
) -> String
where
    I: IntoIterator<Item = (&'a str, &'a [u8])>,
{
    let mut hasher = Sha256::new();
    hasher.update(b"bamboo.workflow-catalog-content.v1\0");
    let public_identity = serde_json::json!({
        "id": entry.id,
        "name": entry.name,
        "description": entry.description,
        "kind": entry.kind,
        "source": entry.source,
        "version": entry.version,
        "invocation_policy": entry.invocation_policy,
        "argument_schema": entry.argument_schema,
        "legacy": entry.legacy,
        "migration_status": entry.migration_status,
    });
    if let Ok(bytes) = serde_json::to_vec(&public_identity) {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    if let Some(definition) = definition {
        if let Ok(bytes) = serde_json::to_vec(definition) {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
    } else {
        hasher.update(b"unavailable-definition");
    }
    let mut resources = resources.into_iter().collect::<Vec<_>>();
    resources.sort_by(|left, right| left.0.cmp(right.0));
    for (path, bytes) in resources {
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowCatalogEventKind {
    Changed,
    Invalid,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCatalogEvent {
    pub workflow_id: String,
    pub revision: u64,
    pub kind: WorkflowCatalogEventKind,
    /// Compatibility discriminator for consumers predating the unified
    /// instruction and orchestration Workflow catalog. Newly published catalog
    /// events always set this; legacy decoded events default to `false`.
    #[serde(default)]
    pub public_workflow: bool,
    /// `global`, `project:<id>`, or an opaque `workspace:<hash>`; never an
    /// absolute filesystem path.
    pub scope: String,
}

#[derive(Debug, Default, Deserialize)]
struct BambooWorkflowMetadata {
    #[serde(default)]
    version: Option<serde_yaml::Value>,
    #[serde(default)]
    invocation_policy: Option<serde_json::Value>,
    #[serde(default)]
    argument_schema: Option<serde_json::Value>,
    // A composition is sufficient to classify legacy WorkflowDefinition YAML.
    #[serde(default)]
    composition: Option<serde_yaml::Value>,
    #[serde(default)]
    workflow_schema: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BundleMetadata {
    pub kind: WorkflowKind,
    pub version: String,
    pub invocation_policy: serde_json::Value,
    pub argument_schema: serde_json::Value,
    pub definition_revision: Option<u64>,
}

impl Default for BundleMetadata {
    fn default() -> Self {
        Self {
            kind: WorkflowKind::Instruction,
            version: "1".to_string(),
            invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
            argument_schema: serde_json::json!({"type": "object", "additionalProperties": true}),
            definition_revision: None,
        }
    }
}

fn yaml_scalar_to_string(value: serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::String(value) => Some(value),
        serde_yaml::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn public_yaml_error(display_name: &str, error: serde_yaml::Error) -> String {
    tracing::warn!("Invalid workflow bundle metadata in {display_name}: {error}");
    match error.location() {
        Some(location) => format!(
            "{display_name}: invalid metadata at line {}, column {}",
            location.line(),
            location.column()
        ),
        None => format!("{display_name}: invalid metadata"),
    }
}

fn public_validation_error(display_name: &str, error: impl std::fmt::Display) -> String {
    tracing::warn!("Invalid workflow definition in {display_name}: {error}");
    format!("{display_name}: invalid workflow definition")
}

pub(crate) async fn load_bundle_metadata(root: &Path) -> Result<BundleMetadata, String> {
    let workflow_path = root.join("workflow.yaml");
    let bamboo_path = root.join("agents").join("bamboo.yaml");
    let path = if tokio::fs::try_exists(&workflow_path).await.unwrap_or(false) {
        Some(workflow_path)
    } else if tokio::fs::try_exists(&bamboo_path).await.unwrap_or(false) {
        Some(bamboo_path)
    } else {
        None
    };

    let Some(path) = path else {
        return Ok(BundleMetadata::default());
    };
    let display_name = if path.ends_with("workflow.yaml") {
        "workflow.yaml"
    } else {
        "agents/bamboo.yaml"
    };
    let raw = tokio::fs::read(&path)
        .await
        .map_err(|error| format!("{display_name}: {error}"))?;
    parse_bundle_metadata_bytes(display_name, &raw, path.ends_with("workflow.yaml"))
}

pub(crate) fn parse_bundle_metadata_bytes(
    display_name: &str,
    raw: &[u8],
    is_workflow_definition: bool,
) -> Result<BundleMetadata, String> {
    let raw = std::str::from_utf8(raw)
        .map_err(|_| format!("{display_name}: metadata is not valid UTF-8"))?;
    let metadata: BambooWorkflowMetadata =
        serde_yaml::from_str(raw).map_err(|error| public_yaml_error(display_name, error))?;
    if is_workflow_definition {
        if metadata.workflow_schema.is_some() {
            let definition: bamboo_domain::WorkflowRunDefinition = serde_yaml::from_str(raw)
                .map_err(|error| public_yaml_error(display_name, error))?;
            bamboo_domain::CompiledWorkflow::compile(definition.clone())
                .map_err(|error| public_validation_error(display_name, error))?;
        } else {
            let definition: bamboo_domain::WorkflowDefinition = serde_yaml::from_str(raw)
                .map_err(|error| public_yaml_error(display_name, error))?;
            definition
                .validate()
                .map_err(|error| public_validation_error(display_name, error))?;
        }
    }
    let mut result = BundleMetadata {
        kind: if metadata.composition.is_some() || is_workflow_definition {
            WorkflowKind::Orchestration
        } else {
            WorkflowKind::Instruction
        },
        ..Default::default()
    };
    if result.kind == WorkflowKind::Orchestration {
        // Starting an orchestration can spend budget and invoke multiple tools;
        // model activation is opt-in at the bundle as well as the session.
        result.invocation_policy = serde_json::json!({"explicit": true, "automatic": false});
    }
    if is_workflow_definition && metadata.workflow_schema.is_some() {
        let definition: bamboo_domain::WorkflowRunDefinition =
            serde_yaml::from_str(raw).map_err(|error| public_yaml_error(display_name, error))?;
        result.version = definition.workflow_schema.to_string();
        result.argument_schema = definition.input_schema.clone();
        result.definition_revision = Some(definition.revision);
    }
    if let Some(version) = metadata.version.and_then(yaml_scalar_to_string) {
        result.version = version;
    }
    if let Some(policy) = metadata.invocation_policy {
        result.invocation_policy = policy;
    }
    if let Some(schema) = metadata.argument_schema {
        result.argument_schema = schema;
    }
    Ok(result)
}

pub(crate) fn legacy_migration_status(
    skill: &SkillDefinition,
) -> Option<LegacyWorkflowMigrationStatus> {
    skill.metadata.as_ref().and_then(|metadata| {
        if metadata
            .get("legacy_migration")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            || metadata
                .get("legacy_import")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            Some(LegacyWorkflowMigrationStatus::Migrated)
        } else if metadata
            .get("legacy_adapter")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            Some(LegacyWorkflowMigrationStatus::Available)
        } else {
            None
        }
    })
}

pub(crate) fn entry_from_skill(
    skill: &SkillDefinition,
    source: SkillDirectorySource,
    revision: u64,
    mut metadata: BundleMetadata,
) -> WorkflowCatalogEntry {
    let migration_status = legacy_migration_status(skill);
    if skill.metadata.as_ref().is_some_and(|metadata| {
        metadata
            .get("legacy_manual_only")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    }) {
        metadata.invocation_policy = serde_json::json!({"explicit": true, "automatic": false});
    }
    WorkflowCatalogEntry {
        id: skill.id.clone(),
        name: skill.name.clone(),
        description: skill.description.clone(),
        kind: metadata.kind,
        source: source.into(),
        revision: metadata.definition_revision.unwrap_or(revision),
        content_digest: String::new(),
        version: metadata.version,
        invocation_policy: metadata.invocation_policy,
        argument_schema: metadata.argument_schema,
        status: WorkflowStatus::Valid,
        legacy: migration_status.is_some(),
        migration_status,
        last_error: None,
        winner: true,
        shadowed_candidates: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_serialization_never_contains_instructions_or_resources() {
        let skill = SkillDefinition::new("review", "review", "Reviews code", "SECRET BODY");
        let mut entry = entry_from_skill(
            &skill,
            SkillDirectorySource::Project,
            4,
            BundleMetadata::default(),
        );
        entry.content_digest = workflow_catalog_content_digest(
            &entry,
            Some(&skill),
            [("references/example.md", b"PRIVATE RESOURCE".as_slice())],
        );
        let json = serde_json::to_string(&entry).expect("serialize catalog entry");
        assert!(!json.contains("SECRET BODY"));
        assert!(!json.contains("PRIVATE RESOURCE"));
        assert!(!json.contains("prompt"));
        assert!(!json.contains("SKILL.md"));
        assert!(!json.contains("references"));
        assert!(json.contains(&entry.content_digest));
    }

    #[test]
    fn content_digest_is_order_independent_and_changes_with_exact_identity() {
        let skill = SkillDefinition::new("review", "review", "Reviews code", "PRIVATE BODY");
        let entry = entry_from_skill(
            &skill,
            SkillDirectorySource::Builtin,
            9,
            BundleMetadata::default(),
        );
        let first = workflow_catalog_content_digest(
            &entry,
            Some(&skill),
            [("z.txt", b"z".as_slice()), ("a.txt", b"a".as_slice())],
        );
        let reordered = workflow_catalog_content_digest(
            &entry,
            Some(&skill),
            [("a.txt", b"a".as_slice()), ("z.txt", b"z".as_slice())],
        );
        assert_eq!(first, reordered);
        assert_eq!(first.len(), 64);

        let changed_bytes = workflow_catalog_content_digest(
            &entry,
            Some(&skill),
            [("a.txt", b"changed".as_slice()), ("z.txt", b"z".as_slice())],
        );
        assert_ne!(first, changed_bytes);
        let mut project_entry = entry.clone();
        project_entry.source = WorkflowSource::Project;
        let changed_source = workflow_catalog_content_digest(
            &project_entry,
            Some(&skill),
            [("a.txt", b"a".as_slice()), ("z.txt", b"z".as_slice())],
        );
        assert_ne!(first, changed_source);
    }

    #[test]
    fn workflow_event_decodes_legacy_payload_without_namespace_marker() {
        let event: WorkflowCatalogEvent = serde_json::from_value(serde_json::json!({
            "workflow_id": "legacy-review",
            "revision": 7,
            "kind": "changed",
            "scope": "global"
        }))
        .expect("older event DTO remains readable");

        assert_eq!(event.workflow_id, "legacy-review");
        assert_eq!(event.revision, 7);
        assert_eq!(event.kind, WorkflowCatalogEventKind::Changed);
        assert!(!event.public_workflow);
    }

    #[tokio::test]
    async fn invalid_bundle_error_does_not_echo_private_yaml_values() {
        const PRIVATE_VALUE: &str = "private-catalog-metadata-value";
        const PRIVATE_RESOURCE: &str = "/private/resources/catalog-reference.md";
        let directory = tempfile::tempdir().expect("tempdir");
        tokio::fs::write(
            directory.path().join("workflow.yaml"),
            format!(
                "id: private\nname: Private\ndescription: {PRIVATE_VALUE}\nversion: '1'\ncomposition:\n  type: {PRIVATE_RESOURCE}\n"
            ),
        )
        .await
        .expect("workflow metadata");

        let error = load_bundle_metadata(directory.path())
            .await
            .expect_err("invalid composition type");

        assert!(
            !error.contains(PRIVATE_VALUE),
            "catalog error leaked: {error}"
        );
        assert!(
            !error.contains(PRIVATE_RESOURCE),
            "catalog error leaked: {error}"
        );
        assert!(error.starts_with("workflow.yaml:"));
        assert!(!error.contains(directory.path().to_string_lossy().as_ref()));
    }
}
