//! Metadata-only workflow catalog layered on the canonical SkillStore discovery.

use std::path::Path;

use serde::{Deserialize, Serialize};

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
    User,
    Plugin,
}

impl From<SkillDirectorySource> for WorkflowSource {
    fn from(value: SkillDirectorySource) -> Self {
        match value {
            SkillDirectorySource::Builtin => Self::Builtin,
            SkillDirectorySource::Project => Self::Project,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowedWorkflowCandidate {
    pub source: WorkflowSource,
    pub status: WorkflowStatus,
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
    pub version: String,
    pub invocation_policy: serde_json::Value,
    pub argument_schema: serde_json::Value,
    pub status: WorkflowStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub winner: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shadowed_candidates: Vec<ShadowedWorkflowCandidate>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCatalogSnapshot {
    pub revision: u64,
    pub entries: Vec<WorkflowCatalogEntry>,
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
    /// `global` or an opaque `workspace:<hash>`; never an absolute filesystem path.
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
}

#[derive(Debug, Clone)]
pub(crate) struct BundleMetadata {
    pub kind: WorkflowKind,
    pub version: String,
    pub invocation_policy: serde_json::Value,
    pub argument_schema: serde_json::Value,
}

impl Default for BundleMetadata {
    fn default() -> Self {
        Self {
            kind: WorkflowKind::Instruction,
            version: "1".to_string(),
            invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
            argument_schema: serde_json::json!({"type": "object", "additionalProperties": true}),
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
    let raw = tokio::fs::read_to_string(&path)
        .await
        .map_err(|error| format!("{display_name}: {error}"))?;
    let metadata: BambooWorkflowMetadata =
        serde_yaml::from_str(&raw).map_err(|error| public_yaml_error(display_name, error))?;
    let is_workflow_definition = path.ends_with("workflow.yaml");
    if is_workflow_definition {
        let definition: bamboo_domain::WorkflowDefinition =
            serde_yaml::from_str(&raw).map_err(|error| public_yaml_error(display_name, error))?;
        definition
            .validate()
            .map_err(|error| public_validation_error(display_name, error))?;
    }
    let mut result = BundleMetadata {
        kind: if metadata.composition.is_some() || is_workflow_definition {
            WorkflowKind::Orchestration
        } else {
            WorkflowKind::Instruction
        },
        ..Default::default()
    };
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

pub(crate) fn entry_from_skill(
    skill: &SkillDefinition,
    source: SkillDirectorySource,
    revision: u64,
    metadata: BundleMetadata,
) -> WorkflowCatalogEntry {
    WorkflowCatalogEntry {
        id: skill.id.clone(),
        name: skill.name.clone(),
        description: skill.description.clone(),
        kind: metadata.kind,
        source: source.into(),
        revision,
        version: metadata.version,
        invocation_policy: metadata.invocation_policy,
        argument_schema: metadata.argument_schema,
        status: WorkflowStatus::Valid,
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
        let entry = entry_from_skill(
            &skill,
            SkillDirectorySource::Project,
            4,
            BundleMetadata::default(),
        );
        let json = serde_json::to_string(&entry).expect("serialize catalog entry");
        assert!(!json.contains("SECRET BODY"));
        assert!(!json.contains("prompt"));
        assert!(!json.contains("SKILL.md"));
        assert!(!json.contains("references"));
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
