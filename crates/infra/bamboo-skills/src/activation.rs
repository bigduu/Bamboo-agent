//! Typed workflow activation and durable session metadata contracts.

use std::collections::BTreeMap;
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{SkillActivationSnapshot, WorkflowKind, WorkflowSource};

pub const ACTIVE_WORKFLOW_METADATA_KEY: &str = "workflow.active.v1";
pub const WORKFLOW_SELECTION_METADATA_KEY: &str = "workflow.selection.v1";
pub const ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY: &str = "workflow.active.snapshot.v1";
pub const WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY: &str = "workflow.orchestration_opt_in";
pub const WORKFLOW_RUN_IDS_METADATA_KEY: &str = "workflow.run_ids.v1";
pub const WORKFLOW_ACTIVATION_EVENT_METADATA_KEY: &str = "workflow.activation_event.v1";
pub const WORKFLOW_CONTEXT_CACHE_METADATA_KEY: &str = "workflow.context_cache.v1";
pub const WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY: &str = "workflow.dynamic_context.last.v1";
pub const WORKFLOW_CATALOG_DIAGNOSTIC_METADATA_KEY: &str = "workflow.catalog_diagnostic.v1";
pub const MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES: usize = 512 * 1024;

/// Stable identity for at-least-once lifecycle outbox delivery. Publishers may
/// replay after a crash between send and acknowledgement; consumers use this
/// identity to make the externally observable transition idempotent.
pub fn workflow_lifecycle_event_id(session_id: &str, event: &Value) -> String {
    let material = serde_json::json!({"session_id": session_id, "event": event});
    format!(
        "workflow-lifecycle-{}",
        hex::encode(Sha256::digest(material.to_string().as_bytes()))
    )
}

/// Authoritative request contract for an explicitly selected workflow.
///
/// Lotus sends identity plus arguments only. Instructions and resources are
/// always resolved by Bamboo from the immutable catalog revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSelection {
    pub id: String,
    pub source: WorkflowSource,
    pub revision: u64,
    #[serde(default)]
    pub args: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowInvokedBy {
    User,
    Model,
    Api,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowActivationStatus {
    Active,
    Degraded,
    Deactivated,
}

/// Durable runtime activation metadata. This includes caller arguments and
/// dynamic provider context and must not be serialized directly at a public
/// API boundary. The immutable bundle payload is stored separately under
/// [`ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveWorkflow {
    pub id: String,
    pub source: WorkflowSource,
    pub revision: u64,
    pub kind: WorkflowKind,
    #[serde(default)]
    pub args: Value,
    pub invoked_by: WorkflowInvokedBy,
    pub activated_at: DateTime<Utc>,
    pub status: WorkflowActivationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<WorkflowActivationDiagnostic>,
    /// Stable hash of the injected runtime block. Compaction/resume compares
    /// this value instead of appending instructions to a user message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dynamic_context: Vec<DynamicContextBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowActivationErrorCode {
    InvalidSelection,
    SourceMismatch,
    RevisionMissing,
    RevisionMismatch,
    ManualOnly,
    SnapshotUnavailable,
    SnapshotTooLarge,
    ProviderFailed,
    ProviderOutputInvalid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowActivationDiagnostic {
    pub code: WorkflowActivationErrorCode,
    pub message: String,
    #[serde(default)]
    pub recoverable: bool,
}

/// Session-persisted immutable activation payload used as the last-known-good
/// source when the live catalog revision is stale/missing or after restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableWorkflowActivation {
    pub active: ActiveWorkflow,
    pub snapshot: SkillActivationSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCatalogDiagnostic {
    pub total_candidates: usize,
    pub advertised_candidates: usize,
    pub initial_chars: usize,
    pub final_chars: usize,
    pub char_budget: usize,
    pub token_budget: usize,
    pub compressed_descriptions: bool,
    pub shortlisted: bool,
    #[serde(default)]
    pub omitted_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamicContextDeclaration {
    pub id: String,
    pub tool: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default = "default_dynamic_context_chars")]
    pub max_chars: usize,
    #[serde(default = "default_dynamic_context_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub cache_ttl_secs: u64,
    #[serde(default)]
    pub stop_on_failure: bool,
}

fn default_dynamic_context_chars() -> usize {
    8_192
}

fn default_dynamic_context_timeout_ms() -> u64 {
    5_000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamicContextBlock {
    pub provider_id: String,
    pub tool: String,
    pub provenance: String,
    pub generated_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub status: WorkflowActivationStatus,
    /// A degraded provider with this flag prevents workflow activation, but
    /// never aborts the surrounding model conversation.
    #[serde(default)]
    pub stop_on_failure: bool,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<WorkflowActivationDiagnostic>,
}

pub type DynamicContextCache = BTreeMap<String, DynamicContextBlock>;

/// Persist one exact, already-pinned instruction candidate before a chat turn
/// is acknowledged. This closes the chat-to-execute restart window: the
/// server can restore the immutable revision even if the live catalog changes
/// or the process restarts after `POST /chat` and before `POST /execute`.
///
/// Every serialized value is prepared before the metadata map is mutated, so
/// an error leaves the caller's prior runtime candidate untouched.
pub fn persist_explicit_workflow_candidate(
    metadata: &mut HashMap<String, String>,
    selection: &WorkflowSelection,
    activation: &crate::SkillActivationSelection,
    snapshot: &SkillActivationSnapshot,
) -> Result<(), WorkflowActivationDiagnostic> {
    let diagnostic = |code, message: &str, recoverable| WorkflowActivationDiagnostic {
        code,
        message: message.to_string(),
        recoverable,
    };
    let entry = match activation.catalog_entries.as_slice() {
        [entry] if entry.id == selection.id => entry,
        _ => {
            return Err(diagnostic(
                WorkflowActivationErrorCode::RevisionMissing,
                "selected workflow is unavailable or disabled",
                true,
            ));
        }
    };
    if entry.status != crate::WorkflowStatus::Valid {
        return Err(diagnostic(
            WorkflowActivationErrorCode::RevisionMissing,
            "selected workflow is invalid",
            true,
        ));
    }
    if entry.source != selection.source {
        return Err(diagnostic(
            WorkflowActivationErrorCode::SourceMismatch,
            "selected workflow source changed; refresh the catalog",
            true,
        ));
    }
    if entry.revision != selection.revision {
        return Err(diagnostic(
            WorkflowActivationErrorCode::RevisionMismatch,
            "selected workflow revision changed; refresh the catalog",
            true,
        ));
    }
    if entry.kind != WorkflowKind::Instruction {
        return Err(diagnostic(
            WorkflowActivationErrorCode::InvalidSelection,
            "orchestration workflows must be started through the Workflow Run API",
            false,
        ));
    }
    if entry.invocation_policy["explicit"].as_bool() != Some(true) {
        return Err(diagnostic(
            WorkflowActivationErrorCode::ManualOnly,
            "selected workflow does not allow explicit activation",
            false,
        ));
    }
    if let Err(error) = bamboo_domain::validate_schema(&entry.argument_schema, &selection.args) {
        return Err(diagnostic(
            WorkflowActivationErrorCode::InvalidSelection,
            &format!("workflow arguments do not match the catalog schema: {error}"),
            true,
        ));
    }
    let Some(snapshot_entry) = snapshot.skills.get(&selection.id) else {
        return Err(diagnostic(
            WorkflowActivationErrorCode::SnapshotUnavailable,
            "pinned workflow snapshot is missing the selected definition",
            true,
        ));
    };
    if snapshot.skills.len() != 1
        || snapshot_entry.revision != selection.revision
        || snapshot_entry.catalog_entry != *entry
    {
        return Err(diagnostic(
            WorkflowActivationErrorCode::RevisionMismatch,
            "pinned workflow snapshot does not match the selected catalog identity",
            true,
        ));
    }

    use crate::runtime_metadata::{
        SKILL_RUNTIME_ACTIVATION_ERROR_KEY, SKILL_RUNTIME_ACTIVATION_GENERATION_KEY,
        SKILL_RUNTIME_PINNED_SNAPSHOT_KEY, SKILL_RUNTIME_SELECTED_CATALOG_KEY,
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY, SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,
        SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY, SKILL_RUNTIME_SELECTION_COUNT_KEY,
        SKILL_RUNTIME_SELECTION_SOURCE_KEY, SKILL_RUNTIME_SELECTION_TRACE_KEY,
    };
    let snapshot_json = serde_json::to_string(snapshot).map_err(|_| {
        diagnostic(
            WorkflowActivationErrorCode::SnapshotUnavailable,
            "pinned workflow snapshot could not be serialized",
            true,
        )
    })?;
    if snapshot_json.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
        return Err(diagnostic(
            WorkflowActivationErrorCode::SnapshotTooLarge,
            "selected workflow snapshot exceeds the durable session limit",
            true,
        ));
    }
    let catalog_json = serde_json::to_string(&activation.catalog_entries).map_err(|_| {
        diagnostic(
            WorkflowActivationErrorCode::SnapshotUnavailable,
            "selected workflow catalog identity could not be serialized",
            true,
        )
    })?;
    let selected_ids = vec![selection.id.clone()];
    let selected_ids_json = serde_json::to_string(&selected_ids).map_err(|_| {
        diagnostic(
            WorkflowActivationErrorCode::SnapshotUnavailable,
            "selected workflow id could not be serialized",
            true,
        )
    })?;
    let revisions_json =
        serde_json::to_string(&activation.descriptor.skill_revisions).map_err(|_| {
            diagnostic(
                WorkflowActivationErrorCode::SnapshotUnavailable,
                "selected workflow revision map could not be serialized",
                true,
            )
        })?;
    let diagnostic_json = serde_json::to_string(&activation.catalog_diagnostic).map_err(|_| {
        diagnostic(
            WorkflowActivationErrorCode::SnapshotUnavailable,
            "selected workflow catalog diagnostic could not be serialized",
            true,
        )
    })?;
    let trace_json = serde_json::json!({
        "source": "explicit",
        "selected_skill_ids": selected_ids,
        "selected_skill_mode": activation.descriptor.selected_skill_mode,
        "request_hint_present": false
    })
    .to_string();

    metadata.insert(
        SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    metadata.insert(SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(), catalog_json);
    metadata.insert(
        WORKFLOW_CATALOG_DIAGNOSTIC_METADATA_KEY.to_string(),
        diagnostic_json,
    );
    metadata.insert(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(), snapshot_json);
    metadata.insert(
        SKILL_RUNTIME_SELECTION_COUNT_KEY.to_string(),
        "1".to_string(),
    );
    metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        selected_ids_json,
    );
    metadata.insert(
        SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        activation.descriptor.catalog_revision.to_string(),
    );
    metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        revisions_json,
    );
    metadata.insert(SKILL_RUNTIME_SELECTION_TRACE_KEY.to_string(), trace_json);
    if let Some(mode) = activation.descriptor.selected_skill_mode.as_ref() {
        metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY.to_string(),
            mode.clone(),
        );
    } else {
        metadata.remove(SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY);
    }
    metadata.remove(SKILL_RUNTIME_ACTIVATION_ERROR_KEY);
    Ok(())
}

/// Record activation only after `load_skill` has returned the pinned payload.
/// This is shared by explicit preload and model-driven automatic activation.
pub fn record_loaded_workflow_activation(
    metadata: &mut HashMap<String, String>,
    skill_id: &str,
    context_fingerprint: String,
) -> Result<ActiveWorkflow, WorkflowActivationDiagnostic> {
    let catalog = metadata
        .get(crate::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<crate::WorkflowCatalogEntry>>(raw).ok())
        .ok_or_else(|| WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotUnavailable,
            message: "pinned workflow catalog metadata is unavailable".to_string(),
            recoverable: true,
        })?;
    let entry = catalog
        .into_iter()
        .find(|entry| entry.id == skill_id)
        .ok_or_else(|| WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::InvalidSelection,
            message: "loaded workflow is not in the pinned catalog selection".to_string(),
            recoverable: false,
        })?;
    if entry.kind != WorkflowKind::Instruction {
        return Err(WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::InvalidSelection,
            message: "orchestration workflows cannot activate as instructions".to_string(),
            recoverable: false,
        });
    }
    let selection_source = metadata
        .get(crate::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY)
        .map(String::as_str)
        .unwrap_or("auto");
    let invoked_by = if selection_source == "explicit" {
        WorkflowInvokedBy::User
    } else {
        WorkflowInvokedBy::Model
    };
    let policy_key = if invoked_by == WorkflowInvokedBy::User {
        "explicit"
    } else {
        "automatic"
    };
    if entry.invocation_policy[policy_key].as_bool() != Some(true) {
        return Err(WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::ManualOnly,
            message: format!("workflow invocation policy denies {policy_key} activation"),
            recoverable: false,
        });
    }
    let typed = metadata
        .get(WORKFLOW_SELECTION_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<WorkflowSelection>(raw).ok());
    if let Some(selection) = typed.as_ref() {
        if selection.id != entry.id
            || selection.source != entry.source
            || selection.revision != entry.revision
        {
            return Err(WorkflowActivationDiagnostic {
                code: WorkflowActivationErrorCode::RevisionMismatch,
                message: "typed selection does not match the pinned workflow".to_string(),
                recoverable: false,
            });
        }
    }
    let mut snapshot = metadata
        .get(crate::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
        .and_then(|raw| serde_json::from_str::<SkillActivationSnapshot>(raw).ok())
        .ok_or_else(|| WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotUnavailable,
            message: "pinned workflow snapshot is unavailable".to_string(),
            recoverable: true,
        })?;
    let snapshot_entry =
        snapshot
            .skills
            .get(skill_id)
            .ok_or_else(|| WorkflowActivationDiagnostic {
                code: WorkflowActivationErrorCode::SnapshotUnavailable,
                message: "loaded workflow is absent from its pinned snapshot".to_string(),
                recoverable: true,
            })?;
    if snapshot_entry.revision != entry.revision || snapshot_entry.catalog_entry != entry {
        return Err(WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::RevisionMismatch,
            message: "pinned workflow snapshot does not match catalog metadata".to_string(),
            recoverable: false,
        });
    }
    snapshot.skills.retain(|id, _| id == skill_id);
    let snapshot_json =
        serde_json::to_string(&snapshot).map_err(|_| WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotTooLarge,
            message: "active workflow snapshot could not be serialized".to_string(),
            recoverable: true,
        })?;
    if snapshot_json.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
        return Err(WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotTooLarge,
            message: "active workflow snapshot exceeds the durable session limit".to_string(),
            recoverable: true,
        });
    }
    let dynamic_context = match metadata.get(WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY) {
        Some(raw) => serde_json::from_str::<Vec<DynamicContextBlock>>(raw).map_err(|_| {
            WorkflowActivationDiagnostic {
                code: WorkflowActivationErrorCode::ProviderOutputInvalid,
                message: "dynamic workflow context metadata is invalid".to_string(),
                recoverable: true,
            }
        })?,
        None => Vec::new(),
    };
    let active = ActiveWorkflow {
        id: entry.id,
        source: entry.source,
        revision: entry.revision,
        kind: entry.kind,
        args: typed
            .map(|selection| selection.args)
            .unwrap_or_else(|| serde_json::json!({})),
        invoked_by,
        activated_at: Utc::now(),
        status: WorkflowActivationStatus::Active,
        diagnostic: None,
        context_fingerprint: Some(context_fingerprint),
        dynamic_context,
    };
    let durable = DurableWorkflowActivation {
        active: active.clone(),
        snapshot,
    };
    // Serialize the complete publication first. A serialization failure must
    // leave the caller's prior durable activation untouched rather than
    // exposing a mixture of old and new metadata.
    let active_json = serde_json::to_string(&active).map_err(|_| WorkflowActivationDiagnostic {
        code: WorkflowActivationErrorCode::ProviderOutputInvalid,
        message: "active workflow metadata could not be serialized".to_string(),
        recoverable: true,
    })?;
    let durable_json =
        serde_json::to_string(&durable).map_err(|_| WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotTooLarge,
            message: "durable workflow snapshot could not be serialized".to_string(),
            recoverable: true,
        })?;
    if durable_json.len() > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES {
        return Err(WorkflowActivationDiagnostic {
            code: WorkflowActivationErrorCode::SnapshotTooLarge,
            message: "durable workflow activation exceeds the session limit".to_string(),
            recoverable: true,
        });
    }
    let event_json = serde_json::json!({
        "type": "workflow.activated",
        "workflow_id": active.id,
        "revision": active.revision,
        "invoked_by": active.invoked_by,
        "activated_at": active.activated_at,
    })
    .to_string();
    metadata.insert(
        crate::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
        snapshot_json,
    );
    metadata.insert(ACTIVE_WORKFLOW_METADATA_KEY.to_string(), active_json);
    metadata.insert(
        ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.to_string(),
        durable_json,
    );
    metadata.insert(
        WORKFLOW_ACTIVATION_EVENT_METADATA_KEY.to_string(),
        event_json,
    );
    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_metadata::{
        SKILL_RUNTIME_PINNED_SNAPSHOT_KEY, SKILL_RUNTIME_SELECTED_CATALOG_KEY,
        SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    };
    use crate::{
        SkillActivationDescriptor, SkillActivationSelection, SkillActivationSnapshotEntry,
        SkillDefinition, WorkflowCatalogEntry, WorkflowStatus,
    };

    fn entry(id: &str, revision: u64) -> WorkflowCatalogEntry {
        WorkflowCatalogEntry {
            id: id.to_string(),
            name: id.to_string(),
            description: format!("{id} metadata"),
            kind: WorkflowKind::Instruction,
            source: WorkflowSource::Project,
            revision,
            content_digest: "test-digest".to_string(),
            version: "1".to_string(),
            invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
            argument_schema: serde_json::json!({"type": "object"}),
            status: WorkflowStatus::Valid,
            legacy: false,
            migration_status: None,
            last_error: None,
            winner: true,
            shadowed_candidates: Vec::new(),
        }
    }

    fn snapshot_entry(
        catalog_entry: WorkflowCatalogEntry,
        resources: BTreeMap<String, Vec<u8>>,
    ) -> SkillActivationSnapshotEntry {
        SkillActivationSnapshotEntry {
            definition: SkillDefinition::new(
                catalog_entry.id.clone(),
                catalog_entry.name.clone(),
                catalog_entry.description.clone(),
                format!("{} fixed instructions", catalog_entry.id),
            ),
            revision: catalog_entry.revision,
            catalog_entry,
            resources,
        }
    }

    #[test]
    fn activation_narrows_large_candidate_pin_to_selected_exact_revision() {
        let selected = entry("selected", 7);
        let unselected = entry("large-unselected", 9);
        let mut skills = BTreeMap::new();
        skills.insert(
            selected.id.clone(),
            snapshot_entry(selected.clone(), BTreeMap::new()),
        );
        skills.insert(
            unselected.id.clone(),
            snapshot_entry(
                unselected.clone(),
                BTreeMap::from([("assets/large.bin".to_string(), vec![1; 600 * 1024])]),
            ),
        );
        let candidate = SkillActivationSnapshot {
            catalog_revision: 41,
            selected_skill_mode: None,
            skills,
        };
        assert!(
            serde_json::to_vec(&candidate)
                .expect("candidate serialization")
                .len()
                > MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES
        );
        let mut metadata = HashMap::from([
            (
                SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(),
                serde_json::to_string(&vec![selected.clone(), unselected])
                    .expect("catalog serialization"),
            ),
            (
                SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
                "auto".to_string(),
            ),
            (
                SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
                serde_json::to_string(&candidate).expect("snapshot serialization"),
            ),
        ]);

        let active = record_loaded_workflow_activation(
            &mut metadata,
            "selected",
            "fixed-context".to_string(),
        )
        .expect("selected workflow activation");

        assert_eq!(active.revision, 7);
        assert_eq!(active.invoked_by, WorkflowInvokedBy::Model);
        let narrowed: SkillActivationSnapshot = serde_json::from_str(
            metadata
                .get(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
                .expect("narrowed pinned snapshot"),
        )
        .expect("valid narrowed snapshot");
        assert_eq!(narrowed.catalog_revision, 41);
        assert_eq!(
            narrowed.skills.keys().cloned().collect::<Vec<_>>(),
            ["selected"]
        );
        assert_eq!(narrowed.skills["selected"].revision, 7);
        let durable: DurableWorkflowActivation = serde_json::from_str(
            metadata
                .get(ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
                .expect("durable active snapshot"),
        )
        .expect("valid durable snapshot");
        assert_eq!(durable.active.revision, 7);
        assert_eq!(durable.snapshot.skills["selected"].revision, 7);
        assert!(
            metadata[ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY].len()
                <= MAX_DURABLE_WORKFLOW_ACTIVATION_BYTES
        );
    }

    #[test]
    fn activation_rejects_selected_workflow_over_durable_limit_atomically() {
        let selected = entry("oversized", 11);
        let snapshot = SkillActivationSnapshot {
            catalog_revision: 52,
            selected_skill_mode: None,
            skills: BTreeMap::from([(
                selected.id.clone(),
                snapshot_entry(
                    selected.clone(),
                    BTreeMap::from([("assets/large.bin".to_string(), vec![2; 600 * 1024])]),
                ),
            )]),
        };
        let mut metadata = HashMap::from([
            (
                SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(),
                serde_json::to_string(&vec![selected]).expect("catalog serialization"),
            ),
            (
                SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
                "auto".to_string(),
            ),
            (
                SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
                serde_json::to_string(&snapshot).expect("snapshot serialization"),
            ),
        ]);

        let diagnostic = record_loaded_workflow_activation(
            &mut metadata,
            "oversized",
            "fixed-context".to_string(),
        )
        .expect_err("oversized selected workflow must not become active");

        assert_eq!(
            diagnostic.code,
            WorkflowActivationErrorCode::SnapshotTooLarge
        );
        assert!(!metadata.contains_key(ACTIVE_WORKFLOW_METADATA_KEY));
        assert!(!metadata.contains_key(ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
        assert!(!metadata.contains_key(WORKFLOW_ACTIVATION_EVENT_METADATA_KEY));
        let unchanged: SkillActivationSnapshot = serde_json::from_str(
            metadata
                .get(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
                .expect("candidate snapshot remains available"),
        )
        .expect("valid unchanged candidate snapshot");
        assert_eq!(unchanged.skills["oversized"].revision, 11);
    }

    #[test]
    fn explicit_candidate_rejects_invalid_arguments_without_metadata_mutation() {
        let mut selected = entry("review", 13);
        selected.argument_schema = serde_json::json!({
            "type": "object",
            "properties": {"depth": {"type": "integer"}},
            "required": ["depth"],
            "additionalProperties": false
        });
        let descriptor = SkillActivationDescriptor {
            catalog_revision: 71,
            skill_revisions: BTreeMap::from([("review".to_string(), 13)]),
            selected_skill_mode: Some("explicit".to_string()),
        };
        let activation = SkillActivationSelection {
            skills: vec![snapshot_entry(selected.clone(), BTreeMap::new()).definition],
            catalog_entries: vec![selected.clone()],
            catalog_diagnostic: WorkflowCatalogDiagnostic {
                total_candidates: 1,
                advertised_candidates: 1,
                initial_chars: 128,
                final_chars: 128,
                char_budget: 1024,
                token_budget: 256,
                compressed_descriptions: false,
                shortlisted: false,
                omitted_ids: Vec::new(),
            },
            descriptor,
        };
        let snapshot = SkillActivationSnapshot {
            catalog_revision: 71,
            selected_skill_mode: Some("explicit".to_string()),
            skills: BTreeMap::from([(
                "review".to_string(),
                snapshot_entry(selected, BTreeMap::new()),
            )]),
        };
        let selection = WorkflowSelection {
            id: "review".to_string(),
            source: WorkflowSource::Project,
            revision: 13,
            args: serde_json::json!({"depth": "deep"}),
        };
        let mut metadata = HashMap::from([
            ("preserve".to_string(), "exact".to_string()),
            (
                SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
                "prior".to_string(),
            ),
        ]);
        let before = metadata.clone();

        let diagnostic =
            persist_explicit_workflow_candidate(&mut metadata, &selection, &activation, &snapshot)
                .expect_err("invalid arguments fail before metadata publication");

        assert_eq!(
            diagnostic.code,
            WorkflowActivationErrorCode::InvalidSelection
        );
        assert_eq!(metadata, before);
    }
}
