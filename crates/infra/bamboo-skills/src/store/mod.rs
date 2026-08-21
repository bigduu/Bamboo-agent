//! Skill store with in-memory cache and markdown persistence.
//!
//! This module provides the central storage and management system for skills.
//! Skills are loaded from Markdown files on disk and cached in memory for
//! fast access during agent execution.
//!
//! # Architecture
//!
//! The skill store uses a dual-layer architecture:
//! 1. **Disk Storage**: Skills are persisted as Markdown files in the skills directory
//! 2. **In-Memory Cache**: Loaded skills are cached in a `RwLock<HashMap>` for fast access
//!
//! # Skill Discovery
//!
//! On initialization, the store:
//! 1. Scans the skills directory for `SKILL.md` files
//! 2. Parses frontmatter and content for each skill
//! 3. Loads skills into the in-memory cache
//! 4. Creates built-in skills if the directory is empty
//!
//! # Read-Only Design
//!
//! Skills are designed to be edited as Markdown files directly, not through
//! the API. All modification methods return `SkillError::ReadOnly`.
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::skill::{SkillStore, SkillStoreConfig};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = SkillStoreConfig {
//!         skills_dir: PathBuf::from("./skills"),
//!         ..Default::default()
//!     };
//!
//!     let store = SkillStore::new(config);
//!     store.initialize().await.expect("Failed to initialize");
//!
//!     // List all skills
//!     let skills = store.list_skills(None, false).await;
//!     for skill in skills {
//!         println!("{}: {}", skill.name, skill.description);
//!     }
//! }
//! ```

pub mod builtin;
pub mod parser;
pub mod storage;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tracing::info;

use crate::catalog::{
    entry_from_skill, legacy_migration_status, load_bundle_metadata, LegacyWorkflowMigrationStatus,
    ShadowedWorkflowCandidate, WorkflowCatalogEntry, WorkflowCatalogEvent,
    WorkflowCatalogEventKind, WorkflowCatalogSnapshot, WorkflowKind, WorkflowStatus,
};
use crate::store::builtin::{archive_exact_legacy_materialization, load_builtin_skill_bundles};
use crate::store::parser::render_skill_markdown;
use crate::store::storage::{
    discover_plugin_skill_dirs, ensure_skills_dir,
    load_skills_from_discovery_dirs_detailed_with_limits, open_skill_file_no_follow,
    write_skill_file, FailedSkillRecord, LoadedSkillRecord, SkillDirectorySource,
    SkillDiscoveryDir,
};
use crate::types::{
    SkillDefinition, SkillError, SkillFilter, SkillId, SkillResult, SkillStoreConfig,
};

const MAX_PINNED_SKILL_ACTIVATIONS: usize = 256;
const MAX_WORKFLOW_FILE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKFLOW_SKILL_BYTES: usize = 32 * 1024 * 1024;
const MAX_WORKFLOW_PUBLICATION_BYTES: usize = 128 * 1024 * 1024;
const MAX_RETAINED_WORKFLOW_BYTES: usize = 256 * 1024 * 1024;
const MAX_WORKFLOW_RESOURCES_PER_SKILL: usize = 1024;
const MAX_WORKFLOW_RESOURCES_PER_PUBLICATION: usize = 4096;
const MAX_WORKFLOW_RESOURCE_PATH_BYTES: usize = 1024;
const MAX_WORKFLOWS_PER_PUBLICATION: usize = 1024;
const MAX_CACHED_WORKSPACE_STORES: usize = 64;
const MAX_CACHED_WORKSPACE_ALIASES: usize = 256;
const MAX_CACHED_MODE_STORES: usize = 16;
static NEXT_SKILL_STORE_TOKEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
pub(crate) struct SkillSnapshotLimits {
    pub(crate) max_file_bytes: usize,
    pub(crate) max_skill_bytes: usize,
    pub(crate) max_publication_bytes: usize,
    pub(crate) max_retained_bytes: usize,
}

impl Default for SkillSnapshotLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: MAX_WORKFLOW_FILE_BYTES,
            max_skill_bytes: MAX_WORKFLOW_SKILL_BYTES,
            max_publication_bytes: MAX_WORKFLOW_PUBLICATION_BYTES,
            max_retained_bytes: MAX_RETAINED_WORKFLOW_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
struct ActivationBudgetCharge {
    definition_bytes: usize,
    resources: Vec<(usize, usize)>,
}

#[derive(Debug, Default)]
struct RetainedResourceBudgetState {
    total_bytes: usize,
    activations: HashMap<String, ActivationBudgetCharge>,
    publications: HashMap<u64, ActivationBudgetCharge>,
    resources: HashMap<usize, (usize, usize)>,
}

#[derive(Debug, Default)]
struct RetainedResourceBudget {
    state: Mutex<RetainedResourceBudgetState>,
}

impl RetainedResourceBudget {
    fn replace(
        &self,
        activation_id: &str,
        definition_bytes: usize,
        resources: Vec<(usize, usize)>,
        limit: usize,
    ) -> SkillResult<()> {
        let mut state = self.state.lock().expect("workflow retained budget lock");
        if !state.activations.contains_key(activation_id)
            && state.activations.len() >= MAX_PINNED_SKILL_ACTIVATIONS
        {
            return Err(SkillError::Storage(format!(
                "global active workflow snapshot capacity ({MAX_PINNED_SKILL_ACTIVATIONS}) reached"
            )));
        }
        let old = state.activations.get(activation_id);
        let old_definition_bytes = old.map(|charge| charge.definition_bytes).unwrap_or(0);
        let old_resource_ids: HashSet<usize> = old
            .map(|charge| charge.resources.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default();
        let mut projected = state.total_bytes.saturating_sub(old_definition_bytes);
        for resource_id in &old_resource_ids {
            if state
                .resources
                .get(resource_id)
                .is_some_and(|(_, references)| *references == 1)
            {
                projected = projected.saturating_sub(state.resources[resource_id].0);
            }
        }
        projected = projected.saturating_add(definition_bytes);
        for (resource_id, bytes) in &resources {
            let remains_after_replace =
                state
                    .resources
                    .get(resource_id)
                    .is_some_and(|(_, references)| {
                        *references > usize::from(old_resource_ids.contains(resource_id))
                    });
            if !remains_after_replace {
                projected = projected.saturating_add(*bytes);
            }
        }
        if projected > limit {
            return Err(SkillError::Storage(format!(
                "retained workflow snapshot budget exceeded ({projected} > {limit} bytes)"
            )));
        }

        if let Some(old) = state.activations.remove(activation_id) {
            state.total_bytes = state.total_bytes.saturating_sub(old.definition_bytes);
            for (resource_id, _) in old.resources {
                if let Some((bytes, references)) = state.resources.get_mut(&resource_id) {
                    *references -= 1;
                    if *references == 0 {
                        let bytes = *bytes;
                        state.resources.remove(&resource_id);
                        state.total_bytes = state.total_bytes.saturating_sub(bytes);
                    }
                }
            }
        }
        state.total_bytes = state.total_bytes.saturating_add(definition_bytes);
        for (resource_id, bytes) in &resources {
            if !state.resources.contains_key(resource_id) {
                state.total_bytes = state.total_bytes.saturating_add(*bytes);
                state.resources.insert(*resource_id, (*bytes, 0));
            }
            state
                .resources
                .get_mut(resource_id)
                .expect("retained resource charge")
                .1 += 1;
        }
        state.activations.insert(
            activation_id.to_string(),
            ActivationBudgetCharge {
                definition_bytes,
                resources,
            },
        );
        Ok(())
    }

    fn release(&self, activation_id: &str) {
        let mut state = self.state.lock().expect("workflow retained budget lock");
        let Some(old) = state.activations.remove(activation_id) else {
            return;
        };
        state.total_bytes = state.total_bytes.saturating_sub(old.definition_bytes);
        for (resource_id, _) in old.resources {
            if let Some((bytes, references)) = state.resources.get_mut(&resource_id) {
                *references -= 1;
                if *references == 0 {
                    let bytes = *bytes;
                    state.resources.remove(&resource_id);
                    state.total_bytes = state.total_bytes.saturating_sub(bytes);
                }
            }
        }
    }

    fn replace_publication(
        &self,
        store_token: u64,
        definition_bytes: usize,
        resources: Vec<(usize, usize)>,
        limit: usize,
    ) -> SkillResult<()> {
        let mut state = self.state.lock().expect("workflow retained budget lock");
        let old = state.publications.get(&store_token);
        let old_definition_bytes = old.map(|charge| charge.definition_bytes).unwrap_or(0);
        let old_resource_ids: HashSet<usize> = old
            .map(|charge| charge.resources.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default();
        let mut projected = state.total_bytes.saturating_sub(old_definition_bytes);
        for resource_id in &old_resource_ids {
            if state
                .resources
                .get(resource_id)
                .is_some_and(|(_, refs)| *refs == 1)
            {
                projected = projected.saturating_sub(state.resources[resource_id].0);
            }
        }
        projected = projected.saturating_add(definition_bytes);
        for (resource_id, bytes) in &resources {
            let remains = state.resources.get(resource_id).is_some_and(|(_, refs)| {
                *refs > usize::from(old_resource_ids.contains(resource_id))
            });
            if !remains {
                projected = projected.saturating_add(*bytes);
            }
        }
        if projected > limit {
            return Err(SkillError::Storage(format!(
                "global workflow snapshot budget exceeded ({projected} > {limit} bytes)"
            )));
        }
        if let Some(old) = state.publications.remove(&store_token) {
            Self::remove_charge(&mut state, old);
        }
        Self::add_charge(&mut state, definition_bytes, &resources);
        state.publications.insert(
            store_token,
            ActivationBudgetCharge {
                definition_bytes,
                resources,
            },
        );
        Ok(())
    }

    fn release_publication(&self, store_token: u64) {
        let mut state = self.state.lock().expect("workflow retained budget lock");
        if let Some(old) = state.publications.remove(&store_token) {
            Self::remove_charge(&mut state, old);
        }
    }

    fn remove_charge(state: &mut RetainedResourceBudgetState, charge: ActivationBudgetCharge) {
        state.total_bytes = state.total_bytes.saturating_sub(charge.definition_bytes);
        for (resource_id, _) in charge.resources {
            if let Some((bytes, references)) = state.resources.get_mut(&resource_id) {
                *references -= 1;
                if *references == 0 {
                    let bytes = *bytes;
                    state.resources.remove(&resource_id);
                    state.total_bytes = state.total_bytes.saturating_sub(bytes);
                }
            }
        }
    }

    fn add_charge(
        state: &mut RetainedResourceBudgetState,
        definition_bytes: usize,
        resources: &[(usize, usize)],
    ) {
        state.total_bytes = state.total_bytes.saturating_add(definition_bytes);
        for (resource_id, bytes) in resources {
            if !state.resources.contains_key(resource_id) {
                state.total_bytes = state.total_bytes.saturating_add(*bytes);
                state.resources.insert(*resource_id, (*bytes, 0));
            }
            state
                .resources
                .get_mut(resource_id)
                .expect("resource charge")
                .1 += 1;
        }
    }
}

pub(crate) type SkillResourceSnapshot = std::sync::Arc<HashMap<String, std::sync::Arc<Vec<u8>>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillActivationDescriptor {
    pub catalog_revision: u64,
    pub skill_revisions: BTreeMap<SkillId, u64>,
    pub selected_skill_mode: Option<String>,
}

/// Serializable immutable activation payload. This is intentionally separate
/// from the in-memory Arc graph so a session can restore the exact workflow
/// revision after a server restart without consulting a newer catalog.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SkillActivationSnapshot {
    /// Opaque identity of the global/Project/workspace publication that owns
    /// these bytes. Durable snapshots must never be replayed in another
    /// resource scope after a session is reassigned.
    #[serde(default)]
    pub resource_scope_fingerprint: String,
    pub catalog_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_skill_mode: Option<String>,
    pub skills: BTreeMap<SkillId, SkillActivationSnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SkillActivationSnapshotEntry {
    pub definition: SkillDefinition,
    pub catalog_entry: WorkflowCatalogEntry,
    pub revision: u64,
    #[serde(default)]
    pub resources: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
struct PinnedSkillDefinition {
    definition: Arc<SkillDefinition>,
    catalog_entry: WorkflowCatalogEntry,
    root: PathBuf,
    revision: u64,
    resources: SkillResourceSnapshot,
}

#[derive(Debug, Clone)]
struct PinnedSkillActivation {
    catalog_revision: u64,
    selected_skill_mode: Option<String>,
    restored_from_durable_snapshot: bool,
    skills: HashMap<SkillId, PinnedSkillDefinition>,
}

#[derive(Debug, Default)]
struct PinnedSkillActivations {
    by_id: HashMap<String, PinnedSkillActivation>,
}

impl PinnedSkillActivations {
    fn insert(
        &mut self,
        activation_id: String,
        activation: PinnedSkillActivation,
    ) -> SkillResult<()> {
        if !self.by_id.contains_key(&activation_id)
            && self.by_id.len() >= MAX_PINNED_SKILL_ACTIVATIONS
        {
            return Err(SkillError::Storage(format!(
                "active workflow snapshot capacity ({MAX_PINNED_SKILL_ACTIVATIONS}) reached"
            )));
        }
        self.by_id.insert(activation_id, activation);
        Ok(())
    }

    fn remove(&mut self, activation_id: &str) -> Option<PinnedSkillActivation> {
        self.by_id.remove(activation_id)
    }
}

fn invalid_placeholder(
    id: &str,
    source: SkillDirectorySource,
    revision: u64,
    error: &str,
    migration_status: Option<LegacyWorkflowMigrationStatus>,
) -> WorkflowCatalogEntry {
    WorkflowCatalogEntry {
        id: id.to_string(),
        name: id.to_string(),
        description: "Invalid workflow bundle".to_string(),
        kind: WorkflowKind::Instruction,
        source: source.into(),
        revision,
        version: "1".to_string(),
        invocation_policy: serde_json::json!({"explicit": false, "automatic": false}),
        argument_schema: serde_json::json!({"type": "object"}),
        status: WorkflowStatus::Invalid,
        legacy: migration_status.is_some(),
        migration_status,
        last_error: Some(error.to_string()),
        winner: true,
        shadowed_candidates: Vec::new(),
    }
}

fn stable_workspace_hash(path: &Path) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    path.as_os_str()
        .to_string_lossy()
        .as_bytes()
        .iter()
        .fold(OFFSET, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
        })
}

async fn snapshot_skill_resources(
    roots: &HashMap<SkillId, PathBuf>,
    definitions: &HashMap<SkillId, SkillDefinition>,
    catalog_entries: &[WorkflowCatalogEntry],
    previous: &HashMap<SkillId, SkillResourceSnapshot>,
    limits: SkillSnapshotLimits,
) -> SkillResult<HashMap<SkillId, SkillResourceSnapshot>> {
    let mut snapshots = HashMap::with_capacity(roots.len());
    let mut publication_bytes = 0usize;
    let mut publication_resource_count = 0usize;
    for (skill_id, root) in roots {
        let definition_bytes = definitions
            .get(skill_id)
            .and_then(|definition| serde_json::to_vec(definition).ok())
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        let reuses_last_known_good = catalog_entries
            .iter()
            .find(|entry| entry.id == *skill_id)
            .is_some_and(|entry| entry.status == WorkflowStatus::Invalid);
        if reuses_last_known_good {
            if let Some(resources) = previous.get(skill_id) {
                let resource_bytes = resources.values().map(|bytes| bytes.len()).sum::<usize>();
                let skill_bytes = definition_bytes.saturating_add(resource_bytes);
                if skill_bytes > limits.max_skill_bytes {
                    return Err(SkillError::Storage(format!(
                        "workflow '{skill_id}' snapshot exceeds per-skill limit ({skill_bytes} > {} bytes)",
                        limits.max_skill_bytes
                    )));
                }
                publication_bytes = publication_bytes.saturating_add(skill_bytes);
                publication_resource_count =
                    publication_resource_count.saturating_add(resources.len());
                if publication_bytes > limits.max_publication_bytes
                    || publication_resource_count > MAX_WORKFLOW_RESOURCES_PER_PUBLICATION
                {
                    return Err(SkillError::Storage(
                        "workflow catalog publication exceeds snapshot limits".to_string(),
                    ));
                }
                snapshots.insert(skill_id.clone(), resources.clone());
                continue;
            }
        }
        let skill_markdown_bytes = tokio::fs::metadata(root.join("SKILL.md"))
            .await
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);
        if skill_markdown_bytes > limits.max_file_bytes {
            return Err(SkillError::Storage(format!(
                "workflow '{skill_id}' SKILL.md exceeds per-file limit ({skill_markdown_bytes} > {} bytes)",
                limits.max_file_bytes
            )));
        }
        let paths = crate::resource_helpers::list_skill_resource_paths_bounded(
            root,
            MAX_WORKFLOW_RESOURCES_PER_SKILL,
            MAX_WORKFLOW_RESOURCE_PATH_BYTES,
        )?;
        let mut resources = HashMap::with_capacity(paths.len());
        let mut resource_bytes = 0usize;
        for relative_path in paths {
            let resource = root.join(&relative_path);
            let file_bytes = tokio::fs::metadata(&resource).await?.len() as usize;
            if file_bytes > limits.max_file_bytes {
                return Err(SkillError::Storage(format!(
                    "workflow '{skill_id}' resource '{relative_path}' exceeds per-file limit ({file_bytes} > {} bytes)",
                    limits.max_file_bytes
                )));
            }
            let file = open_skill_file_no_follow(&resource).await?;
            let mut bytes = Vec::with_capacity(file_bytes.min(limits.max_file_bytes));
            file.take(limits.max_file_bytes.saturating_add(1) as u64)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > limits.max_file_bytes {
                return Err(SkillError::Storage(format!(
                    "workflow '{skill_id}' resource '{relative_path}' exceeds per-file limit ({} > {} bytes)",
                    bytes.len(),
                    limits.max_file_bytes
                )));
            }
            resource_bytes = resource_bytes.saturating_add(bytes.len());
            let projected_skill_bytes = definition_bytes
                .saturating_add(skill_markdown_bytes)
                .saturating_add(resource_bytes);
            if projected_skill_bytes > limits.max_skill_bytes {
                return Err(SkillError::Storage(format!(
                    "workflow '{skill_id}' snapshot exceeds per-skill limit ({projected_skill_bytes} > {} bytes)",
                    limits.max_skill_bytes
                )));
            }
            resources.insert(relative_path, std::sync::Arc::new(bytes));
        }
        let skill_bytes = definition_bytes
            .saturating_add(skill_markdown_bytes)
            .saturating_add(resource_bytes);
        if skill_bytes > limits.max_skill_bytes {
            return Err(SkillError::Storage(format!(
                "workflow '{skill_id}' snapshot exceeds per-skill limit ({skill_bytes} > {} bytes)",
                limits.max_skill_bytes
            )));
        }
        publication_bytes = publication_bytes.saturating_add(skill_bytes);
        if publication_bytes > limits.max_publication_bytes {
            return Err(SkillError::Storage(format!(
                "workflow catalog publication exceeds limit ({publication_bytes} > {} bytes)",
                limits.max_publication_bytes
            )));
        }
        publication_resource_count = publication_resource_count.saturating_add(resources.len());
        if publication_resource_count > MAX_WORKFLOW_RESOURCES_PER_PUBLICATION {
            return Err(SkillError::Storage(format!(
                "workflow catalog resource count exceeds limit ({publication_resource_count} > {MAX_WORKFLOW_RESOURCES_PER_PUBLICATION})"
            )));
        }
        snapshots.insert(skill_id.clone(), std::sync::Arc::new(resources));
    }
    Ok(snapshots)
}

fn skill_metadata_flag(skill: &SkillDefinition, name: &str) -> bool {
    skill.metadata.as_ref().is_some_and(|metadata| {
        metadata.get(name).and_then(serde_json::Value::as_bool) == Some(true)
    })
}

async fn loaded_record_is_workflow(
    record: &LoadedSkillRecord,
    previous_workflow_roots: &HashMap<SkillId, PathBuf>,
) -> bool {
    // Explicit user-requested migration materializes a real Skill. The
    // read-only source adapter remains a separate Workflow with the same ID.
    if skill_metadata_flag(&record.skill, "legacy_migration") {
        return false;
    }
    if skill_metadata_flag(&record.skill, "legacy_adapter")
        || skill_metadata_flag(&record.skill, "legacy_import")
    {
        return true;
    }
    if tokio::fs::try_exists(record.skill_root.join("workflow.yaml"))
        .await
        .unwrap_or(false)
    {
        return true;
    }
    match load_bundle_metadata(&record.skill_root).await {
        Ok(metadata) => metadata.kind == WorkflowKind::Orchestration,
        Err(_) => previous_workflow_roots.get(&record.skill.id) == Some(&record.skill_root),
    }
}

async fn failed_record_is_workflow(
    record: &FailedSkillRecord,
    previous_workflow_roots: &HashMap<SkillId, PathBuf>,
) -> bool {
    if record
        .skill_root
        .extension()
        .and_then(|value| value.to_str())
        == Some("md")
        || tokio::fs::try_exists(record.skill_root.join("workflow.yaml"))
            .await
            .unwrap_or(false)
    {
        return true;
    }
    record
        .skill_id
        .as_ref()
        .is_some_and(|id| previous_workflow_roots.get(id) == Some(&record.skill_root))
}

fn preserve_catalog_entry_revisions(
    entries: &mut [WorkflowCatalogEntry],
    previous_catalog: &WorkflowCatalogSnapshot,
    definition_changed: &HashSet<String>,
) {
    for entry in entries {
        let Some(previous) = previous_catalog
            .entries
            .iter()
            .find(|previous| previous.id == entry.id)
        else {
            continue;
        };
        let mut comparable = entry.clone();
        comparable.revision = previous.revision;
        if !definition_changed.contains(&entry.id) && comparable == *previous {
            entry.revision = previous.revision;
        }
    }
}

/// Persistent storage for skills with in-memory caching.
///
/// Manages a collection of skills loaded from Markdown files on disk.
/// Uses a `RwLock<HashMap>` for thread-safe concurrent access.
///
/// # Thread Safety
///
/// All operations use async/await with `RwLock` to allow multiple readers
/// or a single writer, ensuring safe concurrent access from multiple tasks.
///
/// # Example
///
/// ```rust,ignore
/// let store = SkillStore::new(SkillStoreConfig::default());
/// store.initialize().await?;
///
/// // Get a specific skill
/// let skill = store.get_skill("my-skill").await?;
/// println!("Skill: {}", skill.name);
/// ```
type ProjectWorkspaceStoreKey = (String, PathBuf, Option<PathBuf>);
type SharedSkillStore = std::sync::Arc<SkillStore>;

const SKILL_WATCH_CHANNEL_CAPACITY: usize = 256;
const SKILL_WATCH_QUIET_PERIOD: std::time::Duration = std::time::Duration::from_millis(120);
const SKILL_WATCH_MAX_BATCH: std::time::Duration = std::time::Duration::from_secs(1);

/// Access events (and access-time metadata updates) cannot change a catalog.
/// Reject them in the native callback so a Linux reload's own file reads do
/// not refill the queue and trigger an unbounded reload feedback loop.
fn watcher_event_can_change_catalog(event: &notify::Event) -> bool {
    !matches!(
        event.kind,
        notify::EventKind::Access(_)
            | notify::EventKind::Modify(notify::event::ModifyKind::Metadata(
                notify::event::MetadataKind::AccessTime
            ))
    )
}

#[derive(Debug, Default)]
struct SkillWatcherCounters {
    received_events: AtomicU64,
    rejected_events: AtomicU64,
    coalesced_events: AtomicU64,
    overflowed_events: AtomicU64,
    reloads: AtomicU64,
    reload_failures: AtomicU64,
    registration_canonicalizations: AtomicU64,
    watch_rebinds: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SkillWatcherActivity {
    pub received_events: u64,
    pub rejected_events: u64,
    pub coalesced_events: u64,
    pub overflowed_events: u64,
    pub reloads: u64,
    pub reload_failures: u64,
    pub registration_canonicalizations: u64,
    pub watch_rebinds: u64,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct ProcessedWatcherBatch {
    paths: Vec<PathBuf>,
    relevant: bool,
}

impl SkillWatcherCounters {
    fn snapshot(&self) -> SkillWatcherActivity {
        SkillWatcherActivity {
            received_events: self.received_events.load(Ordering::Relaxed),
            rejected_events: self.rejected_events.load(Ordering::Relaxed),
            coalesced_events: self.coalesced_events.load(Ordering::Relaxed),
            overflowed_events: self.overflowed_events.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            reload_failures: self.reload_failures.load(Ordering::Relaxed),
            registration_canonicalizations: self
                .registration_canonicalizations
                .load(Ordering::Relaxed),
            watch_rebinds: self.watch_rebinds.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
struct CatalogWatchPlan {
    /// Actual catalog roots. Existing roots are watched recursively; missing
    /// roots are discovered through a shallow nearest-existing ancestor.
    catalog_roots: Vec<PathBuf>,
    /// Structural containers whose creation makes a missing catalog root
    /// reachable (for example `<workspace>/.bamboo`). Exact-match only.
    container_paths: Vec<PathBuf>,
}

impl CatalogWatchPlan {
    fn is_relevant(&self, path: &Path) -> bool {
        self.catalog_roots
            .iter()
            .any(|root| path == root || path.starts_with(root))
            || self
                .container_paths
                .iter()
                .any(|container| path == container)
    }
}

fn lexical_normalize_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !path.is_absolute() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn nearest_existing_directory(path: &Path) -> Option<PathBuf> {
    let mut candidate = Some(path);
    while let Some(current) = candidate {
        if current.is_dir() {
            return Some(current.to_path_buf());
        }
        candidate = current.parent();
    }
    None
}

fn desired_catalog_watch_registrations(
    plan: &CatalogWatchPlan,
) -> BTreeMap<PathBuf, notify::RecursiveMode> {
    fn insert(
        registrations: &mut BTreeMap<PathBuf, notify::RecursiveMode>,
        path: PathBuf,
        mode: notify::RecursiveMode,
    ) {
        registrations
            .entry(path)
            .and_modify(|existing| {
                if matches!(mode, notify::RecursiveMode::Recursive) {
                    *existing = mode;
                }
            })
            .or_insert(mode);
    }

    let mut desired = BTreeMap::new();
    for root in &plan.catalog_roots {
        if root.is_dir() {
            insert(&mut desired, root.clone(), notify::RecursiveMode::Recursive);
        }
        if let Some(parent) = root.parent().and_then(nearest_existing_directory) {
            insert(&mut desired, parent, notify::RecursiveMode::NonRecursive);
        }
    }
    for container in &plan.container_paths {
        if container.is_dir() {
            insert(
                &mut desired,
                container.clone(),
                notify::RecursiveMode::NonRecursive,
            );
        }
        if let Some(parent) = container.parent().and_then(nearest_existing_directory) {
            insert(&mut desired, parent, notify::RecursiveMode::NonRecursive);
        }
    }
    desired
}

fn sync_catalog_watch_registrations<W: notify::Watcher>(
    watcher: &mut W,
    plan: &CatalogWatchPlan,
    registered: &mut HashMap<PathBuf, notify::RecursiveMode>,
    counters: &SkillWatcherCounters,
) {
    let desired = desired_catalog_watch_registrations(plan);
    let stale: Vec<_> = registered
        .iter()
        .filter(|(path, mode)| desired.get(*path) != Some(*mode))
        .map(|(path, _)| path.clone())
        .collect();
    for path in stale {
        let _ = watcher.unwatch(&path);
        registered.remove(&path);
        counters.watch_rebinds.fetch_add(1, Ordering::Relaxed);
    }

    for (path, mode) in desired {
        if registered.get(&path) == Some(&mode) {
            continue;
        }
        match watcher.watch(&path, mode) {
            Ok(()) => {
                registered.insert(path.clone(), mode);
                counters.watch_rebinds.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    watch_root = %path.display(),
                    recursive = matches!(mode, notify::RecursiveMode::Recursive),
                    "skill watcher registration bound"
                );
            }
            Err(error) => tracing::warn!(
                "Failed to watch skill catalog root {}: {error}",
                path.display()
            ),
        }
    }
}

pub struct SkillStore {
    store_token: u64,
    /// Serializes publication and observation of the correlated snapshot maps below.
    snapshot_publish_lock: RwLock<()>,
    /// In-memory cache of loaded skills, keyed by skill ID.
    skills: RwLock<HashMap<SkillId, SkillDefinition>>,
    /// Root directory of each loaded skill (keyed by skill ID).
    skill_roots: RwLock<HashMap<SkillId, PathBuf>>,
    /// Immutable bytes for every resource in the currently published generation.
    /// Activations retain these snapshots after a watcher publishes a newer bundle.
    skill_resources: RwLock<HashMap<SkillId, SkillResourceSnapshot>>,
    /// Policy metadata for prompt/explicit Skill activation only.
    skill_catalog: RwLock<WorkflowCatalogSnapshot>,
    /// Workflow definitions are published independently from Skills so equal
    /// IDs can coexist without either identity shadowing the other.
    workflow_definitions: RwLock<HashMap<SkillId, SkillDefinition>>,
    workflow_roots: RwLock<HashMap<SkillId, PathBuf>>,
    workflow_resources: RwLock<HashMap<SkillId, SkillResourceSnapshot>>,
    /// Public Workflow catalog (legacy adapters and orchestration bundles).
    catalog: RwLock<WorkflowCatalogSnapshot>,
    /// Session/activation-scoped immutable workflow generations. The cache is bounded,
    /// and normal runtime finalization explicitly removes completed activations.
    pinned_activations: RwLock<PinnedSkillActivations>,
    next_revision: AtomicU64,
    watcher_started: AtomicBool,
    watcher_counters: Arc<SkillWatcherCounters>,
    #[cfg(test)]
    processed_watcher_batches: tokio::sync::broadcast::Sender<ProcessedWatcherBatch>,
    catalog_events: tokio::sync::broadcast::Sender<WorkflowCatalogEvent>,
    reload_lock: tokio::sync::Mutex<()>,
    mode_stores: RwLock<HashMap<String, std::sync::Arc<SkillStore>>>,
    workspace_stores: RwLock<HashMap<PathBuf, std::sync::Arc<SkillStore>>>,
    project_workspace_stores: RwLock<HashMap<ProjectWorkspaceStoreKey, SharedSkillStore>>,
    retained_budget: Arc<RetainedResourceBudget>,
    snapshot_limits: SkillSnapshotLimits,
    project_home_dir: Option<PathBuf>,
    workspace_overlay_dir: Option<PathBuf>,

    /// Configuration specifying the skills directory path.
    config: SkillStoreConfig,
}

impl SkillStore {
    /// Return an opaque, restart-stable identity for this resource
    /// publication. No filesystem path is persisted in the snapshot.
    pub fn resource_scope_fingerprint(&self) -> String {
        fn update_path(hasher: &mut Sha256, path: &Path) {
            let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                let bytes = canonical.as_os_str().as_bytes();
                hasher.update(b"unix\0");
                hasher.update((bytes.len() as u64).to_be_bytes());
                hasher.update(bytes);
            }
            #[cfg(windows)]
            {
                use std::os::windows::ffi::OsStrExt;
                let units = canonical.as_os_str().encode_wide().collect::<Vec<_>>();
                hasher.update(b"windows-utf16\0");
                hasher.update((units.len() as u64).to_be_bytes());
                for unit in units {
                    hasher.update(unit.to_le_bytes());
                }
            }
        }

        let mut hasher = Sha256::new();
        hasher.update(b"bamboo.skill-resource-scope.v1\0");
        update_path(&mut hasher, &self.config.skills_dir);
        match (
            self.project_home_dir.as_deref(),
            self.workspace_overlay_dir.as_deref(),
        ) {
            (Some(project_home), workspace) => {
                hasher.update(b"project\0");
                update_path(&mut hasher, project_home);
                if let Some(workspace) = workspace {
                    hasher.update(b"workspace\0");
                    update_path(&mut hasher, workspace);
                }
            }
            (None, Some(workspace)) => {
                hasher.update(b"workspace\0");
                update_path(&mut hasher, workspace);
            }
            (None, None) => hasher.update(b"global\0"),
        }
        if let Some(mode) = self.effective_mode(None) {
            hasher.update(b"mode\0");
            hasher.update((mode.len() as u64).to_be_bytes());
            hasher.update(mode.as_bytes());
        }
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }

    fn normalize_mode(raw_mode: Option<&str>) -> Option<String> {
        let raw = raw_mode?.trim();
        if raw.is_empty() {
            return None;
        }

        let normalized = raw.to_ascii_lowercase();
        if !normalized.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        }) {
            tracing::warn!(
                "Ignoring invalid skill mode '{}' (allowed: lowercase letters, digits, hyphen)",
                raw
            );
            return None;
        }

        Some(normalized)
    }

    fn effective_mode(&self, mode_override: Option<&str>) -> Option<String> {
        Self::normalize_mode(mode_override)
            .or_else(|| Self::normalize_mode(self.config.active_mode.as_deref()))
    }

    fn sibling_skills_mode_dir(base_skills_dir: &Path, mode: &str) -> PathBuf {
        let parent = base_skills_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        parent.join(format!("skills-{mode}"))
    }

    fn project_home_skills_dir(project_home: &Path) -> PathBuf {
        project_home.join("skills")
    }

    fn project_home_skills_mode_dir(project_home: &Path, mode: &str) -> PathBuf {
        project_home.join(format!("skills-{mode}"))
    }

    fn workspace_skills_dir(workspace: &Path) -> PathBuf {
        workspace.join(".bamboo").join("skills")
    }

    fn workspace_skills_mode_dir(workspace: &Path, mode: &str) -> PathBuf {
        workspace.join(".bamboo").join(format!("skills-{mode}"))
    }

    /// Embedded bundles live outside the user-editable global skills directory.
    ///
    /// Keeping the version in the directory name lets Bamboo replace its own
    /// read-only materialization without mistaking a user clone with the same
    /// id for a builtin.
    fn builtin_skills_dir(base_skills_dir: &Path) -> PathBuf {
        base_skills_dir
            .parent()
            .map(|parent| parent.join("skills-builtin-v1"))
            .unwrap_or_else(|| PathBuf::from("skills-builtin-v1"))
    }

    /// Root directory under which installed plugins live, derived as a
    /// sibling of `skills_dir` (same pattern as [`Self::sibling_skills_mode_dir`]),
    /// so tests that point `skills_dir` at a tempdir automatically get an
    /// isolated `<tempdir>/plugins` instead of accidentally globbing the
    /// real `~/.bamboo/plugins` on whatever machine runs the test. In
    /// production `skills_dir` is `${BAMBOO_DATA_DIR}/skills`, so this
    /// resolves to the same place as `bamboo_config::paths::plugins_dir()`
    /// (`${BAMBOO_DATA_DIR}/plugins`).
    fn plugins_root_dir(base_skills_dir: &Path) -> PathBuf {
        let parent = base_skills_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        parent.join("plugins")
    }

    fn discovery_dirs_for_mode(&self, mode_override: Option<&str>) -> Vec<SkillDiscoveryDir> {
        let mut dirs = vec![SkillDiscoveryDir {
            dir: Self::builtin_skills_dir(&self.config.skills_dir),
            source: SkillDirectorySource::Builtin,
            mode: None,
        }];
        let active_mode = self.effective_mode(mode_override);

        // Enable the conventional user-level source for the production store.
        // Custom/test stores remain hermetic instead of reading the developer's
        // real home directory unexpectedly.
        if self.config.skills_dir == bamboo_config::paths::bamboo_dir().join("skills") {
            if let Some(dir) = bamboo_config::paths::agents_skills_dir() {
                dirs.push(SkillDiscoveryDir {
                    dir,
                    source: SkillDirectorySource::Agents,
                    mode: None,
                });
            }
        }

        dirs.push(SkillDiscoveryDir {
            dir: self.config.skills_dir.clone(),
            source: SkillDirectorySource::Global,
            mode: None,
        });
        if let Some(mode) = active_mode.as_ref() {
            dirs.push(SkillDiscoveryDir {
                dir: Self::sibling_skills_mode_dir(&self.config.skills_dir, mode),
                source: SkillDirectorySource::Global,
                mode: Some(mode.clone()),
            });
        }

        if let Some(project_home) = self.project_home_dir.as_ref() {
            dirs.push(SkillDiscoveryDir {
                dir: Self::project_home_skills_dir(project_home),
                source: SkillDirectorySource::Project,
                mode: None,
            });
            if let Some(mode) = active_mode.as_ref() {
                dirs.push(SkillDiscoveryDir {
                    dir: Self::project_home_skills_mode_dir(project_home, mode),
                    source: SkillDirectorySource::Project,
                    mode: Some(mode.clone()),
                });
            }
        }
        if let Some(workspace) = self.workspace_overlay_dir.as_ref() {
            dirs.push(SkillDiscoveryDir {
                dir: Self::workspace_skills_dir(workspace),
                source: SkillDirectorySource::Workspace,
                mode: None,
            });
            if let Some(mode) = active_mode.as_ref() {
                dirs.push(SkillDiscoveryDir {
                    dir: Self::workspace_skills_mode_dir(workspace, mode),
                    source: SkillDirectorySource::Workspace,
                    mode: Some(mode.clone()),
                });
            }
        }

        dirs
    }

    async fn resolve_skills_maps_for_mode(
        &self,
        mode_override: Option<&str>,
    ) -> SkillResult<(HashMap<SkillId, SkillDefinition>, HashMap<SkillId, PathBuf>)> {
        let mode_store = self.skill_store_for_mode(mode_override).await?;
        let store = mode_store.as_deref().unwrap_or(self);
        store.reload().await?;
        let _snapshot_guard = store.snapshot_publish_lock.read().await;
        let skills = store.skills.read().await.clone();
        let roots = store.skill_roots.read().await.clone();
        Ok((skills, roots))
    }

    /// Precedence rank: higher wins when two discovery dirs provide the same
    /// skill id. `~/.agents` augments Bamboo without shadowing Bamboo-owned
    /// global/project definitions; plugin skills remain the lowest tier.
    /// an installed plugin can never silently shadow a user's own global or
    /// project skill of the same id; within the same source tier, a
    /// mode-specific candidate still overrides a generic one (unchanged from
    /// the pre-plugin behavior).
    fn source_rank(source: SkillDirectorySource) -> u8 {
        match source {
            SkillDirectorySource::Builtin => 0,
            SkillDirectorySource::Plugin => 1,
            SkillDirectorySource::Agents => 2,
            SkillDirectorySource::Global => 3,
            SkillDirectorySource::Project => 4,
            SkillDirectorySource::Workspace => 5,
        }
    }

    /// Create a new skill store with the given configuration.
    ///
    /// The store is created empty and must be initialized using [`initialize`](Self::initialize)
    /// before it can be used.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying the skills directory path.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::skill::{SkillStore, SkillStoreConfig};
    /// use std::path::PathBuf;
    ///
    /// let config = SkillStoreConfig {
    ///     skills_dir: PathBuf::from("./skills"),
    ///     ..Default::default()
    /// };
    /// let store = SkillStore::new(config);
    /// ```
    pub fn new(config: SkillStoreConfig) -> Self {
        Self::new_with_shared_snapshot_state(
            config,
            Arc::new(RetainedResourceBudget::default()),
            SkillSnapshotLimits::default(),
        )
    }

    fn new_with_shared_snapshot_state(
        config: SkillStoreConfig,
        retained_budget: Arc<RetainedResourceBudget>,
        snapshot_limits: SkillSnapshotLimits,
    ) -> Self {
        let workspace_overlay_dir = config.project_dir.clone();
        Self::new_with_resource_scope(
            config,
            retained_budget,
            snapshot_limits,
            None,
            workspace_overlay_dir,
        )
    }

    fn new_with_resource_scope(
        config: SkillStoreConfig,
        retained_budget: Arc<RetainedResourceBudget>,
        snapshot_limits: SkillSnapshotLimits,
        project_home_dir: Option<PathBuf>,
        workspace_overlay_dir: Option<PathBuf>,
    ) -> Self {
        let (catalog_events, _) = tokio::sync::broadcast::channel(128);
        #[cfg(test)]
        let (processed_watcher_batches, _) = tokio::sync::broadcast::channel(128);
        Self {
            store_token: NEXT_SKILL_STORE_TOKEN.fetch_add(1, Ordering::Relaxed),
            snapshot_publish_lock: RwLock::new(()),
            skills: RwLock::new(HashMap::new()),
            skill_roots: RwLock::new(HashMap::new()),
            skill_resources: RwLock::new(HashMap::new()),
            skill_catalog: RwLock::new(WorkflowCatalogSnapshot::default()),
            workflow_definitions: RwLock::new(HashMap::new()),
            workflow_roots: RwLock::new(HashMap::new()),
            workflow_resources: RwLock::new(HashMap::new()),
            catalog: RwLock::new(WorkflowCatalogSnapshot::default()),
            pinned_activations: RwLock::new(PinnedSkillActivations::default()),
            next_revision: AtomicU64::new(1),
            watcher_started: AtomicBool::new(false),
            watcher_counters: Arc::new(SkillWatcherCounters::default()),
            #[cfg(test)]
            processed_watcher_batches,
            catalog_events,
            reload_lock: tokio::sync::Mutex::new(()),
            mode_stores: RwLock::new(HashMap::new()),
            workspace_stores: RwLock::new(HashMap::new()),
            project_workspace_stores: RwLock::new(HashMap::new()),
            retained_budget,
            snapshot_limits,
            project_home_dir,
            workspace_overlay_dir,
            config,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_snapshot_limits(
        config: SkillStoreConfig,
        snapshot_limits: SkillSnapshotLimits,
    ) -> Self {
        Self::new_with_shared_snapshot_state(
            config,
            Arc::new(RetainedResourceBudget::default()),
            snapshot_limits,
        )
    }

    /// Initialize the store, loading skills from disk.
    ///
    /// This method performs the following steps:
    /// 1. Creates the skills directory if it doesn't exist.
    /// 2. Syncs built-in skill bundles from compile-time embedded files (overwrites built-ins).
    /// 3. Reloads all skills into memory after synchronization.
    ///
    /// # Returns
    ///
    /// `Ok(())` on successful initialization.
    ///
    /// # Errors
    ///
    /// Returns `SkillError` if:
    /// - The skills directory cannot be created.
    /// - Skill files cannot be read or parsed.
    /// - Built-in skills cannot be written.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let store = SkillStore::new(SkillStoreConfig::default());
    /// store.initialize().await.expect("Failed to initialize");
    /// ```
    pub async fn initialize(&self) -> SkillResult<()> {
        info!("Initializing skill store...");
        ensure_skills_dir(&self.config.skills_dir).await?;
        let workflows_dir = self
            .config
            .skills_dir
            .parent()
            .map(|parent| parent.join("workflows"))
            .unwrap_or_else(|| PathBuf::from("workflows"));
        self.create_builtin_skills().await?;
        for diagnostic in
            crate::legacy::migrate_legacy_yaml_workflows(&workflows_dir, &self.config.skills_dir)
                .await
        {
            if !diagnostic.can_map_to_bundle {
                tracing::warn!("Legacy YAML migration: {}", diagnostic.message);
            }
        }
        self.load().await?;

        info!("Skill store initialized");
        Ok(())
    }

    /// Load skills from disk into the in-memory cache.
    ///
    /// Scans the skills directory for all `SKILL.md` files, parses them,
    /// and loads them into the internal HashMap cache.
    ///
    /// # Returns
    ///
    /// The number of skills successfully loaded.
    ///
    /// # Errors
    ///
    /// Returns `SkillError` if the skills directory cannot be read.
    async fn load(&self) -> SkillResult<usize> {
        let _reload_guard = self.reload_lock.lock().await;
        self.load_locked().await
    }

    async fn load_locked(&self) -> SkillResult<usize> {
        let dirs = self.discovery_dirs_for_mode(None);
        let mut dirs = dirs;
        let plugins_root = Self::plugins_root_dir(&self.config.skills_dir);
        dirs.extend(discover_plugin_skill_dirs(&plugins_root).await);
        let mut report = load_skills_from_discovery_dirs_detailed_with_limits(
            &dirs,
            self.snapshot_limits.max_file_bytes,
            MAX_WORKFLOWS_PER_PUBLICATION,
        )
        .await?;
        let mut legacy_dirs =
            crate::legacy::discover_plugin_legacy_workflow_dirs(&plugins_root).await;
        if let Some(data_dir) = self.config.skills_dir.parent() {
            legacy_dirs.push(SkillDiscoveryDir {
                dir: data_dir.join("workflows"),
                source: SkillDirectorySource::Global,
                mode: None,
            });
        }
        if let Some(workspace) = self.workspace_overlay_dir.as_ref() {
            legacy_dirs.push(SkillDiscoveryDir {
                dir: workspace.join(".bamboo").join("workflows"),
                source: SkillDirectorySource::Workspace,
                mode: None,
            });
        }
        let used_candidates = report.loaded.len().saturating_add(report.failed.len());
        let remaining_candidates = MAX_WORKFLOWS_PER_PUBLICATION.saturating_sub(used_candidates);
        let legacy_report = crate::legacy::load_legacy_markdown_workflow_records(
            &legacy_dirs,
            self.snapshot_limits.max_file_bytes,
            remaining_candidates,
        )
        .await?;
        report.loaded.extend(legacy_report.loaded);
        report.failed.extend(legacy_report.failed);

        let (
            previous_skills,
            previous_roots,
            previous_resources,
            previous_skill_catalog,
            previous_workflows,
            previous_workflow_roots,
            previous_workflow_resources,
            previous_catalog,
        ) = {
            let _snapshot_guard = self.snapshot_publish_lock.read().await;
            (
                self.skills.read().await.clone(),
                self.skill_roots.read().await.clone(),
                self.skill_resources.read().await.clone(),
                self.skill_catalog.read().await.clone(),
                self.workflow_definitions.read().await.clone(),
                self.workflow_roots.read().await.clone(),
                self.workflow_resources.read().await.clone(),
                self.catalog.read().await.clone(),
            )
        };
        let mut skill_loaded = Vec::new();
        let mut workflow_loaded = Vec::new();
        for record in report.loaded {
            if loaded_record_is_workflow(&record, &previous_workflow_roots).await {
                workflow_loaded.push(record);
            } else {
                skill_loaded.push(record);
            }
        }
        let mut skill_failed = Vec::new();
        let mut workflow_failed = Vec::new();
        for record in report.failed {
            if failed_record_is_workflow(&record, &previous_workflow_roots).await {
                workflow_failed.push(record);
            } else {
                skill_failed.push(record);
            }
        }

        let revision = self.next_revision.load(Ordering::SeqCst);
        let (resolved_skills, resolved_roots, mut skill_entries) = self
            .resolve_catalog(
                skill_loaded,
                skill_failed,
                &previous_skills,
                &previous_roots,
                &previous_skill_catalog,
                revision,
            )
            .await;
        let (resolved_workflows, resolved_workflow_roots, mut workflow_entries) = self
            .resolve_catalog(
                workflow_loaded,
                workflow_failed,
                &previous_workflows,
                &previous_workflow_roots,
                &previous_catalog,
                revision,
            )
            .await;
        for entry in &mut workflow_entries {
            if !entry.legacy && entry.kind != WorkflowKind::Orchestration {
                // This partition contains only Workflow identities. A brand
                // new invalid workflow.yaml has no parsed metadata yet, but it
                // must remain catalog-visible as a degraded orchestration.
                entry.kind = WorkflowKind::Orchestration;
            }
        }
        let count = resolved_skills
            .len()
            .saturating_add(resolved_workflows.len());
        let resolved_resources = snapshot_skill_resources(
            &resolved_roots,
            &resolved_skills,
            &skill_entries,
            &previous_resources,
            self.snapshot_limits,
        )
        .await?;
        let resolved_workflow_resources = snapshot_skill_resources(
            &resolved_workflow_roots,
            &resolved_workflows,
            &workflow_entries,
            &previous_workflow_resources,
            self.snapshot_limits,
        )
        .await?;
        let skill_definition_changed: HashSet<String> = resolved_skills
            .iter()
            .filter(|(id, skill)| {
                previous_skills.get(*id) != Some(*skill)
                    || previous_roots.get(*id) != resolved_roots.get(*id)
                    || previous_resources.get(*id) != resolved_resources.get(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        let workflow_definition_changed: HashSet<String> = resolved_workflows
            .iter()
            .filter(|(id, workflow)| {
                previous_workflows.get(*id) != Some(*workflow)
                    || previous_workflow_roots.get(*id) != resolved_workflow_roots.get(*id)
                    || previous_workflow_resources.get(*id) != resolved_workflow_resources.get(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        // Entry revisions identify each definition generation rather than the
        // containing publication. Preserve them independently in both
        // namespaces across unrelated updates.
        preserve_catalog_entry_revisions(
            &mut skill_entries,
            &previous_skill_catalog,
            &skill_definition_changed,
        );
        preserve_catalog_entry_revisions(
            &mut workflow_entries,
            &previous_catalog,
            &workflow_definition_changed,
        );
        let next_skill_catalog = WorkflowCatalogSnapshot {
            revision,
            entries: skill_entries,
        };
        let next_catalog = WorkflowCatalogSnapshot {
            revision,
            entries: workflow_entries,
        };
        let mut comparable_previous_skill_catalog = previous_skill_catalog.clone();
        comparable_previous_skill_catalog.revision = revision;
        let mut comparable_previous = previous_catalog.clone();
        comparable_previous.revision = revision;
        if resolved_skills == previous_skills
            && resolved_roots == previous_roots
            && resolved_resources == previous_resources
            && next_skill_catalog == comparable_previous_skill_catalog
            && resolved_workflows == previous_workflows
            && resolved_workflow_roots == previous_workflow_roots
            && resolved_workflow_resources == previous_workflow_resources
            && next_catalog == comparable_previous
        {
            return Ok(count);
        }
        let publication_definition_bytes = resolved_skills
            .values()
            .chain(resolved_workflows.values())
            .map(|skill| {
                serde_json::to_vec(skill)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0)
            })
            .sum::<usize>();
        let mut publication_resources = HashMap::<usize, usize>::new();
        for snapshot in resolved_resources
            .values()
            .chain(resolved_workflow_resources.values())
        {
            for bytes in snapshot.values() {
                publication_resources
                    .entry(Arc::as_ptr(bytes) as usize)
                    .or_insert(bytes.len());
            }
        }
        // Acquire every async publication guard before changing the synchronous
        // shared budget. After `replace_publication` succeeds there are no await
        // points until all correlated maps and the revision are committed.
        let _snapshot_guard = self.snapshot_publish_lock.write().await;
        let mut skills_guard = self.skills.write().await;
        let mut roots_guard = self.skill_roots.write().await;
        let mut resources_guard = self.skill_resources.write().await;
        let mut skill_catalog_guard = self.skill_catalog.write().await;
        let mut workflows_guard = self.workflow_definitions.write().await;
        let mut workflow_roots_guard = self.workflow_roots.write().await;
        let mut workflow_resources_guard = self.workflow_resources.write().await;
        let mut catalog_guard = self.catalog.write().await;
        self.retained_budget.replace_publication(
            self.store_token,
            publication_definition_bytes,
            publication_resources.into_iter().collect(),
            self.snapshot_limits.max_retained_bytes,
        )?;
        self.next_revision.fetch_add(1, Ordering::SeqCst);
        *skills_guard = resolved_skills;
        *roots_guard = resolved_roots;
        *resources_guard = resolved_resources;
        *skill_catalog_guard = next_skill_catalog.clone();
        *workflows_guard = resolved_workflows;
        *workflow_roots_guard = resolved_workflow_roots;
        *workflow_resources_guard = resolved_workflow_resources;
        *catalog_guard = next_catalog.clone();
        drop(catalog_guard);
        drop(workflow_resources_guard);
        drop(workflow_roots_guard);
        drop(workflows_guard);
        drop(skill_catalog_guard);
        drop(resources_guard);
        drop(roots_guard);
        drop(skills_guard);
        drop(_snapshot_guard);
        self.publish_catalog_events(
            &previous_skill_catalog,
            &next_skill_catalog,
            &skill_definition_changed,
        );
        self.publish_catalog_events(
            &previous_catalog,
            &next_catalog,
            &workflow_definition_changed,
        );

        Ok(count)
    }

    async fn resolve_catalog(
        &self,
        loaded: Vec<LoadedSkillRecord>,
        failed: Vec<FailedSkillRecord>,
        previous_skills: &HashMap<SkillId, SkillDefinition>,
        previous_roots: &HashMap<SkillId, PathBuf>,
        previous_catalog: &WorkflowCatalogSnapshot,
        revision: u64,
    ) -> (
        HashMap<SkillId, SkillDefinition>,
        HashMap<SkillId, PathBuf>,
        Vec<WorkflowCatalogEntry>,
    ) {
        #[derive(Debug)]
        enum Candidate {
            Valid(LoadedSkillRecord),
            Invalid(FailedSkillRecord),
        }
        impl Candidate {
            fn source(&self) -> SkillDirectorySource {
                match self {
                    Self::Valid(record) => record.source,
                    Self::Invalid(record) => record.source,
                }
            }
            fn mode(&self) -> Option<&str> {
                match self {
                    Self::Valid(record) => record.mode.as_deref(),
                    Self::Invalid(record) => record.mode.as_deref(),
                }
            }
            fn root(&self) -> &Path {
                match self {
                    Self::Valid(record) => &record.skill_root,
                    Self::Invalid(record) => &record.skill_root,
                }
            }
            fn status(&self) -> WorkflowStatus {
                match self {
                    Self::Valid(_) => WorkflowStatus::Valid,
                    Self::Invalid(_) => WorkflowStatus::Invalid,
                }
            }
            fn error(&self) -> Option<String> {
                match self {
                    Self::Valid(_) => None,
                    Self::Invalid(record) => Some(record.error.clone()),
                }
            }
            fn migration_status(&self) -> Option<LegacyWorkflowMigrationStatus> {
                match self {
                    Self::Valid(record) => legacy_migration_status(&record.skill),
                    Self::Invalid(record)
                        if record
                            .skill_root
                            .extension()
                            .and_then(|value| value.to_str())
                            == Some("md") =>
                    {
                        Some(LegacyWorkflowMigrationStatus::Available)
                    }
                    Self::Invalid(_) => None,
                }
            }
            fn identity_rank(&self) -> u8 {
                match self {
                    Self::Valid(record)
                        if record.skill.metadata.as_ref().is_some_and(|metadata| {
                            metadata
                                .get("legacy_import")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true)
                        }) =>
                    {
                        1
                    }
                    Self::Valid(record)
                        if record.skill.metadata.as_ref().is_some_and(|metadata| {
                            metadata
                                .get("legacy_adapter")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true)
                        }) =>
                    {
                        2
                    }
                    Self::Invalid(record)
                        if record
                            .skill_root
                            .extension()
                            .and_then(|value| value.to_str())
                            == Some("md") =>
                    {
                        2
                    }
                    // A normal Skill or an explicitly migrated legacy bundle
                    // keeps existing precedence over a read-only adapter.
                    _ => 3,
                }
            }
        }

        let mut grouped: HashMap<String, Vec<Candidate>> = HashMap::new();
        for record in loaded {
            grouped
                .entry(record.skill.id.clone())
                .or_default()
                .push(Candidate::Valid(record));
        }
        for record in failed {
            if let Some(id) = record.skill_id.clone() {
                grouped
                    .entry(id)
                    .or_default()
                    .push(Candidate::Invalid(record));
            }
        }

        let mut ids: Vec<_> = grouped.keys().cloned().collect();
        ids.sort();
        let mut skills = HashMap::new();
        let mut roots = HashMap::new();
        let mut entries = Vec::new();
        for id in ids {
            let previous_entry = previous_catalog.entries.iter().find(|entry| entry.id == id);
            let mut candidates = grouped.remove(&id).unwrap_or_default();
            candidates.sort_by(|left, right| {
                Self::source_rank(right.source())
                    .cmp(&Self::source_rank(left.source()))
                    .then_with(|| right.mode().is_some().cmp(&left.mode().is_some()))
                    .then_with(|| right.identity_rank().cmp(&left.identity_rank()))
                    .then_with(|| left.root().cmp(right.root()))
            });
            let Some(winner) = candidates.first() else {
                continue;
            };
            let shadowed_candidates = candidates
                .iter()
                .skip(1)
                .map(|candidate| ShadowedWorkflowCandidate {
                    source: candidate.source().into(),
                    status: candidate.status(),
                    legacy: candidate.migration_status().is_some(),
                    migration_status: candidate.migration_status(),
                    last_error: candidate.error(),
                })
                .collect();

            let mut entry = match winner {
                Candidate::Valid(record) => match load_bundle_metadata(&record.skill_root).await {
                    Ok(metadata) => {
                        skills.insert(id.clone(), record.skill.clone());
                        roots.insert(id.clone(), record.skill_root.clone());
                        entry_from_skill(&record.skill, record.source, revision, metadata)
                    }
                    Err(error) => {
                        if previous_roots.get(&id) == Some(&record.skill_root) {
                            if let (Some(skill), Some(previous_entry)) =
                                (previous_skills.get(&id), previous_entry)
                            {
                                skills.insert(id.clone(), skill.clone());
                                roots.insert(id.clone(), record.skill_root.clone());
                                let mut entry = previous_entry.clone();
                                entry.status = WorkflowStatus::Invalid;
                                entry.last_error = Some(error);
                                entry
                            } else {
                                invalid_placeholder(
                                    &id,
                                    record.source,
                                    revision,
                                    &error,
                                    legacy_migration_status(&record.skill),
                                )
                            }
                        } else {
                            invalid_placeholder(
                                &id,
                                record.source,
                                revision,
                                &error,
                                legacy_migration_status(&record.skill),
                            )
                        }
                    }
                },
                Candidate::Invalid(record) => {
                    if previous_roots.get(&id) == Some(&record.skill_root) {
                        if let Some(skill) = previous_skills.get(&id) {
                            skills.insert(id.clone(), skill.clone());
                            roots.insert(id.clone(), record.skill_root.clone());
                            let mut entry = previous_entry.cloned().unwrap_or_else(|| {
                                entry_from_skill(skill, record.source, revision, Default::default())
                            });
                            entry.status = WorkflowStatus::Invalid;
                            entry.last_error = Some(record.error.clone());
                            entry
                        } else {
                            invalid_placeholder(
                                &id,
                                record.source,
                                revision,
                                &record.error,
                                winner.migration_status(),
                            )
                        }
                    } else {
                        invalid_placeholder(
                            &id,
                            record.source,
                            revision,
                            &record.error,
                            winner.migration_status(),
                        )
                    }
                }
            };
            entry.shadowed_candidates = shadowed_candidates;
            entries.push(entry);
        }
        (skills, roots, entries)
    }

    /// Return the current immutable metadata-only catalog snapshot.
    pub async fn workflow_catalog_snapshot(&self) -> WorkflowCatalogSnapshot {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        self.catalog.read().await.clone()
    }

    /// Return Skill activation metadata from the same publication machinery as
    /// the public Workflow catalog, but from its independent namespace.
    pub async fn skill_catalog_snapshot(&self) -> WorkflowCatalogSnapshot {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        self.skill_catalog.read().await.clone()
    }

    /// Return both command-facing namespaces from one publication generation.
    pub async fn command_catalog_snapshots(
        &self,
    ) -> (WorkflowCatalogSnapshot, WorkflowCatalogSnapshot) {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        (
            self.skill_catalog.read().await.clone(),
            self.catalog.read().await.clone(),
        )
    }

    /// Pin every new-format orchestration definition from one validated store
    /// publication. `workflow.yaml` bytes come from the immutable resource
    /// snapshot (including LKG recovery), never from a second filesystem read.
    pub async fn pin_workflow_definition_bundle(
        &self,
        root_id: &str,
        root_revision: u64,
    ) -> SkillResult<bamboo_domain::WorkflowDefinitionBundle> {
        if let Err(error) = self.reload().await {
            tracing::warn!(
                "Failed to reload workflows before bundle pin; using LKG publication: {error}"
            );
        }
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let catalog = self.catalog.read().await;
        let resources = self.workflow_resources.read().await;
        let mut definitions = BTreeMap::new();
        for entry in catalog
            .entries
            .iter()
            .filter(|entry| entry.winner && entry.kind == WorkflowKind::Orchestration)
        {
            let Some(bytes) = resources
                .get(&entry.id)
                .and_then(|resources| resources.get("workflow.yaml"))
            else {
                continue;
            };
            let value: serde_yaml::Value = serde_yaml::from_slice(bytes)
                .map_err(|_| SkillError::Validation("invalid pinned workflow yaml".to_string()))?;
            if value.get("workflow_schema").is_none() {
                // Legacy definitions stay catalog-visible but are not executable
                // by the versioned #578 runtime.
                continue;
            }
            let definition: bamboo_domain::WorkflowRunDefinition = serde_yaml::from_slice(bytes)
                .map_err(|_| SkillError::Validation("invalid pinned workflow definition".into()))?;
            bamboo_domain::CompiledWorkflow::compile(definition.clone())
                .map_err(|_| SkillError::Validation("invalid pinned workflow definition".into()))?;
            if definition.id != entry.id || definition.revision != entry.revision {
                return Err(SkillError::Validation(
                    "workflow identity does not match its catalog entry".to_string(),
                ));
            }
            definitions.insert(
                bamboo_domain::WorkflowDefinitionBundle::key(&definition.id, definition.revision),
                definition,
            );
        }
        let root_key = bamboo_domain::WorkflowDefinitionBundle::key(root_id, root_revision);
        if !definitions.contains_key(&root_key) {
            return Err(SkillError::NotFound(format!("{root_id}@{root_revision}")));
        }
        let root_invocation_policy = catalog
            .entries
            .iter()
            .find(|entry| entry.winner && entry.id == root_id && entry.revision == root_revision)
            .map(|entry| entry.invocation_policy.clone())
            .ok_or_else(|| SkillError::NotFound(format!("{root_id}@{root_revision}")))?;

        // Persist only the transitive closure reachable from the selected root.
        // Keeping every orchestration definition in the publication would copy
        // unrelated workflow contents into each run snapshot and unnecessarily
        // amplify its durable storage footprint.
        let mut reachable = BTreeMap::new();
        let mut pending = vec![root_key];
        while let Some(key) = pending.pop() {
            if reachable.contains_key(&key) {
                continue;
            }
            let definition = definitions.get(&key).cloned().ok_or_else(|| {
                SkillError::Validation(format!("pinned workflow dependency is missing: {key}"))
            })?;
            for step in &definition.steps {
                if let bamboo_domain::WorkflowStepKind::Workflow {
                    workflow_id,
                    revision,
                    ..
                } = &step.kind
                {
                    pending.push(bamboo_domain::WorkflowDefinitionBundle::key(
                        workflow_id,
                        *revision,
                    ));
                }
            }
            reachable.insert(key, definition);
        }
        Ok(bamboo_domain::WorkflowDefinitionBundle {
            publication_revision: catalog.revision,
            root_id: root_id.to_string(),
            root_revision,
            root_invocation_policy,
            definitions: reachable,
        })
    }

    /// Return definitions, roots, immutable resource bytes, and metadata from one
    /// validated publication. Callers that create an activation must pin directly
    /// from this tuple rather than resolving any component from the live store again.
    pub(crate) async fn activation_source_for_mode(
        &self,
        mode_override: Option<&str>,
    ) -> SkillResult<(
        Vec<SkillDefinition>,
        HashMap<SkillId, PathBuf>,
        HashMap<SkillId, SkillResourceSnapshot>,
        WorkflowCatalogSnapshot,
    )> {
        let mode_store = self.skill_store_for_mode(mode_override).await?;
        let store = mode_store.as_deref().unwrap_or(self);
        if let Err(error) = store.reload().await {
            tracing::warn!(
                "Failed to reload skills before policy-aware selection; using the last validated snapshot: {}",
                error
            );
        }

        let _snapshot_guard = store.snapshot_publish_lock.read().await;
        let mut skills = store
            .skills
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        skills.sort_by_key(|skill| skill.name.clone());
        let roots = store.skill_roots.read().await.clone();
        let resources = store.skill_resources.read().await.clone();
        let catalog = store.skill_catalog.read().await.clone();
        Ok((skills, roots, resources, catalog))
    }

    /// Return prompt-visible skills and their catalog metadata from one validated snapshot.
    pub(crate) async fn skills_and_catalog_for_mode(
        &self,
        mode_override: Option<&str>,
    ) -> SkillResult<(Vec<SkillDefinition>, WorkflowCatalogSnapshot)> {
        let (skills, _, _, catalog) = self.activation_source_for_mode(mode_override).await?;
        Ok((skills, catalog))
    }

    pub(crate) async fn pin_activation_from_source(
        &self,
        activation_id: &str,
        mode_override: Option<&str>,
        selected_skills: &[SkillDefinition],
        roots: &HashMap<SkillId, PathBuf>,
        resources: &HashMap<SkillId, SkillResourceSnapshot>,
        catalog: &WorkflowCatalogSnapshot,
    ) -> SkillResult<SkillActivationDescriptor> {
        let catalog_entries = catalog
            .entries
            .iter()
            .map(|entry| (entry.id.as_str(), entry))
            .collect::<HashMap<_, _>>();
        let mut pinned_skills = HashMap::with_capacity(selected_skills.len());
        let mut skill_revisions = BTreeMap::new();
        let mut definition_bytes = 0usize;
        let mut retained_resources = HashMap::<usize, usize>::new();
        for skill in selected_skills {
            let root = roots
                .get(&skill.id)
                .cloned()
                .ok_or_else(|| SkillError::NotFound(skill.id.clone()))?;
            let resource_snapshot = resources
                .get(&skill.id)
                .cloned()
                .ok_or_else(|| SkillError::NotFound(skill.id.clone()))?;
            let revision = catalog_entries
                .get(skill.id.as_str())
                .map(|entry| entry.revision)
                .ok_or_else(|| SkillError::NotFound(skill.id.clone()))?;
            let catalog_entry = (*catalog_entries
                .get(skill.id.as_str())
                .ok_or_else(|| SkillError::NotFound(skill.id.clone()))?)
            .clone();
            skill_revisions.insert(skill.id.clone(), revision);
            definition_bytes = definition_bytes.saturating_add(
                serde_json::to_vec(skill)
                    .map(|serialized| serialized.len())
                    .unwrap_or(0),
            );
            for bytes in resource_snapshot.values() {
                retained_resources
                    .entry(Arc::as_ptr(bytes) as usize)
                    .or_insert(bytes.len());
            }
            pinned_skills.insert(
                skill.id.clone(),
                PinnedSkillDefinition {
                    definition: Arc::new(skill.clone()),
                    catalog_entry,
                    root,
                    revision,
                    resources: resource_snapshot,
                },
            );
        }

        let mut activations = self.pinned_activations.write().await;
        if !activations.by_id.contains_key(activation_id)
            && activations.by_id.len() >= MAX_PINNED_SKILL_ACTIVATIONS
        {
            return Err(SkillError::Storage(format!(
                "active workflow snapshot capacity ({MAX_PINNED_SKILL_ACTIVATIONS}) reached"
            )));
        }
        self.retained_budget.replace(
            activation_id,
            definition_bytes,
            retained_resources.into_iter().collect(),
            self.snapshot_limits.max_retained_bytes,
        )?;
        activations.insert(
            activation_id.to_string(),
            PinnedSkillActivation {
                catalog_revision: catalog.revision,
                selected_skill_mode: self.effective_mode(mode_override),
                restored_from_durable_snapshot: false,
                skills: pinned_skills,
            },
        )?;
        Ok(SkillActivationDescriptor {
            catalog_revision: catalog.revision,
            skill_revisions,
            selected_skill_mode: self.effective_mode(mode_override),
        })
    }

    /// Lazily establish an activation for non-runner callers. The agent runtime pins
    /// during selection, before the first model/tool call; this fallback preserves the
    /// same invariant for SDK and direct-tool integrations.
    pub async fn pin_current_activation(
        &self,
        activation_id: &str,
        selected_skill_ids: &[String],
        mode_override: Option<&str>,
    ) -> SkillResult<SkillActivationDescriptor> {
        if let Some(descriptor) = self.activation_descriptor(activation_id).await {
            let requested = selected_skill_ids
                .iter()
                .map(|id| id.trim())
                .filter(|id| !id.is_empty())
                .collect::<HashSet<_>>();
            let pinned = descriptor
                .skill_revisions
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>();
            if requested == pinned
                && descriptor.selected_skill_mode == self.effective_mode(mode_override)
            {
                return Ok(descriptor);
            }
        }
        let (skills, roots, resources, catalog) =
            self.activation_source_for_mode(mode_override).await?;
        let selected = selected_skill_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .collect::<HashSet<_>>();
        let selected_skills = skills
            .into_iter()
            .filter(|skill| selected.contains(skill.id.as_str()))
            .collect::<Vec<_>>();
        if selected_skills.len() != selected.len() {
            let missing = selected_skill_ids
                .iter()
                .find(|id| !selected_skills.iter().any(|skill| &skill.id == *id))
                .cloned()
                .unwrap_or_default();
            return Err(SkillError::NotFound(missing));
        }
        self.pin_activation_from_source(
            activation_id,
            mode_override,
            &selected_skills,
            &roots,
            &resources,
            &catalog,
        )
        .await
    }

    pub async fn activation_descriptor(
        &self,
        activation_id: &str,
    ) -> Option<SkillActivationDescriptor> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        Some(SkillActivationDescriptor {
            catalog_revision: activation.catalog_revision,
            skill_revisions: activation
                .skills
                .iter()
                .map(|(id, skill)| (id.clone(), skill.revision))
                .collect(),
            selected_skill_mode: activation.selected_skill_mode.clone(),
        })
    }

    pub async fn pinned_activation_skills(
        &self,
        activation_id: &str,
    ) -> Option<(Vec<SkillDefinition>, SkillActivationDescriptor)> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        let mut skills = activation
            .skills
            .values()
            .map(|skill| skill.definition.as_ref().clone())
            .collect::<Vec<_>>();
        skills.sort_by(|left, right| left.id.cmp(&right.id));
        let descriptor = SkillActivationDescriptor {
            catalog_revision: activation.catalog_revision,
            skill_revisions: activation
                .skills
                .iter()
                .map(|(id, skill)| (id.clone(), skill.revision))
                .collect(),
            selected_skill_mode: activation.selected_skill_mode.clone(),
        };
        Some((skills, descriptor))
    }

    pub async fn pinned_activation_catalog_entries(
        &self,
        activation_id: &str,
    ) -> Option<Vec<WorkflowCatalogEntry>> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        let mut entries = activation
            .skills
            .values()
            .map(|skill| skill.catalog_entry.clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.id.cmp(&right.id));
        Some(entries)
    }

    pub async fn activation_was_restored(&self, activation_id: &str) -> bool {
        self.pinned_activations
            .read()
            .await
            .by_id
            .get(activation_id)
            .is_some_and(|activation| activation.restored_from_durable_snapshot)
    }

    pub async fn export_activation_snapshot(
        &self,
        activation_id: &str,
    ) -> Option<SkillActivationSnapshot> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        Some(SkillActivationSnapshot {
            resource_scope_fingerprint: self.resource_scope_fingerprint(),
            catalog_revision: activation.catalog_revision,
            selected_skill_mode: activation.selected_skill_mode.clone(),
            skills: activation
                .skills
                .iter()
                .map(|(id, skill)| {
                    (
                        id.clone(),
                        SkillActivationSnapshotEntry {
                            definition: skill.definition.as_ref().clone(),
                            catalog_entry: skill.catalog_entry.clone(),
                            revision: skill.revision,
                            resources: skill
                                .resources
                                .iter()
                                .map(|(path, bytes)| (path.clone(), bytes.as_ref().clone()))
                                .collect(),
                        },
                    )
                })
                .collect(),
        })
    }

    /// Restore an activation from session-persisted LKG bytes. The snapshot is
    /// recharged against the same global retained budget as live catalog pins.
    pub async fn restore_activation_snapshot(
        &self,
        activation_id: &str,
        snapshot: SkillActivationSnapshot,
    ) -> SkillResult<SkillActivationDescriptor> {
        const MAX_DURABLE_ACTIVATION_BYTES: usize = 512 * 1024;
        const MAX_DURABLE_ACTIVATION_SKILLS: usize = 32;
        const MAX_DURABLE_ACTIVATION_RESOURCES: usize = 1_024;
        let expected_scope = self.resource_scope_fingerprint();
        if snapshot.resource_scope_fingerprint.is_empty()
            || snapshot.resource_scope_fingerprint != expected_scope
        {
            return Err(SkillError::Validation(
                "persisted workflow activation resource scope mismatch".to_string(),
            ));
        }
        if snapshot.skills.is_empty() {
            return Err(SkillError::Validation(
                "persisted workflow activation is empty".to_string(),
            ));
        }
        if snapshot.skills.len() > MAX_DURABLE_ACTIVATION_SKILLS {
            return Err(SkillError::Validation(
                "persisted workflow activation contains too many skills".to_string(),
            ));
        }
        let serialized_bytes = serde_json::to_vec(&snapshot).map_err(|_| {
            SkillError::Validation("persisted workflow activation is invalid".to_string())
        })?;
        if serialized_bytes.len() > MAX_DURABLE_ACTIVATION_BYTES {
            return Err(SkillError::Validation(
                "persisted workflow activation exceeds the durable size limit".to_string(),
            ));
        }
        let resource_count = snapshot
            .skills
            .values()
            .map(|entry| entry.resources.len())
            .sum::<usize>();
        if resource_count > MAX_DURABLE_ACTIVATION_RESOURCES {
            return Err(SkillError::Validation(
                "persisted workflow activation contains too many resources".to_string(),
            ));
        }
        for (id, entry) in &snapshot.skills {
            if id != &entry.definition.id
                || id != &entry.catalog_entry.id
                || entry.revision == 0
                || entry.revision != entry.catalog_entry.revision
                || !entry.catalog_entry.winner
                || entry.catalog_entry.status != WorkflowStatus::Valid
            {
                return Err(SkillError::Validation(
                    "persisted workflow activation identity mismatch".to_string(),
                ));
            }
            for path in entry.resources.keys() {
                crate::resource_helpers::normalize_relative_resource_path(path)
                    .map_err(SkillError::Validation)?;
            }
        }
        let mut definition_bytes = 0usize;
        let mut retained_resources = HashMap::<usize, usize>::new();
        let mut pinned_skills = HashMap::with_capacity(snapshot.skills.len());
        let mut skill_revisions = BTreeMap::new();
        for (id, entry) in snapshot.skills {
            if id != entry.definition.id {
                return Err(SkillError::Validation(
                    "persisted workflow activation identity mismatch".to_string(),
                ));
            }
            definition_bytes = definition_bytes.saturating_add(
                serde_json::to_vec(&entry.definition)
                    .map(|bytes| bytes.len())
                    .unwrap_or(0),
            );
            let resources: SkillResourceSnapshot = Arc::new(
                entry
                    .resources
                    .into_iter()
                    .map(|(path, bytes)| (path, Arc::new(bytes)))
                    .collect(),
            );
            for bytes in resources.values() {
                retained_resources
                    .entry(Arc::as_ptr(bytes) as usize)
                    .or_insert(bytes.len());
            }
            skill_revisions.insert(id.clone(), entry.revision);
            pinned_skills.insert(
                id,
                PinnedSkillDefinition {
                    definition: Arc::new(entry.definition),
                    catalog_entry: entry.catalog_entry,
                    // Durable snapshots never persist an absolute filesystem path.
                    // Resource reads use the immutable embedded byte map.
                    root: PathBuf::new(),
                    revision: entry.revision,
                    resources,
                },
            );
        }

        let mut activations = self.pinned_activations.write().await;
        if !activations.by_id.contains_key(activation_id)
            && activations.by_id.len() >= MAX_PINNED_SKILL_ACTIVATIONS
        {
            return Err(SkillError::Storage(format!(
                "active workflow snapshot capacity ({MAX_PINNED_SKILL_ACTIVATIONS}) reached"
            )));
        }
        self.retained_budget.replace(
            activation_id,
            definition_bytes,
            retained_resources.into_iter().collect(),
            self.snapshot_limits.max_retained_bytes,
        )?;
        activations.insert(
            activation_id.to_string(),
            PinnedSkillActivation {
                catalog_revision: snapshot.catalog_revision,
                selected_skill_mode: snapshot.selected_skill_mode.clone(),
                restored_from_durable_snapshot: true,
                skills: pinned_skills,
            },
        )?;
        Ok(SkillActivationDescriptor {
            catalog_revision: snapshot.catalog_revision,
            skill_revisions,
            selected_skill_mode: snapshot.selected_skill_mode,
        })
    }

    pub async fn get_pinned_skill_with_root(
        &self,
        activation_id: &str,
        skill_id: &str,
    ) -> SkillResult<(SkillDefinition, PathBuf, u64, Vec<String>)> {
        let activations = self.pinned_activations.read().await;
        let activation = activations
            .by_id
            .get(activation_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let pinned = activation
            .skills
            .get(skill_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let mut resource_paths = pinned.resources.keys().cloned().collect::<Vec<_>>();
        resource_paths.sort();
        Ok((
            pinned.definition.as_ref().clone(),
            pinned.root.clone(),
            pinned.revision,
            resource_paths,
        ))
    }

    pub async fn get_pinned_skill_with_root_and_descriptor(
        &self,
        activation_id: &str,
        skill_id: &str,
    ) -> SkillResult<(
        SkillDefinition,
        PathBuf,
        u64,
        Vec<String>,
        SkillActivationDescriptor,
    )> {
        let activations = self.pinned_activations.read().await;
        let activation = activations
            .by_id
            .get(activation_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let pinned = activation
            .skills
            .get(skill_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let mut resource_paths = pinned.resources.keys().cloned().collect::<Vec<_>>();
        resource_paths.sort();
        let descriptor = SkillActivationDescriptor {
            catalog_revision: activation.catalog_revision,
            skill_revisions: activation
                .skills
                .iter()
                .map(|(id, skill)| (id.clone(), skill.revision))
                .collect(),
            selected_skill_mode: activation.selected_skill_mode.clone(),
        };
        Ok((
            pinned.definition.as_ref().clone(),
            pinned.root.clone(),
            pinned.revision,
            resource_paths,
            descriptor,
        ))
    }

    pub async fn read_pinned_skill_resource(
        &self,
        activation_id: &str,
        skill_id: &str,
        resource_path: &Path,
    ) -> SkillResult<Vec<u8>> {
        let activations = self.pinned_activations.read().await;
        let activation = activations
            .by_id
            .get(activation_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let pinned = activation
            .skills
            .get(skill_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let key = crate::resource_helpers::display_relative_path(resource_path);
        pinned
            .resources
            .get(&key)
            .map(|bytes| bytes.as_ref().clone())
            .ok_or_else(|| SkillError::NotFound(format!("{skill_id}/{key}")))
    }

    pub async fn read_pinned_skill_resource_with_descriptor(
        &self,
        activation_id: &str,
        skill_id: &str,
        resource_path: &Path,
    ) -> SkillResult<(Vec<u8>, SkillActivationDescriptor)> {
        let activations = self.pinned_activations.read().await;
        let activation = activations
            .by_id
            .get(activation_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let pinned = activation
            .skills
            .get(skill_id)
            .ok_or_else(|| SkillError::NotFound(skill_id.to_string()))?;
        let key = crate::resource_helpers::display_relative_path(resource_path);
        let bytes = pinned
            .resources
            .get(&key)
            .map(|bytes| bytes.as_ref().clone())
            .ok_or_else(|| SkillError::NotFound(format!("{skill_id}/{key}")))?;
        let descriptor = SkillActivationDescriptor {
            catalog_revision: activation.catalog_revision,
            skill_revisions: activation
                .skills
                .iter()
                .map(|(id, skill)| (id.clone(), skill.revision))
                .collect(),
            selected_skill_mode: activation.selected_skill_mode.clone(),
        };
        Ok((bytes, descriptor))
    }

    pub async fn pinned_allowed_tools(
        &self,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
    ) -> Option<Vec<String>> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        let mut tools = activation
            .skills
            .iter()
            .filter(|(skill_id, _)| !disabled_skill_ids.contains(*skill_id))
            .map(|(_, skill)| skill)
            .flat_map(|skill| skill.definition.tool_refs.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        tools.sort();
        Some(tools)
    }

    pub async fn pinned_allowed_tools_with_descriptor(
        &self,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
    ) -> Option<(Vec<String>, SkillActivationDescriptor)> {
        let activations = self.pinned_activations.read().await;
        let activation = activations.by_id.get(activation_id)?;
        let mut tools = activation
            .skills
            .iter()
            .filter(|(skill_id, _)| !disabled_skill_ids.contains(*skill_id))
            .flat_map(|(_, skill)| skill.definition.tool_refs.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        tools.sort();
        let descriptor = SkillActivationDescriptor {
            catalog_revision: activation.catalog_revision,
            skill_revisions: activation
                .skills
                .iter()
                .map(|(id, skill)| (id.clone(), skill.revision))
                .collect(),
            selected_skill_mode: activation.selected_skill_mode.clone(),
        };
        Some((tools, descriptor))
    }

    pub async fn release_activation(&self, activation_id: &str) {
        let mut activations = self.pinned_activations.write().await;
        // Hold the removed activation (and therefore its last resource Arcs)
        // through budget release. This prevents allocator address reuse from
        // turning raw Arc-pointer identity accounting into an ABA collision.
        let removed = activations.remove(activation_id);
        if removed.is_some() {
            self.retained_budget.release(activation_id);
        }
        drop(removed);
    }

    /// Release a session pin without depending on its workspace still existing.
    /// Session IDs are unique across scopes, so clearing the root and every
    /// cached workspace, Project/workspace, and mode store is both safe and
    /// robust to reassignment or deleted/unmounted resources.
    pub async fn release_activation_across_cached_scopes(&self, activation_id: &str) {
        self.release_activation(activation_id).await;
        let mut pending = self.cached_dependent_stores().await;
        let mut visited = HashSet::new();
        while let Some(store) = pending.pop() {
            if !visited.insert(store.store_token) {
                continue;
            }
            store.release_activation(activation_id).await;
            pending.extend(store.cached_dependent_stores().await);
        }
    }

    pub fn subscribe_workflow_catalog(
        &self,
    ) -> tokio::sync::broadcast::Receiver<WorkflowCatalogEvent> {
        self.catalog_events.subscribe()
    }

    /// Resolve a cached catalog/store for a per-session mode override. This keeps mode-specific
    /// selection on the same validated snapshot/LKG path as the default catalog instead of
    /// bypassing workflow metadata through a parallel directory scan.
    async fn skill_store_for_mode(
        &self,
        mode_override: Option<&str>,
    ) -> SkillResult<Option<std::sync::Arc<SkillStore>>> {
        let Some(mode) = self.effective_mode(mode_override) else {
            return Ok(None);
        };
        if Self::normalize_mode(self.config.active_mode.as_deref()).as_deref() == Some(&mode) {
            return Ok(None);
        }
        if let Some(store) = self.mode_stores.read().await.get(&mode).cloned() {
            return Ok(Some(store));
        }
        let mut stores = self.mode_stores.write().await;
        if let Some(store) = stores.get(&mode).cloned() {
            return Ok(Some(store));
        }
        if stores.len() >= MAX_CACHED_MODE_STORES {
            return Err(SkillError::Storage(format!(
                "cached workflow mode store capacity ({MAX_CACHED_MODE_STORES}) reached"
            )));
        }
        let store = std::sync::Arc::new(SkillStore::new_with_resource_scope(
            SkillStoreConfig {
                skills_dir: self.config.skills_dir.clone(),
                project_dir: self.config.project_dir.clone(),
                active_mode: Some(mode.clone()),
            },
            self.retained_budget.clone(),
            self.snapshot_limits,
            self.project_home_dir.clone(),
            self.workspace_overlay_dir.clone(),
        ));
        store.load().await?;
        store.start_live_reload();

        let mut events = store.subscribe_workflow_catalog();
        let aggregate = self.catalog_events.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        let _ = aggregate.send(event);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        stores.insert(mode, store.clone());
        Ok(Some(store))
    }

    fn publish_catalog_events(
        &self,
        previous: &WorkflowCatalogSnapshot,
        next: &WorkflowCatalogSnapshot,
        definition_changed: &HashSet<String>,
    ) {
        if previous.revision == 0 {
            return;
        }
        let previous_by_id: HashMap<_, _> = previous
            .entries
            .iter()
            .map(|entry| (entry.id.as_str(), entry))
            .collect();
        for entry in &next.entries {
            let old = previous_by_id.get(entry.id.as_str()).copied();
            let kind = match (old.map(|item| item.status), entry.status) {
                (Some(WorkflowStatus::Valid), WorkflowStatus::Invalid) => {
                    WorkflowCatalogEventKind::Invalid
                }
                (Some(WorkflowStatus::Invalid), WorkflowStatus::Valid) => {
                    WorkflowCatalogEventKind::Recovered
                }
                _ if definition_changed.contains(&entry.id)
                    || old.is_none()
                    || old.is_some_and(|item| item != entry) =>
                {
                    WorkflowCatalogEventKind::Changed
                }
                _ => continue,
            };
            let _ = self.catalog_events.send(WorkflowCatalogEvent {
                workflow_id: entry.id.clone(),
                revision: next.revision,
                kind,
                public_workflow: entry.is_public_workflow()
                    || old.is_some_and(WorkflowCatalogEntry::is_public_workflow),
                scope: "global".to_string(),
            });
        }
        for removed in previous.entries.iter().filter(|entry| {
            !next
                .entries
                .iter()
                .any(|candidate| candidate.id == entry.id)
        }) {
            let _ = self.catalog_events.send(WorkflowCatalogEvent {
                workflow_id: removed.id.clone(),
                revision: next.revision,
                kind: WorkflowCatalogEventKind::Changed,
                public_workflow: removed.is_public_workflow(),
                scope: "global".to_string(),
            });
        }
    }

    /// Resolve the complete isolated store for a server-owned session workspace.
    ///
    /// Catalog advertisement, prompt selection, and runtime resource access must
    /// all retain this same store so they observe the same winner and root.
    pub async fn skill_store_for_workspace(
        &self,
        workspace: &Path,
    ) -> SkillResult<std::sync::Arc<SkillStore>> {
        let requested_workspace = workspace.to_path_buf();
        // Keep the server-owned path as an alias. Once an activation has pinned
        // immutable bytes, deleting the workspace must not make its store
        // unreachable merely because canonicalization now fails.
        if let Some(store) = self
            .workspace_stores
            .read()
            .await
            .get(&requested_workspace)
            .cloned()
        {
            return Ok(store);
        }
        let workspace = tokio::fs::canonicalize(workspace).await?;
        let cached_canonical = self.workspace_stores.read().await.get(&workspace).cloned();
        if let Some(store) = cached_canonical {
            let mut stores = self.workspace_stores.write().await;
            if !stores.contains_key(&requested_workspace)
                && stores.len() >= MAX_CACHED_WORKSPACE_ALIASES
            {
                return Err(SkillError::Storage(format!(
                    "cached workflow workspace alias capacity ({MAX_CACHED_WORKSPACE_ALIASES}) reached"
                )));
            }
            stores.insert(requested_workspace, store.clone());
            return Ok(store);
        }
        let mut stores = self.workspace_stores.write().await;
        if let Some(store) = stores.get(&workspace).cloned() {
            if !stores.contains_key(&requested_workspace)
                && stores.len() >= MAX_CACHED_WORKSPACE_ALIASES
            {
                return Err(SkillError::Storage(format!(
                    "cached workflow workspace alias capacity ({MAX_CACHED_WORKSPACE_ALIASES}) reached"
                )));
            }
            stores.insert(requested_workspace, store.clone());
            return Ok(store);
        }
        let unique_store_count = stores
            .values()
            .map(Arc::as_ptr)
            .collect::<HashSet<_>>()
            .len();
        if unique_store_count >= MAX_CACHED_WORKSPACE_STORES {
            return Err(SkillError::Storage(format!(
                "cached workflow workspace store capacity ({MAX_CACHED_WORKSPACE_STORES}) reached"
            )));
        }
        let new_aliases = 1 + usize::from(requested_workspace != workspace);
        if stores.len().saturating_add(new_aliases) > MAX_CACHED_WORKSPACE_ALIASES {
            return Err(SkillError::Storage(format!(
                "cached workflow workspace alias capacity ({MAX_CACHED_WORKSPACE_ALIASES}) reached"
            )));
        }
        let store = std::sync::Arc::new(SkillStore::new_with_shared_snapshot_state(
            SkillStoreConfig {
                skills_dir: self.config.skills_dir.clone(),
                project_dir: Some(workspace.clone()),
                active_mode: self.config.active_mode.clone(),
            },
            self.retained_budget.clone(),
            self.snapshot_limits,
        ));
        store.load().await?;
        store.start_live_reload();

        let scope = format!("workspace:{:016x}", stable_workspace_hash(&workspace));
        let mut events = store.subscribe_workflow_catalog();
        let aggregate = self.catalog_events.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(mut event) => {
                        event.scope = scope.clone();
                        let _ = aggregate.send(event);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        stores.insert(workspace, store.clone());
        stores.insert(requested_workspace, store.clone());
        Ok(store)
    }

    /// Resolve one stable Project-home publication with an optional current
    /// workspace overlay.
    ///
    /// Precedence is builtin < global/user < Project home < workspace. The
    /// project id is used only for opaque diagnostics/cache identity; no
    /// filesystem path is exposed through catalog events.
    pub async fn skill_store_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
    ) -> SkillResult<std::sync::Arc<SkillStore>> {
        let project_home = tokio::fs::canonicalize(project_home).await?;
        let workspace = match workspace {
            Some(workspace) => Some(tokio::fs::canonicalize(workspace).await?),
            None => None,
        };
        let key = (
            project_id.to_string(),
            project_home.clone(),
            workspace.clone(),
        );
        if let Some(store) = self
            .project_workspace_stores
            .read()
            .await
            .get(&key)
            .cloned()
        {
            return Ok(store);
        }

        let mut stores = self.project_workspace_stores.write().await;
        if let Some(store) = stores.get(&key).cloned() {
            return Ok(store);
        }
        if stores.len() >= MAX_CACHED_WORKSPACE_STORES {
            return Err(SkillError::Storage(format!(
                "cached Project/workspace store capacity ({MAX_CACHED_WORKSPACE_STORES}) reached"
            )));
        }

        let store = std::sync::Arc::new(SkillStore::new_with_resource_scope(
            SkillStoreConfig {
                skills_dir: self.config.skills_dir.clone(),
                project_dir: None,
                active_mode: self.config.active_mode.clone(),
            },
            self.retained_budget.clone(),
            self.snapshot_limits,
            Some(project_home),
            workspace.clone(),
        ));
        store.load().await?;
        store.start_live_reload();

        let scope = match workspace.as_deref() {
            Some(workspace) => format!(
                "project:{}:workspace:{:016x}",
                project_id,
                stable_workspace_hash(workspace)
            ),
            None => format!("project:{project_id}"),
        };
        let mut events = store.subscribe_workflow_catalog();
        let aggregate = self.catalog_events.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(mut event) => {
                        event.scope = scope.clone();
                        let _ = aggregate.send(event);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        stores.insert(key, store.clone());
        Ok(store)
    }

    pub(crate) async fn cached_workspace_stores(&self) -> Vec<std::sync::Arc<SkillStore>> {
        let mut unique = Vec::new();
        let workspace_stores = self.workspace_stores.read().await;
        let project_workspace_stores = self.project_workspace_stores.read().await;
        for store in workspace_stores
            .values()
            .chain(project_workspace_stores.values())
        {
            if !unique
                .iter()
                .any(|existing| std::sync::Arc::ptr_eq(existing, store))
            {
                unique.push(store.clone());
            }
        }
        unique
    }

    async fn cached_dependent_stores(&self) -> Vec<std::sync::Arc<SkillStore>> {
        let mut unique = HashMap::<u64, std::sync::Arc<SkillStore>>::new();
        for store in self.mode_stores.read().await.values() {
            unique
                .entry(store.store_token)
                .or_insert_with(|| store.clone());
        }
        for store in self.workspace_stores.read().await.values() {
            unique
                .entry(store.store_token)
                .or_insert_with(|| store.clone());
        }
        for store in self.project_workspace_stores.read().await.values() {
            unique
                .entry(store.store_token)
                .or_insert_with(|| store.clone());
        }
        unique.into_values().collect()
    }

    /// Publish a global Workflow source mutation synchronously to every cached
    /// catalog view. Workspace, Project/workspace, and mode stores own
    /// independent immutable snapshots; relying on their filesystem watchers
    /// would leave the mutating request with a stale same-session read and has
    /// a registration window in which the event can be lost entirely.
    pub async fn reload_global_workflow_views(&self) -> SkillResult<()> {
        self.reload().await?;
        let mut pending = self.cached_dependent_stores().await;
        let mut visited = HashSet::new();
        while let Some(store) = pending.pop() {
            if !visited.insert(store.store_token) {
                continue;
            }
            store.reload().await?;
            pending.extend(store.cached_dependent_stores().await);
        }
        Ok(())
    }

    /// Resolve an isolated catalog view for a specific session workspace without changing the
    /// server-wide snapshot or consulting the server process current directory.
    pub async fn workflow_catalog_for_workspace(
        &self,
        workspace: &Path,
    ) -> SkillResult<WorkflowCatalogSnapshot> {
        Ok(self
            .skill_store_for_workspace(workspace)
            .await?
            .workflow_catalog_snapshot()
            .await)
    }

    fn normalize_catalog_watch_root(&self, path: &Path) -> PathBuf {
        let mut cursor = path.to_path_buf();
        let mut missing = Vec::new();
        loop {
            if let Ok(mut canonical) = std::fs::canonicalize(&cursor) {
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                self.watcher_counters
                    .registration_canonicalizations
                    .fetch_add(1, Ordering::Relaxed);
                return canonical;
            }
            let Some(name) = cursor.file_name().map(|name| name.to_os_string()) else {
                return lexical_normalize_path(path);
            };
            missing.push(name);
            if !cursor.pop() {
                return lexical_normalize_path(path);
            }
            if cursor.as_os_str().is_empty() {
                cursor.push(".");
            }
        }
    }

    /// Catalog sources and structural containers, normalized once when the
    /// native watcher is registered. Event filtering remains purely lexical.
    fn catalog_watch_plan(&self) -> CatalogWatchPlan {
        let mut raw_roots: BTreeSet<PathBuf> = self
            .discovery_dirs_for_mode(None)
            .into_iter()
            .map(|source| source.dir)
            .collect();
        let plugins_root = Self::plugins_root_dir(&self.config.skills_dir);
        raw_roots.insert(plugins_root);
        if let Some(data_dir) = self.config.skills_dir.parent() {
            raw_roots.insert(data_dir.join("workflows"));
        }
        if let Some(workspace) = self.workspace_overlay_dir.as_ref() {
            raw_roots.insert(workspace.join(".bamboo").join("workflows"));
        }

        let mut raw_containers = BTreeSet::new();
        if let Some(workspace) = self.workspace_overlay_dir.as_ref() {
            raw_containers.insert(workspace.join(".bamboo"));
        }
        if self.config.skills_dir == bamboo_config::paths::bamboo_dir().join("skills") {
            if let Some(agents_skills) = bamboo_config::paths::agents_skills_dir() {
                if let Some(agents_root) = agents_skills.parent() {
                    raw_containers.insert(agents_root.to_path_buf());
                }
            }
        }

        let catalog_roots: Vec<_> = raw_roots
            .into_iter()
            .map(|path| self.normalize_catalog_watch_root(&path))
            .collect();
        let mut container_paths: BTreeSet<_> = raw_containers
            .into_iter()
            .map(|path| self.normalize_catalog_watch_root(&path))
            .collect();
        // If more than the catalog leaf is missing (for example an as-yet
        // uncreated project home), retain each missing intermediate directory
        // as an exact structural trigger. This advances the shallow watch one
        // level at a time without treating unrelated siblings as catalog churn.
        for root in &catalog_roots {
            let mut parent = root.parent();
            while let Some(path) = parent {
                if path.is_dir() {
                    break;
                }
                container_paths.insert(path.to_path_buf());
                parent = path.parent();
            }
        }

        CatalogWatchPlan {
            catalog_roots,
            container_paths: container_paths.into_iter().collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn watcher_activity(&self) -> SkillWatcherActivity {
        self.watcher_counters.snapshot()
    }

    #[cfg(test)]
    fn subscribe_processed_watcher_batches(
        &self,
    ) -> tokio::sync::broadcast::Receiver<ProcessedWatcherBatch> {
        self.processed_watcher_batches.subscribe()
    }

    /// Start an OS-backed catalog watcher. Existing catalog directories alone
    /// are recursive; their nearest existing parents are shallow anchors that
    /// let missing roots be created and dynamically rebound. Raw events enter a
    /// bounded queue, coalesce to a trailing-edge batch, and only then undergo
    /// lexical filtering. Queue overflow marks the batch dirty and forces a
    /// conservative full reload rather than losing a relevant change.
    pub fn start_live_reload(self: &std::sync::Arc<Self>) {
        if self.watcher_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = std::sync::Arc::downgrade(self);
        let plan = self.catalog_watch_plan();
        let counters = self.watcher_counters.clone();
        let overflow_dirty = Arc::new(AtomicBool::new(false));
        let callback_overflow = overflow_dirty.clone();
        let callback_counters = counters.clone();
        #[cfg(test)]
        let processed_watcher_batches = self.processed_watcher_batches.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(
            SKILL_WATCH_CHANNEL_CAPACITY,
        );
        let mut watcher =
            match notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                callback_counters
                    .received_events
                    .fetch_add(1, Ordering::Relaxed);
                if event
                    .as_ref()
                    .is_ok_and(|event| !watcher_event_can_change_catalog(event))
                {
                    callback_counters
                        .rejected_events
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                match sender.try_send(event) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        callback_counters
                            .overflowed_events
                            .fetch_add(1, Ordering::Relaxed);
                        callback_overflow.store(true, Ordering::Release);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                }
            }) {
                Ok(watcher) => watcher,
                Err(error) => {
                    tracing::warn!("Failed to start skill catalog watcher: {error}");
                    self.watcher_started.store(false, Ordering::SeqCst);
                    return;
                }
            };
        let mut registered = HashMap::new();
        sync_catalog_watch_registrations(&mut watcher, &plan, &mut registered, counters.as_ref());

        tokio::spawn(async move {
            // Keep the native watcher alive for exactly as long as the receiver loop.
            let mut watcher = watcher;
            while let Some(first) = receiver.recv().await {
                let mut batch = vec![first];
                let quiet = tokio::time::sleep(SKILL_WATCH_QUIET_PERIOD);
                let maximum = tokio::time::sleep(SKILL_WATCH_MAX_BATCH);
                tokio::pin!(quiet);
                tokio::pin!(maximum);
                loop {
                    tokio::select! {
                        maybe = receiver.recv() => match maybe {
                            Some(event) => {
                                batch.push(event);
                                quiet.as_mut().reset(
                                    tokio::time::Instant::now() + SKILL_WATCH_QUIET_PERIOD,
                                );
                            }
                            None => break,
                        },
                        _ = &mut quiet => break,
                        _ = &mut maximum => break,
                    }
                }

                counters
                    .coalesced_events
                    .fetch_add(batch.len().saturating_sub(1) as u64, Ordering::Relaxed);
                let overflowed = overflow_dirty.swap(false, Ordering::AcqRel);
                let mut relevant = overflowed;
                let mut rejected = 0u64;
                #[cfg(test)]
                let mut observed_paths = Vec::new();
                for event in batch {
                    match event {
                        Ok(event) => {
                            let event_relevant = event
                                .paths
                                .iter()
                                .any(|path| plan.is_relevant(&lexical_normalize_path(path)));
                            #[cfg(test)]
                            observed_paths.extend(
                                event.paths.iter().map(|path| lexical_normalize_path(path)),
                            );
                            relevant |= event_relevant;
                            rejected += u64::from(!event_relevant);
                        }
                        Err(error) => {
                            // A backend error can mean the native watcher lost
                            // events or a registration. Reconcile registrations
                            // and publish one conservative full snapshot.
                            relevant = true;
                            tracing::warn!("Skill catalog watcher error: {error}");
                        }
                    }
                }
                #[cfg(test)]
                let processed_batch = ProcessedWatcherBatch {
                    paths: observed_paths,
                    relevant,
                };
                counters
                    .rejected_events
                    .fetch_add(rejected, Ordering::Relaxed);

                let Some(store) = weak.upgrade() else { break };
                if relevant {
                    sync_catalog_watch_registrations(
                        &mut watcher,
                        &plan,
                        &mut registered,
                        counters.as_ref(),
                    );
                    match store.reload().await {
                        Ok(_) => {
                            counters.reloads.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            counters.reload_failures.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!("Live skill catalog reload failed: {error}");
                        }
                    }
                }
                #[cfg(test)]
                let _ = processed_watcher_batches.send(processed_batch);
                let activity = counters.snapshot();
                tracing::debug!(
                    store_token = store.store_token,
                    watcher_received = activity.received_events,
                    watcher_rejected = activity.rejected_events,
                    watcher_coalesced = activity.coalesced_events,
                    watcher_overflowed = activity.overflowed_events,
                    watcher_reloads = activity.reloads,
                    watcher_reload_failures = activity.reload_failures,
                    watcher_registration_canonicalizations =
                        activity.registration_canonicalizations,
                    watcher_event_canonicalizations = 0,
                    watcher_rebinds = activity.watch_rebinds,
                    "skill watcher batch processed"
                );
            }
        });
    }

    /// Create built-in skills on disk.
    ///
    /// Generates default skills that ship with Bamboo (e.g., skill-creator).
    /// For each built-in skill, this method:
    /// 1. Loads built-in skill bundles from compile-time embedded files.
    /// 2. Writes the skill definition to disk (overwriting previous built-in content).
    /// 3. Writes bundled files (scripts/references/assets/agents/etc.) under each skill dir.
    /// 4. Sets executable permissions on Unix systems.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns `SkillError` if file operations fail.
    async fn create_builtin_skills(&self) -> SkillResult<()> {
        let builtin_skills_dir = Self::builtin_skills_dir(&self.config.skills_dir);
        ensure_skills_dir(&builtin_skills_dir).await?;
        for bundle in load_builtin_skill_bundles()? {
            if archive_exact_legacy_materialization(&self.config.skills_dir, &bundle).await? {
                info!(
                    "Archived exact legacy builtin materialization '{}' before using versioned storage",
                    bundle.skill.id
                );
            }
            let skill_id = bundle.skill.id.clone();
            write_skill_file(&builtin_skills_dir, &bundle.skill).await?;

            for (relative_path, file) in bundle.files {
                let full_path = builtin_skills_dir.join(&skill_id).join(&relative_path);
                if let Some(parent) = full_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&full_path, file.bytes).await?;
                // Reproduce the embedded Git permission contract exactly.
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = if file.executable { 0o755 } else { 0o644 };
                    tokio::fs::set_permissions(&full_path, std::fs::Permissions::from_mode(mode))
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Reload skills from disk into the in-memory cache.
    ///
    /// This is useful when skills have been modified on disk and you want
    /// to pick up the changes without restarting the application.
    ///
    /// # Returns
    ///
    /// The number of skills loaded.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After editing a skill file externally
    /// let count = store.reload().await?;
    /// println!("Loaded {} skills", count);
    /// ```
    pub async fn reload(&self) -> SkillResult<usize> {
        info!("Reloading skills from disk...");
        let _reload_guard = self.reload_lock.lock().await;
        let workflows_dir = self
            .config
            .skills_dir
            .parent()
            .map(|parent| parent.join("workflows"))
            .unwrap_or_else(|| PathBuf::from("workflows"));
        for diagnostic in
            crate::legacy::migrate_legacy_yaml_workflows(&workflows_dir, &self.config.skills_dir)
                .await
        {
            if !diagnostic.can_map_to_bundle {
                tracing::warn!("Legacy YAML migration: {}", diagnostic.message);
            }
        }
        self.load_locked().await
    }

    /// Remove a legacy workflow source and, when present, only the historical
    /// adapter bundle whose metadata confirms ownership by that source.
    /// Same-id ordinary or explicitly migrated Skills are never removed.
    pub async fn remove_legacy_workflow(&self, source: &Path, id: &str) -> SkillResult<bool> {
        let _reload_guard = self.reload_lock.lock().await;
        let removed_bundle =
            crate::legacy::remove_legacy_markdown_bundle(source, &self.config.skills_dir, id)
                .await?;
        if let Err(error) = tokio::fs::remove_file(source).await {
            // Restore the owned bundle when the authoritative source could not
            // be removed, so callers never observe a silent partial deletion.
            if removed_bundle {
                if let Ok(body) = tokio::fs::read_to_string(source).await {
                    let _ = crate::legacy::sync_legacy_markdown_bundle(
                        source,
                        &self.config.skills_dir,
                        id,
                        &body,
                    )
                    .await;
                }
            }
            return Err(error.into());
        }
        self.load_locked().await?;
        Ok(true)
    }

    /// List all skills with optional filtering.
    ///
    /// Returns a sorted list of skills matching the specified filter criteria.
    /// Optionally refreshes the cache from disk before listing.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional filter criteria.
    /// * `refresh` - If true, reload skills from disk before listing.
    ///
    /// # Returns
    ///
    /// A vector of matching skills, sorted alphabetically by name.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // List skills matching a search query, refreshing from disk
    /// let filter = SkillFilter::new().with_search("dashboard");
    /// let skills = store.list_skills(Some(filter), true).await;
    /// ```
    pub async fn list_skills(
        &self,
        filter: Option<SkillFilter>,
        refresh: bool,
    ) -> Vec<SkillDefinition> {
        self.skills_and_catalog_snapshot(filter, refresh).await.0
    }

    /// Clone the visible Skills and their activation policy catalog while
    /// holding one publication read lock. API callers can therefore never
    /// combine generation N definitions with generation N+1 identity data.
    pub async fn skills_and_catalog_snapshot(
        &self,
        filter: Option<SkillFilter>,
        refresh: bool,
    ) -> (Vec<SkillDefinition>, WorkflowCatalogSnapshot) {
        // Optionally reload from disk to pick up new/updated skills
        if refresh {
            if let Err(e) = self.reload().await {
                tracing::warn!("Failed to reload skills: {}", e);
            }
        }

        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let skills = self.skills.read().await;
        let catalog = self.skill_catalog.read().await.clone();

        let mut result: Vec<SkillDefinition> = skills
            .values()
            .filter(|skill| match &filter {
                Some(active_filter) => active_filter.matches(skill),
                None => true,
            })
            .cloned()
            .collect();

        result.sort_by_key(|s| s.name.clone());
        (result, catalog)
    }

    /// List skills with an optional mode override (without mutating in-memory cache).
    pub async fn list_skills_for_mode(
        &self,
        filter: Option<SkillFilter>,
        mode_override: Option<&str>,
    ) -> Vec<SkillDefinition> {
        let (skills, _) = match self.resolve_skills_maps_for_mode(mode_override).await {
            Ok(maps) => maps,
            Err(error) => {
                tracing::warn!(
                    "Failed to resolve skills for mode {:?}: {}",
                    mode_override,
                    error
                );
                return Vec::new();
            }
        };

        let mut result: Vec<SkillDefinition> = skills
            .values()
            .filter(|skill| match &filter {
                Some(active_filter) => active_filter.matches(skill),
                None => true,
            })
            .cloned()
            .collect();
        result.sort_by_key(|s| s.name.clone());
        result
    }

    /// Get a single skill by its ID.
    ///
    /// Retrieves a skill from the in-memory cache by its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `id` - The skill ID (e.g., "skill-creator").
    ///
    /// # Returns
    ///
    /// The matching `SkillDefinition` if found.
    ///
    /// # Errors
    ///
    /// Returns `SkillError::NotFound` if no skill matches the given ID.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let skill = store.get_skill("my-skill").await?;
    /// println!("Description: {}", skill.description);
    /// ```
    pub async fn get_skill(&self, id: &str) -> SkillResult<SkillDefinition> {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let skills = self.skills.read().await;
        skills
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Get a skill by id with an optional mode override.
    pub async fn get_skill_for_mode(
        &self,
        id: &str,
        mode_override: Option<&str>,
    ) -> SkillResult<SkillDefinition> {
        if mode_override.is_none() {
            return self.get_skill(id).await;
        }

        let (skills, _) = self.resolve_skills_maps_for_mode(mode_override).await?;
        skills
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Resolve the definition and resource root from one snapshot generation.
    pub async fn get_skill_with_root_for_mode(
        &self,
        id: &str,
        mode_override: Option<&str>,
    ) -> SkillResult<(SkillDefinition, PathBuf)> {
        if mode_override.is_some() {
            let (skills, roots) = self.resolve_skills_maps_for_mode(mode_override).await?;
            let skill = skills
                .get(id)
                .cloned()
                .ok_or_else(|| SkillError::NotFound(id.to_string()))?;
            let root = roots
                .get(id)
                .cloned()
                .ok_or_else(|| SkillError::NotFound(id.to_string()))?;
            return Ok((skill, root));
        }

        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let skill = self
            .skills
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))?;
        let root = self
            .skill_roots
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))?;
        Ok((skill, root))
    }

    /// Get the root directory path for a loaded skill.
    pub async fn get_skill_root(&self, id: &str) -> SkillResult<PathBuf> {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let roots = self.skill_roots.read().await;
        roots
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Get the immutable publication root for a Workflow identity. Legacy
    /// markdown adapters use the source file itself as this root.
    pub async fn get_workflow_root(&self, id: &str) -> SkillResult<PathBuf> {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        self.workflow_roots
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Resolve only a source-backed legacy Workflow command. Historical
    /// auto-imports and orchestration definitions intentionally have no
    /// command-palette content endpoint.
    pub async fn get_legacy_workflow_source(&self, id: &str) -> SkillResult<PathBuf> {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let catalog = self.catalog.read().await;
        let selectable = catalog.entries.iter().any(|entry| {
            entry.id == id
                && entry.winner
                && entry.kind == WorkflowKind::Instruction
                && entry.status == WorkflowStatus::Valid
                && entry.migration_status == Some(LegacyWorkflowMigrationStatus::Available)
        });
        if !selectable {
            return Err(SkillError::NotFound(id.to_string()));
        }
        self.workflow_roots
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Get the root directory path for a loaded skill with an optional mode override.
    pub async fn get_skill_root_for_mode(
        &self,
        id: &str,
        mode_override: Option<&str>,
    ) -> SkillResult<PathBuf> {
        if mode_override.is_none() {
            return self.get_skill_root(id).await;
        }

        let (_, roots) = self.resolve_skills_maps_for_mode(mode_override).await?;
        roots
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Create a new skill (not supported - read-only mode).
    ///
    /// Skills must be created by writing Markdown files directly to the
    /// skills directory. This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn create_skill(&self, _skill: SkillDefinition) -> SkillResult<SkillDefinition> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Update an existing skill (not supported - read-only mode).
    ///
    /// Skills must be edited by modifying Markdown files directly.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn update_skill(
        &self,
        _id: &str,
        _updates: SkillUpdate,
    ) -> SkillResult<SkillDefinition> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Delete a skill (not supported - read-only mode).
    ///
    /// Skills must be deleted by removing their Markdown files directly.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn delete_skill(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Enable a skill globally (not supported - read-only mode).
    ///
    /// Skill enablement is controlled outside this read-only store.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn enable_skill_global(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Disable a skill globally (not supported - read-only mode).
    ///
    /// Skill enablement is controlled outside this read-only store.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn disable_skill_global(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Enable a skill for a specific chat (not supported - read-only mode).
    ///
    /// Skill chat associations are managed externally, not through this API.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn enable_skill_for_chat(&self, _skill_id: &str, _chat_id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Disable a skill for a specific chat (not supported - read-only mode).
    ///
    /// Skill chat associations are managed externally, not through this API.
    /// This method always returns an error.
    ///
    /// # Errors
    ///
    /// Always returns `SkillError::ReadOnly`.
    pub async fn disable_skill_for_chat(&self, _skill_id: &str, _chat_id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Get all skills from the cache.
    ///
    /// Returns all loaded skills, sorted alphabetically by name.
    /// This is a convenience method equivalent to `list_skills(None, false)`.
    ///
    /// # Returns
    ///
    /// A vector of all skills in the store.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let skills = store.get_all_skills().await;
    /// println!("Total skills: {}", skills.len());
    /// ```
    pub async fn get_all_skills(&self) -> Vec<SkillDefinition> {
        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let mut skills: Vec<SkillDefinition> = self.skills.read().await.values().cloned().collect();
        skills.sort_by_key(|s| s.name.clone());
        skills
    }

    /// Get the path to the skills directory.
    ///
    /// Returns the configured directory where skill Markdown files are stored.
    ///
    /// # Returns
    ///
    /// Reference to the skills directory path.
    pub fn skills_dir(&self) -> &PathBuf {
        &self.config.skills_dir
    }

    /// Export skills to Markdown format.
    ///
    /// Renders one or more skills as Markdown documents with YAML frontmatter.
    /// Useful for creating backups or sharing skills.
    ///
    /// # Arguments
    ///
    /// * `skill_ids` - Optional list of skill IDs to export.
    ///   If `None`, exports all skills.
    ///
    /// # Returns
    ///
    /// A Markdown string containing all exported skills, separated by blank lines.
    ///
    /// # Errors
    ///
    /// Returns `SkillError` if Markdown rendering fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Export specific skills
    /// let markdown = store.export_to_markdown(
    ///     Some(vec!["skill-creator".to_string()])
    /// ).await?;
    /// println!("{}", markdown);
    ///
    /// // Export all skills
    /// let all_markdown = store.export_to_markdown(None).await?;
    /// ```
    pub async fn export_to_markdown(&self, skill_ids: Option<Vec<String>>) -> SkillResult<String> {
        let skills = self.skills.read().await;

        let selected_skills: Vec<&SkillDefinition> = match skill_ids {
            Some(ids) => ids.iter().filter_map(|id| skills.get(id)).collect(),
            None => skills.values().collect(),
        };

        let mut chunks = Vec::new();
        for skill in selected_skills {
            chunks.push(render_skill_markdown(skill)?);
        }

        Ok(chunks.join("\n\n"))
    }
}

impl Drop for SkillStore {
    fn drop(&mut self) {
        self.retained_budget.release_publication(self.store_token);
    }
}

impl Default for SkillStore {
    fn default() -> Self {
        Self::new(SkillStoreConfig::default())
    }
}

/// Update fields for skill modification.
///
/// This struct is used to specify which fields of a skill should be updated.
/// All fields are optional - only provided fields will be changed.
///
/// Note: This is currently not used as skills are read-only, but is kept
/// for future API compatibility and documentation purposes.
///
/// # Example
///
/// ```ignore
/// let update = SkillUpdate::new()
///     .with_name("New Name")
///     .with_description("Updated description")
///     .with_tool_refs(vec!["read_file".to_string()]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct SkillUpdate {
    /// New name for the skill.
    pub name: Option<String>,

    /// New description for the skill.
    pub description: Option<String>,

    /// New prompt template for the skill.
    pub prompt: Option<String>,

    /// New list of tool references for the skill.
    pub tool_refs: Option<Vec<String>>,

    /// New license for the skill.
    pub license: Option<String>,

    /// New compatibility notes for the skill.
    pub compatibility: Option<String>,

    /// New metadata payload for the skill.
    pub metadata: Option<serde_json::Value>,
}

impl SkillUpdate {
    /// Create a new empty update struct.
    ///
    /// All fields will be `None`, indicating no changes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the name field.
    ///
    /// # Arguments
    ///
    /// * `name` - The new name for the skill.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the description field.
    ///
    /// # Arguments
    ///
    /// * `description` - The new description for the skill.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the prompt field.
    ///
    /// # Arguments
    ///
    /// * `prompt` - The new prompt template for the skill.
    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }

    /// Set the tool references field.
    ///
    /// # Arguments
    ///
    /// * `tool_refs` - The new list of tool references for the skill.
    pub fn with_tool_refs(mut self, tool_refs: Vec<String>) -> Self {
        self.tool_refs = Some(tool_refs);
        self
    }

    /// Set the license field.
    ///
    /// # Arguments
    ///
    /// * `license` - The new license string for the skill.
    pub fn with_license(mut self, license: impl Into<String>) -> Self {
        self.license = Some(license.into());
        self
    }

    /// Set the compatibility field.
    ///
    /// # Arguments
    ///
    /// * `compatibility` - The new compatibility notes for the skill.
    pub fn with_compatibility(mut self, compatibility: impl Into<String>) -> Self {
        self.compatibility = Some(compatibility.into());
        self
    }

    /// Set the metadata field.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The new metadata payload for the skill.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::fs;

    use super::{
        lexical_normalize_path, ProcessedWatcherBatch, RetainedResourceBudget, SkillSnapshotLimits,
        SkillStore,
    };
    use crate::store::builtin::{
        load_builtin_skill_bundles, BuiltinSkillBundle, WORKFLOW_BUILTINS,
    };
    use crate::store::storage::write_skill_file;
    use crate::store::storage::SkillDirectorySource;
    use crate::types::SkillStoreConfig;
    use crate::{SkillManager, WorkflowCatalogEventKind, WorkflowSource, WorkflowStatus};

    #[test]
    fn agents_skill_precedence_is_below_bamboo_global_and_above_plugin() {
        assert!(
            SkillStore::source_rank(SkillDirectorySource::Global)
                > SkillStore::source_rank(SkillDirectorySource::Agents)
        );
        assert!(
            SkillStore::source_rank(SkillDirectorySource::Agents)
                > SkillStore::source_rank(SkillDirectorySource::Plugin)
        );
    }

    async fn write_skill(
        skills_root: &Path,
        id: &str,
        description: &str,
        prompt: &str,
    ) -> std::io::Result<PathBuf> {
        let skill_dir = skills_root.join(id);
        fs::create_dir_all(&skill_dir).await?;
        let skill_file = skill_dir.join("SKILL.md");
        let content = format!(
            "---\nname: {id}\ndescription: {description}\n---\n{prompt}\n",
            id = id,
            description = description,
            prompt = prompt
        );
        fs::write(&skill_file, content).await?;
        Ok(skill_dir)
    }

    async fn wait_for_processed_watcher_path(
        batches: &mut tokio::sync::broadcast::Receiver<ProcessedWatcherBatch>,
        expected_path: &Path,
    ) -> ProcessedWatcherBatch {
        let mut existing_prefix = expected_path;
        let mut missing_suffix = Vec::new();
        while !existing_prefix.exists() {
            if let Some(name) = existing_prefix.file_name() {
                missing_suffix.push(name.to_os_string());
            }
            existing_prefix = existing_prefix
                .parent()
                .expect("a watcher test path should have an existing ancestor");
        }
        let mut expected_path = std::fs::canonicalize(existing_prefix)
            .unwrap_or_else(|_| lexical_normalize_path(existing_prefix));
        for component in missing_suffix.iter().rev() {
            expected_path.push(component);
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match batches.recv().await {
                    Ok(batch)
                        if batch.paths.iter().any(|path| {
                            path == &expected_path
                                || path.starts_with(&expected_path)
                                || expected_path.starts_with(path)
                        }) =>
                    {
                        return batch;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        panic!("skill watcher test observation lagged by {skipped} batches")
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("skill watcher stopped before processing the expected path")
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "skill watcher should process the filesystem event touching {}",
                expected_path.display()
            )
        })
    }

    #[test]
    fn watcher_event_filter_rejects_non_mutating_access() {
        use notify::event::{AccessKind, DataChange, MetadataKind, ModifyKind};
        use notify::{Event, EventKind};

        let access = Event::new(EventKind::Access(AccessKind::Read));
        let access_time = Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::AccessTime,
        )));
        let content = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Content)));

        assert!(!super::watcher_event_can_change_catalog(&access));
        assert!(!super::watcher_event_can_change_catalog(&access_time));
        assert!(super::watcher_event_can_change_catalog(&content));
        assert!(super::watcher_event_can_change_catalog(&Event::new(
            EventKind::Any
        )));
    }

    fn orchestration_yaml(id: &str, revision: u64) -> String {
        format!(
            "workflow_schema: 1\nid: {id}\nrevision: {revision}\ninput_schema:\n  type: object\n  properties:\n    path:\n      type: string\n  required: [path]\n  additionalProperties: false\nsteps:\n  - id: inspect\n    type: tool\n    tool: read_file\n    args:\n      path:\n        from: args\n        pointer: /path\n    capabilities: [read]\n    output_schema:\n      type: object\n      additionalProperties: true\nplan:\n  type: step\n  step: inspect\nbudgets:\n  max_concurrency: 1\n  max_agents: 0\n  max_steps: 4\n  max_retries: 1\n  max_nesting_depth: 2\n  wall_time_ms: 10000\n"
        )
    }

    async fn materialize_legacy_builtin(skills_dir: &Path, bundle: &BuiltinSkillBundle) {
        write_skill_file(skills_dir, &bundle.skill)
            .await
            .expect("legacy SKILL.md");
        for (relative, file) in &bundle.files {
            let path = skills_dir.join(&bundle.skill.id).join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await.expect("legacy parent");
            }
            fs::write(&path, &file.bytes)
                .await
                .expect("legacy resource");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(
                    path,
                    std::fs::Permissions::from_mode(if file.executable { 0o755 } else { 0o644 }),
                )
                .await
                .expect("legacy resource mode");
            }
        }
    }

    #[tokio::test]
    async fn load_markdown_skills() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        fs::create_dir_all(&skills_dir).await.expect("create dir");

        let content = r#"---
name: test-skill
description: A test skill
allowed-tools:
  - read_file
---
Use this skill for testing.
"#;

        let skill_dir = skills_dir.join("test-skill");
        fs::create_dir_all(&skill_dir)
            .await
            .expect("create skill dir");
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(&skill_file, content).await.expect("write");

        let config = SkillStoreConfig {
            skills_dir,
            ..Default::default()
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skills = store.list_skills(None, false).await;
        assert!(skills.iter().any(|skill| skill.id == "test-skill"));
        assert!(skills.iter().any(|skill| skill.id == "skill-creator"));
    }

    #[tokio::test]
    async fn create_builtin_skills_when_empty() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skills = store.list_skills(None, false).await;
        assert!(skills.iter().any(|skill| skill.id == "skill-creator"));
    }

    #[tokio::test]
    async fn skill_builtins_are_versioned_instruction_catalog_entries() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");

        let catalog = store.skill_catalog_snapshot().await;
        for (id, automatic) in WORKFLOW_BUILTINS {
            let entry = catalog
                .entries
                .iter()
                .find(|entry| entry.id == id)
                .unwrap_or_else(|| panic!("missing {id} catalog entry"));
            assert_eq!(entry.source, WorkflowSource::Builtin);
            assert_eq!(entry.kind, crate::WorkflowKind::Instruction);
            assert_eq!(entry.status, WorkflowStatus::Valid);
            assert!(entry.revision > 0);
            let expected_version = if id == "review" { "3" } else { "1" };
            assert_eq!(entry.version, expected_version);
            assert_eq!(entry.invocation_policy["explicit"], true);
            assert_eq!(entry.invocation_policy["automatic"], automatic);
            assert_eq!(entry.argument_schema["type"], "object");
        }
    }

    #[tokio::test]
    async fn exact_legacy_builtin_is_migrated_and_cannot_shadow_versioned_builtin() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let bundle = load_builtin_skill_bundles()
            .expect("builtin bundles")
            .into_iter()
            .find(|bundle| bundle.skill.id == "skill-creator")
            .expect("skill creator");
        materialize_legacy_builtin(&skills_dir, &bundle).await;

        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        assert!(
            !skills_dir.join("skill-creator").exists(),
            "only an exact legacy materialization may leave discovery"
        );
        let archive_parent = directory
            .path()
            .join("data/legacy-builtins-v1/skill-creator");
        let mut archives = fs::read_dir(&archive_parent)
            .await
            .expect("recoverable legacy archive");
        let archive = archives
            .next_entry()
            .await
            .expect("archive entry")
            .expect("one archived materialization")
            .path();
        assert!(archive.join("SKILL.md").exists());
        assert!(archives
            .next_entry()
            .await
            .expect("archive exhaustion")
            .is_none());

        let builtin_root = directory
            .path()
            .join("data/skills-builtin-v1/skill-creator");
        let mut upgraded = fs::read_to_string(builtin_root.join("SKILL.md"))
            .await
            .expect("versioned builtin");
        upgraded = upgraded.replacen(
            &format!("description: {}", bundle.skill.description),
            "description: upgraded embedded description",
            1,
        );
        fs::write(builtin_root.join("SKILL.md"), upgraded)
            .await
            .expect("simulate upgraded embedded bundle");
        store.reload().await.expect("reload upgraded builtin");

        assert_eq!(
            store
                .get_skill("skill-creator")
                .await
                .expect("builtin winner")
                .description,
            "upgraded embedded description"
        );
        assert_eq!(
            store
                .skill_catalog_snapshot()
                .await
                .entries
                .into_iter()
                .find(|entry| entry.id == "skill-creator")
                .expect("catalog entry")
                .source,
            WorkflowSource::Builtin
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn historical_all_scripts_executable_builtin_is_still_migrated() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let bundle = load_builtin_skill_bundles()
            .expect("builtin bundles")
            .into_iter()
            .find(|bundle| bundle.skill.id == "skill-creator")
            .expect("skill creator");
        materialize_legacy_builtin(&skills_dir, &bundle).await;
        for relative in bundle
            .files
            .keys()
            .filter(|relative| relative.starts_with("scripts/"))
        {
            fs::set_permissions(
                skills_dir.join("skill-creator").join(relative),
                std::fs::Permissions::from_mode(0o755),
            )
            .await
            .expect("historical executable mode");
        }

        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");

        assert!(
            !skills_dir.join("skill-creator").exists(),
            "known historical generator output must not shadow versioned builtins"
        );
        assert!(directory
            .path()
            .join("data/legacy-builtins-v1/skill-creator")
            .exists());
    }

    #[tokio::test]
    async fn modified_legacy_builtin_is_preserved_as_user_override() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let bundle = load_builtin_skill_bundles()
            .expect("builtin bundles")
            .into_iter()
            .find(|bundle| bundle.skill.id == "skill-creator")
            .expect("skill creator");
        materialize_legacy_builtin(&skills_dir, &bundle).await;
        let legacy_skill = skills_dir.join("skill-creator/SKILL.md");
        let mut customized = fs::read_to_string(&legacy_skill)
            .await
            .expect("legacy skill");
        customized.push_str("\nUser customization must survive.\n");
        fs::write(&legacy_skill, customized)
            .await
            .expect("customize legacy skill");

        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");

        assert!(legacy_skill.exists(), "customized user clone must remain");
        assert!(
            !directory
                .path()
                .join("data/legacy-builtins-v1/skill-creator")
                .exists(),
            "unproven user content must not be archived"
        );
        assert!(store
            .get_skill("skill-creator")
            .await
            .expect("user override")
            .prompt
            .contains("User customization must survive"));
        assert_eq!(
            store
                .skill_catalog_snapshot()
                .await
                .entries
                .into_iter()
                .find(|entry| entry.id == "skill-creator")
                .expect("catalog entry")
                .source,
            WorkflowSource::User
        );
        assert_eq!(
            store
                .get_skill_root("skill-creator")
                .await
                .expect("user root"),
            skills_dir.join("skill-creator")
        );
    }

    #[tokio::test]
    async fn project_skill_overrides_global_skill() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let workspace_dir = directory.path().join("workspace");
        let global_skills_dir = data_dir.join("skills");
        let project_skills_dir = workspace_dir.join(".bamboo").join("skills");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        fs::create_dir_all(&project_skills_dir)
            .await
            .expect("create project skills dir");

        write_skill(
            &global_skills_dir,
            "override-skill",
            "global version",
            "Global prompt",
        )
        .await
        .expect("write global skill");
        let project_skill_root = write_skill(
            &project_skills_dir,
            "override-skill",
            "project version",
            "Project prompt",
        )
        .await
        .expect("write project skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: Some(workspace_dir),
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("override-skill")
            .await
            .expect("override skill must exist");
        assert_eq!(skill.description, "project version");

        let resolved_root = store
            .get_skill_root("override-skill")
            .await
            .expect("skill root");
        let resolved_root = fs::canonicalize(resolved_root)
            .await
            .expect("canonical resolved root");
        let expected_root = fs::canonicalize(project_skill_root)
            .await
            .expect("canonical expected root");
        assert_eq!(resolved_root, expected_root);
        let catalog = store.skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "override-skill")
            .expect("catalog entry");
        assert_eq!(entry.source, WorkflowSource::Workspace);
        assert_eq!(entry.shadowed_candidates.len(), 1);
        assert_eq!(entry.shadowed_candidates[0].source, WorkflowSource::User);
    }

    #[tokio::test]
    async fn mode_specific_skill_overrides_generic_for_same_source() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let global_mode_skills_dir = data_dir.join("skills-code");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        fs::create_dir_all(&global_mode_skills_dir)
            .await
            .expect("create global mode skills dir");

        write_skill(
            &global_skills_dir,
            "mode-target-skill",
            "generic version",
            "Generic prompt",
        )
        .await
        .expect("write generic skill");
        write_skill(
            &global_mode_skills_dir,
            "mode-target-skill",
            "mode version",
            "Mode prompt",
        )
        .await
        .expect("write mode skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: Some("code".to_string()),
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("mode-target-skill")
            .await
            .expect("mode-target-skill must exist");
        assert_eq!(skill.description, "mode version");
    }

    #[tokio::test]
    async fn mode_specific_skill_is_ignored_without_active_mode() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let global_mode_skills_dir = data_dir.join("skills-code");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        fs::create_dir_all(&global_mode_skills_dir)
            .await
            .expect("create global mode skills dir");

        write_skill(
            &global_skills_dir,
            "mode-target-skill",
            "generic version",
            "Generic prompt",
        )
        .await
        .expect("write generic skill");
        write_skill(
            &global_mode_skills_dir,
            "mode-target-skill",
            "mode version",
            "Mode prompt",
        )
        .await
        .expect("write mode skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("mode-target-skill")
            .await
            .expect("mode-target-skill must exist");
        assert_eq!(skill.description, "generic version");
    }

    #[tokio::test]
    async fn plugin_skill_is_discovered_in_place() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let plugin_skills_dir = data_dir.join("plugins").join("hello-plugin").join("skills");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        let plugin_skill_root = write_skill(
            &plugin_skills_dir,
            "hello-world",
            "plugin skill",
            "Say hello from the plugin.",
        )
        .await
        .expect("write plugin skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("hello-world")
            .await
            .expect("plugin skill must be discovered in place");
        assert_eq!(skill.description, "plugin skill");

        // "In place": the resolved root is the plugin's own directory, not a
        // copy elsewhere.
        let resolved_root = store
            .get_skill_root("hello-world")
            .await
            .expect("skill root");
        let resolved_root = fs::canonicalize(resolved_root)
            .await
            .expect("canonical resolved root");
        let expected_root = fs::canonicalize(&plugin_skill_root)
            .await
            .expect("canonical expected root");
        assert_eq!(resolved_root, expected_root);
    }

    #[tokio::test]
    async fn global_skill_overrides_plugin_skill_with_same_id() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let plugin_skills_dir = data_dir.join("plugins").join("hello-plugin").join("skills");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        fs::create_dir_all(&plugin_skills_dir)
            .await
            .expect("create plugin skills dir");

        write_skill(
            &global_skills_dir,
            "shared-skill",
            "global version",
            "Global prompt",
        )
        .await
        .expect("write global skill");
        write_skill(
            &plugin_skills_dir,
            "shared-skill",
            "plugin version",
            "Plugin prompt",
        )
        .await
        .expect("write plugin skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("shared-skill")
            .await
            .expect("shared-skill must exist");
        assert_eq!(
            skill.description, "global version",
            "a global skill must win over a plugin skill sharing its id"
        );
    }

    #[tokio::test]
    async fn two_plugins_same_skill_id_resolve_deterministically_by_plugin_id() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        // Two plugins ship the SAME skill id. Discovery sorts plugin dirs by
        // path, so "alpha-plugin" (sorts first) must deterministically win over
        // "beta-plugin" regardless of read_dir order.
        let alpha_skills = data_dir.join("plugins").join("alpha-plugin").join("skills");
        let beta_skills = data_dir.join("plugins").join("beta-plugin").join("skills");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        write_skill(&alpha_skills, "shared-id", "alpha version", "Alpha prompt")
            .await
            .expect("write alpha skill");
        write_skill(&beta_skills, "shared-id", "beta version", "Beta prompt")
            .await
            .expect("write beta skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skill = store
            .get_skill("shared-id")
            .await
            .expect("shared-id must exist");
        assert_eq!(
            skill.description, "alpha version",
            "lowest-sorting plugin id must deterministically win a same-id collision"
        );
        let catalog = store.skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "shared-id")
            .expect("catalog entry");
        assert_eq!(entry.source, WorkflowSource::Plugin);
        assert_eq!(entry.shadowed_candidates.len(), 1);
        assert_eq!(entry.shadowed_candidates[0].source, WorkflowSource::Plugin);
    }

    #[tokio::test]
    async fn reload_picks_up_a_newly_installed_plugin_skill() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir.clone(),
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        assert!(store.get_skill("late-plugin-skill").await.is_err());

        // Simulate a plugin being installed after the store was initialized.
        let plugin_skills_dir = data_dir.join("plugins").join("late-plugin").join("skills");
        fs::create_dir_all(&plugin_skills_dir)
            .await
            .expect("create plugin skills dir");
        write_skill(
            &plugin_skills_dir,
            "late-plugin-skill",
            "installed later",
            "Hi from a freshly installed plugin.",
        )
        .await
        .expect("write plugin skill");

        let skills = store.list_skills(None, true).await;
        assert!(
            skills.iter().any(|skill| skill.id == "late-plugin-skill"),
            "list_skills(refresh=true) must pick up a plugin installed after initialize()"
        );
    }

    #[tokio::test]
    async fn get_skill_for_mode_overrides_cached_generic_selection() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let global_mode_skills_dir = data_dir.join("skills-code");

        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("create global skills dir");
        fs::create_dir_all(&global_mode_skills_dir)
            .await
            .expect("create global mode skills dir");

        write_skill(
            &global_skills_dir,
            "mode-target-skill",
            "generic version",
            "Generic prompt",
        )
        .await
        .expect("write generic skill");
        write_skill(
            &global_mode_skills_dir,
            "mode-target-skill",
            "mode version",
            "Mode prompt",
        )
        .await
        .expect("write mode skill");

        let config = SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        // Cached default view stays generic because no active_mode is configured.
        let generic = store
            .get_skill("mode-target-skill")
            .await
            .expect("generic skill exists");
        assert_eq!(generic.description, "generic version");

        // Per-call mode override should resolve the mode-specific variant.
        let mode_specific = store
            .get_skill_for_mode("mode-target-skill", Some("code"))
            .await
            .expect("mode-specific skill exists");
        assert_eq!(mode_specific.description, "mode version");
    }

    #[tokio::test]
    async fn mode_orchestration_stays_out_of_skills_and_retains_workflow_lkg() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let global_skills_dir = data_dir.join("skills");
        let mode_skills_dir = data_dir.join("skills-code");
        fs::create_dir_all(&global_skills_dir)
            .await
            .expect("global skills");
        let mode_root = write_skill(
            &mode_skills_dir,
            "mode-catalog",
            "mode v1",
            "Mode prompt v1",
        )
        .await
        .expect("mode skill");
        fs::write(
            mode_root.join("workflow.yaml"),
            "id: mode-catalog\nname: Mode catalog\ndescription: Mode workflow\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("valid workflow metadata");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: global_skills_dir,
            project_dir: None,
            active_mode: None,
        });
        store.initialize().await.expect("initialize");
        assert!(store
            .get_skill_for_mode("mode-catalog", Some("code"))
            .await
            .is_err());
        let mode_store = store
            .skill_store_for_mode(Some("code"))
            .await
            .expect("mode store")
            .expect("non-default mode");
        assert!(mode_store
            .skill_catalog_snapshot()
            .await
            .entries
            .iter()
            .all(|entry| entry.id != "mode-catalog"));
        assert_eq!(
            mode_store
                .workflow_definitions
                .read()
                .await
                .get("mode-catalog")
                .expect("initial workflow")
                .prompt,
            "Mode prompt v1"
        );

        fs::write(
            mode_root.join("SKILL.md"),
            "---\nname: mode-catalog\ndescription: mode v2\n---\nMode prompt v2\n",
        )
        .await
        .expect("updated instructions");
        fs::write(mode_root.join("workflow.yaml"), "version: 2\n")
            .await
            .expect("invalid workflow metadata");
        mode_store
            .reload()
            .await
            .expect("invalid reload is isolated");
        assert_eq!(
            mode_store
                .workflow_definitions
                .read()
                .await
                .get("mode-catalog")
                .expect("workflow LKG")
                .prompt,
            "Mode prompt v1",
            "invalid mode metadata must not activate new workflow instructions"
        );

        fs::write(
            mode_root.join("workflow.yaml"),
            "id: mode-catalog\nname: Mode catalog\ndescription: Mode workflow\nversion: '2'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("recovered metadata");
        mode_store.reload().await.expect("recovered workflow");
        assert_eq!(
            mode_store
                .workflow_definitions
                .read()
                .await
                .get("mode-catalog")
                .expect("recovered workflow")
                .prompt,
            "Mode prompt v2"
        );
    }

    #[tokio::test]
    async fn invalid_skill_reload_retains_lkg_and_publishes_sanitized_lifecycle_events() {
        const PRIVATE_FIELD: &str = "private-lkg-frontmatter-field";
        const PRIVATE_INSTRUCTIONS: &str = "Private LKG replacement instructions";
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = write_skill(&skills_dir, "steady", "original", "Original prompt")
            .await
            .expect("skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let mut events = store.subscribe_workflow_catalog();

        fs::write(
            root.join("SKILL.md"),
            format!(
                "---\nname: steady\ndescription: changed too early\n{PRIVATE_FIELD}: secret\n---\n{PRIVATE_INSTRUCTIONS}\n"
            ),
        )
            .await
            .expect("break skill");
        store.reload().await.expect("invalid reload is isolated");
        let skill = store.get_skill("steady").await.expect("LKG retained");
        assert_eq!(skill.description, "original");
        let invalid = store
            .skill_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "steady")
            .expect("invalid entry");
        assert_eq!(invalid.status, WorkflowStatus::Invalid);
        let public_error = invalid.last_error.as_deref().expect("public error");
        assert!(public_error.starts_with("SKILL.md:"));
        assert!(!public_error.contains(PRIVATE_FIELD));
        assert!(!public_error.contains(PRIVATE_INSTRUCTIONS));
        assert!(!public_error.contains(root.to_string_lossy().as_ref()));
        let invalid_event = events.try_recv().expect("instruction invalid event");
        assert_eq!(invalid_event.workflow_id, "steady");
        assert_eq!(invalid_event.kind, WorkflowCatalogEventKind::Invalid);
        assert!(!invalid_event.public_workflow);
        assert_eq!(invalid_event.scope, "global");

        write_skill(
            root.parent().expect("skills root"),
            "steady",
            "recovered",
            "Recovered prompt",
        )
        .await
        .expect("repair skill");
        store.reload().await.expect("recovered reload");
        assert_eq!(
            store
                .get_skill("steady")
                .await
                .expect("recovered skill")
                .description,
            "recovered"
        );
        let recovered_event = events.try_recv().expect("instruction recovered event");
        assert_eq!(recovered_event.workflow_id, "steady");
        assert_eq!(recovered_event.kind, WorkflowCatalogEventKind::Recovered);
        assert!(!recovered_event.public_workflow);
        assert_eq!(recovered_event.scope, "global");
    }

    #[tokio::test]
    async fn invalid_workflow_yaml_retains_orchestration_lkg_metadata() {
        const PRIVATE_RESOURCE: &str = "/private/resources/lkg-reference.md";
        const PRIVATE_INSTRUCTIONS: &str = "NEW BODY MUST NOT ACTIVATE";
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = write_skill(&skills_dir, "orchestrate", "orchestrates", "Instructions")
            .await
            .expect("skill");
        fs::write(
            root.join("workflow.yaml"),
            "id: orchestrate\nname: Orchestrate\ndescription: Runs tools\nversion: '2'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("workflow yaml");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let original = store
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "orchestrate")
            .expect("orchestration entry");
        assert_eq!(original.kind, crate::WorkflowKind::Orchestration);
        assert_eq!(original.version, "2");

        fs::write(
            root.join("SKILL.md"),
            format!(
                "---\nname: orchestrate\ndescription: changed too early\n---\n{PRIVATE_INSTRUCTIONS}\n"
            ),
        )
        .await
        .expect("change instructions");
        fs::write(
            root.join("workflow.yaml"),
            format!(
                "id: orchestrate\nname: Orchestrate\ndescription: Runs tools\nversion: '3'\ncomposition:\n  type: {PRIVATE_RESOURCE}\n"
            ),
        )
            .await
            .expect("break workflow yaml");
        store.reload().await.expect("isolated invalid metadata");
        let invalid = store
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "orchestrate")
            .expect("invalid entry");
        assert_eq!(invalid.status, WorkflowStatus::Invalid);
        assert_eq!(invalid.kind, crate::WorkflowKind::Orchestration);
        assert_eq!(invalid.version, "2");
        assert!(store.get_skill("orchestrate").await.is_err());
        let active = store
            .workflow_definitions
            .read()
            .await
            .get("orchestrate")
            .cloned()
            .expect("workflow LKG active");
        assert_eq!(active.description, "orchestrates");
        assert_eq!(active.prompt, "Instructions");
        let public_error = invalid.last_error.as_deref().expect("public error");
        assert!(public_error.starts_with("workflow.yaml:"));
        assert!(!public_error.contains(PRIVATE_RESOURCE));
        assert!(!public_error.contains(PRIVATE_INSTRUCTIONS));
        assert!(!public_error.contains(root.to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn new_orchestration_definition_is_compiled_and_pinned_from_one_publication() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = write_skill(&skills_dir, "review-flow", "review", "Instructions")
            .await
            .expect("skill");
        fs::write(
            root.join("workflow.yaml"),
            orchestration_yaml("review-flow", 42),
        )
        .await
        .expect("workflow yaml");
        let unrelated = write_skill(&skills_dir, "unrelated-flow", "other", "Unrelated")
            .await
            .expect("unrelated skill");
        fs::write(
            unrelated.join("workflow.yaml"),
            orchestration_yaml("unrelated-flow", 9),
        )
        .await
        .expect("unrelated workflow yaml");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");

        let catalog = store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review-flow")
            .expect("catalog entry");
        assert_eq!(entry.kind, crate::WorkflowKind::Orchestration);
        assert_eq!(entry.revision, 42);
        assert_eq!(
            entry.argument_schema["properties"]["path"]["type"],
            "string"
        );

        let bundle = store
            .pin_workflow_definition_bundle("review-flow", 42)
            .await
            .expect("pinned definition");
        assert_eq!(bundle.publication_revision, catalog.revision);
        assert_eq!(bundle.root().expect("root").id, "review-flow");
        assert_eq!(bundle.root().expect("root").revision, 42);
        assert_eq!(
            bundle.definitions.len(),
            1,
            "unrelated definitions must not be persisted in this run bundle"
        );
    }

    #[tokio::test]
    async fn invalid_new_definition_keeps_exact_lkg_bytes_and_definition_revision() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = write_skill(&skills_dir, "steady-flow", "review", "Original")
            .await
            .expect("skill");
        fs::write(
            root.join("workflow.yaml"),
            orchestration_yaml("steady-flow", 7),
        )
        .await
        .expect("workflow yaml");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let original = store
            .pin_workflow_definition_bundle("steady-flow", 7)
            .await
            .expect("original pin");

        fs::write(
            root.join("workflow.yaml"),
            "workflow_schema: 1\nid: steady-flow\nrevision: 8\nsteps: []\n",
        )
        .await
        .expect("invalid replacement");
        store.reload().await.expect("invalid reload isolated");
        let entry = store
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "steady-flow")
            .expect("LKG entry");
        assert_eq!(entry.status, WorkflowStatus::Invalid);
        assert_eq!(entry.revision, 7, "invalid bytes cannot advance identity");
        let pinned = store
            .pin_workflow_definition_bundle("steady-flow", 7)
            .await
            .expect("LKG remains executable");
        assert_eq!(pinned.root(), original.root());
    }

    #[tokio::test]
    async fn first_invalid_bundle_never_enters_active_skill_store() {
        const PRIVATE_RESOURCE: &str = "/private/resources/first-load-reference.md";
        const PRIVATE_INSTRUCTIONS: &str = "Secret body";
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = write_skill(
            &skills_dir,
            "never-active",
            "invalid bundle",
            PRIVATE_INSTRUCTIONS,
        )
        .await
        .expect("skill");
        fs::write(
            root.join("workflow.yaml"),
            format!(
                "id: never-active\nname: Never active\ndescription: Invalid workflow\nversion: '1'\ncomposition:\n  type: {PRIVATE_RESOURCE}\n"
            ),
        )
            .await
            .expect("invalid workflow metadata");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store
            .initialize()
            .await
            .expect("initialize isolates invalid");
        assert!(store.get_skill("never-active").await.is_err());
        let snapshot = store.workflow_catalog_snapshot().await;
        let serialized = serde_json::to_string(&snapshot).expect("serialize catalog");
        let entry = snapshot
            .entries
            .into_iter()
            .find(|entry| entry.id == "never-active")
            .expect("invalid diagnostic entry");
        assert_eq!(entry.status, WorkflowStatus::Invalid);
        let public_error = entry.last_error.as_deref().expect("public error");
        assert!(public_error.starts_with("workflow.yaml:"));
        assert!(!public_error.contains(PRIVATE_RESOURCE));
        assert!(!serialized.contains(PRIVATE_INSTRUCTIONS));
        assert!(!serialized.contains(directory.path().to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn invalid_skill_catalog_error_does_not_echo_private_frontmatter() {
        const PRIVATE_FIELD: &str = "private-catalog-frontmatter-field";
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let root = skills_dir.join("private-skill");
        fs::create_dir_all(&root).await.expect("skill root");
        fs::write(
            root.join("SKILL.md"),
            format!(
                "---\nname: private-skill\ndescription: Private skill\n{PRIVATE_FIELD}: secret\n---\nPrivate instructions\n"
            ),
        )
        .await
        .expect("invalid skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });

        store.initialize().await.expect("isolate invalid skill");
        let serialized = serde_json::to_string(&store.skill_catalog_snapshot().await)
            .expect("serialize catalog");

        assert!(
            !serialized.contains(PRIVATE_FIELD),
            "catalog leaked: {serialized}"
        );
        assert!(!serialized.contains("Private instructions"));
        assert!(!serialized.contains(root.to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn shadowed_invalid_skill_error_is_also_sanitized() {
        const PRIVATE_FIELD: &str = "private-shadowed-frontmatter-field";
        const PRIVATE_INSTRUCTIONS: &str = "Private shadowed instructions";
        let directory = tempfile::tempdir().expect("tempdir");
        let data_dir = directory.path().join("data");
        let skills_dir = data_dir.join("skills");
        write_skill(&skills_dir, "shared-skill", "winner", "Winner instructions")
            .await
            .expect("winner skill");
        let shadowed_root = data_dir.join("plugins/shadowed-plugin/skills/shared-skill");
        fs::create_dir_all(&shadowed_root)
            .await
            .expect("shadowed skill root");
        fs::write(
            shadowed_root.join("SKILL.md"),
            format!(
                "---\nname: shared-skill\ndescription: shadowed\n{PRIVATE_FIELD}: secret\n---\n{PRIVATE_INSTRUCTIONS}\n"
            ),
        )
        .await
        .expect("invalid shadowed skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });

        store.initialize().await.expect("initialize");
        let snapshot = store.skill_catalog_snapshot().await;
        let serialized = serde_json::to_string(&snapshot).expect("serialize catalog");
        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == "shared-skill")
            .expect("winner entry");

        assert_eq!(entry.shadowed_candidates.len(), 1);
        let public_error = entry.shadowed_candidates[0]
            .last_error
            .as_deref()
            .expect("shadowed public error");
        assert!(public_error.starts_with("SKILL.md:"));
        assert!(!serialized.contains(PRIVATE_FIELD));
        assert!(!serialized.contains(PRIVATE_INSTRUCTIONS));
        assert!(!serialized.contains(shadowed_root.to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn unrelated_reload_preserves_each_definition_revision() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let alpha_root = write_skill(&skills_dir, "alpha", "alpha", "Alpha prompt")
            .await
            .expect("alpha skill");
        write_skill(&skills_dir, "beta", "beta", "Beta prompt")
            .await
            .expect("beta skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let before = store.skill_catalog_snapshot().await;
        let alpha_before = before
            .entries
            .iter()
            .find(|entry| entry.id == "alpha")
            .expect("alpha catalog entry")
            .revision;
        let beta_before = before
            .entries
            .iter()
            .find(|entry| entry.id == "beta")
            .expect("beta catalog entry")
            .revision;

        fs::write(
            alpha_root.join("SKILL.md"),
            "---\nname: alpha\ndescription: alpha changed\n---\nChanged prompt\n",
        )
        .await
        .expect("update alpha");
        store.reload().await.expect("reload");

        let after = store.skill_catalog_snapshot().await;
        assert!(after.revision > before.revision);
        assert!(
            after
                .entries
                .iter()
                .find(|entry| entry.id == "alpha")
                .expect("updated alpha")
                .revision
                > alpha_before
        );
        assert_eq!(
            after
                .entries
                .iter()
                .find(|entry| entry.id == "beta")
                .expect("unchanged beta")
                .revision,
            beta_before,
            "an unrelated definition must keep its activation revision"
        );
    }

    #[tokio::test]
    async fn watcher_plan_accepts_catalog_sources_and_rejects_workspace_build_churn() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = std::fs::canonicalize(directory.path()).expect("canonical tempdir");
        let skills_dir = root.join("data/skills");
        let project_home = root.join("project-home");
        let workspace = root.join("workspace");
        fs::create_dir_all(&skills_dir).await.expect("skills dir");
        fs::create_dir_all(&project_home)
            .await
            .expect("project home");
        fs::create_dir_all(&workspace).await.expect("workspace");
        let store = SkillStore::new_with_resource_scope(
            SkillStoreConfig {
                skills_dir: skills_dir.clone(),
                active_mode: Some("code".to_string()),
                ..Default::default()
            },
            Arc::new(RetainedResourceBudget::default()),
            SkillSnapshotLimits::default(),
            Some(project_home.clone()),
            Some(workspace.clone()),
        );
        let plan = store.catalog_watch_plan();
        let relevant = [
            skills_dir.join("global/SKILL.md"),
            root.join("data/skills-code/mode/SKILL.md"),
            root.join("data/plugins/example/skills/plugin/SKILL.md"),
            root.join("data/workflows/legacy.md"),
            project_home.join("skills/project/SKILL.md"),
            project_home.join("skills-code/project-mode/SKILL.md"),
            workspace.join(".bamboo/skills/local/SKILL.md"),
            workspace.join(".bamboo/skills-code/local-mode/SKILL.md"),
            workspace.join(".bamboo/workflows/legacy.md"),
            workspace.join(".bamboo"),
        ];
        for path in relevant {
            assert!(
                plan.is_relevant(&lexical_normalize_path(&path)),
                "catalog path should be relevant: {}",
                path.display()
            );
        }

        let irrelevant = [
            root.join("data/sessions/session.json"),
            project_home.join("memory/index.json"),
            workspace.join("target/debug/build/output"),
            workspace.join("node_modules/pkg/index.js"),
        ];
        let canonicalizations = store.watcher_activity().registration_canonicalizations;
        for _ in 0..10 {
            for path in &irrelevant {
                assert!(
                    !plan.is_relevant(&lexical_normalize_path(path)),
                    "non-catalog churn should be rejected: {}",
                    path.display()
                );
            }
        }
        assert_eq!(
            store.watcher_activity().registration_canonicalizations,
            canonicalizations,
            "event classification must remain lexical and filesystem-free"
        );
    }

    #[tokio::test]
    async fn os_watcher_rebinds_missing_plugin_root_and_tracks_add_edit_remove() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        fs::create_dir_all(&skills_dir).await.expect("skills dir");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");
        let initial_revision = manager.store().skill_catalog_snapshot().await.revision;
        let initial_activity = manager.store().watcher_activity();
        let plugin_skills = directory.path().join("data/plugins/late/skills");
        let mut added_batches = manager.store().subscribe_processed_watcher_batches();
        let plugin_root = write_skill(&plugin_skills, "hot-plugin", "hot discovered", "Prompt")
            .await
            .expect("plugin skill");
        let plugin_file = plugin_root.join("SKILL.md");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = manager.store().skill_catalog_snapshot().await;
                if snapshot.revision > initial_revision
                    && snapshot
                        .entries
                        .iter()
                        .any(|entry| entry.id == "hot-plugin")
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("watcher should publish plugin without explicit refresh");
        assert!(
            wait_for_processed_watcher_path(&mut added_batches, &plugin_file)
                .await
                .relevant,
            "a plugin skill addition must be classified as catalog-relevant"
        );

        let added_activity = manager.store().watcher_activity();
        assert!(added_activity.reloads > initial_activity.reloads);
        assert!(
            added_activity.watch_rebinds > initial_activity.watch_rebinds,
            "creating the missing plugins root must bind its recursive watch"
        );
        let mut edited_batches = manager.store().subscribe_processed_watcher_batches();
        fs::write(
            &plugin_file,
            "---\nname: hot-plugin\ndescription: hot edited\n---\nEdited prompt\n",
        )
        .await
        .expect("edit plugin skill");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if manager
                    .store()
                    .skill_catalog_snapshot()
                    .await
                    .entries
                    .iter()
                    .any(|entry| entry.id == "hot-plugin" && entry.description == "hot edited")
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("recursive plugin watch should publish an edit");
        assert!(
            wait_for_processed_watcher_path(&mut edited_batches, &plugin_file)
                .await
                .relevant,
            "a plugin skill edit must be classified as catalog-relevant"
        );

        let mut removed_batches = manager.store().subscribe_processed_watcher_batches();
        fs::remove_dir_all(&plugin_root)
            .await
            .expect("remove plugin skill");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !manager
                    .store()
                    .skill_catalog_snapshot()
                    .await
                    .entries
                    .iter()
                    .any(|entry| entry.id == "hot-plugin")
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("recursive plugin watch should publish removal");
        assert!(
            wait_for_processed_watcher_path(&mut removed_batches, &plugin_root)
                .await
                .relevant,
            "a plugin skill removal must be classified as catalog-relevant"
        );
    }

    #[tokio::test]
    async fn os_watcher_hot_discovers_workspace_legacy_workflow() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&skills_dir).await.expect("skills dir");
        fs::create_dir_all(&workspace).await.expect("workspace");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");
        let store = manager
            .store_for_workspace(Some(&workspace))
            .await
            .expect("workspace store");
        let initial_revision = store.workflow_catalog_snapshot().await.revision;

        let workflows = workspace.join(".bamboo/workflows");
        let workflow_file = workflows.join("live-review.md");
        let mut workflow_batches = store.subscribe_processed_watcher_batches();
        fs::create_dir_all(&workflows).await.expect("workflow dir");
        fs::write(&workflow_file, "Review the live change.\n")
            .await
            .expect("legacy workflow");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = store.workflow_catalog_snapshot().await;
                if snapshot.revision > initial_revision
                    && snapshot.entries.iter().any(|entry| {
                        entry.id == "live-review"
                            && entry.migration_status
                                == Some(crate::LegacyWorkflowMigrationStatus::Available)
                    })
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("watcher should publish workspace legacy workflow");
        assert!(
            wait_for_processed_watcher_path(&mut workflow_batches, &workflow_file)
                .await
                .relevant,
            "a workspace workflow write must be classified as catalog-relevant"
        );

        // The watcher keeps only a shallow registration on the workspace root
        // so it can notice `.bamboo` being recreated. A project build tree may
        // therefore yield a cheap top-level event, but its recursive churn must
        // never cause catalog reloads or event-path canonicalization.
        let stable_revision = store.workflow_catalog_snapshot().await.revision;
        let stable_activity = store.watcher_activity();
        let build_output = workspace.join("target/debug/build/example/out");
        let build_root = workspace.join("target");
        let mut build_batches = store.subscribe_processed_watcher_batches();
        fs::create_dir_all(&build_output)
            .await
            .expect("project build output");
        for index in 0..64 {
            fs::write(build_output.join(format!("artifact-{index}")), b"churn")
                .await
                .expect("project build artifact");
        }
        assert!(
            !wait_for_processed_watcher_path(&mut build_batches, &build_root)
                .await
                .relevant,
            "a project build directory must be classified as catalog-irrelevant"
        );
        let after_churn = store.watcher_activity();
        assert_eq!(
            store.workflow_catalog_snapshot().await.revision,
            stable_revision,
            "project build churn must not publish a catalog revision"
        );
        assert_eq!(
            after_churn.reloads, stable_activity.reloads,
            "project build churn must not reload the catalog"
        );
        assert_eq!(
            after_churn.registration_canonicalizations,
            stable_activity.registration_canonicalizations,
            "project build churn must not canonicalize delivered event paths"
        );
    }

    #[tokio::test]
    async fn workspace_catalog_views_are_isolated_per_session() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let one = directory.path().join("one");
        let two = directory.path().join("two");
        fs::create_dir_all(one.join(".bamboo/skills"))
            .await
            .expect("workspace one");
        fs::create_dir_all(two.join(".bamboo/skills"))
            .await
            .expect("workspace two");
        write_skill(&one.join(".bamboo/skills"), "only-one", "one", "One")
            .await
            .expect("one skill");
        write_skill(&two.join(".bamboo/skills"), "only-two", "two", "Two")
            .await
            .expect("two skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let mut events = store.subscribe_workflow_catalog();
        let first_store = store
            .skill_store_for_workspace(&one)
            .await
            .expect("first store");
        let second_store = store
            .skill_store_for_workspace(&two)
            .await
            .expect("second store");
        let first = first_store.skill_catalog_snapshot().await;
        let second = second_store.skill_catalog_snapshot().await;
        assert!(first.entries.iter().any(|entry| entry.id == "only-one"));
        assert!(!first.entries.iter().any(|entry| entry.id == "only-two"));
        assert!(second.entries.iter().any(|entry| entry.id == "only-two"));
        assert!(!second.entries.iter().any(|entry| entry.id == "only-one"));

        let repeated = first_store.skill_catalog_snapshot().await;
        assert_eq!(repeated.revision, first.revision, "read must not bump");

        let skill_file = one.join(".bamboo/skills/only-one/SKILL.md");
        let staging = one.join(".bamboo/skills/only-one/.SKILL.md.atomic");
        fs::write(
            &staging,
            "---\nname: only-one\ndescription: one changed\n---\nChanged\n",
        )
        .await
        .expect("atomic staging");
        fs::rename(&staging, &skill_file)
            .await
            .expect("atomic rename");
        let updated = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = first_store.skill_catalog_snapshot().await;
                if snapshot.revision > first.revision
                    && snapshot
                        .entries
                        .iter()
                        .any(|entry| entry.id == "only-one" && entry.description == "one changed")
                {
                    break snapshot;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("workspace watcher publication");
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("workspace catalog event bridge")
            .expect("workspace catalog event");
        assert_eq!(event.workflow_id, "only-one");
        assert_eq!(event.kind, WorkflowCatalogEventKind::Changed);
        assert!(!event.public_workflow);
        assert!(event.scope.starts_with("workspace:"));
        let untouched = second_store.skill_catalog_snapshot().await;
        assert!(updated.revision > first.revision);
        assert_eq!(untouched.revision, second.revision);
        assert_eq!(
            updated
                .entries
                .iter()
                .find(|entry| entry.id == "only-one")
                .expect("updated entry")
                .description,
            "one changed"
        );
    }

    #[tokio::test]
    async fn activation_pins_definition_revision_and_resources_across_reload() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = write_skill(&skills_dir, "revision-demo", "revision N", "prompt N")
            .await
            .expect("skill N");
        fs::create_dir_all(skill_dir.join("references"))
            .await
            .expect("references");
        fs::write(skill_dir.join("references/value.txt"), "resource N")
            .await
            .expect("resource N");
        let store = Arc::new(SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        }));
        store.initialize().await.expect("initialize");
        let ids = vec!["revision-demo".to_string()];

        let revision_n = store
            .pin_current_activation("session-n", &ids, None)
            .await
            .expect("pin N");
        let (_, _, pinned_revision_n, _) = store
            .get_pinned_skill_with_root("session-n", "revision-demo")
            .await
            .expect("pinned N");
        assert_eq!(
            revision_n.skill_revisions["revision-demo"],
            pinned_revision_n
        );

        let start = Arc::new(tokio::sync::Barrier::new(2));
        let reader_observed_n = Arc::new(tokio::sync::Notify::new());
        let writer_published_n1 = Arc::new(tokio::sync::Notify::new());
        let reader = {
            let store = store.clone();
            let start = start.clone();
            let reader_observed_n = reader_observed_n.clone();
            let writer_published_n1 = writer_published_n1.clone();
            tokio::spawn(async move {
                start.wait().await;
                let before = store
                    .get_pinned_skill_with_root("session-n", "revision-demo")
                    .await
                    .expect("reader observes N before replacement");
                reader_observed_n.notify_one();
                writer_published_n1.notified().await;
                let after = store
                    .get_pinned_skill_with_root("session-n", "revision-demo")
                    .await
                    .expect("reader retains N after replacement");
                let resource = store
                    .read_pinned_skill_resource(
                        "session-n",
                        "revision-demo",
                        Path::new("references/value.txt"),
                    )
                    .await
                    .expect("reader retains resource N");
                (before, after, resource)
            })
        };
        let writer = {
            let store = store.clone();
            let start = start.clone();
            let reader_observed_n = reader_observed_n.clone();
            let writer_published_n1 = writer_published_n1.clone();
            let ids = ids.clone();
            tokio::spawn(async move {
                start.wait().await;
                reader_observed_n.notified().await;
                let staged_skill = skill_dir.join("SKILL.md.next");
                fs::write(
                    &staged_skill,
                    "---\nname: revision-demo\ndescription: revision N+1\n---\nprompt N+1\n",
                )
                .await
                .expect("stage N+1");
                fs::rename(&staged_skill, skill_dir.join("SKILL.md"))
                    .await
                    .expect("publish skill N+1");
                let staged_resource = skill_dir.join("references/value.txt.next");
                fs::write(&staged_resource, "resource N+1")
                    .await
                    .expect("stage resource N+1");
                fs::rename(&staged_resource, skill_dir.join("references/value.txt"))
                    .await
                    .expect("publish resource N+1");
                store.reload().await.expect("reload N+1");
                let revision = store
                    .pin_current_activation("session-n1", &ids, None)
                    .await
                    .expect("pin N+1");
                let new_activation = store
                    .get_pinned_skill_with_root("session-n1", "revision-demo")
                    .await
                    .expect("new activation N+1");
                let resource = store
                    .read_pinned_skill_resource(
                        "session-n1",
                        "revision-demo",
                        Path::new("references/value.txt"),
                    )
                    .await
                    .expect("new resource N+1");
                writer_published_n1.notify_one();
                (revision, new_activation, resource)
            })
        };

        let (before, after, active_resource) = reader.await.expect("reader task");
        let (revision_n1, new_activation, new_resource) = writer.await.expect("writer task");
        assert_eq!(before.0.prompt, "prompt N");
        assert_eq!(after.0.prompt, "prompt N");
        assert_eq!(before.2, pinned_revision_n);
        assert_eq!(after.2, pinned_revision_n);
        assert_eq!(active_resource, b"resource N");
        assert_eq!(new_activation.0.prompt, "prompt N+1");
        assert!(new_activation.2 > after.2);
        assert_eq!(
            revision_n1.skill_revisions["revision-demo"],
            new_activation.2
        );
        assert_eq!(new_resource, b"resource N+1");
    }

    #[tokio::test]
    async fn activation_capacity_does_not_evict_live_sessions() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        write_skill(&skills_dir, "bounded", "bounded", "bounded")
            .await
            .expect("skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let ids = vec!["bounded".to_string()];
        for index in 0..super::MAX_PINNED_SKILL_ACTIVATIONS {
            store
                .pin_current_activation(&format!("active-{index}"), &ids, None)
                .await
                .expect("capacity slot");
        }
        let error = store
            .pin_current_activation("over-capacity", &ids, None)
            .await
            .expect_err("must reject instead of evicting a live activation");
        assert!(error.to_string().contains("capacity"));
        assert!(store.activation_descriptor("active-0").await.is_some());

        store.release_activation("active-0").await;
        store
            .pin_current_activation("after-release", &ids, None)
            .await
            .expect("released slot is reusable");
    }

    #[tokio::test]
    async fn durable_restore_capacity_failure_preserves_existing_activations() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        write_skill(&skills_dir, "bounded", "bounded", "bounded")
            .await
            .expect("skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let ids = vec!["bounded".to_string()];
        for index in 0..super::MAX_PINNED_SKILL_ACTIVATIONS {
            store
                .pin_current_activation(&format!("active-{index}"), &ids, None)
                .await
                .expect("capacity slot");
        }
        let snapshot = store
            .export_activation_snapshot("active-0")
            .await
            .expect("durable snapshot");
        let error = store
            .restore_activation_snapshot("restore-over-capacity", snapshot)
            .await
            .expect_err("restore must fail atomically at capacity");
        assert!(error.to_string().contains("capacity"));
        assert!(store.activation_descriptor("active-0").await.is_some());
        assert!(store
            .activation_descriptor("restore-over-capacity")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn durable_restore_rejects_another_resource_scope_and_legacy_unscoped_bytes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        let workspace_a = directory.path().join("workspace-a");
        let workspace_b = directory.path().join("workspace-b");
        fs::create_dir_all(&workspace_a).await.expect("workspace A");
        fs::create_dir_all(&workspace_b).await.expect("workspace B");
        write_skill(
            &workspace_a.join(".bamboo/skills"),
            "private-a",
            "private",
            "A-only instructions",
        )
        .await
        .expect("workspace A skill");

        let store_a = SkillStore::new(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            project_dir: Some(workspace_a),
            active_mode: None,
        });
        store_a.initialize().await.expect("initialize A");
        store_a
            .pin_current_activation("scope-a", &["private-a".to_string()], None)
            .await
            .expect("pin A");
        let snapshot = store_a
            .export_activation_snapshot("scope-a")
            .await
            .expect("snapshot A");
        assert!(snapshot.resource_scope_fingerprint.starts_with("sha256:"));

        let store_b = SkillStore::new(SkillStoreConfig {
            skills_dir,
            project_dir: Some(workspace_b),
            active_mode: None,
        });
        store_b.initialize().await.expect("initialize B");
        let mismatch = store_b
            .restore_activation_snapshot("scope-b", snapshot.clone())
            .await
            .expect_err("scope A bytes must not restore in B");
        assert!(mismatch.to_string().contains("resource scope mismatch"));
        assert!(store_b.activation_descriptor("scope-b").await.is_none());

        let mut legacy = snapshot;
        legacy.resource_scope_fingerprint.clear();
        let legacy_error = store_a
            .restore_activation_snapshot("legacy-unscoped", legacy)
            .await
            .expect_err("unscoped legacy bytes fail closed");
        assert!(legacy_error.to_string().contains("resource scope mismatch"));
        assert!(store_a
            .activation_descriptor("legacy-unscoped")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn invalid_transition_preserves_lkg_definition_and_resources_until_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = write_skill(&skills_dir, "lkg-demo", "valid N", "prompt N")
            .await
            .expect("skill N");
        fs::create_dir_all(skill_dir.join("references"))
            .await
            .expect("references");
        fs::write(skill_dir.join("references/value.txt"), "resource N")
            .await
            .expect("resource N");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let ids = vec!["lkg-demo".to_string()];
        store
            .pin_current_activation("active-n", &ids, None)
            .await
            .expect("active N");

        fs::write(skill_dir.join("SKILL.md"), "---\nname: [\n")
            .await
            .expect("corrupt skill");
        fs::write(skill_dir.join("references/value.txt"), "corrupt resource")
            .await
            .expect("corrupt resource");
        store.reload().await.expect("publish invalid LKG");
        store
            .pin_current_activation("invalid-new", &ids, None)
            .await
            .expect("pin retained LKG");
        assert_eq!(
            store
                .get_pinned_skill_with_root("invalid-new", "lkg-demo")
                .await
                .expect("retained definition")
                .0
                .prompt,
            "prompt N"
        );
        assert_eq!(
            store
                .read_pinned_skill_resource(
                    "invalid-new",
                    "lkg-demo",
                    Path::new("references/value.txt"),
                )
                .await
                .expect("retained resource"),
            b"resource N"
        );

        write_skill(&skills_dir, "lkg-demo", "valid N+1", "prompt N+1")
            .await
            .expect("recover skill");
        fs::write(skill_dir.join("references/value.txt"), "resource N+1")
            .await
            .expect("recover resource");
        store.reload().await.expect("recover N+1");
        store
            .pin_current_activation("recovered-new", &ids, None)
            .await
            .expect("pin recovered");
        assert_eq!(
            store
                .get_pinned_skill_with_root("active-n", "lkg-demo")
                .await
                .expect("old active N")
                .0
                .prompt,
            "prompt N"
        );
        assert_eq!(
            store
                .get_pinned_skill_with_root("recovered-new", "lkg-demo")
                .await
                .expect("new recovered N+1")
                .0
                .prompt,
            "prompt N+1"
        );
    }

    #[tokio::test]
    async fn direct_activation_replaces_same_ids_when_mode_changes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        write_skill(&skills_dir, "mode-demo", "default", "default prompt")
            .await
            .expect("default skill");
        write_skill(
            &directory.path().join("skills-fast"),
            "mode-demo",
            "fast",
            "fast prompt",
        )
        .await
        .expect("fast skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let ids = vec!["mode-demo".to_string()];
        store
            .pin_current_activation("mode-session", &ids, None)
            .await
            .expect("default pin");
        let descriptor = store
            .pin_current_activation("mode-session", &ids, Some("fast"))
            .await
            .expect("mode replacement");
        assert_eq!(descriptor.selected_skill_mode.as_deref(), Some("fast"));
        assert_eq!(
            store
                .get_pinned_skill_with_root("mode-session", "mode-demo")
                .await
                .expect("fast activation")
                .0
                .prompt,
            "fast prompt"
        );
    }

    #[tokio::test]
    async fn pinned_allowed_tools_use_pinned_definition_and_honor_live_disabled_skills() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = skills_dir.join("tools-demo");
        fs::create_dir_all(&skill_dir).await.expect("skill root");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: tools-demo\ndescription: tools N\nallowed-tools:\n  - read_file\n---\ntools N\n",
        )
        .await
        .expect("tools N");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let ids = vec!["tools-demo".to_string()];
        store
            .pin_current_activation("tools-n", &ids, None)
            .await
            .expect("pin tools N");

        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: tools-demo\ndescription: tools N+1\nallowed-tools:\n  - write_file\n---\ntools N+1\n",
        )
        .await
        .expect("tools N+1");
        store.reload().await.expect("reload tools N+1");
        assert_eq!(
            store
                .pinned_allowed_tools("tools-n", &std::collections::BTreeSet::new())
                .await
                .expect("pinned tools"),
            vec!["Read"]
        );
        assert_eq!(
            store
                .pinned_allowed_tools(
                    "tools-n",
                    &std::collections::BTreeSet::from(["tools-demo".to_string()]),
                )
                .await
                .expect("disabled pinned tools"),
            Vec::<String>::new()
        );
        store
            .pin_current_activation("tools-n1", &ids, None)
            .await
            .expect("pin tools N+1");
        assert_eq!(
            store
                .pinned_allowed_tools("tools-n1", &std::collections::BTreeSet::new())
                .await
                .expect("new tools"),
            vec!["Write"]
        );
    }

    #[tokio::test]
    async fn release_does_not_require_cached_workspace_to_still_exist() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let project_skills = workspace.join(".bamboo/skills");
        write_skill(
            &project_skills,
            "workspace-release",
            "workspace release",
            "workspace release",
        )
        .await
        .expect("workspace skill");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");
        let workspace_store = store
            .skill_store_for_workspace(&workspace)
            .await
            .expect("workspace store");
        workspace_store
            .pin_current_activation(
                "deleted-workspace-session",
                &["workspace-release".to_string()],
                None,
            )
            .await
            .expect("workspace activation");
        fs::remove_dir_all(&workspace)
            .await
            .expect("delete workspace");

        let cached_after_delete = store
            .skill_store_for_workspace(&workspace)
            .await
            .expect("deleted workspace must resolve cached immutable store");
        assert!(Arc::ptr_eq(&workspace_store, &cached_after_delete));
        assert_eq!(
            cached_after_delete
                .get_pinned_skill_with_root("deleted-workspace-session", "workspace-release")
                .await
                .expect("pinned definition after delete")
                .0
                .prompt,
            "workspace release"
        );

        store
            .release_activation_across_cached_scopes("deleted-workspace-session")
            .await;
        assert!(workspace_store
            .activation_descriptor("deleted-workspace-session")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn oversized_reload_keeps_previous_publication_and_retained_budget_is_reusable() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = write_skill(&skills_dir, "bounded-bytes", "bounded", "prompt N")
            .await
            .expect("skill");
        fs::create_dir_all(skill_dir.join("references"))
            .await
            .expect("references");
        fs::write(skill_dir.join("references/value.txt"), vec![b'a'; 32])
            .await
            .expect("resource N");
        let store = SkillStore::new_with_snapshot_limits(
            SkillStoreConfig {
                skills_dir,
                ..Default::default()
            },
            super::SkillSnapshotLimits {
                max_file_bytes: 128,
                max_skill_bytes: 512,
                max_publication_bytes: 4096,
                max_retained_bytes: 2048,
            },
        );
        store.reload().await.expect("publish N");
        let catalog_n = store.skill_catalog_snapshot().await;
        let ids = vec!["bounded-bytes".to_string()];
        store
            .pin_current_activation("bounded-active", &ids, None)
            .await
            .expect("pin N");
        fs::write(skill_dir.join("references/value.txt"), vec![b'b'; 129])
            .await
            .expect("oversize resource");
        let error = store.reload().await.expect_err("oversize reload rejected");
        assert!(error.to_string().contains("per-file limit"));
        assert_eq!(store.skill_catalog_snapshot().await, catalog_n);
        assert_eq!(
            store
                .get_pinned_skill_with_root("bounded-active", "bounded-bytes")
                .await
                .expect("old pin")
                .0
                .prompt,
            "prompt N"
        );

        store.release_activation("bounded-active").await;
        store
            .pin_current_activation("bounded-lkg", &ids, None)
            .await
            .expect("new activation uses live LKG after failed reload");
        assert_eq!(
            store
                .read_pinned_skill_resource(
                    "bounded-lkg",
                    "bounded-bytes",
                    Path::new("references/value.txt"),
                )
                .await
                .expect("LKG resource"),
            vec![b'a'; 32]
        );
        store.release_activation("bounded-lkg").await;
        fs::write(skill_dir.join("references/value.txt"), vec![b'c'; 32])
            .await
            .expect("bounded resource");
        store.reload().await.expect("recovered publication");
        store
            .pin_current_activation("bounded-reused", &ids, None)
            .await
            .expect("released retained budget is reusable");
    }

    #[tokio::test]
    async fn retained_distinct_generation_budget_rejects_new_pin_without_evicting_old() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = write_skill(&skills_dir, "generation-budget", "budget", "prompt N")
            .await
            .expect("skill N");
        fs::create_dir_all(skill_dir.join("references"))
            .await
            .expect("references");
        fs::write(skill_dir.join("references/value.txt"), vec![b'n'; 32])
            .await
            .expect("resource N");
        let definition_n = serde_json::to_vec(&crate::SkillDefinition::new(
            "generation-budget",
            "generation-budget",
            "budget",
            "prompt N",
        ))
        .expect("definition N")
        .len();
        let definition_n1 = serde_json::to_vec(&crate::SkillDefinition::new(
            "generation-budget",
            "generation-budget",
            "budget",
            "prompt N+1",
        ))
        .expect("definition N+1")
        .len();
        let store = SkillStore::new_with_snapshot_limits(
            SkillStoreConfig {
                skills_dir,
                ..Default::default()
            },
            super::SkillSnapshotLimits {
                max_file_bytes: 256,
                max_skill_bytes: 1024,
                max_publication_bytes: 2048,
                max_retained_bytes: definition_n + definition_n1 + 65,
            },
        );
        store.reload().await.expect("publish N");
        let ids = vec!["generation-budget".to_string()];
        store
            .pin_current_activation("active-n", &ids, None)
            .await
            .expect("pin N");
        write_skill(
            skill_dir.parent().expect("skills root"),
            "generation-budget",
            "budget",
            "prompt N+1",
        )
        .await
        .expect("skill N+1");
        fs::write(skill_dir.join("references/value.txt"), vec![b'1'; 32])
            .await
            .expect("resource N+1");
        store.reload().await.expect("publish N+1");
        let catalog_n1 = store.skill_catalog_snapshot().await;
        let error = store
            .pin_current_activation("new-n1", &ids, None)
            .await
            .expect_err("N and current N+1 fit, but pinning retained N+1 must exceed budget");
        assert!(error.to_string().contains("budget"));
        assert_eq!(store.skill_catalog_snapshot().await, catalog_n1);
        assert_eq!(
            store
                .read_pinned_skill_resource(
                    "active-n",
                    "generation-budget",
                    Path::new("references/value.txt"),
                )
                .await
                .expect("active N retained"),
            vec![b'n'; 32]
        );
        store.release_activation("active-n").await;
        store
            .pin_current_activation("new-n1", &ids, None)
            .await
            .expect("release frees N generation budget");
        assert_eq!(
            store
                .get_pinned_skill_with_root("new-n1", "generation-budget")
                .await
                .expect("N+1 pin")
                .0
                .prompt,
            "prompt N+1"
        );
    }

    #[tokio::test]
    async fn workspace_publications_share_one_global_retained_budget() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace_a = directory.path().join("workspace-a");
        let workspace_b = directory.path().join("workspace-b");
        for workspace in [&workspace_a, &workspace_b] {
            let root = write_skill(
                &workspace.join(".bamboo/skills"),
                "publication-demo",
                "publication",
                "publication",
            )
            .await
            .expect("workspace skill");
            fs::create_dir_all(root.join("references"))
                .await
                .expect("references");
            fs::write(root.join("references/value.bin"), vec![b'x'; 128])
                .await
                .expect("resource");
        }
        let store = SkillStore::new_with_snapshot_limits(
            SkillStoreConfig {
                skills_dir: directory.path().join("data/skills"),
                ..Default::default()
            },
            super::SkillSnapshotLimits {
                max_file_bytes: 256,
                max_skill_bytes: 1024,
                max_publication_bytes: 1024,
                max_retained_bytes: 400,
            },
        );
        store.reload().await.expect("empty root publication");
        store
            .skill_store_for_workspace(&workspace_a)
            .await
            .expect("first workspace publication fits");
        let error = store
            .skill_store_for_workspace(&workspace_b)
            .await
            .err()
            .expect("second workspace publication must share the same budget");
        assert!(error
            .to_string()
            .contains("global workflow snapshot budget"));
    }

    #[tokio::test]
    async fn empty_workspace_store_cache_is_globally_bounded() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        });
        for index in 0..super::MAX_CACHED_WORKSPACE_STORES {
            let workspace = directory.path().join(format!("workspace-{index}"));
            fs::create_dir_all(&workspace).await.expect("workspace");
            store
                .skill_store_for_workspace(&workspace)
                .await
                .expect("bounded workspace store slot");
        }
        let overflow = directory.path().join("workspace-overflow");
        fs::create_dir_all(&overflow)
            .await
            .expect("overflow workspace");
        let error = store
            .skill_store_for_workspace(&overflow)
            .await
            .err()
            .expect("empty workspace stores must be capped");
        assert!(error.to_string().contains("workspace store capacity"));
    }

    #[tokio::test]
    async fn global_legacy_workflow_is_discovered_without_materializing_a_skill() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let workflows = data.join("workflows");
        fs::create_dir_all(&workflows).await.expect("workflows");
        let source = workflows.join("daily-report.md");
        fs::write(
            &source,
            "---\ndescription: Use for the daily report.\n---\nReport instructions.\n",
        )
        .await
        .expect("legacy workflow");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });

        store.initialize().await.expect("initialize");
        let catalog = store.workflow_catalog_snapshot().await;
        let entry = catalog
            .public_workflows()
            .entries
            .into_iter()
            .find(|entry| entry.id == "daily-report")
            .expect("legacy workflow");
        assert!(entry.legacy);
        assert_eq!(
            entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert!(source.exists());
        assert!(
            !data.join("skills/daily-report/SKILL.md").exists(),
            "discovery must not turn a Workflow into a Skill"
        );

        store.reload().await.expect("reload");
        assert!(
            !data.join("skills/daily-report/SKILL.md").exists(),
            "reload must remain read-only for legacy Workflow sources"
        );
    }

    #[tokio::test]
    async fn same_id_skill_and_legacy_workflow_publish_independent_winners() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let skill_root = write_skill(
            &data.join("skills"),
            "shared",
            "Independent Skill",
            "Skill instructions.",
        )
        .await
        .expect("skill");
        let workflow_source = data.join("workflows/shared.md");
        fs::create_dir_all(workflow_source.parent().unwrap())
            .await
            .expect("workflows");
        fs::write(
            &workflow_source,
            "---\ndescription: Independent Workflow\n---\nWorkflow instructions.\n",
        )
        .await
        .expect("workflow");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });
        store.initialize().await.expect("initialize");

        let skills = store.list_skills(None, false).await;
        let shared = skills
            .iter()
            .filter(|skill| skill.id == "shared")
            .collect::<Vec<_>>();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].description, "Independent Skill");
        let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
        assert_eq!(
            skill_catalog.revision, workflow_catalog.revision,
            "cross-namespace readers must observe one publication generation"
        );
        let skill_entry = skill_catalog
            .entries
            .iter()
            .find(|entry| entry.id == "shared")
            .expect("skill winner");
        assert!(skill_entry.winner);
        assert!(!skill_entry.legacy);
        assert_eq!(skill_entry.description, "Independent Skill");
        let workflow_entry = workflow_catalog
            .entries
            .iter()
            .find(|entry| entry.id == "shared")
            .expect("workflow winner");
        assert!(workflow_entry.winner);
        assert_eq!(
            workflow_entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert_eq!(workflow_entry.description, "Independent Workflow");
        assert_eq!(store.get_skill_root("shared").await.unwrap(), skill_root);
        assert_eq!(
            store.get_workflow_root("shared").await.unwrap(),
            workflow_source
        );
    }

    #[tokio::test]
    async fn authoritative_legacy_source_wins_over_stale_automatic_import_bundle() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let source = data.join("workflows/legacy.md");
        fs::create_dir_all(source.parent().unwrap())
            .await
            .expect("workflows");
        fs::write(
            &source,
            "---\ndescription: Current Workflow description.\n---\nCurrent Workflow body.\n",
        )
        .await
        .expect("source");
        let bundle = data.join("skills/legacy/SKILL.md");
        fs::create_dir_all(bundle.parent().unwrap())
            .await
            .expect("skills");
        let stale = format!(
            "---\nname: legacy\ndescription: Stale imported description\nmetadata:\n  legacy_import: true\n  legacy_name: legacy\n  original_source: '{}'\n---\nStale copied body.\n",
            source.display()
        );
        fs::write(&bundle, &stale).await.expect("stale bundle");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });

        store.initialize().await.expect("initialize");
        assert!(store.get_skill("legacy").await.is_err());
        assert!(store
            .skill_catalog_snapshot()
            .await
            .entries
            .iter()
            .all(|entry| entry.id != "legacy"));
        let entry = store
            .workflow_catalog_snapshot()
            .await
            .public_workflows()
            .entries
            .into_iter()
            .find(|entry| entry.id == "legacy")
            .expect("workflow catalog entry");
        assert_eq!(entry.description, "Current Workflow description.");
        assert_eq!(
            entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert!(entry.shadowed_candidates.iter().any(|candidate| {
            candidate.migration_status == Some(crate::LegacyWorkflowMigrationStatus::Migrated)
        }));
        assert_eq!(
            fs::read_to_string(&bundle).await.expect("bundle preserved"),
            stale
        );
    }

    #[tokio::test]
    async fn workspace_legacy_workflow_is_read_only_catalog_input_with_lkg() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let workflows = workspace.join(".bamboo/workflows");
        fs::create_dir_all(&workflows).await.expect("workflows");
        let source = workflows.join("review.md");
        fs::write(
            &source,
            "---\ndescription: Use when reviewing a code change.\n---\nStable review instructions.\n",
        )
        .await
        .expect("legacy workflow");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        });
        let workspace_store = store
            .skill_store_for_workspace(&workspace)
            .await
            .expect("workspace store");
        let catalog = workspace_store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review")
            .expect("legacy entry");
        assert_eq!(entry.source, crate::WorkflowSource::Workspace);
        assert!(entry.legacy);
        assert_eq!(
            entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert_eq!(entry.status, WorkflowStatus::Valid);
        assert_eq!(entry.invocation_policy["automatic"], true);
        assert!(workspace_store.get_skill("review").await.is_err());
        assert_eq!(
            fs::canonicalize(
                workspace_store
                    .get_legacy_workflow_source("review")
                    .await
                    .expect("legacy source")
            )
            .await
            .expect("canonical resolved source"),
            fs::canonicalize(&source)
                .await
                .expect("canonical expected source")
        );
        assert!(source.exists());
        assert!(!workspace.join(".bamboo/skills/review/SKILL.md").exists());

        fs::write(
            &source,
            "---\ndescription: [private-broken-value\n---\nPRIVATE BROKEN BODY\n",
        )
        .await
        .expect("break source");
        workspace_store
            .reload()
            .await
            .expect("isolated invalid reload");
        let catalog = workspace_store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review")
            .expect("LKG entry");
        assert_eq!(entry.status, WorkflowStatus::Invalid);
        assert!(entry.legacy);
        assert_eq!(
            entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        let public = serde_json::to_string(entry).expect("catalog JSON");
        assert!(!public.contains("private-broken-value"));
        assert!(!public.contains("PRIVATE BROKEN BODY"));
        assert!(!public.contains(workspace.to_string_lossy().as_ref()));
        assert!(workspace_store.get_skill("review").await.is_err());
        assert!(workspace_store
            .get_legacy_workflow_source("review")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn explicit_migrated_skill_coexists_with_legacy_workflow_source() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let workflows = workspace.join(".bamboo/workflows");
        let skills = workspace.join(".bamboo/skills");
        fs::create_dir_all(&workflows).await.expect("workflows");
        let source = workflows.join("review.md");
        fs::write(&source, "Review the diff.\n")
            .await
            .expect("legacy source");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        });
        let workspace_store = store
            .skill_store_for_workspace(&workspace)
            .await
            .expect("workspace store");
        crate::legacy::migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            Some("Use when reviewing a code change."),
        )
        .await
        .expect("migration");
        workspace_store.reload().await.expect("reload migration");
        let (skill_catalog, workflow_catalog) = workspace_store.command_catalog_snapshots().await;
        assert_eq!(skill_catalog.revision, workflow_catalog.revision);
        let skill_entry = skill_catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review")
            .expect("migrated Skill");
        assert!(skill_entry.legacy);
        assert_eq!(
            skill_entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Migrated)
        );
        assert_eq!(skill_entry.source, crate::WorkflowSource::Workspace);
        let workflow_entry = workflow_catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review")
            .expect("source Workflow");
        assert_eq!(
            workflow_entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert_eq!(
            workspace_store
                .get_skill("review")
                .await
                .expect("migrated Skill")
                .prompt,
            "Review the diff."
        );
        assert_eq!(
            fs::canonicalize(
                workspace_store
                    .get_workflow_root("review")
                    .await
                    .expect("source Workflow")
            )
            .await
            .expect("canonical resolved source"),
            fs::canonicalize(&source)
                .await
                .expect("canonical expected source")
        );
    }

    #[tokio::test]
    async fn plugin_legacy_workflow_is_discovered_in_place_without_global_copy() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let workflows = data.join("plugins/reporter/workflows");
        fs::create_dir_all(&workflows)
            .await
            .expect("plugin workflows");
        fs::write(
            workflows.join("daily-report.md"),
            "---\ndescription: Use for the daily report.\n---\nReport instructions.\n",
        )
        .await
        .expect("plugin workflow");
        fs::write(
            data.join("plugins/reporter/plugin.json"),
            r#"{"id":"reporter","provides":{"workflows":["daily-report.md"]}}"#,
        )
        .await
        .expect("plugin manifest");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });
        store.reload().await.expect("plugin catalog");
        let catalog = store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "daily-report")
            .expect("plugin legacy entry");
        assert_eq!(entry.source, crate::WorkflowSource::Plugin);
        assert_eq!(
            entry.migration_status,
            Some(crate::LegacyWorkflowMigrationStatus::Available)
        );
        assert!(!data.join("workflows/daily-report.md").exists());
        assert!(!data.join("skills/daily-report/SKILL.md").exists());
    }

    #[tokio::test]
    async fn undeclared_plugin_workflow_is_not_published() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let plugin = data.join("plugins/reporter");
        fs::create_dir_all(plugin.join("workflows"))
            .await
            .expect("plugin workflows");
        fs::write(
            plugin.join("workflows/private-notes.md"),
            "Private undeclared instructions.\n",
        )
        .await
        .expect("undeclared workflow");
        fs::write(
            plugin.join("plugin.json"),
            r#"{"id":"reporter","provides":{"workflows":[]}}"#,
        )
        .await
        .expect("plugin manifest");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });
        store.reload().await.expect("plugin catalog");
        assert!(store
            .workflow_catalog_snapshot()
            .await
            .entries
            .iter()
            .all(|entry| entry.id != "private-notes"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn workspace_alias_cache_is_bounded_for_one_canonical_store() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("target");
        fs::create_dir_all(&target).await.expect("target");
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        });
        store
            .skill_store_for_workspace(&target)
            .await
            .expect("canonical target");
        let initial_aliases = store.workspace_stores.read().await.len();
        for index in 0..(super::MAX_CACHED_WORKSPACE_ALIASES - initial_aliases) {
            let alias = directory.path().join(format!("alias-{index}"));
            symlink(&target, &alias).expect("alias");
            store
                .skill_store_for_workspace(&alias)
                .await
                .expect("bounded alias slot");
        }
        let overflow = directory.path().join("alias-overflow");
        symlink(&target, &overflow).expect("overflow alias");
        let error = store
            .skill_store_for_workspace(&overflow)
            .await
            .err()
            .expect("aliases must be capped");
        assert!(error.to_string().contains("workspace alias capacity"));
    }
}
