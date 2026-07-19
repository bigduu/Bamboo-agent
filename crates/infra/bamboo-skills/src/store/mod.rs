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

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::RwLock;
use tracing::info;

use crate::catalog::{
    entry_from_skill, load_bundle_metadata, ShadowedWorkflowCandidate, WorkflowCatalogEntry,
    WorkflowCatalogEvent, WorkflowCatalogEventKind, WorkflowCatalogSnapshot, WorkflowKind,
    WorkflowStatus,
};
use crate::store::builtin::{archive_exact_legacy_materialization, load_builtin_skill_bundles};
use crate::store::parser::render_skill_markdown;
use crate::store::storage::{
    discover_plugin_skill_dirs, ensure_skills_dir, load_skills_from_discovery_dirs_detailed,
    write_skill_file, FailedSkillRecord, LoadedSkillRecord, SkillDirectorySource,
    SkillDiscoveryDir,
};
use crate::types::{
    SkillDefinition, SkillError, SkillFilter, SkillId, SkillResult, SkillStoreConfig,
};

fn invalid_placeholder(
    id: &str,
    source: SkillDirectorySource,
    revision: u64,
    error: &str,
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
pub struct SkillStore {
    /// Serializes publication and observation of the correlated snapshot maps below.
    snapshot_publish_lock: RwLock<()>,
    /// In-memory cache of loaded skills, keyed by skill ID.
    skills: RwLock<HashMap<SkillId, SkillDefinition>>,
    /// Root directory of each loaded skill (keyed by skill ID).
    skill_roots: RwLock<HashMap<SkillId, PathBuf>>,
    catalog: RwLock<WorkflowCatalogSnapshot>,
    next_revision: AtomicU64,
    watcher_started: AtomicBool,
    catalog_events: tokio::sync::broadcast::Sender<WorkflowCatalogEvent>,
    reload_lock: tokio::sync::Mutex<()>,
    mode_stores: RwLock<HashMap<String, std::sync::Arc<SkillStore>>>,
    workspace_stores: RwLock<HashMap<PathBuf, std::sync::Arc<SkillStore>>>,

    /// Configuration specifying the skills directory path.
    config: SkillStoreConfig,
}

impl SkillStore {
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

    fn project_skills_dir(project_dir: &Path) -> PathBuf {
        project_dir.join(".bamboo").join("skills")
    }

    fn project_skills_mode_dir(project_dir: &Path, mode: &str) -> PathBuf {
        project_dir.join(".bamboo").join(format!("skills-{mode}"))
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

        if let Some(project_dir) = self.config.project_dir.as_ref() {
            dirs.push(SkillDiscoveryDir {
                dir: Self::project_skills_dir(project_dir),
                source: SkillDirectorySource::Project,
                mode: None,
            });
            if let Some(mode) = active_mode.as_ref() {
                dirs.push(SkillDiscoveryDir {
                    dir: Self::project_skills_mode_dir(project_dir, mode),
                    source: SkillDirectorySource::Project,
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
        let (catalog_events, _) = tokio::sync::broadcast::channel(128);
        Self {
            snapshot_publish_lock: RwLock::new(()),
            skills: RwLock::new(HashMap::new()),
            skill_roots: RwLock::new(HashMap::new()),
            catalog: RwLock::new(WorkflowCatalogSnapshot::default()),
            next_revision: AtomicU64::new(1),
            watcher_started: AtomicBool::new(false),
            catalog_events,
            reload_lock: tokio::sync::Mutex::new(()),
            mode_stores: RwLock::new(HashMap::new()),
            workspace_stores: RwLock::new(HashMap::new()),
            config,
        }
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
        let report = crate::legacy::import_legacy_markdown_workflows(
            &workflows_dir,
            &self.config.skills_dir,
        )
        .await?;
        if !report.imported.is_empty() {
            info!("Imported legacy workflows as skills: {:?}", report.imported);
        }
        for diagnostic in report.diagnostics {
            tracing::warn!("Legacy workflow import: {diagnostic}");
        }
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
        let dirs = self.discovery_dirs_for_mode(None);
        let mut dirs = dirs;
        let plugins_root = Self::plugins_root_dir(&self.config.skills_dir);
        dirs.extend(discover_plugin_skill_dirs(&plugins_root).await);
        let report = load_skills_from_discovery_dirs_detailed(&dirs).await?;

        let (previous_skills, previous_roots, previous_catalog) = {
            let _snapshot_guard = self.snapshot_publish_lock.read().await;
            (
                self.skills.read().await.clone(),
                self.skill_roots.read().await.clone(),
                self.catalog.read().await.clone(),
            )
        };
        let revision = self.next_revision.load(Ordering::SeqCst);
        let (resolved_skills, resolved_roots, mut entries) = self
            .resolve_catalog(
                report.loaded,
                report.failed,
                &previous_skills,
                &previous_roots,
                &previous_catalog,
                revision,
            )
            .await;
        let count = resolved_skills.len();
        let definition_changed: HashSet<String> = resolved_skills
            .iter()
            .filter(|(id, skill)| {
                previous_skills.get(*id) != Some(*skill)
                    || previous_roots.get(*id) != resolved_roots.get(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        // `WorkflowCatalogEntry::revision` identifies the revision of that workflow,
        // not merely the revision of the containing snapshot. Preserve it across
        // unrelated catalog updates so an activation can pin a meaningful definition
        // revision. Prompt-only changes are covered by `definition_changed` even though
        // the metadata-only public entry would otherwise compare equal.
        for entry in &mut entries {
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
        let next_catalog = WorkflowCatalogSnapshot { revision, entries };
        let mut comparable_previous = previous_catalog.clone();
        comparable_previous.revision = revision;
        if resolved_skills == previous_skills
            && resolved_roots == previous_roots
            && next_catalog == comparable_previous
        {
            return Ok(count);
        }
        self.next_revision.fetch_add(1, Ordering::SeqCst);
        {
            // Definition, root, and metadata become visible as one immutable generation.
            let _snapshot_guard = self.snapshot_publish_lock.write().await;
            *self.skills.write().await = resolved_skills;
            *self.skill_roots.write().await = resolved_roots;
            *self.catalog.write().await = next_catalog.clone();
        }
        self.publish_catalog_events(&previous_catalog, &next_catalog, &definition_changed);

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
                                entry.revision = revision;
                                entry.status = WorkflowStatus::Invalid;
                                entry.last_error = Some(error);
                                entry
                            } else {
                                invalid_placeholder(&id, record.source, revision, &error)
                            }
                        } else {
                            invalid_placeholder(&id, record.source, revision, &error)
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
                            entry.revision = revision;
                            entry.status = WorkflowStatus::Invalid;
                            entry.last_error = Some(record.error.clone());
                            entry
                        } else {
                            invalid_placeholder(&id, record.source, revision, &record.error)
                        }
                    } else {
                        invalid_placeholder(&id, record.source, revision, &record.error)
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
        let store = std::sync::Arc::new(SkillStore::new(SkillStoreConfig {
            skills_dir: self.config.skills_dir.clone(),
            project_dir: self.config.project_dir.clone(),
            active_mode: Some(mode.clone()),
        }));
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
        let workspace = tokio::fs::canonicalize(workspace).await?;
        if let Some(store) = self.workspace_stores.read().await.get(&workspace).cloned() {
            return Ok(store);
        }
        let mut stores = self.workspace_stores.write().await;
        if let Some(store) = stores.get(&workspace).cloned() {
            return Ok(store);
        }
        let store = std::sync::Arc::new(SkillStore::new(SkillStoreConfig {
            skills_dir: self.config.skills_dir.clone(),
            project_dir: Some(workspace.clone()),
            active_mode: self.config.active_mode.clone(),
        }));
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
        Ok(store)
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

    /// Roots watched for skill, workflow metadata, project, and plugin changes.
    pub(crate) fn watch_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self
            .config
            .skills_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.config.skills_dir.clone())];
        if let Some(project) = self.config.project_dir.as_ref() {
            roots.push(project.clone());
        }
        roots
    }

    fn is_catalog_watch_path(&self, path: &Path) -> bool {
        fn normalized(path: &Path) -> std::borrow::Cow<'_, Path> {
            std::fs::canonicalize(path)
                .map(std::borrow::Cow::Owned)
                .unwrap_or_else(|_| std::borrow::Cow::Borrowed(path))
        }
        fn starts_with(path: &Path, root: &Path) -> bool {
            path.starts_with(root) || normalized(path).starts_with(normalized(root).as_ref())
        }

        if starts_with(path, &self.config.skills_dir) {
            return true;
        }
        if let Some(data_dir) = self.config.skills_dir.parent() {
            if starts_with(path, &data_dir.join("workflows"))
                || starts_with(path, &data_dir.join("plugins"))
            {
                return true;
            }
            let normalized_path = normalized(path);
            let normalized_data = normalized(data_dir);
            if normalized_path
                .strip_prefix(normalized_data.as_ref())
                .ok()
                .is_some_and(|relative| {
                    relative
                        .components()
                        .next()
                        .and_then(|component| component.as_os_str().to_str())
                        .is_some_and(|name| name.starts_with("skills-"))
                })
            {
                return true;
            }
        }
        self.config.project_dir.as_ref().is_some_and(|project| {
            normalized(path)
                .strip_prefix(normalized(&project.join(".bamboo")).as_ref())
                .ok()
                .and_then(|relative| relative.components().next())
                .and_then(|component| component.as_os_str().to_str())
                .is_some_and(|name| name == "skills" || name.starts_with("skills-"))
        })
    }

    /// Start an OS-backed recursive watcher. Debouncing coalesces editor atomic-renames and
    /// avoids reloading once per low-level self-write event.
    pub fn start_live_reload(self: &std::sync::Arc<Self>) {
        use notify::{RecursiveMode, Watcher};
        if self.watcher_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = std::sync::Arc::downgrade(self);
        let roots = self.watch_roots();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut watcher = match notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        }) {
            Ok(watcher) => watcher,
            Err(error) => {
                tracing::warn!("Failed to start skill catalog watcher: {error}");
                self.watcher_started.store(false, Ordering::SeqCst);
                return;
            }
        };
        for root in roots {
            if root.exists() {
                if let Err(error) = watcher.watch(&root, RecursiveMode::Recursive) {
                    tracing::warn!(
                        "Failed to watch skill catalog root {}: {error}",
                        root.display()
                    );
                }
            }
        }
        tokio::spawn(async move {
            // Keep the native watcher alive for exactly as long as the receiver loop.
            let _watcher = watcher;
            while let Some(event) = receiver.recv().await {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::warn!("Skill catalog watcher error: {error}");
                        continue;
                    }
                };
                let Some(store) = weak.upgrade() else { break };
                if !event
                    .paths
                    .iter()
                    .any(|path| store.is_catalog_watch_path(path))
                {
                    continue;
                }
                // Editors commonly emit write + chmod + rename. Publish one snapshot after the
                // filesystem has settled, while still reacting promptly to external changes.
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                while receiver.try_recv().is_ok() {}
                if let Err(error) = store.reload().await {
                    tracing::warn!("Live skill catalog reload failed: {error}");
                }
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

            for (relative_path, content) in bundle.files {
                let full_path = builtin_skills_dir.join(&skill_id).join(&relative_path);
                if let Some(parent) = full_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&full_path, content).await?;
                // Make script files executable on Unix
                #[cfg(unix)]
                {
                    if relative_path.starts_with("scripts/") {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = tokio::fs::metadata(&full_path).await?.permissions();
                        perms.set_mode(0o755);
                        tokio::fs::set_permissions(&full_path, perms).await?;
                    }
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
        let workflows_dir = self
            .config
            .skills_dir
            .parent()
            .map(|parent| parent.join("workflows"))
            .unwrap_or_else(|| PathBuf::from("workflows"));
        let report = crate::legacy::import_legacy_markdown_workflows(
            &workflows_dir,
            &self.config.skills_dir,
        )
        .await?;
        for diagnostic in report.diagnostics {
            tracing::warn!("Legacy workflow import: {diagnostic}");
        }
        for diagnostic in
            crate::legacy::migrate_legacy_yaml_workflows(&workflows_dir, &self.config.skills_dir)
                .await
        {
            if !diagnostic.can_map_to_bundle {
                tracing::warn!("Legacy YAML migration: {}", diagnostic.message);
            }
        }
        self.load().await
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
        // Optionally reload from disk to pick up new/updated skills
        if refresh {
            if let Err(e) = self.reload().await {
                tracing::warn!("Failed to reload skills: {}", e);
            }
        }

        let _snapshot_guard = self.snapshot_publish_lock.read().await;
        let skills = self.skills.read().await;

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

    use tokio::fs;

    use super::SkillStore;
    use crate::store::builtin::{load_builtin_skill_bundles, BuiltinSkillBundle};
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

    async fn materialize_legacy_builtin(skills_dir: &Path, bundle: &BuiltinSkillBundle) {
        write_skill_file(skills_dir, &bundle.skill)
            .await
            .expect("legacy SKILL.md");
        for (relative, bytes) in &bundle.files {
            let path = skills_dir.join(&bundle.skill.id).join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await.expect("legacy parent");
            }
            fs::write(path, bytes).await.expect("legacy resource");
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
                .workflow_catalog_snapshot()
                .await
                .entries
                .into_iter()
                .find(|entry| entry.id == "skill-creator")
                .expect("catalog entry")
                .source,
            WorkflowSource::Builtin
        );
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
                .workflow_catalog_snapshot()
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
        let catalog = store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "override-skill")
            .expect("catalog entry");
        assert_eq!(entry.source, WorkflowSource::Project);
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
        let catalog = store.workflow_catalog_snapshot().await;
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
    async fn mode_override_uses_catalog_validation_and_retains_lkg() {
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
        assert_eq!(
            store
                .get_skill_for_mode("mode-catalog", Some("code"))
                .await
                .expect("initial mode skill")
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
        assert_eq!(
            store
                .get_skill_for_mode("mode-catalog", Some("code"))
                .await
                .expect("mode LKG")
                .prompt,
            "Mode prompt v1",
            "invalid mode metadata must not activate new instructions"
        );

        fs::write(
            mode_root.join("workflow.yaml"),
            "id: mode-catalog\nname: Mode catalog\ndescription: Mode workflow\nversion: '2'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("recovered metadata");
        assert_eq!(
            store
                .get_skill_for_mode("mode-catalog", Some("code"))
                .await
                .expect("recovered mode skill")
                .prompt,
            "Mode prompt v2"
        );
    }

    #[tokio::test]
    async fn invalid_reload_retains_lkg_and_emits_invalid_then_recovered() {
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
            .workflow_catalog_snapshot()
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
        assert_eq!(
            events.recv().await.expect("invalid event").kind,
            WorkflowCatalogEventKind::Invalid
        );

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
        assert_eq!(
            events.recv().await.expect("recovered event").kind,
            WorkflowCatalogEventKind::Recovered
        );
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
        let active = store.get_skill("orchestrate").await.expect("LKG active");
        assert_eq!(active.description, "orchestrates");
        assert_eq!(active.prompt, "Instructions");
        let public_error = invalid.last_error.as_deref().expect("public error");
        assert!(public_error.starts_with("workflow.yaml:"));
        assert!(!public_error.contains(PRIVATE_RESOURCE));
        assert!(!public_error.contains(PRIVATE_INSTRUCTIONS));
        assert!(!public_error.contains(root.to_string_lossy().as_ref()));
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
        let serialized = serde_json::to_string(&store.workflow_catalog_snapshot().await)
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
        let snapshot = store.workflow_catalog_snapshot().await;
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
        let before = store.workflow_catalog_snapshot().await;
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

        let after = store.workflow_catalog_snapshot().await;
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
    async fn os_watcher_hot_discovers_plugin_installed_after_startup() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("data/skills");
        fs::create_dir_all(&skills_dir).await.expect("skills dir");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");
        let initial_revision = manager.store().workflow_catalog_snapshot().await.revision;
        let plugin_skills = directory.path().join("data/plugins/late/skills");
        write_skill(&plugin_skills, "hot-plugin", "hot discovered", "Prompt")
            .await
            .expect("plugin skill");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = manager.store().workflow_catalog_snapshot().await;
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

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let stable_revision = manager.store().workflow_catalog_snapshot().await.revision;
        fs::create_dir_all(directory.path().join("data/sessions"))
            .await
            .expect("sessions dir");
        fs::write(directory.path().join("data/sessions/unrelated.json"), "{}")
            .await
            .expect("unrelated write");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            manager.store().workflow_catalog_snapshot().await.revision,
            stable_revision,
            "unrelated data-dir writes must not publish a catalog revision"
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
        let first = store
            .workflow_catalog_for_workspace(&one)
            .await
            .expect("first view");
        let second = store
            .workflow_catalog_for_workspace(&two)
            .await
            .expect("second view");
        assert!(first.entries.iter().any(|entry| entry.id == "only-one"));
        assert!(!first.entries.iter().any(|entry| entry.id == "only-two"));
        assert!(second.entries.iter().any(|entry| entry.id == "only-two"));
        assert!(!second.entries.iter().any(|entry| entry.id == "only-one"));

        let repeated = store
            .workflow_catalog_for_workspace(&one)
            .await
            .expect("cached first view");
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
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let event = events.recv().await.expect("workspace event");
                if event.workflow_id == "only-one" {
                    break event;
                }
            }
        })
        .await
        .expect("workspace watcher event");
        assert!(event.scope.starts_with("workspace:"));
        let updated = store
            .workflow_catalog_for_workspace(&one)
            .await
            .expect("updated first view");
        let untouched = store
            .workflow_catalog_for_workspace(&two)
            .await
            .expect("untouched second view");
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
}
