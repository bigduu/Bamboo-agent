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

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio::sync::RwLock;
use tracing::info;

use crate::store::builtin::load_builtin_skill_bundles;
use crate::store::parser::render_skill_markdown;
use crate::store::storage::{
    discover_plugin_skill_dirs, ensure_skills_dir, load_skills_from_discovery_dirs,
    write_skill_file, SkillDirectorySource, SkillDiscoveryDir,
};
use crate::types::{
    SkillDefinition, SkillError, SkillFilter, SkillId, SkillResult, SkillStoreConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillCandidateMeta {
    source: SkillDirectorySource,
    mode: Option<String>,
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
    /// In-memory cache of loaded skills, keyed by skill ID.
    skills: RwLock<HashMap<SkillId, SkillDefinition>>,
    /// Root directory of each loaded skill (keyed by skill ID).
    skill_roots: RwLock<HashMap<SkillId, PathBuf>>,

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
        let mut dirs = Vec::new();
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

    fn resolve_from_loaded_records(
        loaded_records: Vec<crate::store::storage::LoadedSkillRecord>,
    ) -> (HashMap<SkillId, SkillDefinition>, HashMap<SkillId, PathBuf>) {
        let mut resolved_skills: HashMap<SkillId, SkillDefinition> = HashMap::new();
        let mut resolved_roots: HashMap<SkillId, PathBuf> = HashMap::new();
        let mut resolved_meta: HashMap<SkillId, SkillCandidateMeta> = HashMap::new();

        for record in loaded_records {
            let skill_id = record.skill.id.clone();
            let candidate_meta = SkillCandidateMeta {
                source: record.source,
                mode: record.mode.clone(),
            };

            let should_replace = resolved_meta
                .get(&skill_id)
                .is_some_and(|existing| Self::should_override_skill(existing, &candidate_meta));
            let should_keep_existing = resolved_meta.contains_key(&skill_id) && !should_replace;

            if should_keep_existing {
                // A same-tier, same-mode collision is a genuine AMBIGUITY (two
                // plugins, or two dirs at the same precedence, shipping the
                // same skill id) — the winner is decided only by discovery
                // order, so surface it at WARN. Legitimate precedence
                // overrides (project > Bamboo global > ~/.agents > plugin, or mode-specific >
                // generic) are expected and stay at debug.
                let existing_meta = resolved_meta.get(&skill_id);
                let is_ambiguous_collision = existing_meta.is_some_and(|existing| {
                    existing.source == candidate_meta.source && existing.mode == candidate_meta.mode
                });
                if is_ambiguous_collision {
                    tracing::warn!(
                        "Skill id '{}' is shipped by more than one source at the same precedence \
                         ({:?}); keeping the first and shadowing this duplicate (mode={})",
                        skill_id,
                        candidate_meta.source,
                        candidate_meta.mode.as_deref().unwrap_or("generic")
                    );
                } else {
                    tracing::debug!(
                        "Keeping existing skill '{}' over candidate from {:?} (mode={})",
                        skill_id,
                        candidate_meta.source,
                        candidate_meta.mode.as_deref().unwrap_or("generic")
                    );
                }
                continue;
            }

            if should_replace {
                tracing::info!(
                    "Skill '{}' overridden by {:?} (mode={})",
                    skill_id,
                    candidate_meta.source,
                    candidate_meta.mode.as_deref().unwrap_or("generic")
                );
            }

            resolved_skills.insert(skill_id.clone(), record.skill);
            resolved_roots.insert(skill_id.clone(), record.skill_root);
            resolved_meta.insert(skill_id, candidate_meta);
        }

        (resolved_skills, resolved_roots)
    }

    async fn resolve_skills_maps_for_mode(
        &self,
        mode_override: Option<&str>,
    ) -> SkillResult<(HashMap<SkillId, SkillDefinition>, HashMap<SkillId, PathBuf>)> {
        let mut dirs = self.discovery_dirs_for_mode(mode_override);
        let plugins_root = Self::plugins_root_dir(&self.config.skills_dir);
        dirs.extend(discover_plugin_skill_dirs(&plugins_root).await);

        let loaded_records = load_skills_from_discovery_dirs(&dirs).await?;
        Ok(Self::resolve_from_loaded_records(loaded_records))
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
            SkillDirectorySource::Plugin => 0,
            SkillDirectorySource::Agents => 1,
            SkillDirectorySource::Global => 2,
            SkillDirectorySource::Project => 3,
        }
    }

    fn should_override_skill(
        existing: &SkillCandidateMeta,
        candidate: &SkillCandidateMeta,
    ) -> bool {
        let existing_rank = Self::source_rank(existing.source);
        let candidate_rank = Self::source_rank(candidate.source);
        if existing_rank != candidate_rank {
            return candidate_rank > existing_rank;
        }

        match (existing.mode.is_some(), candidate.mode.is_some()) {
            (false, true) => true,
            (true, false) => false,
            _ => false,
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
        Self {
            skills: RwLock::new(HashMap::new()),
            skill_roots: RwLock::new(HashMap::new()),
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
        self.create_builtin_skills().await?;
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
        let (resolved_skills, resolved_roots) = self.resolve_skills_maps_for_mode(None).await?;
        let count = resolved_skills.len();
        let mut skills = self.skills.write().await;
        let mut roots = self.skill_roots.write().await;
        *skills = resolved_skills;
        *roots = resolved_roots;

        Ok(count)
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
        for bundle in load_builtin_skill_bundles()? {
            let skill_id = bundle.skill.id.clone();
            write_skill_file(&self.config.skills_dir, &bundle.skill).await?;

            for (relative_path, content) in bundle.files {
                let full_path = self.config.skills_dir.join(&skill_id).join(&relative_path);
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

    /// Get the root directory path for a loaded skill.
    pub async fn get_skill_root(&self, id: &str) -> SkillResult<PathBuf> {
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

    use super::{SkillCandidateMeta, SkillStore};
    use crate::store::storage::SkillDirectorySource;
    use crate::types::SkillStoreConfig;

    #[test]
    fn agents_skill_precedence_is_below_bamboo_global_and_above_plugin() {
        let agents = SkillCandidateMeta {
            source: SkillDirectorySource::Agents,
            mode: None,
        };
        let global = SkillCandidateMeta {
            source: SkillDirectorySource::Global,
            mode: None,
        };
        let plugin = SkillCandidateMeta {
            source: SkillDirectorySource::Plugin,
            mode: None,
        };
        assert!(SkillStore::should_override_skill(&agents, &global));
        assert!(SkillStore::should_override_skill(&plugin, &agents));
        assert!(!SkillStore::should_override_skill(&global, &agents));
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
}
