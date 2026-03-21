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
//! ```no_run
//! use bamboo_agent::skill::{SkillStore, SkillStoreConfig};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = SkillStoreConfig {
//!         skills_dir: PathBuf::from("./skills"),
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
use std::path::PathBuf;

use tokio::sync::RwLock;
use tracing::info;

use crate::agent::skill::store::builtin::load_builtin_skill_bundles;
use crate::agent::skill::store::parser::render_skill_markdown;
use crate::agent::skill::store::storage::{
    ensure_skills_dir, load_skills_from_dir, write_skill_file,
};
use crate::agent::skill::types::{
    SkillDefinition, SkillError, SkillFilter, SkillId, SkillResult, SkillStoreConfig,
};

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

    /// Configuration specifying the skills directory path.
    config: SkillStoreConfig,
}

impl SkillStore {
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
    /// ```no_run
    /// use bamboo_agent::skill::{SkillStore, SkillStoreConfig};
    /// use std::path::PathBuf;
    ///
    /// let config = SkillStoreConfig {
    ///     skills_dir: PathBuf::from("./skills"),
    /// };
    /// let store = SkillStore::new(config);
    /// ```
    pub fn new(config: SkillStoreConfig) -> Self {
        Self {
            skills: RwLock::new(HashMap::new()),
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
        let loaded = load_skills_from_dir(&self.config.skills_dir).await?;
        let count = loaded.len();

        let mut skills = self.skills.write().await;
        *skills = loaded;

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

        result.sort_by(|left, right| left.name.cmp(&right.name));
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
        skills.sort_by(|left, right| left.name.cmp(&right.name));
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
    use tokio::fs;

    use super::SkillStore;
    use crate::agent::skill::types::SkillStoreConfig;

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

        let config = SkillStoreConfig { skills_dir };
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
        };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skills = store.list_skills(None, false).await;
        assert!(skills.iter().any(|skill| skill.id == "skill-creator"));
    }
}
