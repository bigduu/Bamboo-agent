//! Skill store with in-memory cache and markdown persistence.

pub mod builtin;
pub mod parser;
pub mod storage;

use std::collections::HashMap;
use std::path::PathBuf;

use log::info;
use tokio::sync::RwLock;

use crate::store::builtin::{create_builtin_skills, get_builtin_scripts};
use crate::store::parser::render_skill_markdown;
use crate::store::storage::{
    ensure_skills_dir, load_skills_from_dir, skill_path, write_skill_file,
};
use crate::types::{
    SkillDefinition, SkillError, SkillFilter, SkillId, SkillResult, SkillStoreConfig,
    SkillVisibility,
};

/// Persistent storage for skills.
pub struct SkillStore {
    skills: RwLock<HashMap<SkillId, SkillDefinition>>,
    config: SkillStoreConfig,
}

impl SkillStore {
    /// Create a new skill store with the given configuration.
    pub fn new(config: SkillStoreConfig) -> Self {
        Self {
            skills: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Initialize the store, loading from disk if available.
    pub async fn initialize(&self) -> SkillResult<()> {
        info!("Initializing skill store...");
        ensure_skills_dir(&self.config.skills_dir).await?;

        let loaded = self.load().await?;
        if loaded == 0 {
            info!("No existing skills found, creating built-in skills");
            self.create_builtin_skills().await?;
            self.load().await?;
        }

        info!("Skill store initialized");
        Ok(())
    }

    /// Load skills from disk into memory.
    async fn load(&self) -> SkillResult<usize> {
        let loaded = load_skills_from_dir(&self.config.skills_dir).await?;
        let count = loaded.len();

        let mut skills = self.skills.write().await;
        *skills = loaded;

        Ok(count)
    }

    async fn create_builtin_skills(&self) -> SkillResult<()> {
        for skill in create_builtin_skills() {
            let path = skill_path(&self.config.skills_dir, &skill.id);
            if path.exists() {
                continue;
            }
            write_skill_file(&self.config.skills_dir, &skill).await?;

            // Write embedded scripts for builtin skills (e.g., skill-creator)
            let scripts = get_builtin_scripts(&skill.id);
            for (script_path, content) in scripts {
                let full_path = self.config.skills_dir.join(&skill.id).join(script_path);
                if let Some(parent) = full_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&full_path, content).await?;
                // Make scripts executable on Unix
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = tokio::fs::metadata(&full_path).await?.permissions();
                    perms.set_mode(0o755);
                    tokio::fs::set_permissions(&full_path, perms).await?;
                }
            }
        }

        Ok(())
    }

    /// Reload skills from disk.
    pub async fn reload(&self) -> SkillResult<usize> {
        info!("Reloading skills from disk...");
        self.load().await
    }

    /// List all skills with optional filtering.
    /// Optionally reload from disk before listing.
    pub async fn list_skills(&self, filter: Option<SkillFilter>, refresh: bool) -> Vec<SkillDefinition> {
        // Optionally reload from disk to pick up new/updated skills
        if refresh {
            if let Err(e) = self.reload().await {
                log::warn!("Failed to reload skills: {}", e);
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

    /// Get a single skill by ID.
    pub async fn get_skill(&self, id: &str) -> SkillResult<SkillDefinition> {
        let skills = self.skills.read().await;
        skills
            .get(id)
            .cloned()
            .ok_or_else(|| SkillError::NotFound(id.to_string()))
    }

    /// Create a new skill (not supported in read-only mode).
    pub async fn create_skill(&self, _skill: SkillDefinition) -> SkillResult<SkillDefinition> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Update an existing skill (not supported in read-only mode).
    pub async fn update_skill(
        &self,
        _id: &str,
        _updates: SkillUpdate,
    ) -> SkillResult<SkillDefinition> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Delete a skill (not supported in read-only mode).
    pub async fn delete_skill(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Enable a skill globally (not supported in read-only mode).
    pub async fn enable_skill_global(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Disable a skill globally (not supported in read-only mode).
    pub async fn disable_skill_global(&self, _id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Enable a skill for a specific chat (not supported in read-only mode).
    pub async fn enable_skill_for_chat(&self, _skill_id: &str, _chat_id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Disable a skill for a specific chat (not supported in read-only mode).
    pub async fn disable_skill_for_chat(&self, _skill_id: &str, _chat_id: &str) -> SkillResult<()> {
        Err(SkillError::ReadOnly(
            "Skills are read-only and must be edited as Markdown files".to_string(),
        ))
    }

    /// Get all skills.
    pub async fn get_all_skills(&self) -> Vec<SkillDefinition> {
        let mut skills: Vec<SkillDefinition> = self
            .skills
            .read()
            .await
            .values()
            .cloned()
            .collect();
        skills.sort_by(|left, right| left.name.cmp(&right.name));
        skills
    }

    pub fn skills_dir(&self) -> &PathBuf {
        &self.config.skills_dir
    }

    /// Get all categories.
    pub async fn get_categories(&self) -> Vec<String> {
        let mut categories: Vec<String> = self
            .skills
            .read()
            .await
            .values()
            .map(|skill| skill.category.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        categories.sort();
        categories
    }

    /// Get all tags.
    pub async fn get_all_tags(&self) -> Vec<String> {
        let mut tags: Vec<String> = self
            .skills
            .read()
            .await
            .values()
            .flat_map(|skill| skill.tags.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        tags.sort();
        tags
    }

    /// Export skills to markdown.
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
#[derive(Debug, Clone, Default)]
pub struct SkillUpdate {
    pub name: Option<String>,
    pub description: Option<String>,
    pub category: Option<String>,
    pub tags: Option<Vec<String>>,
    pub prompt: Option<String>,
    pub tool_refs: Option<Vec<String>>,
    pub workflow_refs: Option<Vec<String>>,
    pub visibility: Option<SkillVisibility>,
    pub version: Option<String>,
}

impl SkillUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }

    pub fn with_tool_refs(mut self, tool_refs: Vec<String>) -> Self {
        self.tool_refs = Some(tool_refs);
        self
    }

    pub fn with_workflow_refs(mut self, workflow_refs: Vec<String>) -> Self {
        self.workflow_refs = Some(workflow_refs);
        self
    }

    pub fn with_visibility(mut self, visibility: SkillVisibility) -> Self {
        self.visibility = Some(visibility);
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use tokio::fs;

    use super::SkillStore;
    use crate::types::SkillStoreConfig;

    #[tokio::test]
    async fn load_markdown_skills() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        fs::create_dir_all(&skills_dir).await.expect("create dir");

        let content = r#"---
id: test-skill
name: Test Skill
description: A test skill
category: test
tags:
  - demo
tool_refs:
  - read_file
workflow_refs: []
visibility: public
version: 1.0.0
created_at: "2026-02-01T00:00:00Z"
updated_at: "2026-02-01T00:00:00Z"
---
Use this skill for testing.
"#;

        let skill_dir = skills_dir.join("test-skill");
        fs::create_dir_all(&skill_dir).await.expect("create skill dir");
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(&skill_file, content).await.expect("write");

        let config = SkillStoreConfig { skills_dir };
        let store = SkillStore::new(config);
        store.initialize().await.expect("initialize");

        let skills = store.list_skills(None, false).await;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].id, "test-skill");
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
        assert!(!skills.is_empty());
    }
}
