//! Agent skill management crate.

pub mod context;
pub mod selection;
pub mod store;
pub mod types;

pub use store::{SkillStore, SkillUpdate};
pub use types::*;

use std::collections::HashSet;
use std::sync::Arc;

/// Skill manager instance (convenience wrapper around SkillStore).
#[derive(Clone)]
pub struct SkillManager {
    store: Arc<SkillStore>,
}

impl SkillManager {
    /// Create a new skill manager with default configuration.
    pub fn new() -> Self {
        Self {
            store: Arc::new(SkillStore::default()),
        }
    }

    /// Create a new skill manager with custom configuration.
    pub fn with_config(config: SkillStoreConfig) -> Self {
        Self {
            store: Arc::new(SkillStore::new(config)),
        }
    }

    /// Initialize the manager.
    pub async fn initialize(&self) -> SkillResult<()> {
        self.store.initialize().await
    }

    /// Get the underlying store.
    pub fn store(&self) -> &SkillStore {
        &self.store
    }

    async fn list_skills_for_selection(
        &self,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> Vec<SkillDefinition> {
        // Reload to get latest skills.
        let skills = if selected_skill_mode.is_some() {
            self.store
                .list_skills_for_mode(None, selected_skill_mode)
                .await
        } else {
            self.store.list_skills(None, true).await
        };
        let Some(selected_skill_ids) = selected_skill_ids else {
            return skills;
        };

        let selected_set: HashSet<&str> = selected_skill_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .collect();
        if selected_set.is_empty() {
            return skills;
        }

        let filtered: Vec<SkillDefinition> = skills
            .into_iter()
            .filter(|skill| selected_set.contains(skill.id.as_str()))
            .collect();

        if filtered.len() != selected_set.len() {
            let missing: Vec<&str> = selected_set
                .iter()
                .copied()
                .filter(|selected| !filtered.iter().any(|skill| skill.id == *selected))
                .collect();
            if !missing.is_empty() {
                tracing::warn!(
                    "Some selected skills were not found on disk and will be ignored: {:?}",
                    missing
                );
            }
        }

        filtered
    }

    /// Build system prompt context from a selected subset of skills.
    pub async fn build_skill_context_for_selection(
        &self,
        selected_skill_ids: Option<&[String]>,
    ) -> String {
        self.build_skill_context_for_selection_with_mode(selected_skill_ids, None)
            .await
    }

    /// Build system prompt context from a selected subset of skills with mode override.
    pub async fn build_skill_context_for_selection_with_mode(
        &self,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> String {
        let skills = self
            .list_skills_for_selection(selected_skill_ids, selected_skill_mode)
            .await;
        tracing::info!(
            "Building skill context with {} skill(s), selection_mode={}, skill_mode={}",
            skills.len(),
            if selected_skill_ids.is_some() {
                "selected"
            } else {
                "all"
            },
            selected_skill_mode.unwrap_or("default"),
        );
        context::build_skill_context(&skills)
    }

    /// Build system prompt context from all skills.
    pub async fn build_skill_context(&self, _chat_id: Option<&str>) -> String {
        self.build_skill_context_for_selection(None).await
    }

    /// Get allowed tool refs from a selected subset of skills.
    pub async fn get_allowed_tools_for_selection(
        &self,
        selected_skill_ids: Option<&[String]>,
    ) -> Vec<String> {
        self.get_allowed_tools_for_selection_with_mode(selected_skill_ids, None)
            .await
    }

    /// Get allowed tool refs from a selected subset of skills with mode override.
    pub async fn get_allowed_tools_for_selection_with_mode(
        &self,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> Vec<String> {
        let skills = self
            .list_skills_for_selection(selected_skill_ids, selected_skill_mode)
            .await;

        let mut tools: Vec<String> = skills
            .into_iter()
            .flat_map(|skill| skill.tool_refs)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        tools.sort();
        tools
    }

    /// Get allowed tool refs from all skills.
    pub async fn get_allowed_tools(&self, _chat_id: Option<&str>) -> Vec<String> {
        self.get_allowed_tools_for_selection(None).await
    }
}

impl Default for SkillManager {
    fn default() -> Self {
        Self::new()
    }
}
