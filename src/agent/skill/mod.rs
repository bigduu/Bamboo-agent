//! Agent skill management crate.

pub mod context;
pub mod selection;
pub mod store;
pub mod types;

pub use store::{SkillStore, SkillUpdate};
pub use types::*;

use std::collections::HashSet;
use std::sync::Arc;

const MAX_UNSELECTED_SKILLS_IN_CONTEXT: usize = 24;

fn tokenize_request_hint(request_hint: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();

    for token in request_hint
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '-')
        .map(|token| token.trim().to_lowercase())
        .filter(|token| token.len() >= 3)
    {
        if seen.insert(token.clone()) {
            tokens.push(token);
        }
    }

    tokens
}

fn skill_match_score(skill: &SkillDefinition, tokens: &[String]) -> usize {
    if tokens.is_empty() {
        return 0;
    }

    let searchable = format!(
        "{} {} {} {}",
        skill.id.to_lowercase(),
        skill.name.to_lowercase(),
        skill.description.to_lowercase(),
        skill
            .tool_refs
            .iter()
            .map(|tool| tool.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ")
    );

    tokens
        .iter()
        .map(|token| {
            if searchable.contains(token) {
                if skill.id.to_lowercase().contains(token)
                    || skill.name.to_lowercase().contains(token)
                {
                    3
                } else {
                    1
                }
            } else {
                0
            }
        })
        .sum()
}

fn shortlist_skills_for_context(
    mut skills: Vec<SkillDefinition>,
    request_hint: Option<&str>,
) -> Vec<SkillDefinition> {
    if skills.len() <= MAX_UNSELECTED_SKILLS_IN_CONTEXT {
        return skills;
    }

    let hint_tokens = request_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(tokenize_request_hint)
        .unwrap_or_default();

    if hint_tokens.is_empty() {
        skills.sort_by(|left, right| left.id.cmp(&right.id));
        skills.truncate(MAX_UNSELECTED_SKILLS_IN_CONTEXT);
        return skills;
    }

    let mut ranked: Vec<(usize, SkillDefinition)> = skills
        .into_iter()
        .map(|skill| (skill_match_score(&skill, &hint_tokens), skill))
        .collect();

    ranked.sort_by(|(left_score, left_skill), (right_score, right_skill)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_skill.id.cmp(&right_skill.id))
    });

    let mut selected = Vec::new();
    let mut selected_ids = HashSet::new();

    for (score, skill) in ranked.iter().cloned() {
        if score == 0 || selected.len() >= MAX_UNSELECTED_SKILLS_IN_CONTEXT {
            break;
        }
        selected_ids.insert(skill.id.clone());
        selected.push(skill);
    }

    if selected.len() < MAX_UNSELECTED_SKILLS_IN_CONTEXT {
        let mut fallback: Vec<SkillDefinition> = ranked
            .into_iter()
            .map(|(_, skill)| skill)
            .filter(|skill| !selected_ids.contains(&skill.id))
            .collect();
        fallback.sort_by(|left, right| left.id.cmp(&right.id));
        let remaining = MAX_UNSELECTED_SKILLS_IN_CONTEXT - selected.len();
        selected.extend(fallback.into_iter().take(remaining));
    }

    selected.sort_by(|left, right| left.id.cmp(&right.id));
    selected
}

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
        self.build_skill_context_for_request_with_mode(selected_skill_ids, None, None)
            .await
    }

    /// Build system prompt context from a selected subset of skills with mode override.
    pub async fn build_skill_context_for_selection_with_mode(
        &self,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> String {
        self.build_skill_context_for_request_with_mode(
            selected_skill_ids,
            selected_skill_mode,
            None,
        )
        .await
    }

    /// Build system prompt context from a selected subset of skills with mode and user request hint.
    pub async fn build_skill_context_for_request_with_mode(
        &self,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> String {
        let mut skills = self
            .list_skills_for_selection(selected_skill_ids, selected_skill_mode)
            .await;

        if selected_skill_ids.is_none() {
            let original_len = skills.len();
            skills = shortlist_skills_for_context(skills, request_hint);
            if skills.len() < original_len {
                tracing::info!(
                    "Skill context shortlisted from {} to {} entries (request_hint_present={})",
                    original_len,
                    skills.len(),
                    request_hint
                        .map(str::trim)
                        .is_some_and(|value| !value.is_empty())
                );
            }
        }

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

#[cfg(test)]
mod tests {
    use super::{shortlist_skills_for_context, tokenize_request_hint, SkillDefinition};

    fn demo_skill(id: &str, description: &str) -> SkillDefinition {
        SkillDefinition::new(id, id, description, "prompt")
    }

    #[test]
    fn tokenize_request_hint_dedupes_and_filters_short_tokens() {
        let tokens = tokenize_request_hint("fix ui ui in app and api");
        assert!(tokens.contains(&"fix".to_string()));
        assert!(tokens.contains(&"app".to_string()));
        assert!(tokens.contains(&"api".to_string()));
        assert_eq!(
            tokens.iter().filter(|token| token.as_str() == "ui").count(),
            0
        );
    }

    #[test]
    fn shortlist_skills_for_context_prefers_request_matches() {
        let mut skills = Vec::new();
        for index in 0..30 {
            skills.push(demo_skill(
                &format!("skill-{index:02}"),
                "generic helper skill",
            ));
        }
        skills.push(demo_skill("react-optimizer", "react vite optimization"));

        let shortlisted = shortlist_skills_for_context(skills, Some("optimize react vite build"));
        assert!(shortlisted.len() <= 24);
        assert!(shortlisted
            .iter()
            .any(|skill| skill.id == "react-optimizer"));
    }
}
