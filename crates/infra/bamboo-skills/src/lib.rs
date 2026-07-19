//! Agent skill management (re-exported from bamboo-agent-skill crate).

pub mod access_control;
pub mod catalog;
pub mod context;
pub mod legacy;
pub mod resource_helpers;
pub mod runtime_metadata;
pub mod selection;
pub mod session_port;
pub mod store;
pub mod types;

pub use catalog::{
    ShadowedWorkflowCandidate, WorkflowCatalogEntry, WorkflowCatalogEvent,
    WorkflowCatalogEventKind, WorkflowCatalogSnapshot, WorkflowKind, WorkflowSource,
    WorkflowStatus,
};
pub use store::{SkillActivationDescriptor, SkillStore, SkillUpdate};
pub use types::*;

use std::collections::{BTreeSet, HashSet};
use std::path::Path;
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
        skills.sort_by_key(|s| s.id.clone());
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
        fallback.sort_by_key(|s| s.id.clone());
        let remaining = MAX_UNSELECTED_SKILLS_IN_CONTEXT - selected.len();
        selected.extend(fallback.into_iter().take(remaining));
    }

    selected.sort_by_key(|s| s.id.clone());
    selected
}

fn filter_disabled_skills(
    skills: Vec<SkillDefinition>,
    disabled_skill_ids: &BTreeSet<String>,
) -> Vec<SkillDefinition> {
    if disabled_skill_ids.is_empty() {
        return skills;
    }

    skills
        .into_iter()
        .filter(|skill| !disabled_skill_ids.contains(&skill.id))
        .collect()
}

fn invocation_allowed_skill_ids<'a>(
    catalog: &'a WorkflowCatalogSnapshot,
    policy: &str,
) -> HashSet<&'a str> {
    catalog
        .entries
        .iter()
        .filter(|entry| entry.winner && entry.invocation_policy[policy].as_bool() == Some(true))
        .map(|entry| entry.id.as_str())
        .collect()
}

/// Skill manager instance (convenience wrapper around SkillStore).
#[derive(Clone)]
pub struct SkillManager {
    store: Arc<SkillStore>,
    activation_scope_coordinator: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct SkillActivationSelection {
    pub skills: Vec<SkillDefinition>,
    pub descriptor: SkillActivationDescriptor,
}

impl SkillManager {
    /// Create a new skill manager with default configuration.
    pub fn new() -> Self {
        Self {
            store: Arc::new(SkillStore::default()),
            activation_scope_coordinator: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Create a new skill manager with custom configuration.
    pub fn with_config(config: SkillStoreConfig) -> Self {
        Self {
            store: Arc::new(SkillStore::new(config)),
            activation_scope_coordinator: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Initialize the manager.
    pub async fn initialize(&self) -> SkillResult<()> {
        self.store.initialize().await?;
        self.store.start_live_reload();
        Ok(())
    }

    /// Get the underlying store.
    pub fn store(&self) -> &SkillStore {
        &self.store
    }

    /// Resolve the shared global store or an isolated workspace store.
    /// Callers at API/runtime boundaries must derive `workspace` from trusted
    /// server-side session state rather than request/tool arguments.
    pub async fn store_for_workspace(
        &self,
        workspace: Option<&Path>,
    ) -> SkillResult<Arc<SkillStore>> {
        match workspace {
            Some(workspace) => self.store.skill_store_for_workspace(workspace).await,
            None => Ok(self.store.clone()),
        }
    }

    /// Build an immutable workflow bundle from one global/workspace publication.
    /// The workspace argument must be derived from trusted server-side session state.
    pub async fn pin_workflow_definition_bundle(
        &self,
        workspace: Option<&Path>,
        root_id: &str,
        root_revision: u64,
    ) -> SkillResult<bamboo_domain::WorkflowDefinitionBundle> {
        self.store_for_workspace(workspace)
            .await?
            .pin_workflow_definition_bundle(root_id, root_revision)
            .await
    }

    fn filter_skills_for_selection(
        skills: Vec<SkillDefinition>,
        catalog: &WorkflowCatalogSnapshot,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
    ) -> Vec<SkillDefinition> {
        let skills = filter_disabled_skills(skills, disabled_skill_ids);
        let Some(selected_skill_ids) = selected_skill_ids else {
            let automatic_ids = invocation_allowed_skill_ids(catalog, "automatic");
            return skills
                .into_iter()
                .filter(|skill| automatic_ids.contains(skill.id.as_str()))
                .collect();
        };

        let selected_set: HashSet<&str> = selected_skill_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .collect();
        if selected_set.is_empty() {
            let automatic_ids = invocation_allowed_skill_ids(catalog, "automatic");
            return skills
                .into_iter()
                .filter(|skill| automatic_ids.contains(skill.id.as_str()))
                .collect();
        }

        let explicit_ids = invocation_allowed_skill_ids(catalog, "explicit");
        let denied: Vec<&str> = selected_set
            .iter()
            .copied()
            .filter(|selected| !explicit_ids.contains(selected))
            .collect();
        if !denied.is_empty() {
            tracing::warn!(
                "Some selected skills do not allow explicit invocation and will be ignored: {:?}",
                denied
            );
        }

        let filtered: Vec<SkillDefinition> = skills
            .into_iter()
            .filter(|skill| {
                selected_set.contains(skill.id.as_str()) && explicit_ids.contains(skill.id.as_str())
            })
            .collect();

        let missing: Vec<&str> = selected_set
            .iter()
            .copied()
            .filter(|selected| explicit_ids.contains(selected))
            .filter(|selected| !filtered.iter().any(|skill| skill.id == *selected))
            .collect();
        if !missing.is_empty() {
            tracing::warn!(
                "Some selected skills were not found on disk and will be ignored: {:?}",
                missing
            );
        }

        filtered
    }

    async fn list_skills_for_selection_from_store(
        store: &SkillStore,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> Vec<SkillDefinition> {
        let (skills, catalog) = match store.skills_and_catalog_for_mode(selected_skill_mode).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    "Failed to resolve skills and workflow policy for mode {:?}: {}",
                    selected_skill_mode,
                    error
                );
                return Vec::new();
            }
        };
        Self::filter_skills_for_selection(skills, &catalog, disabled_skill_ids, selected_skill_ids)
    }

    async fn resolve_and_pin_activation_from_store(
        store: &SkillStore,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> SkillResult<SkillActivationSelection> {
        // All four values are cloned under the store's publication read lock.
        // A watcher may publish after this await, but this activation only consumes
        // these correlated generation-N clones and can never mix them with N+1.
        let (skills, roots, resources, catalog) = store
            .activation_source_for_mode(selected_skill_mode)
            .await?;
        let mut selected = Self::filter_skills_for_selection(
            skills,
            &catalog,
            disabled_skill_ids,
            selected_skill_ids,
        );
        if selected_skill_ids.is_none() {
            selected = shortlist_skills_for_context(selected, request_hint);
        }
        let descriptor = store
            .pin_activation_from_source(
                activation_id,
                selected_skill_mode,
                &selected,
                &roots,
                &resources,
                &catalog,
            )
            .await?;
        Ok(SkillActivationSelection {
            skills: selected,
            descriptor,
        })
    }

    pub(crate) async fn list_skills_for_selection(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> Vec<SkillDefinition> {
        Self::list_skills_for_selection_from_store(
            self.store.as_ref(),
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
        )
        .await
    }

    /// Build system prompt context from a selected subset of skills.
    pub async fn build_skill_context_for_selection(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
    ) -> String {
        self.build_skill_context_for_request_with_mode(
            disabled_skill_ids,
            selected_skill_ids,
            None,
            None,
        )
        .await
    }

    /// Build system prompt context from a selected subset of skills with mode override.
    pub async fn build_skill_context_for_selection_with_mode(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> String {
        self.build_skill_context_for_request_with_mode(
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            None,
        )
        .await
    }

    pub async fn resolve_skills_for_request_with_mode(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> Vec<SkillDefinition> {
        let mut skills = self
            .list_skills_for_selection(disabled_skill_ids, selected_skill_ids, selected_skill_mode)
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

        skills
    }

    /// Resolve policy-aware prompt candidates and pin their exact published
    /// definition/resource generation for one runtime activation.
    pub async fn resolve_and_pin_activation_for_request_with_mode(
        &self,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> SkillResult<SkillActivationSelection> {
        let _scope_guard = self.activation_scope_coordinator.lock().await;
        self.prepare_activation_scope(activation_id, &self.store)
            .await;
        Self::resolve_and_pin_activation_from_store(
            self.store.as_ref(),
            activation_id,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            request_hint,
        )
        .await
    }

    /// Resolve prompt-visible skills against the same isolated store used by a
    /// session's workspace catalog.
    pub async fn resolve_skills_for_request_in_workspace_with_mode(
        &self,
        workspace: &Path,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> SkillResult<Vec<SkillDefinition>> {
        let store = self.store.skill_store_for_workspace(workspace).await?;
        let mut skills = Self::list_skills_for_selection_from_store(
            store.as_ref(),
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
        )
        .await;

        if selected_skill_ids.is_none() {
            skills = shortlist_skills_for_context(skills, request_hint);
        }
        Ok(skills)
    }

    pub async fn resolve_and_pin_activation_in_workspace_with_mode(
        &self,
        workspace: &Path,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> SkillResult<SkillActivationSelection> {
        let store = self.store.skill_store_for_workspace(workspace).await?;
        let _scope_guard = self.activation_scope_coordinator.lock().await;
        self.prepare_activation_scope(activation_id, &store).await;
        Self::resolve_and_pin_activation_from_store(
            store.as_ref(),
            activation_id,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            request_hint,
        )
        .await
    }

    async fn prepare_activation_scope(&self, activation_id: &str, target: &Arc<SkillStore>) {
        let mut has_non_target_owner = self
            .store
            .activation_descriptor(activation_id)
            .await
            .is_some()
            && !Arc::ptr_eq(&self.store, target);
        for store in self.store.cached_workspace_stores().await {
            if store.activation_descriptor(activation_id).await.is_some()
                && !Arc::ptr_eq(&store, target)
            {
                has_non_target_owner = true;
                break;
            }
        }
        if has_non_target_owner {
            self.store
                .release_activation_across_cached_scopes(activation_id)
                .await;
        }
    }

    pub async fn pin_current_activation_for_workspace(
        &self,
        activation_id: &str,
        workspace: Option<&Path>,
        selected_skill_ids: &[String],
        selected_skill_mode: Option<&str>,
    ) -> SkillResult<SkillActivationDescriptor> {
        let store = self.store_for_workspace(workspace).await?;
        let _scope_guard = self.activation_scope_coordinator.lock().await;
        self.prepare_activation_scope(activation_id, &store).await;
        store
            .pin_current_activation(activation_id, selected_skill_ids, selected_skill_mode)
            .await
    }

    pub async fn pinned_allowed_tools_for_workspace(
        &self,
        activation_id: &str,
        workspace: Option<&Path>,
        disabled_skill_ids: &BTreeSet<String>,
    ) -> SkillResult<Option<Vec<String>>> {
        let store = self.store_for_workspace(workspace).await?;
        Ok(store
            .pinned_allowed_tools(activation_id, disabled_skill_ids)
            .await)
    }

    pub async fn pinned_activation_for_workspace(
        &self,
        activation_id: &str,
        workspace: Option<&Path>,
    ) -> SkillResult<Option<SkillActivationSelection>> {
        let store = self.store_for_workspace(workspace).await?;
        Ok(store
            .pinned_activation_skills(activation_id)
            .await
            .map(|(skills, descriptor)| SkillActivationSelection { skills, descriptor }))
    }

    pub async fn release_activation_for_workspace(
        &self,
        activation_id: &str,
        _workspace: Option<&Path>,
    ) -> SkillResult<()> {
        let _scope_guard = self.activation_scope_coordinator.lock().await;
        self.store
            .release_activation_across_cached_scopes(activation_id)
            .await;
        Ok(())
    }

    /// Build system prompt context from a selected subset of skills with mode and user request hint.
    pub async fn build_skill_context_for_request_with_mode(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
    ) -> String {
        let skills = self
            .resolve_skills_for_request_with_mode(
                disabled_skill_ids,
                selected_skill_ids,
                selected_skill_mode,
                request_hint,
            )
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
    pub async fn build_skill_context(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        _chat_id: Option<&str>,
    ) -> String {
        self.build_skill_context_for_selection(disabled_skill_ids, None)
            .await
    }

    /// Get allowed tool refs from a selected subset of skills.
    pub async fn get_allowed_tools_for_selection(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
    ) -> Vec<String> {
        self.get_allowed_tools_for_selection_with_mode(disabled_skill_ids, selected_skill_ids, None)
            .await
    }

    /// Get allowed tool refs from a selected subset of skills with mode override.
    pub async fn get_allowed_tools_for_selection_with_mode(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> Vec<String> {
        let skills = self
            .list_skills_for_selection(disabled_skill_ids, selected_skill_ids, selected_skill_mode)
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

    /// Get allowed tools from the winner set of a server-resolved session workspace.
    pub async fn get_allowed_tools_for_workspace_selection_with_mode(
        &self,
        workspace: &Path,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
    ) -> SkillResult<Vec<String>> {
        let store = self.store.skill_store_for_workspace(workspace).await?;
        let skills = Self::list_skills_for_selection_from_store(
            store.as_ref(),
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
        )
        .await;
        let mut tools: Vec<String> = skills
            .into_iter()
            .flat_map(|skill| skill.tool_refs)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        tools.sort();
        Ok(tools)
    }

    /// Get allowed tool refs from all skills.
    pub async fn get_allowed_tools(
        &self,
        disabled_skill_ids: &BTreeSet<String>,
        _chat_id: Option<&str>,
    ) -> Vec<String> {
        self.get_allowed_tools_for_selection(disabled_skill_ids, None)
            .await
    }
}

impl Default for SkillManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use tokio::fs;

    use super::{
        filter_disabled_skills, shortlist_skills_for_context, tokenize_request_hint,
        SkillDefinition, SkillManager, SkillStoreConfig, WorkflowStatus,
    };

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

    #[test]
    fn filter_disabled_skills_removes_matching_skill_ids() {
        let skills = vec![
            demo_skill("pdf", "pdf helper"),
            demo_skill("pptx", "ppt helper"),
        ];
        let disabled: BTreeSet<String> = ["pdf".to_string()].into_iter().collect();

        let filtered = filter_disabled_skills(skills, &disabled);
        let ids: Vec<&str> = filtered.iter().map(|skill| skill.id.as_str()).collect();

        assert_eq!(ids, vec!["pptx"]);
    }

    #[tokio::test]
    async fn automatic_selection_honors_builtin_invocation_policy() {
        let directory = tempfile::tempdir().expect("tempdir");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");

        let selected = manager
            .resolve_skills_for_request_with_mode(
                &BTreeSet::new(),
                None,
                None,
                Some("plan the implementation and review the changes"),
            )
            .await;
        let ids = selected
            .iter()
            .map(|skill| skill.id.as_str())
            .collect::<HashSet<_>>();

        assert!(!ids.contains("plan"));
        assert!(ids.contains("review"));

        let empty_selection = Vec::new();
        let selected = manager
            .resolve_skills_for_request_with_mode(
                &BTreeSet::new(),
                Some(&empty_selection),
                None,
                Some("plan the implementation"),
            )
            .await;
        assert!(!selected.iter().any(|skill| skill.id == "plan"));
    }

    #[tokio::test]
    async fn explicit_selection_can_load_manual_only_builtin() {
        let directory = tempfile::tempdir().expect("tempdir");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");
        let selected_ids = vec!["plan".to_string()];

        let selected = manager
            .resolve_skills_for_request_with_mode(
                &BTreeSet::new(),
                Some(&selected_ids),
                None,
                Some("plan the implementation"),
            )
            .await;

        assert_eq!(
            selected
                .iter()
                .map(|skill| skill.id.as_str())
                .collect::<Vec<_>>(),
            vec!["plan"]
        );
    }

    #[tokio::test]
    async fn automatic_selection_keeps_lkg_policy_until_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let skill_dir = skills_dir.join("steady");
        fs::create_dir_all(skill_dir.join("agents"))
            .await
            .expect("skill directory");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: steady\ndescription: Use for steady tasks.\n---\nRetained instructions.\n",
        )
        .await
        .expect("skill definition");
        fs::write(
            skill_dir.join("agents/bamboo.yaml"),
            "version: '1'\ninvocation_policy:\n  explicit: true\n  automatic: true\n",
        )
        .await
        .expect("skill metadata");

        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir,
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");
        let resolve = || async {
            manager
                .resolve_skills_for_request_with_mode(
                    &BTreeSet::new(),
                    None,
                    None,
                    Some("steady task"),
                )
                .await
        };
        assert!(resolve().await.iter().any(|skill| skill.id == "steady"));

        fs::write(
            skill_dir.join("agents/bamboo.yaml"),
            "version: '2'\ninvocation_policy: [\n",
        )
        .await
        .expect("break metadata");
        assert!(resolve().await.iter().any(|skill| skill.id == "steady"));
        let invalid = manager
            .store()
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "steady")
            .expect("retained catalog entry");
        assert_eq!(invalid.status, WorkflowStatus::Invalid);

        fs::write(
            skill_dir.join("agents/bamboo.yaml"),
            "version: '2'\ninvocation_policy:\n  explicit: true\n  automatic: false\n",
        )
        .await
        .expect("recover metadata");
        assert!(!resolve().await.iter().any(|skill| skill.id == "steady"));
        let recovered = manager
            .store()
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "steady")
            .expect("recovered catalog entry");
        assert_eq!(recovered.status, WorkflowStatus::Valid);
    }

    #[tokio::test]
    async fn activation_scope_moves_atomically_between_workspaces_without_stale_revival() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace_a = directory.path().join("a");
        let workspace_b = directory.path().join("b");
        for (workspace, prompt) in [(&workspace_a, "prompt A"), (&workspace_b, "prompt B")] {
            let skill = workspace.join(".bamboo/skills/scope-demo");
            fs::create_dir_all(&skill).await.expect("skill root");
            fs::write(
                skill.join("SKILL.md"),
                format!("---\nname: scope-demo\ndescription: scope\n---\n{prompt}\n"),
            )
            .await
            .expect("skill");
        }
        let manager = std::sync::Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: directory.path().join("data/skills"),
            ..Default::default()
        }));
        manager.initialize().await.expect("initialize");
        let ids = vec!["scope-demo".to_string()];
        for (workspace, expected) in [
            (&workspace_a, "prompt A"),
            (&workspace_b, "prompt B"),
            (&workspace_a, "prompt A"),
        ] {
            let selection = manager
                .resolve_and_pin_activation_in_workspace_with_mode(
                    workspace,
                    "moving-session",
                    &BTreeSet::new(),
                    Some(&ids),
                    None,
                    None,
                )
                .await
                .expect("move activation");
            assert_eq!(selection.skills[0].prompt, expected);
            let mut owners = 0;
            for store in manager.store.cached_workspace_stores().await {
                owners += usize::from(
                    store
                        .activation_descriptor("moving-session")
                        .await
                        .is_some(),
                );
            }
            assert_eq!(owners, 1);
        }

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for workspace in [workspace_a.clone(), workspace_b.clone()] {
            let manager = manager.clone();
            let ids = ids.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                manager
                    .resolve_and_pin_activation_in_workspace_with_mode(
                        &workspace,
                        "concurrent-session",
                        &BTreeSet::new(),
                        Some(&ids),
                        None,
                        None,
                    )
                    .await
                    .expect("concurrent pin");
            }));
        }
        barrier.wait().await;
        for task in tasks {
            task.await.expect("pin task");
        }
        let mut owners = 0;
        for store in manager.store.cached_workspace_stores().await {
            owners += usize::from(
                store
                    .activation_descriptor("concurrent-session")
                    .await
                    .is_some(),
            );
        }
        assert_eq!(owners, 1, "coordinator must prevent duplicate scope owners");
    }
}
