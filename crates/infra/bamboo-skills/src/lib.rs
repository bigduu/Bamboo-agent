//! Agent skill management (re-exported from bamboo-agent-skill crate).

pub mod access_control;
pub mod activation;
pub mod catalog;
pub mod clone_publication;
pub mod context;
pub mod legacy;
pub mod resource_helpers;
pub mod runtime_metadata;
pub mod selection;
pub mod session_port;
pub mod store;
pub mod types;

pub use activation::*;
pub use catalog::{
    LegacyWorkflowMigrationStatus, ShadowedWorkflowCandidate, WorkflowCatalogEntry,
    WorkflowCatalogEvent, WorkflowCatalogEventKind, WorkflowCatalogSnapshot, WorkflowKind,
    WorkflowSource, WorkflowStatus,
};
pub use store::{
    SkillActivationDescriptor, SkillActivationSnapshot, SkillActivationSnapshotEntry, SkillStore,
    SkillUpdate,
};
pub use types::*;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

pub const DEFAULT_WORKFLOW_CATALOG_MAX_CHARS: usize = 10_240;
pub const DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS: usize = 128_000;
const ESTIMATED_CHARS_PER_TOKEN: usize = 4;
const MAX_COMPRESSED_DESCRIPTION_CHARS: usize = 180;

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

fn budget_skills_for_context(
    skills: Vec<SkillDefinition>,
    catalog: &WorkflowCatalogSnapshot,
    request_hint: Option<&str>,
    max_context_tokens: usize,
) -> (
    Vec<SkillDefinition>,
    Vec<WorkflowCatalogEntry>,
    WorkflowCatalogDiagnostic,
) {
    let entries_by_id = catalog
        .entries
        .iter()
        .filter(|entry| entry.winner)
        .map(|entry| (entry.id.as_str(), entry))
        .collect::<HashMap<_, _>>();
    let total_candidates = skills.len();
    let char_budget = DEFAULT_WORKFLOW_CATALOG_MAX_CHARS.min(
        max_context_tokens
            .saturating_mul(2)
            .saturating_div(100)
            .saturating_mul(ESTIMATED_CHARS_PER_TOKEN),
    );
    let char_budget = char_budget.max(1);
    let token_budget = char_budget / ESTIMATED_CHARS_PER_TOKEN;
    let static_chars = context::workflow_catalog_prefix().chars().count();
    const DIAGNOSTIC_RESERVE_CHARS: usize = 512;
    if char_budget < static_chars.saturating_add(DIAGNOSTIC_RESERVE_CHARS) {
        return (
            Vec::new(),
            Vec::new(),
            WorkflowCatalogDiagnostic {
                total_candidates,
                advertised_candidates: 0,
                initial_chars: 0,
                final_chars: 0,
                char_budget,
                token_budget,
                compressed_descriptions: false,
                shortlisted: total_candidates > 0,
                omitted_ids: skills.into_iter().map(|skill| skill.id).collect(),
            },
        );
    }
    let initial_chars = skills
        .iter()
        .filter_map(|skill| {
            entries_by_id
                .get(skill.id.as_str())
                .map(|entry| (skill, *entry))
        })
        .map(|(skill, entry)| {
            context::render_workflow_catalog_entry(skill, entry)
                .chars()
                .count()
        })
        .sum::<usize>()
        .saturating_add(static_chars)
        .saturating_add(DIAGNOSTIC_RESERVE_CHARS);
    let compressed_descriptions = initial_chars > char_budget;

    let hint_tokens = request_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(tokenize_request_hint)
        .unwrap_or_default();

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
    let mut used = static_chars.saturating_add(DIAGNOSTIC_RESERVE_CHARS);
    for (_, mut skill) in ranked {
        let Some(entry) = entries_by_id.get(skill.id.as_str()).copied() else {
            continue;
        };
        if compressed_descriptions
            && skill.description.chars().count() > MAX_COMPRESSED_DESCRIPTION_CHARS
        {
            skill.description = format!(
                "{}…",
                skill
                    .description
                    .chars()
                    .take(MAX_COMPRESSED_DESCRIPTION_CHARS.saturating_sub(1))
                    .collect::<String>()
            );
        }
        let cost = context::render_workflow_catalog_entry(&skill, entry)
            .chars()
            .count();
        if used.saturating_add(cost) > char_budget {
            continue;
        }
        used = used.saturating_add(cost);
        selected.push(skill);
    }
    let selected_ids = selected
        .iter()
        .map(|skill| skill.id.as_str())
        .collect::<HashSet<_>>();
    let selected_entries = selected
        .iter()
        .filter_map(|skill| {
            entries_by_id
                .get(skill.id.as_str())
                .map(|entry| (*entry).clone())
        })
        .collect::<Vec<_>>();
    let omitted_ids = catalog
        .entries
        .iter()
        .filter(|entry| entry.winner && !selected_ids.contains(entry.id.as_str()))
        .map(|entry| entry.id.clone())
        .collect::<Vec<_>>();
    let selected_len = selected.len();
    let mut diagnostic = WorkflowCatalogDiagnostic {
        total_candidates,
        advertised_candidates: selected_len,
        initial_chars,
        final_chars: 0,
        char_budget,
        token_budget,
        compressed_descriptions,
        shortlisted: selected_len < total_candidates,
        omitted_ids,
    };
    diagnostic.final_chars =
        context::build_workflow_catalog_context(&selected, &selected_entries, &diagnostic)
            .chars()
            .count();
    debug_assert!(diagnostic.final_chars <= diagnostic.char_budget);
    (selected, selected_entries, diagnostic)
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
    pub catalog_entries: Vec<WorkflowCatalogEntry>,
    pub catalog_diagnostic: WorkflowCatalogDiagnostic,
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

    /// Resolve the stable Project shared layer plus the current workspace
    /// overlay. Both values must come from trusted server-side session/Project
    /// state, never directly from request arguments.
    pub async fn store_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
    ) -> SkillResult<Arc<SkillStore>> {
        self.store
            .skill_store_for_project_workspace(project_id, project_home, workspace)
            .await
    }

    pub async fn workflow_catalog_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
    ) -> SkillResult<WorkflowCatalogSnapshot> {
        Ok(self
            .store_for_project_workspace(project_id, project_home, workspace)
            .await?
            .workflow_catalog_snapshot()
            .await)
    }

    pub async fn pin_workflow_definition_bundle_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
        root_id: &str,
        root_revision: u64,
    ) -> SkillResult<bamboo_domain::WorkflowDefinitionBundle> {
        self.store_for_project_workspace(project_id, project_home, workspace)
            .await?
            .pin_workflow_definition_bundle(root_id, root_revision)
            .await
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
            // An explicitly supplied empty selection is a deactivation, not a
            // request to silently fall back to automatic candidates.
            return Vec::new();
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
        max_context_tokens: usize,
    ) -> SkillResult<SkillActivationSelection> {
        // All four values are cloned under the store's publication read lock.
        // A watcher may publish after this await, but this activation only consumes
        // these correlated generation-N clones and can never mix them with N+1.
        let (skills, roots, resources, catalog) = store
            .activation_source_for_mode(selected_skill_mode)
            .await?;
        let selected = Self::filter_skills_for_selection(
            skills,
            &catalog,
            disabled_skill_ids,
            selected_skill_ids,
        );
        let (selected, catalog_entries, catalog_diagnostic) = if selected_skill_ids.is_none() {
            budget_skills_for_context(selected, &catalog, request_hint, max_context_tokens)
        } else {
            let selected_ids = selected
                .iter()
                .map(|skill| skill.id.as_str())
                .collect::<HashSet<_>>();
            let entries = catalog
                .entries
                .iter()
                .filter(|entry| entry.winner && selected_ids.contains(entry.id.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            let chars = selected
                .iter()
                .map(|skill| skill.description.len() + skill.name.len() + skill.id.len() + 160)
                .sum();
            let count = selected.len();
            (
                selected,
                entries,
                WorkflowCatalogDiagnostic {
                    total_candidates: count,
                    advertised_candidates: count,
                    initial_chars: chars,
                    final_chars: chars,
                    char_budget: DEFAULT_WORKFLOW_CATALOG_MAX_CHARS,
                    token_budget: DEFAULT_WORKFLOW_CATALOG_MAX_CHARS / ESTIMATED_CHARS_PER_TOKEN,
                    compressed_descriptions: false,
                    shortlisted: false,
                    omitted_ids: Vec::new(),
                },
            )
        };
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
            catalog_entries,
            catalog_diagnostic,
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
            skills = budget_skills_for_context(
                skills,
                &self.store.skill_catalog_snapshot().await,
                request_hint,
                DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .0;
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
        self.resolve_and_pin_activation_for_request_with_mode_and_budget(
            activation_id,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            request_hint,
            DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        )
        .await
    }

    pub async fn resolve_and_pin_activation_for_request_with_mode_and_budget(
        &self,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
        max_context_tokens: usize,
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
            max_context_tokens,
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
            let catalog = store.skill_catalog_snapshot().await;
            skills = budget_skills_for_context(
                skills,
                &catalog,
                request_hint,
                DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .0;
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
        self.resolve_and_pin_activation_in_workspace_with_mode_and_budget(
            workspace,
            activation_id,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            request_hint,
            DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_and_pin_activation_in_workspace_with_mode_and_budget(
        &self,
        workspace: &Path,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
        max_context_tokens: usize,
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
            max_context_tokens,
        )
        .await
    }

    /// Resolve and pin one immutable activation from the stable Project-home
    /// publication plus the current workspace overlay.
    ///
    /// The Project identity and paths must be resolved from trusted
    /// server-side session state before calling this method.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_and_pin_activation_in_project_workspace_with_mode_and_budget(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
        selected_skill_ids: Option<&[String]>,
        selected_skill_mode: Option<&str>,
        request_hint: Option<&str>,
        max_context_tokens: usize,
    ) -> SkillResult<SkillActivationSelection> {
        let store = self
            .store_for_project_workspace(project_id, project_home, workspace)
            .await?;
        let _scope_guard = self.activation_scope_coordinator.lock().await;
        self.prepare_activation_scope(activation_id, &store).await;
        Self::resolve_and_pin_activation_from_store(
            store.as_ref(),
            activation_id,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            request_hint,
            max_context_tokens,
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

    pub async fn pin_current_activation_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
        activation_id: &str,
        selected_skill_ids: &[String],
        selected_skill_mode: Option<&str>,
    ) -> SkillResult<SkillActivationDescriptor> {
        let store = self
            .store_for_project_workspace(project_id, project_home, workspace)
            .await?;
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

    pub async fn pinned_allowed_tools_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
        activation_id: &str,
        disabled_skill_ids: &BTreeSet<String>,
    ) -> SkillResult<Option<Vec<String>>> {
        let store = self
            .store_for_project_workspace(project_id, project_home, workspace)
            .await?;
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
        Ok(Self::pinned_activation_from_store(store.as_ref(), activation_id).await)
    }

    pub async fn pinned_activation_for_project_workspace(
        &self,
        project_id: &bamboo_domain::ProjectId,
        project_home: &Path,
        workspace: Option<&Path>,
        activation_id: &str,
    ) -> SkillResult<Option<SkillActivationSelection>> {
        let store = self
            .store_for_project_workspace(project_id, project_home, workspace)
            .await?;
        Ok(Self::pinned_activation_from_store(store.as_ref(), activation_id).await)
    }

    async fn pinned_activation_from_store(
        store: &SkillStore,
        activation_id: &str,
    ) -> Option<SkillActivationSelection> {
        let pinned = store.pinned_activation_skills(activation_id).await;
        let catalog_entries = store.pinned_activation_catalog_entries(activation_id).await;
        pinned
            .zip(catalog_entries)
            .map(|((skills, descriptor), catalog_entries)| {
                let chars = skills
                    .iter()
                    .map(|skill| skill.id.len() + skill.name.len() + skill.description.len() + 160)
                    .sum();
                SkillActivationSelection {
                    skills,
                    catalog_entries,
                    catalog_diagnostic: WorkflowCatalogDiagnostic {
                        total_candidates: descriptor.skill_revisions.len(),
                        advertised_candidates: descriptor.skill_revisions.len(),
                        initial_chars: chars,
                        final_chars: chars,
                        char_budget: DEFAULT_WORKFLOW_CATALOG_MAX_CHARS,
                        token_budget: DEFAULT_WORKFLOW_CATALOG_MAX_CHARS
                            / ESTIMATED_CHARS_PER_TOKEN,
                        compressed_descriptions: false,
                        shortlisted: false,
                        omitted_ids: Vec::new(),
                    },
                    descriptor,
                }
            })
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
        budget_skills_for_context, filter_disabled_skills, tokenize_request_hint, SkillDefinition,
        SkillManager, SkillStoreConfig, WorkflowCatalogEntry, WorkflowCatalogSnapshot,
        WorkflowKind, WorkflowSource, WorkflowStatus, DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
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

        let catalog = WorkflowCatalogSnapshot {
            revision: 1,
            entries: skills
                .iter()
                .map(|skill| WorkflowCatalogEntry {
                    id: skill.id.clone(),
                    name: skill.name.clone(),
                    description: skill.description.clone(),
                    kind: WorkflowKind::Instruction,
                    source: WorkflowSource::User,
                    revision: 1,
                    content_digest: String::new(),
                    version: "1".to_string(),
                    invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
                    argument_schema: serde_json::json!({"type": "object"}),
                    status: WorkflowStatus::Valid,
                    legacy: false,
                    migration_status: None,
                    last_error: None,
                    winner: true,
                    shadowed_candidates: Vec::new(),
                })
                .collect(),
        };
        let (shortlisted, _, diagnostic) = budget_skills_for_context(
            skills,
            &catalog,
            Some("optimize react vite build"),
            DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        );
        assert!(shortlisted
            .iter()
            .any(|skill| skill.id == "react-optimizer"));
        assert!(diagnostic.final_chars <= diagnostic.char_budget);
    }

    #[test]
    fn workflow_catalog_never_exceeds_two_percent_of_tiny_context() {
        let skill = demo_skill("large", &"description ".repeat(2_000));
        let catalog = WorkflowCatalogSnapshot {
            revision: 1,
            entries: vec![WorkflowCatalogEntry {
                id: skill.id.clone(),
                name: skill.name.clone(),
                description: skill.description.clone(),
                kind: WorkflowKind::Instruction,
                source: WorkflowSource::User,
                revision: 1,
                content_digest: String::new(),
                version: "1".to_string(),
                invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
                argument_schema: serde_json::json!({"type": "object"}),
                status: WorkflowStatus::Valid,
                legacy: false,
                migration_status: None,
                last_error: None,
                winner: true,
                shadowed_candidates: Vec::new(),
            }],
        };
        let (skills, entries, diagnostic) =
            budget_skills_for_context(vec![skill], &catalog, None, 100);
        let rendered =
            crate::context::build_workflow_catalog_context(&skills, &entries, &diagnostic);
        assert!(diagnostic.char_budget <= 8);
        assert!(rendered.chars().count() <= diagnostic.char_budget);
        assert_eq!(diagnostic.final_chars, rendered.chars().count());
        assert!(diagnostic.shortlisted);
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
    async fn workflows_never_enter_automatic_or_explicit_skill_activation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let orchestration = data.join("skills/orchestrate");
        fs::create_dir_all(&orchestration)
            .await
            .expect("orchestration root");
        fs::write(
            orchestration.join("SKILL.md"),
            "---\nname: orchestrate\ndescription: Runs a workflow.\n---\nWorkflow support text.\n",
        )
        .await
        .expect("workflow instructions");
        fs::write(
            orchestration.join("workflow.yaml"),
            "id: orchestrate\nname: Orchestrate\ndescription: Runs tools\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("workflow metadata");
        let workflows = data.join("workflows");
        fs::create_dir_all(&workflows)
            .await
            .expect("legacy workflows");
        fs::write(
            workflows.join("legacy-review.md"),
            "---\ndescription: Reviews a legacy change.\n---\nReview it.\n",
        )
        .await
        .expect("legacy workflow");

        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");

        let automatic = manager
            .resolve_skills_for_request_with_mode(
                &BTreeSet::new(),
                None,
                None,
                Some("orchestrate and review"),
            )
            .await;
        for id in ["orchestrate", "legacy-review"] {
            assert!(automatic.iter().all(|skill| skill.id != id));
            let explicit_ids = vec![id.to_string()];
            let explicit = manager
                .resolve_skills_for_request_with_mode(
                    &BTreeSet::new(),
                    Some(&explicit_ids),
                    None,
                    Some("run it"),
                )
                .await;
            assert!(explicit.iter().all(|skill| skill.id != id));
        }
        let skill_catalog = manager.store().skill_catalog_snapshot().await;
        assert!(skill_catalog
            .entries
            .iter()
            .all(|entry| !matches!(entry.id.as_str(), "orchestrate" | "legacy-review")));
        let workflow_catalog = manager.store().workflow_catalog_snapshot().await;
        assert!(workflow_catalog
            .entries
            .iter()
            .any(|entry| entry.id == "orchestrate"));
        assert!(workflow_catalog
            .entries
            .iter()
            .any(|entry| entry.id == "legacy-review"));
    }

    #[tokio::test]
    async fn explicit_legacy_migration_is_a_selectable_skill_not_a_workflow_replacement() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data = directory.path().join("data");
        let source = data.join("workflows/migrated-review.md");
        fs::create_dir_all(source.parent().unwrap())
            .await
            .expect("workflow root");
        fs::write(&source, "Review the migrated change.\n")
            .await
            .expect("legacy source");
        crate::legacy::migrate_legacy_markdown_workflow(
            &source,
            "workflows/migrated-review.md",
            &data.join("skills"),
            "migrated-review",
            Some("Use when reviewing a migrated change."),
        )
        .await
        .expect("explicit migration");
        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: data.join("skills"),
            ..Default::default()
        });
        manager.initialize().await.expect("initialize manager");

        let ids = vec!["migrated-review".to_string()];
        let explicit = manager
            .resolve_skills_for_request_with_mode(
                &BTreeSet::new(),
                Some(&ids),
                None,
                Some("review"),
            )
            .await;
        assert_eq!(
            explicit
                .iter()
                .map(|skill| skill.id.as_str())
                .collect::<Vec<_>>(),
            vec!["migrated-review"]
        );
        assert!(manager
            .store()
            .skill_catalog_snapshot()
            .await
            .entries
            .iter()
            .any(|entry| {
                entry.id == "migrated-review"
                    && entry.migration_status
                        == Some(crate::LegacyWorkflowMigrationStatus::Migrated)
            }));
        assert!(manager
            .store()
            .workflow_catalog_snapshot()
            .await
            .entries
            .iter()
            .any(|entry| {
                entry.id == "migrated-review"
                    && entry.migration_status
                        == Some(crate::LegacyWorkflowMigrationStatus::Available)
            }));
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
            .skill_catalog_snapshot()
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
            .skill_catalog_snapshot()
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

    #[tokio::test]
    async fn project_home_and_workspace_are_distinct_overlay_sources() {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_skills = directory.path().join("data/skills");
        let project_home = directory.path().join("projects/project-1");
        let workspace = directory.path().join("workspace");
        let project_skill = project_home.join("skills/shared");
        let workspace_skill = workspace.join(".bamboo/skills/shared");
        fs::create_dir_all(&project_skill)
            .await
            .expect("project skill root");
        fs::create_dir_all(&workspace_skill)
            .await
            .expect("workspace skill root");
        fs::write(
            project_skill.join("SKILL.md"),
            "---\nname: shared\ndescription: project shared\n---\nProject prompt.\n",
        )
        .await
        .expect("project skill");
        fs::write(
            workspace_skill.join("SKILL.md"),
            "---\nname: shared\ndescription: workspace overlay\n---\nWorkspace prompt.\n",
        )
        .await
        .expect("workspace skill");

        let manager = SkillManager::with_config(SkillStoreConfig {
            skills_dir: data_skills,
            ..Default::default()
        });
        manager.initialize().await.expect("initialize");
        let project_id = bamboo_domain::ProjectId::parse("project-1").expect("project id");
        let project_only = manager
            .store_for_project_workspace(&project_id, &project_home, None)
            .await
            .expect("project store")
            .skill_catalog_snapshot()
            .await;
        assert_eq!(
            project_only
                .entries
                .iter()
                .find(|entry| entry.id == "shared")
                .expect("project entry")
                .source,
            WorkflowSource::Project
        );

        let overlaid = manager
            .store_for_project_workspace(&project_id, &project_home, Some(&workspace))
            .await
            .expect("overlay store")
            .skill_catalog_snapshot()
            .await;
        let winner = overlaid
            .entries
            .iter()
            .find(|entry| entry.id == "shared")
            .expect("overlay entry");
        assert_eq!(winner.source, WorkflowSource::Workspace);
        assert_eq!(winner.shadowed_candidates.len(), 1);
        assert_eq!(
            winner.shadowed_candidates[0].source,
            WorkflowSource::Project
        );

        let selected = vec!["shared".to_string()];
        let project_activation = manager
            .resolve_and_pin_activation_in_project_workspace_with_mode_and_budget(
                &project_id,
                &project_home,
                None,
                "project-activation",
                &BTreeSet::new(),
                Some(&selected),
                None,
                Some("shared"),
                DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await
            .expect("project activation");
        assert_eq!(project_activation.skills[0].description, "project shared");
        assert_eq!(
            project_activation.catalog_entries[0].source,
            WorkflowSource::Project
        );

        let workspace_activation = manager
            .resolve_and_pin_activation_in_project_workspace_with_mode_and_budget(
                &project_id,
                &project_home,
                Some(&workspace),
                "workspace-activation",
                &BTreeSet::new(),
                Some(&selected),
                None,
                Some("shared"),
                DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await
            .expect("workspace activation");
        assert_eq!(
            workspace_activation.skills[0].description,
            "workspace overlay"
        );
        assert_eq!(
            workspace_activation.catalog_entries[0].source,
            WorkflowSource::Workspace
        );
    }
}
