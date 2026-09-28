//! Durable Root tool authority, separate from prompt or permission suggestions.

use super::{model_context::ModelContextResetReason, Session, SessionKind};

/// Root product policy, independent of a model call's `ReasoningEffort`.
/// Ultra delegates planning/execution and keeps verification at the Root.
/// It is projected from existing durable Root authority, never persisted twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RootThinkingMode {
    Standard,
    Ultra,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("thinking_mode and root orchestration enabled selection conflict")]
pub struct RootThinkingModeConflict;

impl RootThinkingMode {
    /// Optional DTO field: omission is allowed, explicit null is not a mode.
    pub fn deserialize_selection<'de, D>(deserializer: D) -> Result<Option<Self>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Self as serde::Deserialize>::deserialize(deserializer).map(Some)
    }

    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Ultra
        } else {
            Self::Standard
        }
    }

    pub fn enabled(self) -> bool {
        self == Self::Ultra
    }

    /// Normalize canonical and legacy selectors before existing admission.
    pub fn resolve_selection(
        mode: Option<Self>,
        enabled: Option<bool>,
    ) -> Result<Option<bool>, RootThinkingModeConflict> {
        if let (Some(mode), Some(enabled)) = (mode, enabled) {
            if mode.enabled() != enabled {
                return Err(RootThinkingModeConflict);
            }
        }
        Ok(mode.map(Self::enabled).or(enabled))
    }
}

/// Exact execution identities admitted for an orchestration-only Root.
/// Unknown tools, external providers, and legacy aliases do not enter this
/// list; callers first resolve a registered execution identity.
pub const ROOT_ORCHESTRATION_TOOLS: [&str; 9] = [
    "SubAgent",
    "Plan",
    "Task",
    "session_history_current",
    "Read",
    "Grep",
    "Glob",
    "GetFileInfo",
    "ViewImage",
];

pub fn orchestration_only_allows_execution_name(execution_name: &str) -> bool {
    ROOT_ORCHESTRATION_TOOLS.contains(&execution_name)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RootToolAuthorityError {
    #[error("only a Root Session may select orchestration-only tool authority")]
    NotRoot,
    #[error("Root tool authority revision cannot advance")]
    RevisionOverflow,
    #[error("Root tool authority snapshot is stale or inconsistent")]
    StaleSnapshot,
    #[error("orchestration-only Root cannot enter while legacy Plan mode is active")]
    LegacyPlanActive,
    #[error("orchestration-only Root cannot use a selected Skill or Workflow")]
    WorkflowSelected,
}

impl Session {
    pub fn root_thinking_mode(&self) -> RootThinkingMode {
        RootThinkingMode::from_enabled(self.root_orchestration_only_enabled())
    }

    pub fn root_orchestration_only_enabled(&self) -> bool {
        self.kind == SessionKind::Root
            && self.parent_session_id.is_none()
            && self.root_orchestration_only
    }

    /// Called only by a host-owned selection, never by a model tool.
    pub fn set_root_orchestration_only(
        &mut self,
        enabled: bool,
    ) -> Result<(), RootToolAuthorityError> {
        if self.kind != SessionKind::Root || self.parent_session_id.is_some() {
            return Err(RootToolAuthorityError::NotRoot);
        }
        if enabled {
            self.validate_root_orchestration_compatibility()?;
        }
        if self.root_orchestration_only == enabled {
            return Ok(());
        }
        let next = self
            .root_tool_authority_revision
            .checked_add(1)
            .ok_or(RootToolAuthorityError::RevisionOverflow)?;
        self.root_orchestration_only = enabled;
        self.root_tool_authority_revision = next;
        if self.model_context_state.is_some() {
            self.reset_model_context_epoch(ModelContextResetReason::CacheScopeChanged);
        }
        Ok(())
    }

    /// Keep the narrow tool catalog usable when an old turn or a new request
    /// carries a mode that requires tools outside the Root allowlist.
    pub fn validate_root_orchestration_compatibility(&self) -> Result<(), RootToolAuthorityError> {
        if self
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.plan_mode.is_some())
        {
            return Err(RootToolAuthorityError::LegacyPlanActive);
        }
        if self.selected_skill_ids().is_some_and(|ids| !ids.is_empty()) {
            return Err(RootToolAuthorityError::WorkflowSelected);
        }
        Ok(())
    }

    /// Defense in depth at both catalog projection and final dispatch.
    pub fn allows_model_tool_execution(&self, execution_name: &str) -> bool {
        !self.root_orchestration_only_enabled()
            || orchestration_only_allows_execution_name(execution_name)
    }

    /// Refresh a running snapshot from the durable control plane before tool
    /// dispatch. An older snapshot can never loosen a selected Root policy.
    pub fn adopt_root_tool_authority_from(
        &mut self,
        latest: &Session,
    ) -> Result<(), RootToolAuthorityError> {
        // A tool policy revision is meaningful only within the same Session
        // lifetime. A deleted Root ID can be recreated with a fresh birth, so
        // matching the ID and revision alone would authorize an old SDK run.
        fn root_id(session: &Session) -> &str {
            if session.kind == SessionKind::Root && session.root_session_id.is_empty() {
                session.id.as_str()
            } else {
                session.root_session_id.as_str()
            }
        }
        if self.id != latest.id
            || self.kind != latest.kind
            || self.created_at != latest.created_at
            || self.authority_identity != latest.authority_identity
            || self.parent_session_id != latest.parent_session_id
            || root_id(self) != root_id(latest)
            || self.spawn_depth != latest.spawn_depth
            || (self.root_orchestration_only && self.root_tool_authority_revision == 0)
            || (latest.root_orchestration_only && latest.root_tool_authority_revision == 0)
            || latest.root_tool_authority_revision < self.root_tool_authority_revision
            || (latest.root_tool_authority_revision == self.root_tool_authority_revision
                && latest.root_orchestration_only != self.root_orchestration_only)
            || latest.root_mode_transition_epoch < self.root_mode_transition_epoch
            || (latest.root_mode_transition_epoch == self.root_mode_transition_epoch
                && latest.root_mode_operations != self.root_mode_operations)
        {
            return Err(RootToolAuthorityError::StaleSnapshot);
        }
        let changed = self.root_orchestration_only != latest.root_orchestration_only;
        self.root_orchestration_only = latest.root_orchestration_only;
        self.root_tool_authority_revision = latest.root_tool_authority_revision;
        self.root_mode_transition_epoch = latest.root_mode_transition_epoch;
        self.root_mode_operations = latest.root_mode_operations.clone();
        if changed && self.model_context_state.is_some() {
            self.reset_model_context_epoch(ModelContextResetReason::CacheScopeChanged);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{AgentRuntimeState, ModelContextState, PlanModeState, PlanModeStatus};

    #[test]
    fn thinking_mode_is_a_derived_root_policy_independent_of_effort() {
        use crate::ReasoningEffort;
        let mut root = Session::new("ultra-root", "model");
        root.reasoning_effort = Some(ReasoningEffort::Max);
        root.set_root_orchestration_prompt_enabled(true);
        assert_eq!(root.root_thinking_mode(), RootThinkingMode::Standard);
        root.set_root_orchestration_only(true).unwrap();
        let persisted = serde_json::to_value(&root).unwrap();
        assert!(
            persisted.get("thinking_mode").is_none(),
            "no second persisted authority"
        );
        let reloaded: Session = serde_json::from_value(persisted.clone()).unwrap();
        assert_eq!(reloaded.root_thinking_mode(), RootThinkingMode::Ultra);
        assert_eq!(reloaded.reasoning_effort, Some(ReasoningEffort::Max));
        assert_eq!(
            serde_json::to_value(&reloaded).unwrap(),
            persisted,
            "projection never writes"
        );
        let mut child = Session::new_child_of("ultra-child", &reloaded, "model", "child");
        assert_eq!(child.root_thinking_mode(), RootThinkingMode::Standard);
        assert_eq!(child.reasoning_effort, None);
        child.root_orchestration_only = true;
        assert_eq!(
            child.root_thinking_mode(),
            RootThinkingMode::Standard,
            "raw child bool has no authority"
        );
        assert_eq!(
            child.set_root_orchestration_only(true),
            Err(RootToolAuthorityError::NotRoot)
        );
        assert_eq!(
            serde_json::to_string(&RootThinkingMode::Ultra).unwrap(),
            "\"ultra\""
        );
        assert_eq!(
            serde_json::from_str::<RootThinkingMode>("\"standard\"").unwrap(),
            RootThinkingMode::Standard
        );
        assert!(serde_json::from_str::<RootThinkingMode>("\"max\"").is_err());
        assert!(serde_json::from_str::<ReasoningEffort>("\"ultra\"").is_err());
    }

    #[test]
    fn canonical_and_legacy_thinking_selectors_share_one_boolean_request() {
        assert_eq!(RootThinkingMode::resolve_selection(None, None), Ok(None));
        for enabled in [false, true] {
            let mode = RootThinkingMode::from_enabled(enabled);
            assert_eq!(
                RootThinkingMode::resolve_selection(Some(mode), None),
                Ok(Some(enabled))
            );
            assert_eq!(
                RootThinkingMode::resolve_selection(None, Some(enabled)),
                Ok(Some(enabled))
            );
            assert_eq!(
                RootThinkingMode::resolve_selection(Some(mode), Some(enabled)),
                Ok(Some(enabled))
            );
            assert_eq!(
                RootThinkingMode::resolve_selection(Some(mode), Some(!enabled)),
                Err(RootThinkingModeConflict)
            );
        }
    }

    #[test]
    fn root_selection_is_persistent_and_child_does_not_inherit_it() {
        let mut root = Session::new("root", "model");
        root.set_root_orchestration_only(true).unwrap();
        assert_eq!(root.root_tool_authority_revision, 1);
        let reloaded: Session =
            serde_json::from_str(&serde_json::to_string(&root).unwrap()).unwrap();
        assert!(reloaded.root_orchestration_only_enabled());
        let mut child = Session::new_child_of("child", &reloaded, "model", "child");
        assert!(!child.root_orchestration_only_enabled());
        assert_eq!(child.root_tool_authority_revision, 0);
        assert_eq!(
            child.set_root_orchestration_only(false),
            Err(RootToolAuthorityError::NotRoot)
        );
        assert!(child.allows_model_tool_execution("Bash"));
    }

    #[test]
    fn root_allowlist_is_exact_and_denies_unknown_tools() {
        let mut root = Session::new("root", "model");
        root.set_root_orchestration_only(true).unwrap();
        for tool in ROOT_ORCHESTRATION_TOOLS {
            assert!(root.allows_model_tool_execution(tool), "{tool}");
        }
        for tool in [
            "Bash",
            "Edit",
            "Write",
            "WebFetch",
            "workflow_run",
            "mcp__x__read",
            "read_file",
            "default::Read",
        ] {
            assert!(!root.allows_model_tool_execution(tool), "{tool}");
        }
        assert_eq!(root.root_tool_authority_revision, 1);
        root.set_root_orchestration_only(true).unwrap();
        assert_eq!(root.root_tool_authority_revision, 1);
        root.set_root_orchestration_only(false).unwrap();
        assert_eq!(root.root_tool_authority_revision, 2);
        assert!(root.allows_model_tool_execution("Bash"));
    }

    #[test]
    fn durable_refresh_adopts_tightening_and_rejects_stale_loosen() {
        let mut running = Session::new("root", "model");
        let mut durable = running.clone();
        durable.set_root_orchestration_only(true).unwrap();
        running.adopt_root_tool_authority_from(&durable).unwrap();
        assert!(!running.allows_model_tool_execution("Bash"));
        assert_eq!(
            durable.adopt_root_tool_authority_from(&Session::new("root", "model")),
            Err(RootToolAuthorityError::StaleSnapshot)
        );
        durable.set_root_orchestration_only(false).unwrap();
        running.adopt_root_tool_authority_from(&durable).unwrap();
        assert!(running.allows_model_tool_execution("Bash"));
    }

    #[test]
    fn durable_refresh_rejects_a_different_birth_identity_or_lineage() {
        let running = Session::new("root", "model");
        let mut other_birth = running.clone();
        other_birth.created_at += chrono::Duration::microseconds(1);
        assert_eq!(
            running.clone().adopt_root_tool_authority_from(&other_birth),
            Err(RootToolAuthorityError::StaleSnapshot)
        );

        let mut other_identity = running.clone();
        other_identity.authority_identity = super::super::SessionAuthorityIdentity::Supervisor {
            incarnation_id: uuid::Uuid::new_v4(),
        };
        assert_eq!(
            running
                .clone()
                .adopt_root_tool_authority_from(&other_identity),
            Err(RootToolAuthorityError::StaleSnapshot)
        );

        let mut other_lineage = running.clone();
        other_lineage.root_session_id = "another-root".into();
        assert_eq!(
            running
                .clone()
                .adopt_root_tool_authority_from(&other_lineage),
            Err(RootToolAuthorityError::StaleSnapshot)
        );
        other_lineage = running.clone();
        other_lineage.spawn_depth = 1;
        assert_eq!(
            running
                .clone()
                .adopt_root_tool_authority_from(&other_lineage),
            Err(RootToolAuthorityError::StaleSnapshot)
        );

        // Legacy Roots can omit this derived field without changing lineage.
        let mut legacy = running.clone();
        legacy.root_session_id.clear();
        legacy.adopt_root_tool_authority_from(&running).unwrap();
    }

    #[test]
    fn root_selection_rejects_legacy_plan_or_workflow_that_needs_other_tools() {
        let mut root = Session::new("root", "model");
        let runtime = AgentRuntimeState {
            plan_mode: Some(PlanModeState {
                entered_at: chrono::Utc::now(),
                pre_permission_mode: "default".to_string(),
                plan_file_path: None,
                status: PlanModeStatus::Exploring,
            }),
            ..AgentRuntimeState::default()
        };
        root.agent_runtime_state = Some(runtime);
        assert_eq!(
            root.set_root_orchestration_only(true),
            Err(RootToolAuthorityError::LegacyPlanActive)
        );
        root.agent_runtime_state = None;
        root.set_selected_skill_ids(vec!["workflow".to_string()]);
        assert_eq!(
            root.set_root_orchestration_only(true),
            Err(RootToolAuthorityError::WorkflowSelected)
        );
        root.clear_selected_skill_ids();
        root.set_root_orchestration_only(true).unwrap();
        root.set_selected_skill_ids(vec!["workflow".to_string()]);
        assert_eq!(
            root.validate_root_orchestration_compatibility(),
            Err(RootToolAuthorityError::WorkflowSelected)
        );
    }

    #[test]
    fn live_authority_adoption_resets_prior_model_context_epoch() {
        for enabled in [false, true] {
            let mut running = Session::new("root", "model");
            if !enabled {
                running.set_root_orchestration_only(true).unwrap();
            }
            running.model_context_state = Some(ModelContextState {
                prefix_epoch: 4,
                cache_scope_sha256: Some("prior-provider-scope".to_string()),
                ..ModelContextState::default()
            });
            let mut durable = running.clone();
            durable.set_root_orchestration_only(enabled).unwrap();
            running.adopt_root_tool_authority_from(&durable).unwrap();
            let context = running.model_context_state.as_ref().unwrap();
            assert_eq!(context.prefix_epoch, 5);
            assert!(context.cache_scope_sha256.is_none());
            assert_eq!(
                context.last_reset_reason,
                Some(ModelContextResetReason::CacheScopeChanged)
            );
        }
    }
}
