//! Fresh Host resource observations after queue capacity is reserved.
//! These checks do not grant execution or persist new retry/assignment facts.
use super::*;

impl TicketApplication {
    pub(super) fn validate_project_context(
        &self,
        project_id: &bamboo_domain::ProjectId,
        context: Option<&ProjectContextSnapshot>,
        adopting: bool,
    ) -> Result<()> {
        let store = bamboo_projects::ProjectStore::open(self.root.clone())
            .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
        let manifest = store
            .get(project_id)
            .map_err(|e| Error::InvalidTransition(e.to_string()))?;
        if manifest.status != bamboo_domain::ProjectStatus::Active {
            return Err(Error::InvalidTransition("Project is archived".into()));
        }
        if let Some(context) = context {
            if (adopting && context.project_revision != manifest.revision)
                || context.resource_revision != manifest.resource_revision
                || !manifest
                    .workspace_roots()
                    .any(|p| p == context.workspace_binding.path)
            {
                return Err(Error::RevisionConflict);
            }
            if let Some(binding) = manifest
                .workspace_bindings
                .iter()
                .find(|b| b.path == context.workspace_binding.path)
            {
                if binding != &context.workspace_binding {
                    return Err(Error::RevisionConflict);
                }
            } else if context.workspace_binding.git_common_dir.is_some()
                || context.workspace_binding.label.is_some()
            {
                return Err(Error::InvalidTransition(
                    "primary binding has no auxiliary metadata".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_dispatch_project_context(
        &self,
        snapshot: &Snapshot,
        spec: &DispatchSpec,
    ) -> Result<()> {
        match (
            spec.project_context_hash.as_ref(),
            snapshot.project.links.get(&spec.work_id),
        ) {
            (None, None) => {
                if snapshot
                    .project
                    .assignment_contexts
                    .contains_key(&spec.assignment_id)
                {
                    return Err(Error::IdempotencyConflict);
                }
            }
            (Some(hash), Some(project_id)) => {
                if snapshot
                    .project
                    .assignment_contexts
                    .get(&spec.assignment_id)
                    != Some(hash)
                {
                    return Err(Error::IdempotencyConflict);
                }
                let context = snapshot.project.contexts.get(hash).ok_or_else(|| {
                    Error::InvalidTransition("adopted Project context is missing".into())
                })?;
                if &context.project_id != project_id
                    || context.contract_revision != spec.contract_revision
                    || context.contract != spec.contract
                {
                    return Err(Error::IdempotencyConflict);
                }
                self.validate_project_context(project_id, Some(context), false)?;
            }
            _ => return Err(Error::IdempotencyConflict),
        }
        Ok(())
    }

    pub(super) async fn revalidate_ticket_dispatch_admission(
        &self,
        service: &TicketService,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<()> {
        // Capacity is already held. Every new or recovered Pending admission
        // observes its original Git identity, not a workspace from a prompt.
        if let Some(workspace) = &spec.workspace {
            bamboo_engine::ticket_worker_plan::files::verify_workspace(workspace.clone()).await?;
        } else if spec
            .contract
            .allowed_tools
            .iter()
            .any(|tool| matches!(tool.as_str(), "Read" | "Write"))
        {
            return Err(Error::ScopeDenied(
                "Ticket file tools require an explicit isolated Git worktree".into(),
            ));
        }
        self.preflight_ticket_starts(vec![None]).await?;
        {
            let config = self.config.read().await;
            if !config.features.ticket_dispatch || !config.features.ticket_mutation {
                return Err(Error::AuthorityUnavailable(
                    "Ticket scheduling features are disabled".into(),
                ));
            }
        }
        // Re-read the canonical Supervisor after the asynchronous checks. No
        // observed context or process-local policy is allowed to substitute it.
        let (current, _) = self.authority(Principal::Runtime).await?;
        if current.published()?.1.binding != service.published()?.1.binding {
            return Err(Error::ScopeDenied(
                "Supervisor scope binding changed".into(),
            ));
        }
        let snapshot = current.published()?.1;
        self.validate_dispatch_project_context(&snapshot, spec)?;
        let dispatch = ticket_runtime::dispatch_binding(&current, key, spec)?;
        ticket_runtime::require_dispatch_permission(&current, &dispatch)
    }
}
