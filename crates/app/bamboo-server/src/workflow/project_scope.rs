//! Match workflow execution catalog scope to durable Session/Project authority.
use std::{path::PathBuf, sync::Arc};

use bamboo_agent_core::workspace_state::WorkspaceResolver;
use bamboo_engine::{project_context::SessionProjectIdentity, WorkflowRunError};
use bamboo_skills::SkillStore;

use super::WorkflowRunAccess;

#[derive(Clone)]
pub(super) struct WorkflowProjectScope {
    projects: Arc<bamboo_projects::ProjectStore>,
    workspace_resolver: WorkspaceResolver,
}

pub(super) struct WorkflowStartContext {
    pub store: Arc<SkillStore>,
    pub workspace_trusted: bool,
    pub automatic_opted_in: bool,
}

impl WorkflowRunAccess {
    pub(crate) fn with_project_scope(
        mut self,
        projects: Arc<bamboo_projects::ProjectStore>,
        workspace_resolver: WorkspaceResolver,
    ) -> Self {
        self.project_scope = Some(WorkflowProjectScope {
            projects,
            workspace_resolver,
        });
        self
    }

    pub(super) async fn definition_start_context(
        &self,
        session_id: &str,
    ) -> Result<WorkflowStartContext, WorkflowRunError> {
        let Some(scope) = self.project_scope.as_ref() else {
            // Standalone engine fixtures retain their prior workspace contract.
            // Production AppState always binds the durable Project context.
            let (workspace, workspace_trusted) = self.session_context(session_id).await?;
            let session = self
                .sessions
                .try_load(session_id)
                .await
                .map_err(|_| {
                    WorkflowRunError::Preflight("workflow session is unavailable".to_string())
                })?
                .ok_or(WorkflowRunError::NotFound)?;
            let store = self
                .skills
                .store_for_workspace(workspace.as_deref())
                .await
                .map_err(|_| {
                    WorkflowRunError::Preflight("workflow catalog is unavailable".to_string())
                })?;
            return Ok(WorkflowStartContext {
                store,
                workspace_trusted,
                automatic_opted_in: automatic_opted_in(&session),
            });
        };
        let _guard = self.sessions.persistence().acquire_lock(session_id).await;
        let session = self
            .sessions
            .storage()
            .load_session(session_id)
            .await
            .map_err(|_| {
                WorkflowRunError::Preflight("workflow session is unavailable".to_string())
            })?
            .ok_or(WorkflowRunError::NotFound)?;
        let resolved =
            resolve_durable_workflow_scope(&session, &scope.projects, &scope.workspace_resolver)?;
        scope.workspace_resolver.publish_resolved_workspace(
            session_id,
            resolved.execution_workspace,
            "workflow_durable_context",
        );
        let store = if let Some(project_id) = resolved.project_id {
            let project_home = scope.projects.paths().project_home(&project_id);
            self.skills
                .store_for_project_workspace(
                    &project_id,
                    &project_home,
                    resolved.catalog_workspace.as_deref(),
                )
                .await
        } else {
            self.skills
                .store_for_workspace(resolved.catalog_workspace.as_deref())
                .await
        }
        .map_err(|_| WorkflowRunError::Preflight("workflow catalog is unavailable".to_string()))?;
        Ok(WorkflowStartContext {
            store,
            workspace_trusted: false,
            automatic_opted_in: automatic_opted_in(&session),
        })
    }
}

/// Resolve durable scope without reading or publishing the mutable runtime
/// workspace registry. Both admission and Agent child construction use this
/// authority; only admission publishes the parent's execution workspace.
pub(super) struct ResolvedWorkflowScope {
    pub project_id: Option<bamboo_domain::ProjectId>,
    pub catalog_workspace: Option<PathBuf>,
    pub execution_workspace: PathBuf,
}

pub(super) fn resolve_durable_workflow_scope(
    session: &bamboo_agent_core::Session,
    projects: &bamboo_projects::ProjectStore,
    workspace_resolver: &WorkspaceResolver,
) -> Result<ResolvedWorkflowScope, WorkflowRunError> {
    let project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            session,
        ) {
            SessionProjectIdentity::Assigned(project_id) => Some(project_id),
            SessionProjectIdentity::Unassigned => None,
            SessionProjectIdentity::Invalid { .. } => {
                return Err(WorkflowRunError::Preflight(
                    "invalid workflow Project identity".to_string(),
                ))
            }
        };
    let persisted_workspace = (session
        .metadata
        .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
        .map(String::as_str)
        != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
    .then(|| {
        session.workspace_path_meta().or_else(|| {
            // Only unassigned legacy sessions may use their durable child
            // workspace field; it cannot repair a missing Project path.
            project_id
                .is_none()
                .then(|| session.workspace.clone())
                .flatten()
        })
    })
    .flatten();
    let project = project_id
        .as_ref()
        .map(|id| projects.get(id))
        .transpose()
        .map_err(|_| {
            WorkflowRunError::Preflight("assigned workflow Project is unavailable".to_string())
        })?;
    let workspace = if persisted_workspace.is_none()
        && project
            .as_ref()
            .is_some_and(|project| project.project_path.is_none())
    {
        None
    } else {
        crate::project_context::validate_workspace_assignment_with_resolver(
            projects,
            project_id.as_ref(),
            persisted_workspace.as_deref(),
            workspace_resolver,
        )
        .map_err(|_| {
            WorkflowRunError::Preflight("invalid workflow Project/workspace assignment".to_string())
        })?
    };
    // Never use a cache-first Session.workspace or stale runtime registry
    // to execute a definition selected from a durable Project snapshot.
    let execution_workspace: PathBuf = workspace
        .clone()
        .or_else(|| workspace_resolver.preview_session_fallback(&session.id))
        .ok_or_else(|| {
            WorkflowRunError::Preflight("workflow execution workspace is unavailable".to_string())
        })?;
    let execution_workspace = workspace_resolver
        .materialize_resolved_workspace(&execution_workspace)
        .map_err(|_| {
            WorkflowRunError::Preflight("workflow execution workspace is unavailable".to_string())
        })?;
    let execution_text = execution_workspace.to_string_lossy();
    let validated_execution_workspace =
        crate::project_context::validate_workspace_assignment_with_resolver(
            projects,
            project_id.as_ref(),
            Some(&execution_text),
            workspace_resolver,
        )
        .map_err(|_| {
            WorkflowRunError::Preflight(
                "invalid workflow execution workspace ownership".to_string(),
            )
        })?
        .ok_or_else(|| {
            WorkflowRunError::Preflight("workflow execution workspace is unavailable".to_string())
        })?;
    Ok(ResolvedWorkflowScope {
        project_id,
        catalog_workspace: workspace,
        execution_workspace: validated_execution_workspace,
    })
}

fn automatic_opted_in(session: &bamboo_agent_core::Session) -> bool {
    session
        .metadata
        .get(bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}
