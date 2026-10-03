//! Deterministic Host adapter. All launches use the existing Child scheduler;
//! no model, worker wait, or second placement policy runs in this adapter.
use std::sync::Arc;

use bamboo_engine::session_app::child_session::{
    create_ticket_child_action, ChildSessionPort, CreateChildInput,
};
use bamboo_engine::ticket_runtime::{self, DispatchObservation, TICKET_DISPATCH_KEY};
use bamboo_engine::ticket_worker_plan::tickets::{
    Authority, DispatchSpec, Error, Principal, Result, TicketService,
};

use super::ChildSessionAdapter;

/// Host configuration, never deserialized from a tool/client request. Dispatch
/// is disabled by default; a configured local worker route is still required.
#[derive(Clone, Debug, Default)]
pub struct TicketDispatchPolicy {
    pub enabled: bool,
    pub worker_role: String,
    pub workspace: String,
}

impl ChildSessionAdapter {
    pub async fn query_ticket_dispatch(
        &self,
        service: &TicketService,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<DispatchObservation> {
        ticket_runtime::query_dispatch(self.storage.as_ref(), service, key, spec).await
    }

    /// Same key/spec returns the canonical Child/Run rather than constructing
    /// another attempt. An uncertain old activation is never enqueued again.
    pub async fn ensure_ticket_dispatch(
        &self,
        service: Arc<TicketService>,
        key: &str,
        spec: &DispatchSpec,
        policy: &TicketDispatchPolicy,
    ) -> Result<DispatchObservation> {
        let dispatch = ticket_runtime::dispatch_binding(&service, key, spec)?;
        let (binding, parent) = ticket_runtime::verified_scope_binding(
            self.storage.as_ref(),
            &dispatch.binding.supervisor_session_id,
        )
        .await?;
        if binding != dispatch.binding {
            return Err(Error::ScopeDenied("Supervisor binding changed".into()));
        }
        if !policy.enabled {
            return Err(Error::AuthorityUnavailable(
                "Ticket dispatch feature is disabled".into(),
            ));
        }
        if policy.worker_role.is_empty() || policy.workspace.is_empty() {
            return Err(Error::InvalidTransition(
                "Ticket dispatch requires a configured local worker and workspace".into(),
            ));
        }
        self.scheduler.set_ticket_service(Some(service.clone()));
        let child_id = ticket_runtime::ticket_child_id(key);
        let guard = self.scheduler.lock_child_launch(&child_id).await;
        match self.query_ticket_dispatch(&service, key, spec).await? {
            DispatchObservation::Missing => {
                ticket_runtime::require_dispatch_permission(&service, &dispatch)?;
                if let Some(workspace) = &spec.workspace {
                    bamboo_engine::ticket_worker_plan::files::verify_workspace(workspace.clone())
                        .await?;
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
                let mut metadata = self.resolve_runtime_metadata(&policy.worker_role).await;
                metadata.insert(
                    TICKET_DISPATCH_KEY.into(),
                    serde_json::to_string(&dispatch)?,
                );
                create_ticket_child_action(
                    self,
                    CreateChildInput {
                        parent_session: parent.clone(),
                        child_id: child_id.clone(),
                        title: spec.contract.title.clone(),
                        responsibility: spec.contract.objective.clone(),
                        assignment_prompt: String::new(),
                        subagent_type: policy.worker_role.clone(),
                        workspace: spec
                            .workspace
                            .as_ref()
                            .map_or_else(|| policy.workspace.clone(), |w| w.worktree.clone()),
                        workspace_source: bamboo_engine::project_context::WorkspaceSource::Explicit,
                        model_override: None,
                        model_ref_override: None,
                        runtime_metadata: metadata,
                        read_only: false,
                        auto_run: false,
                        reasoning_effort: None,
                        lifecycle: None,
                        resident_name: None,
                        resident_context: None,
                        disabled_tools: None,
                        context_fork: None,
                    },
                    &service,
                    &Authority::from_verified_host(binding, Principal::Runtime),
                    &spec.assignment_id,
                )
                .await
                .map_err(|e| Error::InvalidTransition(e.to_string()))?;
            }
            DispatchObservation::Pending { .. } => {
                ticket_runtime::require_dispatch_permission(&service, &dispatch)?;
            }
            completed => return Ok(completed),
        }
        let child = self
            .storage
            .load_session(&child_id)
            .await?
            .ok_or_else(|| Error::AuthorityUnavailable("prepared Child missing".into()))?;
        // The scheduler obtains this same guard for its exact-generation
        // reservation. Release before enqueue and retain the durable generation.
        drop(guard);
        self.admit_child_run(&parent, &child, None)
            .await
            .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
        self.query_ticket_dispatch(&service, key, spec).await
    }
}
