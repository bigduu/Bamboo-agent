//! Owned native execution for a Work assignment in an independent Root.
use std::sync::{Arc, OnceLock, Weak};

use bamboo_domain::{Session, SessionInboxReceipt, SupervisorCurrentUserTarget};
use bamboo_orchestration::managed_work;
use bamboo_tickets::{DispatchSpec, Error, Result, RuntimeReceipt, TicketService};
use bamboo_tools::{DrainedLocalTaskResources, LocalTaskResources};
use tokio::sync::RwLock;

use crate::runtime::{ExecuteRequest, ModelRoster};
use crate::ticket_worker_plan::{self, files::LocalFileExecutor, TicketWorkerPlan};
use crate::Agent;

use super::{dispatch_binding, read_dispatch, require_dispatch_permission, TicketDispatchBinding};

/// The server supplies its existing Inbox, live config and Ticket authority.
pub struct ManagedWorkRuntime {
    pub service: Weak<OnceLock<Arc<TicketService>>>,
    pub inbox: Arc<bamboo_storage::FileSessionInbox>,
    pub config: Arc<RwLock<bamboo_llm::Config>>,
    pub projects: Arc<dyn crate::project_context::ProjectContextSource>,
}

pub(crate) struct ManagedWorkRun {
    service: Arc<TicketService>,
    spec: DispatchSpec,
    dispatch: TicketDispatchBinding,
    receipt: RuntimeReceipt,
    pub resources: LocalTaskResources,
}

impl ManagedWorkRuntime {
    fn service(&self) -> Result<Arc<TicketService>> {
        self.service
            .upgrade()
            .and_then(|slot| slot.get().cloned())
            .ok_or_else(|| Error::AuthorityUnavailable("managed Work ledger unavailable".into()))
    }

    pub(crate) fn require_launch_permission(&self, session: &Session) -> Result<()> {
        let service = self.service()?;
        let dispatch = read_dispatch(session)?
            .ok_or_else(|| Error::ScopeDenied("managed Work dispatch missing".into()))?;
        require_dispatch_permission(&service, &dispatch)
    }

    pub(crate) async fn admit(
        &self,
        agent: &Agent,
        session: &mut Session,
        run_id: &str,
        roster: &ModelRoster,
    ) -> Result<ManagedWorkRun> {
        let service = self.service()?;
        let dispatch = read_dispatch(session)?
            .ok_or_else(|| Error::ScopeDenied("managed Work dispatch missing".into()))?;
        let (_, snapshot) = service.published()?;
        let intent = snapshot
            .intents
            .get(&dispatch.dispatch_key)
            .ok_or_else(|| Error::ScopeDenied("managed Work intent missing".into()))?;
        let spec = &intent.immutable_spec;
        let target = spec
            .managed_root
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("Work has no managed Root reservation".into()))?;
        let matches = |session: &Session| {
            managed_work::matches_root(target, spec, session)
                && read_dispatch(session).ok().flatten().as_ref() == Some(&dispatch)
        };
        if !matches(session)
            || dispatch != dispatch_binding(&service, &dispatch.dispatch_key, spec)?
            || intent.receipt.is_some()
            || roster.provider_name.as_deref() != Some(target.model.provider.as_str())
            || roster.model.as_deref() != Some(target.model.model.as_str())
            || target.model.reasoning_effort.is_some()
                && session.reasoning_effort != target.model.reasoning_effort
        {
            return Err(Error::ScopeDenied(
                "managed Work lifetime, admission, or model changed".into(),
            ));
        }
        let writer = agent.persistence().root_actor_writer().ok_or_else(|| {
            Error::ScopeDenied("managed Work requires its exact Root writer".into())
        })?;
        if writer.created_at != session.created_at
            || writer.fence.run_id != run_id
            || writer.fence.actor_id.as_str() != session.id
        {
            return Err(Error::ScopeDenied(
                "managed Work Actor ownership changed".into(),
            ));
        }
        require_dispatch_permission(&service, &dispatch)?;
        let workspace = spec
            .workspace
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("managed Work workspace missing".into()))?;
        ticket_worker_plan::files::verify_workspace(workspace.clone()).await?;
        let current_project = self
            .projects
            .workspace_facts(
                &target.project_id,
                std::path::Path::new(&workspace.worktree),
            )
            .await
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        let current_project = current_project.filter(|project| {
            project.status == bamboo_domain::ProjectStatus::Active
                && project.owner.as_ref() == Some(&target.project_id)
                && project.project_id == target.project_id
        });
        let Some(current_project) = current_project else {
            return Err(Error::ScopeDenied(
                "managed Work Project/worktree binding changed".into(),
            ));
        };
        if spec.project_context_hash.as_ref().is_some_and(|hash| {
            snapshot.project.contexts.get(hash).is_none_or(|context| {
                context.project_id != target.project_id
                    || context.resource_revision != current_project.resource_revision
            })
        }) {
            return Err(Error::ScopeDenied(
                "managed Work Project context changed".into(),
            ));
        }
        let expected = target
            .input_id(&session.id)
            .ok_or_else(|| Error::ScopeDenied("managed Work first input missing".into()))?;
        let admitted = agent
            .persistence()
            .admit_managed_work_input(session, self.inbox.clone(), run_id, &expected)
            .await?;
        if let Some(reason) = admitted.admission_error {
            return Err(Error::AuthorityUnavailable(reason));
        }
        if admitted.committed_messages.len() != 1
            || admitted.committed_messages[0].id != expected.as_str()
        {
            return Err(Error::ScopeDenied(
                "managed Work input was not newly admitted by this Run".into(),
            ));
        }
        let generation = session
            .session_inbox_admission()
            .ok_or_else(|| Error::ScopeDenied("managed Work input cursor missing".into()))?
            .last_admitted_sequence;
        let canonical = agent
            .storage()
            .load_runtime_control_plane(&session.id)
            .await?
            .filter(|root| root.created_at == writer.created_at && matches(root))
            .ok_or_else(|| {
                Error::ScopeDenied("canonical Work Root changed during admission".into())
            })?;
        let root = SupervisorCurrentUserTarget::capture(&canonical)
            .map_err(|error| Error::ScopeDenied(error.into()))?;
        let supervisor = target
            .supervisor()
            .ok_or_else(|| Error::ScopeDenied("managed Work source missing".into()))?;
        let proof = self
            .inbox
            .read_managed_root_admission(
                &target.caller,
                &supervisor,
                &root,
                &SessionInboxReceipt {
                    id: expected,
                    generation,
                },
            )
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?
            .filter(|proof| proof.run_id == run_id)
            .ok_or_else(|| {
                Error::ScopeDenied("managed Work input ACK has no exact Run owner".into())
            })?;
        let config = self.config.read().await;
        if !config.features.ticket_dispatch || !config.features.ticket_mutation {
            return Err(Error::ScopeDenied(
                "managed Work dispatch is disabled".into(),
            ));
        }
        let receipt = managed_work::admit(&service, &dispatch.dispatch_key, spec, session, proof)?;
        Ok(ManagedWorkRun {
            service,
            spec: spec.clone(),
            dispatch,
            receipt,
            resources: LocalTaskResources::new(&session.id, run_id, session.created_at),
        })
    }
}

impl ManagedWorkRun {
    pub(crate) fn bind_request(
        &self,
        agent: &Agent,
        session: &mut Session,
        request: &mut ExecuteRequest,
    ) -> Result<()> {
        let plan = Arc::new(TicketWorkerPlan::from_runtime_receipt(
            self.service.clone(),
            &self.spec.assignment_id,
        )?);
        session
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .run_id = self.receipt.run_id.clone();
        session.metadata.insert(
            ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
            self.spec.assignment_id.clone(),
        );
        plan.bind_session(session)?;
        let tools = request
            .tools
            .take()
            .unwrap_or_else(|| agent.default_tools().clone());
        let names: Vec<_> = self.spec.contract.allowed_tools.iter().cloned().collect();
        request.tools = Some(Arc::new(LocalFileExecutor::new(
            tools,
            self.service.clone(),
            session.clone(),
            self.receipt.run_id.clone(),
            &names,
        )?));
        request.ticket_worker_plan = Some(plan);
        request.ticket_token_budget = Some(
            crate::ticket_token_budget::NativeWorkTokenBudget::managed_root(
                self.service.clone(),
                agent.storage().clone(),
                session,
                self.dispatch.clone(),
                self.receipt.clone(),
            ),
        );
        // The Run uses the contract's tool surface. Ordinary UI skill
        // selection or a Session resume cannot expand that grant.
        request.selected_skill_ids = Some(Vec::new());
        request.selected_skill_mode = None;
        Ok(())
    }

    pub(crate) async fn publish_joined(
        self,
        storage: &dyn bamboo_domain::Storage,
        drained: DrainedLocalTaskResources,
    ) -> Result<()> {
        let identity = drained.identity();
        if identity != self.resources.identity() {
            return Err(Error::ScopeDenied(
                "managed Work stop identity changed".into(),
            ));
        }
        let canonical = storage
            .load_execution_admission_snapshot(&self.receipt.session_id)
            .await?
            .filter(|session| session.created_at == identity.child_birth)
            .ok_or_else(|| Error::ScopeDenied("completed Work Root lifetime changed".into()))?;
        if read_dispatch(&canonical)?.as_ref() != Some(&self.dispatch) {
            return Err(Error::ScopeDenied("completed Work dispatch changed".into()));
        }
        managed_work::publish_stopped(&self.service, &self.receipt, &canonical)
    }
}
