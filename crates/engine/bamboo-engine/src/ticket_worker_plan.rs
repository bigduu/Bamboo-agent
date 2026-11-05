//! Trusted Runtime Task adapter. Session TaskList is only a private display/cache
//! projection; the scope TicketService LocalPlan remains the sole authority.
use bamboo_domain::{Session, SessionKind, TaskItemStatus, TaskList};
use bamboo_tickets::{
    Authority, CommandSource, Error, LocalStep, Operation, Principal, Result, StepStatus,
    TicketService,
};
use bamboo_tools::TaskTool;
use std::sync::Arc;

pub const TICKET_LOCAL_PLAN_KEY: &str = "ticket.local_plan.v1";
pub use bamboo_tickets as tickets;
pub const TICKET_PLAN_PACKET_KEY: &str = "ticket.local_plan.packet.v1";
pub const TICKET_QUESTION_YIELD_KEY: &str = "ticket.worker.question_yield.v1";
pub const TICKET_PLAN_ACTION: &str = "_ticket_local_plan_v1";
pub mod files;
pub mod remote;

/// This host-installed port is never constructed from model tool arguments.
#[async_trait::async_trait]
pub trait WorkerLocalPlan: Send + Sync {
    fn run_id(&self) -> &str;
    fn bind_session(&self, session: &mut Session) -> Result<()>;
    /// A Host-installed execution capability, never inferred from metadata.
    fn workspace_project_context(
        &self,
        _session: &Session,
        _workspace: Option<&std::path::Path>,
    ) -> Result<Option<bamboo_tickets::ProjectContextSnapshot>> {
        Ok(None)
    }
    async fn apply_task(
        &self,
        session: &mut Session,
        call_id: &str,
        args: &serde_json::Value,
    ) -> Result<TaskList>;
}

pub struct TicketWorkerPlan {
    service: Arc<TicketService>,
    authority: Authority,
    assignment_id: String,
    session_id: String,
    supervisor_session_id: String,
    run_id: String,
    session_birth: std::sync::OnceLock<WorkSessionIdentity>,
}

#[derive(PartialEq, Eq)]
struct WorkSessionIdentity {
    birth: chrono::DateTime<chrono::Utc>,
    parent_birth: Option<chrono::DateTime<chrono::Utc>>,
    root: String,
    project: crate::project_context::SessionProjectIdentity,
    authority: bamboo_domain::SessionAuthorityIdentity,
}

impl WorkSessionIdentity {
    fn from_session(session: &Session) -> Self {
        Self {
            birth: session.created_at,
            parent_birth: session.parent_created_at,
            root: session.root_session_id.clone(),
            project: crate::project_context::ProjectContextResolver::session_project_identity(
                session,
            ),
            authority: session.authority_identity.clone(),
        }
    }
}

impl TicketWorkerPlan {
    /// The caller must obtain the receipt from Runtime, never Worker arguments.
    pub fn from_runtime_receipt(service: Arc<TicketService>, assignment_id: &str) -> Result<Self> {
        let plan = Self::from_receipt_identity(service, assignment_id)?;
        plan.service.authorize_tool(&plan.authority, "Task")?;
        Ok(plan)
    }

    // Identity alone permits only a cached exact-input reply. New commands and
    // bootstrap reads still require the current live execution permit.
    fn from_receipt_identity(service: Arc<TicketService>, assignment_id: &str) -> Result<Self> {
        let (_, snapshot) = service.published()?;
        let assignment = snapshot
            .assignments
            .get(assignment_id)
            .ok_or_else(|| Error::InvalidTransition("assignment missing".into()))?;
        let receipt = assignment
            .runtime
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("Runtime admission not confirmed".into()))?;
        let authority = Authority::from_verified_host(
            snapshot.binding.clone(),
            Principal::Worker {
                assignment_id: assignment_id.into(),
                generation: assignment.generation,
                run_id: receipt.run_id.clone(),
                session_id: receipt.session_id.clone(),
            },
        );
        Ok(Self {
            service,
            authority,
            assignment_id: assignment_id.into(),
            session_id: receipt.session_id.clone(),
            supervisor_session_id: snapshot.binding.supervisor_session_id.clone(),
            run_id: receipt.run_id.clone(),
            session_birth: std::sync::OnceLock::new(),
        })
    }

    /// Install only after the trusted host confirms the exact Run/Session receipt.
    pub fn bind_session(&self, session: &mut Session) -> Result<()> {
        self.service.authorize_tool(&self.authority, "Task")?;
        self.require_session_identity(session)?;
        let identity = WorkSessionIdentity::from_session(session);
        if self
            .session_birth
            .get_or_init(|| WorkSessionIdentity::from_session(session))
            != &identity
        {
            return Err(Error::ScopeDenied("LocalPlan Child birth changed".into()));
        }
        self.project_session(session)
    }

    fn require_session_identity(&self, session: &Session) -> Result<()> {
        if session.kind != SessionKind::Child
            || session.id != self.session_id
            || session.parent_session_id.as_deref() != Some(&self.supervisor_session_id)
            || session
                .agent_runtime_state
                .as_ref()
                .is_none_or(|s| s.run_id != self.run_id)
        {
            return Err(Error::ScopeDenied(
                "LocalPlan requires its exact Child Run".into(),
            ));
        }
        Ok(())
    }

    fn project_workspace_context(
        &self,
        session: &Session,
        workspace: Option<&std::path::Path>,
    ) -> Result<Option<bamboo_tickets::ProjectContextSnapshot>> {
        use crate::ticket_runtime;
        self.service.authorize_tool(&self.authority, "Task")?;
        self.require_session_identity(session)?;
        let identity = WorkSessionIdentity::from_session(session);
        if identity.parent_birth.is_none()
            || identity.root != self.supervisor_session_id
            || self.session_birth.get() != Some(&identity)
        {
            return Err(Error::ScopeDenied(
                "Work workspace requires its bound Child birth".into(),
            ));
        }
        let dispatch = ticket_runtime::read_dispatch(session)?
            .ok_or_else(|| Error::ScopeDenied("Work dispatch binding missing".into()))?;
        ticket_runtime::require_dispatch_permission(&self.service, &dispatch)?;
        let (_, snapshot) = self.service.published()?;
        let assignment = &snapshot.assignments[&self.assignment_id];
        let intent = snapshot
            .intents
            .get(&assignment.dispatch_key)
            .ok_or_else(|| Error::ScopeDenied("Work dispatch intent missing".into()))?;
        if dispatch.assignment_id != self.assignment_id
            || dispatch.receipt.as_ref() != assignment.runtime.as_ref()
            || session.id != ticket_runtime::ticket_child_id(&assignment.dispatch_key)
            || intent.spec_hash
                != bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(
                    &intent.immutable_spec,
                )?)
        {
            return Err(Error::ScopeDenied(
                "Work workspace execution identity changed".into(),
            ));
        }
        let Some(expected) = intent.immutable_spec.workspace.as_ref() else {
            return Ok(None);
        };
        let expected_path = std::path::Path::new(&expected.worktree);
        if workspace
            .map(std::path::Path::canonicalize)
            .transpose()?
            .as_deref()
            != Some(expected_path)
            || session.workspace_path_meta().as_deref() != Some(expected.worktree.as_str())
        {
            return Err(Error::ScopeDenied(
                "Work workspace differs from its exact dispatch".into(),
            ));
        }
        let context_hash = snapshot
            .project
            .assignment_contexts
            .get(&self.assignment_id);
        if context_hash != intent.immutable_spec.project_context_hash.as_ref() {
            return Err(Error::ScopeDenied(
                "Work Project context binding changed".into(),
            ));
        }
        let context = context_hash
            .map(|hash| {
                snapshot
                    .project
                    .contexts
                    .get(hash)
                    .cloned()
                    .ok_or_else(|| Error::ScopeDenied("Work Project context missing".into()))
            })
            .transpose()?;
        let hash = context
            .as_ref()
            .map(|context| {
                bamboo_tickets::canonical_bytes(context)
                    .map(|bytes| bamboo_tickets::content_hash(&bytes))
            })
            .transpose()?;
        if hash != intent.immutable_spec.project_context_hash
            || context.as_ref().is_some_and(|context| {
                snapshot.project.links.get(&assignment.work_id) != Some(&context.project_id)
                    || context.workspace_binding.path != expected.repo
            })
        {
            return Err(Error::ScopeDenied(
                "Work Project context differs from its dispatch".into(),
            ));
        }
        Ok(context)
    }

    fn project_session(&self, session: &mut Session) -> Result<()> {
        self.require_session_identity(session)?;
        let (_, snapshot) = self.service.published()?;
        let assignment = &snapshot.assignments[&self.assignment_id];
        session.task_list = (!assignment.plan.steps.is_empty()).then(|| TaskList {
            session_id: self.session_id.clone(),
            title: format!(
                "Local plan: {}",
                snapshot.tickets[&assignment.work_id].contract.title
            ),
            items: assignment
                .plan
                .steps
                .iter()
                .map(|s| bamboo_domain::TaskItem {
                    id: s.id.clone(),
                    description: s.title.clone(),
                    parent_id: s.parent.clone(),
                    status: match s.status {
                        Some(StepStatus::InProgress) => TaskItemStatus::InProgress,
                        Some(StepStatus::Blocked) => TaskItemStatus::Blocked,
                        Some(StepStatus::Completed) => TaskItemStatus::Completed,
                        _ if s.completed => TaskItemStatus::Completed,
                        _ => TaskItemStatus::Pending,
                    },
                    ..Default::default()
                })
                .collect(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        });
        session.set_task_list_version_meta(assignment.plan.plan_revision.to_string());
        session
            .metadata
            .insert(TICKET_LOCAL_PLAN_KEY.into(), self.assignment_id.clone());
        if let Some(request) = assignment
            .awaiting_request
            .as_ref()
            .and_then(|id| snapshot.requests.get(id))
        {
            session.metadata.insert(
                TICKET_QUESTION_YIELD_KEY.into(),
                serde_json::to_string(request)?,
            );
        }
        Ok(())
    }

    pub fn apply_task(
        &self,
        session: &mut Session,
        tool_call_id: &str,
        args: &serde_json::Value,
    ) -> Result<TaskList> {
        if session.kind != SessionKind::Child
            || session.id != self.session_id
            || session.parent_session_id.as_deref() != Some(&self.supervisor_session_id)
            || tool_call_id.is_empty()
            || tool_call_id.len() > 128
            || session.metadata.get(TICKET_LOCAL_PLAN_KEY) != Some(&self.assignment_id)
            || session
                .agent_runtime_state
                .as_ref()
                .is_none_or(|s| s.run_id != self.run_id)
        {
            return Err(Error::ScopeDenied(
                "LocalPlan execution identity changed".into(),
            ));
        }
        let (_, snapshot) = self.service.published()?;
        let assignment = &snapshot.assignments[&self.assignment_id];
        let op_id = format!("worker-plan/{}/{tool_call_id}", assignment.dispatch_key);
        let source = CommandSource::WorkerTask {
            arguments: args.clone(),
        };
        // Replay before parsing against the current plan: retained item IDs and
        // titles may have changed since this exact call was first committed.
        if let Some(command) =
            self.service
                .replay_source_command(&self.authority, &op_id, &source)?
        {
            self.service.execute(&self.authority, &command)?;
            self.project_session(session)?;
            return session.task_list.clone().ok_or_else(|| {
                Error::InvalidTransition("Task produced an empty authoritative plan".into())
            });
        }
        self.service.authorize_tool(&self.authority, "Task")?;
        let question = question_prompt(args)?;
        let task_list = TaskTool::task_list_from_args_with_existing(
            args,
            &self.session_id,
            session.task_list.as_ref(),
            None,
        )
        .map_err(|e| Error::InvalidTransition(e.to_string()))?;
        let steps = task_list
            .items
            .iter()
            .map(|item| LocalStep {
                id: item.id.clone(),
                parent: item.parent_id.clone(),
                title: item.description.clone(),
                completed: item.status == TaskItemStatus::Completed,
                status: Some(match item.status {
                    TaskItemStatus::Pending => StepStatus::Pending,
                    TaskItemStatus::InProgress => StepStatus::InProgress,
                    TaskItemStatus::Completed => StepStatus::Completed,
                    TaskItemStatus::Blocked => StepStatus::Blocked,
                }),
            })
            .collect();
        let operation = Operation::ReplacePlan {
            assignment_id: self.assignment_id.clone(),
            expected_plan_revision: assignment.plan.plan_revision,
            steps,
        };
        let mut operations = vec![operation];
        if let Some(prompt) = question {
            let packet =
                self.service
                    .child_context_packet(&self.authority, &self.assignment_id, 65536)?;
            let packet_hash =
                bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&packet)?);
            operations.push(Operation::Ask {
                work_id: assignment.work_id.clone(),
                temp_id: "question".into(),
                prompt,
                action: None,
            });
            operations.push(Operation::YieldForInput {
                assignment_id: self.assignment_id.clone(),
                request_id: "question".into(),
                packet_hash,
            });
        }
        let command =
            self.service
                .prepare_source_command(&self.authority, &op_id, operations, source)?;
        self.service.execute(&self.authority, &command)?;
        // Receipt replay may occur after a newer plan write. Never project the
        // old call's payload over the current authoritative plan.
        self.project_session(session)?;
        session.task_list.clone().ok_or_else(|| {
            Error::InvalidTransition("Task produced an empty authoritative plan".into())
        })
    }
}

fn question_prompt(args: &serde_json::Value) -> Result<Option<String>> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Question {
        prompt: String,
    }
    let Some(raw) = args.get("question") else {
        return Ok(None);
    };
    let q: Question = serde_json::from_value(raw.clone())?;
    if q.prompt.trim().is_empty() || q.prompt.len() > 2048 {
        return Err(Error::InvalidTransition(
            "question prompt must be 1..2048 UTF-8 bytes".into(),
        ));
    }
    Ok(Some(q.prompt))
}

#[cfg(test)]
pub(crate) mod tests;

#[async_trait::async_trait]
impl WorkerLocalPlan for TicketWorkerPlan {
    fn run_id(&self) -> &str {
        &self.run_id
    }
    fn bind_session(&self, session: &mut Session) -> Result<()> {
        self.bind_session(session)
    }
    fn workspace_project_context(
        &self,
        session: &Session,
        workspace: Option<&std::path::Path>,
    ) -> Result<Option<bamboo_tickets::ProjectContextSnapshot>> {
        self.project_workspace_context(session, workspace)
    }
    async fn apply_task(
        &self,
        session: &mut Session,
        call_id: &str,
        args: &serde_json::Value,
    ) -> Result<TaskList> {
        self.apply_task(session, call_id, args)
    }
}
