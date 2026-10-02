//! Trusted Runtime Task adapter. Session TaskList is only a private display/cache
//! projection; the scope TicketService LocalPlan remains the sole authority.
use bamboo_domain::{Session, SessionKind, TaskItemStatus, TaskList};
use bamboo_tickets::{
    Authority, Error, LocalStep, Operation, Principal, Result, StepStatus, TicketService,
};
use bamboo_tools::TaskTool;
use std::sync::Arc;

pub const TICKET_LOCAL_PLAN_KEY: &str = "ticket.local_plan.v1";

pub struct TicketWorkerPlan {
    service: Arc<TicketService>,
    authority: Authority,
    assignment_id: String,
    session_id: String,
    run_id: String,
}

impl TicketWorkerPlan {
    /// The caller must obtain the receipt from Runtime, never Worker arguments.
    pub fn from_runtime_receipt(service: Arc<TicketService>, assignment_id: &str) -> Result<Self> {
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
        service.authorize_tool(&authority, "Task")?;
        Ok(Self {
            service,
            authority,
            assignment_id: assignment_id.into(),
            session_id: receipt.session_id.clone(),
            run_id: receipt.run_id.clone(),
        })
    }

    /// Install only after the trusted host confirms the exact Run/Session receipt.
    pub fn bind_session(&self, session: &mut Session) -> Result<()> {
        self.service.authorize_tool(&self.authority, "Task")?;
        if session.kind != SessionKind::Child
            || session.id != self.session_id
            || session
                .agent_runtime_state
                .as_ref()
                .is_none_or(|s| s.run_id != self.run_id)
        {
            return Err(Error::ScopeDenied(
                "LocalPlan requires its exact Child Run".into(),
            ));
        }
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
        self.service.authorize_tool(&self.authority, "Task")?;
        let (_, snapshot) = self.service.published()?;
        let assignment = &snapshot.assignments[&self.assignment_id];
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
        let op_id = format!("worker-plan/{}/{tool_call_id}", assignment.dispatch_key);
        let operation = Operation::ReplacePlan {
            assignment_id: self.assignment_id.clone(),
            expected_plan_revision: assignment.plan.plan_revision,
            steps,
        };
        // A repeated call must retain the original expected plan revision too.
        let operations = if let Some(receipt) = snapshot.receipts.get(&op_id) {
            let original: bamboo_tickets::Command =
                serde_json::from_str(&receipt.canonical_request)?;
            let Some(Operation::ReplacePlan {
                expected_plan_revision,
                ..
            }) = original.operations.first()
            else {
                return Err(Error::IdempotencyConflict);
            };
            match operation {
                Operation::ReplacePlan {
                    assignment_id,
                    steps,
                    ..
                } => vec![Operation::ReplacePlan {
                    assignment_id,
                    expected_plan_revision: *expected_plan_revision,
                    steps,
                }],
                _ => unreachable!(),
            }
        } else {
            vec![operation]
        };
        let command = self
            .service
            .prepare_command(&self.authority, &op_id, operations)?;
        self.service.execute(&self.authority, &command)?;
        // Receipt replay may occur after a newer plan write. Never project the
        // old call's payload over the current authoritative plan.
        self.bind_session(session)?;
        session.task_list.clone().ok_or_else(|| {
            Error::InvalidTransition("Task produced an empty authoritative plan".into())
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
