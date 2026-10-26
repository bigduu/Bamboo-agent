//! Host-only bridge to the real cumulative Ticket token ledger. No local wallet
//! or provider execution is created. Construct from a fresh Host authority for
//! each pass; an Authority value cached across a revoke is not a live grant.
use bamboo_tickets::{
    Authority, ExecutionBudgetPurpose, ExecutionTokenUsage, TicketService, WorkTokenAdmission,
    WorkTokenAttempt, WorkTokenBlock, WorkTokenRequest,
};

use super::{
    host, BudgetBlock, BudgetPort, Clock, DiagnosticRequest, ObservationPort, OperationKind,
    PortError, Reservation, Usage,
};

pub struct TicketBudget<'a> {
    pub service: &'a TicketService,
    pub authority: &'a Authority,
    pub clock: &'a dyn Clock,
}

fn request(request: &DiagnosticRequest) -> Result<WorkTokenRequest, PortError> {
    let scope = &request.operation.incident.scope;
    if request.input.scope != *scope
        || !matches!(request.operation.kind, OperationKind::Diagnostic(n) if (1..=8).contains(&n))
        || request.input.input_tokens > request.model.max_input_tokens
        || request.model.max_input_tokens == 0
        || request.model.max_output_tokens == 0
    {
        return Err(PortError::Denied);
    }
    Ok(WorkTokenRequest {
        operation_id: format!(
            "patrol-{}",
            bamboo_tickets::content_hash(
                &bamboo_tickets::canonical_bytes(&request.operation).map_err(host::ticket_error)?
            )
        ),
        project_id: scope.project_id.parse().map_err(|_| PortError::Denied)?,
        work_id: scope.work_id.clone(),
        purpose: ExecutionBudgetPurpose::Patrol,
        request_hash: bamboo_tickets::content_hash(
            &bamboo_tickets::canonical_bytes(request).map_err(host::ticket_error)?,
        ),
        observed_seq: request.input.source_revision,
        input_cap: request.model.max_input_tokens,
        output_cap: request.model.max_output_tokens,
        attempt: Some(WorkTokenAttempt {
            assignment_id: scope.assignment_id.clone(),
            generation: scope.generation,
            authority_epoch: scope.authority_epoch,
        }),
    })
}

impl BudgetPort for TicketBudget<'_> {
    fn reserve(&mut self, diagnostic: &DiagnosticRequest) -> Result<Reservation, BudgetBlock> {
        let scope = &diagnostic.operation.incident.scope;
        let mut source = host::read_observation(self.service, self.authority, scope)
            .map_err(|_| BudgetBlock::Unavailable)?;
        let observation = source
            .observation(scope, 0)
            .map_err(|_| BudgetBlock::Unavailable)?;
        let request = request(diagnostic).map_err(|_| BudgetBlock::Unavailable)?;
        // Exact old receipts reconcile accounting only, never provider permission.
        if let Some(record) = self
            .service
            .work_token_budget(self.authority)
            .map_err(|_| BudgetBlock::Unavailable)?
            .and_then(|budget| budget.reservations.get(&request.operation_id).cloned())
        {
            if record.request != request {
                return Err(BudgetBlock::Unavailable);
            }
            return Ok(Reservation {
                operation: diagnostic.operation.clone(),
                request: Box::new(diagnostic.clone()),
                receipt: record.receipt,
            });
        }
        if diagnostic.input.source_revision != observation.source_revision
            || diagnostic.input.through_cursor != observation.cursor
            || diagnostic.input.since_cursor > diagnostic.input.through_cursor
            || self.clock.now_ms() >= diagnostic.deadline_ms
        {
            return Err(BudgetBlock::Unavailable);
        }
        match self
            .service
            .reserve_work_tokens(self.authority, &request, self.clock.now_ms())
            .map_err(|_| BudgetBlock::Unavailable)?
        {
            WorkTokenAdmission::Reserved(record) => Ok(Reservation {
                operation: diagnostic.operation.clone(),
                request: Box::new(diagnostic.clone()),
                receipt: record.receipt,
            }),
            WorkTokenAdmission::Blocked(block) => Err(match block {
                WorkTokenBlock::Work => BudgetBlock::Work,
                WorkTokenBlock::Daily => BudgetBlock::Daily,
                WorkTokenBlock::Total => BudgetBlock::Total,
                WorkTokenBlock::Capacity => BudgetBlock::Unavailable,
            }),
        }
    }

    fn settle(&mut self, reservation: &Reservation, usage: Usage) -> Result<(), PortError> {
        if reservation.operation != reservation.request.operation {
            return Err(PortError::Denied);
        }
        let request = request(&reservation.request)?;
        // The original Assignment need not remain active. A fresh Runtime proof
        // for this binding may settle its old receipt after restart/Submitted/
        // generation changes, without granting another provider admission.
        let usage = match usage {
            Usage::Known {
                input_tokens,
                output_tokens,
            } => ExecutionTokenUsage::Known {
                input_tokens,
                output_tokens,
            },
            Usage::Unknown => ExecutionTokenUsage::Unknown,
        };
        self.service
            .settle_work_tokens(
                self.authority,
                &request.operation_id,
                &request.request_hash,
                &reservation.receipt,
                usage,
            )
            .map_err(host::ticket_error)?;
        Ok(())
    }
}
