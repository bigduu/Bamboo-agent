//! Host-only bridge to the real cumulative Ticket token ledger. No local wallet
//! or provider execution is created. Construct from a fresh Host authority for
//! each pass; an Authority value cached across a revoke is not a live grant.
use bamboo_tickets::{
    Authority, ExecutionBudgetPurpose, ExecutionTokenOverCapUsage, ExecutionTokenUsage,
    TicketService, WorkTokenAdmission, WorkTokenAttempt, WorkTokenBlock, WorkTokenRequest,
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

fn request(request: &DiagnosticRequest, observed_seq: u64) -> Result<WorkTokenRequest, PortError> {
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
        observed_seq,
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
        let now = self.clock.now_ms();
        if observation.terminal
            || observation.execution_unknown
            || diagnostic.input.source_revision != observation.source_revision
            || diagnostic.input.through_cursor > observation.cursor
            || diagnostic.input.since_cursor > diagnostic.input.through_cursor
            || now >= diagnostic.deadline_ms
        {
            return Err(BudgetBlock::Unavailable);
        }
        let snapshot = self
            .service
            .published()
            .map_err(|_| BudgetBlock::Unavailable)?
            .1;
        if snapshot.execution_facts_revision() != diagnostic.input.source_revision {
            return Err(BudgetBlock::Unavailable);
        }
        let mut request =
            request(diagnostic, snapshot.seq).map_err(|_| BudgetBlock::Unavailable)?;
        let budget = snapshot
            .execution_budget
            .as_ref()
            .ok_or(BudgetBlock::Unavailable)?;
        if let Some(record) = budget.reservations.get(&request.operation_id) {
            if budget
                .reservations
                .values()
                .any(|r| r.over_cap_usage.is_some())
            {
                return Err(BudgetBlock::Unavailable);
            }
            request.observed_seq = record.request.observed_seq;
        }
        match self
            .service
            .reserve_work_tokens(self.authority, &request, now)
            .map_err(|_| BudgetBlock::Unavailable)?
        {
            WorkTokenAdmission::Reserved(record)
                if record.usage.is_none() && record.over_cap_usage.is_none() =>
            {
                Ok(Reservation {
                    operation: diagnostic.operation.clone(),
                    request: Box::new(diagnostic.clone()),
                    receipt: record.receipt,
                })
            }
            WorkTokenAdmission::Reserved(_) => Err(BudgetBlock::Unavailable),
            WorkTokenAdmission::Blocked(block) => Err(match block {
                WorkTokenBlock::Work => BudgetBlock::Work,
                WorkTokenBlock::Daily => BudgetBlock::Daily,
                WorkTokenBlock::Total => BudgetBlock::Total,
                WorkTokenBlock::Capacity | WorkTokenBlock::Accounting => BudgetBlock::Unavailable,
            }),
        }
    }

    fn settle(&mut self, reservation: &Reservation, usage: Usage) -> Result<(), PortError> {
        if reservation.operation != reservation.request.operation {
            return Err(PortError::Denied);
        }
        let identity = request(&reservation.request, 0)?;
        let record = self
            .service
            .work_token_budget(self.authority)
            .map_err(host::ticket_error)?
            .and_then(|budget| budget.reservations.get(&identity.operation_id).cloned())
            .ok_or(PortError::Denied)?;
        let request = request(&reservation.request, record.request.observed_seq)?;
        if record.request != request || record.receipt != reservation.receipt {
            return Err(PortError::Denied);
        }
        if usage == Usage::Unknown
            && (matches!(record.usage, Some(ExecutionTokenUsage::Known { .. }))
                || record.over_cap_usage.is_some())
        {
            // The ledger already knows the outcome, even if the caller lost
            // its checkpoint or provider status after the accounting commit.
            return Ok(());
        }
        // The original Assignment need not remain active. A fresh Runtime proof
        // for this binding may settle its old receipt after restart/Submitted/
        // generation changes, without granting another provider admission.
        let usage = match usage {
            Usage::Known {
                input_tokens,
                output_tokens,
            } if reservation
                .request
                .model
                .exceeds_token_caps(input_tokens, output_tokens) =>
            {
                return self
                    .service
                    .report_work_token_over_cap(
                        self.authority,
                        &request.operation_id,
                        &request.request_hash,
                        &reservation.receipt,
                        ExecutionTokenOverCapUsage {
                            input_tokens: Some(u64::from(input_tokens)),
                            output_tokens: Some(u64::from(output_tokens)),
                            total_tokens: None,
                        },
                    )
                    .map(|_| ())
                    .map_err(host::ticket_error);
            }
            Usage::Known {
                input_tokens,
                output_tokens,
            } => ExecutionTokenUsage::Known {
                input_tokens: u64::from(input_tokens),
                output_tokens: u64::from(output_tokens),
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
