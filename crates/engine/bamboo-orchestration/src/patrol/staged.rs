//! Split semantic patrol at the evidence and provider I/O boundaries. The
//! existing checkpoint and ledger remain the only durable operation owners.

use super::{
    Clock, DiagnosticInput, DiagnosticRequest, DiagnosticStatus, Error, Host, Model, Observation,
    OperationId, Patrol, PortError, ReadOnlyDiagnosticProvider, Report, Reservation, Scope,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputDemand {
    pub model: Model,
    pub through: Observation,
    pub since_cursor: u64,
}

impl InputDemand {
    fn same_evidence(&self, current: &Self) -> bool {
        self.model == current.model
            && self.since_cursor == current.since_cursor
            && self.through.scope == current.through.scope
            && self.through.cursor == current.through.cursor
            && self.through.source_revision == current.through.source_revision
            && self.through.evidence_revision == current.through.evidence_revision
    }
}

/// Input or definite failure bound to a specific previous demand. A mismatched
/// demand is discarded before the budget or journal can be touched.
pub struct PreparedInput {
    pub demand: InputDemand,
    pub result: Result<DiagnosticInput, PortError>,
}

pub enum Outcome {
    InputRequired(InputDemand),
    Complete {
        report: Report,
        submission: Option<Submission>,
        reconciled: Option<Reconciled>,
    },
}

/// One live provider handoff. An accounting receipt recovered from disk cannot
/// construct it. The outer transport consumes it after fresh Host admission.
pub struct Submission(Reservation);

/// The checkpoint or ledger now owns the observed usage for this operation.
pub struct Reconciled(pub(super) OperationId);

impl Submission {
    pub fn reservation(&self) -> &Reservation {
        &self.0
    }

    pub(super) fn into_reservation(self) -> Reservation {
        self.0
    }
}

pub(super) struct Stage {
    prepared: Option<PreparedInput>,
    pub(super) demand: Option<InputDemand>,
    pub(super) submission: Option<Reservation>,
}

impl Stage {
    pub(super) fn input(
        &mut self,
        model: &Model,
        through: &Observation,
        since_cursor: u64,
    ) -> Option<Result<DiagnosticInput, PortError>> {
        let demand = InputDemand {
            model: model.clone(),
            through: through.clone(),
            since_cursor,
        };
        match self.prepared.take() {
            Some(prepared) if prepared.demand.same_evidence(&demand) => Some(prepared.result),
            _ => {
                self.demand = Some(demand);
                None
            }
        }
    }
}

impl Patrol {
    /// The caller can release its journal lease before gathering evidence or
    /// making a provider call. Each resume is a fresh scope check and CAS pass.
    /// A submission only leaves this pass after its checkpoint is durable.
    /// The caller must still recheck its live grant before performing I/O.
    /// Restart may reconcile status by operation, and never yields its submit.
    /// Escalation delivery remains owned by the separate delivery port.
    pub fn tick_staged(
        &self,
        scope: &Scope,
        clock: &dyn Clock,
        host: &mut dyn Host,
        prepared: Option<PreparedInput>,
        status: impl FnMut(&OperationId) -> Result<DiagnosticStatus, PortError>,
    ) -> Result<Outcome, Error> {
        let mut stage = Stage {
            prepared,
            demand: None,
            submission: None,
        };
        let mut provider = StatusOnly {
            model: self.model.as_ref(),
            status,
            reconciled: None,
        };
        let report = self.tick_inner(scope, clock, host, &mut provider, Some(&mut stage))?;
        Ok(match stage.demand {
            Some(demand) => Outcome::InputRequired(demand),
            None => Outcome::Complete {
                report,
                submission: stage.submission.map(Submission),
                reconciled: provider.reconciled,
            },
        })
    }
}

struct StatusOnly<'a, F> {
    model: Option<&'a Model>,
    status: F,
    reconciled: Option<Reconciled>,
}

impl<F: FnMut(&OperationId) -> Result<DiagnosticStatus, PortError>> ReadOnlyDiagnosticProvider
    for StatusOnly<'_, F>
{
    fn identity(&self) -> (&str, &str) {
        self.model
            .map_or(("", ""), |model| (&model.provider, &model.model))
    }

    fn submit(&mut self, _request: &DiagnosticRequest) -> Result<(), PortError> {
        Err(PortError::Denied)
    }

    fn status(&mut self, operation: &OperationId) -> Result<DiagnosticStatus, PortError> {
        let status = (self.status)(operation)?;
        if matches!(
            status,
            DiagnosticStatus::Finished { .. } | DiagnosticStatus::Failed { .. }
        ) {
            self.reconciled = Some(Reconciled(operation.clone()));
        }
        Ok(status)
    }
}
