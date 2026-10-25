//! Program-only patrol over the existing checkpoint and current Host facts.
//!
//! There is no model, budget, provider or delivery capability in this entrypoint.
//! An escalation is a pending intent: only the separately authorized delivery
//! adapter may increment its attempts or acknowledge it. Outstanding diagnostic
//! requests and settlement debt remain intact for their original adapters.

use super::{
    escalate, needs_resync, open_incident, report, save, status, Block, Checkpoint, CheckpointPort,
    Clock, DiagnosticInput, Error, Observation, ObservationPort, Phase, Policy, PortError, Report,
    Scope, Status, StoredCheckpoint, SupervisorReason,
};

/// One bounded program pass. Construction neither installs a policy nor starts
/// a worker; callers still supply fresh authorized observations and canonical CAS.
pub struct ProgramPatrol {
    patrol: super::Patrol,
}

impl ProgramPatrol {
    pub fn new(policy: Policy) -> Result<Self, Error> {
        Ok(Self {
            patrol: super::Patrol::new(policy, None)?,
        })
    }

    /// Read current facts even when semantic adapters are unavailable. A saved
    /// checkpoint never authorizes a read, dispatch, model call or Ticket update.
    ///
    /// Active diagnostic operations and unsettled charges require their exact
    /// original adapters. Report NeedsSupervisor without changing their request,
    /// reservation, phase or delivery receipt, even after observed progress.
    pub fn tick(
        &self,
        scope: &Scope,
        clock: &dyn Clock,
        checkpoints: &mut dyn CheckpointPort,
        observations: &mut dyn ObservationPort,
    ) -> Result<Report, Error> {
        let mut host = ProgramPorts {
            checkpoints,
            observations,
        };
        let (mut cp, mut version, observation, now) =
            self.patrol.begin_tick(scope, clock, &mut host)?;
        if needs_resync(&cp, &observation) {
            save(&mut host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::ResyncRequired));
        }
        cp.cursor = observation.cursor;
        cp.source_revision = observation.source_revision;
        if let Some(wait) = &observation.expected_wait {
            if wait.until_ms > now {
                cp.next_check_ms = cp.next_check_ms.min(wait.until_ms);
            }
        }

        // Do not turn an unavailable budget/provider into a failed settlement,
        // a new operation, refunded credit, or a consumed escalation attempt.
        if cp.incident.as_ref().is_some_and(|incident| {
            matches!(incident.phase, Phase::Diagnosing { .. })
                || incident.pending_settlement.is_some()
        }) {
            save(&mut host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::NeedsSupervisor));
        }

        if let Some(incident) = cp.incident.as_mut() {
            let resumed = observation.progress.is_some_and(|progress| {
                progress.sequence > incident.id.progress_sequence
                    && progress.at_ms >= incident.opened_at_ms
            }) && !observation.execution_unknown;
            if !matches!(incident.phase, Phase::Resolved { .. })
                && (observation.terminal || resumed)
            {
                incident.phase = Phase::Resolved {
                    at_ms: now,
                    cursor: cp.cursor,
                };
                save(&mut host, &mut version, &mut cp)?;
                return Ok(report(&cp, Status::Resolved));
            }
        }

        let detected = self.patrol.reason(&observation, now);
        if detected.is_none()
            && cp
                .incident
                .as_ref()
                .is_none_or(|incident| matches!(incident.phase, Phase::Resolved { .. }))
        {
            save(&mut host, &mut version, &mut cp)?;
            return Ok(report(
                &cp,
                if observation.expected_wait.is_some() {
                    Status::ExpectedWait
                } else {
                    Status::Healthy
                },
            ));
        }
        let reason = detected
            .or_else(|| cp.incident.as_ref().map(|incident| incident.reason))
            .ok_or(Error::InvalidCheckpoint)?;
        let sequence = observation.progress.map_or(0, |progress| progress.sequence);
        if cp.incident.is_none()
            || cp.incident.as_ref().is_some_and(|incident| {
                matches!(incident.phase, Phase::Resolved { .. })
                    && sequence > incident.id.progress_sequence
            })
        {
            cp.incident = Some(open_incident(reason, &observation, now));
            save(&mut host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::Incident));
        }

        let incident = cp.incident.as_mut().ok_or(Error::InvalidCheckpoint)?;
        if !matches!(incident.phase, Phase::Resolved { .. }) {
            incident.reason = reason;
            if observation.execution_unknown && incident.escalation.is_none() {
                incident.phase = Phase::Blocked(Block::UnknownUsage);
                escalate(
                    incident,
                    SupervisorReason::ReconcileReceipt,
                    &observation,
                    now,
                );
            } else if detected.is_some()
                && !matches!(incident.phase, Phase::Cooldown { until_ms } if now < until_ms)
            {
                let reason = match reason {
                    super::Reason::ExecutionUnknown | super::Reason::MissingHeartbeat => {
                        SupervisorReason::ReconcileReceipt
                    }
                    super::Reason::WaitExpired | super::Reason::EvidenceUnknown => {
                        SupervisorReason::DeterministicAnomaly
                    }
                    _ => SupervisorReason::DiagnosticUnavailable,
                };
                escalate(incident, reason, &observation, now);
            }
        }
        save(&mut host, &mut version, &mut cp)?;
        Ok(report(&cp, status(&cp)))
    }
}

// Combine only the two supplied capabilities. In particular this is not a Host
// implementation with pretend budget receipts or an always-successful outbox.
struct ProgramPorts<'a> {
    checkpoints: &'a mut dyn CheckpointPort,
    observations: &'a mut dyn ObservationPort,
}

impl CheckpointPort for ProgramPorts<'_> {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        self.checkpoints.load(scope)
    }

    fn save(
        &mut self,
        expected_version: Option<u64>,
        checkpoint: &Checkpoint,
    ) -> Result<u64, PortError> {
        self.checkpoints.save(expected_version, checkpoint)
    }
}

impl ObservationPort for ProgramPorts<'_> {
    fn observation(&mut self, scope: &Scope, after_cursor: u64) -> Result<Observation, PortError> {
        self.observations.observation(scope, after_cursor)
    }

    fn diagnostic_input(
        &mut self,
        _scope: &Scope,
        _since_cursor: u64,
        _through: &Observation,
    ) -> Result<DiagnosticInput, PortError> {
        Err(PortError::Denied)
    }
}
