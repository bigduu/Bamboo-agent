//! Read-only projection over the canonical Ticket ledger. No runtime I/O,
//! inferred liveness timestamps, Ticket mutations or recovered execution grants.

use std::collections::BTreeSet;

use bamboo_tickets::{
    AssignmentState, Authority, EffectState, Health, InspectOptions, Principal, Snapshot,
    TicketKind, TicketService, WorkState,
};

use super::{
    DiagnosticInput, ExpectedWait, Observation, ObservationKind, ObservationPort, Policy,
    PortError, Scope, WaitKind,
};

/// One read-only Host sample, tied to the exact canonical Ticket snapshot and
/// receipt. It carries no executable permission and is never deserialized from
/// a model, HTTP request or recovery checkpoint. The Host verifies Child birth,
/// native Run identity and the exact current wait before constructing it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeObservation {
    pub scope: Scope,
    pub source_commit: String,
    pub receipt: bamboo_tickets::RuntimeReceipt,
    pub wait: RuntimeWait,
}

/// Fresh Host facts, not a grant or durable runtime history. A live Task-entry
/// sample supplies only start time; heartbeat and progress remain independent.
/// The original wait-only RuntimeObservation API remains source-compatible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeFacts {
    pub scope: Scope,
    pub source_commit: String,
    pub receipt: bamboo_tickets::RuntimeReceipt,
    pub started_at_ms: Option<u64>,
    pub wait: Option<RuntimeWait>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeWait {
    pub kind: WaitKind,
    /// A canonical wait/suspension/yield boundary, never observation time.
    pub registered_at_ms: u64,
    /// Existing runtime deadline, if any. None creates no business deadline.
    pub timeout_at_ms: Option<u64>,
    pub request_id: Option<String>,
    pub evidence_ref: String,
}

/// A single-use observation prepared after the outer Host's fresh authority
/// check. It cannot become a cached read grant or silently refresh itself.
pub struct TicketObservation {
    observation: Option<Observation>,
    scope: Scope,
}

impl TicketObservation {
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
}

impl ObservationPort for TicketObservation {
    fn observation(&mut self, scope: &Scope, after_cursor: u64) -> Result<Observation, PortError> {
        let observation = self.observation.take().ok_or(PortError::Unavailable)?;
        if scope != &observation.scope {
            return Err(PortError::Denied);
        }
        if after_cursor > observation.cursor {
            return Err(PortError::Conflict);
        }
        Ok(observation)
    }

    fn diagnostic_input(
        &mut self,
        _scope: &Scope,
        _since_cursor: u64,
        _through: &Observation,
    ) -> Result<DiagnosticInput, PortError> {
        // No production semantic evidence adapter is enabled by this slice.
        Err(PortError::Unavailable)
    }
}

/// The caller must obtain `authority` through its current Host proof (e.g. the
/// server's existing async `authority(Runtime)`), immediately before this call.
/// Scope IDs are selectors only. The immutable DispatchSpec epoch is the stable
/// recovery identity; mutable Assignment/current writer epochs are not grants.
pub fn scope_for_assignment(
    service: &TicketService,
    authority: &Authority,
    project_id: &str,
    work_id: &str,
    assignment_id: &str,
) -> Result<Scope, PortError> {
    let (commit, snapshot) = checked_snapshot(service, authority, work_id)?;
    let assignment = snapshot
        .assignments
        .get(assignment_id)
        .ok_or(PortError::Denied)?;
    let intent = snapshot
        .intents
        .get(&assignment.dispatch_key)
        .ok_or(PortError::Denied)?;
    let scope = Scope {
        project_id: project_id.into(),
        work_id: work_id.into(),
        assignment_id: assignment_id.into(),
        generation: assignment.generation,
        authority_epoch: intent.immutable_spec.authority_epoch,
    };
    project(&snapshot, &scope)?;
    current(service, &commit)?;
    Ok(scope)
}

/// Canonical attempt selector only; callers still authorize through
/// scope_for_assignment/read_observation. A yielded Question has no active
/// pointer after owned stop, so retain only its unique exact current attempt.
pub fn current_assignment_id(
    snapshot: &Snapshot,
    work_id: &str,
) -> Result<Option<String>, PortError> {
    let work = snapshot.tickets.get(work_id).ok_or(PortError::Denied)?;
    if let Some(id) = work.active_assignment.as_ref().or_else(|| {
        work.current_submission
            .as_ref()
            .and_then(|id| snapshot.submissions.get(id))
            .map(|submission| &submission.assignment_id)
    }) {
        return Ok(Some(id.clone()));
    }
    let mut candidates = snapshot.assignments.values().filter(|assignment| {
        assignment.work_id == work.id
            && assignment.generation == work.generation
            && assignment.contract_revision == work.contract_revision
            && (work.state == WorkState::Cancelled
                || yielded_question(snapshot, assignment).is_some())
    });
    let result = candidates.next().map(|assignment| assignment.id.clone());
    if candidates.next().is_some() {
        return Err(PortError::Conflict);
    }
    Ok(result)
}

fn yielded_question<'a>(
    snapshot: &'a Snapshot,
    assignment: &bamboo_tickets::Assignment,
) -> Option<&'a bamboo_tickets::PendingRequest> {
    let work = snapshot.tickets.get(&assignment.work_id)?;
    let request = snapshot
        .requests
        .get(assignment.awaiting_request.as_ref()?)?;
    (work.state == WorkState::Blocked
        && assignment.state == AssignmentState::Blocked
        && assignment.process_stopped
        && request.work_id == work.id
        && request.assignment_id.as_deref() == Some(&assignment.id)
        && request.generation == work.generation
        && request.contract_revision == work.contract_revision
        && request.status == bamboo_tickets::RequestStatus::Open
        && request.kind == bamboo_tickets::RequestKind::Question
        && request.prompt_revision > 0
        && request.worker_packet_hash.as_ref().is_some_and(|hash| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        }))
    .then_some(request)
}

/// Always projects a bounded authoritative snapshot, so an event gap causes no
/// replay of cached deltas. There are currently no canonical start/heartbeat/
/// progress/wait timestamps in Ticket: these stay explicitly unavailable.
pub fn read_observation(
    service: &TicketService,
    authority: &Authority,
    scope: &Scope,
) -> Result<TicketObservation, PortError> {
    let (commit, snapshot) = checked_snapshot(service, authority, &scope.work_id)?;
    let observation = project(&snapshot, scope)?;
    current(service, &commit)?;
    Ok(TicketObservation {
        observation: Some(observation),
        scope: scope.clone(),
    })
}

/// Join a verified runtime sample to fresh ledger authority. Missing heartbeat
/// and meaningful-progress producers deliberately remain None.
pub fn read_observation_with_runtime(
    service: &TicketService,
    authority: &Authority,
    scope: &Scope,
    runtime: &RuntimeObservation,
    policy: &Policy,
) -> Result<TicketObservation, PortError> {
    read_observation_with_runtime_facts(
        service,
        authority,
        scope,
        &RuntimeFacts {
            scope: runtime.scope.clone(),
            source_commit: runtime.source_commit.clone(),
            receipt: runtime.receipt.clone(),
            started_at_ms: None,
            wait: Some(runtime.wait.clone()),
        },
        policy,
    )
}

pub fn read_observation_with_runtime_facts(
    service: &TicketService,
    authority: &Authority,
    scope: &Scope,
    runtime: &RuntimeFacts,
    policy: &Policy,
) -> Result<TicketObservation, PortError> {
    let (commit, snapshot) = checked_snapshot(service, authority, &scope.work_id)?;
    if runtime.scope != *scope || runtime.source_commit != commit {
        return Err(PortError::Conflict);
    }
    if runtime.started_at_ms == Some(0) {
        return Err(PortError::Denied);
    }
    let mut observation = project(&snapshot, scope)?;
    let assignment = &snapshot.assignments[&scope.assignment_id];
    if assignment.runtime.as_ref() != Some(&runtime.receipt) {
        return Err(PortError::Denied);
    }
    if !observation.execution_unknown && !observation.terminal {
        observation.started_at_ms = runtime.started_at_ms;
        if let Some(wait) = &runtime.wait {
            match wait.kind {
                WaitKind::User => {
                    let request = wait
                        .request_id
                        .as_ref()
                        .and_then(|id| snapshot.requests.get(id))
                        .ok_or(PortError::Denied)?;
                    if assignment.awaiting_request.as_ref() != Some(&request.id)
                        || yielded_question(&snapshot, assignment).map(|question| &question.id)
                            != Some(&request.id)
                        || request.work_id != scope.work_id
                        || request.assignment_id.as_deref() != Some(&scope.assignment_id)
                        || request.generation != scope.generation
                        || request.contract_revision != assignment.contract_revision
                        || request.status != bamboo_tickets::RequestStatus::Open
                        || request.kind != bamboo_tickets::RequestKind::Question
                    {
                        return Err(PortError::Denied);
                    }
                }
                WaitKind::Dependency | WaitKind::RunningTool if wait.request_id.is_some() => {
                    return Err(PortError::Denied);
                }
                _ => {}
            }
            let allowance = wait
                .registered_at_ms
                .checked_add(policy.max_expected_wait_ms)
                .ok_or(PortError::Unavailable)?;
            let until_ms = wait.timeout_at_ms.map_or(allowance, |at| at.min(allowance));
            if until_ms <= wait.registered_at_ms || !super::valid_id(&wait.evidence_ref) {
                return Err(PortError::Denied);
            }
            observation.expected_wait = Some(ExpectedWait {
                kind: wait.kind,
                issued_at_ms: wait.registered_at_ms,
                until_ms,
                progress_sequence: 0,
                evidence_ref: wait.evidence_ref.clone(),
            });
        }
    }
    current(service, &commit)?;
    Ok(TicketObservation {
        observation: Some(observation),
        scope: scope.clone(),
    })
}

fn checked_snapshot(
    service: &TicketService,
    authority: &Authority,
    work_id: &str,
) -> Result<(String, Snapshot), PortError> {
    if service.health() != Health::Writable {
        return Err(PortError::Unavailable);
    }
    if !matches!(
        authority.principal(),
        Principal::Runtime | Principal::Supervisor { .. }
    ) {
        return Err(PortError::Denied);
    }
    let (commit, snapshot) = service.published().map_err(ticket_error)?;
    // Use the existing read authorization against this exact published view.
    // Exclude histories: no transcript/evidence is read for a program probe.
    let view = service
        .work_inspect_sections(
            authority,
            &[work_id.into()],
            &InspectOptions {
                sections: BTreeSet::new(),
                depth: 0,
                budget_bytes: 65_536,
                fixed_commit: Some(commit.clone()),
            },
        )
        .map_err(ticket_error)?;
    if view.snapshot.commit != commit
        || view.snapshot.seq != snapshot.seq
        || view.snapshot.authority_epoch != snapshot.authority_epoch
    {
        return Err(PortError::Conflict);
    }
    Ok((commit, snapshot))
}

fn current(service: &TicketService, commit: &str) -> Result<(), PortError> {
    if service.health() != Health::Writable {
        return Err(PortError::Unavailable);
    }
    if service.published().map_err(ticket_error)?.0 != commit {
        return Err(PortError::Conflict);
    }
    Ok(())
}

pub(super) fn project(snapshot: &Snapshot, scope: &Scope) -> Result<Observation, PortError> {
    if !super::valid_scope(scope) {
        return Err(PortError::Denied);
    }
    let work = snapshot
        .tickets
        .get(&scope.work_id)
        .ok_or(PortError::Denied)?;
    let assignment = snapshot
        .assignments
        .get(&scope.assignment_id)
        .ok_or(PortError::Denied)?;
    let intent = snapshot
        .intents
        .get(&assignment.dispatch_key)
        .ok_or(PortError::Denied)?;
    let spec = &intent.immutable_spec;
    if work.kind != TicketKind::Work
        || assignment.work_id != work.id
        || work.generation != scope.generation
        || assignment.generation != scope.generation
        || assignment.contract_revision != work.contract_revision
        || snapshot.project.links.get(&work.id).map(|id| id.as_str())
            != Some(scope.project_id.as_str())
        || work
            .active_assignment
            .as_ref()
            .is_some_and(|id| id != &assignment.id)
        || intent.assignment_id != assignment.id
        || intent.dispatch_key != assignment.dispatch_key
        || spec.assignment_id != assignment.id
        || spec.work_id != work.id
        || spec.generation != scope.generation
        || spec.contract_revision != work.contract_revision
        || spec.authority_epoch != scope.authority_epoch
        || spec.binding != snapshot.binding
        || spec.authority_epoch > snapshot.authority_epoch
        || assignment.authority_epoch < spec.authority_epoch
        || assignment.authority_epoch > snapshot.authority_epoch
        || bamboo_tickets::content_hash(
            &bamboo_tickets::canonical_bytes(spec).map_err(ticket_error)?,
        ) != intent.spec_hash
        || assignment.runtime != intent.receipt
    {
        return Err(PortError::Denied);
    }
    if let Some(receipt) = &assignment.runtime {
        if receipt.dispatch_key != assignment.dispatch_key
            || receipt.spec_hash != intent.spec_hash
            || !super::valid_id(&receipt.run_id)
            || !super::valid_id(&receipt.session_id)
        {
            return Err(PortError::Denied);
        }
    }
    if work.active_assignment.is_none() {
        let submission = work
            .current_submission
            .as_ref()
            .and_then(|id| snapshot.submissions.get(id));
        let current_submission = submission.is_some_and(|s| {
            s.assignment_id == assignment.id
                && s.work_id == work.id
                && s.generation == scope.generation
                && s.contract_revision == work.contract_revision
                && Some(&s.runtime) == assignment.runtime.as_ref()
        });
        if !current_submission
            && work.state != WorkState::Cancelled
            && yielded_question(snapshot, assignment).is_none()
        {
            return Err(PortError::Denied);
        }
    }
    let unknown_effect = assignment.effects.values().any(|effect| {
        matches!(
            effect.state,
            EffectState::Started | EffectState::OutcomeUnknown
        )
    });
    let execution_unknown = unknown_effect
        || assignment.state == AssignmentState::OutcomeUnknown
        || (!assignment.process_stopped && assignment.runtime.is_none());
    let terminal = matches!(work.state, WorkState::Accepted | WorkState::Cancelled)
        && assignment.process_stopped
        && !execution_unknown;
    Ok(Observation {
        scope: scope.clone(),
        kind: ObservationKind::Snapshot,
        source_revision: snapshot.execution_facts_revision(),
        cursor: snapshot.seq,
        evidence_revision: work.updated_seq.max(assignment.updated_seq),
        started_at_ms: None,
        heartbeat_at_ms: None,
        progress: None,
        expected_wait: None,
        repeated_errors: false,
        execution_unknown,
        terminal,
    })
}

pub(super) fn ticket_error(error: bamboo_tickets::Error) -> PortError {
    match error {
        bamboo_tickets::Error::ScopeDenied(_) => PortError::Denied,
        bamboo_tickets::Error::RevisionConflict | bamboo_tickets::Error::ResyncRequired => {
            PortError::Conflict
        }
        _ => PortError::Unavailable,
    }
}
