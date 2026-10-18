//! Deterministic, explicitly driven patrol policy. No worker loop or authority store.
//!
//! Outer Host adapters project **current** Ticket/Assignment/RuntimeReceipt facts,
//! authorize every read and provider admission, and persist the checkpoint with CAS.
//! These references confer no grant. There is no Ticket mutation, worker dispatch,
//! tool execution, acceptance, credential lookup or fallback to a Supervisor model.
//! All port calls happen outside Ticket locks. Concrete I/O adapters live in the Host.

use serde::{Deserialize, Serialize};

/// Epoch milliseconds from the Host's clock, including across process restarts.
pub trait Clock {
    fn now_ms(&self) -> u64;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub project_id: String,
    pub work_id: String,
    pub assignment_id: String,
    pub generation: u64,
    /// Original immutable DispatchSpec epoch: a recovery identity, never a grant.
    pub authority_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub check_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub progress_timeout_ms: u64,
    pub max_expected_wait_ms: u64,
    pub diagnostic_timeout_ms: u64,
    pub cooldown_ms: u64,
    pub max_diagnostic_attempts: u16,
    pub max_escalation_deliveries: u16,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            check_interval_ms: 30_000,
            heartbeat_timeout_ms: 120_000,
            progress_timeout_ms: 600_000,
            max_expected_wait_ms: 3_600_000,
            diagnostic_timeout_ms: 60_000,
            cooldown_ms: 300_000,
            max_diagnostic_attempts: 2,
            max_escalation_deliveries: 3,
        }
    }
}

/// Independent model selection. None disables semantic diagnostics; no fallback.
/// Protocol-specific adaptation belongs to the outer provider adapter.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub provider: String,
    pub model: String,
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
    pub max_evidence_bytes: usize,
    pub max_evidence_refs: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// Host-verified meaningful progress; heartbeats and recovery ACKs never increment it.
    pub sequence: u64,
    pub at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitKind {
    User,
    Dependency,
    /// A live tool receipt, e.g. a compile, with a finite Host-validated deadline.
    RunningTool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedWait {
    pub kind: WaitKind,
    pub issued_at_ms: u64,
    pub until_ms: u64,
    pub progress_sequence: u64,
    pub evidence_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationKind {
    Snapshot,
    Delta { previous_cursor: u64 },
}

/// Bounded, trusted Host projection, never model- or UI-supplied execution facts.
/// The Host binds progress/heartbeat/wait to the canonical runtime receipt and
/// refuses ended or fenced attempts. `terminal` means authoritative Ticket state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub scope: Scope,
    pub kind: ObservationKind,
    pub source_revision: u64,
    pub cursor: u64,
    /// Changes relevant to diagnosis, excluding mere heartbeat/poll/ACK updates.
    pub evidence_revision: u64,
    /// A canonical start timestamp, never an observation time or Ticket sequence.
    /// None means that production measurement is currently unavailable.
    pub started_at_ms: Option<u64>,
    pub heartbeat_at_ms: Option<u64>,
    pub progress: Option<Progress>,
    pub expected_wait: Option<ExpectedWait>,
    pub repeated_errors: bool,
    /// Started/OutcomeUnknown effects or an unconfirmed execution outcome.
    pub execution_unknown: bool,
    pub terminal: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    NoProgress,
    RepeatedErrors,
    MissingHeartbeat,
    WaitExpired,
    ExecutionUnknown,
    EvidenceUnknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct IncidentId {
    pub scope: Scope,
    /// One incident per meaningful-progress boundary, across repeated events/restarts.
    pub progress_sequence: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Diagnostic(u16),
    Escalation,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct OperationId {
    pub incident: IncidentId,
    pub kind: OperationKind,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    /// Opaque, Host-authorized reference, not a path or retrieval capability.
    pub id: String,
    pub revision: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticInput {
    pub scope: Scope,
    pub source_revision: u64,
    pub since_cursor: u64,
    pub through_cursor: u64,
    pub summary: String,
    pub references: Vec<EvidenceRef>,
    /// Model-specific input metering by the trusted adapter, including its envelope.
    pub input_tokens: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRequest {
    pub operation: OperationId,
    pub model: Model,
    pub input: DiagnosticInput,
    pub deadline_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BudgetBlock {
    Work,
    Daily,
    Total,
    Unavailable,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    pub operation: OperationId,
    /// Exact immutable request authorized by the ledger. A CAS failure followed
    /// by a changed model/cap/input must not repurpose the old reservation.
    pub request: Box<DiagnosticRequest>,
    pub receipt: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Usage {
    Known {
        input_tokens: u32,
        output_tokens: u32,
    },
    Unknown,
}

/// No wallet in patrol: this port uses the authoritative cumulative Work ledger.
/// Reserve atomically checks Work + defined daily window + total execution limits,
/// charging the request's hard input/output caps before any provider submission.
/// Reserve/settle are idempotent by operation AND immutable request. A changed
/// request for an existing operation must be refused, retaining its old charge.
/// Unknown usage, cancellation or a
/// crash retains the reservation; it must never become free retry credit. Prices
/// and amounts are not inferred from rounds or these token ceilings.
pub trait BudgetPort {
    fn reserve(&mut self, request: &DiagnosticRequest) -> Result<Reservation, BudgetBlock>;
    fn settle(&mut self, reservation: &Reservation, usage: Usage) -> Result<(), PortError>;
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorReason {
    ReconcileReceipt,
    DeterministicAnomaly,
    Replan,
    ResourceConflict,
    AcceptanceDispute,
    AuthorityChange,
    Uncertain,
    PersistentAnomaly,
    DiagnosticTimeout,
    DiagnosticUnavailable,
    BudgetBlocked,
    InvalidDiagnostic,
}

/// Suggestions only. Deliberately no Done, repair, dispatch, tool or grant variant.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Advice {
    Observe,
    Escalate(SupervisorReason),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Diagnosis {
    pub advice: Advice,
    pub evidence: Vec<String>,
    pub uncertain: bool,
    pub note: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRecord {
    pub operation: OperationId,
    pub source_revision: u64,
    pub through_cursor: u64,
    pub at_ms: u64,
    pub diagnosis: Diagnosis,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticStatus {
    Pending,
    Finished {
        diagnosis: Diagnosis,
        usage: Usage,
    },
    Failed {
        usage: Usage,
    },
    /// Includes a crash before submit/ACK: reconcile this key; never submit a new one.
    OutcomeUnknown,
}

/// Trusted, nonblocking runtime/provider adapter for one restricted read-only
/// diagnostic session. submit/status perform fresh Host scope/provider admission
/// and expose no tools. All evidence is already authorized/bounded by the Host.
/// submit is idempotent; status only reconciles the original operation and must
/// not start work. The watchdog bounds the session independently of its own claims.
pub trait ReadOnlyDiagnosticProvider {
    fn identity(&self) -> (&str, &str);
    fn submit(&mut self, request: &DiagnosticRequest) -> Result<(), PortError>;
    fn status(&mut self, operation: &OperationId) -> Result<DiagnosticStatus, PortError>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Escalation {
    pub operation: OperationId,
    pub reason: SupervisorReason,
    pub source_revision: u64,
    pub cursor: u64,
    pub deliveries: u16,
    pub queued: bool,
    pub next_delivery_ms: u64,
}

/// Idempotently queue one visible decision item. This does not itself call an
/// LLM or imply recovery. Supervisor consumption still needs current Host grant,
/// admission and the same cumulative budget; a rejected queue stays visible here.
pub trait EscalationPort {
    fn queue_escalation(&mut self, escalation: &Escalation) -> Result<(), PortError>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Block {
    Budget(BudgetBlock),
    Provider,
    Input,
    SessionTimeout,
    UnknownUsage,
    AttemptLimit,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Open,
    /// Saved before submit. Recovery calls status, never re-submits this operation.
    Diagnosing {
        request: Box<DiagnosticRequest>,
        reservation: Reservation,
    },
    Cooldown {
        until_ms: u64,
    },
    Blocked(Block),
    Resolved {
        at_ms: u64,
        cursor: u64,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Incident {
    pub id: IncidentId,
    pub reason: Reason,
    pub opened_at_ms: u64,
    pub opened_cursor: u64,
    pub source_revision: u64,
    pub attempts: u16,
    pub last_diagnostic_cursor: u64,
    pub last_diagnostic_evidence_revision: Option<u64>,
    pub last_diagnosis: Option<DiagnosticRecord>,
    /// A receipt to reconcile, never a second balance. An unavailable ledger
    /// cannot prevent the program from surfacing a timeout or retaining a charge.
    pub pending_settlement: Option<Reservation>,
    pub phase: Phase,
    pub escalation: Option<Escalation>,
}

/// Patrol bookkeeping only. Does not copy Ticket state, owner/lease or balance.
/// Persist this alongside the existing Host checkpoint/incident contract, not in
/// a new database. No recovered checkpoint is authority to execute or read.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub scope: Scope,
    pub cursor: u64,
    pub source_revision: u64,
    pub last_check_ms: u64,
    pub next_check_ms: u64,
    pub incident: Option<Incident>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredCheckpoint {
    pub version: u64,
    pub checkpoint: Checkpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortError {
    Unavailable,
    Denied,
    Conflict,
    OutcomeUnknown,
}

/// CAS save must durably succeed before returning. Conflict/unknown outcomes
/// stop this tick before external effects. Host serializes a scope through CAS.
pub trait CheckpointPort {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError>;
    fn save(
        &mut self,
        expected_version: Option<u64>,
        checkpoint: &Checkpoint,
    ) -> Result<u64, PortError>;
}

pub trait ObservationPort {
    /// Re-read canonical authority/receipt. Gaps require a bounded snapshot;
    /// Project/generation/epoch mismatch must fail, never recover a revoked grant.
    fn observation(&mut self, scope: &Scope, after_cursor: u64) -> Result<Observation, PortError>;
    /// Incremental, Host-authorized summary plus last tool/wait/workspace/error/
    /// resource evidence. Missing/invalid references fail closed; no full history.
    fn diagnostic_input(
        &mut self,
        scope: &Scope,
        since_cursor: u64,
        through: &Observation,
    ) -> Result<DiagnosticInput, PortError>;
}

pub trait Host: CheckpointPort + ObservationPort + BudgetPort + EscalationPort {}
impl<T: CheckpointPort + ObservationPort + BudgetPort + EscalationPort> Host for T {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Healthy,
    ExpectedWait,
    Incident,
    Diagnosing,
    Cooldown,
    Blocked,
    NeedsSupervisor,
    Resolved,
    ResyncRequired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub status: Status,
    pub incident: Option<IncidentId>,
    pub next_check_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    InvalidObservation,
    InvalidCheckpoint,
    ClockRegression,
    Port(PortError),
}

impl From<PortError> for Error {
    fn from(error: PortError) -> Self {
        Self::Port(error)
    }
}

pub struct Patrol {
    policy: Policy,
    model: Option<Model>,
}

impl Patrol {
    pub fn new(policy: Policy, model: Option<Model>) -> Result<Self, Error> {
        if [
            policy.check_interval_ms,
            policy.heartbeat_timeout_ms,
            policy.progress_timeout_ms,
            policy.max_expected_wait_ms,
            policy.diagnostic_timeout_ms,
            policy.cooldown_ms,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 86_400_000)
            || !(1..=8).contains(&policy.max_diagnostic_attempts)
            || !(1..=8).contains(&policy.max_escalation_deliveries)
            || model.as_ref().is_some_and(|m| {
                !valid_id(&m.provider)
                    || !valid_id(&m.model)
                    || m.max_input_tokens == 0
                    || m.max_output_tokens == 0
                    || m.max_input_tokens > 32_768
                    || m.max_output_tokens > 8_192
                    || m.max_evidence_bytes == 0
                    || m.max_evidence_bytes > 131_072
                    || m.max_evidence_refs == 0
                    || m.max_evidence_refs > 32
            })
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self { policy, model })
    }

    /// One bounded pass, driven by the existing event/reconcile caller. Even
    /// when semantic work is blocked, the program reads current execution facts.
    pub fn tick(
        &self,
        scope: &Scope,
        clock: &dyn Clock,
        host: &mut dyn Host,
        provider: &mut dyn ReadOnlyDiagnosticProvider,
    ) -> Result<Report, Error> {
        if !valid_scope(scope) {
            return Err(Error::InvalidObservation);
        }
        let now = clock.now_ms();
        let stored = host.load(scope)?;
        let mut version = stored.as_ref().map(|s| s.version);
        let mut cp = stored.map(|s| s.checkpoint).unwrap_or_else(|| Checkpoint {
            scope: scope.clone(),
            cursor: 0,
            source_revision: 0,
            last_check_ms: now,
            next_check_ms: now,
            incident: None,
        });
        validate_checkpoint(&cp, scope)?;
        if now < cp.last_check_ms {
            return Err(Error::ClockRegression);
        }
        let observation = host.observation(scope, cp.cursor)?;
        validate_observation(&observation, scope, now)?;
        cp.last_check_ms = now;
        cp.next_check_ms = now.saturating_add(self.policy.check_interval_ms);
        if observation.cursor < cp.cursor
            || observation.source_revision < cp.source_revision
            || matches!(observation.kind, ObservationKind::Delta { previous_cursor }
                if observation.cursor > cp.cursor && previous_cursor != cp.cursor)
        {
            save(host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::ResyncRequired));
        }
        cp.cursor = observation.cursor;
        cp.source_revision = observation.source_revision;
        let sequence = observation.progress.map_or(0, |p| p.sequence);
        if let Some(incident) = cp.incident.as_mut() {
            let resumed = observation.progress.is_some_and(|p| {
                p.sequence > incident.id.progress_sequence && p.at_ms >= incident.opened_at_ms
            }) && !observation.execution_unknown;
            if !matches!(incident.phase, Phase::Resolved { .. })
                && (observation.terminal || resumed)
            {
                // A late diagnostic can never change current Ticket facts. Unknown
                // pending usage remains charged even when execution has recovered.
                if let Phase::Diagnosing { reservation, .. } = incident.phase.clone() {
                    settle_usage(incident, &reservation, Usage::Unknown, host);
                }
                incident.phase = Phase::Resolved {
                    at_ms: now,
                    cursor: cp.cursor,
                };
                save(host, &mut version, &mut cp)?;
                return Ok(report(&cp, Status::Resolved));
            }
        }
        let detected = self.reason(&observation, now);
        if let Some(wait) = &observation.expected_wait {
            if wait.until_ms > now {
                cp.next_check_ms = cp.next_check_ms.min(wait.until_ms);
            }
        }
        if detected.is_none()
            && cp
                .incident
                .as_ref()
                .is_none_or(|i| matches!(i.phase, Phase::Resolved { .. }))
        {
            save(host, &mut version, &mut cp)?;
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
            .or_else(|| cp.incident.as_ref().map(|i| i.reason))
            .ok_or(Error::InvalidCheckpoint)?;
        if cp.incident.is_none()
            || cp.incident.as_ref().is_some_and(|i| {
                matches!(i.phase, Phase::Resolved { .. }) && sequence > i.id.progress_sequence
            })
        {
            cp.incident = Some(Incident {
                id: IncidentId {
                    scope: scope.clone(),
                    progress_sequence: sequence,
                },
                reason,
                opened_at_ms: now,
                opened_cursor: cp.cursor,
                source_revision: cp.source_revision,
                attempts: 0,
                last_diagnostic_cursor: cp.cursor,
                last_diagnostic_evidence_revision: None,
                last_diagnosis: None,
                pending_settlement: None,
                phase: Phase::Open,
                escalation: None,
            });
            save(host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::Incident));
        }
        let mut incident = cp.incident.take().ok_or(Error::InvalidCheckpoint)?;
        if matches!(incident.phase, Phase::Resolved { .. }) {
            cp.incident = Some(incident);
            save(host, &mut version, &mut cp)?;
            return Ok(report(&cp, Status::Resolved));
        }
        incident.reason = reason;
        // Unknown side effects always require original-receipt reconciliation,
        // regardless of an earlier semantic diagnosis or expected wait.
        if observation.execution_unknown && incident.escalation.is_none() {
            if let Phase::Diagnosing { reservation, .. } = incident.phase.clone() {
                settle_usage(&mut incident, &reservation, Usage::Unknown, host);
            }
            incident.phase = Phase::Blocked(Block::UnknownUsage);
            escalate(
                &mut incident,
                SupervisorReason::ReconcileReceipt,
                &observation,
                now,
            );
        }
        if incident.escalation.is_none() {
            match incident.phase.clone() {
                Phase::Diagnosing { .. } => {
                    self.reconcile_diagnostic(&mut incident, &observation, now, host, provider)?;
                }
                Phase::Blocked(_) | Phase::Resolved { .. } => {}
                Phase::Cooldown { until_ms } if now < until_ms => {}
                Phase::Open | Phase::Cooldown { .. } if detected.is_some() => {
                    let supervisor = match reason {
                        Reason::ExecutionUnknown | Reason::MissingHeartbeat => {
                            Some(SupervisorReason::ReconcileReceipt)
                        }
                        Reason::WaitExpired | Reason::EvidenceUnknown => {
                            Some(SupervisorReason::DeterministicAnomaly)
                        }
                        _ if self.model.is_none() => Some(SupervisorReason::DiagnosticUnavailable),
                        _ if incident.attempts >= self.policy.max_diagnostic_attempts => {
                            incident.phase = Phase::Blocked(Block::AttemptLimit);
                            Some(SupervisorReason::PersistentAnomaly)
                        }
                        _ if incident.last_diagnostic_evidence_revision
                            == Some(observation.evidence_revision) =>
                        {
                            Some(SupervisorReason::PersistentAnomaly)
                        }
                        _ => None,
                    };
                    if let Some(reason) = supervisor {
                        escalate(&mut incident, reason, &observation, now);
                    } else {
                        self.prepare(&mut incident, &observation, now, host, provider)?;
                        // Persist incident/operation/attempt BEFORE any provider side effect.
                        cp.incident = Some(incident);
                        save(host, &mut version, &mut cp)?;
                        let mut incident = cp.incident.take().ok_or(Error::InvalidCheckpoint)?;
                        if let Phase::Diagnosing {
                            request,
                            reservation,
                        } = incident.phase.clone()
                        {
                            if provider.submit(&request).is_err() {
                                settle_usage(&mut incident, &reservation, Usage::Unknown, host);
                                incident.phase = Phase::Blocked(Block::Provider);
                                escalate(
                                    &mut incident,
                                    SupervisorReason::DiagnosticUnavailable,
                                    &observation,
                                    now,
                                );
                            }
                        }
                        cp.incident = Some(incident);
                        save(host, &mut version, &mut cp)?;
                        return Ok(report(&cp, status(&cp)));
                    }
                }
                Phase::Open | Phase::Cooldown { .. } => {}
            }
        }
        // Save outbox intent and its bounded attempt before queue delivery. A
        // restart retries the SAME key; the Host queue must deduplicate it.
        if let Some(escalation) = incident.escalation.as_mut() {
            if !escalation.queued
                && now >= escalation.next_delivery_ms
                && escalation.deliveries < self.policy.max_escalation_deliveries
            {
                escalation.deliveries += 1;
                escalation.next_delivery_ms = now.saturating_add(self.policy.cooldown_ms);
                cp.incident = Some(incident);
                save(host, &mut version, &mut cp)?;
                let mut incident = cp.incident.take().ok_or(Error::InvalidCheckpoint)?;
                let escalation = incident
                    .escalation
                    .as_mut()
                    .ok_or(Error::InvalidCheckpoint)?;
                escalation.queued = host.queue_escalation(escalation).is_ok();
                cp.incident = Some(incident);
                save(host, &mut version, &mut cp)?;
                return Ok(report(&cp, status(&cp)));
            }
        }
        cp.incident = Some(incident);
        save(host, &mut version, &mut cp)?;
        Ok(report(&cp, status(&cp)))
    }

    fn reason(&self, o: &Observation, now: u64) -> Option<Reason> {
        if o.terminal {
            return None;
        }
        if o.execution_unknown {
            return Some(Reason::ExecutionUnknown);
        }
        if let Some(wait) = &o.expected_wait {
            if wait.progress_sequence != o.progress.map_or(0, |p| p.sequence)
                || wait.issued_at_ms > now
                || wait.until_ms <= wait.issued_at_ms
                || wait.until_ms - wait.issued_at_ms > self.policy.max_expected_wait_ms
                || !valid_id(&wait.evidence_ref)
            {
                return Some(Reason::EvidenceUnknown);
            }
            if now < wait.until_ms {
                return None;
            }
            return Some(Reason::WaitExpired);
        }
        if o.repeated_errors {
            return Some(Reason::RepeatedErrors);
        }
        let Some(started_at_ms) = o.started_at_ms else {
            return Some(Reason::EvidenceUnknown);
        };
        if now.saturating_sub(o.heartbeat_at_ms.unwrap_or(started_at_ms))
            >= self.policy.heartbeat_timeout_ms
        {
            return Some(Reason::MissingHeartbeat);
        }
        let progress_at = o.progress.map_or(started_at_ms, |p| p.at_ms);
        if now.saturating_sub(progress_at) >= self.policy.progress_timeout_ms {
            return Some(Reason::NoProgress);
        }
        None
    }

    fn prepare(
        &self,
        incident: &mut Incident,
        o: &Observation,
        now: u64,
        host: &mut dyn Host,
        provider: &dyn ReadOnlyDiagnosticProvider,
    ) -> Result<(), Error> {
        let model = self.model.as_ref().ok_or(Error::InvalidConfiguration)?;
        if provider.identity() != (model.provider.as_str(), model.model.as_str()) {
            incident.phase = Phase::Blocked(Block::Provider);
            escalate(incident, SupervisorReason::DiagnosticUnavailable, o, now);
            return Ok(());
        }
        let input = match host.diagnostic_input(&o.scope, incident.last_diagnostic_cursor, o) {
            Ok(input) if valid_input(&input, model, o, incident.last_diagnostic_cursor) => input,
            _ => {
                incident.phase = Phase::Blocked(Block::Input);
                escalate(incident, SupervisorReason::InvalidDiagnostic, o, now);
                return Ok(());
            }
        };
        let request = DiagnosticRequest {
            operation: OperationId {
                incident: incident.id.clone(),
                kind: OperationKind::Diagnostic(incident.attempts + 1),
            },
            model: model.clone(),
            input,
            deadline_ms: now.saturating_add(self.policy.diagnostic_timeout_ms),
        };
        match host.reserve(&request) {
            Ok(reservation)
                if reservation.operation == request.operation
                    && reservation.request.operation == reservation.operation
                    && reservation.request.input.scope == o.scope
                    && valid_id(&reservation.receipt) =>
            {
                incident.attempts += 1;
                if reservation.request.as_ref() != &request {
                    // A pre-checkpoint crash can leave an orphan reservation.
                    // Retain its charge and reconcile it, never reuse its token
                    // ceiling/model/input for this now-different request.
                    settle_usage(incident, &reservation, Usage::Unknown, host);
                    incident.phase = Phase::Blocked(Block::UnknownUsage);
                    escalate(incident, SupervisorReason::ReconcileReceipt, o, now);
                    return Ok(());
                }
                incident.last_diagnostic_evidence_revision = Some(o.evidence_revision);
                incident.pending_settlement = Some(reservation.clone());
                incident.phase = Phase::Diagnosing {
                    request: Box::new(request),
                    reservation,
                };
            }
            Ok(_) => return Err(Error::InvalidCheckpoint),
            Err(reason) => {
                incident.phase = Phase::Blocked(Block::Budget(reason));
                escalate(incident, SupervisorReason::BudgetBlocked, o, now);
            }
        }
        Ok(())
    }

    fn reconcile_diagnostic(
        &self,
        incident: &mut Incident,
        o: &Observation,
        now: u64,
        host: &mut dyn Host,
        provider: &mut dyn ReadOnlyDiagnosticProvider,
    ) -> Result<(), Error> {
        let Phase::Diagnosing {
            request,
            reservation,
        } = incident.phase.clone()
        else {
            return Err(Error::InvalidCheckpoint);
        };
        if now >= request.deadline_ms {
            settle_usage(incident, &reservation, Usage::Unknown, host);
            incident.phase = Phase::Blocked(Block::SessionTimeout);
            escalate(incident, SupervisorReason::DiagnosticTimeout, o, now);
            return Ok(());
        }
        if self.model.as_ref() != Some(&request.model)
            || provider.identity()
                != (
                    request.model.provider.as_str(),
                    request.model.model.as_str(),
                )
        {
            settle_usage(incident, &reservation, Usage::Unknown, host);
            incident.phase = Phase::Blocked(Block::Provider);
            escalate(incident, SupervisorReason::DiagnosticUnavailable, o, now);
            return Ok(());
        }
        match provider
            .status(&request.operation)
            .unwrap_or(DiagnosticStatus::OutcomeUnknown)
        {
            DiagnosticStatus::Pending => {}
            DiagnosticStatus::Finished { diagnosis, usage } => {
                let usage_valid = matches!(usage, Usage::Known { input_tokens, output_tokens }
                    if input_tokens <= request.model.max_input_tokens && output_tokens <= request.model.max_output_tokens);
                if !settle_usage(
                    incident,
                    &reservation,
                    if usage_valid { usage } else { Usage::Unknown },
                    host,
                ) {
                    incident.phase = Phase::Blocked(Block::Budget(BudgetBlock::Unavailable));
                    escalate(incident, SupervisorReason::BudgetBlocked, o, now);
                    return Ok(());
                }
                let valid = !diagnosis.note.is_empty()
                    && diagnosis.note.len() <= 4096
                    && !diagnosis.evidence.is_empty()
                    && diagnosis.evidence.len() <= request.model.max_evidence_refs
                    && diagnosis
                        .evidence
                        .iter()
                        .all(|id| request.input.references.iter().any(|r| r.id == *id));
                incident.last_diagnostic_cursor = request.input.through_cursor;
                if !usage_valid || !valid {
                    incident.phase = Phase::Blocked(if usage_valid {
                        Block::Input
                    } else {
                        Block::UnknownUsage
                    });
                    escalate(incident, SupervisorReason::InvalidDiagnostic, o, now);
                } else {
                    incident.last_diagnosis = Some(DiagnosticRecord {
                        operation: request.operation.clone(),
                        source_revision: request.input.source_revision,
                        through_cursor: request.input.through_cursor,
                        at_ms: now,
                        diagnosis: diagnosis.clone(),
                    });
                    incident.phase = Phase::Cooldown {
                        until_ms: now.saturating_add(self.policy.cooldown_ms),
                    };
                    if diagnosis.uncertain {
                        escalate(incident, SupervisorReason::Uncertain, o, now);
                    } else if let Advice::Escalate(reason) = diagnosis.advice {
                        escalate(incident, reason, o, now);
                    }
                }
            }
            DiagnosticStatus::Failed { usage } => {
                // Unknown or malformed usage keeps the full reservation.
                let usage = match usage {
                    Usage::Known {
                        input_tokens,
                        output_tokens,
                    } if input_tokens <= request.model.max_input_tokens
                        && output_tokens <= request.model.max_output_tokens =>
                    {
                        usage
                    }
                    _ => Usage::Unknown,
                };
                settle_usage(incident, &reservation, usage, host);
                incident.phase = Phase::Blocked(Block::Provider);
                escalate(incident, SupervisorReason::DiagnosticUnavailable, o, now);
            }
            DiagnosticStatus::OutcomeUnknown => {
                settle_usage(incident, &reservation, Usage::Unknown, host);
                incident.phase = Phase::Blocked(Block::UnknownUsage);
                escalate(incident, SupervisorReason::ReconcileReceipt, o, now);
            }
        }
        Ok(())
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}

fn settle_usage(
    incident: &mut Incident,
    reservation: &Reservation,
    usage: Usage,
    host: &mut dyn Host,
) -> bool {
    let settled = host.settle(reservation, usage).is_ok();
    incident.pending_settlement = (!settled).then(|| reservation.clone());
    settled
}
fn valid_scope(scope: &Scope) -> bool {
    valid_id(&scope.project_id)
        && valid_id(&scope.work_id)
        && valid_id(&scope.assignment_id)
        && scope.generation > 0
        && scope.authority_epoch > 0
}

fn validate_checkpoint(cp: &Checkpoint, scope: &Scope) -> Result<(), Error> {
    if &cp.scope != scope || cp.next_check_ms < cp.last_check_ms {
        return Err(Error::InvalidCheckpoint);
    }
    let Some(i) = &cp.incident else {
        return Ok(());
    };
    let diagnostic_operation = |op: &OperationId| {
        op.incident == i.id
            && matches!(op.kind, OperationKind::Diagnostic(n) if n > 0 && n <= i.attempts)
    };
    if &i.id.scope != scope
        || i.opened_cursor > cp.cursor
        || i.source_revision > cp.source_revision
        || i.opened_at_ms > cp.last_check_ms
        || i.attempts > 8
        || i.last_diagnostic_cursor > cp.cursor
        || i.last_diagnostic_evidence_revision
            .is_some_and(|r| r > cp.source_revision)
        || i.pending_settlement.as_ref().is_some_and(|r| {
            !diagnostic_operation(&r.operation)
                || r.request.operation != r.operation
                || !valid_id(&r.receipt)
        })
        || i.last_diagnosis.as_ref().is_some_and(|r| {
            !diagnostic_operation(&r.operation)
                || r.source_revision > cp.source_revision
                || r.through_cursor > cp.cursor
                || r.at_ms > cp.last_check_ms
        })
        || i.escalation.as_ref().is_some_and(|e| {
            e.operation.incident != i.id
                || e.operation.kind != OperationKind::Escalation
                || e.deliveries > 8
                || e.source_revision > cp.source_revision
                || e.cursor > cp.cursor
                || (e.queued && e.deliveries == 0)
        })
    {
        return Err(Error::InvalidCheckpoint);
    }
    match &i.phase {
        Phase::Diagnosing {
            request,
            reservation,
        } if i.attempts == 0
            || request.operation.incident != i.id
            || request.operation.kind != OperationKind::Diagnostic(i.attempts)
            || reservation.operation != request.operation
            || reservation.request.as_ref() != request.as_ref()
            || !valid_id(&reservation.receipt)
            || request.input.scope != *scope
            || request.input.source_revision > cp.source_revision
            || request.input.through_cursor > cp.cursor
            || request.input.since_cursor > request.input.through_cursor
            || request.input.since_cursor != i.last_diagnostic_cursor
            || request.deadline_ms <= i.opened_at_ms =>
        {
            return Err(Error::InvalidCheckpoint)
        }
        Phase::Resolved { at_ms, cursor } if *at_ms > cp.last_check_ms || *cursor > cp.cursor => {
            return Err(Error::InvalidCheckpoint)
        }
        _ => {}
    }
    Ok(())
}

fn validate_observation(o: &Observation, scope: &Scope, now: u64) -> Result<(), Error> {
    if &o.scope != scope
        || o.cursor == 0
        || o.source_revision == 0
        || o.evidence_revision > o.source_revision
        || o.started_at_ms.is_some_and(|at| at > now)
        || o.heartbeat_at_ms.is_some_and(|at| at > now)
        || o.progress.is_some_and(|p| {
            p.sequence == 0 || p.at_ms > now || o.started_at_ms.is_some_and(|start| p.at_ms < start)
        })
    {
        return Err(Error::InvalidObservation);
    }
    Ok(())
}

fn valid_input(input: &DiagnosticInput, model: &Model, o: &Observation, since: u64) -> bool {
    input.scope == o.scope
        && input.source_revision == o.source_revision
        && input.since_cursor == since
        && input.through_cursor == o.cursor
        && input.since_cursor <= input.through_cursor
        && !input.summary.is_empty()
        && input.input_tokens > 0
        && input.input_tokens <= model.max_input_tokens
        && input
            .summary
            .len()
            .saturating_add(input.references.iter().map(|r| r.id.len()).sum::<usize>())
            <= model.max_evidence_bytes
        && !input.references.is_empty()
        && input.references.len() <= model.max_evidence_refs
        && input
            .references
            .iter()
            .all(|r| valid_id(&r.id) && r.revision > 0 && r.revision <= o.source_revision)
        && input.references.iter().enumerate().all(|(index, r)| {
            !input.references[..index]
                .iter()
                .any(|other| other.id == r.id)
        })
}

fn escalate(incident: &mut Incident, reason: SupervisorReason, o: &Observation, now: u64) {
    if incident.escalation.is_none() {
        incident.escalation = Some(Escalation {
            operation: OperationId {
                incident: incident.id.clone(),
                kind: OperationKind::Escalation,
            },
            reason,
            source_revision: o.source_revision,
            cursor: o.cursor,
            deliveries: 0,
            queued: false,
            next_delivery_ms: now,
        });
    }
}

fn save(
    host: &mut dyn Host,
    version: &mut Option<u64>,
    checkpoint: &mut Checkpoint,
) -> Result<(), Error> {
    checkpoint.next_check_ms = report(checkpoint, Status::Healthy).next_check_ms;
    *version = Some(host.save(*version, checkpoint)?);
    Ok(())
}

fn status(cp: &Checkpoint) -> Status {
    let Some(incident) = &cp.incident else {
        return Status::Healthy;
    };
    if matches!(incident.phase, Phase::Resolved { .. }) {
        return Status::Resolved;
    }
    if incident.escalation.is_some() {
        return Status::NeedsSupervisor;
    }
    match incident.phase {
        Phase::Open => Status::Incident,
        Phase::Diagnosing { .. } => Status::Diagnosing,
        Phase::Cooldown { .. } => Status::Cooldown,
        Phase::Blocked(_) => Status::Blocked,
        Phase::Resolved { .. } => Status::Resolved,
    }
}

fn report(cp: &Checkpoint, status: Status) -> Report {
    let mut next = cp.next_check_ms;
    if let Some(incident) = &cp.incident {
        let deadline = match &incident.phase {
            Phase::Diagnosing { request, .. } => Some(request.deadline_ms),
            Phase::Cooldown { until_ms } => Some(*until_ms),
            _ => None,
        };
        if let Some(deadline) = deadline.filter(|at| *at > cp.last_check_ms) {
            next = next.min(deadline);
        }
        if let Some(escalation) = &incident.escalation {
            if !escalation.queued && escalation.next_delivery_ms > cp.last_check_ms {
                next = next.min(escalation.next_delivery_ms);
            }
        }
    }
    Report {
        status,
        incident: cp.incident.as_ref().map(|i| i.id.clone()),
        next_check_ms: next,
    }
}
