//! Token facts in the original Ticket HEAD transaction. No provider, clock,
//! scheduler, sidecar wallet or recovered execution grant lives here.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    canonical_bytes, content_hash, service::validate_authority, Authority, Error, Health,
    Principal, Result, Snapshot, TicketKind, TicketService,
};

const DAY_MS: u64 = 86_400_000;
const MAX_RECORDS: usize = 1024;
const MAX_LIMIT: u64 = 1_000_000_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenLimits {
    pub work_tokens: BTreeMap<String, u64>,
    pub utc_day_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBudgetPurpose {
    Worker,
    Patrol,
    SupervisorDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenRequest {
    pub operation_id: String,
    pub project_id: bamboo_domain::ProjectId,
    pub work_id: String,
    pub purpose: ExecutionBudgetPurpose,
    /// Hash of the complete immutable provider request, including attempt scope.
    pub request_hash: String,
    pub observed_seq: u64,
    pub input_cap: u32,
    pub output_cap: u32,
    pub attempt: Option<WorkTokenAttempt>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenAttempt {
    pub assignment_id: String,
    pub generation: u64,
    pub authority_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenConfiguration {
    pub revision: u64,
    pub user_id: String,
    pub committed_seq: u64,
    pub limits: WorkTokenLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTokenUsage {
    Known {
        input_tokens: u32,
        output_tokens: u32,
    },
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenReservation {
    pub request: WorkTokenRequest,
    pub receipt: String,
    pub utc_day: u64,
    /// None/Unknown both retain the full cap, including across process exit.
    pub usage: Option<ExecutionTokenUsage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkTokenBlock {
    Work,
    Daily,
    Total,
    Capacity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkTokenAdmission {
    Reserved(WorkTokenReservation),
    Blocked(WorkTokenBlock),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenBudget {
    pub revision: u64,
    pub limits: WorkTokenLimits,
    pub current_utc_day: Option<u64>,
    pub last_reservation_ms: Option<u64>,
    pub total_charged: u64,
    pub daily_charged: u64,
    pub work_charged: BTreeMap<String, u64>,
    pub reservations: BTreeMap<String, WorkTokenReservation>,
    pub configurations: BTreeMap<u64, WorkTokenConfiguration>,
}

fn invalid(message: &str) -> Error {
    Error::InvalidTransition(message.into())
}
fn runtime(authority: &Authority) -> Result<()> {
    if matches!(authority.principal(), Principal::Runtime) {
        Ok(())
    } else {
        Err(Error::ScopeDenied(
            "trusted Runtime token accounting required".into(),
        ))
    }
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn valid_request(request: &WorkTokenRequest) -> Result<()> {
    if request.operation_id.is_empty()
        || request.operation_id.len() > 256
        || request.work_id.is_empty()
        || request.work_id.len() > 256
        || !hash(&request.request_hash)
        || request.input_cap == 0
        || request.output_cap == 0
        || request.input_cap > 1_048_576
        || request.output_cap > 1_048_576
    {
        return Err(invalid("invalid bounded Work token request"));
    }
    if (request.purpose == ExecutionBudgetPurpose::Patrol && request.attempt.is_none())
        || request.attempt.as_ref().is_some_and(|attempt| {
            attempt.assignment_id.is_empty()
                || attempt.assignment_id.len() > 256
                || attempt.generation == 0
                || attempt.authority_epoch == 0
        })
    {
        return Err(invalid("invalid token attempt identity"));
    }
    Ok(())
}

fn current_attempt(snapshot: &Snapshot, request: &WorkTokenRequest) -> Result<()> {
    let Some(attempt) = &request.attempt else {
        return Ok(());
    };
    let work = snapshot
        .tickets
        .get(&request.work_id)
        .ok_or_else(|| invalid("budget Work missing"))?;
    let assignment = snapshot
        .assignments
        .get(&attempt.assignment_id)
        .ok_or_else(|| Error::ScopeDenied("budget Assignment missing".into()))?;
    let intent = snapshot
        .intents
        .get(&assignment.dispatch_key)
        .ok_or_else(|| Error::ScopeDenied("budget DispatchSpec missing".into()))?;
    let current_submission = work
        .current_submission
        .as_ref()
        .and_then(|id| snapshot.submissions.get(id))
        .is_some_and(|submission| submission.assignment_id == assignment.id);
    if assignment.work_id != work.id
        || work.generation != attempt.generation
        || assignment.generation != attempt.generation
        || assignment.contract_revision != work.contract_revision
        || intent.assignment_id != assignment.id
        || intent.dispatch_key != assignment.dispatch_key
        || intent.immutable_spec.assignment_id != assignment.id
        || intent.immutable_spec.work_id != work.id
        || intent.immutable_spec.generation != attempt.generation
        || intent.immutable_spec.contract_revision != work.contract_revision
        || intent.immutable_spec.authority_epoch != attempt.authority_epoch
        || intent.immutable_spec.binding != snapshot.binding
        || intent.spec_hash != content_hash(&canonical_bytes(&intent.immutable_spec)?)
        || assignment.runtime != intent.receipt
        || (work.active_assignment.as_ref() != Some(&assignment.id) && !current_submission)
        || matches!(
            work.state,
            crate::WorkState::Accepted | crate::WorkState::Cancelled
        )
    {
        return Err(Error::ScopeDenied(
            "token reservation attempt changed".into(),
        ));
    }
    Ok(())
}

fn charge(record: &WorkTokenReservation) -> Result<u64> {
    let request = &record.request;
    match record.usage {
        Some(ExecutionTokenUsage::Known {
            input_tokens,
            output_tokens,
        }) => {
            if input_tokens > request.input_cap || output_tokens > request.output_cap {
                return Err(invalid("measured token usage exceeds the reserved caps"));
            }
            Ok(u64::from(input_tokens) + u64::from(output_tokens))
        }
        None | Some(ExecutionTokenUsage::Unknown) => {
            Ok(u64::from(request.input_cap) + u64::from(request.output_cap))
        }
    }
}
fn limits_valid(limits: &WorkTokenLimits, snapshot: &Snapshot) -> Result<()> {
    if limits.work_tokens.is_empty()
        || limits.work_tokens.len() > 64
        || !(1..=MAX_LIMIT).contains(&limits.utc_day_tokens)
        || !(1..=MAX_LIMIT).contains(&limits.total_tokens)
    {
        return Err(invalid("finite Work/day/total token limits are required"));
    }
    for (work_id, limit) in &limits.work_tokens {
        if !(1..=MAX_LIMIT).contains(limit)
            || snapshot
                .tickets
                .get(work_id)
                .is_none_or(|ticket| ticket.kind != TicketKind::Work)
        {
            return Err(invalid(
                "budget limit needs an existing Work and finite cap",
            ));
        }
    }
    Ok(())
}

/// Validate persisted counters against original immutable reservation facts.
/// Unknown charges are recomputed conservatively; limits never supply evidence
/// that an execution stopped, completed, or can be admitted again.
pub(crate) fn validate(snapshot: &Snapshot) -> Result<()> {
    let Some(budget) = &snapshot.execution_budget else {
        if snapshot.schema >= 8 {
            return Err(invalid("budget schema cannot erase its canonical ledger"));
        }
        return Ok(());
    };
    if snapshot.schema < 8 || budget.revision == 0 || budget.reservations.len() > MAX_RECORDS {
        return Err(invalid(
            "invalid Work token budget schema/revision/capacity",
        ));
    }
    limits_valid(&budget.limits, snapshot)?;
    if budget.configurations.len() > 128 || budget.configurations.len() as u64 != budget.revision {
        return Err(invalid(
            "budget configuration audit capacity/revision mismatch",
        ));
    }
    for revision in 1..=budget.revision {
        let configuration = budget
            .configurations
            .get(&revision)
            .ok_or_else(|| invalid("budget configuration audit gap"))?;
        if configuration.revision != revision
            || configuration.user_id.is_empty()
            || configuration.user_id.len() > 256
            || configuration.committed_seq > snapshot.seq
        {
            return Err(invalid("invalid explicit User budget decision"));
        }
        limits_valid(&configuration.limits, snapshot)?;
    }
    if budget
        .configurations
        .get(&budget.revision)
        .is_none_or(|decision| decision.limits != budget.limits)
    {
        return Err(invalid(
            "budget limits differ from their explicit User decision",
        ));
    }
    let mut total = 0_u64;
    let mut daily = 0_u64;
    let mut works = BTreeMap::<String, u64>::new();
    for (id, record) in &budget.reservations {
        valid_request(&record.request)?;
        if id != &record.request.operation_id
            || record.receipt
                != content_hash(&canonical_bytes(&(&snapshot.binding, &record.request))?)
            || budget
                .current_utc_day
                .is_none_or(|day| record.utc_day > day)
        {
            return Err(invalid("invalid immutable Work token receipt"));
        }
        let charged = charge(record)?;
        total = total
            .checked_add(charged)
            .ok_or_else(|| invalid("token total overflow"))?;
        let work = works.entry(record.request.work_id.clone()).or_default();
        *work = work
            .checked_add(charged)
            .ok_or_else(|| invalid("Work tokens overflow"))?;
        if Some(record.utc_day) == budget.current_utc_day {
            daily = daily
                .checked_add(charged)
                .ok_or_else(|| invalid("daily tokens overflow"))?;
        }
    }
    if total != budget.total_charged
        || daily != budget.daily_charged
        || works != budget.work_charged
        || total > budget.limits.total_tokens
        || daily > budget.limits.utc_day_tokens
        || works.iter().any(|(work, used)| {
            budget
                .limits
                .work_tokens
                .get(work)
                .is_none_or(|limit| used > limit)
        })
        || budget.last_reservation_ms.map(|ms| ms / DAY_MS) != budget.current_utc_day
    {
        return Err(invalid(
            "Work token counters/limits do not match canonical reservations",
        ));
    }
    Ok(())
}

impl TicketService {
    /// Host-only explicit owner decision. No default limit, model-facing tool
    /// or role promotion is installed by this method.
    pub fn configure_work_token_budget(
        &self,
        authority: &Authority,
        expected_revision: u64,
        limits: WorkTokenLimits,
    ) -> Result<u64> {
        let mut store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no Ticket snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        let Principal::User { user_id } = authority.principal() else {
            return Err(Error::ScopeDenied(
                "explicit User token-budget decision required".into(),
            ));
        };
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "token ledger is not writable".into(),
            ));
        }
        limits_valid(&limits, snapshot)?;
        if snapshot
            .execution_budget
            .as_ref()
            .map_or(0, |budget| budget.revision)
            != expected_revision
        {
            return Err(Error::RevisionConflict);
        }
        let mut next = snapshot.clone();
        let budget = next
            .execution_budget
            .get_or_insert_with(|| WorkTokenBudget {
                revision: 0,
                limits: limits.clone(),
                current_utc_day: None,
                last_reservation_ms: None,
                total_charged: 0,
                daily_charged: 0,
                work_charged: BTreeMap::new(),
                reservations: BTreeMap::new(),
                configurations: BTreeMap::new(),
            });
        if budget.configurations.len() >= 128 {
            return Err(Error::ResourceBlocked(
                "budget configuration audit is full".into(),
            ));
        }
        budget.limits = limits;
        budget.revision = budget
            .revision
            .checked_add(1)
            .ok_or(Error::RevisionConflict)?;
        let revision = budget.revision;
        let configuration_limits = budget.limits.clone();
        next.schema = next.schema.max(8);
        next.seq = next.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
        next.execution_budget
            .as_mut()
            .expect("configured")
            .configurations
            .insert(
                revision,
                WorkTokenConfiguration {
                    revision,
                    user_id: user_id.clone(),
                    committed_seq: next.seq,
                    limits: configuration_limits,
                },
            );
        crate::service::validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(revision)
    }

    pub fn work_token_budget(&self, authority: &Authority) -> Result<Option<WorkTokenBudget>> {
        let store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no Ticket snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        runtime(authority)?;
        Ok(snapshot.execution_budget.clone())
    }

    /// Atomic reserve before any provider submission. This current Host clock
    /// value is not inferred from a Ticket sequence, heartbeat or model output.
    pub fn reserve_work_tokens(
        &self,
        authority: &Authority,
        request: &WorkTokenRequest,
        now_ms: u64,
    ) -> Result<WorkTokenAdmission> {
        let mut store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no Ticket snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        runtime(authority)?;
        valid_request(request)?;
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "token ledger is not writable".into(),
            ));
        }
        if snapshot
            .tickets
            .get(&request.work_id)
            .is_none_or(|ticket| ticket.kind != TicketKind::Work)
            || snapshot.project.links.get(&request.work_id) != Some(&request.project_id)
        {
            return Err(Error::ScopeDenied(
                "token request Work/Project mismatch".into(),
            ));
        }
        let budget = snapshot
            .execution_budget
            .as_ref()
            .ok_or_else(|| Error::ResourceBlocked("Work token limits are not configured".into()))?;
        if let Some(record) = budget.reservations.get(&request.operation_id) {
            return if record.request == *request {
                Ok(WorkTokenAdmission::Reserved(record.clone()))
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if request.observed_seq != snapshot.seq {
            return Err(Error::RevisionConflict);
        }
        current_attempt(snapshot, request)?;
        if budget.last_reservation_ms.is_some_and(|last| now_ms < last) {
            return Err(Error::ResourceBlocked("Work token clock regressed".into()));
        }
        if budget.reservations.len() >= MAX_RECORDS {
            return Ok(WorkTokenAdmission::Blocked(WorkTokenBlock::Capacity));
        }
        let tokens = u64::from(request.input_cap) + u64::from(request.output_cap);
        let used = budget
            .work_charged
            .get(&request.work_id)
            .copied()
            .unwrap_or(0);
        if budget
            .limits
            .work_tokens
            .get(&request.work_id)
            .is_none_or(|limit| tokens > limit.saturating_sub(used))
        {
            return Ok(WorkTokenAdmission::Blocked(WorkTokenBlock::Work));
        }
        let day = now_ms / DAY_MS;
        let daily = if Some(day) == budget.current_utc_day {
            budget.daily_charged
        } else {
            0
        };
        if tokens > budget.limits.utc_day_tokens.saturating_sub(daily) {
            return Ok(WorkTokenAdmission::Blocked(WorkTokenBlock::Daily));
        }
        if tokens
            > budget
                .limits
                .total_tokens
                .saturating_sub(budget.total_charged)
        {
            return Ok(WorkTokenAdmission::Blocked(WorkTokenBlock::Total));
        }
        let record = WorkTokenReservation {
            request: request.clone(),
            receipt: content_hash(&canonical_bytes(&(&snapshot.binding, request))?),
            utc_day: day,
            usage: None,
        };
        let mut next = snapshot.clone();
        let budget = next.execution_budget.as_mut().expect("configured");
        budget.current_utc_day = Some(day);
        budget.last_reservation_ms = Some(now_ms);
        budget.daily_charged = daily
            .checked_add(tokens)
            .ok_or_else(|| invalid("daily token overflow"))?;
        budget.total_charged = budget
            .total_charged
            .checked_add(tokens)
            .ok_or_else(|| invalid("total token overflow"))?;
        let work_charge = budget
            .work_charged
            .entry(request.work_id.clone())
            .or_default();
        *work_charge = work_charge
            .checked_add(tokens)
            .ok_or_else(|| invalid("Work token overflow"))?;
        budget
            .reservations
            .insert(request.operation_id.clone(), record.clone());
        next.seq = next.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
        crate::service::validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(WorkTokenAdmission::Reserved(record))
    }

    /// Reconcile only the original receipt. Unknown usage retains all caps;
    /// late known usage may settle that debt once, never rewrite known usage.
    pub fn settle_work_tokens(
        &self,
        authority: &Authority,
        operation_id: &str,
        request_hash: &str,
        receipt: &str,
        usage: ExecutionTokenUsage,
    ) -> Result<WorkTokenReservation> {
        let mut store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no Ticket snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        runtime(authority)?;
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "token ledger is not writable".into(),
            ));
        }
        let budget = snapshot
            .execution_budget
            .as_ref()
            .ok_or_else(|| Error::ResourceBlocked("token ledger unavailable".into()))?;
        let old = budget
            .reservations
            .get(operation_id)
            .ok_or_else(|| Error::ScopeDenied("unknown token receipt".into()))?;
        if old.receipt != receipt || old.request.request_hash != request_hash {
            return Err(Error::IdempotencyConflict);
        }
        if old.usage == Some(usage) {
            return Ok(old.clone());
        }
        if matches!(old.usage, Some(ExecutionTokenUsage::Known { .. })) {
            return Err(Error::IdempotencyConflict);
        }
        let mut record = old.clone();
        record.usage = Some(usage);
        let refund = charge(old)?
            .checked_sub(charge(&record)?)
            .ok_or_else(|| invalid("token settlement increases a reservation"))?;
        let mut next = snapshot.clone();
        let budget = next.execution_budget.as_mut().expect("configured");
        budget.total_charged = budget
            .total_charged
            .checked_sub(refund)
            .ok_or_else(|| invalid("total token refund underflow"))?;
        let work_charge = budget
            .work_charged
            .get_mut(&record.request.work_id)
            .ok_or_else(|| invalid("charged Work missing"))?;
        *work_charge = work_charge
            .checked_sub(refund)
            .ok_or_else(|| invalid("Work token refund underflow"))?;
        if Some(record.utc_day) == budget.current_utc_day {
            budget.daily_charged = budget
                .daily_charged
                .checked_sub(refund)
                .ok_or_else(|| invalid("daily token refund underflow"))?;
        }
        budget
            .reservations
            .insert(operation_id.into(), record.clone());
        next.seq = next.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
        crate::service::validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(record)
    }
}
