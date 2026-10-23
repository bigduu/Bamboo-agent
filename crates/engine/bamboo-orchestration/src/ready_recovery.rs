//! Durable retry metadata, never a recovered execution grant or Task ledger.
//!
//! The outer Host supplies storage and a clock. A fresh in-memory Host policy
//! and the canonical Ticket snapshot are required on every admission.
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bamboo_tickets::{
    canonical_bytes, content_hash, Command, DispatchSpec, Error, Result, ScopeBinding, Snapshot,
};
use serde::{Deserialize, Serialize};

use crate::{ReadyBlockKind, ReadyReconcile, ReadyWorkGrant};

pub const READY_RETRY_LIMIT: u8 = 3;
pub const READY_RETRY_REASON_LIMIT: usize = 2048;

/// Stable across policy reinstallation, wall-clock changes and writer epochs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyRetryKey {
    pub scope_id: String,
    pub work_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadyRetryRequest {
    Start {
        operation_id: String,
        request_hash: String,
    },
    Intent {
        dispatch_key: String,
        spec_hash: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadyRetryPhase {
    Ready,
    Reserved,
    Deferred,
    Quarantined,
}

/// These bytes constrain retries. They cannot authorize Start, change canonical
/// ownership, release claims, prove progress or acknowledge a stop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyRetryRecord {
    pub identity_hash: String,
    pub authority_epoch: u64,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub next_check_at_ms: u64,
    pub attempts: u8,
    pub phase: ReadyRetryPhase,
    pub request: Option<ReadyRetryRequest>,
    pub block_kind: Option<ReadyBlockKind>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedReadyRetry {
    pub revision: u64,
    pub record: ReadyRetryRecord,
}

/// A successful CAS includes durable file/entry synchronization. A failed or
/// uncertain CAS never permits the caller to enter the runtime.
#[async_trait]
pub trait ReadyRetryStore: Send + Sync {
    async fn load(&self, key: &ReadyRetryKey) -> Result<Option<VersionedReadyRetry>>;
    async fn compare_exchange(
        &self,
        key: &ReadyRetryKey,
        expected: Option<u64>,
        next: ReadyRetryRecord,
    ) -> Result<VersionedReadyRetry>;
}

/// Absolute milliseconds permit cross-process deadlines; monotonic policy
/// expiry is checked separately. Backward or malformed clock observations fail
/// closed rather than extending a retry window.
pub trait ReadyRetryClock: Send + Sync {
    fn now_ms(&self) -> Result<u64>;
}

pub struct SystemReadyRetryClock;
impl ReadyRetryClock for SystemReadyRetryClock {
    fn now_ms(&self) -> Result<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::AuthorityUnavailable("retry clock predates its epoch".into()))?
            .as_millis()
            .try_into()
            .map_err(|_| Error::AuthorityUnavailable("retry clock overflow".into()))
    }
}

pub fn validate_ready_retry_record(record: &ReadyRetryRecord) -> Result<()> {
    fn hash(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }
    let request_valid = match &record.request {
        None => record.phase != ReadyRetryPhase::Reserved,
        Some(ReadyRetryRequest::Start {
            operation_id,
            request_hash,
        }) => !operation_id.is_empty() && operation_id.len() <= 256 && hash(request_hash),
        Some(ReadyRetryRequest::Intent {
            dispatch_key,
            spec_hash,
        }) => !dispatch_key.is_empty() && dispatch_key.len() <= 2048 && hash(spec_hash),
    };
    if !hash(&record.identity_hash)
        || record.authority_epoch == 0
        || record.attempts > READY_RETRY_LIMIT
        || record.reason.len() > READY_RETRY_REASON_LIMIT
        || record.expires_at_ms <= record.created_at_ms
        || record.expires_at_ms - record.created_at_ms > 3_600_000
        || record.last_seen_at_ms < record.created_at_ms
        || record.next_check_at_ms < record.created_at_ms
        || record.next_check_at_ms > record.expires_at_ms
        || !request_valid
        || record.phase == ReadyRetryPhase::Reserved && record.attempts == 0
    {
        return Err(Error::AuthorityUnavailable(
            "invalid durable Ready retry metadata".into(),
        ));
    }
    Ok(())
}

/// A writer cannot silently reset a consumed attempt or extend the original
/// window. Quarantine is intentionally terminal for this stable Work key.
pub fn validate_ready_retry_transition(
    previous: &ReadyRetryRecord,
    next: &ReadyRetryRecord,
) -> Result<()> {
    validate_ready_retry_record(next)?;
    if previous.identity_hash != next.identity_hash
        || previous.created_at_ms != next.created_at_ms
        || previous.expires_at_ms != next.expires_at_ms
        || next.attempts < previous.attempts
        || next.last_seen_at_ms < previous.last_seen_at_ms
        || next.authority_epoch < previous.authority_epoch
        || previous.phase == ReadyRetryPhase::Quarantined
            && next.phase != ReadyRetryPhase::Quarantined
    {
        return Err(Error::InvalidTransition(
            "durable retry identity/count/deadline cannot be reset".into(),
        ));
    }
    Ok(())
}

pub(crate) struct RetryContext<'a> {
    pub binding: &'a ScopeBinding,
    pub project_id: &'a bamboo_domain::ProjectId,
    pub grant: &'a ReadyWorkGrant,
    pub max_rounds: u32,
    pub remaining: Duration,
    pub snapshot: &'a Snapshot,
    pub pending_key: Option<&'a str>,
}

pub(crate) struct RetryPermit {
    key: ReadyRetryKey,
    value: VersionedReadyRetry,
}

pub(crate) enum RetryGate {
    Permit(RetryPermit),
    Report(ReadyReconcile),
}

pub(crate) struct ReadyRecovery {
    store: Arc<dyn ReadyRetryStore>,
    clock: Arc<dyn ReadyRetryClock>,
}

#[derive(Clone)]
pub(crate) struct RetryDeadline {
    clock: Arc<dyn ReadyRetryClock>,
    floor_ms: u64,
    expires_ms: u64,
}

impl RetryDeadline {
    pub fn check(&self) -> Result<()> {
        let now = self.clock.now_ms()?;
        if now < self.floor_ms || now >= self.expires_ms {
            return Err(Error::ResourceBlocked(
                "durable retry deadline/floor no longer permits admission".into(),
            ));
        }
        Ok(())
    }
}

impl ReadyRecovery {
    pub fn new(store: Arc<dyn ReadyRetryStore>, clock: Arc<dyn ReadyRetryClock>) -> Self {
        Self { store, clock }
    }

    pub fn deadline(&self, permit: &RetryPermit) -> RetryDeadline {
        RetryDeadline {
            clock: self.clock.clone(),
            floor_ms: permit.value.record.last_seen_at_ms,
            expires_ms: permit.value.record.expires_at_ms,
        }
    }

    async fn save(&self, permit: &mut RetryPermit) -> Result<()> {
        validate_ready_retry_record(&permit.value.record)?;
        let next = self
            .store
            .compare_exchange(
                &permit.key,
                Some(permit.value.revision),
                permit.value.record.clone(),
            )
            .await?;
        if next.revision
            != permit
                .value
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::AuthorityUnavailable("retry revision overflow".into()))?
            || next.record != permit.value.record
        {
            return Err(Error::AuthorityUnavailable(
                "retry store returned a mismatched CAS receipt".into(),
            ));
        }
        permit.value = next;
        Ok(())
    }

    pub async fn check(&self, context: RetryContext<'_>) -> Result<RetryGate> {
        let now = self.clock.now_ms()?;
        let key = ReadyRetryKey {
            scope_id: context.binding.scope_id.clone(),
            work_id: context.grant.work_id.clone(),
        };
        let identity_hash = content_hash(&canonical_bytes(&(
            context.binding,
            context.project_id,
            &context.grant.work_id,
            context.grant.contract_revision,
            &context.grant.context_hash,
            &context.grant.workspace,
            context.max_rounds,
        ))?);
        let value = match self.store.load(&key).await? {
            Some(value) => value,
            None => {
                let remaining: u64 = context
                    .remaining
                    .as_millis()
                    .try_into()
                    .map_err(|_| Error::ContextBudgetExceeded)?;
                let record = ReadyRetryRecord {
                    identity_hash: identity_hash.clone(),
                    authority_epoch: context.snapshot.authority_epoch,
                    created_at_ms: now,
                    expires_at_ms: now
                        .checked_add(remaining)
                        .ok_or(Error::ContextBudgetExceeded)?,
                    last_seen_at_ms: now,
                    next_check_at_ms: now,
                    attempts: 0,
                    phase: ReadyRetryPhase::Ready,
                    request: None,
                    block_kind: None,
                    reason: String::new(),
                };
                validate_ready_retry_record(&record)?;
                let value = self
                    .store
                    .compare_exchange(&key, None, record.clone())
                    .await?;
                if value.revision != 1 || value.record != record {
                    return Err(Error::AuthorityUnavailable(
                        "retry initialization receipt mismatch".into(),
                    ));
                }
                value
            }
        };
        validate_ready_retry_record(&value.record)?;
        if value.revision == 0 {
            return Err(Error::AuthorityUnavailable("invalid retry revision".into()));
        }
        // Storage can await a competing writer. Compare its clock floor to a
        // fresh sample, not the stale sample taken before that await.
        let now = self.clock.now_ms()?;
        let mut permit = RetryPermit { key, value };
        if permit.value.record.identity_hash != identity_hash {
            return Ok(RetryGate::Report(blocked(
                &permit,
                ReadyBlockKind::Permission,
                "durable retry identity differs from the fresh Host decision",
            )));
        }
        if permit.value.record.phase == ReadyRetryPhase::Quarantined {
            return Ok(RetryGate::Report(report(&permit)));
        }
        if now < permit.value.record.last_seen_at_ms {
            return self
                .quarantine(
                    permit,
                    ReadyBlockKind::Configuration,
                    "retry clock moved backwards",
                )
                .await;
        }
        if now >= permit.value.record.expires_at_ms {
            return self
                .quarantine(
                    permit,
                    ReadyBlockKind::Configuration,
                    "original durable retry deadline expired",
                )
                .await;
        }
        if permit.value.record.authority_epoch != context.snapshot.authority_epoch {
            // Only never-admitted generation-zero Work can use a newly explicit
            // Host policy after a writer restart. Keep its old deadline/count.
            let work = &context.snapshot.tickets[&context.grant.work_id];
            if permit.value.record.request.is_some()
                || work.generation != 0
                || work.active_assignment.is_some()
                || context.snapshot.authority_epoch < permit.value.record.authority_epoch
            {
                return self
                    .quarantine(
                        permit,
                        ReadyBlockKind::OutcomeUnknown,
                        "writer epoch changed; original execution needs canonical reconciliation",
                    )
                    .await;
            }
            permit.value.record.authority_epoch = context.snapshot.authority_epoch;
            self.save(&mut permit).await?;
        }
        if permit.value.record.attempts >= READY_RETRY_LIMIT {
            return self
                .quarantine(
                    permit,
                    ReadyBlockKind::Transient,
                    "durable admission attempt limit exhausted",
                )
                .await;
        }
        if now < permit.value.record.next_check_at_ms {
            return Ok(RetryGate::Report(ReadyReconcile::Deferred {
                work_id: permit.key.work_id.clone(),
                next_check_at_ms: permit.value.record.next_check_at_ms,
                attempts: permit.value.record.attempts,
                reason: permit.value.record.reason.clone(),
            }));
        }
        if permit.value.record.request.is_some() {
            let intent = canonical_intent(&permit.value.record, context.snapshot)?;
            let Some(intent) =
                intent.filter(|intent| context.pending_key == Some(intent.dispatch_key.as_str()))
            else {
                return self
                    .quarantine(
                        permit,
                        ReadyBlockKind::OutcomeUnknown,
                        "interrupted admission has no matching original pending receipt",
                    )
                    .await;
            };
            if intent.immutable_spec.authority_epoch != context.snapshot.authority_epoch
                || intent.immutable_spec.generation != 1
            {
                return self
                    .quarantine(
                        permit,
                        ReadyBlockKind::OutcomeUnknown,
                        "pending receipt has a stale execution fence",
                    )
                    .await;
            }
        }
        permit.value.record.last_seen_at_ms = now;
        // Persist the observed clock floor before any asynchronous preflight.
        self.save(&mut permit).await?;
        Ok(RetryGate::Permit(permit))
    }

    async fn quarantine(
        &self,
        mut permit: RetryPermit,
        kind: ReadyBlockKind,
        reason: &str,
    ) -> Result<RetryGate> {
        permit.value.record.phase = ReadyRetryPhase::Quarantined;
        permit.value.record.block_kind = Some(kind);
        permit.value.record.reason = reason.into();
        self.save(&mut permit).await?;
        Ok(RetryGate::Report(report(&permit)))
    }

    pub async fn reserve_start(
        &self,
        permit: RetryPermit,
        command: &Command,
    ) -> Result<RetryPermit> {
        self.reserve(
            permit,
            ReadyRetryRequest::Start {
                operation_id: command.operation_id.clone(),
                request_hash: content_hash(&canonical_bytes(command)?),
            },
        )
        .await
    }

    pub async fn reserve_intent(
        &self,
        permit: RetryPermit,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<RetryPermit> {
        self.reserve(
            permit,
            ReadyRetryRequest::Intent {
                dispatch_key: key.into(),
                spec_hash: content_hash(&canonical_bytes(spec)?),
            },
        )
        .await
    }

    async fn reserve(
        &self,
        mut permit: RetryPermit,
        request: ReadyRetryRequest,
    ) -> Result<RetryPermit> {
        let now = self.check_time(&permit)?;
        if permit.value.record.attempts >= READY_RETRY_LIMIT {
            return Err(Error::ResourceBlocked(
                "durable retry attempts exhausted".into(),
            ));
        }
        permit.value.record.attempts += 1;
        permit.value.record.phase = ReadyRetryPhase::Reserved;
        permit.value.record.request = Some(request);
        permit.value.record.last_seen_at_ms = now;
        permit.value.record.next_check_at_ms =
            next_check(&permit.value.record, backoff(permit.value.record.attempts));
        permit.value.record.reason =
            "admission reserved; original receipt must settle this attempt".into();
        self.save(&mut permit).await?;
        Ok(permit)
    }

    /// Must run again after durable reservation and immediately before runtime.
    pub fn check_time(&self, permit: &RetryPermit) -> Result<u64> {
        let now = self.clock.now_ms()?;
        if now < permit.value.record.last_seen_at_ms || now >= permit.value.record.expires_at_ms {
            return Err(Error::ResourceBlocked(
                "durable retry clock/deadline no longer permits admission".into(),
            ));
        }
        Ok(now)
    }

    pub async fn failure(
        &self,
        mut permit: RetryPermit,
        mut kind: ReadyBlockKind,
        reason: String,
        snapshot: &Snapshot,
    ) -> Result<ReadyReconcile> {
        let reserved = permit.value.record.phase == ReadyRetryPhase::Reserved;
        if reserved
            && kind == ReadyBlockKind::Transient
            && canonical_intent(&permit.value.record, snapshot)?.is_none()
        {
            kind = ReadyBlockKind::OutcomeUnknown;
        }
        if !reserved && kind == ReadyBlockKind::Transient {
            permit.value.record.attempts = permit
                .value
                .record
                .attempts
                .saturating_add(1)
                .min(READY_RETRY_LIMIT);
        }
        permit.value.record.phase = if matches!(
            kind,
            ReadyBlockKind::Configuration
                | ReadyBlockKind::Permission
                | ReadyBlockKind::OutcomeUnknown
        ) || permit.value.record.attempts >= READY_RETRY_LIMIT
        {
            ReadyRetryPhase::Quarantined
        } else {
            ReadyRetryPhase::Deferred
        };
        permit.value.record.block_kind = Some(kind);
        permit.value.record.reason = truncate_reason(reason);
        permit.value.record.last_seen_at_ms = permit
            .value
            .record
            .last_seen_at_ms
            .max(self.clock.now_ms()?);
        permit.value.record.next_check_at_ms = next_check(
            &permit.value.record,
            if kind == ReadyBlockKind::Resource {
                5000
            } else {
                backoff(permit.value.record.attempts)
            },
        );
        self.save(&mut permit).await?;
        Ok(report(&permit))
    }

    pub async fn observed(
        &self,
        mut permit: RetryPermit,
        dispatch_key: &str,
        snapshot: &Snapshot,
    ) -> Result<()> {
        let intent = canonical_intent(&permit.value.record, snapshot)?
            .filter(|intent| intent.dispatch_key == dispatch_key)
            .ok_or_else(|| {
                Error::AuthorityUnavailable(
                    "admission observation has no matching canonical receipt".into(),
                )
            })?;
        permit.value.record.request = Some(ReadyRetryRequest::Intent {
            dispatch_key: dispatch_key.into(),
            spec_hash: intent.spec_hash.clone(),
        });
        permit.value.record.phase = ReadyRetryPhase::Deferred;
        permit.value.record.block_kind = None;
        permit.value.record.reason = "awaiting the original canonical runtime receipt".into();
        permit.value.record.last_seen_at_ms = permit
            .value
            .record
            .last_seen_at_ms
            .max(self.clock.now_ms()?);
        permit.value.record.next_check_at_ms = next_check(&permit.value.record, 5000);
        self.save(&mut permit).await
    }

    pub async fn conflict(&self, mut permit: RetryPermit) -> Result<()> {
        // Only a definite, synchronous scope-CAS rejection proves no Start was
        // published. Its consumed attempt/deadline survive the fresh selection.
        permit.value.record.request = None;
        permit.value.record.phase = ReadyRetryPhase::Deferred;
        permit.value.record.block_kind = Some(ReadyBlockKind::Transient);
        permit.value.record.reason = "scope CAS rejected; fresh eligibility required".into();
        permit.value.record.last_seen_at_ms = permit
            .value
            .record
            .last_seen_at_ms
            .max(self.clock.now_ms()?);
        permit.value.record.next_check_at_ms =
            next_check(&permit.value.record, backoff(permit.value.record.attempts));
        self.save(&mut permit).await
    }
}

fn canonical_intent<'a>(
    record: &ReadyRetryRecord,
    snapshot: &'a Snapshot,
) -> Result<Option<&'a bamboo_tickets::DispatchIntent>> {
    match &record.request {
        None => Ok(None),
        Some(ReadyRetryRequest::Start {
            operation_id,
            request_hash,
        }) => {
            let Some(receipt) = snapshot.receipts.get(operation_id) else {
                return Ok(None);
            };
            if receipt.request_hash != *request_hash {
                return Err(Error::IdempotencyConflict);
            }
            let Some(assignment) = receipt
                .ids
                .get("assignment")
                .and_then(|id| snapshot.assignments.get(id))
            else {
                return Err(Error::AuthorityUnavailable(
                    "original Start receipt has no Assignment".into(),
                ));
            };
            Ok(snapshot.intents.get(&assignment.dispatch_key))
        }
        Some(ReadyRetryRequest::Intent {
            dispatch_key,
            spec_hash,
        }) => {
            let Some(intent) = snapshot.intents.get(dispatch_key) else {
                return Ok(None);
            };
            if intent.spec_hash != *spec_hash {
                return Err(Error::IdempotencyConflict);
            }
            Ok(Some(intent))
        }
    }
}

fn next_check(record: &ReadyRetryRecord, delay: u64) -> u64 {
    record
        .last_seen_at_ms
        .saturating_add(delay)
        .min(record.expires_at_ms)
}
fn backoff(attempts: u8) -> u64 {
    1000u64 << attempts.saturating_sub(1).min(5)
}
fn truncate_reason(mut reason: String) -> String {
    if reason.len() > READY_RETRY_REASON_LIMIT {
        let mut end = READY_RETRY_REASON_LIMIT;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
    }
    reason
}
fn blocked(permit: &RetryPermit, kind: ReadyBlockKind, reason: &str) -> ReadyReconcile {
    ReadyReconcile::Blocked {
        work_id: permit.key.work_id.clone(),
        kind,
        reason: reason.into(),
    }
}
fn report(permit: &RetryPermit) -> ReadyReconcile {
    blocked(
        permit,
        permit
            .value
            .record
            .block_kind
            .unwrap_or(ReadyBlockKind::Resource),
        &permit.value.record.reason,
    )
}
