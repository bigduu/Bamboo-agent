//! Bounded Ready Work orchestration over the existing canonical Ticket ledger.
//!
//! A process-local Host policy grants one initial attempt per Work. This crate
//! owns selection and wake/retry state only; the Host checks current eligibility
//! and dispatches through its existing worker runtime. No model, broker, lease,
//! persistence store or recovered execution grant is introduced here.
pub mod patrol;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use async_trait::async_trait;
use bamboo_tickets::{
    AssignmentState, Authority, ClaimCandidate, ClaimDecision, Command, DispatchSpec, EffectState,
    Error, ExecutionWorkspace, Operation, Result, ScopeBinding, SnapshotRef, TicketService,
    WorkState, CLAIM_CANDIDATE_LIMIT,
};
use tokio::{sync::Notify, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const MIN_WAKE_INTERVAL: Duration = Duration::from_millis(100);
const TRANSIENT_ATTEMPT_LIMIT: u8 = 3;

/// One explicitly authorized initial attempt, never a client/model DTO.
#[derive(Clone, Debug)]
pub struct ReadyWorkGrant {
    pub work_id: String,
    pub contract_revision: u64,
    pub context_hash: String,
    pub workspace: ExecutionWorkspace,
}

/// A finite single-Project Host decision, never persisted or deserialized.
/// Generation-zero Work only: retries, continuations, cumulative Work budgets
/// and foreground/background fairness remain outside this slice.
#[derive(Clone, Debug)]
pub struct ReadyWorkPolicy {
    pub project_id: bamboo_domain::ProjectId,
    pub grants: Vec<ReadyWorkGrant>,
    pub capacity: usize,
    /// Initial native run ceiling; this is not a cumulative money budget.
    pub max_rounds: u32,
    pub lifetime: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyBlockKind {
    Configuration,
    Permission,
    Resource,
    Transient,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadyReconcile {
    Disabled,
    Idle,
    CapacityBlocked {
        occupied: usize,
        capacity: usize,
    },
    Blocked {
        work_id: String,
        kind: ReadyBlockKind,
        reason: String,
    },
    Dispatched {
        work_id: String,
        dispatch_key: String,
    },
}

/// A typed observation of the Host's existing dispatch path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadyDispatch {
    Observed {
        dispatch_key: String,
    },
    Blocked {
        kind: ReadyBlockKind,
        reason: String,
    },
}

/// Trusted Host seam; implementations re-read live authority/resources and use
/// the canonical Start receipt and worker runtime, never a second Task or lease.
#[async_trait]
pub trait ReadyWorkHost: Send + Sync {
    async fn authority(&self) -> Result<(Arc<TicketService>, Authority)>;
    async fn check_budget(&self, max_rounds: u32) -> Result<()>;
    async fn preflight(&self, command: &Command) -> Result<()>;
    async fn ensure(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
    ) -> Result<ReadyDispatch>;
    async fn dispatch(&self, command: &Command, max_rounds: u32) -> Result<ReadyDispatch>;
}

struct InstalledPolicy {
    policy: ReadyWorkPolicy,
    binding: ScopeBinding,
    epoch: u64,
    expires: Instant,
    failures: BTreeMap<String, u8>,
    stopped: BTreeMap<String, ReadyReconcile>,
}

struct Worker {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

/// Process-local policy, bounded reconciliation and one event-driven worker.
#[derive(Default)]
pub struct ReadyWorkScheduler {
    state: tokio::sync::Mutex<Option<InstalledPolicy>>,
    wake: Arc<Notify>,
    worker: Mutex<Option<Worker>>,
}

impl ReadyWorkScheduler {
    pub async fn install(&self, host: &dyn ReadyWorkHost, policy: ReadyWorkPolicy) -> Result<()> {
        if !(1..=3).contains(&policy.capacity)
            || policy.grants.is_empty()
            || policy.grants.len() > CLAIM_CANDIDATE_LIMIT
            || policy.max_rounds == 0
            || policy.lifetime.is_zero()
            || policy.lifetime > Duration::from_secs(3600)
            || policy.grants.iter().any(|grant| {
                grant.work_id.is_empty()
                    || grant.work_id.len() > 256
                    || grant.contract_revision == 0
                    || grant.context_hash.len() != 64
                    || !grant
                        .context_hash
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
            })
            || policy
                .grants
                .iter()
                .map(|grant| &grant.work_id)
                .collect::<BTreeSet<_>>()
                .len()
                != policy.grants.len()
        {
            return Err(Error::InvalidTransition(
                "invalid bounded Ready Work policy".into(),
            ));
        }
        let mut state = self.state.lock().await;
        if state.is_some() {
            return Err(Error::InvalidTransition(
                "revoke the previous Host policy first".into(),
            ));
        }
        let (service, _) = host.authority().await?;
        let snapshot = service.published()?.1;
        host.check_budget(policy.max_rounds).await?;
        *state = Some(InstalledPolicy {
            expires: Instant::now() + policy.lifetime,
            policy,
            binding: snapshot.binding,
            epoch: snapshot.authority_epoch,
            failures: BTreeMap::new(),
            stopped: BTreeMap::new(),
        });
        drop(state);
        self.wake();
        Ok(())
    }

    /// Barrier against this scheduler's in-flight admission. Already admitted
    /// execution retains the Host's existing stop/ACK/resource protocol.
    pub async fn revoke(&self) {
        *self.state.lock().await = None;
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// At most one existing intent reconciliation or one new Start per pass.
    /// A scope conflict requires fresh eligibility, never a refreshed CAS alone.
    pub async fn reconcile(&self, host: &dyn ReadyWorkHost) -> Result<ReadyReconcile> {
        let mut state = self.state.lock().await;
        let Some(installed) = state.as_mut() else {
            return Ok(ReadyReconcile::Disabled);
        };
        if Instant::now() >= installed.expires {
            *state = None;
            return Ok(ReadyReconcile::Disabled);
        }
        let policy = installed.policy.clone();
        host.check_budget(policy.max_rounds).await?;
        let (service, authority) = host.authority().await?;
        let (commit, snapshot) = service.published()?;
        if snapshot.binding != installed.binding || snapshot.authority_epoch != installed.epoch {
            *state = None;
            return Ok(ReadyReconcile::Disabled);
        }
        let mut candidates = Vec::new();
        let mut blocked = None;
        for grant in &policy.grants {
            if let Some(report) = installed.stopped.get(&grant.work_id) {
                blocked.get_or_insert(report.clone());
                continue;
            }
            let Some(work) = snapshot.tickets.get(&grant.work_id) else {
                continue;
            };
            let pending = work
                .active_assignment
                .as_ref()
                .and_then(|id| snapshot.assignments.get(id))
                .filter(|assignment| {
                    assignment.state == AssignmentState::DispatchPending
                        && assignment.runtime.is_none()
                        && assignment.authority_epoch == installed.epoch
                        && assignment.generation == 1
                });
            if pending.is_none()
                && (work.generation != 0
                    || work.state != WorkState::Ready
                    || work.paused
                    || work.archived
                    || work.active_assignment.is_some())
            {
                continue;
            }
            let eligible = async {
                if snapshot.project.links.get(&work.id) != Some(&policy.project_id)
                    || snapshot.project.work_contexts.get(&work.id) != Some(&grant.context_hash)
                    || work.contract_revision != grant.contract_revision
                {
                    return Err(Error::ScopeDenied(
                        "Host Project/context decision is stale".into(),
                    ));
                }
                let command = Command {
                    operation_id: String::new(),
                    binding: snapshot.binding.clone(),
                    expected_seq: snapshot.seq,
                    expected_epoch: snapshot.authority_epoch,
                    source: None,
                    operations: vec![Operation::Start {
                        work_id: work.id.clone(),
                        temp_id: "assignment".into(),
                        workspace: Some(grant.workspace.clone()),
                    }],
                };
                host.preflight(&command).await?;
                // Filter poisoned/overlapping resources before oldest-first
                // selection. Start remains the atomic authoritative validator.
                if snapshot.assignments.values().any(|assignment| {
                    pending.is_none_or(|own| assignment.id != own.id)
                        && (!assignment.process_stopped
                            || assignment.effects.values().any(|effect| {
                                matches!(
                                    effect.state,
                                    EffectState::Started | EffectState::OutcomeUnknown
                                )
                            }))
                        && assignment.workspace.as_ref().is_some_and(|old| {
                            !grant.workspace.claims.is_disjoint(&old.claims)
                                || grant.workspace.write_roots.iter().any(|root| {
                                    old.write_roots.iter().any(|other| {
                                        Path::new(root).starts_with(other)
                                            || Path::new(other).starts_with(root)
                                    })
                                })
                        })
                }) {
                    return Err(Error::ResourceBlocked(
                        "workspace still has an execution/effect owner".into(),
                    ));
                }
                if let Some(assignment) = pending {
                    let intent = &snapshot.intents[&assignment.dispatch_key];
                    if intent.immutable_spec.project_context_hash.as_ref()
                        != Some(&grant.context_hash)
                        || intent.immutable_spec.workspace.as_ref() != Some(&grant.workspace)
                    {
                        return Err(Error::ScopeDenied(
                            "pending intent differs from Host decision".into(),
                        ));
                    }
                    return host
                        .ensure(
                            &assignment.dispatch_key,
                            &intent.immutable_spec,
                            policy.max_rounds,
                        )
                        .await
                        .map(Some);
                }
                Ok(None)
            }
            .await;
            match eligible {
                Ok(Some(ReadyDispatch::Blocked { kind, reason })) => {
                    return Ok(remember_failure(installed, &work.id, kind, reason));
                }
                Ok(Some(ReadyDispatch::Observed { dispatch_key })) => {
                    return Ok(ReadyReconcile::Dispatched {
                        work_id: work.id.clone(),
                        dispatch_key,
                    });
                }
                Ok(None) => candidates.push(ClaimCandidate {
                    work_id: work.id.clone(),
                    workspace: Some(grant.workspace.clone()),
                }),
                Err(error) => {
                    let report = remember_failure(
                        installed,
                        &work.id,
                        classify_block(&error),
                        error.to_string(),
                    );
                    if pending.is_some() {
                        return Ok(report);
                    }
                    blocked.get_or_insert(report);
                }
            }
        }
        let observed = SnapshotRef {
            commit,
            seq: snapshot.seq,
            authority_epoch: snapshot.authority_epoch,
        };
        let operation_id = format!("host-ready/{}/{}", snapshot.authority_epoch, snapshot.seq);
        match service.prepare_ready_claim(
            &authority,
            &observed,
            &operation_id,
            &candidates,
            policy.capacity,
        )? {
            ClaimDecision::CapacityBlocked { occupied, capacity } => {
                Ok(ReadyReconcile::CapacityBlocked { occupied, capacity })
            }
            ClaimDecision::NoEligibleWork => Ok(blocked.unwrap_or(ReadyReconcile::Idle)),
            ClaimDecision::Prepared(command) => {
                let work_id = match &command.operations[0] {
                    Operation::Start { work_id, .. } => work_id.clone(),
                    _ => unreachable!(),
                };
                if Instant::now() >= installed.expires {
                    return Ok(ReadyReconcile::Disabled);
                }
                host.check_budget(policy.max_rounds).await?;
                match host.dispatch(&command, policy.max_rounds).await {
                    Ok(ReadyDispatch::Observed { dispatch_key }) => {
                        Ok(ReadyReconcile::Dispatched {
                            work_id,
                            dispatch_key,
                        })
                    }
                    Ok(ReadyDispatch::Blocked { kind, reason }) => {
                        Ok(remember_failure(installed, &work_id, kind, reason))
                    }
                    // A scope conflict fences this observed selection; retry
                    // only after another full read and eligibility check.
                    Err(Error::RevisionConflict) => Err(Error::RevisionConflict),
                    Err(error) => Ok(remember_failure(
                        installed,
                        &work_id,
                        classify_block(&error),
                        error.to_string(),
                    )),
                }
            }
        }
    }

    pub fn spawn(self: &Arc<Self>, host: Weak<dyn ReadyWorkHost>, shutdown: CancellationToken) {
        let mut slot = self.worker.lock().expect("Ready worker handle");
        if slot.is_some() {
            return;
        }
        let shutdown = shutdown.child_token();
        let cancel = shutdown.clone();
        let weak = Arc::downgrade(self);
        let wake = self.wake.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = wake.notified() => {},
                    _ = interval.tick() => {},
                }
                let (Some(scheduler), Some(host)) = (weak.upgrade(), host.upgrade()) else {
                    break;
                };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = scheduler.reconcile(host.as_ref()) => if let Err(error) = result {
                        tracing::debug!(%error, "Ready Work retained for bounded reconciliation");
                    },
                }
                drop((scheduler, host));
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(MIN_WAKE_INTERVAL) => {},
                }
            }
        });
        *slot = Some(Worker { shutdown, handle });
    }

    /// Stop and join this worker only; never cancel the Host's parent token.
    pub async fn shutdown(&self) {
        let worker = self.worker.lock().expect("Ready worker handle").take();
        if let Some(worker) = worker {
            worker.shutdown.cancel();
            let _ = worker.handle.await;
        }
    }
}

fn remember_failure(
    installed: &mut InstalledPolicy,
    work_id: &str,
    kind: ReadyBlockKind,
    reason: String,
) -> ReadyReconcile {
    let failures = installed.failures.entry(work_id.into()).or_default();
    if kind == ReadyBlockKind::Transient {
        *failures = failures.saturating_add(1);
    }
    let report = ReadyReconcile::Blocked {
        work_id: work_id.into(),
        kind,
        reason,
    };
    if matches!(
        kind,
        ReadyBlockKind::Configuration | ReadyBlockKind::Permission | ReadyBlockKind::OutcomeUnknown
    ) || *failures >= TRANSIENT_ATTEMPT_LIMIT
    {
        installed.stopped.insert(work_id.into(), report.clone());
    }
    report
}

fn classify_block(error: &Error) -> ReadyBlockKind {
    match error {
        Error::ScopeDenied(_) => ReadyBlockKind::Permission,
        Error::InvalidTransition(_)
        | Error::ContextBudgetExceeded
        | Error::Json(_)
        | Error::RevisionConflict
        | Error::DependencyCycle => ReadyBlockKind::Configuration,
        Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ReadyBlockKind::Configuration
        }
        Error::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            ReadyBlockKind::Permission
        }
        Error::ResourceBlocked(_) => ReadyBlockKind::Resource,
        _ => ReadyBlockKind::Transient,
    }
}
