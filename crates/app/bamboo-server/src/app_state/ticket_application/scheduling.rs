//! Explicit, process-local Host opt-in for the first Ready Work runtime slice.
//! The existing Ticket ledger owns claims, intents, receipts and restart fences.
//! Nothing here can restore an execution grant from persisted Work or JSON.
use super::*;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const MIN_WAKE_INTERVAL: Duration = Duration::from_millis(100);
const TRANSIENT_ATTEMPT_LIMIT: u8 = 3;

/// One explicitly authorized initial attempt. Changing the contract, adopted
/// context or workspace requires another Host decision. Not a client DTO.
#[derive(Clone, Debug)]
pub struct ReadyWorkGrant {
    pub work_id: String,
    pub contract_revision: u64,
    pub context_hash: String,
    pub workspace: ExecutionWorkspace,
}

/// A finite single-Project Host decision, never persisted or deserialized.
/// Only generation-zero Work is selected: retries/continuations and cumulative
/// Work accounting remain outside this slice. Rounds are not a money budget.
#[derive(Clone, Debug)]
pub struct ReadyWorkPolicy {
    pub project_id: bamboo_domain::ProjectId,
    pub grants: Vec<ReadyWorkGrant>,
    pub capacity: usize,
    pub max_rounds: u32,
    pub lifetime: Duration,
}

/// Program-only observations also form the seam for a future Host inspection
/// policy. The reconciler never invokes a model to interpret these outcomes.
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

struct InstalledPolicy {
    policy: ReadyWorkPolicy,
    binding: ScopeBinding,
    epoch: u64,
    expires: Instant,
    failures: BTreeMap<String, u8>,
    stopped: BTreeMap<String, ReadyReconcile>,
}

#[derive(Default)]
pub(super) struct ReadySchedule {
    installed: Option<InstalledPolicy>,
}

impl TicketApplication {
    /// Called by a trusted embedding Host, never by a Worker/model HTTP body.
    /// Opening a scope or enabling ticket_dispatch does not install a policy.
    pub async fn install_ready_work_policy(&self, policy: ReadyWorkPolicy) -> Result<()> {
        if !(1..=3).contains(&policy.capacity)
            || policy.grants.is_empty()
            || policy.grants.len() > CLAIM_CANDIDATE_LIMIT
            || policy.max_rounds == 0
            || policy.lifetime.is_zero()
            || policy.lifetime > Duration::from_secs(3600)
            || policy.grants.iter().any(|g| {
                g.work_id.is_empty()
                    || g.work_id.len() > 256
                    || g.contract_revision == 0
                    || g.context_hash.len() != 64
                    || !g.context_hash.bytes().all(|b| b.is_ascii_hexdigit())
            })
            || policy
                .grants
                .iter()
                .map(|g| &g.work_id)
                .collect::<BTreeSet<_>>()
                .len()
                != policy.grants.len()
        {
            return Err(Error::InvalidTransition(
                "invalid bounded Ready Work policy".into(),
            ));
        }
        let mut schedule = self.ready_schedule.lock().await;
        if schedule.installed.is_some() {
            return Err(Error::InvalidTransition(
                "revoke the previous Host policy first".into(),
            ));
        }
        let (service, _) = self
            .authority(Principal::Supervisor {
                session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
            })
            .await?;
        let snapshot = service.published()?.1;
        self.check_ready_budget(&policy).await?;
        schedule.installed = Some(InstalledPolicy {
            expires: Instant::now() + policy.lifetime,
            policy,
            binding: snapshot.binding,
            epoch: snapshot.authority_epoch,
            failures: BTreeMap::new(),
            stopped: BTreeMap::new(),
        });
        drop(schedule);
        self.wake_ready_work();
        Ok(())
    }

    /// A barrier against this scheduler's in-flight admission. Already admitted
    /// executions retain the existing explicit stop/ACK/resource protocol.
    pub async fn revoke_ready_work_policy(&self) {
        self.ready_schedule.lock().await.installed = None;
    }

    pub fn wake_ready_work(&self) {
        self.ready_wake.notify_one();
    }

    async fn check_ready_budget(&self, policy: &ReadyWorkPolicy) -> Result<()> {
        if self
            .adapter
            .get()
            .is_some_and(|adapter| !adapter.supports_native_ticket_budget())
        {
            return Err(Error::ScopeDenied(
                "Ready Work requires the assembled native Tokio backend".into(),
            ));
        }
        let config = self.config.read().await;
        if !config.features.ticket_mutation || !config.features.ticket_dispatch {
            return Err(Error::AuthorityUnavailable(
                "Ticket scheduling features are disabled".into(),
            ));
        }
        if !config
            .run_budget
            .max_rounds
            .is_some_and(|n| n > 0 && n <= policy.max_rounds)
        {
            return Err(Error::ResourceBlocked(
                "a finite configured run round ceiling is required".into(),
            ));
        }
        Ok(())
    }

    /// At most one existing intent reconciliation or one new Start per pass.
    /// Re-read eligibility after every scope conflict; never just refresh a CAS.
    pub async fn reconcile_ready_work(&self) -> Result<ReadyReconcile> {
        // Serialize selection with policy revocation, not with Worker execution.
        let mut schedule = self.ready_schedule.lock().await;
        let Some(installed) = schedule.installed.as_mut() else {
            return Ok(ReadyReconcile::Disabled);
        };
        if Instant::now() >= installed.expires {
            schedule.installed = None;
            return Ok(ReadyReconcile::Disabled);
        }
        let policy = installed.policy.clone();
        self.check_ready_budget(&policy).await?;
        let principal = Principal::Supervisor {
            session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
        };
        let (service, authority) = self.authority(principal.clone()).await?;
        let (commit, snapshot) = service.published()?;
        if snapshot.binding != installed.binding || snapshot.authority_epoch != installed.epoch {
            schedule.installed = None;
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
                .filter(|a| {
                    a.state == AssignmentState::DispatchPending
                        && a.runtime.is_none()
                        && a.authority_epoch == installed.epoch
                        && a.generation == 1
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
                self.validate_project_operations(&service, &command)?;
                self.preflight_ticket_starts(vec![Some(grant.workspace.clone())])
                    .await?;
                // Conservatively omit conflicting resources before oldest-first
                // selection so one poisoned/blocked Work cannot monopolize it.
                // Start remains the atomic authoritative resource validator.
                if snapshot.assignments.values().any(|a| {
                    pending.is_none_or(|own| a.id != own.id)
                        && (!a.process_stopped
                            || a.effects.values().any(|e| {
                                matches!(
                                    e.state,
                                    EffectState::Started | EffectState::OutcomeUnknown
                                )
                            }))
                        && a.workspace.as_ref().is_some_and(|old| {
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
                    // ensure queries the same canonical dispatch identity first;
                    // Unknown/Started outcomes cannot become a fresh dispatch.
                    if let ticket_runtime::DispatchObservation::OutcomeUnknown { reason, .. } = self
                        .ensure_with_round_ceiling(
                            &assignment.dispatch_key,
                            &intent.immutable_spec,
                            Some(policy.max_rounds),
                        )
                        .await?
                    {
                        return Ok(Some(ReadyReconcile::Blocked {
                            work_id: work.id.clone(),
                            kind: ReadyBlockKind::OutcomeUnknown,
                            reason,
                        }));
                    }
                    return Ok(Some(ReadyReconcile::Dispatched {
                        work_id: work.id.clone(),
                        dispatch_key: assignment.dispatch_key.clone(),
                    }));
                }
                Ok(None)
            }
            .await;
            match eligible {
                Ok(Some(ReadyReconcile::Blocked {
                    work_id,
                    kind,
                    reason,
                })) => {
                    return Ok(remember_failure(installed, &work_id, kind, reason));
                }
                Ok(Some(result)) => return Ok(result),
                Ok(None) => candidates.push(ClaimCandidate {
                    work_id: work.id.clone(),
                    workspace: Some(grant.workspace.clone()),
                }),
                Err(error) => {
                    let kind = classify_block(&error);
                    let report = remember_failure(installed, &work.id, kind, error.to_string());
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
        // This ID belongs to exactly this scope observation. A lost ACK leaves
        // the immutable pending intent above, not permission to create a new ID.
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
                self.check_ready_budget(&policy).await?;
                let response = self
                    .dispatch_with_round_ceiling(principal, &command, Some(policy.max_rounds))
                    .await?;
                if response["errors"]
                    .as_array()
                    .is_some_and(|errors| !errors.is_empty())
                {
                    let kind = match response["errors"][0]["status_code"].as_u64() {
                        Some(403) => ReadyBlockKind::Permission,
                        Some(409 | 422) => ReadyBlockKind::Configuration,
                        Some(423) => ReadyBlockKind::Resource,
                        _ => ReadyBlockKind::Transient,
                    };
                    return Ok(remember_failure(
                        installed,
                        &work_id,
                        kind,
                        response["errors"].to_string(),
                    ));
                }
                if response["runtime"].as_array().is_some_and(|observations| {
                    observations
                        .iter()
                        .any(|o| o["observation"]["status"] == "outcome_unknown")
                }) {
                    return Ok(remember_failure(
                        installed,
                        &work_id,
                        ReadyBlockKind::OutcomeUnknown,
                        "runtime admission outcome is unknown; retained the same intent".into(),
                    ));
                }
                let (_, latest) = service.published()?;
                let id = response["receipt"]["ids"]["assignment"]
                    .as_str()
                    .ok_or_else(|| {
                        Error::AuthorityUnavailable("claimed Assignment missing".into())
                    })?;
                Ok(ReadyReconcile::Dispatched {
                    work_id,
                    dispatch_key: latest.assignments[id].dispatch_key.clone(),
                })
            }
        }
    }

    pub(super) fn spawn_ready_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let mut slot = self.ready_worker.lock().expect("Ready worker handle");
        if slot.is_some() {
            return;
        }
        let weak = Arc::downgrade(self);
        let wake = self.ready_wake.clone();
        *slot = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = wake.notified() => {},
                    _ = interval.tick() => {},
                }
                let Some(app) = weak.upgrade() else {
                    break;
                };
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    result = app.reconcile_ready_work() => if let Err(error) = result {
                        tracing::debug!(%error, "Ready Work retained for bounded reconciliation");
                    },
                }
                drop(app);
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(MIN_WAKE_INTERVAL) => {},
                }
            }
        }));
    }

    pub(super) async fn shutdown_ready_worker(&self) {
        let handle = self
            .ready_worker
            .lock()
            .expect("Ready worker handle")
            .take();
        if let Some(handle) = handle {
            let _ = handle.await;
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
