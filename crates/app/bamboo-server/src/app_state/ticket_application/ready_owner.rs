//! Explicit, process-local owner control. Only Ticket HEAD stores Work facts,
//! dispatch receipts and cumulative spending. These receipts confer no authority
//! after revocation, expiry or restart and are never written to disk.
use super::*;
use bamboo_orchestration::ReadyWorkHost;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

const RECEIPT_LIMIT: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyPolicyGrant {
    pub work_id: String,
    pub contract_revision: u64,
    pub context_hash: String,
    pub workspace: ExecutionWorkspace,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyPolicyInstall {
    pub binding: ScopeBinding,
    pub observed: SnapshotRef,
    pub project_id: bamboo_domain::ProjectId,
    pub grants: Vec<ReadyPolicyGrant>,
    pub capacity: usize,
    pub max_rounds: u32,
    pub lifetime_secs: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadyPolicyAction {
    Install { policy: ReadyPolicyInstall },
    Revoke,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyPolicyRequest {
    pub process_id: uuid::Uuid,
    pub operation_id: String,
    pub expected_revision: u64,
    pub action: ReadyPolicyAction,
}

struct Cached {
    request: ReadyPolicyRequest,
    receipt: Value,
    complete: bool,
}

pub(super) struct ReadyOwnerControl {
    process_id: uuid::Uuid,
    revision: u64,
    receipts: BTreeMap<String, Cached>,
    pending_revoke: Option<String>,
    policy: Option<ReadyPolicyInstall>,
}

impl Default for ReadyOwnerControl {
    fn default() -> Self {
        Self {
            process_id: uuid::Uuid::new_v4(),
            revision: 0,
            receipts: BTreeMap::new(),
            pending_revoke: None,
            policy: None,
        }
    }
}

fn owner(principal: &Principal) -> Result<()> {
    if matches!(principal, Principal::User { user_id } if user_id == "host-owner") {
        Ok(())
    } else {
        Err(Error::ScopeDenied("verified owner access required".into()))
    }
}

impl TicketApplication {
    /// No authority/bootstrap or model call. A live policy is permission to try
    /// admission, never evidence that a worker started or made progress.
    pub async fn ready_policy_status(&self, principal: Principal) -> Result<Value> {
        owner(&principal)?;
        let control = self.ready_owner.lock().await;
        let active = self.ready_scheduler.policy_is_active();
        Ok(json!({
            "process_id":control.process_id,"revision":control.revision,
            "active":active,"policy":control.policy,
            "pending_revoke_operation":control.pending_revoke,
            "receipt_capacity_remaining":RECEIPT_LIMIT - control.receipts.len(),
            "execution_progress":"unknown",
        }))
    }

    pub async fn control_ready_policy(
        &self,
        principal: Principal,
        request: ReadyPolicyRequest,
    ) -> Result<Value> {
        owner(&principal)?;
        let mut control = self.ready_owner.lock().await;
        if request.process_id != control.process_id {
            return Err(Error::RevisionConflict);
        }
        if request.operation_id.is_empty() || request.operation_id.len() > 128 {
            return Err(Error::InvalidTransition(
                "invalid policy operation id".into(),
            ));
        }
        if let Some(cached) = control.receipts.get(&request.operation_id) {
            if serde_json::to_value(&cached.request)? != serde_json::to_value(&request)? {
                return Err(Error::IdempotencyConflict);
            }
            // Cancellation of an HTTP waiter cannot lose the revocation fence.
            // A retry drains the same retired generation before returning its
            // receipt. No new installation may pass an unfinished drain.
            if !cached.complete {
                self.ready_scheduler.revoke().await;
                control.pending_revoke = None;
                control
                    .receipts
                    .get_mut(&request.operation_id)
                    .unwrap()
                    .complete = true;
            }
            return Ok(control.receipts[&request.operation_id].receipt.clone());
        }
        if control.pending_revoke.is_some() {
            return Err(Error::ResourceBlocked(
                "retry the pending revoke operation to finish its admission drain".into(),
            ));
        }
        if request.expected_revision != control.revision {
            return Err(Error::RevisionConflict);
        }
        // Never evict receipts and accidentally turn an old retry into a grant.
        // A restart rotates process_id and discards all executable permission.
        if control.receipts.len() >= RECEIPT_LIMIT
            || (matches!(request.action, ReadyPolicyAction::Install { .. })
                && control.receipts.len() >= RECEIPT_LIMIT - 1)
        {
            return Err(Error::ResourceBlocked(
                "process policy receipt limit reached".into(),
            ));
        }
        let revision = control
            .revision
            .checked_add(1)
            .ok_or(Error::RevisionConflict)?;
        let (action, expires_at) = match &request.action {
            ReadyPolicyAction::Install { policy } => {
                if control.policy.is_some()
                    || self.ready_scheduler.policy_is_active() != Some(false)
                {
                    return Err(Error::ResourceBlocked(
                        "revoke the previous Host policy first".into(),
                    ));
                }
                self.validate_owner_ready_policy(policy).await?;
                self.install_ready_work_policy(ReadyWorkPolicy {
                    project_id: policy.project_id.clone(),
                    grants: policy
                        .grants
                        .iter()
                        .map(|grant| ReadyWorkGrant {
                            work_id: grant.work_id.clone(),
                            contract_revision: grant.contract_revision,
                            context_hash: grant.context_hash.clone(),
                            workspace: grant.workspace.clone(),
                        })
                        .collect(),
                    capacity: policy.capacity,
                    max_rounds: policy.max_rounds,
                    lifetime: Duration::from_secs(u64::from(policy.lifetime_secs)),
                })
                .await?;
                let expires_at =
                    chrono::Utc::now() + chrono::Duration::seconds(i64::from(policy.lifetime_secs));
                control.policy = Some(policy.clone());
                ("installed", Some(expires_at))
            }
            ReadyPolicyAction::Revoke => {
                control.policy = None;
                ("revoked", None)
            }
        };
        let receipt = json!({
            "process_id":control.process_id,"operation_id":request.operation_id,
            "revision":revision,"action":action,"expires_at":expires_at,
        });
        let revoking = matches!(request.action, ReadyPolicyAction::Revoke);
        control.revision = revision;
        if revoking {
            control.pending_revoke = Some(request.operation_id.clone());
        }
        let operation_id = request.operation_id.clone();
        control.receipts.insert(
            operation_id.clone(),
            Cached {
                request,
                receipt: receipt.clone(),
                complete: !revoking,
            },
        );
        if revoking {
            // First poll synchronously invalidates existing/pending fences.
            // If cancelled during the drain, the cached pending operation is
            // deliberately retained and must be reconciled, never reinstalled.
            self.ready_scheduler.revoke().await;
            control.pending_revoke = None;
            control.receipts.get_mut(&operation_id).unwrap().complete = true;
        }
        Ok(receipt)
    }

    async fn validate_owner_ready_policy(&self, policy: &ReadyPolicyInstall) -> Result<()> {
        if !(1..=3).contains(&policy.capacity)
            || policy.grants.is_empty()
            || policy.grants.len() > CLAIM_CANDIDATE_LIMIT
            || policy.max_rounds == 0
            || !(1..=3600).contains(&policy.lifetime_secs)
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
        if !self
            .adapter
            .get()
            .is_some_and(|adapter| adapter.supports_native_ticket_budget())
        {
            return Err(Error::ScopeDenied(
                "assembled native Tokio backend required".into(),
            ));
        }
        self.check_budget(policy.max_rounds).await?;
        let (service, _) = self
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await?;
        let (commit, snapshot) = service.published()?;
        if service.health() != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "writable Ticket scope required".into(),
            ));
        }
        if policy.binding != snapshot.binding
            || policy.observed.commit != commit
            || policy.observed.seq != snapshot.seq
            || policy.observed.authority_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        // Owner setup uses the existing host-only ledger configuration port.
        // Installation never creates limits, reserves money or resets spending.
        let budget = snapshot.execution_budget.as_ref().ok_or_else(|| {
            Error::ResourceBlocked(
                "configure canonical cumulative Work token limits before installation".into(),
            )
        })?;
        let today = chrono::Utc::now().timestamp_millis().max(0) as u64 / 86_400_000;
        if budget.total_charged >= budget.limits.total_tokens
            || budget.current_utc_day.is_some_and(|day| day > today)
            || (budget.current_utc_day == Some(today)
                && budget.daily_charged >= budget.limits.utc_day_tokens)
            || budget.limits.utc_day_tokens == 0
        {
            return Err(Error::ResourceBlocked(
                "canonical token budget exhausted".into(),
            ));
        }
        let mut workspaces = Vec::new();
        for grant in &policy.grants {
            let work = snapshot
                .tickets
                .get(&grant.work_id)
                .ok_or_else(|| Error::InvalidTransition("granted Work missing".into()))?;
            let context = snapshot
                .project
                .contexts
                .get(&grant.context_hash)
                .ok_or_else(|| {
                    Error::InvalidTransition("granted Project context missing".into())
                })?;
            if work.kind != TicketKind::Work
                || work.state != WorkState::Ready
                || work.generation != 0
                || work.active_assignment.is_some()
                || work.paused
                || work.archived
                || work.contract_revision != grant.contract_revision
                || snapshot.project.links.get(&grant.work_id) != Some(&policy.project_id)
                || snapshot.project.work_contexts.get(&grant.work_id) != Some(&grant.context_hash)
                || context.project_id != policy.project_id
                || context.contract != work.contract
                || context.contract_revision != work.contract_revision
                || context.workspace_binding.path != grant.workspace.repo
            {
                return Err(Error::RevisionConflict);
            }
            if budget
                .limits
                .work_tokens
                .get(&grant.work_id)
                .copied()
                .unwrap_or(0)
                <= budget
                    .work_charged
                    .get(&grant.work_id)
                    .copied()
                    .unwrap_or(0)
            {
                return Err(Error::ResourceBlocked(
                    "canonical Work token budget exhausted".into(),
                ));
            }
            self.validate_project_context(&policy.project_id, Some(context), false)?;
            workspaces.push(Some(grant.workspace.clone()));
        }
        self.preflight_ticket_starts(workspaces).await?;
        // Disk/workspace admission may await; stale observations confer nothing.
        let (current, _) = self.authority(Principal::Runtime).await?;
        if current.published()?.1.seq != snapshot.seq {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }
}
