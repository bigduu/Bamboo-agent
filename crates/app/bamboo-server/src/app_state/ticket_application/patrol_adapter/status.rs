//! Owner-scoped reads of saved bookkeeping and separate live runtime facts.
use super::super::{Error, Principal, Result, TicketApplication};
use super::{
    checkpoint_store::{self, CheckpointRead},
    patrol_error, runtime_observation,
};
use bamboo_orchestration::patrol::{
    host, ObservationPort, Phase, Policy, Scope, StoredCheckpoint, WaitKind,
};
use bamboo_tickets::{Authority, Snapshot, TicketService};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Serialize)]
pub(crate) struct PatrolStatus {
    state: &'static str,
    scope: Scope,
    ticket_commit: String,
    ticket_cursor: u64,
    execution_facts_revision: u64,
    checkpoint: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct PatrolRuntimeObservation {
    state: &'static str,
    scope: Scope,
    ticket_commit: String,
    started_at_ms: Option<u64>,
    heartbeat_at_ms: Option<u64>,
    progress: Option<ObservedProgress>,
    expected_wait: Option<ObservedWait>,
    execution_outcome_unknown: bool,
}

#[derive(Serialize)]
struct ObservedProgress {
    sequence: u64,
    at_ms: u64,
}

#[derive(Serialize)]
struct ObservedWait {
    kind: &'static str,
    issued_at_ms: u64,
    diagnostic_allowance_until_ms: u64,
}

fn require_owner(principal: &Principal) -> Result<()> {
    if !matches!(principal, Principal::User { user_id } if user_id == "host-owner") {
        return Err(Error::ScopeDenied("owner patrol read required".into()));
    }
    Ok(())
}

fn current_scope(
    service: &TicketService,
    authority: &Authority,
    snapshot: &Snapshot,
    project: &str,
    work: &str,
    assignment: &str,
) -> Result<Scope> {
    let scope = host::scope_for_assignment(service, authority, project, work, assignment)
        .map_err(patrol_error)?;
    if host::current_assignment_id(snapshot, work)
        .map_err(patrol_error)?
        .as_deref()
        != Some(assignment)
    {
        return Err(Error::ScopeDenied("current patrol attempt required".into()));
    }
    Ok(scope)
}

fn summary(stored: StoredCheckpoint, seq: u64, facts: u64) -> Result<(&'static str, Value)> {
    let cp = stored.checkpoint;
    if cp.cursor > seq || cp.source_revision > facts {
        return Err(Error::RevisionConflict);
    }
    let incident=cp.incident.as_ref().map(|incident| {
        let phase=match incident.phase {Phase::Open=>"open",Phase::Diagnosing{..}=>"diagnosing",Phase::Cooldown{..}=>"cooldown",Phase::Blocked(_)=>"blocked",Phase::Resolved{..}=>"resolved"};
        json!({"id":incident.id,"reason":incident.reason,"phase":phase,"attempts":incident.attempts,
            "blocked_reason":match &incident.phase {Phase::Blocked(block)=>Some(block),_=>None},
            "last_diagnosis_at_ms":incident.last_diagnosis.as_ref().map(|record|record.at_ms),
            "accounting_pending":incident.pending_settlement.is_some(),
            "escalation":incident.escalation.as_ref().map(|escalation|json!({"operation":escalation.operation,"reason":escalation.reason,
                "deliveries":escalation.deliveries,"queued":escalation.queued,"next_delivery_ms":escalation.next_delivery_ms}))})
    });
    let stale = cp.source_revision < facts;
    Ok((
        if stale { "stale" } else { "available" },
        json!({"version":stored.version,"cursor":cp.cursor,"source_revision":cp.source_revision,
        "last_check_ms":cp.last_check_ms,"next_check_ms":cp.next_check_ms,"execution_facts_match":!stale,"incident":incident}),
    ))
}

impl TicketApplication {
    pub(crate) async fn patrol_status(
        &self,
        principal: Principal,
        project: &str,
        work: &str,
        assignment: &str,
    ) -> Result<PatrolStatus> {
        require_owner(&principal)?;
        let (service, authority) = self.authority(Principal::Runtime).await?;
        let (commit, snapshot) = service.published()?;
        let scope = current_scope(&service, &authority, &snapshot, project, work, assignment)?;
        if service.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        let binding =
            bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&snapshot.binding)?);
        let root = self
            .root
            .canonicalize()?
            .join("orchestration/patrol")
            .join(binding);
        let selector = scope.clone();
        let read =
            tokio::task::spawn_blocking(move || checkpoint_store::read_existing(&root, &selector))
                .await
                .map_err(|_| Error::AuthorityUnavailable("Patrol status reader stopped".into()))?;
        // Missing/busy/corrupt results require the same fresh proof as records.
        let (fresh, authority) = self.authority(Principal::Runtime).await?;
        if !Arc::ptr_eq(&fresh, &service) || fresh.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        let current = host::scope_for_assignment(&fresh, &authority, project, work, assignment)
            .map_err(patrol_error)?;
        if current != scope
            || host::current_assignment_id(&fresh.published()?.1, work)
                .map_err(patrol_error)?
                .as_deref()
                != Some(assignment)
        {
            return Err(Error::RevisionConflict);
        }
        if fresh.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        let (state, checkpoint) = match read {
            CheckpointRead::Available(stored) => {
                let (state, value) =
                    summary(*stored, snapshot.seq, snapshot.execution_facts_revision())?;
                (state, Some(value))
            }
            CheckpointRead::NotChecked => ("not_checked", None),
            CheckpointRead::Busy => ("busy", None),
            CheckpointRead::Corrupt => ("corrupt", None),
            CheckpointRead::Unavailable => ("unavailable", None),
            CheckpointRead::Unsupported => ("unsupported", None),
        };
        let response = PatrolStatus {
            state,
            scope,
            ticket_commit: commit,
            ticket_cursor: snapshot.seq,
            execution_facts_revision: snapshot.execution_facts_revision(),
            checkpoint,
        };
        if serde_json::to_vec(&response)?.len() > 8192 {
            return Err(Error::ContextBudgetExceeded);
        }
        if fresh.published()?.0 != response.ticket_commit {
            return Err(Error::RevisionConflict);
        }
        Ok(response)
    }

    pub(crate) async fn patrol_runtime_observation(
        &self,
        principal: Principal,
        project: &str,
        work: &str,
        assignment: &str,
    ) -> Result<PatrolRuntimeObservation> {
        require_owner(&principal)?;
        let (service, authority) = self.authority(Principal::Runtime).await?;
        let (commit, snapshot) = service.published()?;
        let scope = current_scope(&service, &authority, &snapshot, project, work, assignment)?;
        if service.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        let prepared = runtime_observation::prepare(self, &service, &authority, &scope).await?;
        // A prepared sample already includes the final Host proof. Recheck
        // the fallback separately because preparation can return None early.
        let (service, authority) = if prepared.is_none() {
            let (fresh, authority) = self.authority(Principal::Runtime).await?;
            if !Arc::ptr_eq(&fresh, &service) {
                return Err(Error::RevisionConflict);
            }
            (fresh, authority)
        } else {
            (service, authority)
        };
        if service.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }

        let now_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
        let mut port = match prepared {
            Some(runtime) => runtime.read_observation(
                &service,
                &authority,
                &scope,
                &Policy::default(),
                Some(now_ms),
            ),
            None => host::read_observation(&service, &authority, &scope),
        }
        .map_err(patrol_error)?;
        let observation = port.observation(&scope, 0).map_err(patrol_error)?;
        let started_at_ms = observation.started_at_ms.filter(|at| *at <= now_ms);
        let expected_wait = observation
            .expected_wait
            .filter(|wait| wait.issued_at_ms <= now_ms);
        let state = if started_at_ms.is_some()
            || observation.heartbeat_at_ms.is_some()
            || observation.progress.is_some()
            || expected_wait.is_some()
        {
            "observed"
        } else {
            "unknown"
        };
        let response = PatrolRuntimeObservation {
            state,
            scope,
            ticket_commit: commit,
            started_at_ms,
            heartbeat_at_ms: observation.heartbeat_at_ms,
            progress: observation.progress.map(|p| ObservedProgress {
                sequence: p.sequence,
                at_ms: p.at_ms,
            }),
            expected_wait: expected_wait.map(|wait| ObservedWait {
                kind: match wait.kind {
                    WaitKind::User => "user",
                    WaitKind::Dependency => "dependency",
                    WaitKind::RunningTool => "running_tool",
                },
                issued_at_ms: wait.issued_at_ms,
                diagnostic_allowance_until_ms: wait.until_ms,
            }),
            execution_outcome_unknown: observation.execution_unknown,
        };
        if serde_json::to_vec(&response)?.len() > 8192 {
            return Err(Error::ContextBudgetExceeded);
        }
        if service.published()?.0 != response.ticket_commit {
            return Err(Error::RevisionConflict);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests;
