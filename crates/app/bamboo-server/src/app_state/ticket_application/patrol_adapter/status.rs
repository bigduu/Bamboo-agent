//! Owner-scoped saved bookkeeping, never a current runtime-health observation.
use super::super::{Error, Principal, Result, TicketApplication};
use super::{
    checkpoint_store::{self, CheckpointRead},
    patrol_error,
};
use bamboo_orchestration::patrol::{host, Phase, Scope, StoredCheckpoint};
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
        if !matches!(&principal,Principal::User{user_id} if user_id=="host-owner") {
            return Err(Error::ScopeDenied("owner patrol read required".into()));
        }
        let (service, authority) = self.authority(Principal::Runtime).await?;
        let (commit, snapshot) = service.published()?;
        let scope = host::scope_for_assignment(&service, &authority, project, work, assignment)
            .map_err(patrol_error)?;
        if host::current_assignment_id(&snapshot, work)
            .map_err(patrol_error)?
            .as_deref()
            != Some(assignment)
        {
            return Err(Error::ScopeDenied("current patrol attempt required".into()));
        }
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
}

#[cfg(test)]
mod tests;
