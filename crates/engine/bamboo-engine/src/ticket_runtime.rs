//! Host-only Ticket dispatch over canonical SessionInbox activation. Runtime
//! evidence lives in the existing Child control plane, never in Worker JSON.
use std::sync::Arc;

use bamboo_domain::{Session, SessionAuthorityIdentity, SessionKind, Storage};
use bamboo_tickets::{
    canonical_bytes, content_hash, AssignmentState, Authority, DispatchSpec, Error, Operation,
    Principal, Result, RuntimeReceipt, ScopeBinding, TicketService, WorkState,
};
use serde::{Deserialize, Serialize};

use crate::execution::spawn::SessionInboxRuntimeBinding;

pub const TICKET_DISPATCH_KEY: &str = "ticket.runtime.dispatch.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketDispatchBinding {
    pub dispatch_key: String,
    pub spec_hash: String,
    pub assignment_id: String,
    pub binding: ScopeBinding,
    pub receipt: Option<RuntimeReceipt>,
    pub receipt_created_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DispatchObservation {
    Missing,
    Pending {
        session_id: String,
    },
    Admitted {
        receipt: RuntimeReceipt,
    },
    Terminal {
        receipt: RuntimeReceipt,
        terminal_status: String,
    },
    OutcomeUnknown {
        receipt: Option<RuntimeReceipt>,
        reason: String,
    },
}

pub fn ticket_child_id(dispatch_key: &str) -> String {
    format!("ticket-{}", &content_hash(dispatch_key.as_bytes())[..32])
}

/// Storage's strict Root reader validates its Supervisor proof/runtime sidecar.
/// No model/client scope token is accepted as authority.
pub async fn verified_scope_binding(
    storage: &dyn Storage,
    supervisor_id: &str,
) -> Result<(ScopeBinding, Session)> {
    let supervisor = storage
        .load_root_authority(supervisor_id)
        .await?
        .ok_or_else(|| Error::ScopeDenied("Supervisor missing".into()))?;
    let SessionAuthorityIdentity::Supervisor { incarnation_id } = supervisor.authority_identity
    else {
        return Err(Error::ScopeDenied("canonical Supervisor required".into()));
    };
    if supervisor.kind != SessionKind::Root
        || !supervisor.root_orchestration_only_enabled()
        || supervisor.root_tool_authority_revision == 0
    {
        return Err(Error::ScopeDenied(
            "Supervisor orchestration authority required".into(),
        ));
    }
    let binding = ScopeBinding {
        scope_id: format!("supervisor/{incarnation_id}"),
        supervisor_session_id: supervisor.id.clone(),
        binding_revision: supervisor.root_tool_authority_revision,
    };
    Ok((binding, supervisor))
}

pub fn dispatch_binding(
    service: &TicketService,
    key: &str,
    spec: &DispatchSpec,
) -> Result<TicketDispatchBinding> {
    let (_, snapshot) = service.published()?;
    let intent = snapshot
        .intents
        .get(key)
        .ok_or_else(|| Error::InvalidTransition("dispatch intent missing".into()))?;
    let spec_hash = content_hash(&canonical_bytes(spec)?);
    if snapshot.binding != spec.binding || intent.spec_hash != spec_hash {
        return Err(Error::IdempotencyConflict);
    }
    Ok(TicketDispatchBinding {
        dispatch_key: key.into(),
        spec_hash,
        assignment_id: spec.assignment_id.clone(),
        binding: snapshot.binding,
        receipt: None,
        receipt_created_at: None,
    })
}

pub fn require_dispatch_permission(
    service: &TicketService,
    dispatch: &TicketDispatchBinding,
) -> Result<()> {
    if service.health() != bamboo_tickets::Health::Writable {
        return Err(Error::AuthorityUnavailable(
            "dispatch requires writable authority".into(),
        ));
    }
    let (_, snapshot) = service.published()?;
    let assignment = snapshot
        .assignments
        .get(&dispatch.assignment_id)
        .ok_or_else(|| Error::ScopeDenied("Assignment missing".into()))?;
    let work = &snapshot.tickets[&assignment.work_id];
    if snapshot.binding != dispatch.binding
        || snapshot
            .intents
            .get(&dispatch.dispatch_key)
            .is_none_or(|i| i.spec_hash != dispatch.spec_hash)
        || assignment.dispatch_key != dispatch.dispatch_key
        || assignment.authority_epoch != snapshot.authority_epoch
        || work.generation != assignment.generation
        || work.contract_revision != assignment.contract_revision
        || work.active_assignment.as_deref() != Some(&assignment.id)
        || work.state != WorkState::Active
        || !matches!(
            assignment.state,
            AssignmentState::DispatchPending | AssignmentState::Admitted | AssignmentState::Running
        )
    {
        return Err(Error::ScopeDenied(
            "dispatch permission is stale or inactive".into(),
        ));
    }
    Ok(())
}

pub fn read_dispatch(session: &Session) -> Result<Option<TicketDispatchBinding>> {
    session
        .metadata
        .get(TICKET_DISPATCH_KEY)
        .map(|raw| {
            if raw.len() > 4096 {
                return Err(Error::ScopeDenied("dispatch binding exceeds limit".into()));
            }
            Ok(serde_json::from_str(raw)?)
        })
        .transpose()
}

pub async fn query_dispatch(
    storage: &dyn Storage,
    service: &TicketService,
    key: &str,
    spec: &DispatchSpec,
) -> Result<DispatchObservation> {
    let expected = dispatch_binding(service, key, spec)?;
    let (binding, _) =
        verified_scope_binding(storage, &expected.binding.supervisor_session_id).await?;
    if binding != expected.binding {
        return Err(Error::ScopeDenied("Supervisor binding changed".into()));
    }
    let child = storage
        .load_runtime_control_plane(&ticket_child_id(key))
        .await?;
    let Some(child) = child else {
        return Ok(
            if require_dispatch_permission(service, &expected).is_ok()
                && service.published()?.1.assignments[&spec.assignment_id]
                    .runtime
                    .is_none()
            {
                DispatchObservation::Missing
            } else {
                DispatchObservation::OutcomeUnknown {
                    receipt: None,
                    reason: "canonical dispatch evidence unavailable".into(),
                }
            },
        );
    };
    let actual = read_dispatch(&child)?
        .ok_or_else(|| Error::ScopeDenied("Child lacks dispatch binding".into()))?;
    let mut comparable = actual.clone();
    comparable.receipt = None;
    comparable.receipt_created_at = None;
    if comparable != expected
        || child.kind != SessionKind::Child
        || child.parent_session_id.as_deref() != Some(&binding.supervisor_session_id)
        || child
            .metadata
            .get(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY)
            != Some(&expected.assignment_id)
    {
        return Err(Error::ScopeDenied(
            "canonical Child dispatch changed".into(),
        ));
    }
    if let Some(receipt) = actual.receipt {
        if receipt.dispatch_key != key
            || receipt.spec_hash != actual.spec_hash
            || receipt.session_id != child.id
            || receipt.run_id.is_empty()
            || actual.receipt_created_at != Some(child.created_at)
        {
            return Err(Error::ScopeDenied(
                "canonical receipt identity changed".into(),
            ));
        }
        let snapshot = service.published()?.1;
        if snapshot.assignments[&spec.assignment_id].runtime.as_ref() != Some(&receipt) {
            return Ok(DispatchObservation::OutcomeUnknown {
                receipt: Some(receipt),
                reason: "Runtime receipt prepared but Ticket admission not confirmed".into(),
            });
        }
        let status = child.last_run_status();
        if let Some(status @ ("completed" | "error" | "timeout" | "cancelled" | "skipped")) =
            status.as_deref()
        {
            return Ok(DispatchObservation::Terminal {
                receipt,
                terminal_status: status.into(),
            });
        }
        if require_dispatch_permission(service, &expected).is_err() {
            return Ok(DispatchObservation::OutcomeUnknown {
                receipt: Some(receipt),
                reason: "old writer or revoked execution; reconciliation required".into(),
            });
        }
        Ok(DispatchObservation::Admitted { receipt })
    } else if require_dispatch_permission(service, &expected).is_ok()
        && child.last_run_status().as_deref() == Some("pending")
    {
        Ok(DispatchObservation::Pending {
            session_id: child.id,
        })
    } else {
        Ok(DispatchObservation::OutcomeUnknown {
            receipt: None,
            reason: "dispatch has no confirmed run receipt".into(),
        })
    }
}

fn actual_with_receipt(
    expected: &TicketDispatchBinding,
    receipt: &RuntimeReceipt,
    created_at: chrono::DateTime<chrono::Utc>,
) -> TicketDispatchBinding {
    TicketDispatchBinding {
        receipt: Some(receipt.clone()),
        receipt_created_at: Some(created_at),
        ..expected.clone()
    }
}

/// Called after canonical activation registration, before sending any RunSpec.
/// The Host receipt is committed first; an uncertain Ticket publication blocks
/// execution. No lease or Worker event can supply the Run identity.
pub async fn admit_ticket_run(
    service: &Arc<TicketService>,
    runtime: &SessionInboxRuntimeBinding,
    session: &mut Session,
    run_id: &str,
) -> Result<()> {
    let dispatch = read_dispatch(session)?
        .ok_or_else(|| Error::ScopeDenied("dispatch binding missing".into()))?;
    require_dispatch_permission(service, &dispatch)?;
    let (binding, _) = verified_scope_binding(
        runtime.storage.as_ref(),
        &dispatch.binding.supervisor_session_id,
    )
    .await?;
    if binding != dispatch.binding || !runtime.router.owns_run(&session.id, run_id).await {
        return Err(Error::ScopeDenied(
            "canonical activation ownership changed".into(),
        ));
    }
    let store = runtime.parent_question_lock.as_ref().ok_or_else(|| {
        Error::AuthorityUnavailable("canonical write coordinator unavailable".into())
    })?;
    let receipt = RuntimeReceipt {
        dispatch_key: dispatch.dispatch_key.clone(),
        spec_hash: dispatch.spec_hash.clone(),
        session_id: session.id.clone(),
        run_id: run_id.into(),
    };
    let saved = store
        .mutate_runtime_session_and_publish(
            &session.id,
            || None,
            |latest| {
                let current = read_dispatch(latest)?
                    .ok_or_else(|| Error::ScopeDenied("canonical dispatch missing".into()))?;
                let mut comparable = current.clone();
                comparable.receipt = None;
                comparable.receipt_created_at = None;
                let mut expected = dispatch.clone();
                expected.receipt = None;
                expected.receipt_created_at = None;
                if latest.created_at != session.created_at
                    || latest.parent_session_id != session.parent_session_id
                    || latest.kind != SessionKind::Child
                    || comparable != expected
                    || current.receipt.as_ref().is_some_and(|r| r != &receipt)
                    || current.receipt.is_some()
                        && current.receipt_created_at != Some(latest.created_at)
                {
                    return Err(Error::ScopeDenied(
                        "dispatch birth or receipt changed".into(),
                    ));
                }
                if current.receipt.is_none() {
                    latest.metadata.insert(
                        TICKET_DISPATCH_KEY.into(),
                        serde_json::to_string(&actual_with_receipt(
                            &dispatch,
                            &receipt,
                            latest.created_at,
                        ))?,
                    );
                    latest.metadata_version =
                        latest.metadata_version.checked_add(1).ok_or_else(|| {
                            Error::AuthorityUnavailable("metadata revision overflow".into())
                        })?;
                }
                Ok::<_, Error>(())
            },
            |_| {},
        )
        .await??
        .ok_or_else(|| Error::ScopeDenied("canonical Child missing".into()))?;
    // Preserve the same Host-owned receipt in the eventual final checkpoint.
    session.metadata.insert(
        TICKET_DISPATCH_KEY.into(),
        saved.metadata[TICKET_DISPATCH_KEY].clone(),
    );
    session.metadata_version = saved.metadata_version;
    let authority = Authority::from_verified_host(dispatch.binding, Principal::Runtime);
    execute_runtime_command(
        service,
        &authority,
        &format!("runtime-admit/{}", dispatch.dispatch_key),
        vec![
            Operation::Admitted {
                assignment_id: dispatch.assignment_id.clone(),
                receipt,
            },
            Operation::Running {
                assignment_id: dispatch.assignment_id.clone(),
            },
        ],
    )?;
    // A restarted writer may only replay a historical receipt. It must not
    // inherit the old Worker's run permission from that receipt.
    crate::ticket_worker_plan::TicketWorkerPlan::from_runtime_receipt(
        service.clone(),
        &dispatch.assignment_id,
    )?;
    Ok(())
}

pub(crate) fn execute_runtime_command(
    service: &TicketService,
    authority: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> Result<bamboo_tickets::OperationReceipt> {
    for attempt in 0..4 {
        let command = service.prepare_command(authority, id, operations.clone())?;
        match service.execute(authority, &command) {
            Err(Error::RevisionConflict) if attempt < 3 => continue,
            result => return result,
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests;
