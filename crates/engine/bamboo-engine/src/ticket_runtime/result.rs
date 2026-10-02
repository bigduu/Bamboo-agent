//! Convert only a canonical, completed Child checkpoint to a Submission. This
//! is Host postprocessing, not a Worker-declared Artifact or quality verdict.
use super::*;
use bamboo_domain::{MessagePhase, Role};
use bamboo_tickets::OperationReceipt;

pub const TICKET_OWNED_STOP_KEY: &str = "ticket.runtime.owned_stop.v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedStopCheckpoint {
    receipt: RuntimeReceipt,
    child_birth: chrono::DateTime<chrono::Utc>,
    pid: u32,
    completed: bool,
}

/// Only a reaped local process from the exact current Host driver enters this
/// port. Persist its independent stop fact before Ticket publication, so a
/// failed Ticket write cannot turn a lease or display status into stop proof.
pub async fn checkpoint_owned_ticket_stop(
    service: &TicketService,
    runtime: &SessionInboxRuntimeBinding,
    session: &mut Session,
    run_id: &str,
    proof: &bamboo_subagent::fleet::ConfirmedLocalStop,
    completed: bool,
) -> Result<()> {
    let dispatch = read_dispatch(session)?
        .ok_or_else(|| Error::ScopeDenied("stop dispatch missing".into()))?;
    let receipt = dispatch
        .receipt
        .ok_or_else(|| Error::ScopeDenied("stop Runtime receipt missing".into()))?;
    if receipt.session_id != session.id
        || receipt.run_id != run_id
        || dispatch.receipt_created_at != Some(session.created_at)
    {
        return Err(Error::ScopeDenied("stop execution identity changed".into()));
    }
    let checkpoint = OwnedStopCheckpoint {
        receipt: receipt.clone(),
        child_birth: session.created_at,
        pid: proof.pid(),
        completed,
    };
    let raw = serde_json::to_string(&checkpoint)?;
    let expected_birth = session.created_at;
    let expected_receipt = receipt.clone();
    let store = runtime.parent_question_lock.as_ref().ok_or_else(|| {
        Error::AuthorityUnavailable("canonical stop write coordinator unavailable".into())
    })?;
    let saved = store
        .mutate_runtime_session_and_publish(
            &session.id,
            || None,
            |current| {
                let exact = read_dispatch(current).ok().flatten().is_some_and(|d| {
                    d.receipt.as_ref() == Some(&expected_receipt)
                        && d.receipt_created_at == Some(expected_birth)
                });
                if current.created_at != expected_birth || !exact {
                    return Err(Error::ScopeDenied(
                        "stop Child birth or receipt changed".into(),
                    ));
                }
                if current
                    .metadata
                    .get(TICKET_OWNED_STOP_KEY)
                    .is_some_and(|previous| previous != &raw)
                {
                    return Err(Error::ScopeDenied("stop checkpoint already differs".into()));
                }
                if !current.metadata.contains_key(TICKET_OWNED_STOP_KEY) {
                    current
                        .metadata
                        .insert(TICKET_OWNED_STOP_KEY.into(), raw.clone());
                    current.metadata_version =
                        current.metadata_version.checked_add(1).ok_or_else(|| {
                            Error::AuthorityUnavailable("stop metadata revision overflow".into())
                        })?;
                }
                Ok::<_, Error>(())
            },
            |_| {},
        )
        .await??
        .ok_or_else(|| Error::AuthorityUnavailable("stop Child disappeared".into()))?;
    if saved.metadata.get(TICKET_OWNED_STOP_KEY) != Some(&raw) {
        return Err(Error::ScopeDenied("stop Child checkpoint rejected".into()));
    }
    session.metadata.insert(TICKET_OWNED_STOP_KEY.into(), raw);
    session.metadata_version = saved.metadata_version;
    checkpoint_stop_operation(
        service,
        &dispatch.binding,
        &dispatch.assignment_id,
        &checkpoint,
    )?;
    Ok(())
}

fn checkpoint_stop_operation(
    service: &TicketService,
    binding: &ScopeBinding,
    assignment_id: &str,
    checkpoint: &OwnedStopCheckpoint,
) -> Result<()> {
    let authority = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    execute_runtime_command(
        service,
        &authority,
        &format!(
            "runtime-stop/{}/{}",
            checkpoint.receipt.dispatch_key, checkpoint.receipt.run_id
        ),
        vec![Operation::RuntimeStopped {
            assignment_id: assignment_id.into(),
            receipt: checkpoint.receipt.clone(),
            completed: checkpoint.completed,
        }],
    )?;
    Ok(())
}

pub async fn checkpoint_ticket_result(
    service: &TicketService,
    storage: &dyn Storage,
    expected: &Session,
    run_id: &str,
) -> Result<Option<OperationReceipt>> {
    if !expected
        .metadata
        .contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY)
    {
        return Ok(None);
    }
    let canonical = storage
        .load_session(&expected.id)
        .await?
        .ok_or_else(|| Error::AuthorityUnavailable("canonical Child checkpoint missing".into()))?;
    let dispatch = read_dispatch(&canonical)?
        .ok_or_else(|| Error::ScopeDenied("canonical dispatch receipt missing".into()))?;
    let receipt = dispatch
        .receipt
        .as_ref()
        .ok_or_else(|| Error::ScopeDenied("Runtime receipt missing".into()))?;
    if canonical.kind != SessionKind::Child
        || canonical.created_at != expected.created_at
        || canonical.parent_session_id != expected.parent_session_id
        || canonical.root_session_id != expected.root_session_id
        || dispatch.receipt_created_at != Some(canonical.created_at)
        || receipt.run_id != run_id
        || receipt.session_id != canonical.id
        || receipt.dispatch_key != dispatch.dispatch_key
        || receipt.spec_hash != dispatch.spec_hash
    {
        return Err(Error::ScopeDenied(
            "completed Child execution identity changed".into(),
        ));
    }
    if canonical.last_run_status().as_deref() != Some("completed") {
        return Ok(None);
    }
    let (binding, _) =
        verified_scope_binding(storage, &dispatch.binding.supervisor_session_id).await?;
    let snapshot = service.published()?.1;
    let assignment = snapshot
        .assignments
        .get(&dispatch.assignment_id)
        .ok_or_else(|| Error::ScopeDenied("Assignment missing".into()))?;
    if snapshot.binding != binding
        || dispatch.binding != binding
        || snapshot
            .intents
            .get(&dispatch.dispatch_key)
            .is_none_or(|i| i.spec_hash != dispatch.spec_hash)
        || assignment.runtime.as_ref() != Some(receipt)
    {
        return Err(Error::ScopeDenied(
            "completed Child differs from immutable Assignment".into(),
        ));
    }
    let output = canonical
        .messages
        .last()
        .filter(|m| {
            m.role == Role::Assistant
                && m.phase != Some(MessagePhase::Commentary)
                && m.tool_calls.as_ref().is_none_or(Vec::is_empty)
                && !m.content.trim().is_empty()
        })
        .ok_or_else(|| {
            Error::InvalidTransition("completed Child has no final output checkpoint".into())
        })?;
    let runtime = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    // A prior Host saved this fact only after reaping its owned process.
    // Recover Ticket publication before any broker ACK, without launching a
    // run or granting Worker tools. Missing proof keeps old attempts fenced.
    if let Some(raw) = canonical.metadata.get(TICKET_OWNED_STOP_KEY) {
        if raw.len() > 4096 {
            return Err(Error::ScopeDenied("stop checkpoint exceeds limit".into()));
        }
        let checkpoint: OwnedStopCheckpoint = serde_json::from_str(raw)?;
        if checkpoint.receipt != *receipt
            || checkpoint.child_birth != canonical.created_at
            || checkpoint.pid == 0
            || !checkpoint.completed
        {
            return Err(Error::ScopeDenied(
                "canonical stop checkpoint mismatch".into(),
            ));
        }
        checkpoint_stop_operation(service, &snapshot.binding, &assignment.id, &checkpoint)?;
        let current = service.published()?.1;
        let a = &current.assignments[&assignment.id];
        let work = &current.tickets[&a.work_id];
        let submit_id = format!("runtime-submit/{}/{run_id}", dispatch.dispatch_key);
        if !current.receipts.contains_key(&submit_id)
            && a.process_stopped
            && a.generation == work.generation
            && a.contract_revision == work.contract_revision
            && work.active_assignment.as_deref() == Some(&a.id)
            && work.state != WorkState::Cancelled
            && (a.authority_epoch != current.authority_epoch
                || a.state == AssignmentState::OutcomeUnknown)
        {
            // Explicit canonical completion reconciliation. It never resumes
            // execution; process_stopped continues to deny tools/dispatch.
            let reconciled = execute_runtime_command(
                service,
                &runtime,
                &format!(
                    "runtime-completed-reconcile/{}/{}",
                    dispatch.dispatch_key, current.authority_epoch
                ),
                vec![Operation::ReconcileCompleted {
                    assignment_id: a.id.clone(),
                    receipt: receipt.clone(),
                }],
            );
            if let Err(error) = reconciled {
                if !matches!(
                    error,
                    Error::ResourceBlocked(_) | Error::InvalidTransition(_)
                ) {
                    return Err(error);
                }
                // Retain a stale Submission for changed inputs/unknown effects.
            }
        }
    }
    let artifact = service.store_artifact(&runtime, output.content.as_bytes())?;
    let worker = Authority::from_verified_host(
        binding,
        Principal::Worker {
            assignment_id: assignment.id.clone(),
            generation: assignment.generation,
            run_id: receipt.run_id.clone(),
            session_id: receipt.session_id.clone(),
        },
    );
    let id = format!("runtime-submit/{}/{run_id}", dispatch.dispatch_key);
    let receipt = execute_runtime_command(
        service,
        &worker,
        &id,
        vec![Operation::Submit {
            assignment_id: assignment.id.clone(),
            temp_id: "submission".into(),
            evidence: vec![format!(
                "canonical Child {}; run {}; message {}; sha256 {}",
                canonical.id, run_id, output.id, artifact.sha256
            )],
            artifacts: vec![artifact],
        }],
    )?;
    // Submit's generation/contract/input fence preserves a late result as
    // stale and cannot replace a newer generation or reverse cancellation.
    Ok(Some(receipt))
}
