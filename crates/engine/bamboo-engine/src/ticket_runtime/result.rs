//! Convert only a canonical, completed Child checkpoint to a Submission. This
//! is Host postprocessing, not a Worker-declared Artifact or quality verdict.
use super::*;
use bamboo_domain::{MessagePhase, Role};
use bamboo_tickets::OperationReceipt;

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
