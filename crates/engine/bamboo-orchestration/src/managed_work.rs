//! Explicit Work dispatch policy over the existing Ticket and Root facts.
use bamboo_domain::{
    ManagedRootAdmission, Message, Session, SessionAuthorityIdentity, SessionInboxReceipt,
    SessionKind, SupervisedRootOutcomeResult,
};
use bamboo_tickets::{
    AssignmentState, Authority, Command, Contract, DispatchIntent, DispatchSpec, Error,
    ManagedWorkRoot, Operation, OperationReceipt, Principal, Result, RuntimeReceipt, TicketService,
};

const PLAN_GUIDANCE: &str = concat!(
    "Complete the Work contract below in the assigned workspace. ",
    "Use Task to record and update your plan and progress. "
);
const READ_GUIDANCE: &str = "Use absolute paths inside the assigned worktree for Read. ";
const HANDOFF_GUIDANCE: &str =
    "Finish with a handoff covering each acceptance item and any open findings for Supervisor review.";

pub const WORK_HANDOFF_GUIDANCE: &str = concat!(
    "Put the full text of any requested checklist, note, or Markdown deliverable in the final reply. The Host saves that reply on the original Work as the artifact and computes its hash. ",
    "Submit once the deliverable is ready. The designated user reviews and accepts the submission from the Work card afterward. Ask a question only if an essential input prevents you from producing the deliverable. ",
    "Verify each acceptance item with the tools and evidence authorized for this Work. ",
    "In the final reply, give each item's status (passed, failed, or unverified), the check actually performed, its result, and an evidence reference when available. ",
    "For code changes, give the changed paths and the exact commit SHA if one exists. ",
    "For failed or unverified items, state the blocker and the next authorized check or decision. ",
    "Give the Supervisor the handoff and evidence needed to review the submission."
);

pub fn validate_start(spec: &DispatchSpec) -> Result<()> {
    if spec.workspace.is_none()
        || !spec.contract.allowed_tools.contains("Task")
        || spec
            .contract
            .allowed_tools
            .iter()
            .any(|tool| !matches!(tool.as_str(), "Read" | "Task"))
    {
        return Err(Error::ScopeDenied(
            "Managed Work requires an explicit worktree and a Task or Read/Task contract".into(),
        ));
    }
    Ok(())
}

pub fn first_instruction(
    service: &TicketService,
    spec: &DispatchSpec,
    max_bytes: usize,
) -> Result<String> {
    let runtime = Authority::from_verified_host(spec.binding.clone(), Principal::Runtime);
    let packet = service.child_context_packet(&runtime, &spec.assignment_id, max_bytes)?;
    format_instruction(&packet, max_bytes)
}

pub fn format_instruction(
    packet: &bamboo_tickets::WorkContextPacket,
    max_bytes: usize,
) -> Result<String> {
    let read_guidance = if packet.contract.allowed_tools.contains("Read") {
        READ_GUIDANCE
    } else {
        ""
    };
    let message = format!(
        "{PLAN_GUIDANCE}{read_guidance}{HANDOFF_GUIDANCE}\n\n{}",
        serde_json::to_string(packet)?,
    );
    if message.len() > max_bytes {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(message)
}

/// Reconstruct the historical first input for comparison with a stored creation receipt.
pub fn legacy_task_only_instruction_for_replay(
    message: &str,
    contract: &Contract,
) -> Option<String> {
    if contract.allowed_tools.len() != 1 || !contract.allowed_tools.contains("Task") {
        return None;
    }
    let packet = message
        .strip_prefix(PLAN_GUIDANCE)?
        .strip_prefix(HANDOFF_GUIDANCE)?
        .strip_prefix("\n\n")?;
    Some(format!(
        "{PLAN_GUIDANCE}{READ_GUIDANCE}{HANDOFF_GUIDANCE}\n\n{packet}"
    ))
}

pub fn matches_root(target: &ManagedWorkRoot, spec: &DispatchSpec, session: &Session) -> bool {
    session.kind == SessionKind::Root
        && session.authority_identity == SessionAuthorityIdentity::Ordinary
        && session.parent_session_id.is_none()
        && session.parent_created_at.is_none()
        && session.root_session_id == session.id
        && target.session_id().as_deref() == Ok(session.id.as_str())
        && session.project_id_meta().as_deref() == Some(target.project_id.as_str())
        && session.conversation_reset.is_none()
        && spec.workspace.as_ref().is_some_and(|workspace| {
            session.workspace_path_meta().as_deref() == Some(workspace.worktree.as_str())
                && session
                    .workspace
                    .as_ref()
                    .is_none_or(|path| path == &workspace.worktree)
        })
}

/// Admission consumes only a verified Root Inbox ACK from the active owner.
/// The Ticket transaction also fences assignment generation and claims.
pub fn admit(
    service: &TicketService,
    key: &str,
    spec: &DispatchSpec,
    session: &Session,
    admission: ManagedRootAdmission,
) -> Result<RuntimeReceipt> {
    let (_, snapshot) = service.published()?;
    let intent = snapshot
        .intents
        .get(key)
        .filter(|intent| &intent.immutable_spec == spec)
        .ok_or_else(|| Error::ScopeDenied("managed Work intent changed".into()))?;
    if !spec
        .managed_root
        .as_ref()
        .is_some_and(|target| matches_root(target, spec, session))
        || session.created_at != admission.target_created_at
        || session.id != admission.target_session_id
    {
        return Err(Error::ScopeDenied(
            "managed Work Session lifetime changed".into(),
        ));
    }
    let receipt = RuntimeReceipt {
        dispatch_key: key.into(),
        spec_hash: intent.spec_hash.clone(),
        run_id: admission.run_id.clone(),
        session_id: admission.target_session_id.clone(),
    };
    let runtime = Authority::from_verified_host(spec.binding.clone(), Principal::Runtime);
    execute_runtime_command(
        service,
        &runtime,
        &format!("runtime-admit/{key}"),
        vec![
            Operation::ManagedAdmitted {
                assignment_id: spec.assignment_id.clone(),
                receipt: receipt.clone(),
                admission,
            },
            Operation::Running {
                assignment_id: spec.assignment_id.clone(),
            },
        ],
    )?;
    Ok(receipt)
}

pub fn execute_runtime_command(
    service: &TicketService,
    authority: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> Result<OperationReceipt> {
    for attempt in 0..4 {
        let command = service.prepare_command(authority, id, operations.clone())?;
        match service.execute(authority, &command) {
            Err(Error::RevisionConflict) if attempt < 3 => continue,
            result => return result,
        }
    }
    unreachable!()
}

/// The Host has proved no launch or joined the exact unadmitted Root. A
/// concurrent admission or a changed assignment must keep its own stop path.
pub fn confirm_unadmitted_stop(
    service: &TicketService,
    key: &str,
    spec: &DispatchSpec,
) -> Result<()> {
    let (_, snapshot) = service.published()?;
    let intent = snapshot
        .intents
        .get(key)
        .filter(|intent| &intent.immutable_spec == spec && spec.managed_root.is_some())
        .ok_or(Error::IdempotencyConflict)?;
    let assignment = &snapshot.assignments[&intent.assignment_id];
    if intent.receipt.is_some()
        || intent.managed_admission.is_some()
        || assignment.runtime.is_some()
    {
        return Err(Error::ScopeDenied("managed Work was admitted".into()));
    }
    if assignment.process_stopped {
        return Ok(());
    }
    if !matches!(
        assignment.state,
        AssignmentState::Cancelling | AssignmentState::OutcomeUnknown
    ) {
        return Err(Error::ScopeDenied(
            "managed Work stop was not requested".into(),
        ));
    }
    let authority = Authority::from_verified_host(snapshot.binding.clone(), Principal::Runtime);
    service.execute(
        &authority,
        &Command {
            operation_id: format!("runtime-queued-stop/{key}"),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            operations: vec![Operation::ConfirmStopped {
                assignment_id: assignment.id.clone(),
                effects_reconciled: true,
            }],
            source: None,
        },
    )?;
    Ok(())
}

/// The Host calls this after joining the Root task and its exact resources.
/// Canonical Main supplies the outcome and reply, independently of stop proof.
pub fn publish_stopped(
    service: &TicketService,
    receipt: &RuntimeReceipt,
    main: &Session,
) -> Result<()> {
    let (_, snapshot) = service.published()?;
    let intent = snapshot
        .intents
        .get(&receipt.dispatch_key)
        .ok_or_else(|| Error::ScopeDenied("completed Work intent missing".into()))?;
    let reply = terminal_reply(intent, receipt, main)?;
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    execute_runtime_command(
        service,
        &runtime,
        &format!("runtime-stop/{}/{}", receipt.dispatch_key, receipt.run_id),
        vec![Operation::RuntimeStopped {
            assignment_id: intent.assignment_id.clone(),
            receipt: receipt.clone(),
            completed: reply.is_some(),
        }],
    )?;
    if let Some(reply) = reply {
        publish_reply(service, &runtime, intent, receipt, reply)?;
    }
    Ok(())
}

/// Only a stop already published by the exact owner authorizes result repair.
pub fn reconcile_stopped_result(
    service: &TicketService,
    receipt: &RuntimeReceipt,
    main: &Session,
) -> Result<()> {
    let (_, snapshot) = service.published()?;
    let intent = snapshot
        .intents
        .get(&receipt.dispatch_key)
        .ok_or_else(|| Error::ScopeDenied("stopped Work intent missing".into()))?;
    let assignment = &snapshot.assignments[&intent.assignment_id];
    if !assignment.process_stopped
        || snapshot.receipts.contains_key(&format!(
            "runtime-submit/{}/{}",
            receipt.dispatch_key, receipt.run_id
        ))
    {
        return Ok(());
    }
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    if let Some(reply) = terminal_reply(intent, receipt, main)? {
        if assignment.state == bamboo_tickets::AssignmentState::OutcomeUnknown
            && snapshot.tickets[&assignment.work_id]
                .active_assignment
                .as_deref()
                == Some(&assignment.id)
        {
            let reconciliation = execute_runtime_command(
                service,
                &runtime,
                &format!(
                    "runtime-completed-reconcile/{}/{}",
                    receipt.dispatch_key, snapshot.authority_epoch
                ),
                vec![Operation::ReconcileCompleted {
                    assignment_id: assignment.id.clone(),
                    receipt: receipt.clone(),
                }],
            );
            // Invalidated attempts still retain their exact late result.
            if let Err(error) = reconciliation {
                if !matches!(
                    error,
                    Error::ResourceBlocked(_) | Error::InvalidTransition(_)
                ) {
                    return Err(error);
                }
            }
        }
        publish_reply(service, &runtime, intent, receipt, reply)?;
    }
    Ok(())
}

/// Resolve the sealed reply for an exact managed Work attempt in canonical
/// Main. A newer assistant message or an old Session birth cannot substitute.
pub fn terminal_reply<'a>(
    intent: &DispatchIntent,
    receipt: &RuntimeReceipt,
    main: &'a Session,
) -> Result<Option<&'a Message>> {
    let spec = &intent.immutable_spec;
    let admission = intent
        .managed_admission
        .as_ref()
        .filter(|admission| {
            intent.receipt.as_ref() == Some(receipt)
                && admission.target_created_at == main.created_at
                && spec
                    .managed_root
                    .as_ref()
                    .is_some_and(|target| matches_root(target, spec, main))
        })
        .ok_or_else(|| Error::ScopeDenied("completed managed Work lifetime changed".into()))?;
    let (fact, _) = main
        .runtime_metadata
        .as_ref()
        .and_then(|metadata| metadata.supervised_root_outcomes.as_ref())
        .and_then(|outcomes| {
            outcomes.unique_input_run(&SessionInboxReceipt {
                id: admission.input.receipt_id.clone(),
                generation: admission.input.generation,
            })
        })
        .filter(|(fact, _)| fact.run_id == receipt.run_id)
        .ok_or_else(|| {
            Error::ScopeDenied("managed Work outcome lacks its exact admitted Run".into())
        })?;
    match fact.result {
        Some(SupervisedRootOutcomeResult::Completed) => {
            let reference = fact.verified_final_message(main);
            Ok(reference.and_then(|reference| {
                main.messages
                    .iter()
                    .find(|message| message.id == reference.message_id)
            }))
        }
        Some(
            SupervisedRootOutcomeResult::Failed
            | SupervisedRootOutcomeResult::Cancelled
            | SupervisedRootOutcomeResult::Suspended,
        ) => Ok(None),
        _ => Err(Error::AuthorityUnavailable(
            "managed Work terminal outcome unconfirmed".into(),
        )),
    }
}

fn publish_reply(
    service: &TicketService,
    runtime: &Authority,
    intent: &DispatchIntent,
    receipt: &RuntimeReceipt,
    reply: &Message,
) -> Result<()> {
    let spec = &intent.immutable_spec;
    let artifact = service.store_artifact(runtime, reply.content.as_bytes())?;
    let evidence = format!(
        "canonical Root {}; run {}; message {}; sha256 {}",
        receipt.session_id, receipt.run_id, reply.id, artifact.sha256,
    );
    let worker = Authority::from_verified_host(
        spec.binding.clone(),
        Principal::Worker {
            assignment_id: spec.assignment_id.clone(),
            generation: spec.generation,
            run_id: receipt.run_id.clone(),
            session_id: receipt.session_id.clone(),
        },
    );
    execute_runtime_command(
        service,
        &worker,
        &format!("runtime-submit/{}/{}", receipt.dispatch_key, receipt.run_id),
        vec![Operation::Submit {
            assignment_id: spec.assignment_id.clone(),
            temp_id: "submission".into(),
            artifacts: vec![artifact],
            evidence: vec![evidence],
        }],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_tickets::{ExecutionWorkspace, ScopeBinding, WorkContextPacket};

    #[test]
    fn first_instruction_respects_allowed_tools_and_the_byte_budget() {
        let mut packet = WorkContextPacket {
            project_context: None,
            contract_ref: "work-fixture".into(),
            contract_revision: 1,
            generation: 1,
            assignment_id: "assignment-fixture".into(),
            binding: ScopeBinding {
                scope_id: "scope-fixture".into(),
                supervisor_session_id: "supervisor-fixture".into(),
                binding_revision: 1,
            },
            contract: Contract {
                title: "Fixture".into(),
                objective: "交付结果".into(),
                constraints: vec![],
                acceptance: vec![],
                user_acceptance_required: false,
                allowed_tools: ["Task".into()].into(),
            },
            workspace: Some(ExecutionWorkspace {
                repo: "fixture".into(),
                base_commit: "base".into(),
                branch: "branch".into(),
                worktree: "/fixture/worktree".into(),
                write_roots: vec![],
                claims: Default::default(),
            }),
            inputs: vec![],
            input_artifacts: vec![],
            answers: vec![],
            result_contract: "Handoff".into(),
        };
        let task_only_guidance = concat!(
            "Complete the Work contract below in the assigned workspace. ",
            "Use Task to record and update your plan and progress. ",
            "Finish with a handoff covering each acceptance item and any open findings ",
            "for Supervisor review."
        );
        let read_task_guidance = concat!(
            "Complete the Work contract below in the assigned workspace. ",
            "Use Task to record and update your plan and progress. ",
            "Use absolute paths inside the assigned worktree for Read. ",
            "Finish with a handoff covering each acceptance item and any open findings ",
            "for Supervisor review."
        );

        for guidance in [task_only_guidance, read_task_guidance] {
            let serialized = serde_json::to_string(&packet).unwrap();
            let expected = format!("{guidance}\n\n{serialized}");
            assert_eq!(format_instruction(&packet, 4096).unwrap(), expected);
            assert_eq!(
                format_instruction(&packet, expected.len()).unwrap(),
                expected
            );
            assert!(matches!(
                format_instruction(&packet, expected.len() - 1),
                Err(Error::ContextBudgetExceeded)
            ));
            let historical = legacy_task_only_instruction_for_replay(&expected, &packet.contract);
            if guidance == task_only_guidance {
                assert_eq!(
                    historical,
                    Some(format!("{read_task_guidance}\n\n{serialized}"))
                );
            } else {
                assert!(historical.is_none());
            }
            packet.contract.allowed_tools.insert("Read".into());
        }
    }
}
