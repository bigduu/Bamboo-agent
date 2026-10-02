//! Deterministic application-service fixture, deliberately not a live Runtime.
use bamboo_tickets::*;
use std::collections::BTreeSet;

fn main() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let binding = ScopeBinding {
        scope_id: "fixture".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    };
    let service = TicketService::open(temporary.path(), binding.clone())?;
    let supervisor = Authority::from_verified_host(
        binding.clone(),
        Principal::Supervisor {
            session_id: "supervisor".into(),
        },
    );
    let runtime = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    let user = Authority::from_verified_host(
        binding.clone(),
        Principal::User {
            user_id: "fixture-user".into(),
        },
    );
    let execute =
        |authority: &Authority, op: &str, operations: Vec<Operation>| -> Result<OperationReceipt> {
            let snapshot = service.published()?.1;
            service.execute(
                authority,
                &Command {
                    operation_id: op.into(),
                    binding: binding.clone(),
                    expected_seq: snapshot.seq,
                    expected_epoch: snapshot.authority_epoch,
                    operations,
                },
            )
        };
    let receipt = execute(
        &supervisor,
        "create",
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: Contract {
                    title: "Deterministic sum".into(),
                    objective: "Compute 2+3".into(),
                    constraints: vec!["No external side effects".into()],
                    acceptance: vec!["Result equals 5".into()],
                    user_acceptance_required: true,
                    allowed_tools: BTreeSet::new(),
                },
                depends_on: BTreeSet::new(),
            },
            Operation::Ready {
                work_id: "work".into(),
            },
            Operation::Start {
                work_id: "work".into(),
                temp_id: "assignment".into(),
                workspace: None,
            },
        ],
    )?;
    let work = &receipt.ids["work"];
    let assignment = &receipt.ids["assignment"];
    let snapshot = service.published()?.1;
    let intent = snapshot.intents.values().next().expect("atomic intent");
    let receipt = RuntimeReceipt {
        dispatch_key: intent.dispatch_key.clone(),
        spec_hash: intent.spec_hash.clone(),
        run_id: "fixture-run".into(),
        session_id: "fixture-session".into(),
    };
    execute(
        &runtime,
        "admit",
        vec![
            Operation::Admitted {
                assignment_id: assignment.clone(),
                receipt: receipt.clone(),
            },
            Operation::Running {
                assignment_id: assignment.clone(),
            },
        ],
    )?;
    let worker = Authority::from_verified_host(
        binding.clone(),
        Principal::Worker {
            assignment_id: assignment.clone(),
            generation: 1,
            run_id: receipt.run_id,
            session_id: receipt.session_id,
        },
    );
    let output = std::thread::spawn(|| (2 + 3).to_string())
        .join()
        .expect("fake worker");
    let submission = execute(
        &worker,
        "submit",
        vec![Operation::Submit {
            assignment_id: assignment.clone(),
            temp_id: "submission".into(),
            artifacts: vec![Artifact {
                uri: "artifact://sum.txt".into(),
                sha256: content_hash(output.as_bytes()),
            }],
            evidence: vec![format!("output={output}")],
        }],
    )?
    .ids["submission"]
        .clone();
    assert_eq!(
        service.published()?.1.tickets[work].state,
        WorkState::Submitted
    );
    assert_eq!(output, "5");
    execute(
        &user,
        "accept",
        vec![Operation::Accept {
            work_id: work.clone(),
            submission_id: submission,
            evidence: vec!["Fixture user verifies sum=5".into()],
        }],
    )?;
    let snapshot = service.published()?;
    println!(
        "{}",
        serde_json::json!({"runtime_kind":"deterministic_service_fixture","commit":snapshot.0,"work_id":work,
        "generation":snapshot.1.tickets[work].generation,"state":snapshot.1.tickets[work].state,"receipt_count":snapshot.1.receipts.len(),"result":output})
    );
    Ok(())
}
