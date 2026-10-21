use bamboo_tickets::*;
use std::collections::BTreeSet;

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "completion".into(),
        supervisor_session_id: "root".into(),
        binding_revision: 1,
    }
}
fn owner() -> Authority {
    Authority::from_verified_host(
        binding(),
        Principal::User {
            user_id: "owner".into(),
        },
    )
}
fn runtime() -> Authority {
    Authority::from_verified_host(binding(), Principal::Runtime)
}
fn command(service: &TicketService, id: &str, ops: Vec<Operation>) -> Command {
    service.prepare_command(&owner(), id, ops).unwrap()
}
fn execute(
    service: &TicketService,
    who: &Authority,
    id: &str,
    ops: Vec<Operation>,
) -> OperationReceipt {
    service.execute(who, &command(service, id, ops)).unwrap()
}
fn create(service: &TicketService) -> String {
    execute(
        service,
        &owner(),
        "create",
        vec![
            Operation::Create {
                temp_id: "w".into(),
                kind: TicketKind::Work,
                parent: None,
                depends_on: BTreeSet::new(),
                contract: Contract {
                    title: "Owner completion".into(),
                    objective: "Keep evidence".into(),
                    constraints: vec![],
                    acceptance: vec!["User review".into()],
                    user_acceptance_required: true,
                    allowed_tools: BTreeSet::from(["Task".into()]),
                },
            },
            Operation::Ready {
                work_id: "w".into(),
            },
        ],
    )
    .ids["w"]
        .clone()
}
fn running(service: &TicketService, work: &str, n: u64) -> (String, RuntimeReceipt, Authority) {
    let id = execute(
        service,
        &owner(),
        &format!("start-{n}"),
        vec![Operation::Start {
            work_id: work.into(),
            temp_id: "a".into(),
            workspace: None,
        }],
    )
    .ids["a"]
        .clone();
    let s = service.published().unwrap().1;
    let a = &s.assignments[&id];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: s.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: format!("run-{n}"),
        session_id: format!("child-{n}"),
    };
    execute(
        service,
        &runtime(),
        &format!("admit-{n}"),
        vec![
            Operation::Admitted {
                assignment_id: id.clone(),
                receipt: receipt.clone(),
            },
            Operation::Running {
                assignment_id: id.clone(),
            },
        ],
    );
    let worker = Authority::from_verified_host(
        binding(),
        Principal::Worker {
            assignment_id: id.clone(),
            generation: a.generation,
            run_id: receipt.run_id.clone(),
            session_id: receipt.session_id.clone(),
        },
    );
    (id, receipt, worker)
}
fn submit(service: &TicketService, worker: &Authority, assignment: &str, n: u64) -> String {
    let artifact = service
        .store_artifact(&runtime(), b"real preserved output")
        .unwrap();
    execute(
        service,
        worker,
        &format!("submit-{n}"),
        vec![Operation::Submit {
            assignment_id: assignment.into(),
            temp_id: "s".into(),
            artifacts: vec![artifact],
            evidence: vec!["Worker evidence remains unverified until User review".into()],
        }],
    )
    .ids["s"]
        .clone()
}
fn accept(work: &str, submission: &str) -> Operation {
    Operation::Accept {
        work_id: work.into(),
        submission_id: submission.into(),
        evidence: vec!["explicit User acceptance, not automated validation".into()],
    }
}

#[test]
fn cleared_active_assignment_does_not_authorize_acceptance_until_host_stop() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service);
    let (assignment, receipt, worker) = running(&service, &work, 1);
    let submission = submit(&service, &worker, &assignment, 1);
    let before = service.published().unwrap();
    assert!(before.1.tickets[&work].active_assignment.is_none());
    assert!(!before.1.assignments[&assignment].process_stopped);
    let c = command(&service, "accept", vec![accept(&work, &submission)]);
    assert!(matches!(
        service.execute(&owner(), &c),
        Err(Error::ResourceBlocked(_))
    ));
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    let stop = command(
        &service,
        "runtime-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: assignment.clone(),
            receipt: receipt.clone(),
            completed: true,
        }],
    );
    assert!(matches!(
        service.execute(&owner(), &stop),
        Err(Error::ScopeDenied(_))
    ));
    let mut wrong = stop.clone();
    if let Operation::RuntimeStopped { receipt, .. } = &mut wrong.operations[0] {
        receipt.run_id = "unbound".into();
    }
    assert!(matches!(
        service.execute(&runtime(), &wrong),
        Err(Error::ScopeDenied(_))
    ));
    service.execute(&runtime(), &stop).unwrap();
    let acceptance = command(&service, "accept", vec![accept(&work, &submission)]);
    let supervisor = Authority::from_verified_host(
        binding(),
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    assert!(matches!(
        service.execute(&supervisor, &acceptance),
        Err(Error::ScopeDenied(_))
    ));
    let accepted = service.execute(&owner(), &acceptance).unwrap();
    assert_eq!(accepted.principal, "user:owner");
    assert_eq!(service.execute(&owner(), &acceptance).unwrap(), accepted);
    let after = service.published().unwrap().1;
    assert_eq!(after.tickets[&work].state, WorkState::Accepted);
    assert_eq!(
        canonical_bytes(&before.1.submissions).unwrap(),
        canonical_bytes(&after.submissions).unwrap()
    );
    drop(service);
    let restored = TicketService::open(dir.path(), binding()).unwrap();
    assert_eq!(restored.execute(&owner(), &acceptance).unwrap(), accepted);
    assert!(restored.published().unwrap().1.assignments[&assignment].process_stopped);
}

#[test]
fn an_old_generation_stop_receipt_cannot_confirm_the_new_submission_process() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service);
    let (first, old_receipt, worker) = running(&service, &work, 1);
    let first_submission = submit(&service, &worker, &first, 1);
    execute(
        &service,
        &runtime(),
        "first-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: first.clone(),
            receipt: old_receipt.clone(),
            completed: true,
        }],
    );
    execute(
        &service,
        &owner(),
        "first-accept",
        vec![accept(&work, &first_submission)],
    );
    execute(
        &service,
        &owner(),
        "reopen",
        vec![Operation::Reopen {
            work_id: work.clone(),
        }],
    );
    let (second, new_receipt, worker) = running(&service, &work, 2);
    let second_submission = submit(&service, &worker, &second, 2);
    execute(
        &service,
        &runtime(),
        "late-old-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: first,
            receipt: old_receipt,
            completed: true,
        }],
    );
    let c = command(
        &service,
        "second-accept",
        vec![accept(&work, &second_submission)],
    );
    assert!(matches!(
        service.execute(&owner(), &c),
        Err(Error::ResourceBlocked(_))
    ));
    let s = service.published().unwrap().1;
    assert!(!s.assignments[&second].process_stopped);
    assert_eq!(s.tickets[&work].state, WorkState::Submitted);
    assert_eq!(s.submissions.len(), 2);
    execute(
        &service,
        &runtime(),
        "second-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: second,
            receipt: new_receipt,
            completed: true,
        }],
    );
    execute(
        &service,
        &owner(),
        "second-accept",
        vec![accept(&work, &second_submission)],
    );
}

#[test]
fn process_stop_does_not_replace_the_exact_submission_or_user_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service);
    let (assignment, receipt, worker) = running(&service, &work, 1);
    let submission = submit(&service, &worker, &assignment, 1);
    execute(
        &service,
        &runtime(),
        "stop",
        vec![Operation::RuntimeStopped {
            assignment_id: assignment,
            receipt,
            completed: true,
        }],
    );
    let before = service.published().unwrap();
    for (n, op) in [
        Operation::Accept {
            work_id: work.clone(),
            submission_id: submission.clone(),
            evidence: vec![],
        },
        Operation::Accept {
            work_id: work.clone(),
            submission_id: "missing".into(),
            evidence: vec!["decision".into()],
        },
    ]
    .into_iter()
    .enumerate()
    {
        assert!(service
            .execute(
                &owner(),
                &command(&service, &format!("invalid-{n}"), vec![op])
            )
            .is_err());
        assert_eq!(
            serde_json::to_value(service.published().unwrap()).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    let revise = Operation::UpdateContract {
        work_id: work.clone(),
        contract: Contract {
            title: "Changed".into(),
            ..before.1.tickets[&work].contract.clone()
        },
    };
    execute(&service, &owner(), "revise", vec![revise]);
    assert!(service
        .execute(
            &owner(),
            &command(&service, "stale-accept", vec![accept(&work, &submission)])
        )
        .is_err());
    assert!(service.published().unwrap().1.tickets[&work]
        .accepted_submission
        .is_none());
}

#[test]
fn opt_in_control_facts_use_the_requested_snapshot_and_never_grant_worker_access() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service);
    let options = |commit| InspectOptions {
        sections: BTreeSet::from([InspectSection::Controls]),
        depth: 0,
        budget_bytes: 65536,
        fixed_commit: commit,
    };
    let view = service
        .work_inspect_sections(&owner(), std::slice::from_ref(&work), &options(None))
        .unwrap();
    let facts = view.data[0].controls.as_ref().unwrap();
    assert!(facts.accept_requires_stopped_assignment);
    assert!(facts.process_stopped.is_none());
    let (assignment, receipt, worker) = running(&service, &work, 1);
    let submission = submit(&service, &worker, &assignment, 1);
    let before_stop = service.published().unwrap().0;
    let view = service
        .work_inspect_sections(&owner(), std::slice::from_ref(&work), &options(None))
        .unwrap();
    let facts = view.data[0].controls.as_ref().unwrap();
    assert_eq!(facts.submission_id.as_deref(), Some(submission.as_str()));
    assert_eq!(facts.assignment_id.as_deref(), Some(assignment.as_str()));
    assert_eq!(facts.process_stopped, Some(false));
    assert!(matches!(
        service.work_inspect_sections(&worker, std::slice::from_ref(&work), &options(None)),
        Err(Error::ScopeDenied(_))
    ));
    execute(
        &service,
        &runtime(),
        "stop",
        vec![Operation::RuntimeStopped {
            assignment_id: assignment,
            receipt,
            completed: true,
        }],
    );
    let current = service
        .work_inspect_sections(&owner(), std::slice::from_ref(&work), &options(None))
        .unwrap();
    assert_eq!(
        current.data[0].controls.as_ref().unwrap().process_stopped,
        Some(true)
    );
    let historical = service
        .work_inspect_sections(
            &owner(),
            std::slice::from_ref(&work),
            &options(Some(before_stop)),
        )
        .unwrap();
    assert_eq!(
        historical.data[0]
            .controls
            .as_ref()
            .unwrap()
            .process_stopped,
        Some(false)
    );
    let legacy = service
        .work_inspect_sections(
            &owner(),
            &[work],
            &InspectOptions {
                sections: all_inspect_sections(),
                depth: 0,
                budget_bytes: 65536,
                fixed_commit: None,
            },
        )
        .unwrap();
    assert!(legacy.data[0].controls.is_none());
    assert!(serde_json::to_value(&legacy.data[0])
        .unwrap()
        .get("controls")
        .is_none());
}
