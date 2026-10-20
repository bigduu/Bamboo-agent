use bamboo_domain::{ProjectId, TaskPriority};
use bamboo_tickets::*;
use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "planning".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}
fn authority(principal: Principal) -> Authority {
    Authority::from_verified_host(binding(), principal)
}
fn owner() -> Authority {
    authority(Principal::User {
        user_id: "owner".into(),
    })
}
fn runtime() -> Authority {
    authority(Principal::Runtime)
}
fn command(service: &TicketService, id: &str, operations: Vec<Operation>) -> Command {
    service.prepare_command(&owner(), id, operations).unwrap()
}
fn execute(service: &TicketService, id: &str, ops: Vec<Operation>) -> OperationReceipt {
    service
        .execute(&owner(), &command(service, id, ops))
        .unwrap()
}
fn create(service: &TicketService) -> String {
    execute(
        service,
        "create",
        vec![Operation::Create {
            temp_id: "w".into(),
            kind: TicketKind::Work,
            parent: None,
            depends_on: BTreeSet::new(),
            contract: Contract {
                title: "Planning".into(),
                objective: "keep execution evidence".into(),
                constraints: vec![],
                acceptance: vec!["User review".into()],
                user_acceptance_required: true,
                allowed_tools: BTreeSet::from(["Task".into()]),
            },
        }],
    )
    .ids["w"]
        .clone()
}
fn set(service: &TicketService, id: &str, priority: TaskPriority, time: Option<&str>) -> Operation {
    Operation::SetPlanningMetadata {
        ticket_id: id.into(),
        expected_record_revision: service.published().unwrap().1.tickets[id].record_revision,
        priority,
        expected_completion_at: time.map(str::to_owned),
    }
}
fn same_snapshot(service: &TicketService, before: &(String, Snapshot)) {
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}
fn running(service: &TicketService, id: &str) -> (String, Authority) {
    let assignment = execute(
        service,
        "start",
        vec![
            Operation::Ready { work_id: id.into() },
            Operation::Start {
                work_id: id.into(),
                temp_id: "a".into(),
                workspace: None,
            },
        ],
    )
    .ids["a"]
        .clone();
    let s = service.published().unwrap().1;
    let a = &s.assignments[&assignment];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: s.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: "run".into(),
        session_id: "child".into(),
    };
    service
        .execute(
            &runtime(),
            &command(
                service,
                "admit",
                vec![
                    Operation::Admitted {
                        assignment_id: assignment.clone(),
                        receipt,
                    },
                    Operation::Running {
                        assignment_id: assignment.clone(),
                    },
                ],
            ),
        )
        .unwrap();
    let worker = authority(Principal::Worker {
        assignment_id: assignment.clone(),
        generation: a.generation,
        run_id: "run".into(),
        session_id: "child".into(),
    });
    (assignment, worker)
}

#[test]
fn legacy_defaults_preserve_wire_hashes_and_commands_require_explicit_null() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let (_, snapshot) = service.published().unwrap();
    assert_eq!(snapshot.schema, 1);
    let ticket = &snapshot.tickets[&id];
    assert_eq!(ticket.priority, TaskPriority::Medium);
    assert!(ticket.expected_completion_at.is_none());
    let wire = canonical_bytes(&snapshot).unwrap();
    let value = serde_json::to_value(ticket).unwrap();
    assert!(value.get("priority").is_none());
    assert!(value.get("expected_completion_at").is_none());
    let restored: Snapshot = serde_json::from_slice(&wire).unwrap();
    assert_eq!(wire, canonical_bytes(&restored).unwrap());
    let mut operation = serde_json::json!({"op":"set_planning_metadata", "ticket_id":id, "expected_record_revision":1, "priority":"high"});
    assert!(serde_json::from_value::<Operation>(operation.clone()).is_err());
    operation["expected_completion_at"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<Operation>(operation.clone()).is_ok());
    operation["priority"] = "urgent".into();
    assert!(serde_json::from_value::<Operation>(operation).is_err());
}

#[test]
fn owner_metadata_is_audited_replayable_and_cleared_without_schema_downgrade() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let op = set(
        &service,
        &id,
        TaskPriority::High,
        Some("2026-11-01T09:30:00+05:30"),
    );
    let c = command(&service, "owner-plan", vec![op]);
    let receipt = service.execute(&owner(), &c).unwrap();
    assert_eq!(receipt.principal, "user:owner");
    assert_eq!(
        receipt.canonical_request.as_bytes(),
        canonical_bytes(&c).unwrap()
    );
    let s = service.published().unwrap().1;
    assert_eq!(s.schema, 7);
    assert_eq!(
        s.tickets[&id].expected_completion_at.as_deref(),
        Some("2026-11-01T09:30:00+05:30")
    );
    assert_eq!(s.tickets[&id].priority, TaskPriority::High);
    assert_eq!(s.tickets[&id].record_revision, 2);
    assert_eq!(service.execute(&owner(), &c).unwrap(), receipt);
    let mut changed = c.clone();
    changed.operations = vec![set(&service, &id, TaskPriority::Critical, None)];
    assert!(matches!(
        service.execute(&owner(), &changed),
        Err(Error::IdempotencyConflict)
    ));
    let other = authority(Principal::User {
        user_id: "other".into(),
    });
    assert!(matches!(
        service.execute(&other, &c),
        Err(Error::ScopeDenied(_))
    ));
    drop(service);
    let service = TicketService::open(dir.path(), binding()).unwrap();
    assert_eq!(service.execute(&owner(), &c).unwrap(), receipt);
    let clear = set(&service, &id, TaskPriority::Medium, None);
    execute(&service, "clear-plan", vec![clear]);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.schema, 7);
    assert_eq!(snapshot.tickets[&id].priority, TaskPriority::Medium);
    assert!(snapshot.tickets[&id].expected_completion_at.is_none());
    assert!(snapshot.receipts.contains_key("owner-plan"));
}

#[test]
fn competing_owner_updates_share_scope_cas_and_fence_the_original_ticket_revision() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let commands = [
        command(
            &service,
            "left",
            vec![set(&service, &id, TaskPriority::Low, None)],
        ),
        command(
            &service,
            "right",
            vec![set(&service, &id, TaskPriority::Critical, None)],
        ),
    ];
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = commands
        .into_iter()
        .map(|c| {
            let service = service.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.execute(&owner(), &c)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::RevisionConflict)))
            .count(),
        1
    );
    let before = service.published().unwrap();
    let stale = Operation::SetPlanningMetadata {
        ticket_id: id,
        expected_record_revision: 1,
        priority: TaskPriority::High,
        expected_completion_at: None,
    };
    assert!(matches!(
        service.execute(
            &owner(),
            &command(&service, "fresh-scope-stale-row", vec![stale])
        ),
        Err(Error::RevisionConflict)
    ));
    same_snapshot(&service, &before);
}

#[test]
fn metadata_requires_owner_identity_even_for_an_existing_live_worker() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let (_, worker) = running(&service, &id);
    let before = service.published().unwrap();
    let c = command(
        &service,
        "denied",
        vec![set(&service, &id, TaskPriority::Critical, None)],
    );
    for who in [
        worker,
        runtime(),
        authority(Principal::Supervisor {
            session_id: "supervisor".into(),
        }),
        Authority::from_verified_host(
            ScopeBinding {
                scope_id: "other".into(),
                ..binding()
            },
            Principal::User {
                user_id: "owner".into(),
            },
        ),
    ] {
        assert!(matches!(
            service.execute(&who, &c),
            Err(Error::ScopeDenied(_))
        ));
        same_snapshot(&service, &before);
    }
}

#[test]
fn malformed_or_unknown_timezone_and_partial_batches_publish_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let before = service.published().unwrap();
    for (n, time) in [
        "",
        "tomorrow",
        "2026-11-01T09:30:00",
        "2026-02-30T09:30:00Z",
        "2026-11-01T09:30:00-00:00",
        "2026-11-01T09:30:00+25:00",
        "2026-11-01 09:30:00Z",
        "2026-11-01T09:30:00Z ",
    ]
    .into_iter()
    .enumerate()
    {
        let c = command(
            &service,
            &format!("invalid-{n}"),
            vec![set(&service, &id, TaskPriority::High, Some(time))],
        );
        assert!(
            matches!(
                service.execute(&owner(), &c),
                Err(Error::InvalidTransition(_))
            ),
            "{time}"
        );
        same_snapshot(&service, &before);
    }
    let valid = set(
        &service,
        &id,
        TaskPriority::Low,
        Some("2026-11-01T09:30:00Z"),
    );
    let invalid = Operation::SetPlanningMetadata {
        ticket_id: id,
        expected_record_revision: 2,
        priority: TaskPriority::Critical,
        expected_completion_at: Some("naive".into()),
    };
    assert!(service
        .execute(&owner(), &command(&service, "atomic", vec![valid, invalid]))
        .is_err());
    same_snapshot(&service, &before);
}

#[test]
fn active_and_submitted_planning_edits_preserve_execution_and_user_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let id = create(&service);
    let (assignment, worker) = running(&service, &id);
    let before = service.published().unwrap().1;
    execute(
        &service,
        "active-plan",
        vec![set(
            &service,
            &id,
            TaskPriority::High,
            Some("2026-12-01T00:00:00Z"),
        )],
    );
    let after = service.published().unwrap().1;
    assert_eq!(
        canonical_bytes(&before.assignments).unwrap(),
        canonical_bytes(&after.assignments).unwrap()
    );
    assert_eq!(
        canonical_bytes(&before.intents).unwrap(),
        canonical_bytes(&after.intents).unwrap()
    );
    assert_eq!(
        before.tickets[&id].generation,
        after.tickets[&id].generation
    );
    assert_eq!(
        before.tickets[&id].contract_revision,
        after.tickets[&id].contract_revision
    );
    let artifact = service.store_artifact(&runtime(), b"real output").unwrap();
    let submit = command(
        &service,
        "submit",
        vec![Operation::Submit {
            assignment_id: assignment.clone(),
            temp_id: "s".into(),
            artifacts: vec![artifact],
            evidence: vec!["worker claim remains unverified".into()],
        }],
    );
    let submission = service.execute(&worker, &submit).unwrap().ids["s"].clone();
    let before = service.published().unwrap().1;
    assert!(before.tickets[&id].active_assignment.is_none());
    assert!(!before.assignments[&assignment].process_stopped);
    execute(
        &service,
        "submitted-plan",
        vec![set(&service, &id, TaskPriority::Critical, None)],
    );
    let after = service.published().unwrap().1;
    assert_eq!(
        canonical_bytes(&before.submissions).unwrap(),
        canonical_bytes(&after.submissions).unwrap()
    );
    assert_eq!(after.tickets[&id].state, WorkState::Submitted);
    service
        .execute(
            &runtime(),
            &command(
                &service,
                "stopped",
                vec![Operation::ConfirmStopped {
                    assignment_id: assignment,
                    effects_reconciled: true,
                }],
            ),
        )
        .unwrap();
    let accept = command(
        &service,
        "accept",
        vec![Operation::Accept {
            work_id: id.clone(),
            submission_id: submission.clone(),
            evidence: vec!["explicit User review; no automated test claim".into()],
        }],
    );
    assert!(matches!(
        service.execute(
            &authority(Principal::Supervisor {
                session_id: "supervisor".into()
            }),
            &accept
        ),
        Err(Error::ScopeDenied(_))
    ));
    service.execute(&owner(), &accept).unwrap();
    let accepted = service.published().unwrap().1;
    assert_eq!(
        accepted.tickets[&id].accepted_submission.as_deref(),
        Some(submission.as_str())
    );
    assert_eq!(
        accepted.submissions[&submission].evidence,
        vec!["worker claim remains unverified"]
    );
}

#[test]
fn schema_seven_stopped_transfer_retains_sprint_metadata_and_command_history() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let destination = dir.path().join("destination");
    let service = TicketService::open(&root, binding()).unwrap();
    let id = create(&service);
    let project = ProjectId::parse("planning-project").unwrap();
    execute(
        &service,
        "link",
        vec![Operation::AssociateProject {
            ticket_id: id.clone(),
            project_id: project.clone(),
        }],
    );
    let s = service.published().unwrap().1;
    service
        .execute_sprint(
            &owner(),
            &SprintCommand {
                operation_id: "sprint".into(),
                binding: s.binding,
                expected_seq: s.seq,
                expected_epoch: s.authority_epoch,
                project_id: project,
                sprint_id: SprintId::parse("next").unwrap(),
                source: "owner plan".into(),
                action: SprintAction::Create {
                    definition: SprintDefinition {
                        name: "next".into(),
                        goal: "retain planning".into(),
                        starts_at: None,
                        ends_at: None,
                    },
                },
            },
        )
        .unwrap();
    assert_eq!(service.published().unwrap().1.schema, 6);
    execute(
        &service,
        "plan",
        vec![set(
            &service,
            &id,
            TaskPriority::Critical,
            Some("2026-12-01T18:00:00+08:00"),
        )],
    );
    let before = service.published().unwrap().1;
    drop(service);
    let source = TicketService::open_offline(&root, binding()).unwrap();
    let (commit, snapshot) = source.published().unwrap();
    let request = MigrationRequest {
        operation_id: "transfer".into(),
        binding: binding(),
        expected_commit: commit,
        expected_seq: snapshot.seq,
        expected_epoch: snapshot.authority_epoch,
        source_root: root.canonicalize().unwrap().to_string_lossy().into(),
        destination_root: canonical_migration_destination(&destination).unwrap(),
        supervisor_snapshot_hash: None,
    };
    let proof = StoppedScopeProof::from_verified_host(binding());
    source
        .retire_for_migration(&owner(), &request, &proof)
        .unwrap();
    source
        .export_retired_migration(&owner(), &request, &proof)
        .unwrap();
    let target = TicketService::open_offline(&destination, binding()).unwrap();
    target
        .activate_migrated_copy(&source, &owner(), &request, &proof)
        .unwrap();
    let after = target.published().unwrap().1;
    assert_eq!(after.schema, 7);
    assert_eq!(
        canonical_bytes(&before.sprint).unwrap(),
        canonical_bytes(&after.sprint).unwrap()
    );
    assert_eq!(after.tickets[&id].priority, TaskPriority::Critical);
    assert_eq!(
        after.tickets[&id].expected_completion_at,
        before.tickets[&id].expected_completion_at
    );
    assert_eq!(after.receipts["plan"], before.receipts["plan"]);
    assert_eq!(after.receipts["sprint"], before.receipts["sprint"]);
}
