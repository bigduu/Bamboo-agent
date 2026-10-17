use bamboo_tickets::*;
use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "scope-a".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 7,
    }
}
fn authority(principal: Principal) -> Authority {
    Authority::from_verified_host(binding(), principal)
}
fn supervisor() -> Authority {
    authority(Principal::Supervisor {
        session_id: "supervisor".into(),
    })
}
fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: format!("Deliver {title}"),
        constraints: vec!["Keep the scope".into()],
        acceptance: vec!["Explicit evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}
fn command(service: &TicketService, id: &str, operations: Vec<Operation>) -> Command {
    let snapshot = service.published().unwrap().1;
    Command {
        operation_id: id.into(),
        binding: binding(),
        expected_seq: snapshot.seq,
        expected_epoch: snapshot.authority_epoch,
        source: None,
        operations,
    }
}
fn execute(
    service: &TicketService,
    authority: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> OperationReceipt {
    service
        .execute(authority, &command(service, id, operations))
        .unwrap()
}
fn create(service: &TicketService, title: &str, dependencies: BTreeSet<String>) -> String {
    execute(
        service,
        &supervisor(),
        &format!("create-{title}"),
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract(title),
                depends_on: dependencies,
            },
            Operation::Ready {
                work_id: "work".into(),
            },
        ],
    )
    .ids["work"]
        .clone()
}

fn observed(service: &TicketService) -> SnapshotRef {
    let (commit, snapshot) = service.published().unwrap();
    SnapshotRef {
        commit,
        seq: snapshot.seq,
        authority_epoch: snapshot.authority_epoch,
    }
}
fn candidates(ids: &[String]) -> Vec<ClaimCandidate> {
    ids.iter()
        .map(|id| ClaimCandidate {
            work_id: id.clone(),
            workspace: None,
        })
        .collect()
}
fn prepare(service: &TicketService, id: &str, work: &[String], capacity: usize) -> ClaimDecision {
    service
        .prepare_ready_claim(
            &supervisor(),
            &observed(service),
            id,
            &candidates(work),
            capacity,
        )
        .unwrap()
}
fn prepared(decision: ClaimDecision) -> Command {
    match decision {
        ClaimDecision::Prepared(command) => command,
        other => panic!("{other:?}"),
    }
}

#[test]
fn competing_consumers_cannot_double_claim_same_work_or_last_slot() {
    for same_work in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let service = TicketService::open(dir.path(), binding()).unwrap();
        let first = create(&service, "first", BTreeSet::new());
        let second = if same_work {
            first.clone()
        } else {
            create(&service, "second", BTreeSet::new())
        };
        let commands = [
            prepared(prepare(&service, "claim-a", &[first], 1)),
            prepared(prepare(&service, "claim-b", &[second], 1)),
        ];
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = commands
            .into_iter()
            .map(|command| {
                let service = service.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    service.execute(&supervisor(), &command)
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
        let snapshot = service.published().unwrap().1;
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(snapshot.intents.len(), 1);
    }
}

#[test]
fn lost_ack_replays_same_assignment_even_after_restart_without_new_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "durable", BTreeSet::new());
    let claim = prepared(prepare(&service, "claim", std::slice::from_ref(&work), 1));
    let receipt = service.execute(&supervisor(), &claim).unwrap();
    assert_eq!(
        service.execute(&supervisor(), &claim).unwrap().ids,
        receipt.ids
    );
    drop(service);
    let service = TicketService::open(dir.path(), binding()).unwrap();
    assert_eq!(
        service.execute(&supervisor(), &claim).unwrap().ids,
        receipt.ids
    );
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
    let assignment = &snapshot.assignments[&receipt.ids["assignment"]];
    assert_eq!(assignment.state, AssignmentState::OutcomeUnknown);
    assert_eq!(
        snapshot.intents[&assignment.dispatch_key].dispatch_key,
        assignment.dispatch_key
    );
    assert!(matches!(
        prepare(&service, "fresh-claim", &[work], 1),
        ClaimDecision::CapacityBlocked {
            occupied: 1,
            capacity: 1
        }
    ));
}

#[test]
fn fourth_work_stays_persisted_and_pending_dispatch_reserves_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work: Vec<_> = (0..4)
        .map(|i| create(&service, &format!("work-{i}"), BTreeSet::new()))
        .collect();
    for i in 0..3 {
        let claim = prepared(prepare(&service, &format!("claim-{i}"), &work, 3));
        let receipt = service.execute(&supervisor(), &claim).unwrap();
        assert_eq!(
            service.published().unwrap().1.assignments[&receipt.ids["assignment"]].work_id,
            work[i]
        );
    }
    let before = service.published().unwrap().0;
    assert!(matches!(
        prepare(&service, "fourth", &work, 3),
        ClaimDecision::CapacityBlocked {
            occupied: 3,
            capacity: 3
        }
    ));
    let (after, snapshot) = service.published().unwrap();
    assert_eq!(before, after);
    assert_eq!(snapshot.tickets[&work[3]].state, WorkState::Ready);
    assert_eq!(snapshot.tickets.len(), 4);
}

#[test]
fn stale_host_eligibility_and_unaccepted_dependencies_cannot_claim() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let stale = observed(&service);
    let upstream = create(&service, "upstream", BTreeSet::new());
    let dependent = create(&service, "dependent", BTreeSet::from([upstream]));
    assert!(matches!(
        service.prepare_ready_claim(
            &supervisor(),
            &stale,
            "stale",
            &candidates(std::slice::from_ref(&dependent)),
            1
        ),
        Err(Error::RevisionConflict)
    ));
    assert!(matches!(
        prepare(&service, "dependency", &[dependent], 1),
        ClaimDecision::NoEligibleWork
    ));
    assert!(matches!(
        prepare(&service, "empty", &[], 1),
        ClaimDecision::NoEligibleWork
    ));
    assert!(matches!(
        prepare(&service, "disabled", &[], 0),
        ClaimDecision::CapacityBlocked { capacity: 0, .. }
    ));
}

#[test]
fn pause_without_stop_ack_retains_slot_then_confirmed_stop_releases_it() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let first = create(&service, "first", BTreeSet::new());
    let waiting = create(&service, "waiting", BTreeSet::new());
    let claim = prepared(prepare(
        &service,
        "first-claim",
        std::slice::from_ref(&first),
        1,
    ));
    let receipt = service.execute(&supervisor(), &claim).unwrap();
    execute(
        &service,
        &supervisor(),
        "pause",
        vec![Operation::Pause {
            work_id: first,
            reason: "yield after stop".into(),
        }],
    );
    assert!(matches!(
        prepare(&service, "waiting-claim", std::slice::from_ref(&waiting), 1),
        ClaimDecision::CapacityBlocked { occupied: 1, .. }
    ));
    execute(
        &service,
        &authority(Principal::Runtime),
        "stop",
        vec![Operation::ConfirmStopped {
            assignment_id: receipt.ids["assignment"].clone(),
            effects_reconciled: true,
        }],
    );
    let next = prepared(prepare(&service, "waiting-claim", &[waiting], 1));
    service.execute(&supervisor(), &next).unwrap();
}

#[test]
fn bounded_host_inputs_reject_ambiguous_candidates_and_runtime_authority() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "first", BTreeSet::new());
    assert!(matches!(
        service.prepare_ready_claim(
            &authority(Principal::Runtime),
            &observed(&service),
            "runtime",
            &candidates(std::slice::from_ref(&work)),
            1
        ),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        service.prepare_ready_claim(
            &supervisor(),
            &observed(&service),
            "duplicate",
            &candidates(&[work.clone(), work]),
            1
        ),
        Err(Error::InvalidTransition(_))
    ));
    let ids: Vec<_> = (0..=CLAIM_CANDIDATE_LIMIT)
        .map(|i| format!("work-{i}"))
        .collect();
    assert!(matches!(
        service.prepare_ready_claim(
            &supervisor(),
            &observed(&service),
            "too-many",
            &candidates(&ids),
            1
        ),
        Err(Error::InvalidTransition(_))
    ));
}
