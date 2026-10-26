use bamboo_tickets::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "budget-scope".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}
fn runtime() -> Authority {
    Authority::from_verified_host(binding(), Principal::Runtime)
}
fn user() -> Authority {
    Authority::from_verified_host(
        binding(),
        Principal::User {
            user_id: "budget-owner".into(),
        },
    )
}
fn open(path: &std::path::Path) -> TicketService {
    TicketService::open(path, binding()).unwrap()
}
fn create(service: &TicketService) -> String {
    let command = service
        .prepare_command(
            &user(),
            "create-budget-work",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: Contract {
                        title: "Budgeted Work".into(),
                        objective: "Produce an attributable result".into(),
                        constraints: vec![],
                        acceptance: vec!["Explicit acceptance".into()],
                        user_acceptance_required: true,
                        allowed_tools: BTreeSet::from(["Task".into()]),
                    },
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: "budget-project".parse().unwrap(),
                },
                Operation::Ready {
                    work_id: "work".into(),
                },
            ],
        )
        .unwrap();
    service.execute(&user(), &command).unwrap().ids["work"].clone()
}
fn configure(service: &TicketService, work: &str, work_cap: u64, day: u64, total: u64) {
    service
        .configure_work_token_budget(
            &user(),
            0,
            WorkTokenLimits {
                work_tokens: BTreeMap::from([(work.into(), work_cap)]),
                utc_day_tokens: day,
                total_tokens: total,
            },
        )
        .unwrap();
}
fn input_request(service: &TicketService, work: &str, id: &str) -> WorkTokenRequest {
    WorkTokenRequest {
        operation_id: id.into(),
        project_id: "budget-project".parse().unwrap(),
        work_id: work.into(),
        purpose: ExecutionBudgetPurpose::SupervisorDecision,
        request_hash: content_hash(id.as_bytes()),
        observed_seq: service.published().unwrap().1.seq,
        input_cap: 60,
        output_cap: 40,
        attempt: None,
    }
}
fn reserved(admission: WorkTokenAdmission) -> WorkTokenReservation {
    match admission {
        WorkTokenAdmission::Reserved(record) => record,
        other => panic!("expected reservation: {other:?}"),
    }
}

#[test]
fn no_defaults_and_only_an_explicit_user_can_configure_limits() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    assert!(service
        .reserve_work_tokens(
            &runtime(),
            &input_request(&service, &work, "unconfigured"),
            1
        )
        .is_err());
    assert!(service
        .configure_work_token_budget(
            &runtime(),
            0,
            WorkTokenLimits {
                work_tokens: BTreeMap::from([(work.clone(), 100)]),
                utc_day_tokens: 100,
                total_tokens: 100,
            }
        )
        .is_err());
    assert!(service.work_token_budget(&runtime()).unwrap().is_none());
    configure(&service, &work, 100, 100, 100);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.schema, 8);
    let budget = service.work_token_budget(&runtime()).unwrap().unwrap();
    assert_eq!(budget.total_charged, 0);
    assert_eq!(budget.configurations[&1].user_id, "budget-owner");
}

#[test]
fn exact_receipt_reopens_without_free_retry_and_changed_payload_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let request = input_request(&service, &work, "original");
    let original = reserved(
        service
            .reserve_work_tokens(&runtime(), &request, 1)
            .unwrap(),
    );
    service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &original.receipt,
            ExecutionTokenUsage::Unknown,
        )
        .unwrap();
    drop(service);
    let service = open(dir.path());
    let recovered = reserved(
        service
            .reserve_work_tokens(&runtime(), &request, 2)
            .unwrap(),
    );
    assert_eq!(recovered.receipt, original.receipt);
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        100
    );
    let mut changed = request.clone();
    changed.output_cap += 1;
    assert!(matches!(
        service.reserve_work_tokens(&runtime(), &changed, 3),
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(
        service
            .reserve_work_tokens(&runtime(), &input_request(&service, &work, "next"), 3)
            .unwrap(),
        WorkTokenAdmission::Blocked(WorkTokenBlock::Work)
    );
}

#[test]
fn known_usage_refunds_once_and_over_cap_usage_keeps_the_original_charge() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let request = input_request(&service, &work, "measure");
    let record = reserved(
        service
            .reserve_work_tokens(&runtime(), &request, 1)
            .unwrap(),
    );
    assert!(service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &record.receipt,
            ExecutionTokenUsage::Known {
                input_tokens: 61,
                output_tokens: 0
            }
        )
        .is_err());
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        100
    );
    let measured = ExecutionTokenUsage::Known {
        input_tokens: 10,
        output_tokens: 5,
    };
    service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &record.receipt,
            ExecutionTokenUsage::Unknown,
        )
        .unwrap();
    service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &record.receipt,
            measured,
        )
        .unwrap();
    let seq = service.published().unwrap().1.seq;
    service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &record.receipt,
            measured,
        )
        .unwrap();
    assert_eq!(service.published().unwrap().1.seq, seq);
    assert!(service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &record.receipt,
            ExecutionTokenUsage::Unknown
        )
        .is_err());
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        15
    );
}

#[test]
fn utc_rollover_does_not_reset_work_total_or_refund_the_new_days_usage() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 200, 100, 200);
    let old = input_request(&service, &work, "old-day");
    let receipt = reserved(service.reserve_work_tokens(&runtime(), &old, 1).unwrap());
    let new = input_request(&service, &work, "new-day");
    service
        .reserve_work_tokens(&runtime(), &new, 86_400_001)
        .unwrap();
    service
        .settle_work_tokens(
            &runtime(),
            &old.operation_id,
            &old.request_hash,
            &receipt.receipt,
            ExecutionTokenUsage::Known {
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .unwrap();
    let budget = service.work_token_budget(&runtime()).unwrap().unwrap();
    assert_eq!(budget.daily_charged, 100);
    assert_eq!(budget.total_charged, 100);
    assert_eq!(budget.work_charged[&work], 100);
    let next = input_request(&service, &work, "third");
    assert!(service
        .reserve_work_tokens(&runtime(), &next, 86_400_000)
        .is_err());
    assert_eq!(
        service
            .reserve_work_tokens(&runtime(), &next, 86_400_002)
            .unwrap(),
        WorkTokenAdmission::Blocked(WorkTokenBlock::Daily)
    );
}

#[test]
fn each_limit_and_the_current_attempt_fence_blocks_without_a_charge() {
    for (work_cap, day_cap, total_cap, block) in [
        (99, 200, 200, WorkTokenBlock::Work),
        (200, 99, 200, WorkTokenBlock::Daily),
        (200, 200, 99, WorkTokenBlock::Total),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let service = open(dir.path());
        let work = create(&service);
        configure(&service, &work, work_cap, day_cap, total_cap);
        let input = input_request(&service, &work, "blocked");
        let before = service.published().unwrap().0;
        assert_eq!(
            service.reserve_work_tokens(&runtime(), &input, 1).unwrap(),
            WorkTokenAdmission::Blocked(block)
        );
        assert_eq!(service.published().unwrap().0, before);
    }
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let mut forged = input_request(&service, &work, "forged-attempt");
    forged.purpose = ExecutionBudgetPurpose::Patrol;
    forged.attempt = Some(WorkTokenAttempt {
        assignment_id: "unrelated".into(),
        generation: 1,
        authority_epoch: 1,
    });
    assert!(service.reserve_work_tokens(&runtime(), &forged, 1).is_err());
    let mut foreign = input_request(&service, &work, "foreign");
    foreign.project_id = "other-project".parse().unwrap();
    assert!(service
        .reserve_work_tokens(&runtime(), &foreign, 1)
        .is_err());
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        0
    );
}

#[test]
fn concurrent_same_operation_consumes_one_persistent_charge() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let input = input_request(&service, &work, "same-operation");
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let service = service.clone();
            let input = input.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                reserved(service.reserve_work_tokens(&runtime(), &input, 1).unwrap())
            })
        })
        .collect();
    let records: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(records[0], records[1]);
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        100
    );
}

#[test]
fn head_swap_without_ack_reopens_the_original_charge() {
    let dir = tempfile::tempdir().unwrap();
    let service = open(dir.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let input = input_request(&service, &work, "lost-ack");
    service.set_publication_fault(Some(Arc::new(|point| {
        if point == FaultPoint::AfterHeadRename {
            Err(std::io::Error::other("lost ACK"))
        } else {
            Ok(())
        }
    })));
    assert!(service.reserve_work_tokens(&runtime(), &input, 1).is_err());
    assert_ne!(service.health(), Health::Writable);
    drop(service);
    let service = open(dir.path());
    let record = reserved(service.reserve_work_tokens(&runtime(), &input, 2).unwrap());
    assert_eq!(record.request, input);
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .total_charged,
        100
    );
}
