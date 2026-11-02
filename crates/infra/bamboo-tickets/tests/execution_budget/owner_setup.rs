//! Canonical configuration receipts, without a second operation store.
use super::*;

fn epoch(service: &TicketService) -> u64 {
    service.published().unwrap().1.authority_epoch
}
fn limits(work: &str, cap: u64) -> WorkTokenLimits {
    WorkTokenLimits {
        work_tokens: BTreeMap::from([(work.into(), cap)]),
        utc_day_tokens: cap,
        total_tokens: cap,
    }
}

#[test]
fn original_budget_decision_survives_later_limits_unknown_charge_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    let original_epoch = epoch(&service);
    let original = service
        .configure_work_token_budget_at_epoch(&user(), original_epoch, 0, limits(&work, 400))
        .unwrap();
    service
        .configure_work_token_budget_at_epoch(&user(), original_epoch, 1, limits(&work, 500))
        .unwrap();
    let request = input_request(&service, &work, "unknown-owner-setup");
    let receipt = reserved(
        service
            .reserve_work_tokens(&runtime(), &request, 1)
            .unwrap(),
    );
    service
        .settle_work_tokens(
            &runtime(),
            &request.operation_id,
            &request.request_hash,
            &receipt.receipt,
            ExecutionTokenUsage::Unknown,
        )
        .unwrap();
    let before = service.published().unwrap().0;
    assert_eq!(
        service
            .configure_work_token_budget_at_epoch(&user(), original_epoch, 0, limits(&work, 400))
            .unwrap(),
        original
    );
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &user(),
            original_epoch,
            0,
            limits(&work, 401)
        ),
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(service.published().unwrap().0, before);
    drop(service);
    let service = open(root.path());
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &user(),
            original_epoch,
            0,
            limits(&work, 400)
        ),
        Err(Error::RevisionConflict)
    ));
    let before = service.published().unwrap().0;
    // Fresh host authority can read the same decision identity, not write it again.
    assert_eq!(
        service
            .configure_work_token_budget_at_epoch(&user(), epoch(&service), 0, limits(&work, 400))
            .unwrap(),
        original
    );
    assert_eq!(service.published().unwrap().0, before);
    let budget = service.work_token_budget(&runtime()).unwrap().unwrap();
    assert_eq!(budget.revision, 2);
    assert_eq!(budget.configurations.len(), 2);
    assert_eq!(budget.limits, limits(&work, 500));
    assert_eq!(budget.total_charged, 100);
    let mut expected = receipt;
    expected.usage = Some(ExecutionTokenUsage::Unknown);
    assert_eq!(budget.reservations[&request.operation_id], expected);
}

#[test]
fn concurrent_identical_configuration_has_one_receipt_and_changed_limits_one_winner() {
    let root = tempfile::tempdir().unwrap();
    let service = Arc::new(open(root.path()));
    let work = create(&service);
    let original_seq = service.published().unwrap().1.seq;
    let current_epoch = epoch(&service);
    let barrier = Arc::new(Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let (service, work, barrier) = (service.clone(), work.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                service
                    .configure_work_token_budget_at_epoch(
                        &user(),
                        current_epoch,
                        0,
                        limits(&work, 400),
                    )
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let receipts: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(receipts[0], receipts[1]);
    assert_eq!(service.published().unwrap().1.seq, original_seq + 1);
    let barrier = Arc::new(Barrier::new(3));
    let workers: Vec<_> = [500, 600]
        .into_iter()
        .map(|cap| {
            let (service, work, barrier) = (service.clone(), work.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                service.configure_work_token_budget_at_epoch(
                    &user(),
                    current_epoch,
                    1,
                    limits(&work, cap),
                )
            })
        })
        .collect();
    barrier.wait();
    let outcomes: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(Error::IdempotencyConflict)))
            .count(),
        1
    );
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    assert_eq!(service.published().unwrap().1.seq, original_seq + 2);
}

#[test]
fn scope_epoch_roles_and_spent_caps_are_checked_without_changing_the_ledger() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    let current_epoch = epoch(&service);
    service
        .configure_work_token_budget_at_epoch(&user(), current_epoch, 0, limits(&work, 200))
        .unwrap();
    let request = input_request(&service, &work, "retained-unknown");
    service
        .reserve_work_tokens(&runtime(), &request, 1)
        .unwrap();
    let before = service.published().unwrap().0;
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &runtime(),
            current_epoch,
            1,
            limits(&work, 300)
        ),
        Err(Error::ScopeDenied(_))
    ));
    let mut other = binding();
    other.scope_id = "foreign-scope".into();
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &Authority::from_verified_host(
                other,
                Principal::User {
                    user_id: "budget-owner".into()
                }
            ),
            current_epoch,
            1,
            limits(&work, 300)
        ),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &user(),
            current_epoch + 1,
            1,
            limits(&work, 300)
        ),
        Err(Error::RevisionConflict)
    ));
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &user(),
            current_epoch,
            99,
            limits(&work, 300)
        ),
        Err(Error::RevisionConflict)
    ));
    assert!(service
        .configure_work_token_budget_at_epoch(&user(), current_epoch, 1, limits(&work, 50))
        .is_err());
    assert!(service
        .configure_work_token_budget_at_epoch(&user(), current_epoch, 1, limits(&work, 0))
        .is_err());
    let other_user = Authority::from_verified_host(
        binding(),
        Principal::User {
            user_id: "different-owner".into(),
        },
    );
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &other_user,
            current_epoch,
            0,
            limits(&work, 200)
        ),
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(service.published().unwrap().0, before);
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
fn lost_configuration_reply_reconciles_canonical_head_before_retry() {
    for point in [FaultPoint::BeforeHeadRename, FaultPoint::AfterHeadRename] {
        let root = tempfile::tempdir().unwrap();
        let service = open(root.path());
        let work = create(&service);
        let original_epoch = epoch(&service);
        let seq = service.published().unwrap().1.seq;
        service.set_publication_fault(Some(Arc::new(move |observed| {
            if observed == point {
                Err(std::io::Error::other("lost owner configuration ACK"))
            } else {
                Ok(())
            }
        })));
        assert!(service
            .configure_work_token_budget_at_epoch(&user(), original_epoch, 0, limits(&work, 300))
            .is_err());
        service.set_publication_fault(None);
        if point == FaultPoint::AfterHeadRename {
            assert_ne!(service.health(), Health::Writable);
            assert!(service
                .configure_work_token_budget_at_epoch(
                    &user(),
                    original_epoch,
                    0,
                    limits(&work, 300)
                )
                .is_err());
            drop(service);
            let service = open(root.path());
            let before = service.published().unwrap().0;
            let receipt = service
                .configure_work_token_budget_at_epoch(
                    &user(),
                    epoch(&service),
                    0,
                    limits(&work, 300),
                )
                .unwrap();
            assert_eq!(receipt.committed_seq, seq + 1);
            assert_eq!(service.published().unwrap().0, before);
            assert_eq!(
                service
                    .work_token_budget(&runtime())
                    .unwrap()
                    .unwrap()
                    .configurations
                    .len(),
                1
            );
        } else {
            assert!(service.work_token_budget(&runtime()).unwrap().is_none());
            let receipt = service
                .configure_work_token_budget_at_epoch(
                    &user(),
                    original_epoch,
                    0,
                    limits(&work, 300),
                )
                .unwrap();
            assert_eq!(receipt.committed_seq, seq + 1);
            assert_eq!(
                service
                    .work_token_budget(&runtime())
                    .unwrap()
                    .unwrap()
                    .configurations
                    .len(),
                1
            );
        }
    }
}

#[test]
fn full_configuration_history_keeps_old_receipts_and_rejects_new_decisions() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    let current_epoch = epoch(&service);
    let first = service
        .configure_work_token_budget_at_epoch(&user(), current_epoch, 0, limits(&work, 300))
        .unwrap();
    for revision in 1..128 {
        service
            .configure_work_token_budget_at_epoch(
                &user(),
                current_epoch,
                revision,
                limits(&work, 300 + revision),
            )
            .unwrap();
    }
    let before = service.published().unwrap().0;
    assert!(matches!(
        service.configure_work_token_budget_at_epoch(
            &user(),
            current_epoch,
            128,
            limits(&work, 500)
        ),
        Err(Error::ResourceBlocked(_))
    ));
    assert_eq!(
        service
            .configure_work_token_budget_at_epoch(&user(), current_epoch, 0, limits(&work, 300))
            .unwrap(),
        first
    );
    assert_eq!(service.published().unwrap().0, before);
    assert_eq!(
        service
            .work_token_budget(&runtime())
            .unwrap()
            .unwrap()
            .revision,
        128
    );
}
