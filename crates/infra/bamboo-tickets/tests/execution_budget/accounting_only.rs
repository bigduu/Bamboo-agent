//! The existing canonical ledger can account without imposing Worker quotas.
use super::*;

fn configure_accounting(service: &TicketService, revision: u64) -> WorkTokenConfiguration {
    service
        .configure_work_token_accounting_at_epoch(
            &user(),
            service.published().unwrap().1.authority_epoch,
            revision,
            None,
        )
        .unwrap()
}

#[test]
fn explicit_null_and_legacy_finite_json_round_trip_but_missing_limits_are_invalid() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    configure(&service, &work, 100, 200, 300);
    let finite = service.work_token_budget(&runtime()).unwrap().unwrap();
    let legacy = serde_json::to_value(&finite).unwrap();
    assert_eq!(legacy["limits"]["total_tokens"], 300);
    assert_eq!(
        legacy["configurations"]["1"]["limits"]["work_tokens"][&work],
        100
    );
    assert_eq!(
        serde_json::from_value::<WorkTokenBudget>(legacy).unwrap(),
        finite
    );

    let decision = configure_accounting(&service, 1);
    let budget = service.work_token_budget(&runtime()).unwrap().unwrap();
    let encoded = serde_json::to_value(&budget).unwrap();
    assert!(encoded["limits"].is_null());
    assert!(encoded["configurations"]["2"]["limits"].is_null());
    assert_eq!(
        serde_json::from_value::<WorkTokenBudget>(encoded.clone()).unwrap(),
        budget
    );
    let mut missing_budget = encoded.clone();
    missing_budget.as_object_mut().unwrap().remove("limits");
    assert!(serde_json::from_value::<WorkTokenBudget>(missing_budget).is_err());
    let mut missing_history = encoded;
    missing_history["configurations"]["2"]
        .as_object_mut()
        .unwrap()
        .remove("limits");
    assert!(serde_json::from_value::<WorkTokenBudget>(missing_history).is_err());
    let mut missing_decision = serde_json::to_value(decision).unwrap();
    missing_decision.as_object_mut().unwrap().remove("limits");
    assert!(serde_json::from_value::<WorkTokenConfiguration>(missing_decision).is_err());
}

#[test]
fn accounting_mode_changes_preserve_unknown_charge_and_original_owner_decisions() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    configure(&service, &work, 100, 100, 100);
    let first = input_request(&service, &work, "finite-unknown");
    let receipt = reserved(service.reserve_work_tokens(&runtime(), &first, 1).unwrap());
    service
        .settle_work_tokens(
            &runtime(),
            &first.operation_id,
            &first.request_hash,
            &receipt.receipt,
            ExecutionTokenUsage::Unknown,
        )
        .unwrap();
    let current_epoch = service.published().unwrap().1.authority_epoch;
    let before = service.published().unwrap().0;
    assert!(matches!(
        service.configure_work_token_accounting_at_epoch(&runtime(), current_epoch, 1, None),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        service.configure_work_token_accounting_at_epoch(&user(), current_epoch + 1, 1, None),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(service.published().unwrap().0, before);
    let decision = configure_accounting(&service, 1);
    assert!(decision.limits.is_none());
    let second = input_request(&service, &work, "semantic-without-quota");
    reserved(service.reserve_work_tokens(&runtime(), &second, 2).unwrap());
    let charged = service.work_token_budget(&runtime()).unwrap().unwrap();
    assert_eq!(charged.total_charged, 200);
    assert_eq!(
        charged.reservations[&first.operation_id].usage,
        Some(ExecutionTokenUsage::Unknown)
    );
    assert_eq!(
        charged.configurations[&1]
            .limits
            .as_ref()
            .unwrap()
            .total_tokens,
        100
    );
    let before = service.published().unwrap().0;
    let too_small = WorkTokenLimits {
        work_tokens: BTreeMap::from([(work.clone(), 100)]),
        utc_day_tokens: 100,
        total_tokens: 100,
    };
    assert!(service
        .configure_work_token_budget(&user(), 2, too_small)
        .is_err());
    assert_eq!(service.published().unwrap().0, before);
    drop(service);
    let service = open(root.path());
    assert_eq!(
        service.work_token_budget(&runtime()).unwrap().unwrap(),
        charged
    );
    let before = service.published().unwrap().0;
    assert_eq!(configure_accounting(&service, 1), decision);
    assert_eq!(service.published().unwrap().0, before);
    service
        .configure_work_token_budget(
            &user(),
            2,
            WorkTokenLimits {
                work_tokens: BTreeMap::from([(work, 500)]),
                utc_day_tokens: 500,
                total_tokens: 500,
            },
        )
        .unwrap();
    let finite = service.work_token_budget(&runtime()).unwrap().unwrap();
    assert_eq!(finite.total_charged, 200);
    assert_eq!(finite.reservations, charged.reservations);
    assert_eq!(finite.limits.unwrap().total_tokens, 500);
}

#[test]
fn accounting_without_quotas_still_validates_original_work_project_identity_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let service = open(root.path());
    let work = create(&service);
    configure_accounting(&service, 0);
    let request = input_request(&service, &work, "semantic-project-identity");
    reserved(
        service
            .reserve_work_tokens(&runtime(), &request, 1)
            .unwrap(),
    );
    drop(service);
    // Controlled persisted negative fixture, never an installed or user scope.
    let mut store = store::FileStore::open(root.path(), binding()).unwrap();
    let mut snapshot = store.published.as_ref().unwrap().1.clone();
    let record = snapshot
        .execution_budget
        .as_mut()
        .unwrap()
        .reservations
        .get_mut(&request.operation_id)
        .unwrap();
    record.request.project_id = "another-project".parse().unwrap();
    record.receipt = content_hash(&canonical_bytes(&(&snapshot.binding, &record.request)).unwrap());
    snapshot.seq += 1;
    store.publish(snapshot).unwrap();
    drop(store);
    let service = open(root.path());
    let Health::ReadOnly { reason } = service.health() else {
        panic!("a rehashed reservation cannot change immutable Project membership");
    };
    assert!(
        reason.contains("Work token receipt lost its Work/Project identity"),
        "{reason}"
    );
}
