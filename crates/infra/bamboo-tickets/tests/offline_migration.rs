use bamboo_tickets::*;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "scope".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}
fn user(id: &str) -> Authority {
    Authority::from_verified_host(binding(), Principal::User { user_id: id.into() })
}
fn contract() -> Contract {
    Contract {
        title: "legacy work".into(),
        objective: "reviewed result".into(),
        constraints: vec![],
        acceptance: vec!["explicit review".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}
fn execute(
    service: &TicketService,
    authority: &Authority,
    id: &str,
    ops: Vec<Operation>,
) -> OperationReceipt {
    let command = service.prepare_command(authority, id, ops).unwrap();
    service.execute(authority, &command).unwrap()
}
fn seed(root: &Path) {
    let service = TicketService::open(root, binding()).unwrap();
    let runtime = Authority::from_verified_host(binding(), Principal::Runtime);
    let bytes=canonical_bytes(&serde_json::json!({"id":"old-session","task_list":{"items":[{"id":"old-task","status":"completed"}]}})).unwrap();
    let artifact = service.store_artifact(&runtime, &bytes).unwrap();
    let receipt = execute(
        &service,
        &user("owner"),
        "import",
        vec![Operation::Import {
            temp_id: "work".into(),
            contract: contract(),
            source: ImportSource {
                session_id: "old-session".into(),
                task_id: "old-task".into(),
                snapshot_hash: artifact.sha256.clone(),
                original_state: "completed".into(),
                artifact: Some(artifact),
            },
        }],
    );
    let action = Action {
        kind: "payment".into(),
        target: "synthetic recipient".into(),
        data_hash: "a".repeat(64),
        amount: Some("100 CNY".into()),
        permissions: BTreeSet::new(),
        risk: "synthetic; never executed".into(),
    };
    let q = execute(
        &service,
        &user("owner"),
        "ask",
        vec![Operation::Ask {
            work_id: receipt.ids["work"].clone(),
            temp_id: "approval".into(),
            prompt: "approve exact synthetic action".into(),
            action: Some(action),
        }],
    );
    let request = &service.published().unwrap().1.requests[&q.ids["approval"]];
    let RequestKind::Approval { fingerprint, .. } = &request.kind else {
        panic!()
    };
    execute(
        &service,
        &user("owner"),
        "approve",
        vec![Operation::DecideApproval {
            request_id: request.id.clone(),
            prompt_revision: request.prompt_revision,
            fingerprint: fingerprint.clone(),
            approve: true,
        }],
    );
}
fn request(source: &TicketService, root: &Path, destination: &Path) -> MigrationRequest {
    let (commit, s) = source.published().unwrap();
    MigrationRequest {
        operation_id: "migrate".into(),
        binding: s.binding,
        expected_commit: commit,
        expected_seq: s.seq,
        expected_epoch: s.authority_epoch,
        source_root: root.canonicalize().unwrap().to_string_lossy().into_owned(),
        destination_root: canonical_migration_destination(destination).unwrap(),
        supervisor_snapshot_hash: None,
    }
}
fn stopped() -> StoppedScopeProof {
    StoppedScopeProof::from_verified_host(binding())
}
fn prepare(dir: &Path) -> (PathBuf, PathBuf, TicketService, MigrationRequest) {
    let root = dir.join("original");
    let dest = dir.join("copy");
    seed(&root);
    let source = TicketService::open_offline(&root, binding()).unwrap();
    let request = request(&source, &root, &dest);
    (root, dest, source, request)
}

#[test]
fn exact_transfer_full_snapshot_old_authority_retired_new_epoch_and_approval_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (root, dest, source, request) = prepare(dir.path());
    let before = source.published().unwrap();
    assert_eq!(
        before.1.requests.values().next().unwrap().status,
        RequestStatus::Approved
    );
    let retired = source
        .retire_for_migration(&user("owner"), &request, &stopped())
        .unwrap();
    assert_eq!(
        source.published().unwrap().1.migration.unwrap().stage,
        MigrationStage::SourceRetired
    );
    assert!(matches!(source.health(), Health::ReadOnly { .. }));
    assert_eq!(
        source
            .retire_for_migration(&user("owner"), &request, &stopped())
            .unwrap(),
        retired
    );
    assert!(TicketService::open_offline(&root, binding()).is_err()); // actual OS writer lock
    let copied = source
        .export_retired_migration(&user("owner"), &request, &stopped())
        .unwrap();
    assert_eq!(
        source
            .export_retired_migration(&user("owner"), &request, &stopped())
            .unwrap(),
        copied
    );
    let destination = TicketService::open_offline(&dest, binding()).unwrap();
    assert_eq!(destination.published().unwrap().0, copied);
    let receipt = destination
        .activate_migrated_copy(&source, &user("owner"), &request, &stopped())
        .unwrap();
    assert_eq!(receipt.stage, MigrationStage::DestinationActivated);
    let snapshot = destination.published().unwrap().1;
    assert!(snapshot.authority_epoch > before.1.authority_epoch);
    assert!(snapshot
        .requests
        .values()
        .all(|q| q.status == RequestStatus::Expired));
    assert!(snapshot
        .tickets
        .values()
        .all(|w| w.accepted_submission.is_none() && w.state == WorkState::Blocked));
    let artifact = snapshot
        .tickets
        .values()
        .next()
        .unwrap()
        .import_source
        .as_ref()
        .unwrap()
        .artifact
        .as_ref()
        .unwrap();
    assert_eq!(
        content_hash(
            &destination
                .read_artifact(&user("owner"), artifact, 1024 * 1024)
                .unwrap()
        ),
        artifact.sha256
    );
    assert_eq!(
        destination
            .activate_migrated_copy(&source, &user("owner"), &request, &stopped())
            .unwrap(),
        receipt
    );
    assert!(!dest.join("BACKUP_READ_ONLY").exists());
    assert_eq!(
        source.published().unwrap().1.migration.unwrap().stage,
        MigrationStage::SourceTransferred
    );
    assert_eq!(
        source
            .retire_for_migration(&user("owner"), &request, &stopped())
            .unwrap(),
        retired
    );
    drop(destination);
    drop(source);
    let original = TicketService::open(&root, binding()).unwrap();
    assert!(matches!(original.health(), Health::ReadOnly { .. }));
    let reopened = TicketService::open(&dest, binding()).unwrap();
    assert_eq!(reopened.health(), Health::Writable);
    assert!(reopened.published().unwrap().1.authority_epoch > snapshot.authority_epoch);
    let mut altered = request.clone();
    altered.destination_root = dir.path().join("other").to_string_lossy().into_owned();
    assert!(matches!(
        original.retire_for_migration(&user("owner"), &altered, &stopped()),
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        original.retire_for_migration(&user("intruder"), &request, &stopped()),
        Err(Error::ScopeDenied(_))
    ));
    let commit: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("commits").join(original.published().unwrap().0)).unwrap(),
    )
    .unwrap();
    assert_ne!(commit["schema"], 1); // exact older store schema-1 gate fails closed
}

#[test]
fn ordinary_copy_corruption_relocation_and_wrong_binding_cannot_activate() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("original");
    seed(&root);
    let active = TicketService::open_offline(&root, binding()).unwrap();
    let copy = dir.path().join("backup");
    active.export(&copy).unwrap();
    let backup = TicketService::open_offline(&copy, binding()).unwrap();
    let r = request(&backup, &copy, &dir.path().join("next"));
    assert!(backup
        .retire_for_migration(&user("owner"), &r, &stopped())
        .is_err());
    drop(backup);
    drop(active);
    let source = TicketService::open_offline(&root, binding()).unwrap();
    let dest = dir.path().join("destination");
    let r = request(&source, &root, &dest);
    let wrong = StoppedScopeProof::from_verified_host(ScopeBinding {
        binding_revision: 2,
        ..binding()
    });
    assert!(matches!(
        source.retire_for_migration(&user("owner"), &r, &wrong),
        Err(Error::ScopeDenied(_))
    ));
    source
        .retire_for_migration(&user("owner"), &r, &stopped())
        .unwrap();
    source
        .export_retired_migration(&user("owner"), &r, &stopped())
        .unwrap();
    let snap = source.published().unwrap().1;
    let hash = &snap
        .tickets
        .values()
        .next()
        .unwrap()
        .import_source
        .as_ref()
        .unwrap()
        .snapshot_hash;
    fs::write(
        dest.join("objects").join(hash),
        b"truncated source snapshot",
    )
    .unwrap();
    assert!(TicketService::open_offline(&dest, binding()).is_err());
    assert!(source
        .export_retired_migration(&user("owner"), &r, &stopped())
        .is_err());
    assert!(dest.join("BACKUP_READ_ONLY").exists());
}

#[test]
fn unconfirmed_execution_and_unknown_effects_reject_transfer_before_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("original");
    let service = TicketService::open(&root, binding()).unwrap();
    let ids = execute(
        &service,
        &user("owner"),
        "work",
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract(),
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
    )
    .ids;
    drop(service);
    let source = TicketService::open_offline(&root, binding()).unwrap();
    let r = request(&source, &root, &dir.path().join("copy"));
    let before = source.published().unwrap().0;
    assert!(matches!(
        source.retire_for_migration(&user("owner"), &r, &stopped()),
        Err(Error::ResourceBlocked(_))
    ));
    assert_eq!(source.published().unwrap().0, before);
    assert!(!dir.path().join("copy").exists());
    drop(source);
    // Test-only deterministic ledger injection: a stop flag alone never grants
    // a transfer while an external effect's outcome remains unknown.
    let mut store = bamboo_tickets::store::FileStore::open(&root, binding()).unwrap();
    let mut s = store.published.clone().unwrap().1;
    s.seq += 1;
    let a = s.assignments.get_mut(&ids["assignment"]).unwrap();
    a.process_stopped = true;
    a.effects.insert(
        "unknown".into(),
        Effect {
            action_fingerprint: "f".into(),
            state: EffectState::OutcomeUnknown,
            provider_receipt: None,
            artifact: None,
        },
    );
    store.publish(s).unwrap();
    drop(store);
    let source = TicketService::open_offline(&root, binding()).unwrap();
    let r = request(&source, &root, &dir.path().join("copy"));
    assert!(matches!(
        source.retire_for_migration(&user("owner"), &r, &stopped()),
        Err(Error::ResourceBlocked(_))
    ));
}

#[test]
fn all_activation_publication_and_release_failures_resume_same_epoch_and_receipt() {
    let dir = tempfile::tempdir().unwrap();
    for fault_source in [false, true] {
        let measure = dir.path().join(format!("measure-{fault_source}"));
        fs::create_dir(&measure).unwrap();
        let (_, dest, source, r) = prepare(&measure);
        source
            .retire_for_migration(&user("owner"), &r, &stopped())
            .unwrap();
        source
            .export_retired_migration(&user("owner"), &r, &stopped())
            .unwrap();
        let target = TicketService::open_offline(&dest, binding()).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let trace = count.clone();
        let fault = Arc::new(move |_| {
            trace.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        (if fault_source { &source } else { &target }).set_publication_fault(Some(fault));
        target
            .activate_migrated_copy(&source, &user("owner"), &r, &stopped())
            .unwrap();
        let boundaries = count.load(Ordering::SeqCst);
        drop(target);
        drop(source);
        for boundary in 0..boundaries {
            let case = dir.path().join(format!("case-{fault_source}-{boundary}"));
            fs::create_dir(&case).unwrap();
            let (root, dest, source, r) = prepare(&case);
            source
                .retire_for_migration(&user("owner"), &r, &stopped())
                .unwrap();
            source
                .export_retired_migration(&user("owner"), &r, &stopped())
                .unwrap();
            let target = TicketService::open_offline(&dest, binding()).unwrap();
            let trace = Arc::new(AtomicUsize::new(0));
            (if fault_source { &source } else { &target }).set_publication_fault(Some(Arc::new(
                move |_| {
                    if trace.fetch_add(1, Ordering::SeqCst) == boundary {
                        Err(std::io::Error::other("injected migration failure"))
                    } else {
                        Ok(())
                    }
                },
            )));
            assert!(target
                .activate_migrated_copy(&source, &user("owner"), &r, &stopped())
                .is_err());
            assert!(matches!(source.health(), Health::ReadOnly { .. }));
            drop(target);
            drop(source);
            let source = TicketService::open_offline(&root, binding()).unwrap();
            let target = TicketService::open_offline(&dest, binding()).unwrap();
            let receipt = target
                .activate_migrated_copy(&source, &user("owner"), &r, &stopped())
                .unwrap();
            assert_eq!(
                target.published().unwrap().1.authority_epoch,
                r.expected_epoch + 1
            );
            assert_eq!(
                target
                    .activate_migrated_copy(&source, &user("owner"), &r, &stopped())
                    .unwrap(),
                receipt
            );
            assert_eq!(
                source.published().unwrap().1.migration.unwrap().stage,
                MigrationStage::SourceTransferred
            );
        }
        eprintln!("verified {boundaries} actual migration persistence/release I/O failures (source={fault_source})");
    }
}

#[test]
fn every_retirement_and_export_io_window_resumes_exact_frozen_request() {
    let dir = tempfile::tempdir().unwrap();
    for exporting in [false, true] {
        let measure = dir.path().join(format!("measure-copy-{exporting}"));
        fs::create_dir(&measure).unwrap();
        let (_, _, source, request) = prepare(&measure);
        if exporting {
            source
                .retire_for_migration(&user("owner"), &request, &stopped())
                .unwrap();
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let trace = counter.clone();
        source.set_publication_fault(Some(Arc::new(move |_| {
            trace.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })));
        if exporting {
            source
                .export_retired_migration(&user("owner"), &request, &stopped())
                .unwrap();
        } else {
            source
                .retire_for_migration(&user("owner"), &request, &stopped())
                .unwrap();
        }
        let boundaries = counter.load(Ordering::SeqCst);
        drop(source);
        for boundary in 0..boundaries {
            let case = dir.path().join(format!("copy-{exporting}-{boundary}"));
            fs::create_dir(&case).unwrap();
            let (root, dest, source, request) = prepare(&case);
            if exporting {
                source
                    .retire_for_migration(&user("owner"), &request, &stopped())
                    .unwrap();
            }
            let counter = Arc::new(AtomicUsize::new(0));
            source.set_publication_fault(Some(Arc::new(move |_| {
                if counter.fetch_add(1, Ordering::SeqCst) == boundary {
                    Err(std::io::Error::other("injected offline copy failure"))
                } else {
                    Ok(())
                }
            })));
            if exporting {
                assert!(source
                    .export_retired_migration(&user("owner"), &request, &stopped())
                    .is_err());
            } else {
                assert!(source
                    .retire_for_migration(&user("owner"), &request, &stopped())
                    .is_err());
            }
            drop(source);
            let recovered = TicketService::open_offline(&root, binding()).unwrap();
            let receipt = recovered
                .retire_for_migration(&user("owner"), &request, &stopped())
                .unwrap();
            assert_eq!(receipt.committed_seq, request.expected_seq + 1);
            recovered
                .export_retired_migration(&user("owner"), &request, &stopped())
                .unwrap();
            let copy = TicketService::open_offline(&dest, binding()).unwrap();
            assert!(matches!(copy.health(), Health::ReadOnly { .. }));
            assert_eq!(
                copy.published().unwrap().0,
                recovered.published().unwrap().0
            );
            copy.activate_migrated_copy(&recovered, &user("owner"), &request, &stopped())
                .unwrap();
            assert_eq!(
                recovered
                    .retire_for_migration(&user("owner"), &request, &stopped())
                    .unwrap(),
                receipt
            );
            assert_eq!(
                copy.published().unwrap().1.authority_epoch,
                request.expected_epoch + 1
            );
        }
        eprintln!(
            "verified {boundaries} offline {} I/O windows",
            if exporting { "export" } else { "retirement" }
        );
    }
}
