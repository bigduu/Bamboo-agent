use bamboo_tickets::*;
use std::collections::BTreeSet;

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "fixture".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}
fn supervisor() -> Authority {
    Authority::from_verified_host(
        binding(),
        Principal::Supervisor {
            session_id: "supervisor".into(),
        },
    )
}
fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: format!("Deliver {title}"),
        constraints: vec!["Do not expand scope".into()],
        acceptance: vec!["Explicit evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::new(),
    }
}
fn execute(service: &TicketService, id: &str, ops: Vec<Operation>) -> OperationReceipt {
    let state = service.published().unwrap().1;
    service
        .execute(
            &supervisor(),
            &Command {
                operation_id: id.into(),
                binding: binding(),
                expected_seq: state.seq,
                expected_epoch: state.authority_epoch,
                source: None,
                operations: ops,
            },
        )
        .unwrap()
}
fn create(service: &TicketService, title: &str) -> String {
    execute(
        service,
        &format!("create-{title}"),
        vec![
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract(title),
                depends_on: BTreeSet::new(),
            },
            Operation::Ready {
                work_id: "work".into(),
            },
        ],
    )
    .ids["work"]
        .clone()
}
fn start(service: &TicketService, work: &str, op: &str) -> (String, Authority) {
    let id = execute(
        service,
        op,
        vec![Operation::Start {
            work_id: work.into(),
            temp_id: "assignment".into(),
            workspace: None,
        }],
    )
    .ids["assignment"]
        .clone();
    let state = service.published().unwrap().1;
    let a = &state.assignments[&id];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: state.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: format!("run-{id}"),
        session_id: format!("session-{id}"),
    };
    let command = Command {
        operation_id: format!("admit-{id}"),
        binding: binding(),
        expected_seq: state.seq,
        expected_epoch: state.authority_epoch,
        source: None,
        operations: vec![Operation::Admitted {
            assignment_id: id.clone(),
            receipt: receipt.clone(),
        }],
    };
    service
        .execute(
            &Authority::from_verified_host(binding(), Principal::Runtime),
            &command,
        )
        .unwrap();
    (
        id.clone(),
        Authority::from_verified_host(
            binding(),
            Principal::Worker {
                assignment_id: id,
                generation: 1,
                run_id: receipt.run_id,
                session_id: receipt.session_id,
            },
        ),
    )
}

#[test]
fn stable_pagination_retains_old_snapshot_during_concurrent_publication() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let expected: BTreeSet<_> = ["A", "B", "C", "D", "E"]
        .iter()
        .map(|n| create(&service, n))
        .collect();
    let first = service
        .work_search(&supervisor(), &SearchFilter::default(), 2, None, None)
        .unwrap();
    assert_eq!(first.omitted_count, 3);
    assert!(first.truncated);
    assert_eq!(first.index_seq, first.snapshot.seq);
    create(&service, "new-after-page-one");
    let mut rows = first.data;
    let mut cursor = first.next_cursor;
    while let Some(current) = cursor {
        let page = service
            .work_search(
                &supervisor(),
                &SearchFilter::default(),
                2,
                Some(&current),
                None,
            )
            .unwrap();
        assert_eq!(page.snapshot.commit, first.snapshot.commit);
        rows.extend(page.data);
        cursor = page.next_cursor;
    }
    assert_eq!(
        rows.iter().map(|r| r.id.clone()).collect::<BTreeSet<_>>(),
        expected
    );
    assert_eq!(
        service
            .work_overview(&supervisor())
            .unwrap()
            .data
            .work_count,
        6
    );
}

#[test]
fn cursor_query_mismatch_and_missing_commit_require_explicit_resync() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    create(&service, "A");
    create(&service, "B");
    let cursor = service
        .work_search(&supervisor(), &SearchFilter::default(), 1, None, None)
        .unwrap()
        .next_cursor
        .unwrap();
    let filter = SearchFilter {
        query: "A".into(),
        ..SearchFilter::default()
    };
    assert!(matches!(
        service.work_search(&supervisor(), &filter, 1, Some(&cursor), None),
        Err(Error::ResyncRequired)
    ));
    let missing = ReadCursor {
        commit: "0".repeat(64),
        ..cursor
    };
    assert!(matches!(
        service.work_search(
            &supervisor(),
            &SearchFilter::default(),
            1,
            Some(&missing),
            None
        ),
        Err(Error::ResyncRequired)
    ));
    assert!(service
        .work_search(&supervisor(), &filter, 101, None, None)
        .is_err());
}

#[test]
fn inspect_context_budget_never_silently_truncates_constraints_or_authority() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "A");
    let (id, _) = start(&service, &work, "start");
    assert!(matches!(
        service.work_inspect(&supervisor(), std::slice::from_ref(&work), 0, 128, None),
        Err(Error::ContextBudgetExceeded)
    ));
    assert!(service
        .work_inspect(&supervisor(), std::slice::from_ref(&work), 5, 65536, None)
        .is_err());
    assert!(matches!(
        service.child_context_packet(&supervisor(), &id, 32),
        Err(Error::ContextBudgetExceeded)
    ));
    let packet = service
        .child_context_packet(&supervisor(), &id, 65536)
        .unwrap();
    assert_eq!(packet.contract.constraints, vec!["Do not expand scope"]);
    let serialized = String::from_utf8(canonical_bytes(&packet).unwrap()).unwrap();
    assert!(!serialized.contains("task_list"));
    assert!(!serialized.contains("history"));
}

#[test]
fn worker_reads_do_not_expose_sibling_private_plan_or_questions() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let a = create(&service, "A");
    let b = create(&service, "B");
    let (id, worker) = start(&service, &a, "start-A");
    let (other, _) = start(&service, &b, "start-B");
    assert_eq!(
        service.work_overview(&worker).unwrap().coverage,
        "capability_refs"
    );
    assert_eq!(service.work_overview(&worker).unwrap().data.work_count, 1);
    assert!(matches!(
        service.work_inspect(&worker, &[b], 0, 65536, None),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        service.child_context_packet(&worker, &other, 65536),
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        service
            .child_context_packet(&worker, &id, 65536)
            .unwrap()
            .contract_ref,
        a
    );
}

#[test]
fn changes_cursor_has_fixed_high_watermark_and_retains_intermediate_revisions() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "A");
    let start_seq = service.published().unwrap().1.seq;
    execute(
        &service,
        "archive",
        vec![Operation::Archive {
            ticket_id: work.clone(),
            archived: true,
        }],
    );
    execute(
        &service,
        "unarchive",
        vec![Operation::Archive {
            ticket_id: work.clone(),
            archived: false,
        }],
    );
    let first = service
        .work_changes(&supervisor(), start_seq, 1, None)
        .unwrap();
    assert_eq!(first.data[0].id, work);
    assert_eq!(first.omitted_count, 1);
    let cursor = first.next_cursor.unwrap();
    create(&service, "B");
    let second = service
        .work_changes(&supervisor(), start_seq, 1, Some(&cursor))
        .unwrap();
    assert_eq!(second.snapshot.seq, first.snapshot.seq);
    assert!(second.data[0].seq > first.data[0].seq);
    assert_eq!(second.data[0].id, work);
    assert!(!second.truncated);
    assert!(matches!(
        service.work_changes(&supervisor(), u64::MAX, 1, None),
        Err(Error::ResyncRequired)
    ));
}

#[test]
fn measure_full_manifest_write_amplification_at_bounded_fixture_sizes() {
    for count in [5, 50, 200] {
        let dir = tempfile::tempdir().unwrap();
        let service = TicketService::open(dir.path(), binding()).unwrap();
        let mut first_id = None;
        for base in (0..count).step_by(50) {
            let operations = (base..(base + 50).min(count))
                .map(|i| Operation::Create {
                    temp_id: format!("work-{i}"),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(&format!("Work {i}")),
                    depends_on: BTreeSet::new(),
                })
                .collect();
            let receipt = execute(&service, &format!("seed-{base}"), operations);
            if first_id.is_none() {
                first_id = receipt.ids.values().next().cloned();
            }
        }
        let started = std::time::Instant::now();
        execute(
            &service,
            "edit-one",
            vec![Operation::Archive {
                ticket_id: first_id.unwrap(),
                archived: true,
            }],
        );
        let elapsed = started.elapsed().as_millis();
        let head = service.published().unwrap().0;
        let commit: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("commits").join(head)).unwrap())
                .unwrap();
        let size = std::fs::metadata(
            dir.path()
                .join("manifests")
                .join(commit["manifest"].as_str().unwrap()),
        )
        .unwrap()
        .len();
        eprintln!(
            "full_manifest_fixture tickets={count} manifest_bytes={size} edit_one_ms={elapsed}"
        );
        assert_eq!(
            service
                .work_overview(&supervisor())
                .unwrap()
                .data
                .ticket_count,
            count
        );
    }
}
