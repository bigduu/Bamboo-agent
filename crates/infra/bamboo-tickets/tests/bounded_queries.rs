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

#[test]
fn inspect_sections_declare_omissions_and_legacy_inspect_keeps_all_sections() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "sections");
    start(&service, &work, "start-sections");
    execute(
        &service,
        "question",
        vec![Operation::Ask {
            work_id: work.clone(),
            temp_id: "request".into(),
            prompt: "A precise question".into(),
            action: None,
        }],
    );
    let read = service
        .work_inspect_sections(
            &supervisor(),
            std::slice::from_ref(&work),
            &InspectOptions {
                sections: BTreeSet::from([InspectSection::Requests]),
                depth: 0,
                budget_bytes: 65536,
                fixed_commit: None,
            },
        )
        .unwrap();
    assert_eq!(read.data[0].requests.len(), 1);
    assert!(read.data[0].assignments.is_empty());
    assert_eq!(
        read.data[0].sections,
        BTreeSet::from([InspectSection::Requests])
    );
    let all = service
        .work_inspect(&supervisor(), &[work], 0, 65536, None)
        .unwrap();
    assert_eq!(all.data[0].assignments.len(), 1);
    assert_eq!(all.data[0].sections, all_inspect_sections());
}

#[test]
fn accepted_dependency_context_has_complete_verified_bytes_or_refuses_dispatch_packet() {
    for bytes in [
        "完整 accepted result\nwith exact quotes: \"x\"".as_bytes(),
        &[0xff, 0x00][..],
    ] {
        let root = tempfile::tempdir().unwrap();
        let service = TicketService::open(root.path(), binding()).unwrap();
        let upstream = create(&service, "upstream");
        let (attempt, worker) = start(&service, &upstream, "start-upstream");
        let runtime = Authority::from_verified_host(binding(), Principal::Runtime);
        let artifact = service.store_artifact(&runtime, bytes).unwrap();
        let submitted = service
            .prepare_command(
                &worker,
                "submit-input",
                vec![Operation::Submit {
                    assignment_id: attempt,
                    temp_id: "submission".into(),
                    artifacts: vec![artifact.clone()],
                    evidence: vec!["verified".into()],
                }],
            )
            .unwrap();
        let submission = service.execute(&worker, &submitted).unwrap().ids["submission"].clone();
        let saved = service.published().unwrap().1.submissions[&submission].clone();
        service
            .execute(
                &runtime,
                &service
                    .prepare_command(
                        &runtime,
                        "input-owned-stop",
                        vec![Operation::RuntimeStopped {
                            assignment_id: saved.assignment_id,
                            receipt: saved.runtime,
                            completed: true,
                        }],
                    )
                    .unwrap(),
            )
            .unwrap();
        let user = Authority::from_verified_host(
            binding(),
            Principal::User {
                user_id: "human".into(),
            },
        );
        service
            .execute(
                &user,
                &service
                    .prepare_command(
                        &user,
                        "accept-input",
                        vec![Operation::Accept {
                            work_id: upstream.clone(),
                            submission_id: submission.clone(),
                            evidence: vec!["User reviewed input".into()],
                        }],
                    )
                    .unwrap(),
            )
            .unwrap();
        let downstream = create(&service, "downstream");
        execute(
            &service,
            "dependency",
            vec![Operation::SetDependencies {
                work_id: downstream.clone(),
                depends_on: BTreeSet::from([upstream.clone()]),
            }],
        );
        let (assignment, worker) = start(&service, &downstream, "start-downstream");
        match service.child_context_packet(&worker, &assignment, 65536) {
            Ok(packet) => {
                assert!(std::str::from_utf8(bytes).is_ok());
                assert_eq!(packet.input_artifacts.len(), 1);
                let input = &packet.input_artifacts[0];
                assert_eq!(input.utf8.as_bytes(), bytes);
                assert_eq!(input.source.work_id, upstream);
                assert_eq!(input.source.submission_id, submission);
                assert_eq!(input.source.contract_revision, 1);
                assert_eq!(input.artifact, artifact);
                let budget = canonical_bytes(&packet).unwrap().len() - 1;
                assert!(matches!(
                    service.child_context_packet(&worker, &assignment, budget),
                    Err(Error::ContextBudgetExceeded)
                ));
                let original_contract = service.published().unwrap().1.tickets[&downstream]
                    .contract
                    .clone();
                execute(
                    &service,
                    "revoke-upstream",
                    vec![Operation::Reopen { work_id: upstream }],
                );
                assert!(service
                    .child_context_packet(&worker, &assignment, 65536)
                    .is_err());
                assert_eq!(
                    service.published().unwrap().1.tickets[&downstream].contract,
                    original_contract
                );
            }
            Err(Error::AuthorityUnavailable(_)) => assert!(std::str::from_utf8(bytes).is_err()),
            other => panic!("unexpected input resolution: {other:?}"),
        }
    }
}

#[test]
fn external_artifact_without_trusted_resolver_cannot_become_worker_context() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let upstream = create(&service, "external");
    let (attempt, worker) = start(&service, &upstream, "external-start");
    let request = service
        .prepare_command(
            &worker,
            "external-submit",
            vec![Operation::Submit {
                assignment_id: attempt,
                temp_id: "s".into(),
                artifacts: vec![Artifact {
                    uri: "artifact://external.txt".into(),
                    sha256: content_hash(b"external"),
                }],
                evidence: vec!["external fixture".into()],
            }],
        )
        .unwrap();
    let submission = service.execute(&worker, &request).unwrap().ids["s"].clone();
    let user = Authority::from_verified_host(
        binding(),
        Principal::User {
            user_id: "human".into(),
        },
    );
    service
        .execute(
            &user,
            &service
                .prepare_command(
                    &user,
                    "external-accept",
                    vec![Operation::Accept {
                        work_id: upstream.clone(),
                        submission_id: submission,
                        evidence: vec!["fixture acceptance".into()],
                    }],
                )
                .unwrap(),
        )
        .unwrap();
    let downstream = create(&service, "consumer");
    execute(
        &service,
        "external-dep",
        vec![Operation::SetDependencies {
            work_id: downstream.clone(),
            depends_on: BTreeSet::from([upstream]),
        }],
    );
    let (attempt, worker) = start(&service, &downstream, "consumer-start");
    assert!(matches!(
        service.child_context_packet(&worker, &attempt, 65536),
        Err(Error::AuthorityUnavailable(_))
    ));
}
