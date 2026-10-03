use bamboo_tickets::*;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
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
fn user() -> Authority {
    authority(Principal::User {
        user_id: "human".into(),
    })
}
fn runtime() -> Authority {
    authority(Principal::Runtime)
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

#[test]
fn adapter_source_does_not_grant_worker_authority() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let initial_seq = service.published().unwrap().1.seq;
    let source = CommandSource::WorkerTask {
        arguments: serde_json::json!({"tasks":[]}),
    };
    assert!(matches!(
        service.prepare_source_command(&user(), "fake", vec![], source.clone()),
        Err(Error::ScopeDenied(_))
    ));
    let mut cmd = command(&service, "fake", vec![]);
    cmd.source = Some(source);
    assert!(matches!(
        service.execute(&supervisor(), &cmd),
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(service.published().unwrap().1.seq, initial_seq);
}
fn start(service: &TicketService, work: &str, op: &str) -> (String, Authority) {
    let id = execute(
        service,
        &supervisor(),
        op,
        vec![Operation::Start {
            work_id: work.into(),
            temp_id: "assignment".into(),
            workspace: None,
        }],
    )
    .ids["assignment"]
        .clone();
    let snapshot = service.published().unwrap().1;
    let a = &snapshot.assignments[&id];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: format!("run-{id}"),
        session_id: format!("session-{id}"),
    };
    execute(
        service,
        &runtime(),
        &format!("admit-{id}"),
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
    let worker = authority(Principal::Worker {
        assignment_id: id.clone(),
        generation: a.generation,
        run_id: receipt.run_id,
        session_id: receipt.session_id,
    });
    (id, worker)
}
fn submit(service: &TicketService, id: &str, worker: &Authority, op: &str) -> String {
    execute(
        service,
        worker,
        op,
        vec![Operation::Submit {
            assignment_id: id.into(),
            temp_id: "submission".into(),
            artifacts: vec![Artifact {
                uri: "artifact://result.txt".into(),
                sha256: content_hash(b"result"),
            }],
            evidence: vec!["verified output".into()],
        }],
    )
    .ids["submission"]
        .clone()
}

#[test]
fn managed_artifact_bytes_are_scoped_manifest_reachable_and_backup_verified() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("scope");
    let service = TicketService::open(&root, binding()).unwrap();
    let work = create(&service, "Artifact", BTreeSet::new());
    let (id, worker) = start(&service, &work, "start-artifact");
    assert!(service
        .store_artifact(&worker, b"forged host bytes")
        .is_err());
    let bytes = "完整 canonical output\nwith evidence".as_bytes();
    let artifact = service.store_artifact(&runtime(), bytes).unwrap();
    assert_eq!(artifact.sha256, content_hash(bytes));
    assert!(service.read_artifact(&user(), &artifact, 65536).is_err());
    let seq = service.published().unwrap().1.seq;
    let missing = Artifact {
        uri: format!("{}{}", store::MANAGED_ARTIFACT_PREFIX, "0".repeat(64)),
        sha256: "0".repeat(64),
    };
    assert!(service
        .execute(
            &worker,
            &command(
                &service,
                "missing-bytes",
                vec![Operation::Submit {
                    assignment_id: id.clone(),
                    temp_id: "s".into(),
                    artifacts: vec![missing],
                    evidence: vec!["declared hash is insufficient".into()],
                }]
            )
        )
        .is_err());
    assert_eq!(service.published().unwrap().1.seq, seq);
    execute(
        &service,
        &worker,
        "real-bytes",
        vec![Operation::Submit {
            assignment_id: id,
            temp_id: "s".into(),
            artifacts: vec![artifact.clone()],
            evidence: vec!["Host checkpoint".into()],
        }],
    );
    assert_eq!(
        service.read_artifact(&worker, &artifact, 65536).unwrap(),
        bytes
    );
    assert!(matches!(
        service.read_artifact(&user(), &artifact, 1),
        Err(Error::ContextBudgetExceeded)
    ));
    let sibling = create(&service, "Sibling", BTreeSet::new());
    let (_, sibling_worker) = start(&service, &sibling, "start-sibling");
    assert!(matches!(
        service.read_artifact(&sibling_worker, &artifact, 65536),
        Err(Error::ScopeDenied(_))
    ));
    let backup = dir.path().join("backup");
    let commit = service.export(&backup).unwrap();
    let copy = TicketService::open(&backup, binding()).unwrap();
    assert!(matches!(copy.health(), Health::ReadOnly { .. }));
    assert_eq!(copy.published().unwrap().0, commit);
    assert_eq!(
        copy.read_artifact(&user(), &artifact, 65536).unwrap(),
        bytes
    );
    drop(copy);
    std::fs::write(backup.join("objects").join(&artifact.sha256), b"truncated").unwrap();
    let damaged = TicketService::open(&backup, binding()).unwrap();
    assert!(matches!(damaged.health(), Health::ReadOnly { .. }));
    assert!(damaged.published().is_err());
}

#[test]
fn single_work_closed_loop_requires_exact_submission_and_user_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (id, worker) = start(&service, &work, "start-A");
    let snapshot = service.published().unwrap().1;
    assert!(snapshot.assignments[&id].plan.steps.is_empty());
    assert_eq!(snapshot.intents.len(), 1);
    service.authorize_tool(&worker, "Task").unwrap();
    assert!(service.authorize_tool(&worker, "Shell").is_err());
    let submission = submit(&service, &id, &worker, "submit-A");
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Submitted
    );
    let accept = vec![Operation::Accept {
        work_id: work.clone(),
        submission_id: submission.clone(),
        evidence: vec!["User reviewed".into()],
    }];
    assert!(service
        .execute(
            &supervisor(),
            &command(&service, "auto-accept", accept.clone())
        )
        .is_err());
    execute(&service, &user(), "accept-A", accept);
    assert_eq!(
        service.published().unwrap().1.tickets[&work].accepted_submission,
        Some(submission)
    );
    assert!(service.authorize_tool(&worker, "Task").is_err());
}

#[test]
fn cas_receipt_replay_precedes_cas_and_checks_subject_and_payload() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let request = command(
        &service,
        "op",
        vec![Operation::Create {
            temp_id: "A".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract("A"),
            depends_on: BTreeSet::new(),
        }],
    );
    let receipt = service.execute(&supervisor(), &request).unwrap();
    assert_eq!(service.execute(&supervisor(), &request).unwrap(), receipt);
    assert!(matches!(
        service.execute(&user(), &request),
        Err(Error::ScopeDenied(_))
    ));
    let mut changed = request.clone();
    changed.operations.clear();
    assert!(matches!(
        service.execute(&supervisor(), &changed),
        Err(Error::IdempotencyConflict)
    ));
    drop(service);
    let service = TicketService::open(dir.path(), binding()).unwrap();
    assert_eq!(service.execute(&supervisor(), &request).unwrap(), receipt);
    let cmd = command(
        &service,
        "race",
        vec![Operation::Archive {
            ticket_id: receipt.ids["A"].clone(),
            archived: true,
        }],
    );
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|i| {
            let service = service.clone();
            let barrier = barrier.clone();
            let mut cmd = cmd.clone();
            cmd.operation_id = format!("race-{i}");
            std::thread::spawn(move || {
                barrier.wait();
                service.execute(&supervisor(), &cmd)
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
}

#[test]
fn graph_and_atomic_group_reject_cycles_work_containment_and_foreign_scope() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let a = create(&service, "A", BTreeSet::new());
    let b = create(&service, "B", BTreeSet::from([a.clone()]));
    let before = service.published().unwrap().0;
    let cycle = command(
        &service,
        "cycle",
        vec![Operation::SetDependencies {
            work_id: a.clone(),
            depends_on: BTreeSet::from([b]),
        }],
    );
    assert!(matches!(
        service.execute(&supervisor(), &cycle),
        Err(Error::DependencyCycle)
    ));
    assert_eq!(service.published().unwrap().0, before);
    let child = command(
        &service,
        "work-work",
        vec![Operation::Create {
            temp_id: "child".into(),
            kind: TicketKind::Work,
            parent: Some(a),
            contract: contract("child"),
            depends_on: BTreeSet::new(),
        }],
    );
    assert!(service.execute(&supervisor(), &child).is_err());
    let mut other = binding();
    other.scope_id = "foreign".into();
    assert!(matches!(
        service.execute(
            &Authority::from_verified_host(other, Principal::Runtime),
            &child
        ),
        Err(Error::ScopeDenied(_))
    ));
}

#[test]
fn five_plans_and_five_questions_are_independent_and_answered_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let mut rows = Vec::new();
    for name in ["A", "B", "C", "D", "E"] {
        let work = create(&service, name, BTreeSet::new());
        let (id, worker) = start(&service, &work, &format!("start-{name}"));
        execute(
            &service,
            &worker,
            &format!("plan-{name}"),
            vec![Operation::ReplacePlan {
                assignment_id: id.clone(),
                expected_plan_revision: 0,
                steps: vec![LocalStep {
                    id: name.into(),
                    parent: None,
                    title: format!("private {name}"),
                    completed: false,
                    status: None,
                }],
            }],
        );
        let request = execute(
            &service,
            &worker,
            &format!("ask-{name}"),
            vec![Operation::Ask {
                work_id: work.clone(),
                temp_id: "question".into(),
                prompt: format!("Question {name}"),
                action: None,
            }],
        )
        .ids["question"]
            .clone();
        rows.push((work, id, worker, request));
    }
    let attempt = command(
        &service,
        "write-sibling",
        vec![Operation::ReplacePlan {
            assignment_id: rows[1].1.clone(),
            expected_plan_revision: 1,
            steps: vec![],
        }],
    );
    assert!(matches!(
        service.execute(&rows[0].2, &attempt),
        Err(Error::ScopeDenied(_))
    ));
    for index in [4, 1, 3, 0, 2] {
        execute(
            &service,
            &user(),
            &format!("answer-{index}"),
            vec![Operation::Answer {
                request_id: rows[index].3.clone(),
                prompt_revision: 1,
                answer: format!("answer-{index}"),
            }],
        );
        let snapshot = service.published().unwrap().1;
        assert_eq!(
            snapshot
                .requests
                .values()
                .filter(|r| r.status == RequestStatus::Open)
                .count(),
            5 - snapshot
                .requests
                .values()
                .filter(|r| r.status == RequestStatus::Answered)
                .count()
        );
    }
    let snapshot = service.published().unwrap().1;
    for (index, (_, id, _, request)) in rows.iter().enumerate() {
        assert_eq!(
            snapshot.assignments[id].plan.steps[0].title,
            format!("private {}", ["A", "B", "C", "D", "E"][index])
        );
        assert_eq!(
            snapshot.requests[request].answer,
            Some(format!("answer-{index}"))
        );
    }
}

#[test]
fn approval_is_exact_question_answer_cannot_authorize_and_consumption_is_bound() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (id, worker) = start(&service, &work, "start");
    let action = Action {
        kind: "payment".into(),
        target: "A".into(),
        data_hash: content_hash(b"data"),
        amount: Some("10 CNY".into()),
        permissions: BTreeSet::new(),
        risk: "test-only".into(),
    };
    let fingerprint = content_hash(&canonical_bytes(&action).unwrap());
    let req = execute(
        &service,
        &worker,
        "approval",
        vec![Operation::Ask {
            work_id: work.clone(),
            temp_id: "request".into(),
            prompt: "Approve payment A 10 CNY?".into(),
            action: Some(action),
        }],
    )
    .ids["request"]
        .clone();
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "question-as-approve",
                vec![Operation::Answer {
                    request_id: req.clone(),
                    prompt_revision: 1,
                    answer: "可以".into()
                }]
            )
        )
        .is_err());
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "wrong-fingerprint",
                vec![Operation::DecideApproval {
                    request_id: req.clone(),
                    prompt_revision: 1,
                    fingerprint: content_hash(b"20 CNY"),
                    approve: true
                }]
            )
        )
        .is_err());
    execute(
        &service,
        &user(),
        "approve",
        vec![Operation::DecideApproval {
            request_id: req.clone(),
            prompt_revision: 1,
            fingerprint: fingerprint.clone(),
            approve: true,
        }],
    );
    execute(
        &service,
        &worker,
        "consume",
        vec![Operation::ConsumeApproval {
            request_id: req.clone(),
            fingerprint: fingerprint.clone(),
            attempt_id: "attempt-A".into(),
        }],
    );
    assert!(service
        .execute(
            &worker,
            &command(
                &service,
                "consume-B",
                vec![Operation::ConsumeApproval {
                    request_id: req,
                    fingerprint: fingerprint.clone(),
                    attempt_id: "attempt-B".into()
                }]
            )
        )
        .is_err());
    execute(
        &service,
        &worker,
        "start-effect",
        vec![Operation::RecordEffect {
            assignment_id: id.clone(),
            attempt_id: "attempt-A".into(),
            effect: Effect {
                action_fingerprint: fingerprint,
                state: EffectState::Started,
                provider_receipt: None,
                artifact: None,
            },
        }],
    );
    execute(
        &service,
        &runtime(),
        "unknown",
        vec![Operation::OutcomeUnknown {
            assignment_id: id.clone(),
            reason: "cannot confirm external effect".into(),
        }],
    );
    assert!(service.authorize_tool(&worker, "Task").is_err());
    assert!(service
        .execute(
            &runtime(),
            &command(
                &service,
                "unsafe-release",
                vec![Operation::ConfirmStopped {
                    assignment_id: id,
                    effects_reconciled: true
                }]
            )
        )
        .is_err());
}

#[test]
fn late_generation_is_archived_and_never_reverses_cancel_or_current_submission() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (one, worker_one) = start(&service, &work, "start-one");
    execute(
        &service,
        &user(),
        "cancel",
        vec![Operation::Cancel {
            work_id: work.clone(),
        }],
    );
    let late = submit(&service, &one, &worker_one, "late-after-cancel");
    assert!(service.published().unwrap().1.submissions[&late].stale);
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Cancelled
    );
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "unsafe-reopen",
                vec![Operation::Reopen {
                    work_id: work.clone()
                }]
            )
        )
        .is_err());
    execute(
        &service,
        &runtime(),
        "stopped",
        vec![Operation::ConfirmStopped {
            assignment_id: one.clone(),
            effects_reconciled: true,
        }],
    );
    execute(
        &service,
        &user(),
        "reopen",
        vec![Operation::Reopen {
            work_id: work.clone(),
        }],
    );
    let (two, worker_two) = start(&service, &work, "start-two");
    let current = submit(&service, &two, &worker_two, "submit-two");
    let stale = submit(&service, &one, &worker_one, "late-one");
    let snapshot = service.published().unwrap().1;
    assert!(snapshot.submissions[&stale].stale);
    assert_eq!(snapshot.tickets[&work].current_submission, Some(current));
    assert_eq!(snapshot.tickets[&work].generation, 2);
}

#[test]
fn submitted_does_not_unlock_dependency_and_revoked_inputs_require_review() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let a = create(&service, "A", BTreeSet::new());
    let b = create(&service, "B", BTreeSet::from([a.clone()]));
    let (id, worker) = start(&service, &a, "start-A");
    let submission = submit(&service, &id, &worker, "submit-A");
    assert!(matches!(
        service.execute(
            &supervisor(),
            &command(
                &service,
                "too-early",
                vec![Operation::Start {
                    work_id: b.clone(),
                    temp_id: "b".into(),
                    workspace: None
                }]
            )
        ),
        Err(Error::ResourceBlocked(_))
    ));
    execute(
        &service,
        &user(),
        "accept-A",
        vec![Operation::Accept {
            work_id: a.clone(),
            submission_id: submission,
            evidence: vec!["reviewed".into()],
        }],
    );
    let (_, worker_b) = start(&service, &b, "start-B");
    execute(
        &service,
        &user(),
        "reopen-A",
        vec![Operation::Reopen { work_id: a }],
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&b].state,
        WorkState::Blocked
    );
    assert!(service.authorize_tool(&worker_b, "Task").is_err());
}

#[test]
fn second_writer_corruption_and_backup_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("authority");
    let service = TicketService::open(&store, binding()).unwrap();
    assert!(matches!(
        TicketService::open(&store, binding()),
        Err(Error::AuthorityUnavailable(_))
    ));
    create(&service, "A", BTreeSet::new());
    let head = service.export(dir.path().join("backup")).unwrap();
    let backup = TicketService::open(dir.path().join("backup"), binding()).unwrap();
    assert!(matches!(backup.health(), Health::ReadOnly { .. }));
    assert_eq!(backup.published().unwrap().0, head);
    assert!(backup
        .execute(&supervisor(), &command(&backup, "write-backup", vec![]))
        .is_err());
    drop(service);
    std::fs::write(store.join("HEAD"), b"../../escape").unwrap();
    let corrupt = TicketService::open(&store, binding()).unwrap();
    assert!(matches!(corrupt.health(), Health::ReadOnly { .. }));
    assert!(corrupt.published().is_err());
}

#[test]
fn after_head_rename_failure_is_readonly_until_restart_and_receipt_survives() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let request = command(
        &service,
        "published",
        vec![Operation::Create {
            temp_id: "A".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract("A"),
            depends_on: BTreeSet::new(),
        }],
    );
    let old = service.published().unwrap().0;
    service.set_publication_fault(Some(Arc::new(|point| {
        if point == FaultPoint::AfterHeadRename {
            Err(std::io::Error::other("directory flush failed"))
        } else {
            Ok(())
        }
    })));
    assert!(service.execute(&supervisor(), &request).is_err());
    assert!(matches!(service.health(), Health::ReadOnly { .. }));
    assert_eq!(service.published().unwrap().0, old);
    drop(service);
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let receipt = service.execute(&supervisor(), &request).unwrap();
    assert_eq!(service.published().unwrap().1.tickets.len(), 1);
    assert_eq!(receipt.ids.len(), 1);
}

#[cfg(unix)]
#[test]
fn injected_enospc_and_eacces_preserve_receipts_and_fence_uncertain_publication() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    // macOS and Linux errno values; no real volume fill or chmod is performed.
    for errno in [28, 13] {
        for (index, point) in [
            FaultPoint::BeforeWrite,
            FaultPoint::BeforeFileSync,
            FaultPoint::BeforeHeadRename,
            FaultPoint::AfterHeadRename,
            FaultPoint::BeforeDirectorySync,
        ]
        .into_iter()
        .enumerate()
        {
            let root = dir.path().join(format!("errno-{errno}-{index}"));
            let service = TicketService::open(&root, binding()).unwrap();
            let request = command(
                &service,
                "create-errno",
                vec![Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("Errno recovery"),
                    depends_on: BTreeSet::new(),
                }],
            );
            let old = service.published().unwrap().0;
            let renamed = Arc::new(AtomicBool::new(false));
            let renamed_at_hook = renamed.clone();
            service.set_publication_fault(Some(Arc::new(move |observed| {
                if observed == FaultPoint::AfterHeadRename {
                    renamed_at_hook.store(true, Ordering::SeqCst);
                }
                // Exercise the HEAD's directory flush, not an earlier object's.
                if observed == point
                    && (point != FaultPoint::BeforeDirectorySync
                        || renamed_at_hook.load(Ordering::SeqCst))
                {
                    Err(std::io::Error::from_raw_os_error(errno))
                } else {
                    Ok(())
                }
            })));
            let failure = service.execute(&supervisor(), &request).unwrap_err();
            assert!(matches!(failure, Error::Io(ref error) if error.raw_os_error() == Some(errno)));
            assert_eq!(service.published().unwrap().0, old);
            let receipt = if renamed.load(Ordering::SeqCst) {
                assert!(matches!(service.health(), Health::ReadOnly { .. }));
                assert!(service.execute(&supervisor(), &request).is_err());
                drop(service);
                let recovered = TicketService::open(&root, binding()).unwrap();
                let persisted = recovered.published().unwrap().1.receipts["create-errno"].clone();
                assert_eq!(
                    recovered.execute(&supervisor(), &request).unwrap(),
                    persisted
                );
                persisted
            } else {
                assert_eq!(service.health(), Health::Writable);
                assert!(service.published().unwrap().1.receipts.is_empty());
                service.set_publication_fault(None);
                let receipt = service.execute(&supervisor(), &request).unwrap();
                drop(service);
                receipt
            };
            let recovered = TicketService::open(&root, binding()).unwrap();
            assert_eq!(recovered.execute(&supervisor(), &request).unwrap(), receipt);
            let snapshot = recovered.published().unwrap().1;
            assert_eq!(snapshot.tickets.len(), 1);
            assert_eq!(snapshot.receipts.len(), 1);
        }
    }
}

#[test]
fn every_publication_failure_boundary_recovers_a_complete_old_or_new_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("measure");
    let service = TicketService::open(&root, binding()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let trace = count.clone();
    service.set_publication_fault(Some(Arc::new(move |_| {
        trace.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })));
    create(&service, "A", BTreeSet::new());
    let boundaries = count.load(Ordering::SeqCst);
    assert!(boundaries > 30);
    for fail_at in 0..boundaries {
        let root = dir.path().join(format!("fault-{fail_at}"));
        let service = TicketService::open(&root, binding()).unwrap();
        let request = command(
            &service,
            "create-A",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("A"),
                    depends_on: BTreeSet::new(),
                },
                Operation::Ready {
                    work_id: "work".into(),
                },
            ],
        );
        let counter = Arc::new(AtomicUsize::new(0));
        let counter2 = counter.clone();
        service.set_publication_fault(Some(Arc::new(move |_| {
            if counter2.fetch_add(1, Ordering::SeqCst) == fail_at {
                Err(std::io::Error::other(
                    "injected disk/permission/flush failure",
                ))
            } else {
                Ok(())
            }
        })));
        assert!(
            service.execute(&supervisor(), &request).is_err(),
            "boundary {fail_at}"
        );
        drop(service);
        let service = TicketService::open(&root, binding()).unwrap();
        assert_eq!(service.health(), Health::Writable, "boundary {fail_at}");
        let snapshot = service.published().unwrap().1;
        assert!(snapshot.tickets.len() <= 1);
        assert_eq!(snapshot.receipts.len(), snapshot.tickets.len());
        if !snapshot.tickets.is_empty() {
            assert_eq!(
                snapshot.tickets.values().next().unwrap().state,
                WorkState::Ready
            );
            service.execute(&supervisor(), &request).unwrap();
        }
    }
}

#[test]
fn publication_process_child() {
    let Ok(root) = std::env::var("BAMBOO_TICKET_CRASH_ROOT") else {
        return;
    };
    if std::env::var("BAMBOO_TICKET_CHILD_MODE").as_deref() == Ok("lock") {
        assert!(TicketService::open(&root, binding()).is_err());
        return;
    }
    let fail_at: usize = std::env::var("BAMBOO_TICKET_CRASH_BOUNDARY")
        .unwrap()
        .parse()
        .unwrap();
    let service = TicketService::open(&root, binding()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    service.set_publication_fault(Some(Arc::new(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) == fail_at {
            std::process::exit(71);
        }
        Ok(())
    })));
    create(&service, "A", BTreeSet::new());
    panic!("requested crash boundary was not reached");
}

#[test]
fn actual_process_exit_at_every_publication_boundary_and_os_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("measure");
    let service = TicketService::open(&root, binding()).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "publication_process_child", "--nocapture"])
        .env("BAMBOO_TICKET_CRASH_ROOT", &root)
        .env("BAMBOO_TICKET_CHILD_MODE", "lock")
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "second process did not reject writer: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let count = Arc::new(AtomicUsize::new(0));
    let trace = count.clone();
    service.set_publication_fault(Some(Arc::new(move |_| {
        trace.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })));
    create(&service, "A", BTreeSet::new());
    let boundaries = count.load(Ordering::SeqCst);
    for boundary in 0..boundaries {
        let root = dir.path().join(format!("crash-{boundary}"));
        drop(TicketService::open(&root, binding()).unwrap());
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "publication_process_child", "--nocapture"])
            .env("BAMBOO_TICKET_CRASH_ROOT", &root)
            .env("BAMBOO_TICKET_CRASH_BOUNDARY", boundary.to_string())
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(71),
            "boundary {boundary}: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let service = TicketService::open(&root, binding()).unwrap();
        assert_eq!(service.health(), Health::Writable, "boundary {boundary}");
        let snapshot = service.published().unwrap().1;
        assert!(snapshot.tickets.len() <= 1);
        assert_eq!(
            snapshot.receipts.len(),
            snapshot.tickets.len(),
            "boundary {boundary}"
        );
        if !snapshot.tickets.is_empty() {
            assert_eq!(
                snapshot.tickets.values().next().unwrap().state,
                WorkState::Ready
            );
        }
    }
    eprintln!("verified {boundaries} real process-exit publication boundaries on local filesystem");
}

#[test]
fn resources_remain_claimed_after_submission_until_runtime_confirms_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path().join("store"), binding()).unwrap();
    let worktree = dir.path().join("worktree");
    std::fs::create_dir(&worktree).unwrap();
    let canonical = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let workspace = ExecutionWorkspace {
        repo: "fixture-repo".into(),
        base_commit: "a".repeat(40),
        branch: "fixture".into(),
        worktree: canonical.clone(),
        write_roots: vec![canonical.clone()],
        claims: BTreeSet::from([format!("worktree:{canonical}")]),
    };
    let a = create(&service, "A", BTreeSet::new());
    let b = create(&service, "B", BTreeSet::new());
    let first = execute(
        &service,
        &supervisor(),
        "claim",
        vec![Operation::Start {
            work_id: a,
            temp_id: "assignment".into(),
            workspace: Some(workspace.clone()),
        }],
    )
    .ids["assignment"]
        .clone();
    assert!(matches!(
        service.execute(
            &supervisor(),
            &command(
                &service,
                "contend",
                vec![Operation::Start {
                    work_id: b.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(workspace.clone())
                }]
            )
        ),
        Err(Error::ResourceBlocked(_))
    ));
    execute(
        &service,
        &user(),
        "cancel",
        vec![Operation::Cancel {
            work_id: service.published().unwrap().1.assignments[&first]
                .work_id
                .clone(),
        }],
    );
    execute(
        &service,
        &runtime(),
        "confirm-stopped",
        vec![Operation::ConfirmStopped {
            assignment_id: first,
            effects_reconciled: true,
        }],
    );
    execute(
        &service,
        &supervisor(),
        "claim-again",
        vec![Operation::Start {
            work_id: b,
            temp_id: "assignment".into(),
            workspace: Some(workspace),
        }],
    );
}

#[test]
fn stopped_attempt_cannot_execute_tools_but_retains_successful_submission_state() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (id, worker) = start(&service, &work, "start-A");
    let receipt = service.published().unwrap().1.assignments[&id]
        .runtime
        .clone()
        .unwrap();
    let stop = command(
        &service,
        "stop-A",
        vec![Operation::RuntimeStopped {
            assignment_id: id.clone(),
            receipt: receipt.clone(),
            completed: true,
        }],
    );
    assert!(matches!(
        service.execute(&worker, &stop),
        Err(Error::ScopeDenied(_))
    ));
    let mut wrong = receipt.clone();
    wrong.run_id = "forged-run".into();
    assert!(matches!(
        service.execute(
            &runtime(),
            &command(
                &service,
                "wrong-stop",
                vec![Operation::RuntimeStopped {
                    assignment_id: id.clone(),
                    receipt: wrong,
                    completed: true
                }]
            )
        ),
        Err(Error::ScopeDenied(_))
    ));
    service.execute(&runtime(), &stop).unwrap();
    assert!(service.authorize_tool(&worker, "Task").is_err());
    let result = submit(&service, &id, &worker, "submit-stopped");
    let snapshot = service.published().unwrap().1;
    assert!(!snapshot.submissions[&result].stale);
    assert_eq!(snapshot.tickets[&work].state, WorkState::Submitted);
    execute(
        &service,
        &runtime(),
        "confirm-stopped",
        vec![Operation::ConfirmStopped {
            assignment_id: id.clone(),
            effects_reconciled: true,
        }],
    );
    assert_eq!(
        service.published().unwrap().1.assignments[&id].state,
        AssignmentState::Submitted
    );
}

#[test]
fn completion_reconciliation_requires_stopped_exact_current_attempt() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (id, _) = start(&service, &work, "start-A");
    let receipt = service.published().unwrap().1.assignments[&id]
        .runtime
        .clone()
        .unwrap();
    let op = Operation::ReconcileCompleted {
        assignment_id: id.clone(),
        receipt: receipt.clone(),
    };
    assert!(service
        .execute(&runtime(), &command(&service, "no-stop", vec![op.clone()]))
        .is_err());
    execute(
        &service,
        &runtime(),
        "stop",
        vec![Operation::RuntimeStopped {
            assignment_id: id.clone(),
            receipt: receipt.clone(),
            completed: true,
        }],
    );
    let epoch = service.published().unwrap().1.authority_epoch;
    drop(service);
    let service = TicketService::open(root.path(), binding()).unwrap();
    assert!(service.published().unwrap().1.authority_epoch > epoch);
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Blocked
    );
    execute(&service, &runtime(), "reconcile", vec![op.clone()]);
    let snapshot = service.published().unwrap().1;
    assert!(snapshot.assignments[&id].process_stopped);
    assert_eq!(
        snapshot.assignments[&id].authority_epoch,
        snapshot.authority_epoch
    );
    execute(
        &service,
        &supervisor(),
        "cancel",
        vec![Operation::Cancel {
            work_id: work.clone(),
        }],
    );
    assert!(service
        .execute(
            &runtime(),
            &command(&service, "cancelled-reconcile", vec![op])
        )
        .is_err());
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Cancelled
    );
}

#[test]
fn stopped_process_does_not_release_an_unknown_external_effect() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "A", BTreeSet::new());
    let (id, worker) = start(&service, &work, "start-A");
    let action = Action {
        kind: "payment".into(),
        target: "A".into(),
        data_hash: content_hash(b"data"),
        amount: Some("10 CNY".into()),
        permissions: BTreeSet::new(),
        risk: "fixture".into(),
    };
    let fingerprint = content_hash(&canonical_bytes(&action).unwrap());
    let request = execute(
        &service,
        &worker,
        "ask",
        vec![Operation::Ask {
            work_id: work,
            temp_id: "request".into(),
            prompt: "Approve exact action?".into(),
            action: Some(action),
        }],
    )
    .ids["request"]
        .clone();
    execute(
        &service,
        &user(),
        "approve",
        vec![Operation::DecideApproval {
            request_id: request.clone(),
            prompt_revision: 1,
            fingerprint: fingerprint.clone(),
            approve: true,
        }],
    );
    execute(
        &service,
        &worker,
        "consume",
        vec![Operation::ConsumeApproval {
            request_id: request,
            fingerprint: fingerprint.clone(),
            attempt_id: "attempt".into(),
        }],
    );
    execute(
        &service,
        &worker,
        "effect-started",
        vec![Operation::RecordEffect {
            assignment_id: id.clone(),
            attempt_id: "attempt".into(),
            effect: Effect {
                action_fingerprint: fingerprint,
                state: EffectState::Started,
                provider_receipt: None,
                artifact: None,
            },
        }],
    );
    let receipt = service.published().unwrap().1.assignments[&id]
        .runtime
        .clone()
        .unwrap();
    execute(
        &service,
        &runtime(),
        "stopped",
        vec![Operation::RuntimeStopped {
            assignment_id: id.clone(),
            receipt: receipt.clone(),
            completed: true,
        }],
    );
    let snapshot = service.published().unwrap().1;
    assert!(snapshot.assignments[&id].process_stopped);
    assert_eq!(
        snapshot.assignments[&id].state,
        AssignmentState::OutcomeUnknown
    );
    assert!(matches!(
        service.execute(
            &user(),
            &command(
                &service,
                "unsafe-retry",
                vec![Operation::Reopen {
                    work_id: snapshot.assignments[&id].work_id.clone(),
                }]
            )
        ),
        Err(Error::ResourceBlocked(_))
    ));
    assert!(service
        .execute(
            &runtime(),
            &command(
                &service,
                "release",
                vec![Operation::ConfirmStopped {
                    assignment_id: id.clone(),
                    effects_reconciled: true,
                }]
            )
        )
        .is_err());
    assert!(service
        .execute(
            &runtime(),
            &command(
                &service,
                "reconcile",
                vec![Operation::ReconcileCompleted {
                    assignment_id: id,
                    receipt,
                }]
            )
        )
        .is_err());
}

#[test]
fn explicit_import_is_idempotent_and_legacy_completed_never_becomes_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let source = ImportSource {
        session_id: "old-session".into(),
        task_id: "old-task".into(),
        snapshot_hash: content_hash(b"old snapshot"),
        original_state: "completed".into(),
        artifact: None,
    };
    let op = Operation::Import {
        temp_id: "import".into(),
        contract: contract("old task"),
        source,
    };
    let first = execute(&service, &user(), "import-one", vec![op.clone()]);
    let second = execute(&service, &user(), "import-two", vec![op]);
    assert_eq!(first.ids, second.ids);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.tickets.len(), 1);
    assert_eq!(
        snapshot.tickets[&first.ids["import"]].state,
        WorkState::Blocked
    );
    assert!(snapshot.tickets[&first.ids["import"]]
        .accepted_submission
        .is_none());
}

#[test]
fn goal_requires_its_own_acceptance_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let service = TicketService::open(dir.path(), binding()).unwrap();
    let goal = execute(
        &service,
        &supervisor(),
        "goal",
        vec![Operation::Create {
            temp_id: "goal".into(),
            kind: TicketKind::Goal,
            parent: None,
            contract: contract("Goal"),
            depends_on: BTreeSet::new(),
        }],
    )
    .ids["goal"]
        .clone();
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "no-goal-evidence",
                vec![Operation::AcceptGoal {
                    goal_id: goal.clone(),
                    evidence: vec![]
                }]
            )
        )
        .is_err());
    assert_eq!(
        service.published().unwrap().1.tickets[&goal].state,
        WorkState::Draft
    );
    execute(
        &service,
        &user(),
        "goal-reviewed",
        vec![Operation::AcceptGoal {
            goal_id: goal.clone(),
            evidence: vec!["Separate Goal criteria reviewed".into()],
        }],
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&goal].state,
        WorkState::Accepted
    );
    let parent = execute(
        &service,
        &user(),
        "parent-goal",
        vec![
            Operation::Create {
                temp_id: "parent".into(),
                kind: TicketKind::Goal,
                parent: None,
                contract: contract("Parent"),
                depends_on: BTreeSet::new(),
            },
            Operation::Create {
                temp_id: "child".into(),
                kind: TicketKind::Goal,
                parent: Some("parent".into()),
                contract: contract("Child"),
                depends_on: BTreeSet::new(),
            },
        ],
    );
    execute(
        &service,
        &user(),
        "accept-child-goal",
        vec![Operation::AcceptGoal {
            goal_id: parent.ids["child"].clone(),
            evidence: vec!["Child reviewed".into()],
        }],
    );
    execute(
        &service,
        &user(),
        "accept-parent-goal",
        vec![Operation::AcceptGoal {
            goal_id: parent.ids["parent"].clone(),
            evidence: vec!["Parent reviewed independently".into()],
        }],
    );
    execute(
        &service,
        &user(),
        "reopen-child-goal",
        vec![Operation::Reopen {
            work_id: parent.ids["child"].clone(),
        }],
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&parent.ids["parent"]].state,
        WorkState::Blocked
    );
}

#[test]
fn pause_stays_blocked_after_owned_stop_and_resumes_only_explicitly() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "pause", BTreeSet::new());
    let (old, worker) = start(&service, &work, "start-pause");
    let receipt = service.published().unwrap().1.assignments[&old]
        .runtime
        .clone()
        .unwrap();
    execute(
        &service,
        &user(),
        "pause",
        vec![Operation::Pause {
            work_id: work.clone(),
            reason: "User postponed this work".into(),
        }],
    );
    assert!(service.authorize_tool(&worker, "Task").is_err());
    assert!(matches!(
        service.execute(
            &user(),
            &command(
                &service,
                "too-early",
                vec![Operation::Reopen {
                    work_id: work.clone()
                }]
            )
        ),
        Err(Error::ResourceBlocked(_))
    ));
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "too-early-ready",
                vec![Operation::Ready {
                    work_id: work.clone()
                }]
            )
        )
        .is_err());
    execute(
        &service,
        &runtime(),
        "owned-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: old.clone(),
            receipt,
            completed: false,
        }],
    );
    let state = service.published().unwrap().1;
    assert!(state.tickets[&work].paused);
    assert_eq!(state.tickets[&work].state, WorkState::Blocked);
    assert!(state.tickets[&work].active_assignment.is_none());
    drop(service);
    let service = TicketService::open(root.path(), binding()).unwrap();
    assert!(service.published().unwrap().1.tickets[&work].paused);
    execute(
        &service,
        &user(),
        "resume",
        vec![Operation::Ready {
            work_id: work.clone(),
        }],
    );
    let next = execute(
        &service,
        &user(),
        "fresh-start",
        vec![Operation::Start {
            work_id: work.clone(),
            temp_id: "next".into(),
            workspace: None,
        }],
    )
    .ids["next"]
        .clone();
    let late = submit(&service, &old, &worker, "late-old");
    let state = service.published().unwrap().1;
    assert_eq!(state.assignments[&next].generation, 2);
    assert!(state.submissions[&late].stale);
    assert_eq!(
        state.tickets[&work].active_assignment.as_deref(),
        Some(next.as_str())
    );
    assert!(state.tickets[&work].current_submission.is_none());
}

#[test]
fn explicit_retry_after_owned_failed_stop_creates_fresh_generation() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "retry", BTreeSet::new());
    let (old, _) = start(&service, &work, "start-retry");
    let receipt = service.published().unwrap().1.assignments[&old]
        .runtime
        .clone()
        .unwrap();
    execute(
        &service,
        &runtime(),
        "failed-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: old,
            receipt,
            completed: false,
        }],
    );
    execute(
        &service,
        &user(),
        "explicit-retry",
        vec![Operation::Reopen {
            work_id: work.clone(),
        }],
    );
    let (next, _) = start(&service, &work, "next-retry");
    assert_eq!(
        service.published().unwrap().1.assignments[&next].generation,
        2
    );
}

#[test]
fn question_answer_before_owned_stop_stays_blocked_and_fresh_context_has_only_own_answer() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "question-yield", BTreeSet::new());
    let (old, worker) = start(&service, &work, "question-start");
    let packet = service.child_context_packet(&worker, &old, 65536).unwrap();
    let ops = vec![
        Operation::Ask {
            work_id: work.clone(),
            temp_id: "question".into(),
            prompt: "Own question".into(),
            action: None,
        },
        Operation::YieldForInput {
            assignment_id: old.clone(),
            request_id: "question".into(),
            packet_hash: content_hash(&canonical_bytes(&packet).unwrap()),
        },
    ];
    let held = command(&service, "question-yield", ops);
    let receipt = service.execute(&worker, &held).unwrap();
    let question = receipt.ids["question"].clone();
    assert_eq!(service.execute(&worker, &held).unwrap(), receipt);
    assert!(service.authorize_tool(&worker, "Task").is_err());
    assert!(service.execute(&user(), &held).is_err());
    execute(
        &service,
        &user(),
        "answer-first",
        vec![Operation::Answer {
            request_id: question.clone(),
            prompt_revision: 1,
            answer: "Own exact answer".into(),
        }],
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Blocked
    );
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "premature-reopen",
                vec![Operation::Reopen {
                    work_id: work.clone()
                }]
            )
        )
        .is_err());
    let runtime_receipt = service.published().unwrap().1.assignments[&old]
        .runtime
        .clone()
        .unwrap();
    execute(
        &service,
        &runtime(),
        "question-owned-stop",
        vec![Operation::RuntimeStopped {
            assignment_id: old,
            receipt: runtime_receipt,
            completed: true,
        }],
    );
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Ready
    );
    let (next, next_worker) = start(&service, &work, "question-fresh-start");
    let packet = service
        .child_context_packet(&next_worker, &next, 65536)
        .unwrap();
    assert_eq!(packet.answers.len(), 1);
    assert_eq!(packet.answers[0].request_id, question);
    assert_eq!(packet.answers[0].generation, 1);
    assert_eq!(packet.answers[0].answer, "Own exact answer");
    assert_eq!(packet.generation, 2);
    assert!(service.published().unwrap().1.assignments[&next]
        .plan
        .steps
        .is_empty());
}

#[test]
fn restart_quarantines_unstopped_question_and_keeps_request_for_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let service = TicketService::open(root.path(), binding()).unwrap();
    let work = create(&service, "unstopped-question", BTreeSet::new());
    let (assignment, worker) = start(&service, &work, "unstopped-start");
    let packet = service
        .child_context_packet(&worker, &assignment, 65536)
        .unwrap();
    let receipt = execute(
        &service,
        &worker,
        "unstopped-question",
        vec![
            Operation::Ask {
                work_id: work.clone(),
                temp_id: "q".into(),
                prompt: "Own question".into(),
                action: None,
            },
            Operation::YieldForInput {
                assignment_id: assignment.clone(),
                request_id: "q".into(),
                packet_hash: content_hash(&canonical_bytes(&packet).unwrap()),
            },
        ],
    );
    drop(service);
    let service = TicketService::open(root.path(), binding()).unwrap();
    let state = service.published().unwrap().1;
    assert_eq!(
        state.assignments[&assignment].state,
        AssignmentState::OutcomeUnknown
    );
    assert_eq!(
        state.requests[&receipt.ids["q"]].status,
        RequestStatus::Open
    );
    assert!(service
        .execute(
            &user(),
            &command(
                &service,
                "unknown-reopen",
                vec![Operation::Reopen { work_id: work }]
            )
        )
        .is_err());
}
