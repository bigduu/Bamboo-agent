use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_tickets::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
};

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "scope".into(),
        supervisor_session_id: "supervisor".into(),
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
fn project() -> ProjectId {
    ProjectId::parse("project-a").unwrap()
}
fn sprint_id(id: &str) -> SprintId {
    SprintId::parse(id).unwrap()
}
fn definition(name: &str) -> SprintDefinition {
    SprintDefinition {
        name: name.into(),
        goal: "deliver verified Work".into(),
        starts_at: Some(100),
        ends_at: Some(200),
    }
}
fn contract(name: &str) -> Contract {
    Contract {
        title: name.into(),
        objective: "deliver".into(),
        constraints: vec![],
        acceptance: vec!["review evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}
fn command(service: &TicketService, id: &str, action: SprintAction) -> SprintCommand {
    let s = service.published().unwrap().1;
    SprintCommand {
        operation_id: format!("sprint-{}", s.seq),
        binding: s.binding,
        expected_seq: s.seq,
        expected_epoch: s.authority_epoch,
        project_id: project(),
        sprint_id: sprint_id(id),
        source: "explicit planning request".into(),
        action,
    }
}
fn revision(service: &TicketService, id: &str) -> u64 {
    service.published().unwrap().1.sprint.sprints[&sprint_id(id)].revision
}
fn close_command(
    service: &TicketService,
    id: &str,
    dispositions: BTreeMap<String, SprintDestination>,
) -> SprintCommand {
    let targets = dispositions
        .values()
        .filter_map(|d| match d {
            SprintDestination::Backlog => None,
            SprintDestination::Sprint { sprint_id } => Some((
                sprint_id.clone(),
                service.published().unwrap().1.sprint.sprints[sprint_id].revision,
            )),
        })
        .collect();
    command(
        service,
        id,
        SprintAction::Close {
            expected_revision: revision(service, id),
            dispositions,
            target_revisions: targets,
        },
    )
}
fn ticket_execute(
    service: &TicketService,
    authority: &Authority,
    ops: Vec<Operation>,
) -> OperationReceipt {
    let seq = service.published().unwrap().1.seq;
    let c = service
        .prepare_command(authority, &format!("ticket-{seq}"), ops)
        .unwrap();
    service.execute(authority, &c).unwrap()
}
fn view(
    service: &TicketService,
    id: &str,
    fixed_commit: Option<String>,
) -> ReadEnvelope<SprintView> {
    service
        .inspect_sprint(
            &owner(),
            &SprintInspectRequest {
                project_id: project(),
                sprint_id: sprint_id(id),
                fixed_commit,
            },
        )
        .unwrap()
}
fn unchanged_execution(before: &Snapshot, after: &Snapshot) {
    let mut a = serde_json::to_value(before).unwrap();
    let mut b = serde_json::to_value(after).unwrap();
    for key in ["sprint", "schema", "seq", "receipts"] {
        a.as_object_mut().unwrap().remove(key);
        b.as_object_mut().unwrap().remove(key);
    }
    assert_eq!(
        a, b,
        "planning may only update Sprint data, schema, sequence and receipts"
    );
}

struct Fixture {
    root: tempfile::TempDir,
    workspace: tempfile::TempDir,
    service: TicketService,
    seed: String,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let service = TicketService::open(root.path().join("authority"), binding()).unwrap();
        let mut f = Self {
            root,
            workspace,
            service,
            seed: String::new(),
        };
        f.seed = f.work("seed", Some(project()));
        f
    }
    fn work(&self, title: &str, project: Option<ProjectId>) -> String {
        let mut ops = vec![Operation::Create {
            temp_id: "w".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract(title),
            depends_on: BTreeSet::new(),
        }];
        if let Some(project_id) = project {
            ops.push(Operation::AssociateProject {
                ticket_id: "w".into(),
                project_id,
            });
        }
        ticket_execute(&self.service, &owner(), ops).ids["w"].clone()
    }
    fn sprint(&self, id: &str) {
        self.exec(
            id,
            SprintAction::Create {
                definition: definition(id),
            },
        );
    }
    fn exec(&self, id: &str, action: SprintAction) -> SprintReceipt {
        self.service
            .execute_sprint(&owner(), &command(&self.service, id, action))
            .unwrap()
    }
    fn add(&self, sprint: &str, work: &str) {
        self.exec(
            sprint,
            SprintAction::Add {
                expected_revision: revision(&self.service, sprint),
                work_id: work.into(),
            },
        );
    }
    fn start(&self, sprint: &str) {
        self.exec(
            sprint,
            SprintAction::Start {
                expected_revision: revision(&self.service, sprint),
            },
        );
    }
    fn running(&self, work: &str) -> (String, Authority) {
        let s = self.service.published().unwrap().1;
        let repo = self
            .workspace
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let tree = self.workspace.path().join(work);
        std::fs::create_dir_all(&tree).unwrap();
        let tree = tree.canonicalize().unwrap().to_string_lossy().into_owned();
        let r = ticket_execute(
            &self.service,
            &owner(),
            vec![
                Operation::SetProjectContext {
                    work_id: work.into(),
                    context: ProjectContextSnapshot {
                        project_id: project(),
                        project_revision: 1,
                        resource_revision: 1,
                        workspace_binding: WorkspaceBinding {
                            path: repo.clone(),
                            label: None,
                            git_common_dir: None,
                        },
                        contract_revision: s.tickets[work].contract_revision,
                        contract: s.tickets[work].contract.clone(),
                        sources: BTreeMap::new(),
                    },
                },
                Operation::Ready {
                    work_id: work.into(),
                },
                Operation::Start {
                    work_id: work.into(),
                    temp_id: "a".into(),
                    workspace: Some(ExecutionWorkspace {
                        repo,
                        base_commit: "a".repeat(40),
                        branch: format!("work-{work}"),
                        worktree: tree.clone(),
                        write_roots: vec![tree.clone()],
                        claims: BTreeSet::from([format!("worktree:{tree}")]),
                    }),
                },
            ],
        );
        let id = r.ids["a"].clone();
        let s = self.service.published().unwrap().1;
        let a = &s.assignments[&id];
        let receipt = RuntimeReceipt {
            dispatch_key: a.dispatch_key.clone(),
            spec_hash: s.intents[&a.dispatch_key].spec_hash.clone(),
            run_id: format!("run-{id}"),
            session_id: format!("session-{id}"),
        };
        ticket_execute(
            &self.service,
            &runtime(),
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
        (
            id.clone(),
            Authority::from_verified_host(
                binding(),
                Principal::Worker {
                    assignment_id: id,
                    generation: a.generation,
                    run_id: receipt.run_id,
                    session_id: receipt.session_id,
                },
            ),
        )
    }
    fn submit(&self, assignment: &str, worker: &Authority) -> String {
        let artifact = self
            .service
            .store_artifact(&runtime(), b"verified result")
            .unwrap();
        ticket_execute(
            &self.service,
            worker,
            vec![Operation::Submit {
                assignment_id: assignment.into(),
                temp_id: "submission".into(),
                artifacts: vec![artifact],
                evidence: vec!["controlled fixture output".into()],
            }],
        )
        .ids["submission"]
            .clone()
    }
    fn accept(&self, work: &str, submission: &str) {
        let snapshot = self.service.published().unwrap().1;
        let assignment = &snapshot.assignments[&snapshot.submissions[submission].assignment_id];
        ticket_execute(
            &self.service,
            &runtime(),
            vec![Operation::RuntimeStopped {
                assignment_id: assignment.id.clone(),
                receipt: assignment.runtime.clone().unwrap(),
                completed: true,
            }],
        );
        ticket_execute(
            &self.service,
            &owner(),
            vec![Operation::Accept {
                work_id: work.into(),
                submission_id: submission.into(),
                evidence: vec!["explicit user acceptance".into()],
            }],
        );
    }
}

#[test]
fn lifecycle_revisions_shared_receipts_and_payload_conflicts() {
    let f = Fixture::new();
    let create = command(
        &f.service,
        "one",
        SprintAction::Create {
            definition: definition("one"),
        },
    );
    let r = f.service.execute_sprint(&owner(), &create).unwrap();
    assert_eq!(f.service.execute_sprint(&owner(), &create).unwrap(), r);
    assert_eq!(f.service.published().unwrap().1.schema, 6);
    let mut changed = create.clone();
    changed.source = "different request".into();
    assert!(matches!(
        f.service.execute_sprint(&owner(), &changed),
        Err(Error::IdempotencyConflict)
    ));
    let mut stale = create.clone();
    stale.operation_id = "new-operation".into();
    assert!(matches!(
        f.service.execute_sprint(&owner(), &stale),
        Err(Error::RevisionConflict)
    ));
    let mut collision = f
        .service
        .prepare_command(
            &owner(),
            "different-op",
            vec![Operation::Ready {
                work_id: f.seed.clone(),
            }],
        )
        .unwrap();
    collision.operation_id = create.operation_id.clone();
    assert!(matches!(
        f.service.execute(&owner(), &collision),
        Err(Error::IdempotencyConflict)
    ));
    f.exec(
        "one",
        SprintAction::Edit {
            expected_revision: 1,
            definition: definition("renamed"),
        },
    );
    assert_eq!(
        view(&f.service, "one", None).data.sprint.definition.name,
        "renamed"
    );
    let bad = command(
        &f.service,
        "one",
        SprintAction::Start {
            expected_revision: 1,
        },
    );
    assert!(matches!(
        f.service.execute_sprint(&owner(), &bad),
        Err(Error::RevisionConflict)
    ));
    f.start("one");
    let c = close_command(&f.service, "one", BTreeMap::new());
    let closed = f.service.execute_sprint(&owner(), &c).unwrap();
    assert_eq!(closed.closure.unwrap().counts.total, 0);
    let edit = command(
        &f.service,
        "one",
        SprintAction::Edit {
            expected_revision: revision(&f.service, "one"),
            definition: definition("forbidden"),
        },
    );
    assert!(f.service.execute_sprint(&owner(), &edit).is_err());
}

#[test]
fn competing_starts_and_memberships_are_serialized() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("two");
    let a = command(
        &f.service,
        "one",
        SprintAction::Start {
            expected_revision: 1,
        },
    );
    let mut b = command(
        &f.service,
        "two",
        SprintAction::Start {
            expected_revision: 1,
        },
    );
    b.operation_id.push_str("-other");
    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = [a, b]
        .into_iter()
        .map(|c| {
            let service = f.service.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.execute_sprint(&owner(), &c)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::RevisionConflict)))
            .count(),
        1
    );
    let s = f.service.published().unwrap().1;
    let planned = s
        .sprint
        .sprints
        .values()
        .find(|s| s.status == SprintStatus::Planned)
        .unwrap();
    let retry = command(
        &f.service,
        planned.id.as_str(),
        SprintAction::Start {
            expected_revision: planned.revision,
        },
    );
    assert!(matches!(
        f.service.execute_sprint(&owner(), &retry),
        Err(Error::InvalidTransition(_))
    ));
    f.add("one", &f.seed);
    let duplicate = command(
        &f.service,
        "two",
        SprintAction::Add {
            expected_revision: revision(&f.service, "two"),
            work_id: f.seed.clone(),
        },
    );
    assert!(f.service.execute_sprint(&owner(), &duplicate).is_err());
    f.exec(
        "one",
        SprintAction::Remove {
            expected_revision: revision(&f.service, "one"),
            work_id: f.seed.clone(),
        },
    );
    f.add("two", &f.seed);
    assert_eq!(view(&f.service, "one", None).data.sprint.history.len(), 2);
    assert_eq!(
        view(&f.service, "two", None).data.sprint.members,
        BTreeSet::from([f.seed.clone()])
    );
}

#[test]
fn membership_retries_do_not_duplicate_history_and_cross_project_scope_is_rejected() {
    let f = Fixture::new();
    f.sprint("one");
    let add = command(
        &f.service,
        "one",
        SprintAction::Add {
            expected_revision: 1,
            work_id: f.seed.clone(),
        },
    );
    let receipt = f.service.execute_sprint(&owner(), &add).unwrap();
    assert_eq!(f.service.execute_sprint(&owner(), &add).unwrap(), receipt);
    assert_eq!(view(&f.service, "one", None).data.sprint.history.len(), 1);
    let foreign = f.work("foreign", Some(ProjectId::parse("project-b").unwrap()));
    let legacy = f.work("legacy", None);
    for id in [foreign, legacy] {
        let c = command(
            &f.service,
            "one",
            SprintAction::Add {
                expected_revision: 2,
                work_id: id,
            },
        );
        let before = f.service.published().unwrap().0;
        assert!(matches!(
            f.service.execute_sprint(&owner(), &c),
            Err(Error::ScopeDenied(_))
        ));
        assert_eq!(f.service.published().unwrap().0, before);
    }
    let request = SprintInspectRequest {
        project_id: ProjectId::parse("project-b").unwrap(),
        sprint_id: sprint_id("one"),
        fixed_commit: None,
    };
    assert!(matches!(
        f.service.inspect_sprint(&owner(), &request),
        Err(Error::ScopeDenied(_))
    ));
    let mut wrong_binding = binding();
    wrong_binding.scope_id = "elsewhere".into();
    let foreign_authority = Authority::from_verified_host(
        wrong_binding,
        Principal::User {
            user_id: "owner".into(),
        },
    );
    assert!(matches!(
        f.service.execute_sprint(&foreign_authority, &add),
        Err(Error::ScopeDenied(_))
    ));
    let other_user = Authority::from_verified_host(
        binding(),
        Principal::User {
            user_id: "different".into(),
        },
    );
    assert!(matches!(
        f.service.execute_sprint(&other_user, &add),
        Err(Error::ScopeDenied(_))
    ));
}

#[test]
fn review_archived_member_cannot_be_carried_into_another_sprint() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("next");
    f.add("one", &f.seed);
    f.start("one");
    ticket_execute(
        &f.service,
        &owner(),
        vec![Operation::Archive {
            ticket_id: f.seed.clone(),
            archived: true,
        }],
    );
    let before = f.service.published().unwrap();
    let direct_add = command(
        &f.service,
        "next",
        SprintAction::Add {
            expected_revision: revision(&f.service, "next"),
            work_id: f.seed.clone(),
        },
    );
    assert!(matches!(
        f.service.execute_sprint(&owner(), &direct_add),
        Err(Error::InvalidTransition(reason)) if reason.contains("archived")
    ));
    assert_eq!(f.service.published().unwrap().0, before.0);

    let carry = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        )]),
    );
    assert!(
        matches!(
            f.service.execute_sprint(&owner(), &carry),
            Err(Error::InvalidTransition(reason)) if reason.contains("archived")
        ),
        "carry-over must reject archived Work just like direct Add"
    );
    let after_rejected = f.service.published().unwrap();
    assert_eq!(after_rejected.0, before.0);
    assert_eq!(
        canonical_bytes(&after_rejected.1).unwrap(),
        canonical_bytes(&before.1).unwrap()
    );

    let backlog = close_command(
        &f.service,
        "one",
        BTreeMap::from([(f.seed.clone(), SprintDestination::Backlog)]),
    );
    let receipt = f.service.execute_sprint(&owner(), &backlog).unwrap();
    assert_eq!(
        f.service.execute_sprint(&owner(), &backlog).unwrap(),
        receipt
    );
    assert_eq!(
        receipt.closure.unwrap().members[&f.seed].disposition,
        SprintCloseDisposition::Backlog
    );
    let after = f.service.published().unwrap().1;
    assert!(!after.sprint.sprints[&sprint_id("next")]
        .members
        .contains(&f.seed));
    unchanged_execution(&before.1, &after);
}

#[test]
fn close_requires_complete_dispositions_and_preserves_running_worker_and_all_execution_bytes() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("next");
    let accepted = f.work("accepted", Some(project()));
    let submitted = f.work("submitted", Some(project()));
    let blocked = f.work("blocked", Some(project()));
    let cancelled = f.work("cancelled", Some(project()));
    for w in [&accepted, &submitted, &blocked, &cancelled, &f.seed] {
        f.add("one", w);
    }
    let (a, w) = f.running(&accepted);
    let accepted_submission = f.submit(&a, &w);
    f.accept(&accepted, &accepted_submission);
    let (a, w) = f.running(&submitted);
    f.submit(&a, &w);
    ticket_execute(
        &f.service,
        &owner(),
        vec![
            Operation::Pause {
                work_id: blocked.clone(),
                reason: "waiting".into(),
            },
            Operation::Cancel {
                work_id: cancelled.clone(),
            },
        ],
    );
    let (assignment, worker) = f.running(&f.seed);
    f.start("one");
    let before = f.service.published().unwrap();
    let missing = close_command(&f.service, "one", BTreeMap::new());
    assert!(f.service.execute_sprint(&owner(), &missing).is_err());
    assert_eq!(f.service.published().unwrap().0, before.0);
    let dispositions = BTreeMap::from([
        (submitted.clone(), SprintDestination::Backlog),
        (blocked.clone(), SprintDestination::Backlog),
        (
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        ),
    ]);
    let close = close_command(&f.service, "one", dispositions);
    let r = f.service.execute_sprint(&owner(), &close).unwrap();
    assert_eq!(f.service.execute_sprint(&owner(), &close).unwrap(), r);
    let after = f.service.published().unwrap().1;
    unchanged_execution(&before.1, &after);
    let closure = r.closure.unwrap();
    assert_eq!(closure.source_commit, before.0);
    assert_eq!(
        closure.counts,
        SprintCounts {
            total: 5,
            accepted: 1,
            pending_acceptance: 1,
            blocked: 1,
            cancelled: 1,
            unfinished: 3
        }
    );
    assert_eq!(
        closure.members[&accepted].disposition,
        SprintCloseDisposition::Accepted
    );
    assert_eq!(
        closure.members[&cancelled].disposition,
        SprintCloseDisposition::Cancelled
    );
    assert_eq!(
        after.sprint.sprints[&sprint_id("next")].members,
        BTreeSet::from([f.seed.clone()])
    );
    assert_eq!(
        after.assignments[&assignment].state,
        AssignmentState::Running
    );
    // The same actual Worker identity can still submit; close did not send Stop or revoke its permit.
    let submission = f.submit(&assignment, &worker);
    assert_eq!(
        f.service.published().unwrap().1.tickets[&f.seed].state,
        WorkState::Submitted
    );
    let historical = view(&f.service, "one", None);
    assert_eq!(historical.data.sprint.closure.as_ref().unwrap(), &closure);
    assert_eq!(
        historical.data.current_members[&f.seed]
            .current_submission
            .as_deref(),
        Some(submission.as_str())
    );
    assert_eq!(
        f.service.execute_sprint(&owner(), &close).unwrap().closure,
        Some(closure)
    );
}

#[test]
fn close_conflicts_with_concurrent_submission_and_target_edits_without_partial_membership() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("next");
    f.add("one", &f.seed);
    f.start("one");
    let (assignment, worker) = f.running(&f.seed);
    let close = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        )]),
    );
    f.submit(&assignment, &worker);
    let before = f.service.published().unwrap().0;
    assert!(matches!(
        f.service.execute_sprint(&owner(), &close),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(f.service.published().unwrap().0, before);
    let mut close = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        )]),
    );
    if let SprintAction::Close {
        target_revisions, ..
    } = &mut close.action
    {
        target_revisions.insert(sprint_id("next"), 0);
    }
    assert!(matches!(
        f.service.execute_sprint(&owner(), &close),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(
        view(&f.service, "one", None).data.sprint.status,
        SprintStatus::Active
    );
    assert!(view(&f.service, "next", None)
        .data
        .sprint
        .members
        .is_empty());
}

#[test]
fn frozen_closure_and_fixed_reads_do_not_follow_later_accept_or_reopen() {
    let f = Fixture::new();
    f.sprint("one");
    f.add("one", &f.seed);
    let (assignment, worker) = f.running(&f.seed);
    let submission = f.submit(&assignment, &worker);
    f.start("one");
    let c = close_command(
        &f.service,
        "one",
        BTreeMap::from([(f.seed.clone(), SprintDestination::Backlog)]),
    );
    let receipt = f.service.execute_sprint(&owner(), &c).unwrap();
    let fixed = f.service.published().unwrap().0;
    f.accept(&f.seed, &submission);
    let current = view(&f.service, "one", None);
    assert_eq!(current.data.current_counts.accepted, 1);
    assert_eq!(
        current
            .data
            .sprint
            .closure
            .as_ref()
            .unwrap()
            .counts
            .accepted,
        0
    );
    assert_eq!(
        view(&f.service, "one", Some(fixed.clone()))
            .data
            .current_counts
            .accepted,
        0
    );
    ticket_execute(
        &f.service,
        &owner(),
        vec![Operation::Reopen {
            work_id: f.seed.clone(),
        }],
    );
    assert_eq!(
        view(&f.service, "one", None).data.sprint.closure,
        receipt.closure
    );
    assert_eq!(
        view(&f.service, "one", Some(fixed)).data.current_members[&f.seed].state,
        WorkState::Submitted
    );
    let request = SprintInspectRequest {
        project_id: project(),
        sprint_id: sprint_id("one"),
        fixed_commit: Some("f".repeat(64)),
    };
    assert!(matches!(
        f.service.inspect_sprint(&owner(), &request),
        Err(Error::ResyncRequired)
    ));
}

#[test]
fn runtime_worker_and_forged_done_payload_cannot_plan_or_accept() {
    let f = Fixture::new();
    f.sprint("one");
    let (assignment, worker) = f.running(&f.seed);
    let add = command(
        &f.service,
        "one",
        SprintAction::Add {
            expected_revision: 1,
            work_id: f.seed.clone(),
        },
    );
    let request = SprintInspectRequest {
        project_id: project(),
        sprint_id: sprint_id("one"),
        fixed_commit: None,
    };
    for denied in [runtime(), worker] {
        assert!(matches!(
            f.service.execute_sprint(&denied, &add),
            Err(Error::ScopeDenied(_))
        ));
        assert!(matches!(
            f.service.inspect_sprint(&denied, &request),
            Err(Error::ScopeDenied(_))
        ));
    }
    assert_eq!(
        f.service.published().unwrap().1.assignments[&assignment].state,
        AssignmentState::Running
    );
    let mut forged = serde_json::to_value(add).unwrap();
    forged["action"]["state"] = serde_json::json!("accepted");
    assert!(serde_json::from_value::<SprintCommand>(forged).is_err());
    let bad = serde_json::json!({"op":"accept","expected_revision":1});
    assert!(serde_json::from_value::<SprintAction>(bad).is_err());
}

#[test]
fn malformed_dates_unknown_project_closed_targets_and_extra_dispositions_are_atomic_errors() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("closed");
    f.start("closed");
    f.service
        .execute_sprint(
            &owner(),
            &close_command(&f.service, "closed", BTreeMap::new()),
        )
        .unwrap();
    f.add("one", &f.seed);
    f.start("one");
    let mut bad_definition = definition("bad");
    bad_definition.ends_at = Some(1);
    let bad = command(
        &f.service,
        "one",
        SprintAction::Edit {
            expected_revision: revision(&f.service, "one"),
            definition: bad_definition,
        },
    );
    assert!(f.service.execute_sprint(&owner(), &bad).is_err());
    let mut unknown = command(
        &f.service,
        "unknown",
        SprintAction::Create {
            definition: definition("unknown"),
        },
    );
    unknown.project_id = ProjectId::parse("unlinked-project").unwrap();
    assert!(matches!(
        f.service.execute_sprint(&owner(), &unknown),
        Err(Error::ScopeDenied(_))
    ));
    let closed_target = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("closed"),
            },
        )]),
    );
    assert!(f.service.execute_sprint(&owner(), &closed_target).is_err());
    let self_target = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("one"),
            },
        )]),
    );
    assert!(f.service.execute_sprint(&owner(), &self_target).is_err());
    let extra = close_command(
        &f.service,
        "one",
        BTreeMap::from([
            (f.seed.clone(), SprintDestination::Backlog),
            ("missing-work".into(), SprintDestination::Backlog),
        ]),
    );
    let before = f.service.published().unwrap().0;
    assert!(f.service.execute_sprint(&owner(), &extra).is_err());
    assert_eq!(f.service.published().unwrap().0, before);
}

#[test]
fn ack_loss_reopens_same_receipt_and_history_once() {
    let f = Fixture::new();
    f.sprint("one");
    f.sprint("next");
    f.add("one", &f.seed);
    f.start("one");
    let c = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        )]),
    );
    let expected = f.service.execute_sprint(&owner(), &c).unwrap();
    drop(f.service);
    let reopened = TicketService::open(f.root.path().join("authority"), binding()).unwrap();
    assert_eq!(reopened.health(), Health::Writable);
    assert_eq!(reopened.execute_sprint(&owner(), &c).unwrap(), expected);
    let s = reopened.published().unwrap().1;
    assert_eq!(s.sprint.sprints[&sprint_id("next")].history.len(), 1);
    assert_eq!(s.tickets.len(), 1);
    assert_eq!(s.tickets[&f.seed].generation, 0);
}

#[test]
fn schema_six_planning_survives_stopped_offline_transfer_without_downgrade() {
    let f = Fixture::new();
    f.sprint("one");
    f.add("one", &f.seed);
    f.start("one");
    let c = close_command(
        &f.service,
        "one",
        BTreeMap::from([(f.seed.clone(), SprintDestination::Backlog)]),
    );
    let expected = f.service.execute_sprint(&owner(), &c).unwrap();
    let before = f.service.published().unwrap().1.sprint.sprints.clone();
    drop(f.service);
    let path = f.root.path().join("authority");
    let target = f.root.path().join("migrated");
    let source = TicketService::open_offline(&path, binding()).unwrap();
    let (commit, s) = source.published().unwrap();
    let request = MigrationRequest {
        operation_id: "migration".into(),
        binding: binding(),
        expected_commit: commit,
        expected_seq: s.seq,
        expected_epoch: s.authority_epoch,
        source_root: path.canonicalize().unwrap().to_string_lossy().into_owned(),
        destination_root: canonical_migration_destination(&target).unwrap(),
        supervisor_snapshot_hash: None,
    };
    let stopped = StoppedScopeProof::from_verified_host(binding());
    source
        .retire_for_migration(&owner(), &request, &stopped)
        .unwrap();
    source
        .export_retired_migration(&owner(), &request, &stopped)
        .unwrap();
    let destination = TicketService::open_offline(&target, binding()).unwrap();
    destination
        .activate_migrated_copy(&source, &owner(), &request, &stopped)
        .unwrap();
    let s = destination.published().unwrap().1;
    assert_eq!(s.schema, 6);
    assert_eq!(s.sprint.sprints, before);
    assert_eq!(destination.execute_sprint(&owner(), &c).unwrap(), expected);
}

#[test]
fn crash_close_child() {
    let Ok(root) = std::env::var("SPRINT_CRASH_ROOT") else {
        return;
    };
    let service =
        TicketService::open(std::path::Path::new(&root).join("authority"), binding()).unwrap();
    let work = service
        .published()
        .unwrap()
        .1
        .tickets
        .keys()
        .next()
        .unwrap()
        .clone();
    let request = close_command(
        &service,
        "one",
        BTreeMap::from([(
            work,
            SprintDestination::Sprint {
                sprint_id: sprint_id("next"),
            },
        )]),
    );
    std::fs::write(
        std::path::Path::new(&root).join("request.json"),
        serde_json::to_vec(&request).unwrap(),
    )
    .unwrap();
    let after = std::env::var("SPRINT_CRASH_AFTER").unwrap() == "1";
    service.set_publication_fault(Some(Arc::new(move |point| {
        if point
            == if after {
                FaultPoint::AfterHeadRename
            } else {
                FaultPoint::BeforeHeadRename
            }
        {
            std::process::exit(91);
        }
        Ok(())
    })));
    let _ = service.execute_sprint(&owner(), &request);
    panic!("fault hook must exit");
}

#[test]
fn process_crashes_before_and_after_head_publish_recover_only_complete_old_or_new_result() {
    for after in [false, true] {
        let f = Fixture::new();
        f.sprint("one");
        f.sprint("next");
        f.add("one", &f.seed);
        f.start("one");
        drop(f.service);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_close_child", "--nocapture"])
            .env("SPRINT_CRASH_ROOT", f.root.path())
            .env("SPRINT_CRASH_AFTER", if after { "1" } else { "0" })
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(91),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let service = TicketService::open(f.root.path().join("authority"), binding()).unwrap();
        assert_eq!(service.health(), Health::Writable);
        let s = service.published().unwrap().1;
        assert_eq!(
            s.sprint.sprints[&sprint_id("one")].status,
            if after {
                SprintStatus::Closed
            } else {
                SprintStatus::Active
            }
        );
        assert_eq!(
            s.sprint.sprints[&sprint_id("next")]
                .members
                .contains(&f.seed),
            after
        );
        let mut request: SprintCommand =
            serde_json::from_slice(&std::fs::read(f.root.path().join("request.json")).unwrap())
                .unwrap();
        if !after {
            request.expected_seq = s.seq;
            request.expected_epoch = s.authority_epoch;
        }
        let receipt = service.execute_sprint(&owner(), &request).unwrap();
        assert_eq!(service.execute_sprint(&owner(), &request).unwrap(), receipt);
        let s = service.published().unwrap().1;
        assert_eq!(s.sprint.sprints[&sprint_id("next")].history.len(), 1);
        assert_eq!(s.tickets.len(), 1);
    }
}

#[test]
fn accepted_closure_stays_frozen_after_reopen_and_archive_preserves_references() {
    let f = Fixture::new();
    f.sprint("one");
    f.add("one", &f.seed);
    let (assignment, worker) = f.running(&f.seed);
    let submission = f.submit(&assignment, &worker);
    f.accept(&f.seed, &submission);
    f.start("one");
    let c = close_command(&f.service, "one", BTreeMap::new());
    let receipt = f.service.execute_sprint(&owner(), &c).unwrap();
    assert_eq!(receipt.closure.as_ref().unwrap().counts.accepted, 1);
    ticket_execute(
        &f.service,
        &owner(),
        vec![
            Operation::Reopen {
                work_id: f.seed.clone(),
            },
            Operation::Archive {
                ticket_id: f.seed.clone(),
                archived: true,
            },
        ],
    );
    let v = view(&f.service, "one", None);
    assert_eq!(v.data.sprint.closure, receipt.closure);
    assert_eq!(v.data.current_counts.accepted, 0);
    assert!(v.data.current_members[&f.seed].archived);
    assert!(v.data.sprint.members.contains(&f.seed));
}

#[test]
fn start_and_remove_exact_retries_keep_one_receipt_and_one_history_event() {
    let f = Fixture::new();
    f.sprint("one");
    f.add("one", &f.seed);
    let start = command(
        &f.service,
        "one",
        SprintAction::Start {
            expected_revision: revision(&f.service, "one"),
        },
    );
    let a = f.service.execute_sprint(&owner(), &start).unwrap();
    assert_eq!(f.service.execute_sprint(&owner(), &start).unwrap(), a);
    ticket_execute(
        &f.service,
        &owner(),
        vec![Operation::Archive {
            ticket_id: f.seed.clone(),
            archived: true,
        }],
    );
    let remove = command(
        &f.service,
        "one",
        SprintAction::Remove {
            expected_revision: revision(&f.service, "one"),
            work_id: f.seed.clone(),
        },
    );
    let r = f.service.execute_sprint(&owner(), &remove).unwrap();
    assert_eq!(f.service.execute_sprint(&owner(), &remove).unwrap(), r);
    let v = view(&f.service, "one", None);
    assert!(v.data.sprint.members.is_empty());
    assert_eq!(v.data.sprint.history.len(), 2);
}

#[test]
fn stale_accepted_evidence_is_unfinished_and_cannot_be_silently_retained_as_done() {
    let f = Fixture::new();
    f.sprint("one");
    f.add("one", &f.seed);
    let (assignment, worker) = f.running(&f.seed);
    let submission = f.submit(&assignment, &worker);
    f.accept(&f.seed, &submission);
    f.start("one");
    // Controlled persisted negative fixture: a stale accepted pointer must fail closed
    // even if another writer left the old Work state label behind.
    drop(f.service);
    let root = f.root.path().join("authority");
    let mut raw = store::FileStore::open(&root, binding()).unwrap();
    let mut s = raw.published.as_ref().unwrap().1.clone();
    s.submissions.get_mut(&submission).unwrap().stale = true;
    s.seq += 1;
    raw.publish(s).unwrap();
    drop(raw);
    let service = TicketService::open(&root, binding()).unwrap();
    assert_eq!(service.health(), Health::Writable);
    assert_eq!(view(&service, "one", None).data.current_counts.accepted, 0);
    assert!(service
        .execute_sprint(&owner(), &close_command(&service, "one", BTreeMap::new()))
        .is_err());
    let c = close_command(
        &service,
        "one",
        BTreeMap::from([(f.seed.clone(), SprintDestination::Backlog)]),
    );
    let closure = service
        .execute_sprint(&owner(), &c)
        .unwrap()
        .closure
        .unwrap();
    assert_eq!(closure.counts.unfinished, 1);
    assert_eq!(closure.counts.accepted, 0);
    assert_eq!(
        closure.members[&f.seed].disposition,
        SprintCloseDisposition::Backlog
    );
}

#[test]
fn legacy_snapshot_omits_planning_and_schema_downgrade_quarantines_existing_history() {
    let f = Fixture::new();
    assert!(serde_json::to_value(f.service.published().unwrap().1)
        .unwrap()
        .get("sprint")
        .is_none());
    f.sprint("one");
    f.add("one", &f.seed);
    drop(f.service);
    let root = f.root.path().join("authority");
    let mut raw = store::FileStore::open(&root, binding()).unwrap();
    let mut s = raw.published.as_ref().unwrap().1.clone();
    s.schema = 5;
    s.seq += 1;
    raw.publish(s).unwrap();
    drop(raw);
    let service = TicketService::open(&root, binding()).unwrap();
    assert!(matches!(service.health(), Health::ReadOnly { .. }));
    assert_eq!(
        service.published().unwrap().1.sprint.sprints[&sprint_id("one")]
            .members
            .len(),
        1
    );
}

#[test]
fn review_sprint_only_changes_are_commit_consistent_and_visible_once() {
    fn commit(f: &Fixture, request: &SprintCommand, ids: &[&str]) -> Vec<(u64, String)> {
        let before = f.service.published().unwrap();
        let receipt = f.service.execute_sprint(&owner(), request).unwrap();
        let after = f.service.published().unwrap();
        assert_eq!(after.1.seq, before.1.seq + 1);
        assert_eq!(receipt.operation.committed_seq, after.1.seq);
        unchanged_execution(&before.1, &after.1);
        let changes = f
            .service
            .work_changes(&owner(), before.1.seq, 100, None)
            .unwrap();
        assert_eq!(changes.snapshot.commit, after.0);
        let mut expected: Vec<_> = ids
            .iter()
            .map(|id| (after.1.seq, "sprint".to_string(), id.to_string()))
            .collect();
        expected.sort();
        assert_eq!(
            changes
                .data
                .iter()
                .map(|c| (c.seq, c.entity.clone(), c.id.clone()))
                .collect::<Vec<_>>(),
            expected,
            "each committed Sprint update needs exactly one change marker"
        );
        assert_eq!(
            f.service.execute_sprint(&owner(), request).unwrap(),
            receipt
        );
        assert_eq!(f.service.published().unwrap().0, after.0);
        assert_eq!(
            canonical_bytes(
                &f.service
                    .work_changes(&owner(), before.1.seq, 100, None)
                    .unwrap()
                    .data
            )
            .unwrap(),
            canonical_bytes(&changes.data).unwrap(),
            "an exact replay cannot publish another change"
        );
        expected.into_iter().map(|(seq, _, id)| (seq, id)).collect()
    }

    let f = Fixture::new();
    let (_, worker) = f.running(&f.seed);
    let origin = f.service.published().unwrap();
    let create = command(
        &f.service,
        "one",
        SprintAction::Create {
            definition: definition("one"),
        },
    );
    f.service.set_publication_fault(Some(Arc::new(|point| {
        if point == FaultPoint::BeforeHeadRename {
            Err(std::io::Error::other("Sprint change publication rejected"))
        } else {
            Ok(())
        }
    })));
    assert!(f.service.execute_sprint(&owner(), &create).is_err());
    assert_eq!(f.service.published().unwrap().0, origin.0);
    assert!(f
        .service
        .work_changes(&owner(), origin.1.seq, 100, None)
        .unwrap()
        .data
        .is_empty());
    f.service.set_publication_fault(None);
    let mut expected = commit(&f, &create, &["one"]);
    expected.extend(commit(
        &f,
        &command(
            &f.service,
            "two",
            SprintAction::Create {
                definition: definition("two"),
            },
        ),
        &["two"],
    ));
    for action in [
        SprintAction::Edit {
            expected_revision: 1,
            definition: definition("revised"),
        },
        SprintAction::Add {
            expected_revision: 2,
            work_id: f.seed.clone(),
        },
        SprintAction::Remove {
            expected_revision: 3,
            work_id: f.seed.clone(),
        },
        SprintAction::Add {
            expected_revision: 4,
            work_id: f.seed.clone(),
        },
        SprintAction::Start {
            expected_revision: 5,
        },
    ] {
        expected.extend(commit(&f, &command(&f.service, "one", action), &["one"]));
    }
    let close = close_command(
        &f.service,
        "one",
        BTreeMap::from([(
            f.seed.clone(),
            SprintDestination::Sprint {
                sprint_id: sprint_id("two"),
            },
        )]),
    );
    expected.extend(commit(&f, &close, &["one", "two"]));
    let all = f
        .service
        .work_changes(&owner(), origin.1.seq, 100, None)
        .unwrap();
    assert_eq!(
        all.data
            .iter()
            .map(|c| (c.seq, c.id.clone()))
            .collect::<Vec<_>>(),
        expected
    );
    let supervisor = Authority::from_verified_host(
        binding(),
        Principal::Supervisor {
            session_id: binding().supervisor_session_id,
        },
    );
    assert_eq!(
        canonical_bytes(
            &f.service
                .work_changes(&supervisor, origin.1.seq, 100, None)
                .unwrap()
                .data
        )
        .unwrap(),
        canonical_bytes(&all.data).unwrap()
    );
    for restricted in [worker, runtime()] {
        assert!(f
            .service
            .work_changes(&restricted, origin.1.seq, 100, None)
            .unwrap()
            .data
            .is_empty());
    }
    let first = f
        .service
        .work_changes(&owner(), origin.1.seq, 1, None)
        .unwrap();
    let mut paged = first.data;
    let mut cursor = first.next_cursor;
    f.sprint("later");
    while let Some(next) = cursor {
        let page = f
            .service
            .work_changes(&owner(), origin.1.seq, 1, Some(&next))
            .unwrap();
        assert_eq!(page.snapshot.commit, all.snapshot.commit);
        paged.extend(page.data);
        cursor = page.next_cursor;
    }
    assert_eq!(
        canonical_bytes(&paged).unwrap(),
        canonical_bytes(&all.data).unwrap()
    );
    let later = f
        .service
        .work_changes(&owner(), all.snapshot.seq, 100, None)
        .unwrap();
    assert_eq!(later.data.len(), 1);
    assert_eq!(later.data[0].entity, "sprint");
    assert_eq!(later.data[0].id, "later");
}

#[test]
fn changes_cursors_from_before_sprint_projection_require_resync() {
    let f = Fixture::new();
    let since = f.service.published().unwrap().1.seq;
    f.sprint("before-ticket-pages");
    let first_work = f.work("first-old-page", Some(project()));
    let second_work = f.work("second-old-page", Some(project()));
    let before_read = f.service.published().unwrap();
    let all = f.service.work_changes(&owner(), since, 100, None).unwrap();
    assert_eq!(all.data.len(), 3);
    assert_eq!(all.data[0].entity, "sprint");
    let old_rows: Vec<_> = all
        .data
        .iter()
        .filter(|row| row.entity != "sprint")
        .collect();
    assert_eq!(old_rows.len(), 2);
    assert_eq!(old_rows[0].id, first_work);
    assert_eq!(old_rows[1].id, second_work);
    let old_query_hash =
        content_hash(&canonical_bytes(&serde_json::json!({"changes_since": since})).unwrap());
    // An old producer returned the first Ticket and offset=1, omitting Sprint rows.
    // Accepting that offset now repeats the first Ticket and misses the earlier Sprint.
    for offset in [0, 1] {
        let legacy = ReadCursor {
            commit: all.snapshot.commit.clone(),
            query_hash: old_query_hash.clone(),
            offset,
        };
        assert!(
            matches!(
                f.service.work_changes(&owner(), since, 1, Some(&legacy)),
                Err(Error::ResyncRequired)
            ),
            "legacy changes cursor must require resync after the Sprint projection upgrade"
        );
    }
    assert_eq!(f.service.published().unwrap().0, before_read.0);
    let first = f.service.work_changes(&owner(), since, 1, None).unwrap();
    let cursor = first.next_cursor.unwrap();
    assert_ne!(cursor.query_hash, old_query_hash);
    let next = f
        .service
        .work_changes(&owner(), since, 100, Some(&cursor))
        .unwrap();
    assert_eq!(next.snapshot.commit, all.snapshot.commit);
    let mut pages = first.data;
    pages.extend(next.data);
    assert_eq!(pages, all.data);
}
