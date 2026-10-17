use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_tickets::*;
use std::collections::{BTreeMap, BTreeSet};

struct Fixture {
    root: tempfile::TempDir,
    workspace: tempfile::TempDir,
    service: TicketService,
    authority: Authority,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let binding = ScopeBinding {
            scope_id: "scope".into(),
            supervisor_session_id: "supervisor".into(),
            binding_revision: 1,
        };
        let service = TicketService::open(root.path(), binding.clone()).unwrap();
        let authority = Authority::from_verified_host(
            binding,
            Principal::User {
                user_id: "owner".into(),
            },
        );
        Self {
            root,
            workspace,
            service,
            authority,
        }
    }
    fn command(&self, id: &str, operations: Vec<Operation>) -> Command {
        let s = self.service.published().unwrap().1;
        Command {
            operation_id: id.into(),
            binding: s.binding,
            expected_seq: s.seq,
            expected_epoch: s.authority_epoch,
            operations,
            source: None,
        }
    }
    fn exec(&self, id: &str, ops: Vec<Operation>) -> OperationReceipt {
        self.service
            .execute(&self.authority, &self.command(id, ops))
            .unwrap()
    }
    fn create(&self, title: &str, linked: bool) -> String {
        let mut ops = vec![Operation::Create {
            temp_id: "w".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract(title),
            depends_on: BTreeSet::new(),
        }];
        if linked {
            ops.push(Operation::AssociateProject {
                ticket_id: "w".into(),
                project_id: project(),
            });
        }
        self.exec(title, ops).ids["w"].clone()
    }
    fn context(&self, work: &str) -> ProjectContextSnapshot {
        let s = self.service.published().unwrap().1;
        ProjectContextSnapshot {
            project_id: project(),
            project_revision: 1,
            resource_revision: 1,
            workspace_binding: WorkspaceBinding {
                path: self.workspace.path().to_string_lossy().into(),
                label: None,
                git_common_dir: None,
            },
            contract_revision: s.tickets[work].contract_revision,
            contract: s.tickets[work].contract.clone(),
            sources: BTreeMap::from([("requirements".into(), "revision-1".into())]),
        }
    }
    fn execution(&self) -> ExecutionWorkspace {
        let path = self
            .workspace
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        ExecutionWorkspace {
            repo: path.clone(),
            base_commit: "a".repeat(40),
            branch: "work".into(),
            worktree: path.clone(),
            write_roots: vec![path.clone()],
            claims: BTreeSet::from([format!("worktree:{path}")]),
        }
    }
    fn running(&self, work: &str) -> (String, Authority, Authority) {
        self.exec(
            "setup-running",
            vec![
                Operation::SetProjectContext {
                    work_id: work.into(),
                    context: self.context(work),
                },
                Operation::Ready {
                    work_id: work.into(),
                },
            ],
        );
        let receipt = self.exec(
            "start-running",
            vec![Operation::Start {
                work_id: work.into(),
                temp_id: "assignment".into(),
                workspace: Some(self.execution()),
            }],
        );
        let id = receipt.ids["assignment"].clone();
        let snapshot = self.service.published().unwrap().1;
        let a = &snapshot.assignments[&id];
        let runtime = Authority::from_verified_host(snapshot.binding.clone(), Principal::Runtime);
        let worker = Authority::from_verified_host(
            snapshot.binding,
            Principal::Worker {
                assignment_id: id.clone(),
                generation: a.generation,
                run_id: "run".into(),
                session_id: "worker".into(),
            },
        );
        self.service
            .execute(
                &runtime,
                &self.command(
                    "admit-running",
                    vec![
                        Operation::Admitted {
                            assignment_id: id.clone(),
                            receipt: RuntimeReceipt {
                                dispatch_key: a.dispatch_key.clone(),
                                spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
                                run_id: "run".into(),
                                session_id: "worker".into(),
                            },
                        },
                        Operation::Running {
                            assignment_id: id.clone(),
                        },
                    ],
                ),
            )
            .unwrap();
        (id, runtime, worker)
    }
    fn board(&self, limit: usize, cursor: Option<ReadCursor>) -> ReadEnvelope<ProjectBoard> {
        self.service
            .project_board(
                &self.authority,
                &ProjectBoardRequest {
                    project_id: project(),
                    include_cancelled: false,
                    include_archived: false,
                    limit,
                    cursor,
                    fixed_commit: None,
                },
            )
            .unwrap()
    }
}
fn project() -> ProjectId {
    ProjectId::parse("project-a").unwrap()
}
fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: "deliver".into(),
        constraints: vec!["bounded".into()],
        acceptance: vec!["evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}

#[test]
fn explicit_membership_cas_replay_and_legacy_compatibility() {
    let f = Fixture::new();
    let legacy = f.create("legacy", false);
    let work = f.create("linked", true);
    assert_eq!(f.board(10, None).data.rows[0].ticket.id, work);
    let command = f.command(
        "attach",
        vec![Operation::AssociateProject {
            ticket_id: legacy.clone(),
            project_id: project(),
        }],
    );
    let receipt = f.service.execute(&f.authority, &command).unwrap();
    assert_eq!(f.service.execute(&f.authority, &command).unwrap(), receipt);
    let mut changed = command.clone();
    changed.operations = vec![Operation::AssociateProject {
        ticket_id: legacy.clone(),
        project_id: ProjectId::parse("other").unwrap(),
    }];
    assert!(matches!(
        f.service.execute(&f.authority, &changed),
        Err(Error::IdempotencyConflict)
    ));
    let mut stale = command.clone();
    stale.operation_id = "stale".into();
    assert!(matches!(
        f.service.execute(&f.authority, &stale),
        Err(Error::RevisionConflict)
    ));
    let moved = f.command(
        "move",
        vec![Operation::AssociateProject {
            ticket_id: legacy,
            project_id: ProjectId::parse("other").unwrap(),
        }],
    );
    assert!(f.service.execute(&f.authority, &moved).is_err());
    assert_eq!(f.board(10, None).data.counts.total, 2);
    assert_eq!(f.service.published().unwrap().1.schema, 5);
}

#[test]
fn linked_goals_inherit_membership_and_cross_project_edges_are_atomic_errors() {
    let f = Fixture::new();
    let r = f.exec(
        "goal",
        vec![
            Operation::Create {
                temp_id: "goal".into(),
                kind: TicketKind::Goal,
                parent: None,
                contract: contract("goal"),
                depends_on: BTreeSet::new(),
            },
            Operation::AssociateProject {
                ticket_id: "goal".into(),
                project_id: project(),
            },
            Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: Some("goal".into()),
                contract: contract("child"),
                depends_on: BTreeSet::new(),
            },
        ],
    );
    assert_eq!(
        f.service.published().unwrap().1.project.links[&r.ids["work"]],
        project()
    );
    let legacy = f.create("legacy", false);
    let before = f.service.published().unwrap().0;
    let c = f.command(
        "cross",
        vec![Operation::SetDependencies {
            work_id: r.ids["work"].clone(),
            depends_on: BTreeSet::from([legacy]),
        }],
    );
    assert!(f.service.execute(&f.authority, &c).is_err());
    assert_eq!(f.service.published().unwrap().0, before);
    assert_eq!(f.board(10, None).data.counts.total, 1);
}

#[test]
fn context_is_bounded_required_and_does_not_grant_or_switch_workspace() {
    let f = Fixture::new();
    let w = f.create("linked", true);
    f.exec("ready", vec![Operation::Ready { work_id: w.clone() }]);
    let start = f.command(
        "start",
        vec![Operation::Start {
            work_id: w.clone(),
            temp_id: "a".into(),
            workspace: Some(f.execution()),
        }],
    );
    assert!(f.service.execute(&f.authority, &start).is_err());
    let mut huge = f.context(&w);
    huge.sources
        .insert("big".into(), "x".repeat(PROJECT_CONTEXT_BYTES_LIMIT));
    assert!(matches!(
        f.service.execute(
            &f.authority,
            &f.command(
                "huge",
                vec![Operation::SetProjectContext {
                    work_id: w.clone(),
                    context: huge
                }]
            )
        ),
        Err(Error::ContextBudgetExceeded)
    ));
    f.exec(
        "context",
        vec![Operation::SetProjectContext {
            work_id: w.clone(),
            context: f.context(&w),
        }],
    );
    let mut wrong = f.execution();
    wrong.repo = "another-repo".into();
    assert!(f
        .service
        .execute(
            &f.authority,
            &f.command(
                "wrong",
                vec![Operation::Start {
                    work_id: w.clone(),
                    temp_id: "a".into(),
                    workspace: Some(wrong)
                }]
            )
        )
        .is_err());
    let r = f.exec(
        "start-ok",
        vec![Operation::Start {
            work_id: w.clone(),
            temp_id: "a".into(),
            workspace: Some(f.execution()),
        }],
    );
    let s = f.service.published().unwrap().1;
    let a = &s.assignments[&r.ids["a"]];
    let hash = &s.project.assignment_contexts[&a.id];
    assert_eq!(
        s.intents[&a.dispatch_key]
            .immutable_spec
            .project_context_hash
            .as_ref(),
        Some(hash)
    );
    assert_eq!(s.project.contexts[hash], f.context(&w));
    assert_eq!(a.allowed_tools, contract("linked").allowed_tools);
    assert!(f
        .service
        .execute(
            &f.authority,
            &f.command(
                "midrun",
                vec![Operation::SetProjectContext {
                    work_id: w.clone(),
                    context: {
                        let mut context = f.context(&w);
                        context
                            .sources
                            .insert("requirements".into(), "revision-2".into());
                        context
                    }
                }]
            )
        )
        .is_err());
    assert_eq!(f.board(10, None).data.counts.doing, 1);
}

#[test]
fn snapshot_pagination_counts_cancelled_and_filter_cursor_binding() {
    let f = Fixture::new();
    for i in 0..3 {
        f.create(&format!("w{i}"), true);
    }
    let cancelled = f.create("cancelled", true);
    f.exec("cancel", vec![Operation::Cancel { work_id: cancelled }]);
    let first = f.board(1, None);
    assert_eq!(first.data.counts.total, 4);
    assert_eq!(first.data.counts.cancelled, 1);
    assert_eq!(first.data.counts.done, 0);
    assert_eq!(first.data.counts.todo, 3);
    f.create("later", true);
    let second = f.board(100, first.next_cursor.clone());
    assert_eq!(first.snapshot.commit, second.snapshot.commit);
    assert_eq!(second.data.counts.total, 4);
    assert_eq!(second.data.rows.len(), 2);
    assert!(!second
        .data
        .rows
        .iter()
        .any(|r| r.ticket.id == first.data.rows[0].ticket.id));
    let request = ProjectBoardRequest {
        project_id: ProjectId::parse("other").unwrap(),
        include_cancelled: false,
        include_archived: false,
        limit: 10,
        cursor: first.next_cursor,
        fixed_commit: None,
    };
    assert!(matches!(
        f.service.project_board(&f.authority, &request),
        Err(Error::ResyncRequired)
    ));
}

#[test]
fn pause_keeps_stage_and_context_survives_restart_without_grant_revival() {
    let f = Fixture::new();
    let w = f.create("work", true);
    f.exec(
        "setup",
        vec![
            Operation::SetProjectContext {
                work_id: w.clone(),
                context: f.context(&w),
            },
            Operation::Ready { work_id: w.clone() },
        ],
    );
    let r = f.exec(
        "start",
        vec![Operation::Start {
            work_id: w.clone(),
            temp_id: "a".into(),
            workspace: Some(f.execution()),
        }],
    );
    f.exec(
        "pause",
        vec![Operation::Pause {
            work_id: w.clone(),
            reason: "user pause".into(),
        }],
    );
    let row = &f.board(10, None).data.rows[0];
    assert!(row.ticket.paused);
    assert_eq!(row.ticket.state, WorkState::Blocked);
    assert_eq!(row.stage, Some(ProjectBoardStage::Todo));
    let s = f.service.published().unwrap().1;
    let hash = s.project.assignment_contexts[&r.ids["a"]].clone();
    drop(f.service);
    let reopened = TicketService::open(f.root.path(), s.binding).unwrap();
    let s = reopened.published().unwrap().1;
    assert_eq!(s.project.assignment_contexts[&r.ids["a"]], hash);
    assert_eq!(
        s.assignments[&r.ids["a"]].state,
        AssignmentState::OutcomeUnknown
    );
}

#[test]
fn contract_change_requires_fresh_context_and_foreign_scope_cannot_read_board() {
    let f = Fixture::new();
    let w = f.create("work", true);
    f.exec(
        "context",
        vec![Operation::SetProjectContext {
            work_id: w.clone(),
            context: f.context(&w),
        }],
    );
    f.exec(
        "steer",
        vec![Operation::UpdateContract {
            work_id: w.clone(),
            contract: contract("changed"),
        }],
    );
    assert!(f
        .service
        .execute(
            &f.authority,
            &f.command(
                "start-stale",
                vec![Operation::Start {
                    work_id: w.clone(),
                    temp_id: "a".into(),
                    workspace: Some(f.execution())
                }]
            )
        )
        .is_err());
    f.exec(
        "fresh",
        vec![Operation::SetProjectContext {
            work_id: w.clone(),
            context: f.context(&w),
        }],
    );
    assert_eq!(f.service.published().unwrap().1.project.contexts.len(), 2);
    let foreign = Authority::from_verified_host(
        ScopeBinding {
            scope_id: "other".into(),
            supervisor_session_id: "supervisor".into(),
            binding_revision: 1,
        },
        Principal::User {
            user_id: "owner".into(),
        },
    );
    assert!(matches!(
        f.service.project_board(
            &foreign,
            &ProjectBoardRequest {
                project_id: project(),
                include_cancelled: false,
                include_archived: false,
                limit: 10,
                cursor: None,
                fixed_commit: None
            }
        ),
        Err(Error::ScopeDenied(_))
    ));
}

#[test]
fn submitted_stays_testing_until_exact_evidenced_user_acceptance() {
    let f = Fixture::new();
    let w = f.create("delivery", true);
    f.exec(
        "setup",
        vec![
            Operation::SetProjectContext {
                work_id: w.clone(),
                context: f.context(&w),
            },
            Operation::Ready { work_id: w.clone() },
        ],
    );
    let r = f.exec(
        "start",
        vec![Operation::Start {
            work_id: w.clone(),
            temp_id: "a".into(),
            workspace: Some(f.execution()),
        }],
    );
    let s = f.service.published().unwrap().1;
    let a = &s.assignments[&r.ids["a"]];
    let runtime = Authority::from_verified_host(s.binding.clone(), Principal::Runtime);
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: s.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: "run".into(),
        session_id: "worker".into(),
    };
    f.service
        .execute(
            &runtime,
            &f.command(
                "admit",
                vec![
                    Operation::Admitted {
                        assignment_id: a.id.clone(),
                        receipt,
                    },
                    Operation::Running {
                        assignment_id: a.id.clone(),
                    },
                ],
            ),
        )
        .unwrap();
    let worker = Authority::from_verified_host(
        s.binding.clone(),
        Principal::Worker {
            assignment_id: a.id.clone(),
            generation: a.generation,
            run_id: "run".into(),
            session_id: "worker".into(),
        },
    );
    let board_request = ProjectBoardRequest {
        project_id: project(),
        include_cancelled: false,
        include_archived: false,
        limit: 10,
        cursor: None,
        fixed_commit: None,
    };
    assert!(matches!(
        f.service.project_board(&worker, &board_request),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        f.service.project_board(&runtime, &board_request),
        Err(Error::ScopeDenied(_))
    ));
    let submitted = f
        .service
        .execute(
            &worker,
            &f.command(
                "submit",
                vec![Operation::Submit {
                    assignment_id: a.id.clone(),
                    temp_id: "s".into(),
                    artifacts: vec![Artifact {
                        uri: "artifact://proof/result".into(),
                        sha256: "a".repeat(64),
                    }],
                    evidence: vec!["tested".into()],
                }],
            ),
        )
        .unwrap();
    let board = f.board(10, None);
    assert_eq!(board.data.counts.testing, 1);
    assert_eq!(board.data.counts.done, 0);
    assert_eq!(board.data.rows[0].adopted_context, Some(f.context(&w)));
    let operation = Operation::Accept {
        work_id: w.clone(),
        submission_id: submitted.ids["s"].clone(),
        evidence: vec![],
    };
    assert!(f
        .service
        .execute(&f.authority, &f.command("no-evidence", vec![operation]))
        .is_err());
    let accept = Operation::Accept {
        work_id: w.clone(),
        submission_id: submitted.ids["s"].clone(),
        evidence: vec!["owner verified".into()],
    };
    let supervisor = Authority::from_verified_host(
        s.binding,
        Principal::Supervisor {
            session_id: "supervisor".into(),
        },
    );
    assert!(f
        .service
        .execute(&supervisor, &f.command("not-user", vec![accept.clone()]))
        .is_err());
    f.exec("accept", vec![accept]);
    let board = f.board(10, None);
    assert_eq!(board.data.counts.done, 1);
    assert_eq!(board.data.counts.testing, 0);
    assert_eq!(board.data.rows[0].adopted_context, Some(f.context(&w)));
}

#[test]
fn running_metadata_is_pending_with_cas_and_no_permission_or_input_changes() {
    let f = Fixture::new();
    let work = f.create("metadata", true);
    let (assignment, runtime, worker) = f.running(&work);
    let before = f.service.published().unwrap().1;
    let adopted = f.context(&work);
    let mut pending = adopted.clone();
    pending.project_revision += 1;
    pending.resource_revision += 1;
    let command = f.command(
        "metadata-pending",
        vec![Operation::SetProjectContext {
            work_id: work.clone(),
            context: pending.clone(),
        }],
    );
    let receipt = f.service.execute(&f.authority, &command).unwrap();
    assert_eq!(f.service.execute(&f.authority, &command).unwrap(), receipt);
    let mut conflict = command.clone();
    conflict.operation_id = "stale-metadata".into();
    assert!(matches!(
        f.service.execute(&f.authority, &conflict),
        Err(Error::RevisionConflict)
    ));
    let after = f.service.published().unwrap().1;
    assert_eq!(
        canonical_bytes(&after.assignments).unwrap(),
        canonical_bytes(&before.assignments).unwrap()
    );
    assert_eq!(
        canonical_bytes(&after.intents).unwrap(),
        canonical_bytes(&before.intents).unwrap()
    );
    assert_eq!(
        after.project.assignment_contexts,
        before.project.assignment_contexts
    );
    assert_eq!(after.schema, 5);
    let packet = f
        .service
        .child_context_packet(&worker, &assignment, 65536)
        .unwrap();
    assert_eq!(packet.project_context, Some(adopted.clone()));
    let row = &f.board(10, None).data.rows[0];
    assert_eq!(row.context, Some(pending.clone()));
    assert_eq!(row.pending_context, Some(pending.clone()));
    assert_eq!(row.adopted_context, Some(adopted));
    assert_eq!(row.stage, Some(ProjectBoardStage::Doing));
    for (name, context) in [
        ("source", {
            let mut c = pending.clone();
            c.sources.insert("requirements".into(), "revision-2".into());
            c
        }),
        ("binding", {
            let mut c = pending.clone();
            c.workspace_binding.path.push_str("/other");
            c
        }),
        ("tool", {
            let mut c = pending.clone();
            c.contract.allowed_tools.insert("Bash".into());
            c
        }),
        ("contract", {
            let mut c = pending.clone();
            c.contract_revision += 1;
            c
        }),
    ] {
        let old_commit = f.service.published().unwrap().0;
        assert!(
            f.service
                .execute(
                    &f.authority,
                    &f.command(
                        name,
                        vec![Operation::SetProjectContext {
                            work_id: work.clone(),
                            context,
                        }]
                    )
                )
                .is_err(),
            "{name}"
        );
        assert_eq!(f.service.published().unwrap().0, old_commit);
    }
    for authority in [&worker, &runtime] {
        let old_commit = f.service.published().unwrap().0;
        assert!(matches!(
            f.service.execute(
                authority,
                &f.command(
                    "no-worker-adoption",
                    vec![Operation::SetProjectContext {
                        work_id: work.clone(),
                        context: pending.clone()
                    }]
                )
            ),
            Err(Error::ScopeDenied(_))
        ));
        assert_eq!(f.service.published().unwrap().0, old_commit);
    }
    assert!(matches!(
        f.service.execute(
            &f.authority,
            &f.command(
                "no-regression",
                vec![Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: f.context(&work)
                }]
            )
        ),
        Err(Error::RevisionConflict)
    ));
}

#[test]
fn pending_context_survives_stop_and_restart_and_next_start_adopts_once() {
    let f = Fixture::new();
    let work = f.create("restart", true);
    let (assignment, runtime, _) = f.running(&work);
    let adopted = f.context(&work);
    let mut pending = adopted.clone();
    pending.project_revision += 1;
    pending.resource_revision += 1;
    f.exec(
        "queue",
        vec![Operation::SetProjectContext {
            work_id: work.clone(),
            context: pending.clone(),
        }],
    );
    f.exec(
        "pause-running",
        vec![Operation::Pause {
            work_id: work.clone(),
            reason: "safe metadata boundary".into(),
        }],
    );
    assert!(f
        .service
        .execute(
            &f.authority,
            &f.command(
                "premature-start",
                vec![Operation::Start {
                    work_id: work.clone(),
                    temp_id: "early".into(),
                    workspace: Some(f.execution()),
                }]
            )
        )
        .is_err());
    f.service
        .execute(
            &runtime,
            &f.command(
                "stopped",
                vec![Operation::ConfirmStopped {
                    assignment_id: assignment.clone(),
                    effects_reconciled: true,
                }],
            ),
        )
        .unwrap();
    let row = &f.board(10, None).data.rows[0];
    assert!(row.ticket.paused);
    assert!(row.ticket.active_assignment.is_none());
    assert_eq!(row.adopted_context, Some(adopted.clone()));
    assert_eq!(row.pending_context, Some(pending.clone()));
    let workspace = f.execution();
    let snapshot = f.service.published().unwrap().1;
    let adopted_hash = snapshot.project.assignment_contexts[&assignment].clone();
    drop(f.service);
    let service = TicketService::open(f.root.path(), snapshot.binding).unwrap();
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.project.contexts.len(), 2);
    assert_eq!(
        snapshot.project.assignment_contexts[&assignment],
        adopted_hash
    );
    assert!(snapshot.assignments[&assignment].process_stopped);
    let authority = Authority::from_verified_host(
        snapshot.binding.clone(),
        Principal::User {
            user_id: "owner".into(),
        },
    );
    let request = ProjectBoardRequest {
        project_id: project(),
        include_cancelled: false,
        include_archived: false,
        limit: 10,
        cursor: None,
        fixed_commit: None,
    };
    let row = &service
        .project_board(&authority, &request)
        .unwrap()
        .data
        .rows[0];
    assert_eq!(row.adopted_context, Some(adopted));
    assert_eq!(row.pending_context, Some(pending.clone()));
    let command = service
        .prepare_command(
            &authority,
            "next-start",
            vec![
                Operation::Ready {
                    work_id: work.clone(),
                },
                Operation::Start {
                    work_id: work.clone(),
                    temp_id: "next".into(),
                    workspace: Some(workspace),
                },
            ],
        )
        .unwrap();
    let receipt = service.execute(&authority, &command).unwrap();
    assert_eq!(service.execute(&authority, &command).unwrap(), receipt);
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 2);
    assert_eq!(
        snapshot.project.assignment_contexts[&assignment],
        adopted_hash
    );
    assert_eq!(
        snapshot.project.contexts[&snapshot.project.assignment_contexts[&receipt.ids["next"]]],
        pending
    );
    let row = &service
        .project_board(&authority, &request)
        .unwrap()
        .data
        .rows[0];
    assert!(row.pending_context.is_none());
    assert_eq!(row.adopted_context, Some(pending));
}

#[test]
fn metadata_only_pending_does_not_stale_historical_submission_or_user_acceptance() {
    let f = Fixture::new();
    let work = f.create("submission", true);
    let (assignment, _, worker) = f.running(&work);
    let adopted = f.context(&work);
    let mut pending = adopted.clone();
    pending.project_revision += 1;
    f.exec(
        "rename-pending",
        vec![Operation::SetProjectContext {
            work_id: work.clone(),
            context: pending.clone(),
        }],
    );
    let receipt = f
        .service
        .execute(
            &worker,
            &f.command(
                "submit-metadata",
                vec![Operation::Submit {
                    assignment_id: assignment.clone(),
                    temp_id: "result".into(),
                    artifacts: vec![Artifact {
                        uri: "artifact://proof/result".into(),
                        sha256: "a".repeat(64),
                    }],
                    evidence: vec!["tested adopted inputs".into()],
                }],
            ),
        )
        .unwrap();
    let snapshot = f.service.published().unwrap().1;
    assert!(!snapshot.submissions[&receipt.ids["result"]].stale);
    assert_eq!(
        snapshot.submissions[&receipt.ids["result"]].assignment_id,
        assignment
    );
    f.exec(
        "accept-metadata",
        vec![Operation::Accept {
            work_id: work,
            submission_id: receipt.ids["result"].clone(),
            evidence: vec!["owner verified".into()],
        }],
    );
    let row = &f.board(10, None).data.rows[0];
    assert_eq!(row.stage, Some(ProjectBoardStage::Done));
    assert_eq!(row.adopted_context, Some(adopted));
    assert_eq!(row.pending_context, Some(pending));
}
