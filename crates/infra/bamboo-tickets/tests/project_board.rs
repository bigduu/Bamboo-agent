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
    fn confirm_submission_stopped(&self, submission: &str) {
        let snapshot = self.service.published().unwrap().1;
        let assignment = &snapshot.assignments[&snapshot.submissions[submission].assignment_id];
        let runtime = Authority::from_verified_host(snapshot.binding.clone(), Principal::Runtime);
        self.service
            .execute(
                &runtime,
                &self.command(
                    &format!("fixture-stop-{submission}"),
                    vec![Operation::RuntimeStopped {
                        assignment_id: assignment.id.clone(),
                        receipt: assignment.runtime.clone().unwrap(),
                        completed: true,
                    }],
                ),
            )
            .unwrap();
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
                path: self
                    .workspace
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into(),
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
                    sprint_filter: None,
                    overdue_as_of: None,
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
fn review_context_adoption_rechecks_existing_answers_before_publication() {
    for replace_existing in [false, true] {
        let f = Fixture::new();
        let work = f.create("context-after-answer", true);
        if replace_existing {
            f.exec(
                "initial-context",
                vec![Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: f.context(&work),
                }],
            );
        }
        let request = f
            .exec(
                "context-capacity-question",
                vec![Operation::Ask {
                    work_id: work.clone(),
                    temp_id: "question".into(),
                    prompt: "Provide the bounded input".into(),
                    action: None,
                }],
            )
            .ids["question"]
            .clone();
        f.exec(
            "context-capacity-answer",
            vec![Operation::Answer {
                request_id: request.clone(),
                prompt_revision: 1,
                answer: "a".repeat(56000),
            }],
        );
        let before = f.service.published().unwrap();
        assert_eq!(before.1.requests[&request].status, RequestStatus::Answered);
        let mut oversized = f.context(&work);
        oversized.sources = BTreeMap::from([("large-source".into(), "v".repeat(12000))]);
        assert!(canonical_bytes(&oversized).unwrap().len() < 16 * 1024);
        let result = f.service.execute(
            &f.authority,
            &f.command(
                "oversized-context-after-answer",
                vec![Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: oversized,
                }],
            ),
        );
        assert!(
            matches!(result, Err(Error::ContextBudgetExceeded)),
            "Context adoption must include existing answers before publication (replace={replace_existing}): {result:?}"
        );
        assert_eq!(
            canonical_bytes(&f.service.published().unwrap()).unwrap(),
            canonical_bytes(&before).unwrap()
        );
        let mut bounded = f.context(&work);
        bounded.sources = BTreeMap::from([("bounded-source".into(), "v".repeat(1024))]);
        let command = f.command(
            "bounded-context-after-answer",
            vec![Operation::SetProjectContext {
                work_id: work.clone(),
                context: bounded.clone(),
            }],
        );
        let receipt = f.service.execute(&f.authority, &command).unwrap();
        let committed = f.service.published().unwrap();
        assert_eq!(f.service.execute(&f.authority, &command).unwrap(), receipt);
        assert_eq!(f.service.published().unwrap().0, committed.0);
        assert!(committed.1.assignments.is_empty());
        assert!(committed.1.project.assignment_contexts.is_empty());
        let started = f.exec(
            "context-after-answer-start",
            vec![
                Operation::Ready {
                    work_id: work.clone(),
                },
                Operation::Start {
                    work_id: work,
                    temp_id: "assignment".into(),
                    workspace: Some(f.execution()),
                },
            ],
        );
        let packet = f
            .service
            .child_context_packet(
                &f.authority,
                &started.ids["assignment"],
                WORK_CONTEXT_BYTES_LIMIT,
            )
            .unwrap();
        assert_eq!(packet.project_context, Some(bounded));
        assert_eq!(packet.answers.len(), 1);
        assert_eq!(packet.answers[0].answer.len(), 56000);
        assert!(canonical_bytes(&packet).unwrap().len() <= WORK_CONTEXT_BYTES_LIMIT);
    }
}

#[test]
fn review_answer_budget_includes_the_next_project_context_without_adopting_it() {
    let f = Fixture::new();
    let work = f.create("answer-context-budget", true);
    let mut context = f.context(&work);
    context.sources = BTreeMap::from([("large-source".into(), "v".repeat(12000))]);
    assert!(canonical_bytes(&context).unwrap().len() < 16 * 1024);
    f.exec(
        "answer-budget-context",
        vec![Operation::SetProjectContext {
            work_id: work.clone(),
            context: context.clone(),
        }],
    );
    let request = f
        .exec(
            "answer-budget-question",
            vec![Operation::Ask {
                work_id: work.clone(),
                temp_id: "question".into(),
                prompt: "Provide the bounded input".into(),
                action: None,
            }],
        )
        .ids["question"]
        .clone();
    let before = f.service.published().unwrap();
    let oversized = f.command(
        "answer-budget-oversized",
        vec![Operation::Answer {
            request_id: request.clone(),
            prompt_revision: 1,
            answer: "a".repeat(56000),
        }],
    );
    let result = f.service.execute(&f.authority, &oversized);
    assert!(
        matches!(result, Err(Error::ContextBudgetExceeded)),
        "Answer must include the known Project context in its complete next packet: {result:?}"
    );
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap()).unwrap(),
        canonical_bytes(&before).unwrap()
    );
    assert_eq!(before.1.requests[&request].status, RequestStatus::Open);
    assert!(before.1.project.assignment_contexts.is_empty());
    let bounded = f.command(
        "answer-budget-bounded",
        vec![Operation::Answer {
            request_id: request,
            prompt_revision: 1,
            answer: "short".into(),
        }],
    );
    let receipt = f.service.execute(&f.authority, &bounded).unwrap();
    let answered = f.service.published().unwrap();
    assert_eq!(f.service.execute(&f.authority, &bounded).unwrap(), receipt);
    assert_eq!(f.service.published().unwrap().0, answered.0);
    assert!(answered.1.assignments.is_empty());
    assert!(answered.1.project.assignment_contexts.is_empty());
    let start = f.exec(
        "answer-budget-start",
        vec![
            Operation::Ready {
                work_id: work.clone(),
            },
            Operation::Start {
                work_id: work,
                temp_id: "assignment".into(),
                workspace: Some(f.execution()),
            },
        ],
    );
    let packet = f
        .service
        .child_context_packet(
            &f.authority,
            &start.ids["assignment"],
            WORK_CONTEXT_BYTES_LIMIT,
        )
        .unwrap();
    assert_eq!(packet.project_context, Some(context));
    assert_eq!(packet.answers.len(), 1);
    assert_eq!(packet.answers[0].answer, "short");
    assert!(canonical_bytes(&packet).unwrap().len() <= WORK_CONTEXT_BYTES_LIMIT);
    assert!(!f
        .service
        .published()
        .unwrap()
        .1
        .project
        .assignment_contexts
        .contains_key("00000000-0000-0000-0000-000000000000"));
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
fn review_initial_candidate_context_is_pending_until_first_start() {
    let f = Fixture::new();
    let work = f.create("initial-context", true);
    let board = f.board(10, None);
    assert!(board.data.rows[0].context.is_none());
    assert!(board.data.rows[0].adopted_context.is_none());
    assert!(board.data.rows[0].pending_context.is_none());

    let context = f.context(&work);
    f.exec(
        "initial-candidate",
        vec![Operation::SetProjectContext {
            work_id: work.clone(),
            context: context.clone(),
        }],
    );
    let before_read = f.service.published().unwrap();
    let board = f.board(10, None);
    let row = &board.data.rows[0];
    assert_eq!(row.context.as_ref(), Some(&context));
    assert!(row.adopted_context.is_none());
    assert_eq!(
        row.pending_context.as_ref(),
        Some(&context),
        "a candidate without any actual adoption must be pending"
    );
    assert_eq!(f.service.published().unwrap().0, before_read.0);
    assert!(before_read.1.assignments.is_empty());

    f.exec(
        "first-start",
        vec![
            Operation::Ready {
                work_id: work.clone(),
            },
            Operation::Start {
                work_id: work,
                temp_id: "a".into(),
                workspace: Some(f.execution()),
            },
        ],
    );
    let board = f.board(10, None);
    assert_eq!(board.data.rows[0].adopted_context.as_ref(), Some(&context));
    assert!(board.data.rows[0].pending_context.is_none());
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
        sprint_filter: None,
        overdue_as_of: None,
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
                sprint_filter: None,
                overdue_as_of: None,
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
        sprint_filter: None,
        overdue_as_of: None,
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
    f.confirm_submission_stopped(&submitted.ids["s"]);
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
        sprint_filter: None,
        overdue_as_of: None,
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
    f.confirm_submission_stopped(&receipt.ids["result"]);
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

fn overdue_request(as_of: Option<&str>) -> ProjectBoardRequest {
    serde_json::from_value(serde_json::json!({
        "project_id": project(), "limit": 100, "overdue_as_of": as_of
    }))
    .unwrap()
}

fn overdue_deadline(f: &Fixture, work: &str, value: &str) {
    let snapshot = f.service.published().unwrap().1;
    f.exec(
        &format!("deadline-{}", snapshot.seq),
        vec![Operation::SetPlanningMetadata {
            ticket_id: work.into(),
            expected_record_revision: snapshot.tickets[work].record_revision,
            priority: bamboo_domain::TaskPriority::Medium,
            expected_completion_at: Some(value.into()),
        }],
    );
}

fn overdue_ids(board: &ReadEnvelope<ProjectBoard>) -> BTreeSet<String> {
    board
        .data
        .rows
        .iter()
        .map(|r| r.ticket.id.clone())
        .collect()
}

fn overdue_sprint(f: &Fixture, action: SprintAction) {
    let snapshot = f.service.published().unwrap().1;
    f.service
        .execute_sprint(
            &f.authority,
            &SprintCommand {
                operation_id: format!("overdue-sprint-{}", snapshot.seq),
                binding: snapshot.binding,
                expected_seq: snapshot.seq,
                expected_epoch: snapshot.authority_epoch,
                project_id: project(),
                sprint_id: SprintId::parse("overdue-sprint").unwrap(),
                source: "owner planning".into(),
                action,
            },
        )
        .unwrap();
}

#[test]
fn overdue_instant_offsets_and_terminal_states_are_read_only() {
    let f = Fixture::new();
    let late = f.create("late", true);
    let equal = f.create("equal-offset", true);
    let future = f.create("future", true);
    let undated = f.create("undated", true);
    let cancelled = f.create("cancelled-late", true);
    let archived = f.create("archived-late", true);
    let paused = f.create("paused-late", true);
    let done = f.create("accepted-late", true);
    let foreign = f.create("foreign-late", false);
    let unlinked = f.create("unlinked-late", false);
    f.exec(
        "foreign-project",
        vec![Operation::AssociateProject {
            ticket_id: foreign.clone(),
            project_id: ProjectId::parse("project-b").unwrap(),
        }],
    );
    for work in [
        &late, &cancelled, &archived, &paused, &done, &foreign, &unlinked,
    ] {
        overdue_deadline(&f, work, "2026-10-21T09:59:59Z");
    }
    overdue_deadline(&f, &equal, "2026-10-21T12:00:00+02:00");
    overdue_deadline(&f, &future, "2026-10-21T10:00:01Z");
    f.exec(
        "terminal-and-paused",
        vec![
            Operation::Cancel {
                work_id: cancelled.clone(),
            },
            Operation::Archive {
                ticket_id: archived.clone(),
                archived: true,
            },
            Operation::Pause {
                work_id: paused.clone(),
                reason: "owner pause".into(),
            },
        ],
    );
    let (assignment, _, worker) = f.running(&done);
    let receipt = f
        .service
        .execute(
            &worker,
            &f.command(
                "overdue-submit",
                vec![Operation::Submit {
                    assignment_id: assignment,
                    temp_id: "result".into(),
                    artifacts: vec![Artifact {
                        uri: "artifact://proof/result".into(),
                        sha256: "a".repeat(64),
                    }],
                    evidence: vec!["tested".into()],
                }],
            ),
        )
        .unwrap();
    f.confirm_submission_stopped(&receipt.ids["result"]);
    f.exec(
        "overdue-accept",
        vec![Operation::Accept {
            work_id: done.clone(),
            submission_id: receipt.ids["result"].clone(),
            evidence: vec!["owner verified".into()],
        }],
    );
    let before = canonical_bytes(&f.service.published().unwrap()).unwrap();
    let mut request = overdue_request(Some("2026-10-21T10:00:00Z"));
    request.include_cancelled = true;
    let board = f.service.project_board(&f.authority, &request).unwrap();
    assert_eq!(
        overdue_ids(&board),
        BTreeSet::from([late.clone(), paused.clone()])
    );
    assert_eq!(board.data.counts.total, 2);
    assert_eq!(board.data.counts.blocked, 1);
    assert_eq!(board.data.counts.done, 0);
    assert_eq!(board.data.counts.cancelled, 0);
    let paused_row = board
        .data
        .rows
        .iter()
        .find(|r| r.ticket.id == paused)
        .unwrap();
    assert!(paused_row.ticket.paused);
    assert_eq!(paused_row.stage, Some(ProjectBoardStage::Todo));
    request.overdue_as_of = Some("2026-10-21T14:00:00+04:00".into());
    assert_eq!(
        overdue_ids(&f.service.project_board(&f.authority, &request).unwrap()),
        overdue_ids(&board)
    );
    request.include_archived = true;
    assert_eq!(
        overdue_ids(&f.service.project_board(&f.authority, &request).unwrap()),
        BTreeSet::from([late.clone(), paused.clone(), archived])
    );
    let later = f
        .service
        .project_board(&f.authority, &overdue_request(Some("2026-10-21T10:00:01Z")))
        .unwrap();
    assert_eq!(overdue_ids(&later), BTreeSet::from([late, paused, equal]));
    assert!(!overdue_ids(&later).contains(&undated));
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap()).unwrap(),
        before
    );
}

#[test]
fn overdue_cursor_pins_literal_instant_and_snapshot_during_metadata_updates() {
    let f = Fixture::new();
    let a = f.create("page-a", true);
    let b = f.create("page-b", true);
    let future = f.create("page-future", true);
    overdue_deadline(&f, &a, "2026-10-21T09:00:00Z");
    overdue_deadline(&f, &b, "2026-10-21T09:00:00Z");
    overdue_deadline(&f, &future, "2026-10-21T11:00:00Z");
    let mut request = overdue_request(Some("2026-10-21T10:00:00Z"));
    request.limit = 1;
    let first = f.service.project_board(&f.authority, &request).unwrap();
    assert_eq!(first.data.counts.total, 2);
    let cursor = first.next_cursor.clone().unwrap();
    overdue_deadline(&f, &a, "2026-10-21T12:00:00Z");
    let added = f.create("page-added", true);
    overdue_deadline(&f, &added, "2026-10-21T09:00:00Z");
    request.cursor = Some(cursor);
    request.limit = 100;
    let second = f.service.project_board(&f.authority, &request).unwrap();
    assert_eq!(second.snapshot.commit, first.snapshot.commit);
    assert_eq!(second.data.counts.total, 2);
    assert_eq!(
        overdue_ids(&first)
            .union(&overdue_ids(&second))
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([a, b.clone()])
    );
    let fresh = f
        .service
        .project_board(&f.authority, &overdue_request(Some("2026-10-21T10:00:00Z")))
        .unwrap();
    assert_eq!(overdue_ids(&fresh), BTreeSet::from([b, added]));
    for as_of in [
        None,
        Some("2026-10-21T10:00:00+00:00"),
        Some("2026-10-21T11:00:00Z"),
    ] {
        let mut changed = request.clone();
        changed.overdue_as_of = as_of.map(str::to_owned);
        assert!(matches!(
            f.service.project_board(&f.authority, &changed),
            Err(Error::ResyncRequired)
        ));
    }
    request.fixed_commit = Some(f.service.published().unwrap().0);
    assert!(matches!(
        f.service.project_board(&f.authority, &request),
        Err(Error::ResyncRequired)
    ));
}

#[test]
fn overdue_rejects_unbounded_unknown_or_invalid_instants_without_publication() {
    let f = Fixture::new();
    let work = f.create("validation", true);
    overdue_deadline(&f, &work, "2026-10-21T09:00:00Z");
    let before = canonical_bytes(&f.service.published().unwrap()).unwrap();
    let too_long = "x".repeat(65);
    for invalid in [
        "2026-10-21",
        "2026-10-21T10:00:00",
        "2026-10-21T10:00:00-00:00",
        "2026-02-30T10:00:00Z",
        "2026-10-21 10:00:00Z",
        "２０２６-10-21T10:00:00Z",
        too_long.as_str(),
    ] {
        assert!(matches!(
            f.service
                .project_board(&f.authority, &overdue_request(Some(invalid))),
            Err(Error::InvalidTransition(_))
        ));
    }
    let runtime =
        Authority::from_verified_host(f.service.published().unwrap().1.binding, Principal::Runtime);
    assert!(matches!(
        f.service
            .project_board(&runtime, &overdue_request(Some("invalid"))),
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap()).unwrap(),
        before
    );
}

#[test]
fn overdue_omission_preserves_legacy_cursors_and_intersects_sprint_membership() {
    let f = Fixture::new();
    let member = f.create("sprint-late", true);
    let future = f.create("sprint-future", true);
    let backlog = f.create("backlog-late", true);
    overdue_deadline(&f, &member, "2026-10-21T09:00:00Z");
    overdue_deadline(&f, &future, "2026-10-21T11:00:00Z");
    overdue_deadline(&f, &backlog, "2026-10-21T09:00:00Z");
    let legacy: ProjectBoardRequest =
        serde_json::from_value(serde_json::json!({"project_id":project(),"limit":1})).unwrap();
    assert!(legacy.overdue_as_of.is_none());
    assert!(serde_json::to_value(&legacy)
        .unwrap()
        .get("overdue_as_of")
        .is_none());
    let first = f.service.project_board(&f.authority, &legacy).unwrap();
    let expected_hash =
        content_hash(&canonical_bytes(&("project-board-v1", project(), false, false)).unwrap());
    assert_eq!(
        first.next_cursor.as_ref().unwrap().query_hash,
        expected_hash
    );
    let mut null = serde_json::to_value(&legacy).unwrap();
    null["overdue_as_of"] = serde_json::Value::Null;
    let null: ProjectBoardRequest = serde_json::from_value(null).unwrap();
    assert_eq!(
        canonical_bytes(&f.service.project_board(&f.authority, &null).unwrap()).unwrap(),
        canonical_bytes(&first).unwrap()
    );
    overdue_sprint(
        &f,
        SprintAction::Create {
            definition: SprintDefinition {
                name: "overdue".into(),
                goal: "review".into(),
                starts_at: None,
                ends_at: None,
            },
        },
    );
    for work in [&member, &future] {
        let revision = f.service.published().unwrap().1.sprint.sprints
            [&SprintId::parse("overdue-sprint").unwrap()]
            .revision;
        overdue_sprint(
            &f,
            SprintAction::Add {
                expected_revision: revision,
                work_id: work.clone(),
            },
        );
    }
    let mut request = overdue_request(Some("2026-10-21T10:00:00Z"));
    request.sprint_filter = Some(SprintBoardFilter::Sprint {
        sprint_id: SprintId::parse("overdue-sprint").unwrap(),
    });
    let board = f.service.project_board(&f.authority, &request).unwrap();
    assert_eq!(overdue_ids(&board), BTreeSet::from([member]));
    assert_eq!(board.data.counts.total, 1);
    request.sprint_filter = Some(SprintBoardFilter::Backlog {});
    assert_eq!(
        overdue_ids(&f.service.project_board(&f.authority, &request).unwrap()),
        BTreeSet::from([backlog])
    );
    request.sprint_filter = Some(SprintBoardFilter::Sprint {
        sprint_id: SprintId::parse("overdue-sprint").unwrap(),
    });
    request.project_id = ProjectId::parse("other-project").unwrap();
    assert!(matches!(
        f.service.project_board(&f.authority, &request),
        Err(Error::ScopeDenied(_))
    ));
}
