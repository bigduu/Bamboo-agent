use std::collections::{BTreeMap, BTreeSet};

use bamboo_domain::ProjectId;
use bamboo_tickets::*;

struct Fixture {
    _root: tempfile::TempDir,
    service: TicketService,
    owner: Authority,
}

fn project() -> ProjectId {
    ProjectId::parse("project-a").unwrap()
}
fn sprint(id: &str) -> SprintId {
    SprintId::parse(id).unwrap()
}
fn filter(id: &str) -> SprintBoardFilter {
    SprintBoardFilter::Sprint {
        sprint_id: sprint(id),
    }
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let binding = ScopeBinding {
            scope_id: "scope".into(),
            supervisor_session_id: "supervisor".into(),
            binding_revision: 1,
        };
        let service = TicketService::open(root.path(), binding.clone()).unwrap();
        let owner = Authority::from_verified_host(
            binding,
            Principal::User {
                user_id: "owner".into(),
            },
        );
        Self {
            _root: root,
            service,
            owner,
        }
    }
    fn work_ops(&self, operations: Vec<Operation>) -> OperationReceipt {
        let seq = self.service.published().unwrap().1.seq;
        let command = self
            .service
            .prepare_command(&self.owner, &format!("work-{seq}"), operations)
            .unwrap();
        self.service.execute(&self.owner, &command).unwrap()
    }
    fn work(&self, title: &str, project_id: Option<ProjectId>) -> String {
        let mut ops = vec![Operation::Create {
            temp_id: "w".into(),
            kind: TicketKind::Work,
            parent: None,
            depends_on: BTreeSet::new(),
            contract: Contract {
                title: title.into(),
                objective: "deliver".into(),
                acceptance: vec!["review".into()],
                constraints: vec![],
                allowed_tools: BTreeSet::from(["Task".into()]),
                user_acceptance_required: true,
            },
        }];
        if let Some(project_id) = project_id {
            ops.push(Operation::AssociateProject {
                ticket_id: "w".into(),
                project_id,
            });
        }
        self.work_ops(ops).ids["w"].clone()
    }
    fn revision(&self, id: &str) -> u64 {
        self.service.published().unwrap().1.sprint.sprints[&sprint(id)].revision
    }
    fn execute(&self, project_id: ProjectId, id: &str, action: SprintAction) -> SprintReceipt {
        let s = self.service.published().unwrap().1;
        self.service
            .execute_sprint(
                &self.owner,
                &SprintCommand {
                    operation_id: format!("sprint-{}", s.seq),
                    binding: s.binding,
                    expected_seq: s.seq,
                    expected_epoch: s.authority_epoch,
                    project_id,
                    sprint_id: sprint(id),
                    source: "owner planning".into(),
                    action,
                },
            )
            .unwrap()
    }
    fn create(&self, id: &str) {
        self.execute(
            project(),
            id,
            SprintAction::Create {
                definition: SprintDefinition {
                    name: id.into(),
                    goal: "deliver reviewed Work".into(),
                    starts_at: None,
                    ends_at: None,
                },
            },
        );
    }
    fn add(&self, id: &str, work: &str) {
        self.execute(
            project(),
            id,
            SprintAction::Add {
                expected_revision: self.revision(id),
                work_id: work.into(),
            },
        );
    }
    fn request(&self, sprint_filter: Option<SprintBoardFilter>) -> ProjectBoardRequest {
        ProjectBoardRequest {
            project_id: project(),
            include_cancelled: false,
            include_archived: false,
            sprint_filter,
            limit: 100,
            cursor: None,
            fixed_commit: None,
        }
    }
    fn board(&self, filter: Option<SprintBoardFilter>) -> ReadEnvelope<ProjectBoard> {
        self.service
            .project_board(&self.owner, &self.request(filter))
            .unwrap()
    }
    fn list_request(&self) -> SprintListRequest {
        SprintListRequest {
            project_id: project(),
            status: None,
            limit: 100,
            cursor: None,
            fixed_commit: None,
        }
    }
}

fn ids(board: &ReadEnvelope<ProjectBoard>) -> BTreeSet<String> {
    board
        .data
        .rows
        .iter()
        .map(|r| r.ticket.id.clone())
        .collect()
}

#[test]
fn omitted_filter_preserves_legacy_payload_cursor_identity_and_read_snapshot() {
    let f = Fixture::new();
    let expected: BTreeSet<_> = (0..3)
        .map(|n| f.work(&format!("w{n}"), Some(project())))
        .collect();
    let request: ProjectBoardRequest =
        serde_json::from_value(serde_json::json!({"project_id":project(),"limit":1})).unwrap();
    assert!(request.sprint_filter.is_none());
    assert!(serde_json::to_value(&request)
        .unwrap()
        .get("sprint_filter")
        .is_none());
    let first = f.service.project_board(&f.owner, &request).unwrap();
    let old_hash =
        content_hash(&canonical_bytes(&("project-board-v1", project(), false, false)).unwrap());
    assert_eq!(first.next_cursor.as_ref().unwrap().query_hash, old_hash);
    f.work("later", Some(project()));
    let next = ProjectBoardRequest {
        limit: 100,
        cursor: first.next_cursor.clone(),
        ..request
    };
    let second = f.service.project_board(&f.owner, &next).unwrap();
    assert_eq!(second.snapshot.commit, first.snapshot.commit);
    assert_eq!(second.data.counts.total, 3);
    assert_eq!(
        ids(&first)
            .union(&ids(&second))
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected
    );
    let mut null = serde_json::to_value(&next).unwrap();
    null["sprint_filter"] = serde_json::Value::Null;
    let null = serde_json::from_value(null).unwrap();
    assert_eq!(
        canonical_bytes(&f.service.project_board(&f.owner, &null).unwrap()).unwrap(),
        canonical_bytes(&second).unwrap()
    );
}

#[test]
fn membership_filters_keep_project_scope_counts_and_existing_cancel_archive_policy() {
    let f = Fixture::new();
    let member = f.work("member", Some(project()));
    let backlog = f.work("backlog", Some(project()));
    let cancelled = f.work("cancelled", Some(project()));
    let archived = f.work("archived", Some(project()));
    f.work("foreign", Some(ProjectId::parse("project-b").unwrap()));
    f.work("unlinked", None);
    f.work_ops(vec![
        Operation::Cancel {
            work_id: cancelled.clone(),
        },
        Operation::Archive {
            ticket_id: archived.clone(),
            archived: true,
        },
    ]);
    f.create("planned");
    f.add("planned", &member);
    assert_eq!(
        ids(&f.board(Some(filter("planned")))),
        BTreeSet::from([member])
    );
    let board = f.board(Some(SprintBoardFilter::Backlog {}));
    assert_eq!(board.data.counts.total, 2);
    assert_eq!(board.data.counts.todo, 1);
    assert_eq!(board.data.counts.cancelled, 1);
    assert_eq!(board.data.counts.done, 0);
    assert_eq!(ids(&board), BTreeSet::from([backlog.clone()]));
    let mut request = f.request(Some(SprintBoardFilter::Backlog {}));
    request.include_archived = true;
    request.include_cancelled = true;
    let board = f.service.project_board(&f.owner, &request).unwrap();
    assert_eq!(board.data.counts.total, 3);
    assert_eq!(ids(&board), BTreeSet::from([backlog, cancelled, archived]));
    assert!(board
        .data
        .rows
        .iter()
        .all(|r| r.stage != Some(ProjectBoardStage::Done)));
}

#[test]
fn board_paging_freezes_membership_and_rejects_changed_filter_project_or_snapshot() {
    let f = Fixture::new();
    let works: BTreeSet<_> = (0..3)
        .map(|n| f.work(&format!("w{n}"), Some(project())))
        .collect();
    f.create("a");
    f.create("b");
    for work in &works {
        f.add("a", work);
    }
    let mut request = f.request(Some(filter("a")));
    request.limit = 1;
    let first = f.service.project_board(&f.owner, &request).unwrap();
    let removed = works.first().unwrap();
    f.execute(
        project(),
        "a",
        SprintAction::Remove {
            expected_revision: f.revision("a"),
            work_id: removed.clone(),
        },
    );
    f.add("b", removed);
    request.cursor = first.next_cursor.clone();
    request.limit = 100;
    let second = f.service.project_board(&f.owner, &request).unwrap();
    assert_eq!(second.snapshot.commit, first.snapshot.commit);
    assert_eq!(second.data.counts.total, 3);
    assert!(ids(&first).is_disjoint(&ids(&second)));
    assert_eq!(
        ids(&first)
            .union(&ids(&second))
            .cloned()
            .collect::<BTreeSet<_>>(),
        works
    );
    assert_eq!(f.board(Some(filter("a"))).data.counts.total, 2);
    for filter in [None, Some(filter("b")), Some(SprintBoardFilter::Backlog {})] {
        let mut changed = request.clone();
        changed.sprint_filter = filter;
        assert!(matches!(
            f.service.project_board(&f.owner, &changed),
            Err(Error::ResyncRequired)
        ));
    }
    let mut changed = request.clone();
    changed.project_id = ProjectId::parse("other").unwrap();
    assert!(matches!(
        f.service.project_board(&f.owner, &changed),
        Err(Error::ResyncRequired)
    ));
    let mut changed = request.clone();
    changed.include_archived = true;
    assert!(matches!(
        f.service.project_board(&f.owner, &changed),
        Err(Error::ResyncRequired)
    ));
    request.fixed_commit = Some(f.service.published().unwrap().0);
    assert!(matches!(
        f.service.project_board(&f.owner, &request),
        Err(Error::ResyncRequired)
    ));
}

#[test]
fn close_releases_backlog_and_closed_filter_keeps_history_separate_from_current_work() {
    let f = Fixture::new();
    let work = f.work("w", Some(project()));
    f.create("a");
    f.create("next");
    f.add("a", &work);
    f.execute(
        project(),
        "a",
        SprintAction::Start {
            expected_revision: f.revision("a"),
        },
    );
    f.execute(
        project(),
        "a",
        SprintAction::Close {
            expected_revision: f.revision("a"),
            dispositions: BTreeMap::from([(work.clone(), SprintDestination::Backlog)]),
            target_revisions: BTreeMap::new(),
        },
    );
    assert_eq!(
        ids(&f.board(Some(SprintBoardFilter::Backlog {}))),
        BTreeSet::from([work.clone()])
    );
    let closed_commit = f.service.published().unwrap().0;
    f.add("next", &work);
    f.work_ops(vec![Operation::Cancel {
        work_id: work.clone(),
    }]);
    assert_eq!(
        f.board(Some(SprintBoardFilter::Backlog {}))
            .data
            .counts
            .total,
        0
    );
    let before = f.service.published().unwrap().0;
    let view = f
        .service
        .inspect_sprint(
            &f.owner,
            &SprintInspectRequest {
                project_id: project(),
                sprint_id: sprint("a"),
                fixed_commit: None,
            },
        )
        .unwrap();
    assert_eq!(view.data.sprint.closure.unwrap().counts.unfinished, 1);
    assert_eq!(view.data.current_counts.cancelled, 1);
    let board = f.board(Some(filter("a")));
    assert_eq!(board.data.counts.cancelled, 1);
    assert_eq!(board.data.counts.done, 0);
    assert!(board.data.rows.is_empty());
    let mut request = f.request(Some(filter("a")));
    request.fixed_commit = Some(closed_commit);
    let historical = f.service.project_board(&f.owner, &request).unwrap();
    assert_eq!(historical.data.counts.todo, 1);
    assert_eq!(ids(&historical), BTreeSet::from([work]));
    assert_eq!(
        f.service.published().unwrap().0,
        before,
        "reads must not publish"
    );
}

#[test]
fn sprint_list_is_bounded_project_scoped_and_snapshot_bound() {
    let f = Fixture::new();
    f.work("w", Some(project()));
    for id in ["a", "b", "c"] {
        f.create(id);
    }
    let other = ProjectId::parse("project-b").unwrap();
    f.work("other-project-work", Some(other.clone()));
    f.execute(
        other,
        "0-foreign",
        SprintAction::Create {
            definition: SprintDefinition {
                name: "Foreign".into(),
                goal: "separate Project".into(),
                starts_at: None,
                ends_at: None,
            },
        },
    );
    f.execute(
        project(),
        "a",
        SprintAction::Start {
            expected_revision: 1,
        },
    );
    let mut request = f.list_request();
    request.status = Some(SprintStatus::Planned);
    request.limit = 1;
    let first = f.service.list_sprints(&f.owner, &request).unwrap();
    assert_eq!(first.data[0].id, sprint("b"));
    assert_eq!(first.omitted_count, 1);
    assert!(first.truncated);
    let summary = serde_json::to_value(&first.data[0]).unwrap();
    assert!(summary.get("members").is_none());
    assert!(summary.get("history").is_none());
    f.create("d");
    request.cursor = first.next_cursor;
    request.limit = 100;
    let second = f.service.list_sprints(&f.owner, &request).unwrap();
    assert_eq!(second.snapshot.commit, first.snapshot.commit);
    assert_eq!(second.data.len(), 1);
    assert_eq!(second.data[0].id, sprint("c"));
    assert!(!second.truncated);
    let mut changed = request.clone();
    changed.status = None;
    assert!(matches!(
        f.service.list_sprints(&f.owner, &changed),
        Err(Error::ResyncRequired)
    ));
    let mut changed = request.clone();
    changed.project_id = ProjectId::parse("other").unwrap();
    assert!(matches!(
        f.service.list_sprints(&f.owner, &changed),
        Err(Error::ResyncRequired)
    ));
    request.cursor = None;
    request.fixed_commit = Some(first.snapshot.commit);
    assert_eq!(
        f.service
            .list_sprints(&f.owner, &request)
            .unwrap()
            .data
            .len(),
        2
    );
    for limit in [0, 101] {
        request.limit = limit;
        assert!(matches!(
            f.service.list_sprints(&f.owner, &request),
            Err(Error::InvalidTransition(_))
        ));
    }
}

#[test]
fn filtered_reads_reject_foreign_sprints_roles_bindings_and_missing_fixed_snapshots() {
    let f = Fixture::new();
    f.work("w", Some(project()));
    f.create("a");
    let mut request = f.request(Some(filter("a")));
    request.project_id = ProjectId::parse("other").unwrap();
    assert!(matches!(
        f.service.project_board(&f.owner, &request),
        Err(Error::ScopeDenied(_))
    ));
    request = f.request(Some(filter("missing")));
    assert!(matches!(
        f.service.project_board(&f.owner, &request),
        Err(Error::InvalidTransition(_))
    ));
    let binding = f.service.published().unwrap().1.binding;
    for principal in [
        Principal::Runtime,
        Principal::Worker {
            assignment_id: "missing".into(),
            generation: 1,
            run_id: "run".into(),
            session_id: "worker".into(),
        },
    ] {
        let authority = Authority::from_verified_host(binding.clone(), principal);
        assert!(matches!(
            f.service.list_sprints(&authority, &f.list_request()),
            Err(Error::ScopeDenied(_))
        ));
        assert!(matches!(
            f.service
                .project_board(&authority, &f.request(Some(filter("a")))),
            Err(Error::ScopeDenied(_))
        ));
    }
    let mut foreign = binding;
    foreign.binding_revision += 1;
    let authority = Authority::from_verified_host(
        foreign,
        Principal::User {
            user_id: "owner".into(),
        },
    );
    assert!(matches!(
        f.service.list_sprints(&authority, &f.list_request()),
        Err(Error::ScopeDenied(_))
    ));
    let mut list = f.list_request();
    list.fixed_commit = Some("f".repeat(64));
    assert!(matches!(
        f.service.list_sprints(&f.owner, &list),
        Err(Error::ResyncRequired)
    ));
    request = f.request(Some(SprintBoardFilter::Backlog {}));
    request.fixed_commit = list.fixed_commit;
    assert!(matches!(
        f.service.project_board(&f.owner, &request),
        Err(Error::ResyncRequired)
    ));
    for unknown in [
        serde_json::json!({"kind":"backlog","sprint_id":"a"}),
        serde_json::json!({"kind":"done"}),
    ] {
        assert!(serde_json::from_value::<SprintBoardFilter>(unknown).is_err());
    }
}

#[test]
fn legacy_snapshot_has_empty_sprint_list_and_unchanged_backlog_without_mutation() {
    let f = Fixture::new();
    let work = f.work("w", Some(project()));
    let legacy = f.service.published().unwrap();
    assert_eq!(legacy.1.schema, 5);
    f.create("a");
    f.add("a", &work);
    let before = f.service.published().unwrap().0;
    let mut request = f.request(Some(SprintBoardFilter::Backlog {}));
    request.fixed_commit = Some(legacy.0.clone());
    assert_eq!(
        ids(&f.service.project_board(&f.owner, &request).unwrap()),
        BTreeSet::from([work])
    );
    let mut list = f.list_request();
    list.fixed_commit = Some(legacy.0);
    assert!(f
        .service
        .list_sprints(&f.owner, &list)
        .unwrap()
        .data
        .is_empty());
    assert_eq!(f.service.published().unwrap().0, before);
}
