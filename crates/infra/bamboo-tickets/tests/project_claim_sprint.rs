//! Private fixed-source integration evidence for context, claim CAS and Sprint.
use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_tickets::*;
use std::collections::{BTreeMap, BTreeSet};

struct Fixture {
    root: tempfile::TempDir,
    _workspace: tempfile::TempDir,
    service: TicketService,
    owner: Authority,
    work: String,
    context: ProjectContextSnapshot,
    workspace: ExecutionWorkspace,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let binding = ScopeBinding {
            scope_id: "combined".into(),
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
        let contract = Contract {
            title: "Combined Work".into(),
            objective: "One immutable execution".into(),
            constraints: vec!["No grant from planning".into()],
            acceptance: vec!["Evidence".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let path = directory
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let context = ProjectContextSnapshot {
            project_id: ProjectId::parse("combined-project").unwrap(),
            project_revision: 1,
            resource_revision: 1,
            workspace_binding: WorkspaceBinding {
                path: path.clone(),
                label: None,
                git_common_dir: None,
            },
            contract_revision: 1,
            contract: contract.clone(),
            sources: BTreeMap::from([("spec".into(), "v1".into())]),
        };
        let workspace = ExecutionWorkspace {
            repo: path.clone(),
            base_commit: "a".repeat(40),
            branch: "combined".into(),
            worktree: path.clone(),
            write_roots: vec![path.clone()],
            claims: BTreeSet::from([format!("worktree:{path}")]),
        };
        let command = service
            .prepare_command(
                &owner,
                "create",
                vec![
                    Operation::Create {
                        temp_id: "w".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract,
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "w".into(),
                        project_id: context.project_id.clone(),
                    },
                    Operation::SetProjectContext {
                        work_id: "w".into(),
                        context: context.clone(),
                    },
                    Operation::Ready {
                        work_id: "w".into(),
                    },
                ],
            )
            .unwrap();
        let work = service.execute(&owner, &command).unwrap().ids["w"].clone();
        Self {
            root,
            _workspace: directory,
            service,
            owner,
            work,
            context,
            workspace,
        }
    }
    fn claim(&self, id: &str) -> Command {
        let (commit, snapshot) = self.service.published().unwrap();
        match self
            .service
            .prepare_ready_claim(
                &self.owner,
                &SnapshotRef {
                    commit,
                    seq: snapshot.seq,
                    authority_epoch: snapshot.authority_epoch,
                },
                id,
                &[ClaimCandidate {
                    work_id: self.work.clone(),
                    workspace: Some(self.workspace.clone()),
                }],
                1,
            )
            .unwrap()
        {
            ClaimDecision::Prepared(command) => command,
            other => panic!("{other:?}"),
        }
    }
    fn metadata(&self, id: &str, revision: u64) {
        let mut context = self.context.clone();
        context.project_revision = revision;
        let command = self
            .service
            .prepare_command(
                &self.owner,
                id,
                vec![Operation::SetProjectContext {
                    work_id: self.work.clone(),
                    context,
                }],
            )
            .unwrap();
        self.service.execute(&self.owner, &command).unwrap();
    }
}

#[test]
fn newer_context_fences_prepared_claim_and_claim_replay_keeps_exact_adoption() {
    let f = Fixture::new();
    let stale = f.claim("before-context");
    f.metadata("metadata-before-start", 2);
    let before = f.service.published().unwrap().0;
    assert!(matches!(
        f.service.execute(&f.owner, &stale),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(f.service.published().unwrap().0, before);
    assert!(f.service.published().unwrap().1.assignments.is_empty());
    let command = f.claim("fresh-context");
    let receipt = f.service.execute(&f.owner, &command).unwrap();
    let id = &receipt.ids["assignment"];
    let started = f.service.published().unwrap().1;
    let hash = started.project.assignment_contexts[id].clone();
    assert_eq!(started.project.contexts[&hash].project_revision, 2);
    f.metadata("metadata-while-pending", 3);
    assert_eq!(f.service.execute(&f.owner, &command).unwrap(), receipt);
    let after = f.service.published().unwrap().1;
    assert_eq!(after.assignments.len(), 1);
    assert_eq!(after.project.assignment_contexts[id], hash);
    assert_eq!(
        canonical_bytes(&after.intents).unwrap(),
        canonical_bytes(&started.intents).unwrap()
    );
    assert_ne!(after.project.work_contexts[&f.work], hash);
}

#[test]
fn sprint_cas_shares_claim_fence_and_restart_keeps_pending_owner_and_context() {
    let f = Fixture::new();
    let stale = f.claim("before-sprint");
    let snapshot = f.service.published().unwrap().1;
    let sprint_id = SprintId::parse("combined-sprint").unwrap();
    let command = SprintCommand {
        operation_id: "create-sprint".into(),
        binding: snapshot.binding,
        expected_seq: snapshot.seq,
        expected_epoch: snapshot.authority_epoch,
        project_id: f.context.project_id.clone(),
        sprint_id: sprint_id.clone(),
        source: "combined-test".into(),
        action: SprintAction::Create {
            definition: SprintDefinition {
                name: "Combined".into(),
                goal: "No execution from planning".into(),
                starts_at: None,
                ends_at: None,
            },
        },
    };
    f.service.execute_sprint(&f.owner, &command).unwrap();
    assert!(matches!(
        f.service.execute(&f.owner, &stale),
        Err(Error::RevisionConflict)
    ));
    assert!(f.service.published().unwrap().1.assignments.is_empty());
    let claim = f.claim("after-sprint");
    let receipt = f.service.execute(&f.owner, &claim).unwrap();
    let id = receipt.ids["assignment"].clone();
    f.metadata("next-metadata", 2);
    let snapshot = f.service.published().unwrap().1;
    let hash = snapshot.project.assignment_contexts[&id].clone();
    drop(f.service);
    let service = TicketService::open(f.root.path(), snapshot.binding).unwrap();
    let (commit, snapshot) = service.published().unwrap();
    assert_eq!(snapshot.schema, 6);
    assert!(snapshot.sprint.sprints.contains_key(&sprint_id));
    assert_eq!(
        snapshot.assignments[&id].state,
        AssignmentState::OutcomeUnknown
    );
    assert_eq!(snapshot.project.assignment_contexts[&id], hash);
    let owner = Authority::from_verified_host(
        snapshot.binding.clone(),
        Principal::User {
            user_id: "owner".into(),
        },
    );
    assert!(matches!(
        service
            .prepare_ready_claim(
                &owner,
                &SnapshotRef {
                    commit,
                    seq: snapshot.seq,
                    authority_epoch: snapshot.authority_epoch
                },
                "not-another-owner",
                &[ClaimCandidate {
                    work_id: f.work.clone(),
                    workspace: Some(f.workspace)
                }],
                1
            )
            .unwrap(),
        ClaimDecision::CapacityBlocked {
            occupied: 1,
            capacity: 1
        }
    ));
    let board = service
        .project_board(
            &owner,
            &ProjectBoardRequest {
                project_id: f.context.project_id,
                include_cancelled: false,
                include_archived: false,
                limit: 10,
                cursor: None,
                fixed_commit: None,
            },
        )
        .unwrap();
    assert_eq!(
        board.data.rows[0]
            .adopted_context
            .as_ref()
            .unwrap()
            .project_revision,
        1
    );
    assert_eq!(
        board.data.rows[0]
            .pending_context
            .as_ref()
            .unwrap()
            .project_revision,
        2
    );
}
