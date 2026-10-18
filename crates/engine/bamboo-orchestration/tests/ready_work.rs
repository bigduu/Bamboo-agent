use async_trait::async_trait;
use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_orchestration::{
    ReadyBlockKind, ReadyDispatch, ReadyReconcile, ReadyWorkGrant, ReadyWorkHost, ReadyWorkPolicy,
    ReadyWorkScheduler,
};
use bamboo_tickets::*;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

const ROUNDS: u32 = 4;

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        static SERIAL: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "bamboo-orchestration-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "ready-work-scope".into(),
        supervisor_session_id: "fixture-supervisor".into(),
        binding_revision: 1,
    }
}

fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: format!("Deliver {title}"),
        constraints: vec!["Stay within the Host grant".into()],
        acceptance: vec!["Human checks the evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}

#[derive(Clone, Copy)]
enum PortError {
    Unavailable,
    Io,
}

impl PortError {
    fn error(self) -> Error {
        match self {
            Self::Unavailable => Error::AuthorityUnavailable("controlled Host outage".into()),
            Self::Io => std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "controlled transient I/O error",
            )
            .into(),
        }
    }
}

struct Host {
    service: Mutex<Option<Arc<TicketService>>>,
    authority_calls: AtomicUsize,
    budget_calls: AtomicUsize,
    budget_enabled: AtomicBool,
    preflight_delay_ms: AtomicU64,
    preflights: Mutex<Vec<String>>,
    dispatches: Mutex<Vec<Command>>,
    ensures: Mutex<Vec<String>>,
    ceilings: Mutex<Vec<u32>>,
    outcomes: Mutex<VecDeque<ReadyBlockKind>>,
    dispatch_errors: Mutex<VecDeque<PortError>>,
    ensure_errors: Mutex<VecDeque<PortError>>,
    error_after_commit: AtomicBool,
    conflict_once: AtomicBool,
}

impl Host {
    fn service(&self) -> Arc<TicketService> {
        self.service.lock().unwrap().as_ref().unwrap().clone()
    }
    fn verified_authority(&self) -> Authority {
        Authority::from_verified_host(
            self.service().published().unwrap().1.binding,
            Principal::Supervisor {
                session_id: binding().supervisor_session_id,
            },
        )
    }
    fn next_outcome(&self, key: &str, max_rounds: u32) -> ReadyDispatch {
        self.ceilings.lock().unwrap().push(max_rounds);
        match self.outcomes.lock().unwrap().pop_front() {
            Some(kind) => ReadyDispatch::Blocked {
                kind,
                reason: format!("controlled {kind:?} outcome"),
            },
            None => ReadyDispatch::Observed {
                dispatch_key: key.into(),
            },
        }
    }
    fn restart_writer(&self, root: &Path) {
        let old = self.service.lock().unwrap().take().unwrap();
        drop(old);
        *self.service.lock().unwrap() =
            Some(Arc::new(TicketService::open(root, binding()).unwrap()));
    }
}

#[async_trait]
impl ReadyWorkHost for Host {
    async fn authority(&self) -> Result<(Arc<TicketService>, Authority)> {
        self.authority_calls.fetch_add(1, Ordering::SeqCst);
        Ok((self.service(), self.verified_authority()))
    }

    async fn check_budget(&self, max_rounds: u32) -> Result<()> {
        self.budget_calls.fetch_add(1, Ordering::SeqCst);
        if !self.budget_enabled.load(Ordering::SeqCst) || max_rounds != ROUNDS {
            return Err(Error::ResourceBlocked(
                "finite Host budget unavailable".into(),
            ));
        }
        Ok(())
    }

    async fn preflight(&self, command: &Command) -> Result<()> {
        let Operation::Start {
            work_id,
            workspace: Some(workspace),
            ..
        } = &command.operations[0]
        else {
            return Err(Error::InvalidTransition(
                "explicit workspace required".into(),
            ));
        };
        self.preflights.lock().unwrap().push(work_id.clone());
        let delay = self.preflight_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if !Path::new(&workspace.worktree).is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "controlled missing workspace",
            )
            .into());
        }
        Ok(())
    }

    async fn ensure(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
    ) -> Result<ReadyDispatch> {
        self.ensures.lock().unwrap().push(key.into());
        assert_eq!(
            self.service().published()?.1.intents[key].immutable_spec,
            *spec
        );
        if let Some(error) = self.ensure_errors.lock().unwrap().pop_front() {
            return Err(error.error());
        }
        Ok(self.next_outcome(key, max_rounds))
    }

    async fn dispatch(&self, command: &Command, max_rounds: u32) -> Result<ReadyDispatch> {
        self.dispatches.lock().unwrap().push(command.clone());
        let after_commit = self.error_after_commit.load(Ordering::SeqCst);
        if !after_commit {
            if let Some(error) = self.dispatch_errors.lock().unwrap().pop_front() {
                return Err(error.error());
            }
        }
        let service = self.service();
        let authority = self.verified_authority();
        if self.conflict_once.swap(false, Ordering::SeqCst) {
            let change = service.prepare_command(
                &authority,
                "controlled-intervening-scope-write",
                vec![Operation::Create {
                    temp_id: "unrelated".into(),
                    kind: TicketKind::Goal,
                    parent: None,
                    contract: contract("Scope changed"),
                    depends_on: BTreeSet::new(),
                }],
            )?;
            service.execute(&authority, &change)?;
        }
        let receipt = service.execute(&authority, command)?;
        if after_commit {
            if let Some(error) = self.dispatch_errors.lock().unwrap().pop_front() {
                return Err(error.error());
            }
        }
        let snapshot = service.published()?.1;
        let key = &snapshot.assignments[&receipt.ids["assignment"]].dispatch_key;
        Ok(self.next_outcome(key, max_rounds))
    }
}

struct Fixture {
    host: Arc<Host>,
    grants: Vec<ReadyWorkGrant>,
    project: ProjectId,
    root: TestRoot,
}

impl Fixture {
    fn new(count: usize, ready: bool) -> Self {
        let root = TestRoot::new();
        let service = Arc::new(TicketService::open(root.0.join("ledger"), binding()).unwrap());
        let host = Arc::new(Host {
            service: Mutex::new(Some(service)),
            authority_calls: AtomicUsize::new(0),
            budget_calls: AtomicUsize::new(0),
            budget_enabled: AtomicBool::new(true),
            preflight_delay_ms: AtomicU64::new(0),
            preflights: Mutex::new(Vec::new()),
            dispatches: Mutex::new(Vec::new()),
            ensures: Mutex::new(Vec::new()),
            ceilings: Mutex::new(Vec::new()),
            outcomes: Mutex::new(VecDeque::new()),
            dispatch_errors: Mutex::new(VecDeque::new()),
            ensure_errors: Mutex::new(VecDeque::new()),
            error_after_commit: AtomicBool::new(false),
            conflict_once: AtomicBool::new(false),
        });
        let project = ProjectId::parse("ready-work-project").unwrap();
        let mut fixture = Self {
            host,
            grants: Vec::new(),
            project,
            root,
        };
        for index in 0..count {
            fixture.add_work(index, ready);
        }
        fixture
    }

    fn add_work(&mut self, index: usize, ready: bool) {
        let repo = self.root.0.join(format!("repo-{index}"));
        let tree = self.root.0.join(format!("tree-{index}"));
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&tree).unwrap();
        let repo = repo.to_string_lossy().into_owned();
        let tree = tree.to_string_lossy().into_owned();
        let workspace = ExecutionWorkspace {
            repo: repo.clone(),
            base_commit: "a".repeat(40),
            branch: format!("work-{index}"),
            worktree: tree.clone(),
            write_roots: vec![tree.clone()],
            claims: BTreeSet::from([format!("worktree:{tree}")]),
        };
        let service = self.host.service();
        let authority = self.host.verified_authority();
        let create = service
            .prepare_command(
                &authority,
                &format!("create-{index}"),
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract(&format!("Work {index}")),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "work".into(),
                        project_id: self.project.clone(),
                    },
                ],
            )
            .unwrap();
        let id = service.execute(&authority, &create).unwrap().ids["work"].clone();
        let work = service.published().unwrap().1.tickets[&id].clone();
        let mut operations = vec![Operation::SetProjectContext {
            work_id: id.clone(),
            context: ProjectContextSnapshot {
                project_id: self.project.clone(),
                project_revision: 1,
                resource_revision: 1,
                workspace_binding: WorkspaceBinding {
                    path: repo,
                    label: None,
                    git_common_dir: None,
                },
                contract_revision: work.contract_revision,
                contract: work.contract,
                sources: BTreeMap::from([("spec".into(), "v1".into())]),
            },
        }];
        if ready {
            operations.push(Operation::Ready {
                work_id: id.clone(),
            });
        }
        let adopt = service
            .prepare_command(&authority, &format!("adopt-{index}"), operations)
            .unwrap();
        service.execute(&authority, &adopt).unwrap();
        let snapshot = service.published().unwrap().1;
        self.grants.push(ReadyWorkGrant {
            work_id: id.clone(),
            contract_revision: snapshot.tickets[&id].contract_revision,
            context_hash: snapshot.project.work_contexts[&id].clone(),
            workspace,
        });
    }

    fn policy(&self, capacity: usize) -> ReadyWorkPolicy {
        ReadyWorkPolicy {
            project_id: self.project.clone(),
            grants: self.grants.clone(),
            capacity,
            max_rounds: ROUNDS,
            lifetime: Duration::from_secs(7),
        }
    }
}

#[tokio::test]
async fn absent_policy_and_non_ready_work_never_preflight_or_dispatch() {
    let empty = Fixture::new(0, false);
    let scheduler = ReadyWorkScheduler::default();
    assert_eq!(
        scheduler.reconcile(empty.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(empty.host.authority_calls.load(Ordering::SeqCst), 0);
    assert_eq!(empty.host.budget_calls.load(Ordering::SeqCst), 0);
    assert!(empty.host.preflights.lock().unwrap().is_empty());
    assert!(empty.host.dispatches.lock().unwrap().is_empty());
    let fixture = Fixture::new(1, false);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Idle
    );
    assert!(fixture.host.preflights.lock().unwrap().is_empty());
    assert!(fixture.host.dispatches.lock().unwrap().is_empty());
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn virtual_expiry_revocation_and_budget_failure_publish_nothing() {
    let fixture = Fixture::new(1, false);
    let scheduler = ReadyWorkScheduler::default();
    fixture.host.budget_enabled.store(false, Ordering::SeqCst);
    assert!(scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .is_err());
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    fixture.host.budget_enabled.store(true, Ordering::SeqCst);
    let before = fixture.host.service().published().unwrap().0;
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Idle
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    scheduler.revoke().await;
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(fixture.host.service().published().unwrap().0, before);
}

#[tokio::test(start_paused = true)]
async fn expiry_during_host_preflight_does_not_publish_start() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    fixture
        .host
        .preflight_delay_ms
        .store(8000, Ordering::SeqCst);
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert!(fixture.host.dispatches.lock().unwrap().is_empty());
    assert!(fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .assignments
        .is_empty());
}

#[tokio::test]
async fn missing_workspace_is_quarantined_while_valid_work_still_dispatches() {
    let fixture = Fixture::new(2, true);
    let scheduler = ReadyWorkScheduler::default();
    let bad = &fixture.grants[0].work_id;
    let good = &fixture.grants[1].work_id;
    std::fs::remove_dir(&fixture.grants[0].workspace.worktree).unwrap();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(2))
        .await
        .unwrap();
    let first = scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    let ReadyReconcile::Dispatched {
        work_id,
        dispatch_key,
    } = first
    else {
        panic!("valid grant must survive the bad workspace: {first:?}");
    };
    assert_eq!(&work_id, good);
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Dispatched {
            work_id,
            dispatch_key: dispatch_key.clone()
        }
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.host.ensures.lock().unwrap().as_slice(),
        &[dispatch_key]
    );
    assert_eq!(
        fixture
            .host
            .preflights
            .lock()
            .unwrap()
            .iter()
            .filter(|id| *id == bad)
            .count(),
        1
    );
    let snapshot = fixture.host.service().published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.tickets[bad].state, WorkState::Ready);
    assert!(snapshot.tickets[bad].active_assignment.is_none());
}

#[tokio::test]
async fn typed_transient_attempts_include_initial_dispatch_and_stop_after_three() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    fixture
        .host
        .outcomes
        .lock()
        .unwrap()
        .extend([ReadyBlockKind::Transient; 3]);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Blocked {
                kind: ReadyBlockKind::Transient,
                ..
            }
        ));
    }
    let snapshot = fixture.host.service().published().unwrap().1;
    let key = snapshot
        .assignments
        .values()
        .next()
        .unwrap()
        .dispatch_key
        .clone();
    for _ in 0..3 {
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    }
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.host.ensures.lock().unwrap().as_slice(),
        &[key.clone(), key]
    );
    assert_eq!(
        fixture.host.ceilings.lock().unwrap().as_slice(),
        &[ROUNDS; 3]
    );
    assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
}

#[tokio::test]
async fn direct_dispatch_errors_stop_after_three_without_publishing_start() {
    for error in [PortError::Unavailable, PortError::Io] {
        let fixture = Fixture::new(1, true);
        let scheduler = ReadyWorkScheduler::default();
        fixture
            .host
            .dispatch_errors
            .lock()
            .unwrap()
            .extend([error; 3]);
        scheduler
            .install(fixture.host.as_ref(), fixture.policy(1))
            .await
            .unwrap();
        let before = fixture.host.service().published().unwrap().0;
        for _ in 0..3 {
            assert!(matches!(
                scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
                ReadyReconcile::Blocked {
                    kind: ReadyBlockKind::Transient,
                    ..
                }
            ));
        }
        for _ in 0..4 {
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
        }
        assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 3);
        assert!(fixture.host.ensures.lock().unwrap().is_empty());
        assert_eq!(fixture.host.preflights.lock().unwrap().len(), 3);
        assert_eq!(fixture.host.service().published().unwrap().0, before);
        assert!(fixture
            .host
            .service()
            .published()
            .unwrap()
            .1
            .assignments
            .is_empty());
    }
}

#[tokio::test]
async fn dispatch_error_after_commit_and_ensure_errors_share_one_three_attempt_budget() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    fixture
        .host
        .error_after_commit
        .store(true, Ordering::SeqCst);
    fixture
        .host
        .dispatch_errors
        .lock()
        .unwrap()
        .push_back(PortError::Unavailable);
    fixture
        .host
        .ensure_errors
        .lock()
        .unwrap()
        .extend([PortError::Io, PortError::Unavailable]);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Blocked {
                kind: ReadyBlockKind::Transient,
                ..
            }
        ));
    }
    let snapshot = fixture.host.service().published().unwrap().1;
    let key = snapshot
        .assignments
        .values()
        .next()
        .unwrap()
        .dispatch_key
        .clone();
    for _ in 0..4 {
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    }
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.host.ensures.lock().unwrap().as_slice(),
        &[key.clone(), key]
    );
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
}

#[tokio::test]
async fn permanent_and_unknown_dispatch_outcomes_never_ensure_or_create_another_attempt() {
    for kind in [
        ReadyBlockKind::Configuration,
        ReadyBlockKind::Permission,
        ReadyBlockKind::OutcomeUnknown,
    ] {
        let fixture = Fixture::new(1, true);
        let scheduler = ReadyWorkScheduler::default();
        fixture.host.outcomes.lock().unwrap().push_back(kind);
        scheduler
            .install(fixture.host.as_ref(), fixture.policy(1))
            .await
            .unwrap();
        assert!(
            matches!(scheduler.reconcile(fixture.host.as_ref()).await.unwrap(), ReadyReconcile::Blocked {kind:seen,..} if seen==kind)
        );
        let before = fixture.host.service().published().unwrap().0;
        for _ in 0..4 {
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
        }
        assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
        assert!(fixture.host.ensures.lock().unwrap().is_empty());
        assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
        assert_eq!(fixture.host.service().published().unwrap().0, before);
    }
}

#[tokio::test]
async fn resource_block_is_rechecked_using_the_same_pending_key() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    fixture
        .host
        .outcomes
        .lock()
        .unwrap()
        .push_back(ReadyBlockKind::Resource);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Blocked {
            kind: ReadyBlockKind::Resource,
            ..
        }
    ));
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Dispatched { .. }
    ));
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.ensures.lock().unwrap().len(), 1);
    assert_eq!(
        fixture
            .host
            .service()
            .published()
            .unwrap()
            .1
            .assignments
            .len(),
        1
    );
}

#[tokio::test]
async fn pending_intent_owns_one_pass_before_other_ready_work_is_selected() {
    let fixture = Fixture::new(2, true);
    let scheduler = ReadyWorkScheduler::default();
    fixture
        .host
        .outcomes
        .lock()
        .unwrap()
        .extend([ReadyBlockKind::Transient; 3]);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(2))
        .await
        .unwrap();
    for _ in 0..3 {
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    }
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.ensures.lock().unwrap().len(), 2);
    assert_eq!(
        fixture.host.service().published().unwrap().1.tickets[&fixture.grants[1].work_id].state,
        WorkState::Ready
    );
    assert!(
        matches!(scheduler.reconcile(fixture.host.as_ref()).await.unwrap(), ReadyReconcile::Dispatched {work_id,..} if work_id==fixture.grants[1].work_id)
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn scope_cas_conflict_requires_a_new_selection_and_restart_fences_old_policy() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    fixture.host.conflict_once.store(true, Ordering::SeqCst);
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await,
        Err(Error::RevisionConflict)
    ));
    assert!(fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .assignments
        .is_empty());
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Dispatched { .. }
    ));
    {
        let commands = fixture.host.dispatches.lock().unwrap();
        assert_eq!(commands.len(), 2);
        assert!(commands[1].expected_seq > commands[0].expected_seq);
        assert_ne!(commands[1].operation_id, commands[0].operation_id);
    }
    let epoch = fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .authority_epoch;
    fixture.host.restart_writer(&fixture.root.0.join("ledger"));
    assert!(
        fixture
            .host
            .service()
            .published()
            .unwrap()
            .1
            .authority_epoch
            > epoch
    );
    assert_eq!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(
        ReadyWorkScheduler::default()
            .reconcile(fixture.host.as_ref())
            .await
            .unwrap(),
        ReadyReconcile::Disabled
    );
}

#[tokio::test(start_paused = true)]
async fn one_woken_worker_can_shutdown_without_cancelling_its_host() {
    let fixture = Fixture::new(1, true);
    let scheduler = Arc::new(ReadyWorkScheduler::default());
    let host: Arc<dyn ReadyWorkHost> = fixture.host.clone();
    let parent_shutdown = CancellationToken::new();
    scheduler.spawn(Arc::downgrade(&host), parent_shutdown.clone());
    scheduler.spawn(Arc::downgrade(&host), parent_shutdown.clone());
    scheduler
        .install(host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    scheduler.shutdown().await;
    assert!(!parent_shutdown.is_cancelled());
    let calls = fixture.host.authority_calls.load(Ordering::SeqCst);
    scheduler.wake();
    tokio::time::advance(Duration::from_secs(20)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.host.authority_calls.load(Ordering::SeqCst), calls);
}

// Publish a real candidate while preserving every execution input. This uses
// User authority and the normal Ticket CAS; no snapshot is modified in place.
fn publish_metadata_candidate(fixture: &Fixture) -> String {
    let service = fixture.host.service();
    let before = service.published().unwrap().1;
    let grant = &fixture.grants[0];
    let mut context =
        before.project.contexts[&before.project.work_contexts[&grant.work_id]].clone();
    context.project_revision += 1;
    let owner = Authority::from_verified_host(
        before.binding,
        Principal::User {
            user_id: "context-metadata-owner".into(),
        },
    );
    let command = service
        .prepare_command(
            &owner,
            "publish-metadata-candidate",
            vec![Operation::SetProjectContext {
                work_id: grant.work_id.clone(),
                context,
            }],
        )
        .unwrap();
    service.execute(&owner, &command).unwrap();
    let after = service.published().unwrap().1;
    let candidate_hash = after.project.work_contexts[&grant.work_id].clone();
    assert_ne!(candidate_hash, grant.context_hash);
    candidate_hash
}

#[tokio::test]
async fn pending_metadata_candidate_keeps_original_grant_and_ensures_original_intent() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Dispatched { .. }
    ));
    let started = fixture.host.service().published().unwrap().1;
    let assignment = started.assignments.values().next().unwrap().clone();
    let key = assignment.dispatch_key.clone();
    let spec = started.intents[&key].immutable_spec.clone();
    assert_eq!(assignment.state, AssignmentState::DispatchPending);
    assert!(assignment.runtime.is_none());
    let candidate_hash = publish_metadata_candidate(&fixture);
    let pending = fixture.host.service().published().unwrap().1;
    assert_eq!(pending.authority_epoch, started.authority_epoch);
    assert_eq!(
        pending.project.assignment_contexts[&assignment.id],
        fixture.grants[0].context_hash
    );
    assert_eq!(
        pending.project.work_contexts[&assignment.work_id],
        candidate_hash
    );
    let result = scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    assert!(
        matches!(&result, ReadyReconcile::Dispatched { work_id, dispatch_key }
        if work_id == &assignment.work_id && dispatch_key == &key),
        "{result:?}"
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.host.ensures.lock().unwrap().as_slice(),
        &[key.clone()]
    );
    assert_eq!(
        fixture.host.preflights.lock().unwrap().len(),
        1,
        "pending reconciliation cannot run a synthetic fresh Start preflight"
    );
    assert_eq!(
        fixture.host.ceilings.lock().unwrap().as_slice(),
        &[ROUNDS; 2]
    );
    let after = fixture.host.service().published().unwrap().1;
    assert_eq!(after.assignments.len(), 1);
    assert_eq!(after.intents.len(), 1);
    assert_eq!(after.intents[&key].immutable_spec, spec);
    assert_eq!(
        canonical_bytes(&after).unwrap(),
        canonical_bytes(&pending).unwrap(),
        "ensure must preserve the original Start receipt, Assignment and candidate/adopted split"
    );
}

#[tokio::test]
async fn candidate_grant_cannot_replace_the_adopted_grant_for_a_pending_assignment() {
    let fixture = Fixture::new(1, true);
    let scheduler = ReadyWorkScheduler::default();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Dispatched { .. }
    ));
    let candidate_hash = publish_metadata_candidate(&fixture);
    let pending = fixture.host.service().published().unwrap().1;
    let assignment = pending.assignments.values().next().unwrap();
    assert_eq!(
        pending.project.assignment_contexts[&assignment.id],
        fixture.grants[0].context_hash
    );
    assert_ne!(
        pending.project.assignment_contexts[&assignment.id],
        candidate_hash
    );
    scheduler.revoke().await;
    let mut candidate_policy = fixture.policy(1);
    candidate_policy.grants[0].context_hash = candidate_hash;
    scheduler
        .install(fixture.host.as_ref(), candidate_policy)
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Blocked {
                kind: ReadyBlockKind::Permission,
                ..
            }
        ));
    }
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.ceilings.lock().unwrap().as_slice(), &[ROUNDS]);
    let after = fixture.host.service().published().unwrap().1;
    assert_eq!(after.assignments.len(), 1);
    assert_eq!(after.intents.len(), 1);
    assert_eq!(
        canonical_bytes(&after).unwrap(),
        canonical_bytes(&pending).unwrap()
    );
}

#[tokio::test]
async fn fresh_generation_zero_still_requires_a_grant_for_the_current_candidate() {
    let fixture = Fixture::new(1, true);
    let candidate_hash = publish_metadata_candidate(&fixture);
    let before = fixture.host.service().published().unwrap().1;
    assert_eq!(before.tickets[&fixture.grants[0].work_id].generation, 0);
    assert_eq!(
        before.project.work_contexts[&fixture.grants[0].work_id],
        candidate_hash
    );
    let scheduler = ReadyWorkScheduler::default();
    scheduler
        .install(fixture.host.as_ref(), fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Blocked {
                kind: ReadyBlockKind::Permission,
                ..
            }
        ));
    }
    assert!(fixture.host.preflights.lock().unwrap().is_empty());
    assert!(fixture.host.dispatches.lock().unwrap().is_empty());
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    assert!(fixture.host.ceilings.lock().unwrap().is_empty());
    let after = fixture.host.service().published().unwrap().1;
    assert!(after.assignments.is_empty());
    assert_eq!(
        canonical_bytes(&after).unwrap(),
        canonical_bytes(&before).unwrap()
    );
}
