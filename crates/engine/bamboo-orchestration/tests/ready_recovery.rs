//! The retry store is metadata, not an execution grant. All admission in these
//! tests still passes through a real TicketService and its canonical Start CAS.
use async_trait::async_trait;
use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_orchestration::{
    ready_recovery::{
        ReadyRetryClock, ReadyRetryKey, ReadyRetryPhase, ReadyRetryRecord, ReadyRetryRequest,
        ReadyRetryStore, VersionedReadyRetry, READY_RETRY_LIMIT,
    },
    ReadyAdmissionFence, ReadyBlockKind, ReadyDispatch, ReadyReconcile, ReadyWorkGrant,
    ReadyWorkHost, ReadyWorkPolicy, ReadyWorkScheduler,
};
use bamboo_tickets::*;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Barrier, Notify};

const ROUNDS: u32 = 4;
const START_MS: u64 = 10_000;
const LIFETIME_MS: u64 = 60_000;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        static SERIAL: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "bamboo-ready-recovery-{}-{}-{}",
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

struct Clock {
    now: AtomicU64,
    sequence: Mutex<VecDeque<u64>>,
}

impl Clock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: AtomicU64::new(START_MS),
            sequence: Mutex::new(VecDeque::new()),
        })
    }

    /// Tokio controls process-local policy expiry; this absolute clock controls
    /// persisted deadlines. Advance both explicitly instead of sleeping.
    async fn advance(&self, milliseconds: u64) {
        self.now.fetch_add(milliseconds, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(milliseconds)).await;
    }

    fn set(&self, milliseconds: u64) {
        self.now.store(milliseconds, Ordering::SeqCst);
    }

    fn observe_sequence(&self, observations: impl IntoIterator<Item = u64>) {
        self.sequence.lock().unwrap().extend(observations);
    }
}

impl ReadyRetryClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        if let Some(now) = self.sequence.lock().unwrap().pop_front() {
            Ok(now)
        } else {
            Ok(self.now.load(Ordering::SeqCst))
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StorageError {
    Full,
    Unwritable,
    Corrupt,
}

impl StorageError {
    fn error(self) -> Error {
        match self {
            Self::Full => std::io::Error::from_raw_os_error(28).into(),
            Self::Unwritable => std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "controlled unwritable retry store",
            )
            .into(),
            Self::Corrupt => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "controlled corrupt retry store",
            )
            .into(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StoreFault {
    Load(StorageError),
    Initialize(StorageError),
    ClockFloor(StorageError),
    Reserve(StorageError),
    AfterReserve(StorageError),
    Settle(StorageError),
    BadInitializeReceipt,
    BadReserveReceipt,
    PauseAfterReserve,
}

impl StoreFault {
    fn matches(self, expected: Option<u64>, next: &ReadyRetryRecord) -> bool {
        match self {
            Self::Load(_) => false,
            Self::Initialize(_) | Self::BadInitializeReceipt => expected.is_none(),
            Self::ClockFloor(_) => {
                expected.is_some() && next.phase == ReadyRetryPhase::Ready && next.attempts == 0
            }
            Self::Reserve(_)
            | Self::AfterReserve(_)
            | Self::BadReserveReceipt
            | Self::PauseAfterReserve => next.phase == ReadyRetryPhase::Reserved,
            Self::Settle(_) => next.phase == ReadyRetryPhase::Deferred && next.request.is_some(),
        }
    }
}

#[derive(Default)]
struct StoreData {
    records: BTreeMap<(String, String), VersionedReadyRetry>,
    writes: Vec<VersionedReadyRetry>,
    conflicts: usize,
}

/// A shared durable-image fake: every new scheduler sees the same committed
/// bytes. Faults distinguish a rejected write from a lost post-commit reply.
#[derive(Default)]
struct MemoryStore {
    data: Mutex<StoreData>,
    fault: Mutex<Option<StoreFault>>,
    initial_race: Mutex<Option<Arc<Barrier>>>,
    load_errors: Mutex<BTreeMap<(String, String), StorageError>>,
    after_reserve_clock: Mutex<Option<(Arc<Clock>, u64)>>,
    loads: AtomicUsize,
    reserved_committed: Notify,
    release_reserved: Notify,
}

impl MemoryStore {
    fn map_key(key: &ReadyRetryKey) -> (String, String) {
        (key.scope_id.clone(), key.work_id.clone())
    }

    fn value(&self, key: &ReadyRetryKey) -> VersionedReadyRetry {
        self.data.lock().unwrap().records[&Self::map_key(key)].clone()
    }

    fn replace(&self, key: &ReadyRetryKey, value: VersionedReadyRetry) {
        self.data
            .lock()
            .unwrap()
            .records
            .insert(Self::map_key(key), value);
    }

    fn set_fault(&self, fault: StoreFault) {
        *self.fault.lock().unwrap() = Some(fault);
    }

    fn take_cas_fault(&self, expected: Option<u64>, next: &ReadyRetryRecord) -> Option<StoreFault> {
        let mut fault = self.fault.lock().unwrap();
        if fault.is_some_and(|fault| fault.matches(expected, next)) {
            fault.take()
        } else {
            None
        }
    }
}

#[async_trait]
impl ReadyRetryStore for MemoryStore {
    async fn load(&self, key: &ReadyRetryKey) -> Result<Option<VersionedReadyRetry>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self
            .load_errors
            .lock()
            .unwrap()
            .get(&Self::map_key(key))
            .copied()
        {
            return Err(error.error());
        }
        let error = {
            let mut fault = self.fault.lock().unwrap();
            if let Some(StoreFault::Load(error)) = *fault {
                *fault = None;
                Some(error)
            } else {
                None
            }
        };
        if let Some(error) = error {
            return Err(error.error());
        }
        Ok(self
            .data
            .lock()
            .unwrap()
            .records
            .get(&Self::map_key(key))
            .cloned())
    }

    async fn compare_exchange(
        &self,
        key: &ReadyRetryKey,
        expected: Option<u64>,
        next: ReadyRetryRecord,
    ) -> Result<VersionedReadyRetry> {
        let race = if expected.is_none() {
            self.initial_race.lock().unwrap().clone()
        } else {
            None
        };
        if let Some(race) = race {
            race.wait().await;
        }
        let fault = self.take_cas_fault(expected, &next);
        match fault {
            Some(StoreFault::Initialize(error))
            | Some(StoreFault::ClockFloor(error))
            | Some(StoreFault::Reserve(error))
            | Some(StoreFault::Settle(error)) => return Err(error.error()),
            _ => {}
        }
        let value = {
            let mut data = self.data.lock().unwrap();
            let map_key = Self::map_key(key);
            if data.records.get(&map_key).map(|value| value.revision) != expected {
                data.conflicts += 1;
                return Err(Error::RevisionConflict);
            }
            let value = VersionedReadyRetry {
                revision: expected.map_or(1, |revision| revision + 1),
                record: next,
            };
            data.records.insert(map_key, value.clone());
            data.writes.push(value.clone());
            value
        };
        if value.record.phase == ReadyRetryPhase::Reserved {
            let rollback = self.after_reserve_clock.lock().unwrap().take();
            if let Some((clock, now)) = rollback {
                // Simulate a wall-clock rollback while a durable CAS is in
                // flight, after its bytes committed but before its reply.
                clock.set(now);
            }
        }
        match fault {
            Some(StoreFault::AfterReserve(error)) => Err(error.error()),
            Some(StoreFault::PauseAfterReserve) => {
                // The write is already durable when the caller is cancelled.
                // No Mutex guard crosses this suspension point.
                self.reserved_committed.notify_one();
                self.release_reserved.notified().await;
                Ok(value)
            }
            Some(StoreFault::BadInitializeReceipt | StoreFault::BadReserveReceipt) => {
                let mut reply = value;
                reply.revision += 1;
                Ok(reply)
            }
            _ => Ok(value),
        }
    }
}

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "ready-recovery-scope".into(),
        supervisor_session_id: "ready-recovery-supervisor".into(),
        binding_revision: 1,
    }
}

fn contract(title: &str) -> Contract {
    Contract {
        title: title.into(),
        objective: format!("Deliver {title}"),
        constraints: vec!["Stay within the explicit Host decision".into()],
        acceptance: vec!["Human checks the evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}

#[derive(Clone, Copy)]
enum HostError {
    Unavailable,
    Io,
    Resource,
}

impl HostError {
    fn error(self) -> Error {
        match self {
            Self::Unavailable => Error::AuthorityUnavailable("controlled Host outage".into()),
            Self::Io => std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "controlled Host I/O failure",
            )
            .into(),
            Self::Resource => Error::ResourceBlocked("controlled unavailable workspace".into()),
        }
    }
}

#[derive(Clone, Copy)]
enum DispatchAction {
    Observe,
    FailBefore(HostError),
    CommitThenFail(HostError),
    CommitThenBlock(ReadyBlockKind),
    ScopeConflict,
}

#[derive(Clone, Copy)]
enum EnsureAction {
    Observe,
    Fail(HostError),
    Block(ReadyBlockKind),
}

struct Host {
    service: Mutex<Option<Arc<TicketService>>>,
    authority_calls: AtomicUsize,
    budget_calls: AtomicUsize,
    preflights: Mutex<Vec<String>>,
    dispatches: Mutex<Vec<Command>>,
    ensures: Mutex<Vec<(String, DispatchSpec, u32)>>,
    preflight_errors: Mutex<VecDeque<HostError>>,
    dispatch_actions: Mutex<VecDeque<DispatchAction>>,
    ensure_actions: Mutex<VecDeque<EnsureAction>>,
    conflict_serial: AtomicU64,
    dispatch_wait_ms: AtomicU64,
    ensure_wait_ms: AtomicU64,
    fenced_dispatches: AtomicUsize,
    fenced_ensures: AtomicUsize,
}

impl Host {
    fn service(&self) -> Arc<TicketService> {
        self.service.lock().unwrap().as_ref().unwrap().clone()
    }

    fn authority_for(&self, principal: Principal) -> Authority {
        Authority::from_verified_host(self.service().published().unwrap().1.binding, principal)
    }

    fn supervisor(&self) -> Authority {
        self.authority_for(Principal::Supervisor {
            session_id: binding().supervisor_session_id,
        })
    }

    fn restart_writer(&self, ledger: &Path) -> u64 {
        let old = self.service.lock().unwrap().take().unwrap();
        drop(old);
        let service = Arc::new(TicketService::open(ledger, binding()).unwrap());
        let epoch = service.published().unwrap().1.authority_epoch;
        *self.service.lock().unwrap() = Some(service);
        epoch
    }

    fn execute(
        &self,
        principal: Principal,
        id: &str,
        operations: Vec<Operation>,
    ) -> OperationReceipt {
        let service = self.service();
        let authority = self.authority_for(principal);
        let command = service.prepare_command(&authority, id, operations).unwrap();
        service.execute(&authority, &command).unwrap()
    }

    fn assert_no_execution(&self) {
        assert!(self.dispatches.lock().unwrap().is_empty());
        assert!(self.ensures.lock().unwrap().is_empty());
        assert!(self.service().published().unwrap().1.assignments.is_empty());
    }
}

#[async_trait]
impl ReadyWorkHost for Host {
    async fn authority(&self) -> Result<(Arc<TicketService>, Authority)> {
        self.authority_calls.fetch_add(1, Ordering::SeqCst);
        Ok((self.service(), self.supervisor()))
    }

    async fn check_budget(&self, max_rounds: u32) -> Result<()> {
        self.budget_calls.fetch_add(1, Ordering::SeqCst);
        // The test Host permits both finite ceilings. The durable identity must
        // independently reject replacing an old decision with a higher one.
        if !(1..=16).contains(&max_rounds) {
            return Err(Error::ResourceBlocked("finite Host budget required".into()));
        }
        Ok(())
    }

    async fn preflight(&self, command: &Command) -> Result<()> {
        let [Operation::Start {
            work_id,
            workspace: Some(workspace),
            ..
        }] = command.operations.as_slice()
        else {
            return Err(Error::InvalidTransition(
                "explicit workspace required".into(),
            ));
        };
        self.preflights.lock().unwrap().push(work_id.clone());
        if let Some(error) = self.preflight_errors.lock().unwrap().pop_front() {
            return Err(error.error());
        }
        if !Path::new(&workspace.repo).is_dir() || !Path::new(&workspace.worktree).is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "controlled missing execution workspace",
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
        assert_eq!(
            self.service().published()?.1.intents[key].immutable_spec,
            *spec
        );
        self.ensures
            .lock()
            .unwrap()
            .push((key.into(), spec.clone(), max_rounds));
        let action = self
            .ensure_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(EnsureAction::Observe);
        match action {
            EnsureAction::Observe => Ok(ReadyDispatch::Observed {
                dispatch_key: key.into(),
            }),
            EnsureAction::Fail(error) => Err(error.error()),
            EnsureAction::Block(kind) => Ok(ReadyDispatch::Blocked {
                kind,
                reason: format!("controlled {kind:?} ensure outcome"),
            }),
        }
    }

    async fn dispatch_fenced(
        &self,
        command: &Command,
        max_rounds: u32,
        fence: &ReadyAdmissionFence,
    ) -> Result<ReadyDispatch> {
        self.fenced_dispatches.fetch_add(1, Ordering::SeqCst);
        fence.check()?;
        let delay = self.dispatch_wait_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        fence.check()?;
        self.dispatch(command, max_rounds).await
    }

    async fn ensure_fenced(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
        fence: &ReadyAdmissionFence,
    ) -> Result<ReadyDispatch> {
        self.fenced_ensures.fetch_add(1, Ordering::SeqCst);
        fence.check()?;
        let delay = self.ensure_wait_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        fence.check()?;
        self.ensure(key, spec, max_rounds).await
    }

    async fn dispatch(&self, command: &Command, max_rounds: u32) -> Result<ReadyDispatch> {
        assert_eq!(max_rounds, ROUNDS);
        self.dispatches.lock().unwrap().push(command.clone());
        let action = self
            .dispatch_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(DispatchAction::Observe);
        if let DispatchAction::FailBefore(error) = action {
            return Err(error.error());
        }
        let service = self.service();
        let authority = self.supervisor();
        if matches!(action, DispatchAction::ScopeConflict) {
            let serial = self.conflict_serial.fetch_add(1, Ordering::SeqCst);
            let change = service.prepare_command(
                &authority,
                &format!("intervening-scope-write-{serial}"),
                vec![Operation::Create {
                    temp_id: "unrelated".into(),
                    kind: TicketKind::Goal,
                    parent: None,
                    contract: contract("Unrelated scope change"),
                    depends_on: BTreeSet::new(),
                }],
            )?;
            service.execute(&authority, &change)?;
        }
        let receipt = service.execute(&authority, command)?;
        let snapshot = service.published()?.1;
        let key = snapshot.assignments[&receipt.ids["assignment"]]
            .dispatch_key
            .clone();
        match action {
            DispatchAction::CommitThenFail(error) => Err(error.error()),
            DispatchAction::CommitThenBlock(kind) => Ok(ReadyDispatch::Blocked {
                kind,
                reason: format!("controlled {kind:?} dispatch outcome"),
            }),
            _ => Ok(ReadyDispatch::Observed { dispatch_key: key }),
        }
    }
}

struct Fixture {
    host: Arc<Host>,
    grant: ReadyWorkGrant,
    project: ProjectId,
    root: TestRoot,
}

impl Fixture {
    fn new() -> Self {
        let root = TestRoot::new();
        let service = Arc::new(TicketService::open(root.0.join("ledger"), binding()).unwrap());
        let host = Arc::new(Host {
            service: Mutex::new(Some(service)),
            authority_calls: AtomicUsize::new(0),
            budget_calls: AtomicUsize::new(0),
            preflights: Mutex::new(Vec::new()),
            dispatches: Mutex::new(Vec::new()),
            ensures: Mutex::new(Vec::new()),
            preflight_errors: Mutex::new(VecDeque::new()),
            dispatch_actions: Mutex::new(VecDeque::new()),
            ensure_actions: Mutex::new(VecDeque::new()),
            conflict_serial: AtomicU64::new(0),
            dispatch_wait_ms: AtomicU64::new(0),
            ensure_wait_ms: AtomicU64::new(0),
            fenced_dispatches: AtomicUsize::new(0),
            fenced_ensures: AtomicUsize::new(0),
        });
        let project = ProjectId::parse("ready-recovery-project").unwrap();
        let repo = root.0.join("repo");
        let tree = root.0.join("worktree");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&tree).unwrap();
        let repo = repo.to_string_lossy().into_owned();
        let tree = tree.to_string_lossy().into_owned();
        let workspace = ExecutionWorkspace {
            repo: repo.clone(),
            base_commit: "a".repeat(40),
            branch: "ready-recovery-work".into(),
            worktree: tree.clone(),
            write_roots: vec![tree.clone()],
            claims: BTreeSet::from([format!("worktree:{tree}")]),
        };
        let supervisor = || Principal::Supervisor {
            session_id: binding().supervisor_session_id,
        };
        let receipt = host.execute(
            supervisor(),
            "create-work",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("Durable bounded retry"),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: project.clone(),
                },
            ],
        );
        let work_id = receipt.ids["work"].clone();
        let work = host.service().published().unwrap().1.tickets[&work_id].clone();
        host.execute(
            supervisor(),
            "adopt-context-and-ready",
            vec![
                Operation::SetProjectContext {
                    work_id: work_id.clone(),
                    context: ProjectContextSnapshot {
                        project_id: project.clone(),
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
                },
                Operation::Ready {
                    work_id: work_id.clone(),
                },
            ],
        );
        let snapshot = host.service().published().unwrap().1;
        let grant = ReadyWorkGrant {
            work_id: work_id.clone(),
            contract_revision: snapshot.tickets[&work_id].contract_revision,
            context_hash: snapshot.project.work_contexts[&work_id].clone(),
            workspace,
        };
        Self {
            host,
            grant,
            project,
            root,
        }
    }

    fn key(&self) -> ReadyRetryKey {
        ReadyRetryKey {
            scope_id: binding().scope_id,
            work_id: self.grant.work_id.clone(),
        }
    }

    fn add_work(&self) -> ReadyWorkGrant {
        let tree = self.root.0.join("second-worktree");
        std::fs::create_dir(&tree).unwrap();
        let tree = tree.to_string_lossy().into_owned();
        let mut workspace = self.grant.workspace.clone();
        workspace.branch = "ready-recovery-second-work".into();
        workspace.worktree = tree.clone();
        workspace.write_roots = vec![tree.clone()];
        workspace.claims = BTreeSet::from([format!("worktree:{tree}")]);
        let supervisor = || Principal::Supervisor {
            session_id: binding().supervisor_session_id,
        };
        let receipt = self.host.execute(
            supervisor(),
            "create-second-work",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract("Healthy second Work"),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: self.project.clone(),
                },
            ],
        );
        let work_id = receipt.ids["work"].clone();
        let work = self.host.service().published().unwrap().1.tickets[&work_id].clone();
        self.host.execute(
            supervisor(),
            "adopt-second-context-and-ready",
            vec![
                Operation::SetProjectContext {
                    work_id: work_id.clone(),
                    context: ProjectContextSnapshot {
                        project_id: self.project.clone(),
                        project_revision: 1,
                        resource_revision: 1,
                        workspace_binding: WorkspaceBinding {
                            path: workspace.repo.clone(),
                            label: None,
                            git_common_dir: None,
                        },
                        contract_revision: work.contract_revision,
                        contract: work.contract,
                        sources: BTreeMap::from([("spec".into(), "v1".into())]),
                    },
                },
                Operation::Ready {
                    work_id: work_id.clone(),
                },
            ],
        );
        let snapshot = self.host.service().published().unwrap().1;
        ReadyWorkGrant {
            work_id: work_id.clone(),
            contract_revision: snapshot.tickets[&work_id].contract_revision,
            context_hash: snapshot.project.work_contexts[&work_id].clone(),
            workspace,
        }
    }

    fn policy(&self) -> ReadyWorkPolicy {
        ReadyWorkPolicy {
            project_id: self.project.clone(),
            grants: vec![self.grant.clone()],
            capacity: 1,
            max_rounds: ROUNDS,
            lifetime: Duration::from_millis(LIFETIME_MS),
        }
    }

    fn scheduler(&self, store: &Arc<MemoryStore>, clock: &Arc<Clock>) -> ReadyWorkScheduler {
        ReadyWorkScheduler::with_recovery(store.clone(), clock.clone())
    }

    async fn install(&self, store: &Arc<MemoryStore>, clock: &Arc<Clock>) -> ReadyWorkScheduler {
        let scheduler = self.scheduler(store, clock);
        scheduler
            .install(self.host.as_ref(), self.policy())
            .await
            .unwrap();
        scheduler
    }

    fn fail_preflight(&self, errors: impl IntoIterator<Item = HostError>) {
        self.host.preflight_errors.lock().unwrap().extend(errors);
    }
}

fn assert_blocked(report: ReadyReconcile, work: &str, expected: ReadyBlockKind) {
    assert!(
        matches!(&report, ReadyReconcile::Blocked { work_id, kind, .. } if work_id == work && *kind == expected),
        "expected {expected:?} block for {work}, got {report:?}"
    );
}

fn assert_deferred(report: ReadyReconcile, work: &str, due: u64, charged: u8) {
    assert!(
        matches!(
            &report,
            ReadyReconcile::Deferred { work_id, next_check_at_ms, attempts, .. }
                if work_id == work && *next_check_at_ms == due && *attempts == charged
        ),
        "expected {work} deferred to {due} with {charged} charged attempts, got {report:?}"
    );
}

fn dispatched_key(report: ReadyReconcile, work: &str) -> String {
    match report {
        ReadyReconcile::Dispatched {
            work_id,
            dispatch_key,
        } => {
            assert_eq!(work_id, work);
            dispatch_key
        }
        other => panic!("expected a dispatch, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn durable_metadata_never_restores_a_host_policy_or_polls_an_empty_queue() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    let scheduler = fixture.scheduler(&store, &clock);
    for _ in 0..8 {
        assert_eq!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Disabled
        );
    }
    assert_eq!(store.loads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.host.authority_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.host.budget_calls.load(Ordering::SeqCst), 0);
    fixture.host.assert_no_execution();

    fixture.fail_preflight([HostError::Unavailable]);
    scheduler
        .install(fixture.host.as_ref(), fixture.policy())
        .await
        .unwrap();
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let persisted = store.value(&fixture.key());
    let before = fixture.host.authority_calls.load(Ordering::SeqCst);
    drop(scheduler);
    let recovered = fixture.scheduler(&store, &clock);
    assert_eq!(
        recovered.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(fixture.host.authority_calls.load(Ordering::SeqCst), before);
    assert_eq!(store.value(&fixture.key()), persisted);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn restart_and_fresh_gen_zero_policy_preserve_backoff_deadline_and_three_attempts() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture.fail_preflight([HostError::Unavailable; 3]);
    let first = fixture.install(&store, &clock).await;
    assert_blocked(
        first.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let initial = store.value(&fixture.key()).record;
    assert_eq!(initial.attempts, 1);
    assert_eq!(initial.next_check_at_ms, START_MS + 1_000);
    assert_eq!(initial.expires_at_ms, START_MS + LIFETIME_MS);
    assert!(initial.request.is_none());
    drop(first);

    let second = fixture.install(&store, &clock).await;
    assert_deferred(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 1_000,
        1,
    );
    clock.advance(999).await;
    assert_deferred(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 1_000,
        1,
    );
    let epoch = fixture.host.restart_writer(&fixture.root.0.join("ledger"));
    assert!(epoch > initial.authority_epoch);
    assert_eq!(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        ReadyReconcile::Disabled
    );
    second
        .install(fixture.host.as_ref(), fixture.policy())
        .await
        .unwrap();
    assert_deferred(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 1_000,
        1,
    );
    let reauthorized = store.value(&fixture.key()).record;
    assert_eq!(reauthorized.authority_epoch, epoch);
    assert_eq!(reauthorized.created_at_ms, initial.created_at_ms);
    assert_eq!(reauthorized.expires_at_ms, initial.expires_at_ms);

    clock.advance(1).await;
    assert_blocked(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(
        store.value(&fixture.key()).record.next_check_at_ms,
        START_MS + 3_000
    );
    drop(second);
    let third = fixture.install(&store, &clock).await;
    clock.advance(1_999).await;
    assert_deferred(
        third.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 3_000,
        2,
    );
    clock.advance(1).await;
    assert_blocked(
        third.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let stopped = store.value(&fixture.key()).record;
    assert_eq!(stopped.attempts, READY_RETRY_LIMIT);
    assert_eq!(stopped.phase, ReadyRetryPhase::Quarantined);
    assert_eq!(stopped.expires_at_ms, initial.expires_at_ms);
    assert_eq!(fixture.host.preflights.lock().unwrap().len(), 3);
    third.revoke().await;
    third
        .install(fixture.host.as_ref(), fixture.policy())
        .await
        .unwrap();
    clock.advance(10_000).await;
    assert_blocked(
        third.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(store.value(&fixture.key()).record, stopped);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn reinstall_cannot_extend_the_original_deadline_and_resource_wait_is_five_seconds() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture.fail_preflight([HostError::Resource]);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Resource,
    );
    let original = store.value(&fixture.key()).record;
    assert_eq!(original.attempts, 0);
    assert_eq!(original.next_check_at_ms, START_MS + 5_000);
    scheduler.revoke().await;
    let mut fresh = fixture.policy();
    fresh.lifetime = Duration::from_secs(3_600);
    scheduler
        .install(fixture.host.as_ref(), fresh)
        .await
        .unwrap();
    clock.advance(4_999).await;
    assert_deferred(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 5_000,
        0,
    );
    clock.advance(LIFETIME_MS - 4_999).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Configuration,
    );
    let stopped = store.value(&fixture.key()).record;
    assert_eq!(stopped.expires_at_ms, original.expires_at_ms);
    assert_eq!(stopped.created_at_ms, original.created_at_ms);
    assert_eq!(stopped.phase, ReadyRetryPhase::Quarantined);
    assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn backward_absolute_clock_is_quarantined_without_refunding_attempts() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture.fail_preflight([HostError::Unavailable]);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    clock.set(START_MS - 1);
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Configuration,
    );
    let stopped = store.value(&fixture.key()).record;
    assert_eq!(stopped.attempts, 1);
    assert_eq!(stopped.last_seen_at_ms, START_MS);
    assert_eq!(stopped.phase, ReadyRetryPhase::Quarantined);
    assert!(stopped.reason.contains("backwards"));
    clock.set(START_MS + 30_000);
    scheduler.revoke().await;
    scheduler
        .install(fixture.host.as_ref(), fixture.policy())
        .await
        .unwrap();
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Configuration,
    );
    assert_eq!(store.value(&fixture.key()).record, stopped);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn exact_postcommit_receipt_retries_only_original_intent_with_total_budget_three() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture
        .host
        .dispatch_actions
        .lock()
        .unwrap()
        .push_back(DispatchAction::CommitThenFail(HostError::Unavailable));
    fixture.host.ensure_actions.lock().unwrap().extend([
        EnsureAction::Fail(HostError::Io),
        EnsureAction::Block(ReadyBlockKind::Transient),
    ]);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let first = store.value(&fixture.key()).record;
    let Some(ReadyRetryRequest::Start {
        operation_id,
        request_hash,
    }) = first.request
    else {
        panic!("first reservation must retain its Start identity");
    };
    let snapshot = fixture.host.service().published().unwrap().1;
    assert_eq!(snapshot.receipts[&operation_id].request_hash, request_hash);
    let assignment = snapshot.assignments.values().next().unwrap().clone();
    let original_spec = snapshot.intents[&assignment.dispatch_key]
        .immutable_spec
        .clone();
    assert_eq!(original_spec.generation, 1);
    assert_eq!(original_spec.authority_epoch, snapshot.authority_epoch);
    drop(snapshot);
    drop(scheduler);

    let scheduler = fixture.install(&store, &clock).await;
    assert_deferred(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 1_000,
        1,
    );
    clock.advance(1_000).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let second = store.value(&fixture.key()).record;
    assert_eq!(second.attempts, 2);
    assert_eq!(second.next_check_at_ms, START_MS + 3_000);
    assert!(
        matches!(second.request, Some(ReadyRetryRequest::Intent { ref dispatch_key, .. }) if dispatch_key == &assignment.dispatch_key)
    );
    clock.advance(2_000).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(store.value(&fixture.key()).record.attempts, 3);
    assert_eq!(
        store.value(&fixture.key()).record.phase,
        ReadyRetryPhase::Quarantined
    );
    clock.advance(10_000).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    let ensures = fixture.host.ensures.lock().unwrap().clone();
    assert_eq!(ensures.len(), 2);
    for (key, spec, ceiling) in ensures {
        assert_eq!(key, assignment.dispatch_key);
        assert_eq!(spec, original_spec);
        assert_eq!(ceiling, ROUNDS);
    }
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

#[tokio::test(start_paused = true)]
async fn successful_observation_waits_five_seconds_and_cannot_ensure_forever() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    let scheduler = fixture.install(&store, &clock).await;
    let key = dispatched_key(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
    );
    assert_eq!(
        store.value(&fixture.key()).record.next_check_at_ms,
        START_MS + 5_000
    );
    clock.advance(4_999).await;
    assert_deferred(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 5_000,
        1,
    );
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    clock.advance(1).await;
    assert_eq!(
        dispatched_key(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id
        ),
        key
    );
    assert_eq!(store.value(&fixture.key()).record.attempts, 2);
    clock.advance(5_000).await;
    assert_eq!(
        dispatched_key(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id
        ),
        key
    );
    assert_eq!(store.value(&fixture.key()).record.attempts, 3);
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.ensures.lock().unwrap().len(), 2);
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

#[tokio::test(start_paused = true)]
async fn uncertain_start_without_canonical_receipt_is_never_blindly_readmitted() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture
        .host
        .dispatch_actions
        .lock()
        .unwrap()
        .push_back(DispatchAction::FailBefore(HostError::Unavailable));
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::OutcomeUnknown,
    );
    let unknown = store.value(&fixture.key()).record;
    assert_eq!(unknown.attempts, 1);
    assert_eq!(unknown.phase, ReadyRetryPhase::Quarantined);
    assert!(matches!(
        unknown.request,
        Some(ReadyRetryRequest::Start { .. })
    ));
    assert!(fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .assignments
        .is_empty());
    drop(scheduler);
    clock.advance(20_000).await;
    let scheduler = fixture.install(&store, &clock).await;
    for _ in 0..4 {
        assert_blocked(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id,
            ReadyBlockKind::OutcomeUnknown,
        );
    }
    assert_eq!(store.value(&fixture.key()).record, unknown);
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    assert!(fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .assignments
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn explicit_unknown_even_with_a_receipt_is_quarantined_and_writer_restart_cannot_admit_it() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture
        .host
        .dispatch_actions
        .lock()
        .unwrap()
        .push_back(DispatchAction::CommitThenBlock(
            ReadyBlockKind::OutcomeUnknown,
        ));
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::OutcomeUnknown,
    );
    let record = store.value(&fixture.key()).record;
    assert_eq!(record.phase, ReadyRetryPhase::Quarantined);
    let old = fixture.host.service().published().unwrap().1;
    let assignment = old.assignments.values().next().unwrap().clone();
    let old_spec = old.intents[&assignment.dispatch_key].immutable_spec.clone();
    drop(old);
    drop(scheduler);
    let epoch = fixture.host.restart_writer(&fixture.root.0.join("ledger"));
    assert!(epoch > record.authority_epoch);
    clock.advance(5_000).await;
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::OutcomeUnknown,
    );
    let snapshot = fixture.host.service().published().unwrap().1;
    assert_eq!(
        snapshot.assignments[&assignment.id].state,
        AssignmentState::OutcomeUnknown
    );
    assert_eq!(
        snapshot.intents[&assignment.dispatch_key].immutable_spec,
        old_spec
    );
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(store.value(&fixture.key()).record.attempts, 1);
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn changed_grant_or_round_cap_cannot_replace_a_durable_retry_identity() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture.fail_preflight([HostError::Unavailable]);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    let original = store.value(&fixture.key());
    clock.advance(1_000).await;
    for variant in 0..4 {
        scheduler.revoke().await;
        let mut policy = fixture.policy();
        match variant {
            0 => policy.max_rounds = ROUNDS + 1,
            1 => policy.grants[0].workspace.branch.push_str("-changed"),
            2 => policy.grants[0].contract_revision += 1,
            _ => policy.grants[0].context_hash = "b".repeat(64),
        }
        scheduler
            .install(fixture.host.as_ref(), policy)
            .await
            .unwrap();
        assert_blocked(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id,
            ReadyBlockKind::Permission,
        );
        assert_eq!(store.value(&fixture.key()), original);
    }
    assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn cancellation_after_durable_reservation_charges_once_and_enters_no_runtime() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    store.set_fault(StoreFault::PauseAfterReserve);
    let scheduler = Arc::new(fixture.install(&store, &clock).await);
    let running = scheduler.clone();
    let host = fixture.host.clone();
    let task = tokio::spawn(async move { running.reconcile(host.as_ref()).await });
    store.reserved_committed.notified().await;
    let reserved = store.value(&fixture.key()).record;
    assert_eq!(reserved.phase, ReadyRetryPhase::Reserved);
    assert_eq!(reserved.attempts, 1);
    assert!(matches!(
        reserved.request,
        Some(ReadyRetryRequest::Start { .. })
    ));
    fixture.host.assert_no_execution();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(scheduler);

    clock.advance(1_000).await;
    let recovered = fixture.install(&store, &clock).await;
    assert_blocked(
        recovered.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::OutcomeUnknown,
    );
    let unknown = store.value(&fixture.key()).record;
    assert_eq!(unknown.attempts, 1);
    assert_eq!(unknown.expires_at_ms, reserved.expires_at_ms);
    assert_eq!(unknown.phase, ReadyRetryPhase::Quarantined);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn competing_schedulers_share_one_durable_cas_winner_and_one_start() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    *store.initial_race.lock().unwrap() = Some(Arc::new(Barrier::new(2)));
    let first = fixture.install(&store, &clock).await;
    let second = fixture.install(&store, &clock).await;
    let (left, right) = tokio::join!(
        first.reconcile(fixture.host.as_ref()),
        second.reconcile(fixture.host.as_ref())
    );
    let results = [&left, &right];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(ReadyReconcile::Dispatched { .. })))
            .count(),
        1,
        "exactly one scheduler may dispatch: {left:?}, {right:?}"
    );
    let rejected = results
        .into_iter()
        .find(|result| !matches!(result, Ok(ReadyReconcile::Dispatched { .. })))
        .unwrap();
    // A rejected metadata CAS is a typed per-Work block. If the winner has
    // already committed Start, canonical claiming can also reject the loser's
    // now-stale scope snapshot before that report is returned.
    assert!(
        matches!(
            rejected,
            Err(Error::RevisionConflict)
                | Ok(ReadyReconcile::Blocked {
                    kind: ReadyBlockKind::Resource,
                    ..
                })
        ),
        "unexpected losing CAS result: {rejected:?}"
    );
    let (conflicts, reservations) = {
        let data = store.data.lock().unwrap();
        (
            data.conflicts,
            data.writes
                .iter()
                .filter(|value| value.record.phase == ReadyRetryPhase::Reserved)
                .count(),
        )
    };
    assert_eq!(conflicts, 1);
    assert_eq!(reservations, 1);
    assert_eq!(store.value(&fixture.key()).record.attempts, 1);
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
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
    assert_deferred(
        first.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 5_000,
        1,
    );
    assert_deferred(
        second.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        START_MS + 5_000,
        1,
    );
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn corrupt_full_unwritable_and_mismatched_store_receipts_never_admit() {
    for fault in [
        StoreFault::Load(StorageError::Corrupt),
        StoreFault::Load(StorageError::Unwritable),
        StoreFault::Initialize(StorageError::Full),
        StoreFault::Initialize(StorageError::Unwritable),
        StoreFault::ClockFloor(StorageError::Full),
        StoreFault::Reserve(StorageError::Full),
        StoreFault::Reserve(StorageError::Unwritable),
        StoreFault::BadInitializeReceipt,
        StoreFault::BadReserveReceipt,
    ] {
        let fixture = Fixture::new();
        let store = Arc::new(MemoryStore::default());
        let clock = Clock::new();
        store.set_fault(fault);
        let scheduler = fixture.install(&store, &clock).await;
        let result = scheduler.reconcile(fixture.host.as_ref()).await;
        if matches!(
            fault,
            StoreFault::Load(_)
                | StoreFault::Initialize(_)
                | StoreFault::ClockFloor(_)
                | StoreFault::BadInitializeReceipt
        ) {
            assert_blocked(
                result.unwrap(),
                &fixture.grant.work_id,
                ReadyBlockKind::Resource,
            );
        } else {
            assert!(result.is_err(), "{fault:?} must fail closed");
        }
        fixture.host.assert_no_execution();
    }
}

#[tokio::test(start_paused = true)]
async fn lost_reservation_reply_is_charged_and_cannot_be_replayed_as_a_new_start() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    store.set_fault(StoreFault::AfterReserve(StorageError::Full));
    let scheduler = fixture.install(&store, &clock).await;
    assert!(scheduler.reconcile(fixture.host.as_ref()).await.is_err());
    let reserved = store.value(&fixture.key()).record;
    assert_eq!(reserved.attempts, 1);
    assert_eq!(reserved.phase, ReadyRetryPhase::Reserved);
    fixture.host.assert_no_execution();
    drop(scheduler);
    clock.advance(1_000).await;
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::OutcomeUnknown,
    );
    assert_eq!(store.value(&fixture.key()).record.attempts, 1);
    assert_eq!(
        store.value(&fixture.key()).record.expires_at_ms,
        reserved.expires_at_ms
    );
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn lost_poststart_settlement_recovers_only_the_exact_canonical_intent() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    store.set_fault(StoreFault::Settle(StorageError::Unwritable));
    let scheduler = fixture.install(&store, &clock).await;
    assert!(scheduler.reconcile(fixture.host.as_ref()).await.is_err());
    let reserved = store.value(&fixture.key()).record;
    assert_eq!(reserved.phase, ReadyRetryPhase::Reserved);
    assert_eq!(reserved.attempts, 1);
    let snapshot = fixture.host.service().published().unwrap().1;
    let assignment = snapshot.assignments.values().next().unwrap().clone();
    let spec = snapshot.intents[&assignment.dispatch_key]
        .immutable_spec
        .clone();
    drop(snapshot);
    drop(scheduler);
    clock.advance(1_000).await;
    let scheduler = fixture.install(&store, &clock).await;
    assert_eq!(
        dispatched_key(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id
        ),
        assignment.dispatch_key
    );
    assert_eq!(store.value(&fixture.key()).record.attempts, 2);
    assert_eq!(
        store.value(&fixture.key()).record.expires_at_ms,
        reserved.expires_at_ms
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        *fixture.host.ensures.lock().unwrap(),
        vec![(assignment.dispatch_key, spec, ROUNDS)]
    );
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

#[tokio::test(start_paused = true)]
async fn invalid_persisted_metadata_is_not_treated_as_a_fresh_retry_budget() {
    for variant in 0..7 {
        let fixture = Fixture::new();
        let store = Arc::new(MemoryStore::default());
        let clock = Clock::new();
        fixture.fail_preflight([HostError::Unavailable]);
        let scheduler = fixture.install(&store, &clock).await;
        assert_blocked(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id,
            ReadyBlockKind::Transient,
        );
        drop(scheduler);
        let mut corrupt = store.value(&fixture.key());
        match variant {
            0 => corrupt.revision = 0,
            1 => corrupt.record.attempts = READY_RETRY_LIMIT + 1,
            2 => corrupt.record.identity_hash = "invalid".into(),
            3 => corrupt.record.expires_at_ms = corrupt.record.created_at_ms + 3_600_001,
            4 => corrupt.record.phase = ReadyRetryPhase::Reserved,
            5 => corrupt.record.last_seen_at_ms = corrupt.record.created_at_ms - 1,
            _ => corrupt.record.next_check_at_ms = corrupt.record.expires_at_ms + 1,
        }
        store.replace(&fixture.key(), corrupt.clone());
        let scheduler = fixture.install(&store, &clock).await;
        assert_blocked(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id,
            ReadyBlockKind::Resource,
        );
        assert_eq!(store.value(&fixture.key()), corrupt);
        assert_eq!(fixture.host.preflights.lock().unwrap().len(), 1);
        fixture.host.assert_no_execution();
    }
}

#[tokio::test(start_paused = true)]
async fn definite_scope_cas_rejections_keep_charged_attempts_but_allow_fresh_selection() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture
        .host
        .dispatch_actions
        .lock()
        .unwrap()
        .extend([DispatchAction::ScopeConflict; 3]);
    let scheduler = fixture.install(&store, &clock).await;
    for attempt in 1..=3 {
        assert!(matches!(
            scheduler.reconcile(fixture.host.as_ref()).await,
            Err(Error::RevisionConflict)
        ));
        let record = store.value(&fixture.key()).record;
        assert_eq!(record.attempts, attempt);
        assert!(record.request.is_none());
        assert_eq!(record.expires_at_ms, START_MS + LIFETIME_MS);
        assert!(fixture
            .host
            .service()
            .published()
            .unwrap()
            .1
            .assignments
            .is_empty());
        if attempt < 3 {
            assert_deferred(
                scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
                &fixture.grant.work_id,
                record.next_check_at_ms,
                attempt,
            );
            clock
                .advance(if attempt == 1 { 1_000 } else { 2_000 })
                .await;
        }
    }
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    assert_eq!(
        store.value(&fixture.key()).record.phase,
        ReadyRetryPhase::Quarantined
    );
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 3);
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    assert!(fixture
        .host
        .service()
        .published()
        .unwrap()
        .1
        .assignments
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_new_canonical_generation_never_reuses_an_old_recovery_intent() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    let scheduler = fixture.install(&store, &clock).await;
    let first_key = dispatched_key(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
    );
    let original_retry = store.value(&fixture.key()).record;
    let snapshot = fixture.host.service().published().unwrap().1;
    let old = snapshot.assignments.values().next().unwrap().clone();
    let receipt = RuntimeReceipt {
        dispatch_key: first_key.clone(),
        spec_hash: snapshot.intents[&first_key].spec_hash.clone(),
        run_id: "controlled-first-run".into(),
        session_id: "controlled-first-child".into(),
    };
    drop(snapshot);
    fixture.host.execute(
        Principal::Runtime,
        "admit-and-stop-original",
        vec![
            Operation::Admitted {
                assignment_id: old.id.clone(),
                receipt: receipt.clone(),
            },
            Operation::RuntimeStopped {
                assignment_id: old.id,
                receipt,
                completed: false,
            },
        ],
    );
    let supervisor = || Principal::Supervisor {
        session_id: binding().supervisor_session_id,
    };
    fixture.host.execute(
        supervisor(),
        "explicit-reopen",
        vec![Operation::Reopen {
            work_id: fixture.grant.work_id.clone(),
        }],
    );
    fixture.host.execute(
        supervisor(),
        "explicit-second-start",
        vec![Operation::Start {
            work_id: fixture.grant.work_id.clone(),
            temp_id: "assignment".into(),
            workspace: Some(fixture.grant.workspace.clone()),
        }],
    );
    let current = fixture.host.service().published().unwrap().1;
    let work = &current.tickets[&fixture.grant.work_id];
    assert_eq!(work.generation, 2);
    let second_key = current.assignments[work.active_assignment.as_ref().unwrap()]
        .dispatch_key
        .clone();
    assert_ne!(first_key, second_key);
    drop(current);
    drop(scheduler);
    clock.advance(5_000).await;
    let scheduler = fixture.install(&store, &clock).await;
    let report = scheduler.reconcile(fixture.host.as_ref()).await.unwrap();
    assert!(!matches!(report, ReadyReconcile::Dispatched { .. }));
    assert_eq!(store.value(&fixture.key()).record, original_retry);
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    assert!(fixture.host.ensures.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .host
            .service()
            .published()
            .unwrap()
            .1
            .assignments
            .len(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn a_work_with_unreadable_retry_metadata_cannot_starve_a_healthy_work() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    store
        .load_errors
        .lock()
        .unwrap()
        .insert(MemoryStore::map_key(&fixture.key()), StorageError::Corrupt);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Resource,
    );
    assert!(fixture.host.preflights.lock().unwrap().is_empty());
    fixture.host.assert_no_execution();

    let healthy = fixture.add_work();
    scheduler.revoke().await;
    let mut policy = fixture.policy();
    // A remains first in the policy and in canonical creation order. Its store
    // error must be isolated before oldest-first admission can choose B.
    policy.grants.push(healthy.clone());
    scheduler
        .install(fixture.host.as_ref(), policy)
        .await
        .unwrap();
    let key = dispatched_key(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &healthy.work_id,
    );
    let snapshot = fixture.host.service().published().unwrap().1;
    let bad = &snapshot.tickets[&fixture.grant.work_id];
    assert_eq!(bad.generation, 0);
    assert_eq!(bad.state, WorkState::Ready);
    assert!(bad.active_assignment.is_none());
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(
        snapshot.intents[&key].immutable_spec.work_id,
        healthy.work_id
    );
    let commands = fixture.host.dispatches.lock().unwrap().clone();
    assert_eq!(commands.len(), 1);
    assert!(matches!(
        commands[0].operations.as_slice(),
        [Operation::Start { work_id, .. }] if work_id == &healthy.work_id
    ));
    assert_eq!(
        *fixture.host.preflights.lock().unwrap(),
        vec![healthy.work_id.clone()]
    );
    assert!(!store
        .data
        .lock()
        .unwrap()
        .records
        .contains_key(&MemoryStore::map_key(&fixture.key())));

    clock.advance(5_000).await;
    assert_eq!(
        dispatched_key(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &healthy.work_id
        ),
        key
    );
    let ensures = fixture.host.ensures.lock().unwrap().clone();
    assert_eq!(ensures.len(), 1);
    assert_eq!(ensures[0].0, key);
    assert_eq!(ensures[0].1.work_id, healthy.work_id);
    assert_eq!(fixture.host.dispatches.lock().unwrap().len(), 1);
    let snapshot = fixture.host.service().published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert!(snapshot.tickets[&fixture.grant.work_id]
        .active_assignment
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn sequenced_rollback_during_reservation_never_lowers_the_durable_clock_floor() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    fixture.fail_preflight([HostError::Unavailable]);
    let scheduler = fixture.install(&store, &clock).await;
    assert_blocked(
        scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
        &fixture.grant.work_id,
        ReadyBlockKind::Transient,
    );
    clock.advance(1_000).await;
    let floor = START_MS + 1_000;
    // Read 1 establishes the new floor, read 2 validates reservation, and
    // read 3 rolls back. An extra read while constructing the reservation
    // used to persist floor-1, making the subsequent forward read look safe.
    clock.observe_sequence([floor, floor, floor - 1, floor]);
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await,
        Err(Error::ResourceBlocked(_))
    ));
    let reserved = store.value(&fixture.key()).record;
    assert_eq!(reserved.attempts, 2);
    assert_eq!(reserved.phase, ReadyRetryPhase::Reserved);
    assert_eq!(reserved.last_seen_at_ms, floor);
    let writes = store.data.lock().unwrap().writes.clone();
    assert!(writes
        .windows(2)
        .all(|pair| { pair[0].record.last_seen_at_ms <= pair[1].record.last_seen_at_ms }));
    assert_eq!(fixture.host.fenced_dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.host.fenced_ensures.load(Ordering::SeqCst), 0);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn rollback_after_reservation_cas_commit_is_rejected_before_entering_the_host() {
    let fixture = Fixture::new();
    let store = Arc::new(MemoryStore::default());
    let clock = Clock::new();
    *store.after_reserve_clock.lock().unwrap() = Some((clock.clone(), START_MS - 1));
    let scheduler = fixture.install(&store, &clock).await;
    assert!(matches!(
        scheduler.reconcile(fixture.host.as_ref()).await,
        Err(Error::ResourceBlocked(_))
    ));
    let reserved = store.value(&fixture.key()).record;
    assert_eq!(reserved.phase, ReadyRetryPhase::Reserved);
    assert_eq!(reserved.attempts, 1);
    assert_eq!(reserved.last_seen_at_ms, START_MS);
    assert_eq!(reserved.created_at_ms, START_MS);
    assert_eq!(fixture.host.fenced_dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.host.fenced_ensures.load(Ordering::SeqCst), 0);
    fixture.host.assert_no_execution();
}

#[tokio::test(start_paused = true)]
async fn host_internal_wait_must_recheck_the_fence_before_start_or_child_admission() {
    for pending in [false, true] {
        let fixture = Fixture::new();
        let store = Arc::new(MemoryStore::default());
        let clock = Clock::new();
        let scheduler = fixture.install(&store, &clock).await;
        if pending {
            dispatched_key(
                scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
                &fixture.grant.work_id,
            );
            clock.advance(5_000).await;
            fixture.host.ensure_wait_ms.store(2_000, Ordering::SeqCst);
        } else {
            fixture.host.dispatch_wait_ms.store(2_000, Ordering::SeqCst);
        }
        scheduler.revoke().await;
        let mut policy = fixture.policy();
        policy.lifetime = Duration::from_secs(1);
        scheduler
            .install(fixture.host.as_ref(), policy)
            .await
            .unwrap();
        // Paused Tokio time advances through the controlled Host wait. The
        // policy expires inside the Host, after the outer scheduler check.
        assert_blocked(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            &fixture.grant.work_id,
            ReadyBlockKind::Resource,
        );
        let record = store.value(&fixture.key()).record;
        assert_eq!(record.attempts, if pending { 2 } else { 1 });
        assert!(record.reason.contains("expired"));
        assert_eq!(fixture.host.fenced_dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.host.fenced_ensures.load(Ordering::SeqCst),
            usize::from(pending)
        );
        assert!(fixture.host.ensures.lock().unwrap().is_empty());
        let snapshot = fixture.host.service().published().unwrap().1;
        assert_eq!(snapshot.assignments.len(), usize::from(pending));
        assert_eq!(
            fixture.host.dispatches.lock().unwrap().len(),
            usize::from(pending)
        );
        if pending {
            let assignment = snapshot.assignments.values().next().unwrap();
            assert_eq!(assignment.state, AssignmentState::DispatchPending);
            assert!(assignment.runtime.is_none());
        }
        assert_eq!(
            scheduler.reconcile(fixture.host.as_ref()).await.unwrap(),
            ReadyReconcile::Disabled
        );
    }
}
