use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use bamboo_domain::{ProjectId, WorkspaceBinding};
use bamboo_orchestration::patrol::{checkpoint, host, *};
use bamboo_tickets::{
    Authority, Command, Contract, Operation, Principal, RuntimeReceipt, ScopeBinding, TicketKind,
    TicketService, WorkState,
};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static SERIAL: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "bamboo-patrol-restart-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn binding() -> ScopeBinding {
    ScopeBinding {
        scope_id: "patrol-fixture".into(),
        supervisor_session_id: "supervisor".into(),
        binding_revision: 1,
    }
}

struct Fixture {
    service: Option<TicketService>,
    root: Root,
    scope: Scope,
    receipt: RuntimeReceipt,
    serial: u64,
}

impl Fixture {
    fn service(&self) -> &TicketService {
        self.service.as_ref().unwrap()
    }
    fn authority(&self, principal: Principal) -> Authority {
        Authority::from_verified_host(self.service().published().unwrap().1.binding, principal)
    }
    fn runtime(&self) -> Authority {
        self.authority(Principal::Runtime)
    }
    fn apply(
        &mut self,
        principal: Principal,
        operations: Vec<Operation>,
    ) -> bamboo_tickets::OperationReceipt {
        self.serial += 1;
        let snapshot = self.service().published().unwrap().1;
        self.service()
            .execute(
                &self.authority(principal),
                &Command {
                    operation_id: format!("patrol-fixture-{}", self.serial),
                    binding: snapshot.binding,
                    expected_seq: snapshot.seq,
                    expected_epoch: snapshot.authority_epoch,
                    source: None,
                    operations,
                },
            )
            .unwrap()
    }
    fn new() -> Self {
        let root = Root::new();
        let service = TicketService::open(root.0.join("tickets"), binding()).unwrap();
        let contract = Contract {
            title: "Patrol fixture".into(),
            objective: "Read canonical facts".into(),
            constraints: vec!["No real provider".into()],
            acceptance: vec!["User checks evidence".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let project = ProjectId::parse("patrol-project").unwrap();
        let supervisor = Principal::Supervisor {
            session_id: "supervisor".into(),
        };
        let mut fixture = Self {
            root,
            service: Some(service),
            serial: 0,
            scope: Scope {
                project_id: project.to_string(),
                work_id: String::new(),
                assignment_id: String::new(),
                generation: 1,
                authority_epoch: 1,
            },
            receipt: RuntimeReceipt {
                dispatch_key: String::new(),
                spec_hash: String::new(),
                run_id: "controlled-run".into(),
                session_id: "controlled-session".into(),
            },
        };
        let created = fixture.apply(
            supervisor.clone(),
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract.clone(),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: project.clone(),
                },
            ],
        );
        let work_id = created.ids["work"].clone();
        let repo = fixture.root.0.join("repo");
        let tree = fixture.root.0.join("worktree");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&tree).unwrap();
        let workspace = bamboo_tickets::ExecutionWorkspace {
            repo: repo.to_string_lossy().into_owned(),
            base_commit: "a".repeat(40),
            branch: "fixture".into(),
            worktree: tree.to_string_lossy().into_owned(),
            write_roots: vec![tree.to_string_lossy().into_owned()],
            claims: BTreeSet::from([format!("worktree:{}", tree.to_string_lossy())]),
        };
        let started = fixture.apply(
            supervisor,
            vec![
                Operation::SetProjectContext {
                    work_id: work_id.clone(),
                    context: bamboo_tickets::ProjectContextSnapshot {
                        project_id: project.clone(),
                        project_revision: 1,
                        resource_revision: 1,
                        workspace_binding: WorkspaceBinding {
                            path: workspace.repo.clone(),
                            label: None,
                            git_common_dir: None,
                        },
                        contract_revision: 1,
                        contract,
                        sources: BTreeMap::from([("spec".into(), "v1".into())]),
                    },
                },
                Operation::Ready {
                    work_id: work_id.clone(),
                },
                Operation::Start {
                    work_id: work_id.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(workspace),
                },
            ],
        );
        let assignment_id = started.ids["assignment"].clone();
        let snapshot = fixture.service().published().unwrap().1;
        let assignment = &snapshot.assignments[&assignment_id];
        let intent = &snapshot.intents[&assignment.dispatch_key];
        fixture.receipt.dispatch_key = intent.dispatch_key.clone();
        fixture.receipt.spec_hash = intent.spec_hash.clone();
        fixture.apply(
            Principal::Runtime,
            vec![
                Operation::Admitted {
                    assignment_id: assignment_id.clone(),
                    receipt: fixture.receipt.clone(),
                },
                Operation::Running {
                    assignment_id: assignment_id.clone(),
                },
            ],
        );
        fixture.scope = host::scope_for_assignment(
            fixture.service(),
            &fixture.runtime(),
            project.as_str(),
            &work_id,
            &assignment_id,
        )
        .unwrap();
        fixture
    }
    fn observation(&self) -> host::TicketObservation {
        host::read_observation(self.service(), &self.runtime(), &self.scope).unwrap()
    }
    fn reopen(&mut self) {
        drop(self.service.take());
        self.service = Some(TicketService::open(self.root.0.join("tickets"), binding()).unwrap());
    }
}

struct ClockAt(u64);
impl Clock for ClockAt {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

/// Codec recovery fixture, not a claim of production filesystem acceptance.
/// Actual fs2/rename/fsync fault tests live with the outer Host file adapter.
#[derive(Default)]
struct EncodedJournal {
    bytes: Option<Vec<u8>>,
}
impl CheckpointPort for EncodedJournal {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        self.bytes
            .as_ref()
            .map(|b| checkpoint::decode(scope, b))
            .transpose()
    }
    fn save(&mut self, expected: Option<u64>, next: &Checkpoint) -> Result<u64, PortError> {
        let old = self.load(&next.scope)?;
        if old.as_ref().map(|r| r.version) != expected {
            return Err(PortError::Conflict);
        }
        if let Some(old) = old {
            checkpoint::validate_update(&old.checkpoint, next)?;
        }
        let version = expected.unwrap_or(0) + 1;
        self.bytes = Some(checkpoint::encode(version, next)?);
        Ok(version)
    }
}

struct Probe {
    observed: host::TicketObservation,
    journal: EncodedJournal,
    queues: usize,
    reserve_calls: usize,
}
impl CheckpointPort for Probe {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        self.journal.load(scope)
    }
    fn save(&mut self, expected: Option<u64>, cp: &Checkpoint) -> Result<u64, PortError> {
        self.journal.save(expected, cp)
    }
}
impl ObservationPort for Probe {
    fn observation(&mut self, scope: &Scope, cursor: u64) -> Result<Observation, PortError> {
        self.observed.observation(scope, cursor)
    }
    fn diagnostic_input(
        &mut self,
        scope: &Scope,
        since: u64,
        through: &Observation,
    ) -> Result<DiagnosticInput, PortError> {
        self.observed.diagnostic_input(scope, since, through)
    }
}
impl BudgetPort for Probe {
    fn reserve(&mut self, _request: &DiagnosticRequest) -> Result<Reservation, BudgetBlock> {
        self.reserve_calls += 1;
        Err(BudgetBlock::Unavailable)
    }
    fn settle(&mut self, _reservation: &Reservation, _usage: Usage) -> Result<(), PortError> {
        Err(PortError::Unavailable)
    }
}
impl EscalationPort for Probe {
    fn queue_escalation(&mut self, _record: &Escalation) -> Result<(), PortError> {
        self.queues += 1;
        Ok(())
    }
}
#[derive(Default)]
struct NoProvider {
    calls: usize,
}
impl ReadOnlyDiagnosticProvider for NoProvider {
    fn identity(&self) -> (&str, &str) {
        ("disabled", "disabled")
    }
    fn submit(&mut self, _: &DiagnosticRequest) -> Result<(), PortError> {
        self.calls += 1;
        Err(PortError::Denied)
    }
    fn status(&mut self, _: &OperationId) -> Result<DiagnosticStatus, PortError> {
        self.calls += 1;
        Err(PortError::Denied)
    }
}

fn pass(fixture: &Fixture, journal: EncodedJournal, now: u64) -> (Report, Probe, NoProvider) {
    let mut probe = Probe {
        observed: fixture.observation(),
        journal,
        queues: 0,
        reserve_calls: 0,
    };
    let mut provider = NoProvider::default();
    let report = Patrol::new(Policy::default(), None)
        .unwrap()
        .tick(&fixture.scope, &ClockAt(now), &mut probe, &mut provider)
        .unwrap();
    (report, probe, provider)
}

#[test]
fn production_projection_keeps_unsupported_measurements_unknown_and_is_single_use() {
    let fixture = Fixture::new();
    let mut observed = fixture.observation();
    let value = observed.observation(&fixture.scope, 0).unwrap();
    assert_eq!(value.started_at_ms, None);
    assert_eq!(value.heartbeat_at_ms, None);
    assert_eq!(value.progress, None);
    assert_eq!(value.expected_wait, None);
    assert!(!value.terminal);
    assert_eq!(value.kind, ObservationKind::Snapshot);
    assert_eq!(
        observed.observation(&fixture.scope, value.cursor),
        Err(PortError::Unavailable)
    );
    assert!(observed
        .diagnostic_input(&fixture.scope, 0, &value)
        .is_err());
}

#[test]
fn encoded_restart_and_repeated_canonical_snapshots_keep_one_incident_and_escalation() {
    let fixture = Fixture::new();
    let (report, probe, provider) = pass(&fixture, EncodedJournal::default(), 100);
    assert_eq!(report.status, Status::Incident);
    assert_eq!(provider.calls, 0);
    let id = report.incident.clone();
    let (report, probe, provider) = pass(&fixture, probe.journal, 101);
    assert_eq!(report.status, Status::NeedsSupervisor);
    assert_eq!(probe.queues, 1);
    assert_eq!(probe.reserve_calls, 0);
    assert_eq!(provider.calls, 0);
    let recovered = EncodedJournal {
        bytes: probe.journal.bytes.clone(),
    };
    let (report, mut probe, provider) = pass(&fixture, recovered, 102);
    assert_eq!(report.incident, id);
    assert_eq!(probe.queues, 0);
    assert_eq!(provider.calls, 0);
    let checkpoint = probe.load(&fixture.scope).unwrap().unwrap().checkpoint;
    assert_eq!(checkpoint.incident.unwrap().reason, Reason::EvidenceUnknown);
}

#[test]
fn actual_ticket_writer_restart_retains_original_dispatch_epoch_and_incident_identity() {
    let mut fixture = Fixture::new();
    let (first, probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    let original_scope = fixture.scope.clone();
    fixture.reopen();
    assert!(
        fixture.service().published().unwrap().1.authority_epoch > original_scope.authority_epoch
    );
    let recovered_scope = host::scope_for_assignment(
        fixture.service(),
        &fixture.runtime(),
        &original_scope.project_id,
        &original_scope.work_id,
        &original_scope.assignment_id,
    )
    .unwrap();
    assert_eq!(recovered_scope, original_scope);
    let (second, mut probe, provider) = pass(&fixture, probe.journal, 101);
    assert_eq!(first.incident, second.incident);
    assert_eq!(provider.calls, 0);
    assert_eq!(probe.queues, 1);
    let incident = probe
        .load(&fixture.scope)
        .unwrap()
        .unwrap()
        .checkpoint
        .incident
        .unwrap();
    assert_eq!(
        incident.escalation.unwrap().reason,
        SupervisorReason::ReconcileReceipt
    );
    assert_eq!(incident.attempts, 0);
}

#[test]
fn completed_receipt_reconciliation_changes_no_effective_progress_or_patrol_identity() {
    let mut fixture = Fixture::new();
    let (first, probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    fixture.reopen();
    fixture.apply(
        Principal::Runtime,
        vec![Operation::RuntimeStopped {
            assignment_id: fixture.scope.assignment_id.clone(),
            receipt: fixture.receipt.clone(),
            completed: true,
        }],
    );
    fixture.apply(
        Principal::Runtime,
        vec![Operation::ReconcileCompleted {
            assignment_id: fixture.scope.assignment_id.clone(),
            receipt: fixture.receipt.clone(),
        }],
    );
    let snapshot = fixture.service().published().unwrap().1;
    assert_ne!(
        snapshot.assignments[&fixture.scope.assignment_id].authority_epoch,
        fixture.scope.authority_epoch
    );
    let actual = host::scope_for_assignment(
        fixture.service(),
        &fixture.runtime(),
        &fixture.scope.project_id,
        &fixture.scope.work_id,
        &fixture.scope.assignment_id,
    )
    .unwrap();
    assert_eq!(actual, fixture.scope);
    let (report, mut probe, provider) = pass(&fixture, probe.journal, 101);
    assert_eq!(report.incident, first.incident);
    assert_ne!(report.status, Status::Resolved);
    assert_eq!(provider.calls, 0);
    assert!(!matches!(
        probe
            .load(&fixture.scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident
            .unwrap()
            .phase,
        Phase::Resolved { .. }
    ));
}

#[test]
fn cancellation_is_not_terminal_evidence_until_the_exact_runtime_stop_is_confirmed() {
    let mut fixture = Fixture::new();
    let (_, probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    fixture.apply(
        Principal::Supervisor {
            session_id: "supervisor".into(),
        },
        vec![Operation::Cancel {
            work_id: fixture.scope.work_id.clone(),
        }],
    );
    let (report, probe, _) = pass(&fixture, probe.journal, 101);
    assert_ne!(report.status, Status::Resolved);
    fixture.apply(
        Principal::Runtime,
        vec![Operation::RuntimeStopped {
            assignment_id: fixture.scope.assignment_id.clone(),
            receipt: fixture.receipt.clone(),
            completed: false,
        }],
    );
    let (report, _, provider) = pass(&fixture, probe.journal, 102);
    assert_eq!(report.status, Status::Resolved);
    assert_eq!(provider.calls, 0);
    assert_eq!(
        fixture.service().published().unwrap().1.tickets[&fixture.scope.work_id].state,
        WorkState::Cancelled
    );
}

#[test]
fn foreign_project_generation_epoch_binding_and_worker_readers_are_denied() {
    let fixture = Fixture::new();
    for change in 0..4 {
        let mut scope = fixture.scope.clone();
        match change {
            0 => scope.project_id = "other-project".into(),
            1 => scope.generation += 1,
            2 => scope.authority_epoch += 1,
            _ => scope.assignment_id = "other-assignment".into(),
        }
        assert!(matches!(
            host::read_observation(fixture.service(), &fixture.runtime(), &scope),
            Err(PortError::Denied)
        ));
    }
    let mut wrong = binding();
    wrong.binding_revision += 1;
    let wrong = Authority::from_verified_host(wrong, Principal::Runtime);
    assert!(matches!(
        host::read_observation(fixture.service(), &wrong, &fixture.scope),
        Err(PortError::Denied)
    ));
    let worker = fixture.authority(Principal::Worker {
        assignment_id: fixture.scope.assignment_id.clone(),
        generation: fixture.scope.generation,
        run_id: fixture.receipt.run_id.clone(),
        session_id: fixture.receipt.session_id.clone(),
    });
    assert!(matches!(
        host::read_observation(fixture.service(), &worker, &fixture.scope),
        Err(PortError::Denied)
    ));
}

#[test]
fn codec_rejects_corruption_unknown_schema_and_cross_scope_without_empty_fallback() {
    let fixture = Fixture::new();
    let (_, probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    let bytes = probe.journal.bytes.unwrap();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    corrupt["record"]["checkpoint"]["last_check_ms"] = 9.into();
    assert!(checkpoint::decode(&fixture.scope, &serde_json::to_vec(&corrupt).unwrap()).is_err());
    let mut future: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    future["schema"] = 2.into();
    assert!(checkpoint::decode(&fixture.scope, &serde_json::to_vec(&future).unwrap()).is_err());
    let mut foreign = fixture.scope.clone();
    foreign.project_id = "other".into();
    assert!(checkpoint::decode(&foreign, &bytes).is_err());
    assert!(checkpoint::decode(
        &fixture.scope,
        &vec![b' '; checkpoint::MAX_CHECKPOINT_BYTES + 1]
    )
    .is_err());
    assert_ne!(
        checkpoint::scope_key(&fixture.scope).unwrap(),
        checkpoint::scope_key(&foreign).unwrap()
    );
}

#[test]
fn codec_cas_cannot_reset_attempts_erase_incident_or_redeliver_an_acked_escalation() {
    let fixture = Fixture::new();
    let (_, probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    let (_, mut probe, _) = pass(&fixture, probe.journal, 101);
    let old = probe.load(&fixture.scope).unwrap().unwrap();
    let mut erased = old.checkpoint.clone();
    erased.incident = None;
    assert_eq!(
        probe.save(Some(old.version), &erased),
        Err(PortError::Conflict)
    );
    let mut replay = old.checkpoint.clone();
    replay
        .incident
        .as_mut()
        .unwrap()
        .escalation
        .as_mut()
        .unwrap()
        .queued = false;
    assert_eq!(
        probe.save(Some(old.version), &replay),
        Err(PortError::Conflict)
    );
    assert_eq!(
        probe.save(Some(old.version - 1), &old.checkpoint),
        Err(PortError::Conflict)
    );
    let mut previous = old.checkpoint;
    previous.incident.as_mut().unwrap().attempts = 2;
    let mut reset = previous.clone();
    reset.incident.as_mut().unwrap().attempts = 0;
    assert_eq!(
        checkpoint::validate_update(&previous, &reset),
        Err(PortError::Conflict)
    );
}

#[test]
fn checkpoint_keeps_original_pending_diagnostic_request_and_budget_receipt_on_redecode() {
    let fixture = Fixture::new();
    let (_, mut probe, _) = pass(&fixture, EncodedJournal::default(), 100);
    let mut cp = probe.load(&fixture.scope).unwrap().unwrap().checkpoint;
    let incident = cp.incident.as_mut().unwrap();
    incident.attempts = 1;
    let request = DiagnosticRequest {
        operation: OperationId {
            incident: incident.id.clone(),
            kind: OperationKind::Diagnostic(1),
        },
        model: Model {
            provider: "controlled".into(),
            model: "small".into(),
            max_input_tokens: 100,
            max_output_tokens: 20,
            max_evidence_bytes: 1024,
            max_evidence_refs: 2,
        },
        deadline_ms: 150,
        input: DiagnosticInput {
            scope: fixture.scope.clone(),
            source_revision: cp.source_revision,
            since_cursor: cp.cursor,
            through_cursor: cp.cursor,
            summary: "last tool/wait evidence".into(),
            references: vec![EvidenceRef {
                id: "authorized-ref".into(),
                revision: 1,
            }],
            input_tokens: 50,
        },
    };
    let reservation = Reservation {
        operation: request.operation.clone(),
        request: Box::new(request.clone()),
        receipt: "canonical-budget-receipt".into(),
    };
    incident.pending_settlement = Some(reservation.clone());
    incident.phase = Phase::Diagnosing {
        request: Box::new(request),
        reservation,
    };
    let recovered =
        checkpoint::decode(&fixture.scope, &checkpoint::encode(7, &cp).unwrap()).unwrap();
    assert_eq!(recovered.version, 7);
    assert_eq!(recovered.checkpoint, cp);
    assert_eq!(recovered.checkpoint.incident.unwrap().attempts, 1);
}
