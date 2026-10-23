use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use bamboo_orchestration::patrol::*;

struct FakeClock(u64);
impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

fn scope() -> Scope {
    Scope {
        project_id: "project-a".into(),
        work_id: "work-a".into(),
        assignment_id: "assignment-a".into(),
        generation: 1,
        authority_epoch: 1,
    }
}

fn policy() -> Policy {
    Policy {
        check_interval_ms: 10,
        heartbeat_timeout_ms: 30,
        progress_timeout_ms: 100,
        max_expected_wait_ms: 1000,
        diagnostic_timeout_ms: 50,
        cooldown_ms: 40,
        max_diagnostic_attempts: 2,
        max_escalation_deliveries: 2,
    }
}

fn model() -> Model {
    Model {
        provider: "controlled-native".into(),
        model: "independent-small".into(),
        max_input_tokens: 100,
        max_output_tokens: 20,
        max_evidence_bytes: 1024,
        max_evidence_refs: 4,
    }
}

fn observation() -> Observation {
    Observation {
        scope: scope(),
        kind: ObservationKind::Snapshot,
        source_revision: 10,
        cursor: 10,
        evidence_revision: 2,
        started_at_ms: Some(0),
        heartbeat_at_ms: Some(100),
        progress: Some(Progress {
            sequence: 1,
            at_ms: 0,
        }),
        expected_wait: None,
        repeated_errors: false,
        execution_unknown: false,
        terminal: false,
    }
}

#[derive(Clone)]
struct Charge {
    reservation: Reservation,
    cap: u64,
    settled: Option<Usage>,
}

#[derive(Default)]
struct Durable {
    checkpoints: BTreeMap<Scope, StoredCheckpoint>,
    charges: BTreeMap<OperationId, Charge>,
    queue: BTreeMap<OperationId, Escalation>,
    calls: Vec<&'static str>,
    save_count: usize,
}

struct FakeHost {
    durable: Rc<RefCell<Durable>>,
    observation: Observation,
    input_scope: Option<Scope>,
    input_tokens: u32,
    input_bytes: usize,
    denied: bool,
    budget_block: Option<BudgetBlock>,
    // Three fixture ledgers represent Work/daily/total ceilings. Production has
    // only BudgetPort, so a restart cannot reset an in-module allowance.
    limits: [u64; 3],
    fail_save: Option<usize>,
    fail_queue: bool,
    fail_settle: bool,
}

impl FakeHost {
    fn new() -> Self {
        Self {
            durable: Rc::default(),
            observation: observation(),
            input_scope: None,
            input_tokens: 40,
            input_bytes: 32,
            denied: false,
            budget_block: None,
            limits: [1000; 3],
            fail_save: None,
            fail_queue: false,
            fail_settle: false,
        }
    }
    fn checkpoint(&self) -> Checkpoint {
        self.durable.borrow().checkpoints[&self.observation.scope]
            .checkpoint
            .clone()
    }
    fn incident(&self) -> Incident {
        self.checkpoint().incident.unwrap()
    }
    fn usage(&self) -> u64 {
        self.durable
            .borrow()
            .charges
            .values()
            .map(|charge| match charge.settled {
                Some(Usage::Known {
                    input_tokens,
                    output_tokens,
                }) => u64::from(input_tokens + output_tokens),
                _ => charge.cap,
            })
            .sum()
    }
    fn fresh_heartbeat(&mut self, at: u64) {
        self.observation.heartbeat_at_ms = Some(at);
        self.observation.cursor += 1;
        self.observation.source_revision += 1;
    }
}

impl ObservationPort for FakeHost {
    fn observation(&mut self, _: &Scope, _: u64) -> Result<Observation, PortError> {
        self.durable.borrow_mut().calls.push("observe");
        if self.denied {
            Err(PortError::Denied)
        } else {
            Ok(self.observation.clone())
        }
    }
    fn diagnostic_input(
        &mut self,
        scope: &Scope,
        since_cursor: u64,
        through: &Observation,
    ) -> Result<DiagnosticInput, PortError> {
        self.durable.borrow_mut().calls.push("input");
        Ok(DiagnosticInput {
            scope: self.input_scope.clone().unwrap_or_else(|| scope.clone()),
            source_revision: through.source_revision,
            since_cursor,
            through_cursor: through.cursor,
            summary: "x".repeat(self.input_bytes),
            input_tokens: self.input_tokens,
            references: vec![EvidenceRef {
                id: "last-tool-receipt".into(),
                revision: 2,
            }],
        })
    }
}

impl CheckpointPort for FakeHost {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        Ok(self.durable.borrow().checkpoints.get(scope).cloned())
    }
    fn save(&mut self, expected: Option<u64>, checkpoint: &Checkpoint) -> Result<u64, PortError> {
        let mut state = self.durable.borrow_mut();
        state.save_count += 1;
        if self.fail_save == Some(state.save_count) {
            return Err(PortError::Conflict);
        }
        let current = state.checkpoints.get(&checkpoint.scope).map(|s| s.version);
        if current != expected {
            return Err(PortError::Conflict);
        }
        let version = expected.unwrap_or(0) + 1;
        state.checkpoints.insert(
            checkpoint.scope.clone(),
            StoredCheckpoint {
                version,
                checkpoint: checkpoint.clone(),
            },
        );
        state.calls.push("save");
        Ok(version)
    }
}

impl BudgetPort for FakeHost {
    fn reserve(&mut self, request: &DiagnosticRequest) -> Result<Reservation, BudgetBlock> {
        self.durable.borrow_mut().calls.push("reserve");
        if let Some(block) = self.budget_block {
            return Err(block);
        }
        if let Some(charge) = self.durable.borrow().charges.get(&request.operation) {
            return Ok(charge.reservation.clone());
        }
        let cap =
            u64::from(request.model.max_input_tokens) + u64::from(request.model.max_output_tokens);
        let used = self.usage();
        for (limit, block) in
            self.limits
                .iter()
                .zip([BudgetBlock::Work, BudgetBlock::Daily, BudgetBlock::Total])
        {
            if used + cap > *limit {
                return Err(block);
            }
        }
        let reservation = Reservation {
            operation: request.operation.clone(),
            request: Box::new(request.clone()),
            receipt: "budget-receipt".into(),
        };
        self.durable.borrow_mut().charges.insert(
            request.operation.clone(),
            Charge {
                reservation: reservation.clone(),
                cap,
                settled: None,
            },
        );
        Ok(reservation)
    }
    fn settle(&mut self, reservation: &Reservation, usage: Usage) -> Result<(), PortError> {
        if self.fail_settle {
            return Err(PortError::Unavailable);
        }
        let mut state = self.durable.borrow_mut();
        let charge = state
            .charges
            .get_mut(&reservation.operation)
            .ok_or(PortError::Denied)?;
        if charge.reservation != *reservation {
            return Err(PortError::Denied);
        }
        // Unknown is conservative and idempotent, never overwritten with free credit.
        if charge.settled.is_none() {
            charge.settled = Some(usage);
        }
        state.calls.push("settle");
        Ok(())
    }
}

impl EscalationPort for FakeHost {
    fn queue_escalation(&mut self, escalation: &Escalation) -> Result<(), PortError> {
        let mut state = self.durable.borrow_mut();
        state.calls.push("queue");
        if self.fail_queue {
            return Err(PortError::Unavailable);
        }
        state
            .queue
            .entry(escalation.operation.clone())
            .or_insert_with(|| escalation.clone());
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Messages,
    NativeRpc,
}
#[derive(Debug, PartialEq, Eq)]
enum Wire {
    Messages {
        model: String,
        messages: Vec<String>,
        max_output_tokens: u32,
    },
    NativeRpc {
        method: &'static str,
        input: String,
        ceiling: u32,
    },
}

struct ControlledProvider {
    durable: Rc<RefCell<Durable>>,
    identity: Model,
    protocol: Protocol,
    submits: Vec<DiagnosticRequest>,
    wires: Vec<Wire>,
    polls: Vec<OperationId>,
    status: DiagnosticStatus,
    submit_error: bool,
}
impl ControlledProvider {
    fn new(host: &FakeHost) -> Self {
        Self {
            durable: host.durable.clone(),
            identity: model(),
            protocol: Protocol::NativeRpc,
            submits: vec![],
            wires: vec![],
            polls: vec![],
            status: DiagnosticStatus::Pending,
            submit_error: false,
        }
    }
    fn finish(&mut self, advice: Advice) {
        self.status = DiagnosticStatus::Finished {
            diagnosis: Diagnosis {
                advice,
                evidence: vec!["last-tool-receipt".into()],
                uncertain: false,
                note: "bounded diagnostic suggestion".into(),
            },
            usage: Usage::Known {
                input_tokens: 40,
                output_tokens: 10,
            },
        };
    }
}
impl ReadOnlyDiagnosticProvider for ControlledProvider {
    fn identity(&self) -> (&str, &str) {
        (&self.identity.provider, &self.identity.model)
    }
    fn submit(&mut self, request: &DiagnosticRequest) -> Result<(), PortError> {
        let state = self.durable.borrow();
        let cp = &state.checkpoints[&request.input.scope].checkpoint;
        assert!(matches!(&cp.incident.as_ref().unwrap().phase,
            Phase::Diagnosing { request: saved, .. } if saved.as_ref() == request));
        assert!(state.charges.contains_key(&request.operation));
        drop(state);
        self.durable.borrow_mut().calls.push("submit");
        self.submits.push(request.clone());
        self.wires.push(match self.protocol {
            Protocol::Messages => Wire::Messages {
                model: request.model.model.clone(),
                messages: vec![request.input.summary.clone()],
                max_output_tokens: request.model.max_output_tokens,
            },
            Protocol::NativeRpc => Wire::NativeRpc {
                method: "inspect",
                input: request.input.summary.clone(),
                ceiling: request.model.max_output_tokens,
            },
        });
        if self.submit_error {
            Err(PortError::OutcomeUnknown)
        } else {
            Ok(())
        }
    }
    fn status(&mut self, operation: &OperationId) -> Result<DiagnosticStatus, PortError> {
        self.polls.push(operation.clone());
        Ok(self.status.clone())
    }
}

fn tick(
    patrol: &Patrol,
    host: &mut FakeHost,
    provider: &mut ControlledProvider,
    now: u64,
) -> Report {
    let scope = host.observation.scope.clone();
    patrol
        .tick(&scope, &FakeClock(now), host, provider)
        .unwrap()
}

fn configured() -> Patrol {
    Patrol::new(policy(), Some(model())).unwrap()
}

fn start_diagnosis(patrol: &Patrol, host: &mut FakeHost, provider: &mut ControlledProvider) {
    assert_eq!(tick(patrol, host, provider, 100).status, Status::Incident);
    assert_eq!(tick(patrol, host, provider, 100).status, Status::Diagnosing);
    assert_eq!(provider.submits.len(), 1);
}

#[test]
fn healthy_progress_and_empty_model_configuration_make_zero_provider_calls() {
    for configured_model in [None, Some(model())] {
        let patrol = Patrol::new(policy(), configured_model).unwrap();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        for now in [100, 200, 300, 400, 500] {
            host.fresh_heartbeat(now);
            host.observation.progress = Some(Progress {
                sequence: now,
                at_ms: now,
            });
            assert_eq!(
                tick(&patrol, &mut host, &mut provider, now).status,
                Status::Healthy
            );
        }
        assert!(provider.submits.is_empty());
        assert!(provider.polls.is_empty());
        assert!(host.durable.borrow().charges.is_empty());
    }
}

#[test]
fn expected_user_dependency_and_live_tool_waits_do_not_invoke_models() {
    for kind in [WaitKind::User, WaitKind::Dependency, WaitKind::RunningTool] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        host.observation.expected_wait = Some(ExpectedWait {
            kind,
            issued_at_ms: 0,
            until_ms: 500,
            progress_sequence: 1,
            evidence_ref: "wait-receipt".into(),
        });
        for now in [100, 200, 400, 495] {
            assert_eq!(
                tick(&patrol, &mut host, &mut provider, now).status,
                Status::ExpectedWait
            );
        }
        assert_eq!(
            tick(&patrol, &mut host, &mut provider, 495).next_check_ms,
            500
        );
        assert!(provider.submits.is_empty());
        assert!(host.durable.borrow().queue.is_empty());
    }
}

#[test]
fn heartbeat_does_not_reset_progress_or_duplicate_incident_or_diagnostic() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    let id = host.incident().id;
    for now in 101..120 {
        host.fresh_heartbeat(now);
        assert_eq!(
            tick(&patrol, &mut host, &mut provider, now).incident,
            Some(id.clone())
        );
    }
    assert_eq!(host.incident().reason, Reason::NoProgress);
    assert_eq!(host.incident().attempts, 1);
    assert_eq!(provider.submits.len(), 1);
    assert_eq!(host.durable.borrow().charges.len(), 1);
}

#[test]
fn initial_missing_heartbeat_has_a_finite_startup_grace_period() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    host.observation.heartbeat_at_ms = None;
    host.observation.progress = None;
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 29).status,
        Status::Healthy
    );
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 30).status,
        Status::Incident
    );
    assert_eq!(host.incident().reason, Reason::MissingHeartbeat);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 30).status,
        Status::NeedsSupervisor
    );
    assert_eq!(
        host.durable.borrow().queue.values().next().unwrap().reason,
        SupervisorReason::ReconcileReceipt
    );
    assert!(provider.submits.is_empty());
}

#[test]
fn expired_or_unverifiable_wait_is_a_program_diagnostic_without_llm() {
    for (until_ms, progress_sequence, expected) in [
        (100, 1, Reason::WaitExpired),
        (2000, 1, Reason::EvidenceUnknown),
        (500, 99, Reason::EvidenceUnknown),
    ] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        host.observation.expected_wait = Some(ExpectedWait {
            kind: WaitKind::RunningTool,
            issued_at_ms: 0,
            until_ms,
            progress_sequence,
            evidence_ref: "live-receipt".into(),
        });
        tick(&patrol, &mut host, &mut provider, 100);
        tick(&patrol, &mut host, &mut provider, 100);
        assert_eq!(host.incident().reason, expected);
        assert_eq!(host.durable.borrow().queue.len(), 1);
        assert!(provider.submits.is_empty());
    }
}

#[test]
fn repeated_tool_error_is_an_incident_even_with_recent_progress() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    host.observation.progress.as_mut().unwrap().at_ms = 99;
    host.observation.repeated_errors = true;
    start_diagnosis(&patrol, &mut host, &mut provider);
    assert_eq!(host.incident().reason, Reason::RepeatedErrors);
}

#[test]
fn deterministic_unknown_execution_never_replays_or_spawns_a_diagnostic() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    host.observation.execution_unknown = true;
    tick(&patrol, &mut host, &mut provider, 100);
    for now in 100..130 {
        tick(&patrol, &mut host, &mut provider, now);
    }
    assert_eq!(host.durable.borrow().queue.len(), 1);
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::ReconcileReceipt
    );
    assert!(provider.submits.is_empty());
}

#[test]
fn missing_patrol_model_does_not_fall_back_to_supervisor_model() {
    let patrol = Patrol::new(policy(), None).unwrap();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    tick(&patrol, &mut host, &mut provider, 100);
    tick(&patrol, &mut host, &mut provider, 100);
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::DiagnosticUnavailable
    );
    assert!(provider.submits.is_empty());
}

#[test]
fn native_rpc_and_messages_adapters_share_only_the_read_only_contract() {
    for protocol in [Protocol::NativeRpc, Protocol::Messages] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        provider.protocol = protocol;
        start_diagnosis(&patrol, &mut host, &mut provider);
        provider.finish(Advice::Observe);
        assert_eq!(
            tick(&patrol, &mut host, &mut provider, 101).status,
            Status::Cooldown
        );
        assert!(matches!(
            provider.wires[0],
            Wire::NativeRpc {
                method: "inspect",
                ceiling: 20,
                ..
            } | Wire::Messages {
                max_output_tokens: 20,
                ..
            }
        ));
        assert_eq!(host.usage(), 50);
        assert_eq!(host.incident().attempts, 1);
        assert!(!matches!(host.incident().phase, Phase::Resolved { .. }));
    }
}

#[test]
fn replan_conflict_acceptance_authority_and_uncertainty_escalate_once() {
    for reason in [
        SupervisorReason::Replan,
        SupervisorReason::ResourceConflict,
        SupervisorReason::AcceptanceDispute,
        SupervisorReason::AuthorityChange,
        SupervisorReason::Uncertain,
    ] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        start_diagnosis(&patrol, &mut host, &mut provider);
        provider.finish(Advice::Escalate(reason));
        for now in 101..120 {
            tick(&patrol, &mut host, &mut provider, now);
        }
        assert_eq!(host.durable.borrow().queue.len(), 1);
        assert_eq!(host.incident().escalation.unwrap().reason, reason);
        assert_eq!(provider.submits.len(), 1);
    }
}

#[test]
fn observation_advice_and_recovery_ack_do_not_resolve_without_progress() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    provider.finish(Advice::Observe);
    tick(&patrol, &mut host, &mut provider, 101);
    // Receipt/ACK/heartbeat traffic changes cursors, not meaningful progress.
    host.fresh_heartbeat(120);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 120).status,
        Status::Cooldown
    );
    host.fresh_heartbeat(141);
    tick(&patrol, &mut host, &mut provider, 141);
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::PersistentAnomaly
    );
    assert_eq!(provider.submits.len(), 1);
    host.fresh_heartbeat(142);
    host.observation.progress = Some(Progress {
        sequence: 2,
        at_ms: 142,
    });
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 142).status,
        Status::Resolved
    );
    assert!(!host.observation.terminal);
    // A later, distinct stall opens a new incident only at the new progress boundary.
    host.fresh_heartbeat(242);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 242).status,
        Status::Incident
    );
    assert_eq!(host.incident().id.progress_sequence, 2);
}

#[test]
fn authoritative_terminal_resolves_without_a_model_and_keeps_unknown_charge() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    host.observation.terminal = true;
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 101).status,
        Status::Resolved
    );
    assert_eq!(host.usage(), 120);
    assert!(provider.polls.is_empty());
}

#[test]
fn diagnostic_session_timeout_escalates_without_recursive_patrol_sessions() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    // A later legitimate expected wait does not disable the diagnostic watchdog.
    host.observation.expected_wait = Some(ExpectedWait {
        kind: WaitKind::User,
        issued_at_ms: 100,
        until_ms: 1000,
        progress_sequence: 1,
        evidence_ref: "wait".into(),
    });
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 150).status,
        Status::NeedsSupervisor
    );
    assert_eq!(host.incident().phase, Phase::Blocked(Block::SessionTimeout));
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::DiagnosticTimeout
    );
    for now in [200, 300, 400] {
        tick(&patrol, &mut host, &mut provider, now);
    }
    assert_eq!(provider.submits.len(), 1);
    assert!(provider.polls.is_empty());
    assert_eq!(host.usage(), 120);
}

#[test]
fn diagnostic_checkpoint_survives_restart_and_reconciles_original_operation() {
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&configured(), &mut host, &mut provider);
    let operation = provider.submits[0].operation.clone();
    let restarted = configured();
    let mut recovered_provider = ControlledProvider::new(&host);
    recovered_provider.finish(Advice::Escalate(SupervisorReason::Replan));
    tick(&restarted, &mut host, &mut recovered_provider, 101);
    assert!(recovered_provider.submits.is_empty());
    assert_eq!(recovered_provider.polls, vec![operation]);
    assert_eq!(host.durable.borrow().charges.len(), 1);
    assert_eq!(host.durable.borrow().queue.len(), 1);
}

#[test]
fn crash_after_reservation_before_checkpoint_never_submits_and_reuses_charge() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    tick(&patrol, &mut host, &mut provider, 100);
    host.fail_save = Some(host.durable.borrow().save_count + 1);
    assert_eq!(
        patrol.tick(&scope(), &FakeClock(100), &mut host, &mut provider),
        Err(Error::Port(PortError::Conflict))
    );
    assert!(provider.submits.is_empty());
    assert_eq!(host.usage(), 120);
    host.fail_save = None;
    tick(&configured(), &mut host, &mut provider, 100);
    assert_eq!(provider.submits.len(), 1);
    assert_eq!(host.durable.borrow().charges.len(), 1);
}

#[test]
fn crash_after_checkpoint_before_submit_reconciles_unknown_without_free_retry() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    // The durable state is identical if the process died immediately before submit.
    let mut restarted_provider = ControlledProvider::new(&host);
    restarted_provider.status = DiagnosticStatus::OutcomeUnknown;
    tick(&configured(), &mut host, &mut restarted_provider, 101);
    assert!(restarted_provider.submits.is_empty());
    assert_eq!(host.usage(), 120);
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::ReconcileReceipt
    );
}

#[test]
fn crash_after_queue_before_ack_replays_same_logical_escalation_only() {
    let patrol = Patrol::new(policy(), None).unwrap();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    tick(&patrol, &mut host, &mut provider, 100);
    // Second tick saves intent before queue and then loses its save/ACK.
    host.fail_save = Some(host.durable.borrow().save_count + 2);
    assert!(patrol
        .tick(&scope(), &FakeClock(100), &mut host, &mut provider)
        .is_err());
    assert_eq!(host.durable.borrow().queue.len(), 1);
    let operation = host.incident().escalation.unwrap().operation;
    host.fail_save = None;
    host.fresh_heartbeat(140);
    tick(&patrol, &mut host, &mut provider, 140);
    assert_eq!(host.durable.borrow().queue.len(), 1);
    assert_eq!(host.incident().escalation.unwrap().operation, operation);
}

#[test]
fn work_daily_total_and_unavailable_budgets_fail_before_provider_submission() {
    for block in [
        BudgetBlock::Work,
        BudgetBlock::Daily,
        BudgetBlock::Total,
        BudgetBlock::Unavailable,
    ] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        host.budget_block = Some(block);
        tick(&patrol, &mut host, &mut provider, 100);
        tick(&patrol, &mut host, &mut provider, 100);
        assert_eq!(host.incident().phase, Phase::Blocked(Block::Budget(block)));
        let previous_checks = host
            .durable
            .borrow()
            .calls
            .iter()
            .filter(|c| **c == "observe")
            .count();
        for now in 101..120 {
            tick(&patrol, &mut host, &mut provider, now);
        }
        assert!(
            host.durable
                .borrow()
                .calls
                .iter()
                .filter(|c| **c == "observe")
                .count()
                > previous_checks
        );
        assert!(provider.submits.is_empty());
        assert_eq!(host.durable.borrow().queue.len(), 1);
    }
}

#[test]
fn token_caps_and_per_work_limit_prevent_a_second_reserved_attempt_after_restart() {
    let patrol = configured();
    let mut host = FakeHost::new();
    host.limits[0] = 169; // first reconciles to 50; another hard reservation needs 120.
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    provider.finish(Advice::Observe);
    tick(&patrol, &mut host, &mut provider, 101);
    host.fresh_heartbeat(141);
    host.observation.evidence_revision += 1;
    tick(&configured(), &mut host, &mut provider, 141);
    assert_eq!(
        host.incident().phase,
        Phase::Blocked(Block::Budget(BudgetBlock::Work))
    );
    assert_eq!(provider.submits.len(), 1);
    assert_eq!(host.usage(), 50);
}

#[test]
fn cooldown_and_attempt_limit_survive_repeated_events_and_restart() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    provider.finish(Advice::Observe);
    tick(&patrol, &mut host, &mut provider, 101);
    for now in 102..141 {
        host.fresh_heartbeat(now);
        tick(&patrol, &mut host, &mut provider, now);
    }
    assert_eq!(provider.submits.len(), 1);
    host.fresh_heartbeat(141);
    host.observation.evidence_revision += 1;
    assert_eq!(
        tick(&configured(), &mut host, &mut provider, 141).status,
        Status::Diagnosing
    );
    tick(&patrol, &mut host, &mut provider, 142);
    host.fresh_heartbeat(182);
    host.observation.evidence_revision += 1;
    tick(&configured(), &mut host, &mut provider, 182);
    assert_eq!(host.incident().phase, Phase::Blocked(Block::AttemptLimit));
    assert_eq!(provider.submits.len(), 2);
}

#[test]
fn provider_failure_unknown_usage_and_invalid_evidence_never_release_reservation() {
    for status in [
        DiagnosticStatus::Failed {
            usage: Usage::Unknown,
        },
        DiagnosticStatus::OutcomeUnknown,
        DiagnosticStatus::Finished {
            diagnosis: Diagnosis {
                advice: Advice::Observe,
                evidence: vec!["foreign-project-ref".into()],
                uncertain: false,
                note: "untrusted".into(),
            },
            usage: Usage::Unknown,
        },
    ] {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        start_diagnosis(&patrol, &mut host, &mut provider);
        provider.status = status;
        tick(&patrol, &mut host, &mut provider, 101);
        for now in 102..120 {
            tick(&patrol, &mut host, &mut provider, now);
        }
        assert_eq!(host.usage(), 120);
        assert_eq!(provider.submits.len(), 1);
        assert_eq!(host.durable.borrow().queue.len(), 1);
        assert!(!matches!(host.incident().phase, Phase::Resolved { .. }));
    }
}

#[test]
fn oversized_or_cross_project_input_and_model_mismatch_fail_closed() {
    for case in 0..4 {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        match case {
            0 => host.input_tokens = 101,
            1 => host.input_bytes = 2000,
            2 => {
                let mut foreign = scope();
                foreign.project_id = "other".into();
                host.input_scope = Some(foreign);
            }
            _ => provider.identity.model = "supervisor-large".into(),
        }
        tick(&patrol, &mut host, &mut provider, 100);
        tick(&patrol, &mut host, &mut provider, 100);
        assert!(provider.submits.is_empty());
        assert!(host.durable.borrow().charges.is_empty());
        assert!(host.incident().escalation.is_some());
    }
}

#[test]
fn stale_generation_epoch_project_and_revoked_scope_cannot_poll_or_submit() {
    for case in 0..4 {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        start_diagnosis(&patrol, &mut host, &mut provider);
        match case {
            0 => host.observation.scope.generation += 1,
            1 => host.observation.scope.authority_epoch += 1,
            2 => host.observation.scope.project_id = "foreign".into(),
            _ => host.denied = true,
        }
        assert!(patrol
            .tick(&scope(), &FakeClock(101), &mut host, &mut provider)
            .is_err());
        assert!(provider.polls.is_empty());
        assert_eq!(provider.submits.len(), 1);
    }
}

#[test]
fn event_gap_or_out_of_order_revision_requires_snapshot_without_new_model_calls() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    tick(&patrol, &mut host, &mut provider, 100);
    host.observation.cursor = 12;
    host.observation.kind = ObservationKind::Delta {
        previous_cursor: 11,
    };
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 101).status,
        Status::ResyncRequired
    );
    assert_eq!(host.checkpoint().cursor, 10);
    assert!(provider.submits.is_empty());
    host.observation.kind = ObservationKind::Snapshot;
    tick(&patrol, &mut host, &mut provider, 101);
    host.observation.cursor = 9;
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 102).status,
        Status::ResyncRequired
    );
    assert_eq!(provider.submits.len(), 1);
}

#[test]
fn queue_delivery_is_bounded_and_blocked_patrol_keeps_checking_execution() {
    let patrol = Patrol::new(policy(), None).unwrap();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    host.fail_queue = true;
    tick(&patrol, &mut host, &mut provider, 100);
    for now in [100, 110, 140, 180, 220, 260] {
        host.fresh_heartbeat(now);
        assert_eq!(
            tick(&patrol, &mut host, &mut provider, now).status,
            Status::NeedsSupervisor
        );
    }
    assert_eq!(host.incident().escalation.unwrap().deliveries, 2);
    assert_eq!(
        host.durable
            .borrow()
            .calls
            .iter()
            .filter(|c| **c == "queue")
            .count(),
        2
    );
    assert!(provider.submits.is_empty());
}

#[test]
fn clock_regression_and_invalid_configuration_do_not_reset_state_or_budget() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    assert_eq!(
        patrol.tick(&scope(), &FakeClock(99), &mut host, &mut provider),
        Err(Error::ClockRegression)
    );
    assert_eq!(host.usage(), 120);
    let mut bad = policy();
    bad.max_diagnostic_attempts = 0;
    assert!(Patrol::new(bad, Some(model())).is_err());
}

#[test]
fn two_projects_keep_incidents_operations_evidence_and_checkpoints_isolated() {
    let patrol = configured();
    let mut a = FakeHost::new();
    let mut b = FakeHost::new();
    b.durable = a.durable.clone();
    b.observation.scope.project_id = "project-b".into();
    let mut provider_a = ControlledProvider::new(&a);
    let mut provider_b = ControlledProvider::new(&b);
    start_diagnosis(&patrol, &mut a, &mut provider_a);
    start_diagnosis(&patrol, &mut b, &mut provider_b);
    assert_ne!(a.incident().id, b.incident().id);
    assert_ne!(
        provider_a.submits[0].operation,
        provider_b.submits[0].operation
    );
    assert_eq!(a.durable.borrow().checkpoints.len(), 2);
    assert_eq!(a.durable.borrow().charges.len(), 2);
}

#[test]
fn ledger_outage_cannot_block_the_independent_diagnostic_timeout() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    host.fail_settle = true;
    host.fresh_heartbeat(150);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 150).status,
        Status::NeedsSupervisor
    );
    let incident = host.incident();
    assert_eq!(incident.phase, Phase::Blocked(Block::SessionTimeout));
    assert!(incident.pending_settlement.is_some());
    assert_eq!(
        incident.escalation.unwrap().reason,
        SupervisorReason::DiagnosticTimeout
    );
    assert_eq!(host.usage(), 120);
    assert!(provider.polls.is_empty());
    assert_eq!(provider.submits.len(), 1);
}

#[test]
fn validated_uncertainty_and_evidence_are_preserved_without_self_declared_completion() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    assert_eq!(provider.submits[0].input.since_cursor, 10);
    provider.status = DiagnosticStatus::Finished {
        diagnosis: Diagnosis {
            advice: Advice::Observe,
            evidence: vec!["last-tool-receipt".into()],
            uncertain: true,
            note: "I think this is done".into(),
        },
        usage: Usage::Known {
            input_tokens: 40,
            output_tokens: 10,
        },
    };
    tick(&patrol, &mut host, &mut provider, 101);
    let incident = host.incident();
    assert_eq!(
        incident.escalation.unwrap().reason,
        SupervisorReason::Uncertain
    );
    let record = incident.last_diagnosis.unwrap();
    assert_eq!(record.source_revision, 10);
    assert_eq!(record.through_cursor, 10);
    assert!(record.diagnosis.uncertain);
    assert_eq!(record.diagnosis.evidence, vec!["last-tool-receipt"]);
    assert!(!matches!(incident.phase, Phase::Resolved { .. }));
    assert!(!host.observation.terminal);
}

#[test]
fn crash_after_submit_before_save_recovers_original_session_without_resubmission() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    tick(&patrol, &mut host, &mut provider, 100);
    host.fail_save = Some(host.durable.borrow().save_count + 2);
    assert_eq!(
        patrol.tick(&scope(), &FakeClock(100), &mut host, &mut provider),
        Err(Error::Port(PortError::Conflict))
    );
    assert_eq!(provider.submits.len(), 1);
    let operation = provider.submits[0].operation.clone();
    host.fail_save = None;
    let mut restarted_provider = ControlledProvider::new(&host);
    tick(&configured(), &mut host, &mut restarted_provider, 101);
    assert!(restarted_provider.submits.is_empty());
    assert_eq!(restarted_provider.polls, vec![operation]);
    assert_eq!(host.usage(), 120);
}

#[test]
fn next_checkpoint_is_persisted_at_the_session_deadline_even_with_a_long_check_interval() {
    let mut custom = policy();
    custom.check_interval_ms = 1000;
    let patrol = Patrol::new(custom, Some(model())).unwrap();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    assert_eq!(host.checkpoint().next_check_ms, 150);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 149).next_check_ms,
        150
    );
    assert_eq!(host.checkpoint().next_check_ms, 150);
    assert_eq!(
        tick(&patrol, &mut host, &mut provider, 150).status,
        Status::NeedsSupervisor
    );
}

#[test]
fn corrupt_recovered_operation_fails_before_provider_status_or_new_credit() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    {
        let mut durable = host.durable.borrow_mut();
        let incident = durable
            .checkpoints
            .get_mut(&scope())
            .unwrap()
            .checkpoint
            .incident
            .as_mut()
            .unwrap();
        let Phase::Diagnosing { request, .. } = &mut incident.phase else {
            panic!("diagnosing");
        };
        request.operation.kind = OperationKind::Diagnostic(99);
    }
    assert_eq!(
        patrol.tick(&scope(), &FakeClock(101), &mut host, &mut provider),
        Err(Error::InvalidCheckpoint)
    );
    assert!(provider.polls.is_empty());
    assert_eq!(provider.submits.len(), 1);
    assert_eq!(host.usage(), 120);
}

#[test]
fn over_cap_usage_keeps_the_full_reservation_and_requests_host_reconciliation() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    start_diagnosis(&patrol, &mut host, &mut provider);
    provider.status = DiagnosticStatus::Finished {
        diagnosis: Diagnosis {
            advice: Advice::Observe,
            evidence: vec!["last-tool-receipt".into()],
            uncertain: false,
            note: "bounded".into(),
        },
        usage: Usage::Known {
            input_tokens: 100,
            output_tokens: 21,
        },
    };
    tick(&patrol, &mut host, &mut provider, 101);
    assert_eq!(host.usage(), 120);
    assert_eq!(host.incident().phase, Phase::Blocked(Block::UnknownUsage));
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::InvalidDiagnostic
    );
    assert_eq!(provider.submits.len(), 1);
}

#[test]
fn orphan_reservation_cannot_authorize_changed_caps_model_input_or_deadline() {
    for changed in 0..4 {
        let patrol = configured();
        let mut host = FakeHost::new();
        let mut provider = ControlledProvider::new(&host);
        tick(&patrol, &mut host, &mut provider, 100);
        host.fail_save = Some(host.durable.borrow().save_count + 1);
        assert!(patrol
            .tick(&scope(), &FakeClock(100), &mut host, &mut provider)
            .is_err());
        assert_eq!(host.usage(), 120);
        assert!(provider.submits.is_empty());
        host.fail_save = None;
        let mut changed_model = model();
        let mut now = 100;
        match changed {
            0 => changed_model.max_input_tokens = 200,
            1 => changed_model.model = "different-model".into(),
            2 => host.input_bytes = 33,
            _ => now = 101,
        }
        provider.identity = changed_model.clone();
        let restarted = Patrol::new(policy(), Some(changed_model)).unwrap();
        tick(&restarted, &mut host, &mut provider, now);
        assert_eq!(host.incident().phase, Phase::Blocked(Block::UnknownUsage));
        assert_eq!(host.incident().attempts, 1);
        assert_eq!(
            host.incident().escalation.unwrap().reason,
            SupervisorReason::ReconcileReceipt
        );
        assert!(provider.submits.is_empty());
        assert_eq!(host.usage(), 120);
        assert_eq!(host.durable.borrow().charges.len(), 1);
    }
}

#[test]
fn unavailable_start_measurement_is_unknown_without_semantic_calls_or_invented_progress() {
    let patrol = configured();
    let mut host = FakeHost::new();
    let mut provider = ControlledProvider::new(&host);
    host.observation.started_at_ms = None;
    host.observation.heartbeat_at_ms = None;
    host.observation.progress = None;
    tick(&patrol, &mut host, &mut provider, 100);
    assert_eq!(host.incident().reason, Reason::EvidenceUnknown);
    for now in [101, 200, 1000] {
        tick(&patrol, &mut host, &mut provider, now);
    }
    assert_eq!(
        host.incident().escalation.unwrap().reason,
        SupervisorReason::DeterministicAnomaly
    );
    assert!(provider.submits.is_empty());
    assert!(provider.polls.is_empty());
    assert_eq!(host.durable.borrow().queue.len(), 1);
}

#[path = "patrol/settlement.rs"]
mod settlement;
