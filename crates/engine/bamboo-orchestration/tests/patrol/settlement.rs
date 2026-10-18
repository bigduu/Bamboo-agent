// Controlled ledger/provider ports with the real checkpoint codec.
use super::*;

#[derive(Clone, Copy, Debug)]
enum SettlementSaveUnknown {
    BeforeCommit,
    AfterCommit,
}

struct SettlementProbe {
    host: FakeHost,
    // Observation of port invocations, not a second budget or persisted fact.
    settlements: Vec<(Reservation, Usage)>,
    next_save_unknown: Option<SettlementSaveUnknown>,
}

impl SettlementProbe {
    fn new() -> Self {
        let mut host = FakeHost::new();
        host.limits = [120; 3]; // The first 100+20 reservation fills every cap.
        Self {
            host,
            settlements: vec![],
            next_save_unknown: None,
        }
    }

    fn incident(&self) -> Incident {
        self.host.incident()
    }

    fn stored(&self) -> StoredCheckpoint {
        self.host.durable.borrow().checkpoints[&self.host.observation.scope].clone()
    }

    fn reopen(self) -> Self {
        // A freshly constructed Host/Patrol/provider receives only the decoded
        // checkpoint plus the existing authoritative cumulative ledger/outbox.
        let stored = self.stored();
        let bytes = checkpoint::encode(stored.version, &stored.checkpoint).unwrap();
        let recovered = checkpoint::decode(&stored.checkpoint.scope, &bytes).unwrap();
        assert_eq!(recovered, stored);
        let mut host = FakeHost::new();
        host.durable = self.host.durable.clone();
        host.observation = self.host.observation.clone();
        host.limits = self.host.limits;
        host.fail_settle = self.host.fail_settle;
        host.durable
            .borrow_mut()
            .checkpoints
            .insert(recovered.checkpoint.scope.clone(), recovered);
        Self {
            host,
            settlements: self.settlements,
            next_save_unknown: None,
        }
    }
}

impl ObservationPort for SettlementProbe {
    fn observation(&mut self, scope: &Scope, cursor: u64) -> Result<Observation, PortError> {
        self.host.observation(scope, cursor)
    }
    fn diagnostic_input(
        &mut self,
        scope: &Scope,
        cursor: u64,
        observation: &Observation,
    ) -> Result<DiagnosticInput, PortError> {
        self.host.diagnostic_input(scope, cursor, observation)
    }
}

impl CheckpointPort for SettlementProbe {
    fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        self.host
            .load(scope)?
            .map(|stored| {
                checkpoint::decode(
                    scope,
                    &checkpoint::encode(stored.version, &stored.checkpoint)?,
                )
            })
            .transpose()
    }
    fn save(&mut self, expected: Option<u64>, next: &Checkpoint) -> Result<u64, PortError> {
        let unknown = self.next_save_unknown.take();
        if matches!(unknown, Some(SettlementSaveUnknown::BeforeCommit)) {
            return Err(PortError::OutcomeUnknown);
        }
        let current = self.host.load(&next.scope)?;
        if current.as_ref().map(|stored| stored.version) != expected {
            return Err(PortError::Conflict);
        }
        if let Some(current) = current {
            checkpoint::validate_update(&current.checkpoint, next)?;
        }
        let version = expected.unwrap_or(0) + 1;
        let bytes = checkpoint::encode(version, next)?;
        let decoded = checkpoint::decode(&next.scope, &bytes)?;
        let actual_version = self.host.save(expected, &decoded.checkpoint)?;
        assert_eq!(actual_version, version);
        if matches!(unknown, Some(SettlementSaveUnknown::AfterCommit)) {
            Err(PortError::OutcomeUnknown)
        } else {
            Ok(actual_version)
        }
    }
}

impl BudgetPort for SettlementProbe {
    fn reserve(&mut self, request: &DiagnosticRequest) -> Result<Reservation, BudgetBlock> {
        self.host.reserve(request)
    }
    fn settle(&mut self, reservation: &Reservation, usage: Usage) -> Result<(), PortError> {
        self.settlements.push((reservation.clone(), usage));
        self.host.settle(reservation, usage)
    }
}

impl EscalationPort for SettlementProbe {
    fn queue_escalation(&mut self, escalation: &Escalation) -> Result<(), PortError> {
        self.host.queue_escalation(escalation)
    }
}

fn known_settlement_usage() -> Usage {
    Usage::Known {
        input_tokens: 40,
        output_tokens: 10,
    }
}

fn settlement_pass(
    probe: &mut SettlementProbe,
    provider: &mut ControlledProvider,
    now: u64,
) -> Report {
    let scope = probe.host.observation.scope.clone();
    configured()
        .tick(&scope, &FakeClock(now), probe, provider)
        .unwrap()
}

fn settlement_effect_counts(
    probe: &SettlementProbe,
    provider: &ControlledProvider,
) -> (usize, usize, usize) {
    let reserves = probe
        .host
        .durable
        .borrow()
        .calls
        .iter()
        .filter(|call| **call == "reserve")
        .count();
    (provider.submits.len(), provider.polls.len(), reserves)
}

fn seed_failed_known_settlement() -> (SettlementProbe, ControlledProvider, Reservation) {
    let mut probe = SettlementProbe::new();
    let mut provider = ControlledProvider::new(&probe.host);
    assert_eq!(
        settlement_pass(&mut probe, &mut provider, 100).status,
        Status::Incident
    );
    assert_eq!(
        settlement_pass(&mut probe, &mut provider, 100).status,
        Status::Diagnosing
    );
    assert_eq!(provider.submits.len(), 1);
    assert!(provider.polls.is_empty());
    assert!(probe.settlements.is_empty());
    let reservation = match probe.incident().phase {
        Phase::Diagnosing { reservation, .. } => reservation,
        other => panic!("expected the real reserved request: {other:?}"),
    };
    assert_eq!(reservation.request.deadline_ms, 150);
    assert_eq!(probe.host.usage(), 120);
    probe.host.fail_settle = true;
    probe.host.fail_queue = true;
    provider.finish(Advice::Observe);
    settlement_pass(&mut probe, &mut provider, 101);
    assert_eq!(provider.polls, vec![reservation.operation.clone()]);
    assert_eq!(
        probe.settlements,
        vec![(reservation.clone(), known_settlement_usage())]
    );
    assert_eq!(
        probe.incident().phase,
        Phase::Blocked(Block::Budget(BudgetBlock::Unavailable))
    );
    assert_pending_known_settlement(&probe, &reservation);
    (probe, provider, reservation)
}

fn assert_pending_known_settlement(probe: &SettlementProbe, original: &Reservation) {
    let incident = probe.incident();
    assert_eq!(incident.id, original.operation.incident);
    assert_eq!(incident.attempts, 1);
    assert_eq!(incident.opened_at_ms, 100);
    assert_eq!(incident.pending_settlement.as_ref(), Some(original));
    assert_eq!(
        incident.pending_settlement_usage,
        Some(known_settlement_usage())
    );
    assert_eq!(original.request.deadline_ms, 150);
    assert_eq!(probe.host.usage(), 120);
    let durable = probe.host.durable.borrow();
    assert_eq!(durable.charges.len(), 1);
    let charge = &durable.charges[&original.operation];
    assert_eq!(&charge.reservation, original);
    assert_eq!(charge.settled, None);
}

fn assert_known_settled(probe: &SettlementProbe, original: &Reservation) {
    let incident = probe.incident();
    assert_eq!(incident.id, original.operation.incident);
    assert_eq!(incident.attempts, 1);
    assert_eq!(incident.opened_at_ms, 100);
    assert!(incident.pending_settlement.is_none());
    assert!(incident.pending_settlement_usage.is_none());
    assert_eq!(probe.host.usage(), 50);
    assert_eq!(
        probe.host.limits.map(|limit| limit - probe.host.usage()),
        [70; 3]
    );
    let durable = probe.host.durable.borrow();
    assert_eq!(durable.charges.len(), 1);
    let charge = &durable.charges[&original.operation];
    assert_eq!(&charge.reservation, original);
    assert_eq!(charge.reservation.request.deadline_ms, 150);
    assert_eq!(charge.settled, Some(known_settlement_usage()));
}

fn assert_unpaid_checkpoint_cannot_replace_receipt_usage_or_incident(probe: &SettlementProbe) {
    let old = probe.stored().checkpoint;
    assert!(matches!(
        old.incident.as_ref().unwrap().phase,
        Phase::Resolved { .. }
    ));
    let mut changed_receipt = old.clone();
    changed_receipt
        .incident
        .as_mut()
        .unwrap()
        .pending_settlement
        .as_mut()
        .unwrap()
        .receipt = "replacement-receipt".into();
    assert_eq!(
        checkpoint::validate_update(&old, &changed_receipt),
        Err(PortError::Conflict)
    );
    for usage in [Some(Usage::Unknown), None] {
        let mut downgraded_usage = old.clone();
        downgraded_usage
            .incident
            .as_mut()
            .unwrap()
            .pending_settlement_usage = usage;
        assert_eq!(
            checkpoint::validate_update(&old, &downgraded_usage),
            Err(PortError::Conflict)
        );
    }
    let mut orphan_usage = old.clone();
    orphan_usage.incident.as_mut().unwrap().pending_settlement = None;
    assert_eq!(
        checkpoint::encode(10, &orphan_usage),
        Err(PortError::Unavailable)
    );
    let mut over_cap = old.clone();
    over_cap.incident.as_mut().unwrap().pending_settlement_usage = Some(Usage::Known {
        input_tokens: model().max_input_tokens + 1,
        output_tokens: 0,
    });
    assert_eq!(
        checkpoint::encode(10, &over_cap),
        Err(PortError::Unavailable)
    );
    let mut next_incident = old.clone();
    let next = next_incident.incident.as_mut().unwrap();
    next.id.progress_sequence += 1;
    next.attempts = 0;
    next.last_diagnosis = None;
    next.pending_settlement = None;
    next.pending_settlement_usage = None;
    next.phase = Phase::Open;
    next.escalation = None;
    assert_eq!(
        checkpoint::validate_update(&old, &next_incident),
        Err(PortError::Conflict)
    );
}

fn assert_original_escalation_delivered(probe: &SettlementProbe, original: &Reservation) {
    let durable = probe.host.durable.borrow();
    let operation = OperationId {
        incident: original.operation.incident.clone(),
        kind: OperationKind::Escalation,
    };
    assert_eq!(durable.queue.len(), 1);
    assert_eq!(durable.queue[&operation].operation, operation);
    assert_eq!(
        durable
            .calls
            .iter()
            .filter(|call| **call == "queue")
            .count(),
        2
    );
}

#[test]
fn known_settlement_retries_the_original_receipt_and_usage_live_and_after_codec_restart() {
    for denied in [false, true] {
        let (mut probe, mut provider, original) = seed_failed_known_settlement();
        let settlements = probe.settlements.len();
        let effects = settlement_effect_counts(&probe, &provider);
        if denied {
            probe.host.denied = true;
            assert_eq!(
                configured().tick(&scope(), &FakeClock(111), &mut probe, &mut provider),
                Err(Error::Port(PortError::Denied))
            );
        } else {
            probe.host.observation.cursor += 1;
            probe.host.observation.kind = ObservationKind::Delta { previous_cursor: 1 };
            assert_eq!(
                settlement_pass(&mut probe, &mut provider, 111).status,
                Status::ResyncRequired
            );
        }
        assert_eq!(probe.settlements.len(), settlements);
        assert_pending_known_settlement(&probe, &original);
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
    }
    for restart in [false, true] {
        for unknown in [
            None,
            Some(SettlementSaveUnknown::BeforeCommit),
            Some(SettlementSaveUnknown::AfterCommit),
        ] {
            let (mut probe, mut provider, original) = seed_failed_known_settlement();
            // A ledger outage must not prevent the independent outbox retry.
            probe.host.fresh_heartbeat(141);
            probe.host.fail_queue = false;
            let effects = settlement_effect_counts(&probe, &provider);
            let settlements = probe.settlements.len();
            settlement_pass(&mut probe, &mut provider, 141);
            assert_pending_known_settlement(&probe, &original);
            assert_eq!(settlement_effect_counts(&probe, &provider), effects);
            assert_eq!(
                &probe.settlements[settlements..],
                &[(original.clone(), known_settlement_usage())]
            );
            let escalation = probe.incident().escalation.unwrap();
            assert!(escalation.queued);
            assert_eq!(escalation.deliveries, 2);
            assert_original_escalation_delivered(&probe, &original);
            if restart {
                drop(provider);
                probe = probe.reopen();
                provider = ControlledProvider::new(&probe.host);
            }
            let effects = settlement_effect_counts(&probe, &provider);
            let settlements = probe.settlements.len();
            probe.host.fail_settle = false;
            probe.next_save_unknown = unknown;
            let result = configured().tick(&scope(), &FakeClock(142), &mut probe, &mut provider);
            if unknown.is_some() {
                assert_eq!(result, Err(Error::Port(PortError::OutcomeUnknown)));
            } else {
                result.unwrap();
            }
            // Ledger success precedes an ambiguous checkpoint response. This
            // confirmed Known charge is still 50; it cannot be charged again or
            // re-polled at the provider to rediscover the already saved usage.
            assert_eq!(probe.host.usage(), 50);
            assert_eq!(
                &probe.settlements[settlements..],
                &[(original.clone(), known_settlement_usage())]
            );
            assert_eq!(settlement_effect_counts(&probe, &provider), effects);
            if matches!(unknown, Some(SettlementSaveUnknown::BeforeCommit)) {
                let saved = probe.incident();
                assert_eq!(saved.pending_settlement.as_ref(), Some(&original));
                assert_eq!(
                    saved.pending_settlement_usage,
                    Some(known_settlement_usage())
                );
            }
            settlement_pass(&mut probe, &mut provider, 143);
            assert_known_settled(&probe, &original);
            let expected_calls = if matches!(unknown, Some(SettlementSaveUnknown::BeforeCommit)) {
                2
            } else {
                1
            };
            assert_eq!(probe.settlements.len(), settlements + expected_calls);
            assert!(probe.settlements[settlements..]
                .iter()
                .all(|entry| entry == &(original.clone(), known_settlement_usage())));
            assert_eq!(settlement_effect_counts(&probe, &provider), effects);
            settlement_pass(&mut probe, &mut provider, 144);
            assert_known_settled(&probe, &original);
            assert_original_escalation_delivered(&probe, &original);
            assert_eq!(probe.settlements.len(), settlements + expected_calls);
            assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        }
    }
}

#[test]
fn resolved_and_terminal_incidents_retry_saved_known_usage_after_codec_restart() {
    for terminal in [false, true] {
        let (mut probe, mut provider, original) = seed_failed_known_settlement();
        probe.host.fresh_heartbeat(102);
        if terminal {
            probe.host.observation.terminal = true;
        } else {
            probe.host.observation.progress = Some(Progress {
                sequence: 2,
                at_ms: 102,
            });
        }
        let effects = settlement_effect_counts(&probe, &provider);
        let settlements = probe.settlements.len();
        // Resolution cannot wait for the unavailable budget ledger, and must
        // retain the original known completion instead of degrading to Unknown.
        settlement_pass(&mut probe, &mut provider, 102);
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        assert_eq!(
            &probe.settlements[settlements..],
            &[(original.clone(), known_settlement_usage())]
        );
        assert!(matches!(probe.incident().phase, Phase::Resolved { .. }));
        assert_pending_known_settlement(&probe, &original);
        drop(provider);
        probe = probe.reopen();
        provider = ControlledProvider::new(&probe.host);
        probe.host.fail_settle = false;
        let effects = settlement_effect_counts(&probe, &provider);
        let settlements = probe.settlements.len();
        settlement_pass(&mut probe, &mut provider, 113);
        assert_known_settled(&probe, &original);
        assert!(matches!(probe.incident().phase, Phase::Resolved { .. }));
        assert!(provider.submits.is_empty());
        assert!(provider.polls.is_empty());
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        assert_eq!(
            &probe.settlements[settlements..],
            &[(original.clone(), known_settlement_usage())]
        );
        assert_eq!(
            probe.settlements.last(),
            Some(&(original.clone(), known_settlement_usage()))
        );
    }
}

#[test]
fn failed_settlement_and_unknown_checkpoint_cas_do_not_replace_the_old_resolved_charge() {
    for unknown in [
        SettlementSaveUnknown::BeforeCommit,
        SettlementSaveUnknown::AfterCommit,
    ] {
        let (mut probe, mut provider, original) = seed_failed_known_settlement();
        probe.host.fresh_heartbeat(102);
        probe.host.observation.progress = Some(Progress {
            sequence: 2,
            at_ms: 102,
        });
        settlement_pass(&mut probe, &mut provider, 102);
        assert!(matches!(probe.incident().phase, Phase::Resolved { .. }));
        assert_pending_known_settlement(&probe, &original);
        assert_unpaid_checkpoint_cannot_replace_receipt_usage_or_incident(&probe);
        // The distinct later progress boundary would ordinarily permit a new
        // incident. It cannot erase the old unpaid receipt/Usage while its
        // settlement is still unavailable, even after an ambiguous CAS reply.
        probe.host.fresh_heartbeat(203);
        probe.host.observation.evidence_revision += 1;
        probe.next_save_unknown = Some(unknown);
        let effects = settlement_effect_counts(&probe, &provider);
        let settlements = probe.settlements.len();
        assert_eq!(
            configured().tick(&scope(), &FakeClock(203), &mut probe, &mut provider),
            Err(Error::Port(PortError::OutcomeUnknown))
        );
        assert_pending_known_settlement(&probe, &original);
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        assert_eq!(
            &probe.settlements[settlements..],
            &[(original.clone(), known_settlement_usage())]
        );
        drop(provider);
        probe = probe.reopen();
        provider = ControlledProvider::new(&probe.host);
        let effects = settlement_effect_counts(&probe, &provider);
        let settlements = probe.settlements.len();
        settlement_pass(&mut probe, &mut provider, 204);
        assert_pending_known_settlement(&probe, &original);
        assert_eq!(
            &probe.settlements[settlements..],
            &[(original.clone(), known_settlement_usage())]
        );
        assert!(provider.submits.is_empty());
        assert!(provider.polls.is_empty());
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        assert!(matches!(probe.incident().phase, Phase::Resolved { .. }));
        // Successful debt recovery must still preserve old identity for this
        // entire tick. Only the following tick can open the distinct incident.
        probe.host.fail_settle = false;
        let settlements = probe.settlements.len();
        settlement_pass(&mut probe, &mut provider, 205);
        assert_known_settled(&probe, &original);
        assert!(matches!(probe.incident().phase, Phase::Resolved { .. }));
        assert_eq!(
            &probe.settlements[settlements..],
            &[(original.clone(), known_settlement_usage())]
        );
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
        assert_eq!(
            settlement_pass(&mut probe, &mut provider, 206).status,
            Status::Incident
        );
        assert_eq!(probe.incident().id.progress_sequence, 2);
        assert_ne!(probe.incident().id, original.operation.incident);
        assert_eq!(probe.host.durable.borrow().charges.len(), 1);
        assert_eq!(settlement_effect_counts(&probe, &provider), effects);
    }
}

#[test]
fn legacy_pending_receipt_without_usage_decodes_and_settles_only_as_unknown() {
    let (mut probe, provider, original) = seed_failed_known_settlement();
    // This is the exact information old non-Diagnosing checkpoints retained:
    // the reservation survives, while the completed Usage was lost. Encoding
    // None must omit the new key so the schema-1 record hash remains compatible.
    {
        let mut durable = probe.host.durable.borrow_mut();
        let stored = durable.checkpoints.get_mut(&scope()).unwrap();
        stored
            .checkpoint
            .incident
            .as_mut()
            .unwrap()
            .pending_settlement_usage = None;
        let bytes = checkpoint::encode(stored.version, &stored.checkpoint).unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(envelope["record"]["checkpoint"]["incident"]
            .get("pending_settlement_usage")
            .is_none());
        let decoded = checkpoint::decode(&scope(), &bytes).unwrap();
        assert!(decoded
            .checkpoint
            .incident
            .as_ref()
            .unwrap()
            .pending_settlement_usage
            .is_none());
        *stored = decoded;
    }
    drop(provider);
    probe = probe.reopen();
    let mut provider = ControlledProvider::new(&probe.host);
    probe.host.fail_settle = false;
    let effects = settlement_effect_counts(&probe, &provider);
    let settlements = probe.settlements.len();
    settlement_pass(&mut probe, &mut provider, 111);
    assert_eq!(
        &probe.settlements[settlements..],
        &[(original.clone(), Usage::Unknown)]
    );
    assert_eq!(probe.host.usage(), 120);
    let incident = probe.incident();
    assert_eq!(incident.id, original.operation.incident);
    assert_eq!(incident.attempts, 1);
    assert_eq!(incident.opened_at_ms, 100);
    assert!(incident.pending_settlement.is_none());
    assert!(incident.pending_settlement_usage.is_none());
    {
        let durable = probe.host.durable.borrow();
        assert_eq!(durable.charges.len(), 1);
        let charge = &durable.charges[&original.operation];
        assert_eq!(charge.reservation, original);
        assert_eq!(charge.settled, Some(Usage::Unknown));
    }
    assert!(provider.submits.is_empty());
    assert!(provider.polls.is_empty());
    assert_eq!(settlement_effect_counts(&probe, &provider), effects);
}
