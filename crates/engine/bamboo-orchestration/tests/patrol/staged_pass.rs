use super::*;
use bamboo_orchestration::patrol::staged::{InputDemand, Outcome, PreparedInput};

fn no_prior_call(_: &OperationId) -> Result<DiagnosticStatus, PortError> {
    panic!("a new incident has no provider status to reconcile");
}

fn open(patrol: &Patrol, host: &mut FakeHost) {
    assert!(matches!(
        patrol
            .tick_staged(&scope(), &FakeClock(100), host, None, no_prior_call)
            .unwrap(),
        Outcome::Complete {
            report: Report {
                status: Status::Incident,
                ..
            },
            submission: None
        }
    ));
}

fn demand(patrol: &Patrol, host: &mut FakeHost, now: u64) -> InputDemand {
    let Outcome::InputRequired(demand) = patrol
        .tick_staged(&scope(), &FakeClock(now), host, None, no_prior_call)
        .unwrap()
    else {
        panic!("expected scoped diagnostic input");
    };
    demand
}

fn prepared(host: &mut FakeHost, demand: InputDemand) -> PreparedInput {
    PreparedInput {
        result: host.diagnostic_input(&demand.through.scope, demand.since_cursor, &demand.through),
        demand,
    }
}

#[test]
fn staged_input_is_deferred_and_fenced_before_reservation() {
    let patrol = configured();
    let mut host = FakeHost::new();
    open(&patrol, &mut host);
    let saved = host.load(&scope()).unwrap();
    let request = demand(&patrol, &mut host, 100);
    assert_eq!(request.model, model());
    assert_eq!(request.through, host.observation);
    assert_eq!(request.since_cursor, host.observation.cursor);
    assert_eq!(host.load(&scope()).unwrap(), saved);
    assert_eq!(host.usage(), 0);

    let stale = prepared(&mut host, request);
    host.fresh_heartbeat(101);
    let Outcome::InputRequired(current) = patrol
        .tick_staged(
            &scope(),
            &FakeClock(101),
            &mut host,
            Some(stale),
            no_prior_call,
        )
        .unwrap()
    else {
        panic!("stale evidence cannot produce a submit");
    };
    assert_eq!(current.through, host.observation);
    assert_eq!(host.load(&scope()).unwrap(), saved);
    assert_eq!(host.usage(), 0);

    let input = prepared(&mut host, current);
    // Renewal alone can change while Host I/O runs. It carries no new
    // evidence revision and does not invalidate the exact prepared input.
    host.observation.heartbeat_at_ms = Some(102);
    let Outcome::Complete {
        report,
        submission: Some(submission),
    } = patrol
        .tick_staged(
            &scope(),
            &FakeClock(102),
            &mut host,
            Some(input),
            no_prior_call,
        )
        .unwrap()
    else {
        panic!("expected an admitted operation");
    };
    assert_eq!(report.status, Status::Diagnosing);
    let Phase::Diagnosing {
        request,
        reservation,
    } = host.incident().phase
    else {
        panic!("submission requires a durable diagnostic checkpoint");
    };
    assert_eq!(&reservation, submission.reservation());
    assert_eq!(reservation.request, request);
    assert_eq!(host.usage(), 120);
    assert!(host
        .durable
        .borrow()
        .calls
        .ends_with(&["observe", "reserve", "save", "save"]));

    let Outcome::Complete { report, submission } = patrol
        .tick_staged(&scope(), &FakeClock(103), &mut host, None, |operation| {
            assert_eq!(operation, &reservation.operation);
            Ok(DiagnosticStatus::Finished {
                diagnosis: Diagnosis {
                    advice: Advice::Observe,
                    evidence: vec!["last-tool-receipt".into()],
                    uncertain: false,
                    note: "Observed existing evidence".into(),
                },
                usage: Usage::Known {
                    input_tokens: 40,
                    output_tokens: 2,
                },
            })
        })
        .unwrap()
    else {
        panic!("settlement needs only the original operation status");
    };
    assert_eq!(report.status, Status::Cooldown);
    assert!(submission.is_none());
    assert_eq!(host.usage(), 42);
}

#[test]
fn staged_submission_is_withheld_after_a_checkpoint_error() {
    for failed_save in [1, 2] {
        let patrol = configured();
        let mut host = FakeHost::new();
        open(&patrol, &mut host);
        let request = demand(&patrol, &mut host, 100);
        let input = prepared(&mut host, request);
        host.fail_save = Some(host.durable.borrow().save_count + failed_save);
        assert!(matches!(
            patrol.tick_staged(
                &scope(),
                &FakeClock(100),
                &mut host,
                Some(input),
                no_prior_call,
            ),
            Err(Error::Port(PortError::Conflict))
        ));
        assert_eq!(host.usage(), 120);
        host.fail_save = None;
        if failed_save == 2 {
            let original = host.incident().pending_settlement.unwrap().operation;
            let Outcome::Complete { report, submission } = patrol
                .tick_staged(&scope(), &FakeClock(101), &mut host, None, |operation| {
                    assert_eq!(operation, &original);
                    Ok(DiagnosticStatus::OutcomeUnknown)
                })
                .unwrap()
            else {
                panic!("an already-saved request must reconcile");
            };
            assert!(matches!(
                report.status,
                Status::Blocked | Status::NeedsSupervisor
            ));
            assert!(submission.is_none());
            assert_eq!(host.usage(), 120);
            assert_eq!(host.incident().escalation.unwrap().deliveries, 0);
            assert!(host.durable.borrow().queue.is_empty());
        }
    }
}

#[test]
fn staged_budget_denial_preserves_program_and_outbox_ownership() {
    let patrol = configured();
    let mut host = FakeHost::new();
    open(&patrol, &mut host);
    let request = demand(&patrol, &mut host, 100);
    let input = prepared(&mut host, request);
    host.budget_block = Some(BudgetBlock::Work);
    assert!(matches!(
        patrol
            .tick_staged(
                &scope(),
                &FakeClock(100),
                &mut host,
                Some(input),
                no_prior_call
            )
            .unwrap(),
        Outcome::Complete {
            submission: None,
            ..
        }
    ));
    assert_eq!(host.usage(), 0);
    assert_eq!(
        host.incident().phase,
        Phase::Blocked(Block::Budget(BudgetBlock::Work))
    );
    let Outcome::Complete { submission, .. } = patrol
        .tick_staged(&scope(), &FakeClock(101), &mut host, None, no_prior_call)
        .unwrap()
    else {
        panic!("budget block remains visible");
    };
    assert!(submission.is_none());
    assert_eq!(host.incident().escalation.unwrap().deliveries, 0);
    assert!(host.durable.borrow().queue.is_empty());
}
