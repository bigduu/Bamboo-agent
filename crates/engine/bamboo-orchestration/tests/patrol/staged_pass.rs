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

mod calls {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use bamboo_domain::TokenUsage;
    use bamboo_orchestration::patrol::calls::{CallIo, Calls, ProviderReply};
    use tokio::sync::Notify;

    use super::*;

    #[derive(Default)]
    struct Controlled {
        started: Notify,
        release: Notify,
        sends: AtomicUsize,
        settlements: Mutex<Vec<(Reservation, Usage)>>,
    }

    struct Io {
        control: Arc<Controlled>,
        admitted: bool,
        reply: Option<ProviderReply>,
    }

    #[async_trait::async_trait]
    impl CallIo for Io {
        async fn admit(&self, _: &Reservation) -> Result<(), PortError> {
            if self.admitted {
                Ok(())
            } else {
                Err(PortError::Denied)
            }
        }

        async fn complete(&mut self, _: Duration) -> Result<ProviderReply, PortError> {
            self.control.sends.fetch_add(1, Ordering::SeqCst);
            self.control.started.notify_one();
            self.control.release.notified().await;
            self.reply.take().ok_or(PortError::OutcomeUnknown)
        }

        async fn settle(&self, receipt: &Reservation, usage: Usage) -> Result<(), PortError> {
            self.control
                .settlements
                .lock()
                .unwrap()
                .push((receipt.clone(), usage));
            Ok(())
        }
    }

    fn admitted_submission() -> staged::Submission {
        let mut policy = policy();
        policy.diagnostic_timeout_ms = 5_000;
        let patrol = Patrol::new(policy, Some(model())).unwrap();
        let mut host = FakeHost::new();
        open(&patrol, &mut host);
        let request = demand(&patrol, &mut host, 100);
        let input = prepared(&mut host, request);
        let Outcome::Complete {
            submission: Some(submission),
            ..
        } = patrol
            .tick_staged(
                &scope(),
                &FakeClock(100),
                &mut host,
                Some(input),
                no_prior_call,
            )
            .unwrap()
        else {
            panic!("expected live admission");
        };
        submission
    }

    async fn result(calls: &Calls, operation: &OperationId) -> DiagnosticStatus {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status = calls.status(operation).unwrap();
                if status != DiagnosticStatus::Pending {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn one_live_call_settles_the_exact_receipt_before_status() {
        let calls = Calls::default();
        let submission = admitted_submission();
        let original = submission.reservation().clone();
        let control = Arc::new(Controlled::default());
        let diagnosis = Diagnosis {
            advice: Advice::Observe,
            evidence: vec!["last-tool-receipt".into()],
            uncertain: false,
            note: "One controlled reply".into(),
        };
        calls
            .launch(
                submission,
                101,
                Io {
                    control: control.clone(),
                    admitted: true,
                    reply: Some(ProviderReply {
                        diagnosis: Some(serde_json::to_string(&diagnosis).unwrap()),
                        exact_usage: Some(TokenUsage {
                            prompt_tokens: 40,
                            completion_tokens: 4,
                            total_tokens: 44,
                        }),
                        observed_input_tokens: Some(40),
                        observed_output_tokens: Some(4),
                    }),
                },
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), control.started.notified())
            .await
            .unwrap();
        assert_eq!(
            calls.status(&original.operation).unwrap(),
            DiagnosticStatus::Pending
        );
        assert_eq!(
            Calls::default().status(&original.operation).unwrap(),
            DiagnosticStatus::OutcomeUnknown
        );
        control.release.notify_one();
        let usage = Usage::Known {
            input_tokens: 40,
            output_tokens: 4,
        };
        let expected = DiagnosticStatus::Finished { diagnosis, usage };
        assert_eq!(result(&calls, &original.operation).await, expected);
        assert_eq!(calls.status(&original.operation).unwrap(), expected);
        assert_eq!(&*control.settlements.lock().unwrap(), &[(original, usage)]);
        assert_eq!(control.sends.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn denied_transport_failure_and_partial_overrun_preserve_original_accounting() {
        for (admitted, observed_output, usage) in [
            (false, None, Usage::Unknown),
            (true, None, Usage::Unknown),
            (
                true,
                Some(21),
                Usage::OverCap {
                    input_tokens: None,
                    output_tokens: Some(21),
                },
            ),
        ] {
            let calls = Calls::default();
            let submission = admitted_submission();
            let original = submission.reservation().clone();
            let control = Arc::new(Controlled::default());
            control.release.notify_one();
            calls
                .launch(
                    submission,
                    101,
                    Io {
                        control: control.clone(),
                        admitted,
                        reply: observed_output.map(|output| ProviderReply {
                            diagnosis: None,
                            exact_usage: None,
                            observed_input_tokens: None,
                            observed_output_tokens: Some(output),
                        }),
                    },
                )
                .unwrap();
            let expected = if observed_output.is_some() {
                DiagnosticStatus::Failed { usage }
            } else {
                DiagnosticStatus::OutcomeUnknown
            };
            assert_eq!(result(&calls, &original.operation).await, expected);
            assert_eq!(&*control.settlements.lock().unwrap(), &[(original, usage)]);
            assert_eq!(control.sends.load(Ordering::SeqCst), usize::from(admitted));
        }
    }
}
