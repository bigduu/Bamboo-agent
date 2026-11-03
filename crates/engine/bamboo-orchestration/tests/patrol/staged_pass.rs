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
            submission: None,
            ..
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
        ..
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

    let Outcome::Complete {
        report, submission, ..
    } = patrol
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
            let Outcome::Complete {
                report, submission, ..
            } = patrol
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

#[test]
fn expired_staged_call_retains_the_original_receipt_until_late_usage_is_durable() {
    let patrol = configured();
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
        panic!("expected live operation");
    };
    let original = submission.reservation();
    let Outcome::Complete {
        submission: None,
        reconciled: None,
        ..
    } = patrol
        .tick_staged(&scope(), &FakeClock(150), &mut host, None, |operation| {
            assert_eq!(operation, &original.operation);
            Ok(DiagnosticStatus::Pending)
        })
        .unwrap()
    else {
        panic!("expired advice must not discard unfinished accounting");
    };
    let awaiting = host.checkpoint();
    assert_eq!(
        awaiting.incident.as_ref().unwrap().pending_settlement_usage,
        Some(Usage::AwaitingProvider)
    );
    assert_eq!(host.usage(), 120);
    let mut legacy = awaiting.clone();
    legacy.incident.as_mut().unwrap().pending_settlement_usage = None;
    assert_eq!(
        checkpoint::validate_update(&awaiting, &legacy),
        Err(PortError::Conflict)
    );
    patrol
        .tick_staged(&scope(), &FakeClock(151), &mut host, None, |_| {
            Err(PortError::Unavailable)
        })
        .unwrap();
    assert_eq!(
        host.incident().pending_settlement_usage,
        Some(Usage::AwaitingProvider)
    );
    host.fail_settle = true;
    let late = Usage::OverCap {
        input_tokens: None,
        output_tokens: Some(21),
    };
    assert!(matches!(
        patrol
            .tick_staged(&scope(), &FakeClock(151), &mut host, None, |operation| {
                assert_eq!(operation, &original.operation);
                Ok(DiagnosticStatus::Failed { usage: late })
            })
            .unwrap(),
        Outcome::Complete {
            submission: None,
            reconciled: Some(_),
            ..
        }
    ));
    let settled = host.load(&scope()).unwrap().unwrap();
    checkpoint::validate_update(&awaiting, &settled.checkpoint).unwrap();
    let bytes = checkpoint::encode(settled.version, &settled.checkpoint).unwrap();
    let recovered = checkpoint::decode(&scope(), &bytes).unwrap();
    assert_eq!(
        recovered
            .checkpoint
            .incident
            .as_ref()
            .unwrap()
            .pending_settlement_usage,
        Some(late)
    );
    let mut malformed = recovered.checkpoint.clone();
    malformed
        .incident
        .as_mut()
        .unwrap()
        .pending_settlement_usage = Some(Usage::OverCap {
        input_tokens: None,
        output_tokens: Some(1),
    });
    assert_eq!(
        checkpoint::encode(100, &malformed),
        Err(PortError::Unavailable)
    );
    host.durable
        .borrow_mut()
        .checkpoints
        .insert(scope(), recovered);
    host.fail_settle = false;
    patrol
        .tick_staged(&scope(), &FakeClock(152), &mut host, None, no_prior_call)
        .unwrap();
    assert!(host.incident().pending_settlement.is_none());
    assert_eq!(
        host.durable.borrow().charges[&original.operation].settled,
        Some(late)
    );
    assert_eq!(host.usage(), 120);
}

mod calls {
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
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
        settling: Notify,
        hold_settle: AtomicBool,
    }

    struct Io {
        control: Arc<Controlled>,
        admitted: bool,
        reply: Option<ProviderReply>,
        fail_settle: bool,
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
            self.control.settling.notify_one();
            if self.control.hold_settle.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.fail_settle {
                Err(PortError::Unavailable)
            } else {
                Ok(())
            }
        }
    }

    fn staged_operation() -> (FakeHost, staged::Submission) {
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
        (host, submission)
    }

    fn admitted_submission() -> staged::Submission {
        staged_operation().1
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
                    fail_settle: false,
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
                        fail_settle: false,
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

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_does_not_evict_unreconciled_over_cap_evidence() {
        let calls = Calls::default();
        let (mut old_scope, submission) = staged_operation();
        let operation = submission.reservation().operation.clone();
        let receipt = submission.reservation().clone();
        let control = Arc::new(Controlled::default());
        control.release.notify_one();
        calls
            .launch(
                submission,
                101,
                Io {
                    control: control.clone(),
                    admitted: true,
                    fail_settle: true,
                    reply: Some(ProviderReply {
                        diagnosis: None,
                        exact_usage: None,
                        observed_input_tokens: None,
                        observed_output_tokens: Some(21),
                    }),
                },
            )
            .unwrap();
        let expected = DiagnosticStatus::Failed {
            usage: Usage::OverCap {
                input_tokens: None,
                output_tokens: Some(21),
            },
        };
        assert_eq!(result(&calls, &operation).await, expected);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(
            calls.launch(
                admitted_submission(),
                101,
                Io {
                    control: control.clone(),
                    admitted: true,
                    fail_settle: false,
                    reply: None,
                }
            ),
            Err(PortError::Unavailable)
        );
        assert_eq!(calls.status(&operation).unwrap(), expected);
        assert_eq!(control.sends.load(Ordering::SeqCst), 1);
        old_scope.denied = true;
        assert_eq!(calls.reconcile_accounting(&mut old_scope), Ok(1));
        assert_eq!(calls.reconcile_accounting(&mut old_scope), Ok(0));
        let state = old_scope.durable.borrow();
        assert_eq!(state.charges[&operation].reservation, receipt);
        assert_eq!(
            state.charges[&operation].settled,
            Some(Usage::OverCap {
                input_tokens: None,
                output_tokens: Some(21)
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_observed_usage_and_preserves_failed_settlement_for_drain() {
        let calls = Calls::default();
        let (mut host, submission) = staged_operation();
        let original = submission.reservation().clone();
        let control = Arc::new(Controlled::default());
        control.hold_settle.store(true, Ordering::SeqCst);
        control.release.notify_one();
        calls
            .launch(
                submission,
                101,
                Io {
                    control: control.clone(),
                    admitted: true,
                    fail_settle: false,
                    reply: Some(ProviderReply {
                        diagnosis: None,
                        exact_usage: None,
                        observed_input_tokens: None,
                        observed_output_tokens: Some(21),
                    }),
                },
            )
            .unwrap();
        control.settling.notified().await;
        assert!(!calls.needs_accounting());
        calls.quiesce().await.unwrap();
        assert!(calls.needs_accounting());
        assert_eq!(calls.reconcile_accounting(&mut host), Ok(1));
        assert!(!calls.needs_accounting());
        assert_eq!(
            host.durable.borrow().charges[&original.operation].settled,
            Some(Usage::OverCap {
                input_tokens: None,
                output_tokens: Some(21),
            })
        );
        assert_eq!(control.sends.load(Ordering::SeqCst), 1);
    }
}
