//! A successful pending-objective receipt must retain both real provider
//! admissions: terminal answer for the current goal, then the queued goal.
use super::*;

#[derive(Clone, Copy, Debug)]
struct RoundReservationCase {
    already_pending: bool,
    remaining: u32,
}

struct RoundReservationProvider {
    case: RoundReservationCase,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

#[async_trait]
impl LLMProvider for RoundReservationProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;

        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert_eq!(active, 1);
        let guard = ActiveStream(self.active.clone());
        let output = match round {
            1 => start_call(2, 8),
            2 => {
                let start = paired_result(messages, "start");
                let mut args = json!({
                    "action":"update",
                    "operation_id":UPDATE_OPERATION,
                    "generation":start["generation"],
                    "expected_revision":start["accepted_revision"],
                    "objective":SECOND_OBJECTIVE,
                });
                if !self.case.already_pending {
                    args["max_total_rounds"] = json!(2 + self.case.remaining);
                }
                control_call("queue-objective", args)
            }
            3 if self.case.already_pending => {
                let queued = paired_result(messages, "queue-objective");
                control_call(
                    "tighten-pending-rounds",
                    json!({
                        "action":"update",
                        "operation_id":"44444444-4444-4444-8444-444444444444",
                        "generation":queued["generation"],
                        "expected_revision":queued["accepted_revision"],
                        "max_total_rounds":3 + self.case.remaining,
                    }),
                )
            }
            // A RED source may exhaust before the second answer. Do not
            // manufacture a completion or panic inside the provider first.
            _ => LLMChunk::Token(format!(
                "Actual terminal answer from provider round {round}."
            )),
        };
        Ok(Box::pin(
            futures::stream::iter([Ok(output), Ok(LLMChunk::Done)]).map(move |chunk| {
                let _keep_admission = &guard;
                chunk
            }),
        ))
    }
}

#[tokio::test]
async fn self_control_pending_round_reservation_keeps_both_real_provider_admissions() {
    for already_pending in [false, true] {
        for remaining in [1, 2] {
            let case = RoundReservationCase {
                already_pending,
                remaining,
            };
            let mut fixture = Fixture::new(2, 8).await;
            let provider = Arc::new(RoundReservationProvider {
                case,
                calls: AtomicUsize::new(0),
                active: Arc::new(AtomicUsize::new(0)),
                peak: AtomicUsize::new(0),
            });
            let (result, events) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                fixture.run(provider.clone(), CancellationToken::new()),
            )
            .await
            .expect("finite synthetic admissions must finish");
            result.unwrap();
            let command_id = if already_pending {
                "tighten-pending-rounds"
            } else {
                "queue-objective"
            };
            let paired = fixture
                .session
                .messages
                .iter()
                .filter(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(command_id))
                .collect::<Vec<_>>();
            assert_eq!(paired.len(), 1);
            let accepted = remaining == 2;
            assert_eq!(
                paired[0].tool_success,
                Some(accepted),
                "{case:?}: a one-round receipt would strand the queued goal"
            );
            let owner_result: Value = serde_json::from_str(&paired[0].content).unwrap();
            if !accepted {
                assert_eq!(owner_result["error"], "no_budget_for_new_objective");
            } else {
                let receipt: LoopControlReceipt = serde_json::from_value(owner_result).unwrap();
                assert_eq!(receipt.effective_iteration, Some(2));
                assert!(!receipt.applied);
            }
            let state = read_loop_state(&fixture.session).unwrap().unwrap();
            let rounds = if already_pending { 5 } else { 4 };
            assert_eq!(state.status, LoopStatus::Completed, "{case:?}");
            assert_eq!(
                (
                    state.iterations_started,
                    state.iterations_completed,
                    state.rounds_started
                ),
                (2, 2, rounds)
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), rounds as usize);
            assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
            assert_eq!(provider.active.load(Ordering::SeqCst), 0);
            assert_eq!(state.max_total_rounds, if accepted { rounds } else { 8 });
            let expected_revision = 1 + u64::from(already_pending) + u64::from(accepted);
            let control = state.control.as_ref().unwrap();
            assert_eq!(control.control_revision, expected_revision);
            assert_eq!(control.operations.len(), expected_revision as usize);
            assert!(control.pending_objective.is_none());
            assert_eq!(
                admitted_objectives(&fixture.session.messages),
                vec![
                    (FIRST_OBJECTIVE, 1),
                    if already_pending || accepted {
                        (SECOND_OBJECTIVE, 2)
                    } else {
                        (FIRST_OBJECTIVE, 1)
                    }
                ]
            );
            // These answers are produced by later actual provider calls,
            // not direct terminal() calls from the Update round.
            for round in (rounds - 1)..=rounds {
                let text = format!("Actual terminal answer from provider round {round}.");
                assert!(fixture
                    .session
                    .messages
                    .iter()
                    .any(|m| { m.role == Role::Assistant && m.content == text }));
            }
            assert_eq!(
                events
                    .iter()
                    .filter(|e| matches!(e, AgentEvent::Complete { .. }))
                    .count(),
                1
            );
            let durable = fixture
                .storage
                .load_session(SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
            let durable_pair = durable
                .messages
                .iter()
                .filter(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(command_id))
                .collect::<Vec<_>>();
            assert_eq!(durable_pair.len(), 1);
            assert_eq!(durable_pair[0].tool_success, Some(accepted));
            assert_eq!(durable_pair[0].content, paired[0].content);
            fixture.close().await;
        }
    }
}
