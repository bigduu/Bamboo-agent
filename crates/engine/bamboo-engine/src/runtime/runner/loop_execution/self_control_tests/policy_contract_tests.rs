//! Policy accounting must observe the real serial owner result exactly once.
//! The parent module supplies the existing owned Fixture and runner harness.

use super::*;
use bamboo_agent_core::tools::ToolResult;

const STALE_IDS: [&str; 4] = ["stale-1", "stale-2", "stale-3", "stale-4"];
const STATUS_IDS: [&str; 4] = ["status-1", "status-2", "status-3", "status-4"];

#[derive(Clone, Copy)]
enum PolicyContractScript {
    StaleBatch,
    StatusBatch,
}

struct PolicyContractProvider {
    script: PolicyContractScript,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

impl PolicyContractProvider {
    fn new(script: PolicyContractScript) -> Self {
        Self {
            script,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
        }
    }

    fn response(&self, round: usize, messages: &[Message]) -> LLMChunk {
        match (self.script, round) {
            (PolicyContractScript::StaleBatch, 1) => start_call(1, 4),
            (PolicyContractScript::StaleBatch, 2) => {
                let receipt = paired_result(messages, "start");
                let generation = receipt["generation"].as_str().unwrap();
                assert_eq!(receipt["accepted_revision"], 1);
                LLMChunk::ToolCalls(
                    STALE_IDS
                        .iter()
                        .enumerate()
                        .map(|(index, id)| {
                            policy_call(
                                id,
                                json!({
                                    "action": "update",
                                    "operation_id": format!("44444444-4444-4444-8444-{:012}", index + 1),
                                    "generation": generation,
                                    "expected_revision": 0,
                                    "max_iterations": 1,
                                }),
                            )
                        })
                        .collect(),
                )
            }
            (PolicyContractScript::StatusBatch, 1) => LLMChunk::ToolCalls(
                STATUS_IDS
                    .iter()
                    .map(|id| policy_call(id, json!({"action": "status"})))
                    .collect(),
            ),
            (PolicyContractScript::StaleBatch, 3) | (PolicyContractScript::StatusBatch, 2) => {
                LLMChunk::Token("The bounded policy contract run is complete.".into())
            }
            _ => panic!("unexpected provider round {round}; no retry or summary is allowed"),
        }
    }
}

#[async_trait]
impl LLMProvider for PolicyContractProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;

        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = ActiveStream(self.active.clone());
        assert_eq!(active, 1, "provider admissions must not overlap");
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == "loop_control"));
        assert!(messages
            .iter()
            .any(|message| message.content.contains(ORIGINAL_CONTEXT)));
        assert!(messages
            .iter()
            .any(|message| message.content == ORIGINAL_FACT));
        let response = self.response(round, messages);
        let stream = futures::stream::iter([Ok(response), Ok(LLMChunk::Done)]).map(move |chunk| {
            let _keep_stream_admitted = &guard;
            chunk
        });
        Ok(Box::pin(stream))
    }
}

fn policy_call(id: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "loop_control".into(),
            arguments: arguments.to_string(),
        },
    }
}

fn tool_start_count(events: &[AgentEvent], id: &str) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == id))
        .count()
}

fn completed_result<'a>(events: &'a [AgentEvent], id: &str) -> &'a ToolResult {
    let results = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } if tool_call_id == id => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1, "one ToolComplete for {id}");
    results[0]
}

fn assert_policy_run_integrity(
    fixture: &Fixture,
    provider: &PolicyContractProvider,
    events: &[AgentEvent],
    expected_provider_calls: usize,
) {
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        expected_provider_calls
    );
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        1
    );
    for call in fixture
        .session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
    {
        let result = completed_result(events, &call.id);
        let paired = fixture
            .session
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(call.id.as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(paired.len(), 1, "one paired result for {}", call.id);
        assert_eq!(paired[0].tool_success, Some(result.success), "{}", call.id);
        assert_eq!(paired[0].content, result.result, "{}", call.id);
    }
}

#[tokio::test]
async fn self_control_owner_rejections_open_the_same_batch_policy_circuit() {
    let mut fixture = Fixture::new(1, 4).await;
    let config = fixture.config.as_mut().unwrap();
    config.max_consecutive_failures_per_tool = 2;
    config.max_tool_calls_per_round = 16;
    let provider = Arc::new(PolicyContractProvider::new(
        PolicyContractScript::StaleBatch,
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_policy_run_integrity(&fixture, &provider, &events, 3);
    assert_eq!(tool_start_count(&events, "start"), 1);
    for (index, id) in STALE_IDS.iter().enumerate() {
        let result = completed_result(&events, id);
        assert!(!result.success, "{id}");
        if index < 2 {
            assert_eq!(tool_start_count(&events, id), 1, "owner must handle {id}");
            let rejection: Value = serde_json::from_str(&result.result).unwrap();
            assert_eq!(rejection["error"], "stale_generation_or_revision", "{id}");
        } else {
            assert_eq!(
                tool_start_count(&events, id),
                0,
                "policy must block {id} before executor entry"
            );
            assert!(
                result
                    .result
                    .contains("consecutive failures reached circuit limit"),
                "{id}: {}",
                result.result
            );
            assert!(
                !result.result.contains("stale_generation_or_revision"),
                "{id}"
            );
        }
    }
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Completed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (1, 1, 3)
    );
    assert_eq!((state.max_iterations, state.max_total_rounds), (1, 4));
    assert_eq!(
        (state.objective.as_str(), state.objective_revision),
        (FIRST_OBJECTIVE, 1)
    );
    let control = state.control.as_ref().unwrap();
    assert_eq!((control.control_revision, control.applied_revision), (1, 1));
    assert_eq!(control.operations.len(), 1, "only start may be accepted");
    assert!(control.pending_objective.is_none());
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1)]
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
    fixture.close().await;
}

#[tokio::test]
async fn self_control_successful_status_charges_the_round_call_budget_once() {
    let mut fixture = Fixture::new(1, 4).await;
    let config = fixture.config.as_mut().unwrap();
    config.max_tool_calls_per_round = 3;
    config.max_consecutive_failures_per_tool = 2;
    let provider = Arc::new(PolicyContractProvider::new(
        PolicyContractScript::StatusBatch,
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_policy_run_integrity(&fixture, &provider, &events, 2);
    for (index, id) in STATUS_IDS.iter().enumerate() {
        let result = completed_result(&events, id);
        if index < 3 {
            assert_eq!(
                tool_start_count(&events, id),
                1,
                "one accounting charge must admit {id}"
            );
            assert!(result.success, "{id}: {}", result.result);
            let status: Value = serde_json::from_str(&result.result).unwrap();
            assert_eq!(status["loop"]["generation"], Value::Null);
            assert!(status.get("requires_owner_apply").is_none());
        } else {
            assert_eq!(
                tool_start_count(&events, id),
                0,
                "the fourth call exceeds the round cap"
            );
            assert!(!result.success);
            assert!(
                result
                    .result
                    .contains("per-round tool call limit (3) reached"),
                "{}",
                result.result
            );
        }
    }
    assert!(
        read_loop_state(&fixture.session).unwrap().is_none(),
        "status cannot start a loop"
    );
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(read_loop_state(&durable).unwrap().is_none());
    fixture.close().await;
}
