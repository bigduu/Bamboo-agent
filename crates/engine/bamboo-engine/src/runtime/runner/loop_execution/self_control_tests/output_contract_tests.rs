//! Output-budget contracts for owner-produced acknowledgements, not placeholders.

use super::*;
use bamboo_agent_core::tools::ToolResult;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};

const STATUS_CALL: &str = "bounded-status";
const STATUS_CAP: u32 = 512;

fn result_pair<'a>(messages: &'a [Message], id: &str) -> &'a Message {
    let mut pairs = messages.iter().filter(|message| {
        message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id)
    });
    let pair = pairs.next().expect("tool call must have a paired result");
    assert!(pairs.next().is_none(), "duplicate ToolResult for {id}");
    pair
}

fn completed_result<'a>(events: &'a [AgentEvent], id: &str) -> &'a ToolResult {
    let mut results = events.iter().filter_map(|event| match event {
        AgentEvent::ToolComplete {
            tool_call_id,
            result,
        } if tool_call_id == id => Some(result),
        _ => None,
    });
    let result = results.next().expect("ToolComplete must be emitted");
    assert!(results.next().is_none(), "duplicate ToolComplete for {id}");
    result
}

fn assert_within_cap(text: &str, cap: u32) {
    let tokens = TiktokenTokenCounter::default().count_text(text);
    assert!(
        tokens <= cap,
        "actual owner result used {tokens} tokens, cap {cap}"
    );
}

struct OutputCheckpointRecorder {
    inner: Arc<dyn RuntimeSessionPersistence>,
    snapshots: Mutex<Vec<Session>>,
}

#[async_trait]
impl RuntimeSessionPersistence for OutputCheckpointRecorder {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.checkpoint_runtime_session(session).await?;
        self.snapshots.lock().unwrap().push(session.clone());
        Ok(())
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn load_runtime_admission_snapshot(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_admission_snapshot(id).await
    }
}

#[derive(Clone)]
struct OutputObservation {
    messages: Vec<Message>,
    durable: Session,
}

struct OutputProvider {
    storage: Arc<dyn Storage>,
    tiny_start_only: bool,
    objective: String,
    pending_objective: String,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    seen: Mutex<Vec<OutputObservation>>,
}

impl OutputProvider {
    fn new(storage: Arc<dyn Storage>, tiny_start_only: bool) -> Self {
        let length = crate::runtime::counted_loop::MAX_LOOP_PROMPT_BYTES / 3;
        Self {
            storage,
            tiny_start_only,
            objective: "界".repeat(length),
            pending_objective: "語".repeat(length),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn response(&self, round: usize, messages: &[Message]) -> LLMChunk {
        if self.tiny_start_only {
            return match round {
                1 => start_call(2, 8),
                2 => LLMChunk::Token("The bounded management command could not be applied.".into()),
                _ => panic!("tiny-budget rejection must not start an iteration or summary"),
            };
        }
        match round {
            1 => control_call(
                "start",
                json!({
                    "action":"start",
                    "operation_id": START_OPERATION,
                    "expected_revision": 0,
                    "objective": self.objective,
                    "max_iterations": 2,
                    "max_total_rounds": 8,
                }),
            ),
            2 => {
                let receipt = paired_result(messages, "start");
                control_call(
                    "update",
                    json!({
                        "action":"update",
                        "operation_id": UPDATE_OPERATION,
                        "generation": receipt["generation"],
                        "expected_revision": receipt["accepted_revision"],
                        "objective": self.pending_objective,
                    }),
                )
            }
            3 => control_call(STATUS_CALL, json!({"action":"status"})),
            4 => LLMChunk::Token("The old objective is complete.".into()),
            5 => LLMChunk::Token("The replacement objective is complete.".into()),
            _ => panic!("no additional provider call or final summary is permitted"),
        }
    }
}

#[async_trait]
impl LLMProvider for OutputProvider {
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
        assert_eq!(active, 1, "serial owner may not overlap provider calls");
        let guard = ActiveStream(self.active.clone());
        assert!(tools
            .iter()
            .any(|tool| tool.function.name == "loop_control"));
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        self.seen.lock().unwrap().push(OutputObservation {
            messages: messages.to_vec(),
            durable,
        });
        let output = self.response(round, messages);
        let stream = futures::stream::iter([Ok(output), Ok(LLMChunk::Done)]).map(move |chunk| {
            let _keep_stream_alive = &guard;
            chunk
        });
        Ok(Box::pin(stream))
    }
}

struct LargeStatusFeedback {
    text: String,
    calls: AtomicUsize,
    checkpointed_before_hook: Mutex<Option<String>>,
    recorder: Arc<OutputCheckpointRecorder>,
}

#[async_trait]
impl bamboo_agent_core::AgentHook for LargeStatusFeedback {
    fn point(&self) -> bamboo_domain::AgentHookPoint {
        bamboo_domain::AgentHookPoint::AfterToolExecution
    }

    fn matches(&self, payload: &bamboo_domain::HookPayload) -> bool {
        matches!(payload, bamboo_domain::HookPayload::ToolResult { tool_call_id, .. } if tool_call_id == STATUS_CALL)
    }

    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        payload: &bamboo_domain::HookPayload,
        session: &Session,
    ) -> bamboo_domain::HookResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let bamboo_domain::HookPayload::ToolResult { outcome, .. } = payload else {
            panic!("AfterToolExecution must carry the actual owner result");
        };
        assert!(outcome.success);
        let text = outcome.result.as_ref().unwrap();
        assert_within_cap(text, STATUS_CAP);
        let body: Value = serde_json::from_str(text).unwrap();
        assert!(body.get("hook_feedback").is_none());
        assert_eq!(result_pair(&session.messages, STATUS_CALL).content, *text);
        let snapshots = self.recorder.snapshots.lock().unwrap();
        assert_eq!(
            result_pair(&snapshots.last().unwrap().messages, STATUS_CALL).content,
            *text,
            "first checkpoint must precede native feedback"
        );
        *self.checkpointed_before_hook.lock().unwrap() = Some(text.clone());
        bamboo_domain::HookResult::InjectContext {
            text: self.text.clone(),
        }
    }
}

async fn run_large_status_case(with_feedback: bool) {
    let mut fixture = Fixture::new(2, 8).await;
    fixture.session.token_budget = Some(bamboo_domain::TokenBudget {
        max_tool_output_tokens: STATUS_CAP,
        ..Default::default()
    });
    fixture
        .storage
        .save_session(&fixture.session)
        .await
        .unwrap();
    let recorder = Arc::new(OutputCheckpointRecorder {
        inner: fixture
            .config
            .as_ref()
            .unwrap()
            .persistence
            .clone()
            .unwrap(),
        snapshots: Mutex::new(Vec::new()),
    });
    fixture.config.as_mut().unwrap().persistence = Some(recorder.clone());
    let feedback = Arc::new(LargeStatusFeedback {
        text: "Feedback with quotes \" and escaped lines.\n界".repeat(4_000),
        calls: AtomicUsize::new(0),
        checkpointed_before_hook: Mutex::new(None),
        recorder: recorder.clone(),
    });
    if with_feedback {
        let mut hooks = crate::runtime::HookRunner::new();
        hooks.register(feedback.clone());
        fixture.config.as_mut().unwrap().hook_runner = Arc::new(hooks);
    }
    let provider = Arc::new(OutputProvider::new(fixture.storage.clone(), false));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
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
    let seen = provider.seen.lock().unwrap().clone();
    for id in ["start", "update", STATUS_CALL] {
        let emitted = completed_result(&events, id);
        assert!(
            emitted.success,
            "fixed protocol metadata fits the status budget"
        );
        assert_within_cap(&emitted.result, STATUS_CAP);
        assert_eq!(
            result_pair(&fixture.session.messages, id).content,
            emitted.result
        );
        assert_eq!(result_pair(&durable.messages, id).content, emitted.result);
        for observation in seen.iter().filter(|observation| {
            observation
                .messages
                .iter()
                .any(|message| message.tool_call_id.as_deref() == Some(id))
        }) {
            assert_eq!(
                result_pair(&observation.messages, id).content,
                emitted.result
            );
            assert_eq!(
                result_pair(&observation.durable.messages, id).content,
                emitted.result
            );
        }
    }
    for (id, revision, operation) in [
        ("start", 1, START_OPERATION),
        ("update", 2, UPDATE_OPERATION),
    ] {
        let receipt: LoopControlReceipt =
            serde_json::from_str(&completed_result(&events, id).result).unwrap();
        assert_eq!(receipt.operation_id, operation);
        assert_eq!(receipt.accepted_revision, revision);
        assert!(!receipt.applied);
        assert_eq!(receipt.effective_iteration, Some(revision as u32));
    }
    let status: Value =
        serde_json::from_str(&completed_result(&events, STATUS_CALL).result).unwrap();
    assert_eq!(status["loop"]["control_revision"], 2);
    assert_eq!(status["loop"]["applied_revision"], 1);
    assert_eq!(status["loop"]["objective_revision"], 1);
    assert_eq!(status["loop"]["admitted_objective_revision"], 1);
    assert_eq!(status["host_max_iterations"], 2);
    assert_eq!(status["host_max_total_rounds"], 8);
    assert_eq!(status["can_start"], false);
    assert!(!completed_result(&events, STATUS_CALL)
        .result
        .contains(&provider.objective));
    assert!(!completed_result(&events, STATUS_CALL)
        .result
        .contains(&provider.pending_objective));

    // Status projection must never truncate canonical current or pending goals.
    let pending = read_loop_state(&seen[3].durable).unwrap().unwrap();
    assert_eq!(pending.objective, provider.objective);
    assert_eq!(
        pending
            .control
            .as_ref()
            .unwrap()
            .pending_objective
            .as_ref()
            .unwrap()
            .objective,
        provider.pending_objective
    );
    assert_eq!(pending.control.as_ref().unwrap().operations.len(), 2);
    assert_eq!(
        admitted_objectives(&seen[3].messages),
        vec![(provider.objective.as_str(), 1)]
    );
    assert_eq!(
        admitted_objectives(&seen[4].messages),
        vec![
            (provider.objective.as_str(), 1),
            (provider.pending_objective.as_str(), 2)
        ]
    );
    let final_state = read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(final_state.status, LoopStatus::Completed);
    assert_eq!(final_state.objective, provider.pending_objective);
    assert_eq!(
        (
            final_state.iterations_started,
            final_state.iterations_completed,
            final_state.rounds_started
        ),
        (2, 2, 5)
    );

    let snapshots = recorder.snapshots.lock().unwrap().clone();
    let status_checkpoints = snapshots
        .iter()
        .filter_map(|session| {
            session.messages.iter().find(|message| {
                message.tool_call_id.as_deref() == Some(STATUS_CALL) && message.role == Role::Tool
            })
        })
        .collect::<Vec<_>>();
    assert!(!status_checkpoints.is_empty());
    for pair in &status_checkpoints {
        assert_within_cap(&pair.content, STATUS_CAP);
        let _: Value = serde_json::from_str(&pair.content).unwrap();
    }
    if with_feedback {
        assert_eq!(feedback.calls.load(Ordering::SeqCst), 1);
        assert!(
            status.get("hook_feedback").is_some(),
            "feedback must stay structured JSON"
        );
        let before = feedback.checkpointed_before_hook.lock().unwrap();
        let before = before.as_ref().unwrap();
        assert_ne!(*before, completed_result(&events, STATUS_CALL).result);
        let before: Value = serde_json::from_str(before).unwrap();
        assert_eq!(
            before["loop"], status["loop"],
            "feedback may not rewrite status fields"
        );
        assert!(
            status_checkpoints
                .iter()
                .any(|pair| pair.content == completed_result(&events, STATUS_CALL).result),
            "feedback must be checkpointed on the same pair before publication"
        );
    } else {
        assert_eq!(feedback.calls.load(Ordering::SeqCst), 0);
    }
    fixture.close().await;
}

#[tokio::test]
async fn owner_status_output_is_bounded_before_checkpoint_and_matches_provider_pair() {
    run_large_status_case(false).await;
}

#[tokio::test]
async fn owner_status_feedback_remains_bounded_json_and_is_checkpointed_on_same_pair() {
    run_large_status_case(true).await;
}

#[tokio::test]
async fn tiny_output_caps_reject_start_without_installing_a_generation() {
    for cap in [1, 16, 64] {
        let mut fixture = Fixture::new(2, 8).await;
        fixture.session.token_budget = Some(bamboo_domain::TokenBudget {
            max_tool_output_tokens: cap,
            ..Default::default()
        });
        fixture
            .storage
            .save_session(&fixture.session)
            .await
            .unwrap();
        let policy = fixture
            .config
            .as_ref()
            .unwrap()
            .loop_control
            .clone()
            .unwrap();
        let provider = Arc::new(OutputProvider::new(fixture.storage.clone(), true));
        let (result, events) = fixture
            .run(provider.clone(), CancellationToken::new())
            .await;
        assert!(result.is_ok(), "cap {cap}: {result:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        let failure = completed_result(&events, "start");
        assert!(
            !failure.success,
            "a complete mutation receipt does not fit cap {cap}"
        );
        assert_within_cap(&failure.result, cap);
        if !failure.result.is_empty() {
            let body: Value = serde_json::from_str(&failure.result).unwrap();
            assert!(
                body.get("applied").is_none(),
                "a budget delivery failure must not claim unapplied"
            );
            assert!(body.get("generation").is_none());
        }
        assert!(policy.generation().is_none());
        assert!(policy.active_counted().is_none());
        assert!(read_loop_state(&fixture.session).unwrap().is_none());
        assert!(admitted_objectives(&fixture.session.messages).is_empty());
        let durable = fixture
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        assert!(read_loop_state(&durable).unwrap().is_none());
        for session in [&fixture.session, &durable] {
            let pair = result_pair(&session.messages, "start");
            assert_eq!(pair.tool_success, Some(false));
            assert_eq!(pair.content, failure.result);
        }
        let seen = provider.seen.lock().unwrap().clone();
        let pair = result_pair(&seen[1].messages, "start");
        assert_eq!(pair.tool_success, Some(false));
        assert_eq!(pair.content, failure.result);
        assert_eq!(
            result_pair(&seen[1].durable.messages, "start").content,
            failure.result
        );
        assert!(read_loop_state(&seen[1].durable).unwrap().is_none());
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::Complete { .. }))
                .count(),
            1
        );
        fixture.close().await;
    }
}
