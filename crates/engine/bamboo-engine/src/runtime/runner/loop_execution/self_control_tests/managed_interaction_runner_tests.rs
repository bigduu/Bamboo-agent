//! Tests-only fragment for self_control_tests/managed_interaction_runner_tests.rs.
//! Integration requires only `mod managed_interaction_runner_tests;` in the parent.
//! Uses the existing real Fixture registration, runner, BuiltinToolExecutor and
//! permission chain. The registered foreign test tool returns interaction shapes;
//! it never writes a Session, starts a process, requests authority, or invents an
//! owner. No new suspension API is needed to compile these tests on the RED source.

use super::*;
use bamboo_agent_core::tools::{
    AgenticToolResult, Tool, ToolCtx, ToolError, ToolExecutor, ToolOutcome, ToolResult,
};

const FOREIGN_TOOL: &str = "mcp__contract_fixture__interaction";
const INTERACTION_CALL: &str = "foreign-interaction";

#[derive(Clone, Copy, Debug)]
enum InteractionShape {
    TypedNeedsHuman,
    LegacyPermission,
    AgenticClarification,
    WaitingForChildren,
}

const INTERACTION_SHAPES: [InteractionShape; 4] = [
    InteractionShape::TypedNeedsHuman,
    InteractionShape::LegacyPermission,
    InteractionShape::AgenticClarification,
    InteractionShape::WaitingForChildren,
];

struct ForeignInteractionTool {
    shape: InteractionShape,
    expect_started: bool,
    storage: Arc<dyn Storage>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for ForeignInteractionTool {
    fn name(&self) -> &str {
        FOREIGN_TOOL
    }

    fn description(&self) -> &str {
        "Synthetic foreign interaction result for bounded runner contract tests."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }

    // The default classification is mutating, serial and non-promotable. Thus
    // Start and the following foreign call exercise serial apply in one batch.
    async fn invoke(&self, _: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(ctx.session_id(), Some(SESSION_ID));
        assert_eq!(ctx.tool_call_id.as_ref(), INTERACTION_CALL);
        assert!(ctx.executing_supervisor.is_none());
        assert!(!ctx.plan_read_only);
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let state = read_loop_state(&durable).unwrap();
        if self.expect_started {
            let state =
                state.expect("the real Start receipt must commit before foreign invocation");
            assert_eq!(state.status, LoopStatus::Running);
            assert_eq!(
                durable
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .execution_id
                    .as_deref(),
                Some(state.owner_execution_id.as_str()),
                "foreign output must be handled by the original exact owner"
            );
        } else {
            assert!(state.is_none(), "host opt-in is not managed activation");
        }
        let question = "Choose whether this synthetic interaction may continue.";
        match self.shape {
            InteractionShape::TypedNeedsHuman => Ok(ToolOutcome::NeedsHuman {
                question: bamboo_agent_core::PendingQuestion {
                    tool_call_id: ctx.tool_call_id.to_string(),
                    tool_name: FOREIGN_TOOL.into(),
                    question: question.into(),
                    options: vec!["yes".into(), "no".into()],
                    allow_custom: false,
                    source: bamboo_agent_core::PendingQuestionSource::PauseTool,
                },
                result: ToolResult::text(false, "Synthetic human decision requested."),
            }),
            InteractionShape::LegacyPermission => {
                // This is deliberately only a foreign result shape. It grants
                // no permission and bypasses no native permission check.
                let mut result = ToolResult::text(
                    true,
                    json!({
                        "status":"awaiting_permission_approval",
                        "question":question,
                        "options":["yes","no"],
                        "allow_custom":false,
                    })
                    .to_string(),
                );
                result.display_preference = Some("request_permissions".into());
                Ok(ToolOutcome::Completed(result))
            }
            InteractionShape::AgenticClarification => Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                serde_json::to_string(&AgenticToolResult::NeedClarification {
                    question: question.into(),
                    options: Some(vec!["yes".into(), "no".into()]),
                })
                .unwrap(),
            ))),
            InteractionShape::WaitingForChildren => {
                // No child is created and no wait is stored by the fixture.
                // The engine must not turn an untrusted result marker into a
                // managed suspension, even if there is no real child to await.
                let mut result = ToolResult::text(
                    true,
                    json!({"runtime_control":"waiting_for_children"}).to_string(),
                );
                result.display_preference = Some("runtime_control:waiting_for_children".into());
                Ok(ToolOutcome::Completed(result))
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum InteractionSequence {
    SameBatchAsStart,
    AfterStart,
    WithoutStart,
}

struct InteractionProvider {
    sequence: InteractionSequence,
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    seen: Mutex<Vec<Vec<Message>>>,
    durable_seen: Mutex<Vec<Session>>,
}

impl InteractionProvider {
    fn new(sequence: InteractionSequence, storage: Arc<dyn Storage>) -> Self {
        Self {
            sequence,
            storage,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
            durable_seen: Mutex::new(Vec::new()),
        }
    }
}

fn foreign_interaction_call() -> ToolCall {
    ToolCall {
        id: INTERACTION_CALL.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: FOREIGN_TOOL.into(),
            arguments: "{}".into(),
        },
    }
}

#[async_trait]
impl LLMProvider for InteractionProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;

        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert_eq!(active, 1, "one real owner, one provider stream");
        let guard = ActiveStream(self.active.clone());
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == "loop_control"));
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == FOREIGN_TOOL));
        self.seen.lock().unwrap().push(messages.to_vec());
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        self.durable_seen.lock().unwrap().push(durable);
        let output = match (self.sequence, round) {
            (InteractionSequence::SameBatchAsStart, 1) => {
                let LLMChunk::ToolCalls(mut calls) = start_call(2, 8) else {
                    unreachable!("existing helper emits the real Start call")
                };
                calls.push(foreign_interaction_call());
                LLMChunk::ToolCalls(calls)
            }
            (InteractionSequence::AfterStart, 1) => start_call(2, 8),
            (InteractionSequence::AfterStart, 2) | (InteractionSequence::WithoutStart, 1) => {
                LLMChunk::ToolCalls(vec![foreign_interaction_call()])
            }
            // RED sources may suspend before reaching these rounds. Do not
            // assume a paired success, reject a legitimate suspension here, or
            // synthesize a terminal state. GREEN completion needs actual later
            // provider answers admitted by the same real runner.
            _ => LLMChunk::Token(format!(
                "Real bounded objective answer from provider round {round}."
            )),
        };
        let stream = futures::stream::iter([Ok(output), Ok(LLMChunk::Done)]).map(move |item| {
            let _keep_stream_admitted = &guard;
            item
        });
        Ok(Box::pin(stream))
    }
}

async fn run_interaction_fixture(
    fixture: &mut Fixture,
    provider: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
) -> (Result<(), AgentError>, Vec<AgentEvent>) {
    let (tx, mut rx) = mpsc::channel(512);
    let storage = fixture.storage.clone();
    let config = fixture.config.take().expect("one activation per fixture");
    let run = run_agent_loop_with_config(
        &mut fixture.session,
        INITIAL_PROMPT.into(),
        tx,
        provider,
        tools,
        CancellationToken::new(),
        config,
    );
    tokio::pin!(run);
    let mut events = Vec::new();
    let result = loop {
        tokio::select! {
            result = &mut run => break result,
            event = rx.recv() => {
                if let Some(event) = event {
                    observe_start_receipt(storage.as_ref(), &event).await;
                    events.push(event);
                }
            }
        }
    };
    while let Ok(event) = rx.try_recv() {
        observe_start_receipt(storage.as_ref(), &event).await;
        events.push(event);
    }
    (result, events)
}

async fn observe_start_receipt(storage: &dyn Storage, event: &AgentEvent) {
    if matches!(event, AgentEvent::ToolComplete { tool_call_id, result }
        if tool_call_id == "start" && result.success)
    {
        // The immutable owner receipt has a durable-before-publication
        // contract. Ordinary foreign tool events do not share that promise.
        Fixture::observe_event(storage, event).await;
    }
}

fn assert_no_interaction_wait(session: &Session) {
    assert!(session.pending_question.is_none());
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    // Before the first provider response an ordinary opt-in activation need
    // not have checkpointed a typed runtime yet. Absence is not a wait. The
    // final managed assertions below separately require a Completed runtime.
    if let Some(runtime) = session.agent_runtime_state.as_ref() {
        assert_ne!(runtime.status, bamboo_domain::AgentStatusState::Suspended);
        assert!(runtime.suspension.is_none());
        assert!(runtime.waiting_for_children.is_none());
        assert!(runtime.waiting_for_bash.is_none());
    }
}

fn assert_one_false_interaction_result(session: &Session, events: &[AgentEvent]) {
    let paired = session
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(INTERACTION_CALL)
        })
        .collect::<Vec<_>>();
    assert_eq!(paired.len(), 1, "one real paired foreign result");
    assert_eq!(
        paired[0].tool_success,
        Some(false),
        "interaction must become an ordinary failed tool result"
    );
    assert!(!paired[0].content.is_empty());
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
                AgentEvent::ToolStart { tool_call_id, tool_name, .. }
                    if tool_call_id == INTERACTION_CALL && tool_name == FOREIGN_TOOL
            ))
            .count(),
        1
    );
    let published = events.iter().filter(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, .. } | AgentEvent::ToolError { tool_call_id, .. }
            if tool_call_id == INTERACTION_CALL
    )).collect::<Vec<_>>();
    assert_eq!(
        published.len(),
        1,
        "one legal false completion or error, without a successful interaction event"
    );
    if let AgentEvent::ToolComplete { result, .. } = published[0] {
        assert!(!result.success);
        assert_eq!(result.result, paired[0].content);
    }
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
}

async fn assert_managed_interaction_continues(
    shape: InteractionShape,
    sequence: InteractionSequence,
) {
    let mut fixture = Fixture::new(2, 8).await;
    let capability = fixture
        .config
        .as_ref()
        .unwrap()
        .loop_control
        .as_ref()
        .unwrap()
        .clone();
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(ForeignInteractionTool {
            shape,
            expect_started: true,
            storage: fixture.storage.clone(),
            calls: tool_calls.clone(),
        })
        .unwrap();
    let provider = Arc::new(InteractionProvider::new(sequence, fixture.storage.clone()));
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_interaction_fixture(&mut fixture, provider.clone(), Arc::new(tools)),
    )
    .await
    .expect("synthetic managed run must be bounded");
    result.unwrap();
    assert_eq!(
        tool_calls.load(Ordering::SeqCst),
        1,
        "{shape:?}/{sequence:?}"
    );
    let expected_rounds = match sequence {
        InteractionSequence::SameBatchAsStart => 3,
        InteractionSequence::AfterStart => 4,
        InteractionSequence::WithoutStart => unreachable!(),
    };
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        expected_rounds,
        "{shape:?}/{sequence:?}"
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
    assert_one_false_interaction_result(&fixture.session, &events);
    assert_no_interaction_wait(&fixture.session);
    let start = paired_result(&fixture.session.messages, "start");
    let receipt: LoopControlReceipt = serde_json::from_value(start).unwrap();
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(
        state.status,
        LoopStatus::Completed,
        "{shape:?}/{sequence:?}"
    );
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (2, 2, expected_rounds as u32)
    );
    assert_eq!(state.generation, receipt.generation);
    assert_eq!(capability.generation(), Some(state.generation.as_str()));
    assert_eq!(
        state.control.as_ref().unwrap().operations.len(),
        1,
        "only the real Start is accepted"
    );
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1), (FIRST_OBJECTIVE, 1)]
    );
    let runtime = fixture.session.agent_runtime_state.as_ref().unwrap();
    assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Completed);
    assert_eq!(
        runtime.execution_id.as_deref(),
        Some(state.owner_execution_id.as_str())
    );
    assert_eq!(runtime.round.current_round, expected_rounds as u32);
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap().unwrap(), state);
    assert_no_interaction_wait(&durable);
    assert_one_false_interaction_result(&durable, &events);
    for snapshot in provider.durable_seen.lock().unwrap().iter() {
        assert_no_interaction_wait(snapshot);
    }
    let observations = provider.seen.lock().unwrap();
    for messages in observations.iter() {
        assert!(messages
            .iter()
            .any(|message| message.content == ORIGINAL_FACT));
        assert!(!messages.iter().any(|message| message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("runtime_kind"))
            .and_then(Value::as_str)
            == Some("max_rounds_summary")));
    }
    let answer_round = match sequence {
        InteractionSequence::SameBatchAsStart => 2,
        InteractionSequence::AfterStart => 3,
        InteractionSequence::WithoutStart => unreachable!(),
    };
    for round in answer_round..=expected_rounds {
        let answer = format!("Real bounded objective answer from provider round {round}.");
        assert!(
            fixture
                .session
                .messages
                .iter()
                .any(|message| message.role == Role::Assistant && message.content == answer),
            "each iteration requires its actual no-tool answer"
        );
    }
    drop(observations);
    fixture.close().await;
}

#[tokio::test]
async fn self_control_managed_foreign_interaction_shapes_cannot_suspend_after_start() {
    for shape in INTERACTION_SHAPES {
        assert_managed_interaction_continues(shape, InteractionSequence::AfterStart).await;
    }
}

#[tokio::test]
async fn self_control_start_and_foreign_question_same_batch_recheck_active_policy() {
    for shape in INTERACTION_SHAPES {
        assert_managed_interaction_continues(shape, InteractionSequence::SameBatchAsStart).await;
    }
}

#[tokio::test]
async fn self_control_host_opt_in_without_start_preserves_ordinary_foreign_question_suspend() {
    let mut fixture = Fixture::new(2, 8).await;
    let capability = fixture
        .config
        .as_ref()
        .unwrap()
        .loop_control
        .as_ref()
        .unwrap()
        .clone();
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(ForeignInteractionTool {
            shape: InteractionShape::TypedNeedsHuman,
            expect_started: false,
            storage: fixture.storage.clone(),
            calls: tool_calls.clone(),
        })
        .unwrap();
    let provider = Arc::new(InteractionProvider::new(
        InteractionSequence::WithoutStart,
        fixture.storage.clone(),
    ));
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_interaction_fixture(&mut fixture, provider.clone(), Arc::new(tools)),
    )
    .await
    .expect("ordinary synthetic question must suspend promptly");
    result.unwrap();
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert!(capability.generation().is_none());
    assert!(capability.active_counted().is_none());
    assert!(read_loop_state(&fixture.session).unwrap().is_none());
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    assert!(!fixture
        .session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
        .any(|call| call.id == "start"));
    let question = fixture
        .session
        .pending_question
        .as_ref()
        .expect("ordinary question remains interactive");
    assert_eq!(question.tool_call_id, INTERACTION_CALL);
    assert_eq!(question.tool_name, FOREIGN_TOOL);
    assert_eq!(
        fixture.session.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Suspended
    );
    assert_eq!(
        fixture
            .session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("awaiting_clarification")
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
                AgentEvent::NeedClarification { tool_call_id: Some(id), tool_name: Some(name), .. }
                    if id == INTERACTION_CALL && name == FOREIGN_TOOL
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        0
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(read_loop_state(&durable).unwrap().is_none());
    let durable_question = durable
        .pending_question
        .as_ref()
        .expect("ordinary question is durable");
    assert_eq!(durable_question.tool_call_id, INTERACTION_CALL);
    assert_eq!(durable_question.tool_name, FOREIGN_TOOL);
    assert_eq!(
        durable.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Suspended
    );
    fixture.close().await;
}
