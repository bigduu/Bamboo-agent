//! Tests-only sibling: self_control_tests/running_timeout_observation_tests.rs.
//! Parent adds `mod running_timeout_observation_tests;`; no Fixture API changes.
//! Uses only existing APIs on 8da. A real parallel batch cancels a plugin's
//! invoke future after that plugin has collapsed a RunningHandle. A report
//! returned only by the cancelled per-call future must not erase observation.

use super::*;
use bamboo_agent_core::tools::{
    AsyncWaitKind, RunningCompletion, RunningHandle, Tool, ToolClass, ToolCtx, ToolError,
    ToolExecutor, ToolOutcome, ToolResult,
};

const PENDING_TOOL: &str = "mcp__contract_fixture__pending_after_running";
const PENDING_CALL: &str = "pending-after-running";
const CONTROL_TOOL: &str = "mcp__contract_fixture__timeout_control";
const CONTROL_CALL: &str = "timeout-control";
const INNER_ACK: &str = "{\"status\":\"running\",\"ordinary_inner_ack\":true}";
const CONTROL_RESULT: &str = "Ordinary parallel control completed.";

#[derive(Clone, Copy, Debug)]
struct TimeoutCase {
    batch_timeout: bool,
    same_round_start: bool,
}

#[derive(Default)]
struct TimeoutProbe {
    pending_invoked: AtomicUsize,
    collapsed_ack: Mutex<Option<ToolResult>>,
    invoke_dropped: AtomicUsize,
    kills: AtomicUsize,
    control_invoked: AtomicUsize,
    scope_allowed: Mutex<Vec<bool>>,
}

struct PendingInvocationDrop(Arc<TimeoutProbe>);

impl Drop for PendingInvocationDrop {
    fn drop(&mut self) {
        self.0.invoke_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingAfterCollapsedRunning(Arc<TimeoutProbe>);

#[async_trait]
impl Tool for PendingAfterCollapsedRunning {
    fn name(&self) -> &str {
        PENDING_TOOL
    }
    fn description(&self) -> &str {
        "Synthetic plugin records Running, then stays pending until timeout."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn classify(&self, _: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    async fn invoke(&self, _: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        assert_eq!(ctx.session_id(), Some(SESSION_ID));
        assert_eq!(ctx.tool_call_id.as_ref(), PENDING_CALL);
        assert!(ctx.executing_supervisor.is_none());
        self.0.pending_invoked.fetch_add(1, Ordering::SeqCst);
        let _on_cancellation = PendingInvocationDrop(self.0.clone());
        self.0
            .scope_allowed
            .lock()
            .unwrap()
            .push(bamboo_agent_core::tools::suspension_allowed());
        let cleanup = self.0.clone();
        let ack = ToolOutcome::Running(RunningHandle {
            tool_call_id: ctx.tool_call_id.to_string(),
            ack: ToolResult::text(true, INNER_ACK),
            completion: RunningCompletion::Detached,
            wait_kind: AsyncWaitKind::AsyncTools,
            kill: Box::new(move || {
                cleanup.kills.fetch_add(1, Ordering::SeqCst);
            }),
        })
        .into_tool_result();
        *self.0.collapsed_ack.lock().unwrap() = Some(ack);
        // No spawned task or real external resource exists. The real runner's
        // timeout must drop this pending invocation, including its per-call
        // observer future; no observation tuple is returned from that future.
        std::future::pending::<Result<ToolOutcome, ToolError>>().await
    }
}

struct ParallelTimeoutControl(Arc<TimeoutProbe>);

#[async_trait]
impl Tool for ParallelTimeoutControl {
    fn name(&self) -> &str {
        CONTROL_TOOL
    }
    fn description(&self) -> &str {
        "Ordinary immediate result in the actual parallel batch."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn classify(&self, _: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(&self, _: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        assert_eq!(ctx.session_id(), Some(SESSION_ID));
        assert_eq!(ctx.tool_call_id.as_ref(), CONTROL_CALL);
        self.0.control_invoked.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            CONTROL_RESULT,
        )))
    }
}

fn timeout_call(id: &str, name: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: "{}".into(),
        },
    }
}

struct TimeoutObservationProvider {
    case: TimeoutCase,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

#[async_trait]
impl LLMProvider for TimeoutObservationProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        schemas: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;
        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert_eq!(active, 1);
        let guard = ActiveStream(self.active.clone());
        assert!(schemas
            .iter()
            .any(|schema| schema.function.name == PENDING_TOOL));
        assert!(schemas
            .iter()
            .any(|schema| schema.function.name == CONTROL_TOOL));
        let chunk = if round == 1 {
            let mut calls = vec![
                timeout_call(PENDING_CALL, PENDING_TOOL),
                timeout_call(CONTROL_CALL, CONTROL_TOOL),
            ];
            if self.case.same_round_start {
                let LLMChunk::ToolCalls(start) = start_call(2, 8) else {
                    unreachable!("existing Start helper returns ToolCalls")
                };
                calls.extend(start);
            }
            LLMChunk::ToolCalls(calls)
        } else if round == 2 && !self.case.same_round_start {
            start_call(2, 8)
        } else {
            // Works both after the intended ordinary refusal and on the RED
            // source, where Start incorrectly admits two counted iterations.
            // Never call paired_result on the potentially refused Start here.
            LLMChunk::Token(format!(
                "Ordinary answer after timed-out Running observation, round {round}."
            ))
        };
        Ok(Box::pin(
            futures::stream::iter([Ok(chunk), Ok(LLMChunk::Done)]).map(move |item| {
                let _keep_stream_admitted = &guard;
                item
            }),
        ))
    }
}

async fn run_timeout_observation_fixture(
    fixture: &mut Fixture,
    provider: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
) -> (Result<(), AgentError>, Vec<AgentEvent>) {
    let (tx, mut rx) = mpsc::channel(512);
    let storage = fixture.storage.clone();
    let config = fixture.config.take().expect("one real activation");
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
                    if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
                        if tool_call_id == "start" && result.success)
                    { Fixture::observe_event(storage.as_ref(), &event).await; }
                    events.push(event);
                }
            }
        }
    };
    while let Ok(event) = rx.try_recv() {
        if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == "start" && result.success)
        {
            Fixture::observe_event(storage.as_ref(), &event).await;
        }
        events.push(event);
    }
    (result, events)
}

fn timeout_pair<'a>(session: &'a Session, id: &str) -> &'a Message {
    let pairs = session
        .messages
        .iter()
        .filter(|message| message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id))
        .collect::<Vec<_>>();
    assert_eq!(pairs.len(), 1, "one paired result for {id}");
    pairs[0]
}

#[tokio::test]
async fn self_control_pre_start_running_observation_survives_tool_and_batch_timeout() {
    for case in [
        TimeoutCase {
            batch_timeout: false,
            same_round_start: true,
        },
        TimeoutCase {
            batch_timeout: true,
            same_round_start: false,
        },
    ] {
        let mut fixture = Fixture::new(2, 8).await;
        let config = fixture.config.as_mut().unwrap();
        config.per_tool_timeout_secs = if case.batch_timeout { 4 } else { 1 };
        config.parallel_batch_timeout_secs = if case.batch_timeout { 1 } else { 4 };
        let control = config.loop_control.as_ref().unwrap().clone();
        let persistence = config.persistence.clone().unwrap();
        let probe = Arc::new(TimeoutProbe::default());
        let tools = bamboo_tools::BuiltinToolExecutor::new();
        tools
            .registry()
            .register(PendingAfterCollapsedRunning(probe.clone()))
            .unwrap();
        tools
            .registry()
            .register(ParallelTimeoutControl(probe.clone()))
            .unwrap();
        let provider = Arc::new(TimeoutObservationProvider {
            case,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
        });
        let trace = bamboo_infrastructure::telemetry::test_support::TraceTest::default();
        let (result, events) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            trace.scope(run_timeout_observation_fixture(
                &mut fixture,
                provider.clone(),
                Arc::new(tools),
            )),
        )
        .await
        .expect("existing one-second tool/batch timeout must bound the pending plugin");
        let spans = trace.spans();
        let pending_spans = spans
            .iter()
            .filter(|span| {
                span.attributes.iter().any(|kv| {
                    kv.key.as_str() == "gen_ai.tool.call.id" && kv.value.to_string() == PENDING_CALL
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(pending_spans.len(), 1);
        for (key, value) in [("error.type", "timeout"), ("bamboo.outcome", "error")] {
            assert!(pending_spans[0]
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == key && kv.value.to_string() == value));
        }
        let control_spans = spans
            .iter()
            .filter(|span| {
                span.attributes.iter().any(|kv| {
                    kv.key.as_str() == "gen_ai.tool.call.id" && kv.value.to_string() == CONTROL_CALL
                })
            })
            .collect::<Vec<_>>();
        // A completed dispatch stays successful even when the runtime's existing
        // batch handler later synthesizes timeout replies for the whole batch.
        assert_eq!(control_spans.len(), 1);
        assert!(control_spans[0]
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "bamboo.outcome" && kv.value.to_string() == "success"));
        assert!(!control_spans[0]
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "error.type"));

        result.unwrap();
        // The helper is below public Agent's ordinary terminal save. Persist
        // only the runner's actual returned snapshot, never fabricated state.
        persistence
            .checkpoint_runtime_session(&mut fixture.session)
            .await
            .unwrap();
        assert_eq!(probe.pending_invoked.load(Ordering::SeqCst), 1);
        assert_eq!(
            probe.invoke_dropped.load(Ordering::SeqCst),
            1,
            "the inner observer was cancelled by the real timeout"
        );
        let ack = probe
            .collapsed_ack
            .lock()
            .unwrap()
            .clone()
            .expect("Running was collapsed before the invocation became pending");
        assert!(
            ack.success,
            "observation before Start must not deny ordinary Running"
        );
        assert_eq!(ack.result, INNER_ACK);
        assert_eq!(probe.kills.load(Ordering::SeqCst), 0);
        assert_eq!(*probe.scope_allowed.lock().unwrap(), vec![true]);
        assert_eq!(
            probe.control_invoked.load(Ordering::SeqCst),
            1,
            "this was a true parallel batch, not the single-call direct path"
        );
        let pending = timeout_pair(&fixture.session, PENDING_CALL);
        assert_eq!(pending.tool_success, Some(false));
        assert!(pending.content.contains("timed out"));
        assert_ne!(
            pending.content, INNER_ACK,
            "no synthetic ack for a plugin that never returned"
        );
        let ordinary = timeout_pair(&fixture.session, CONTROL_CALL);
        // The existing batch-timeout handler synthesizes a timeout for every
        // batch call, including a control that already returned. Preserve it.
        assert_eq!(ordinary.tool_success, Some(!case.batch_timeout));
        if case.batch_timeout {
            assert!(ordinary.content.contains("Parallel batch timed out"));
        } else {
            assert_eq!(ordinary.content, CONTROL_RESULT);
        }
        let refused = timeout_pair(&fixture.session, "start");
        // Old 8da and the per-call-only preview both lose this observation and
        // return a true Start receipt. This is the intended actual RED; no
        // provider script assertion or new observation API is needed first.
        assert_eq!(
            refused.tool_success,
            Some(false),
            "{case:?}: timeout must not erase the owner's Running observation"
        );
        let body: Value = serde_json::from_str(&refused.content).unwrap();
        assert_eq!(body["error"], "existing_detached_work_incompatible");
        assert_eq!(body["applied"], false);
        assert!(serde_json::from_str::<LoopControlReceipt>(&refused.content).is_err());
        assert!(control.generation().is_none());
        assert!(control.active_counted().is_none());
        let expected_rounds = if case.same_round_start { 2 } else { 3 };
        assert_eq!(provider.calls.load(Ordering::SeqCst), expected_rounds);
        assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::Complete { .. }))
                .count(),
            1
        );
        assert_eq!(events.iter().filter(|event| matches!(event,
            AgentEvent::ToolComplete { tool_call_id, result }
                if tool_call_id == "start" && !result.success && result.result == refused.content
        )).count(), 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event,
                    AgentEvent::ToolError { tool_call_id, error }
                        if tool_call_id == PENDING_CALL && error.contains("timed out")
                ))
                .count(),
            1
        );
        assert!(!events
            .iter()
            .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
        let durable = fixture
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        for session in [&fixture.session, &durable] {
            assert!(read_loop_state(session).unwrap().is_none());
            assert!(admitted_objectives(&session.messages).is_empty());
            assert!(session.pending_question.is_none());
            assert!(!session.metadata.contains_key("runtime.suspend_reason"));
            let runtime = session.agent_runtime_state.as_ref().unwrap();
            assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Completed);
            assert!(runtime.suspension.is_none());
            assert!(runtime.waiting_for_children.is_none());
            assert!(runtime.waiting_for_bash.is_none());
            let view = crate::runtime::loop_control::inspect_loop(session).unwrap();
            assert!(view.generation.is_none());
            assert_eq!(
                (
                    view.state_revision,
                    view.control_revision,
                    view.iterations_started,
                    view.iterations_completed,
                    view.rounds_started
                ),
                (0, 0, 0, 0, 0)
            );
            assert!(session.messages.iter().any(|message| message.role == Role::Assistant && message.content == format!("Ordinary answer after timed-out Running observation, round {expected_rounds}.")));
        }
        for id in [PENDING_CALL, CONTROL_CALL, "start"] {
            assert_eq!(
                timeout_pair(&durable, id).tool_success,
                timeout_pair(&fixture.session, id).tool_success
            );
            assert_eq!(
                timeout_pair(&durable, id).content,
                timeout_pair(&fixture.session, id).content
            );
        }
        fixture.close().await;
    }
}

mod active_timeout_tests;
