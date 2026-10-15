//! Tests-only fragment for self_control_tests/managed_running_terminal_tests.rs.
//! Parent adds `mod managed_running_terminal_tests;`; no Fixture changes needed.
//! Registers a custom tool in the real BuiltinToolExecutor. Its invoke
//! deliberately ignores suspension_allowed and returns a real RunningHandle.
//! No process, detached task, real provider, fabricated terminal or owner exists.

use super::*;
use bamboo_agent_core::tools::{
    AsyncWaitKind, RunningCompletion, RunningHandle, Tool, ToolCtx, ToolError, ToolExecutor,
    ToolOutcome, ToolResult,
};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

const RUNNING_TOOL: &str = "mcp__contract_fixture__running";
const RUNNING_CALL: &str = "custom-running-handle";
const RUNNING_ACK: &str = "{\"status\":\"running\",\"synthetic_contract\":true}";
const KILL_MARKER: &str = "contract: cooperative cleanup callback returning";
const DROP_MARKER: &str = "contract: unstarted Driven future dropped";

#[derive(Clone, Copy, Debug)]
enum CompletionFlavor {
    // The current public type calls the externally delivered flavor Detached.
    Detached,
    Driven,
}

#[derive(Clone, Copy, Debug)]
enum ResultWrapping {
    Raw,
    CollapseThenForgeSuccess,
}

#[derive(Default)]
struct RunningProbe {
    calls: AtomicUsize,
    kills: AtomicUsize,
    cleanup_returned: AtomicBool,
    driven_drops: AtomicUsize,
    driven_polls: AtomicUsize,
    collapses: AtomicUsize,
    siblings: AtomicUsize,
    cleanup_event_failures: AtomicUsize,
    observed_scope: Mutex<Vec<bool>>,
    publications: Mutex<Vec<CleanupSnapshot>>,
}

#[derive(Clone, Copy, Debug)]
struct CleanupSnapshot {
    kills: usize,
    cleanup_returned: bool,
    driven_drops: usize,
    driven_polls: usize,
}

impl RunningProbe {
    fn snapshot(&self) -> CleanupSnapshot {
        CleanupSnapshot {
            kills: self.kills.load(Ordering::SeqCst),
            cleanup_returned: self.cleanup_returned.load(Ordering::SeqCst),
            driven_drops: self.driven_drops.load(Ordering::SeqCst),
            driven_polls: self.driven_polls.load(Ordering::SeqCst),
        }
    }
}

// The future owns a concrete Drop witness from construction, not a guard made
// only after its first poll. Disallowed work must be dropped without driving it.
struct UnstartedDrivenWork(Arc<RunningProbe>, Option<mpsc::Sender<AgentEvent>>);

impl Future for UnstartedDrivenWork {
    type Output = ToolResult;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_ref()
            .get_ref()
            .0
            .driven_polls
            .fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

impl Drop for UnstartedDrivenWork {
    fn drop(&mut self) {
        self.0.driven_drops.fetch_add(1, Ordering::SeqCst);
        emit_cleanup_marker(&self.0, self.1.as_ref(), DROP_MARKER);
    }
}

fn emit_cleanup_marker(
    probe: &RunningProbe,
    sender: Option<&mpsc::Sender<AgentEvent>>,
    marker: &str,
) {
    // Same real event channel as per_call's publication. FIFO positions prove
    // callback/Drop occurrence precedes the terminal ToolError or false result,
    // even if the receiver only drains the channel after the runner returns.
    let sent = sender.is_some_and(|sender| {
        sender
            .try_send(AgentEvent::ToolToken {
                tool_call_id: RUNNING_CALL.into(),
                content: marker.into(),
            })
            .is_ok()
    });
    if !sent {
        probe.cleanup_event_failures.fetch_add(1, Ordering::SeqCst);
    }
}

struct IgnoresSuspensionRunningTool {
    flavor: CompletionFlavor,
    wrapping: ResultWrapping,
    expect_started: bool,
    probe: Arc<RunningProbe>,
    storage: Arc<dyn Storage>,
}

#[async_trait]
impl Tool for IgnoresSuspensionRunningTool {
    fn name(&self) -> &str {
        RUNNING_TOOL
    }

    fn description(&self) -> &str {
        "Synthetic custom RunningHandle, deliberately ignoring the suspension policy."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }

    // The default mutating/serial/non-promotable classification keeps the
    // same-batch Start and this exact foreign invocation in serial apply order.
    async fn invoke(&self, _: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.probe
            .observed_scope
            .lock()
            .unwrap()
            .push(bamboo_agent_core::tools::suspension_allowed());
        assert_eq!(ctx.session_id(), Some(SESSION_ID));
        assert_eq!(ctx.tool_call_id.as_ref(), RUNNING_CALL);
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
            let state = state.expect("the real Start must be durable before this invocation");
            assert_eq!(state.status, LoopStatus::Running);
            assert_eq!(
                durable
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .execution_id
                    .as_deref(),
                Some(state.owner_execution_id.as_str())
            );
        } else {
            assert!(state.is_none(), "host opt-in is not active management");
        }
        let completion = match self.flavor {
            CompletionFlavor::Detached => RunningCompletion::Detached,
            CompletionFlavor::Driven => RunningCompletion::Driven(Box::pin(UnstartedDrivenWork(
                self.probe.clone(),
                ctx.cloned_sender(),
            ))),
        };
        let cleanup = self.probe.clone();
        let cleanup_sender = ctx.cloned_sender();
        let outcome = ToolOutcome::Running(RunningHandle {
            tool_call_id: ctx.tool_call_id.to_string(),
            ack: ToolResult::text(true, RUNNING_ACK),
            completion,
            wait_kind: AsyncWaitKind::AsyncTools,
            // This is a synchronous cooperative-cleanup witness. It creates
            // no task/process and claims no stronger third-party capability.
            kill: Box::new(move || {
                cleanup.kills.fetch_add(1, Ordering::SeqCst);
                emit_cleanup_marker(&cleanup, cleanup_sender.as_ref(), KILL_MARKER);
                cleanup.cleanup_returned.store(true, Ordering::SeqCst);
            }),
        });
        Ok(match self.wrapping {
            ResultWrapping::Raw => outcome,
            ResultWrapping::CollapseThenForgeSuccess => {
                // Exercise the public collapse seam inside a genuinely
                // registered Builtin custom Tool, not a direct executor call.
                // A wrapper can discard the returned value, so only a sticky
                // host-side violation may stop this fabricated success.
                let _discarded = outcome.into_tool_result();
                self.probe.collapses.fetch_add(1, Ordering::SeqCst);
                ToolOutcome::Completed(ToolResult::text(
                    true,
                    "FORGED_COMPLETED_SUCCESS_AFTER_RUNNING_COLLAPSE",
                ))
            }
        })
    }
}

const SIBLING_TOOL: &str = "mcp__contract_fixture__after_running";
const SIBLING_CALL: &str = "unadmitted-sibling-after-running";

struct UnadmittedSiblingTool(Arc<RunningProbe>);

#[async_trait]
impl Tool for UnadmittedSiblingTool {
    fn name(&self) -> &str {
        SIBLING_TOOL
    }

    fn description(&self) -> &str {
        "Synthetic sibling that must not run after terminal Running rejection."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }

    async fn invoke(&self, _: Value, _: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.0.siblings.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "SIBLING_MUST_NOT_EXECUTE",
        )))
    }
}

#[derive(Clone, Copy, Debug)]
enum RunningSequence {
    SameBatchAsStart,
    AfterStart,
    WithoutStart,
    BeforeStart,
}

struct RunningProviderObservation {
    round: usize,
    cleanup: CleanupSnapshot,
    durable: Session,
}

struct RunningProvider {
    sequence: RunningSequence,
    with_sibling: bool,
    probe: Arc<RunningProbe>,
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    seen: Mutex<Vec<RunningProviderObservation>>,
}

#[async_trait]
impl LLMProvider for RunningProvider {
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
            .any(|schema| schema.function.name == "loop_control"));
        assert!(schemas
            .iter()
            .any(|schema| schema.function.name == RUNNING_TOOL));
        // Capture at provider entry, before any await. Assert after the run so
        // the old source reaches its actual receipt/cleanup RED, not a scripted
        // provider panic. A later cleanup cannot repair this earlier snapshot.
        let cleanup = self.probe.snapshot();
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        self.seen.lock().unwrap().push(RunningProviderObservation {
            round,
            cleanup,
            durable,
        });
        let chunk = match (self.sequence, round) {
            (RunningSequence::SameBatchAsStart, 1) => {
                let LLMChunk::ToolCalls(mut calls) = start_call(2, 8) else {
                    unreachable!("existing Start helper emits ToolCalls")
                };
                calls.push(running_call());
                if self.with_sibling {
                    calls.push(sibling_call());
                }
                LLMChunk::ToolCalls(calls)
            }
            (RunningSequence::AfterStart, 1) | (RunningSequence::BeforeStart, 2) => {
                start_call(2, 8)
            }
            (RunningSequence::AfterStart, 2)
            | (RunningSequence::WithoutStart, 1)
            | (RunningSequence::BeforeStart, 1) => {
                let mut calls = vec![running_call()];
                if self.with_sibling {
                    calls.push(sibling_call());
                }
                LLMChunk::ToolCalls(calls)
            }
            _ => LLMChunk::Token(format!(
                "Actual bounded answer after RunningHandle, round {round}."
            )),
        };
        Ok(Box::pin(
            futures::stream::iter([Ok(chunk), Ok(LLMChunk::Done)]).map(move |item| {
                let _keep_admitted = &guard;
                item
            }),
        ))
    }
}

fn running_call() -> ToolCall {
    ToolCall {
        id: RUNNING_CALL.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: RUNNING_TOOL.into(),
            arguments: "{}".into(),
        },
    }
}

fn sibling_call() -> ToolCall {
    ToolCall {
        id: SIBLING_CALL.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: SIBLING_TOOL.into(),
            arguments: "{}".into(),
        },
    }
}

// Ordinary tool events have no owner-receipt-before-event promise. Only the
// genuine Start event is passed to Fixture's durable receipt witness.
async fn run_with_running_tool(
    fixture: &mut Fixture,
    provider: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    probe: Arc<RunningProbe>,
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
                    record_running_publication(&probe, &event);
                    if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
                        if tool_call_id == "start" && result.success)
                    { Fixture::observe_event(storage.as_ref(), &event).await; }
                    events.push(event);
                }
            }
        }
    };
    while let Ok(event) = rx.try_recv() {
        record_running_publication(&probe, &event);
        if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == "start" && result.success)
        {
            Fixture::observe_event(storage.as_ref(), &event).await;
        }
        events.push(event);
    }
    (result, events)
}

fn record_running_publication(probe: &RunningProbe, event: &AgentEvent) {
    if matches!(event,
        AgentEvent::ToolComplete { tool_call_id, .. } | AgentEvent::ToolError { tool_call_id, .. }
            if tool_call_id == RUNNING_CALL
    ) {
        probe.publications.lock().unwrap().push(probe.snapshot());
    }
}

fn assert_running_cleanup(snapshot: CleanupSnapshot, flavor: CompletionFlavor) {
    assert_eq!(
        snapshot.kills, 1,
        "the returned cleanup callback must run exactly once"
    );
    assert!(
        snapshot.cleanup_returned,
        "no provider admission before cooperative cleanup returns"
    );
    assert_eq!(
        snapshot.driven_polls, 0,
        "disallowed Driven work must never be started"
    );
    assert_eq!(
        snapshot.driven_drops,
        match flavor {
            CompletionFlavor::Detached => 0,
            CompletionFlavor::Driven => 1,
        }
    );
}

fn assert_no_running_wait(session: &Session) {
    assert!(session.pending_question.is_none());
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    if let Some(runtime) = session.agent_runtime_state.as_ref() {
        assert_ne!(runtime.status, bamboo_domain::AgentStatusState::Suspended);
        assert!(runtime.suspension.is_none());
        assert!(runtime.waiting_for_children.is_none());
        assert!(runtime.waiting_for_bash.is_none());
    }
}

fn running_pair(session: &Session) -> &Message {
    let pairs = session
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(RUNNING_CALL)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pairs.len(),
        1,
        "one pair, not a synthetic ack and a second cleanup result"
    );
    pairs[0]
}

async fn assert_managed_running_fails(
    flavor: CompletionFlavor,
    sequence: RunningSequence,
    wrapping: ResultWrapping,
) {
    let mut fixture = Fixture::new(2, 8).await;
    let control = fixture
        .config
        .as_ref()
        .unwrap()
        .loop_control
        .as_ref()
        .unwrap()
        .clone();
    let probe = Arc::new(RunningProbe::default());
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(IgnoresSuspensionRunningTool {
            flavor,
            wrapping,
            expect_started: true,
            probe: probe.clone(),
            storage: fixture.storage.clone(),
        })
        .unwrap();
    tools
        .registry()
        .register(UnadmittedSiblingTool(probe.clone()))
        .unwrap();
    let provider = Arc::new(RunningProvider {
        sequence,
        with_sibling: true,
        probe: probe.clone(),
        storage: fixture.storage.clone(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
        seen: Mutex::new(Vec::new()),
    });
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_with_running_tool(
            &mut fixture,
            provider.clone(),
            Arc::new(tools),
            probe.clone(),
        ),
    )
    .await
    .expect("disallowed Running must fail without driving or joining it");
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    // Keep this before the terminal-result assertion. On 8da the intended
    // genuine RED is kill_count 0, even though the old runner returns Ok and
    // admits later siblings/provider rounds. No provider assertion masks it.
    assert_running_cleanup(probe.snapshot(), flavor);
    assert!(
        result.is_err(),
        "a synchronous cleanup request is not proof of quiescence: {result:?}"
    );
    let error = result.unwrap_err();
    assert!(
        !error.is_cancelled(),
        "the fixture never cancelled the Session"
    );
    assert!(
        !error.is_hook_suspended(),
        "Running rejection is terminal failure, not a resumable wait"
    );
    assert_eq!(*probe.observed_scope.lock().unwrap(), vec![false]);
    assert_eq!(
        probe.collapses.load(Ordering::SeqCst),
        match wrapping {
            ResultWrapping::Raw => 0,
            ResultWrapping::CollapseThenForgeSuccess => 1,
        }
    );
    assert_eq!(
        probe.siblings.load(Ordering::SeqCst),
        0,
        "no later same-batch sibling may execute"
    );
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == SIBLING_CALL
    )));
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == SIBLING_CALL && result.success
    )));
    let (invocation_round, expected_counts, expected_objectives) = match sequence {
        RunningSequence::SameBatchAsStart => (1, (0, 0, 1), vec![]),
        RunningSequence::AfterStart => (2, (1, 0, 2), vec![(FIRST_OBJECTIVE, 1)]),
        RunningSequence::WithoutStart | RunningSequence::BeforeStart => unreachable!(),
    };
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        invocation_round,
        "no next provider admission after unknown detached work"
    );
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    let observations = provider.seen.lock().unwrap();
    assert_eq!(observations.len(), invocation_round);
    for observation in observations.iter() {
        assert!(observation.round <= invocation_round);
        assert_eq!(
            observation.cleanup.kills, 0,
            "all model calls precede the returned RunningHandle"
        );
        assert_no_running_wait(&observation.durable);
    }
    drop(observations);
    let paired = running_pair(&fixture.session);
    assert_eq!(paired.tool_success, Some(false));
    assert_ne!(paired.content, RUNNING_ACK);
    assert!(!paired.content.is_empty());
    let publications = events.iter().filter(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, .. } | AgentEvent::ToolError { tool_call_id, .. }
            if tool_call_id == RUNNING_CALL
    )).collect::<Vec<_>>();
    assert_eq!(
        publications.len(),
        1,
        "one actual failure, no successful Running ack"
    );
    if let AgentEvent::ToolComplete { result, .. } = publications[0] {
        assert!(!result.success);
        assert_eq!(result.result, paired.content);
    }
    let publication_witnesses = probe.publications.lock().unwrap();
    let failure_index = events.iter().position(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, .. } | AgentEvent::ToolError { tool_call_id, .. }
            if tool_call_id == RUNNING_CALL
    )).unwrap();
    assert_eq!(probe.cleanup_event_failures.load(Ordering::SeqCst), 0);
    for (marker, expected) in [
        (KILL_MARKER, 1),
        (
            DROP_MARKER,
            usize::from(matches!(flavor, CompletionFlavor::Driven)),
        ),
    ] {
        let positions = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| match event {
                AgentEvent::ToolToken {
                    tool_call_id,
                    content,
                } if tool_call_id == RUNNING_CALL && content == marker => Some(index),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(positions.len(), expected, "one real callback/Drop marker");
        assert!(
            positions.iter().all(|index| *index < failure_index),
            "cleanup request/Drop must precede false result publication"
        );
    }
    assert_eq!(publication_witnesses.len(), 1);
    for snapshot in publication_witnesses.iter() {
        // Receiver-boundary witness complements the production order. It does
        // not pretend that a callback return is a third-party join receipt.
        assert_running_cleanup(*snapshot, flavor);
    }
    drop(publication_witnesses);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
                AgentEvent::ToolStart { tool_call_id, tool_name, .. }
                    if tool_call_id == RUNNING_CALL && tool_name == RUNNING_TOOL
            ))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        0
    );
    assert_no_running_wait(&fixture.session);
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Failed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        expected_counts
    );
    assert_eq!(control.generation(), Some(state.generation.as_str()));
    assert!(
        control.active_counted().is_some(),
        "do not clear the consumed generation to fake a retry"
    );
    let management = state.control.as_ref().unwrap();
    assert_eq!(management.operations.len(), 1);
    assert!(!management.stop_requested);
    assert!(management.pending_objective.is_none());
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        expected_objectives
    );
    // Keep the unfinished counted input fence when one was already admitted.
    // It is different from pending_question, which must remain absent.
    if matches!(sequence, RunningSequence::AfterStart) {
        assert!(crate::runtime::counted_loop::reject_recovery(&fixture.session).is_err());
    }
    let runtime = fixture.session.agent_runtime_state.as_ref().unwrap();
    assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Failed);
    assert_eq!(
        runtime.execution_id.as_deref(),
        Some(state.owner_execution_id.as_str())
    );
    let start_pair = fixture
        .session
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
        })
        .collect::<Vec<_>>();
    assert_eq!(start_pair.len(), 1);
    assert_eq!(start_pair[0].tool_success, Some(true));
    let receipt: LoopControlReceipt = serde_json::from_str(&start_pair[0].content).unwrap();
    assert_eq!(receipt, management.operations[0].receipt);
    assert_eq!(
        start_pair[0].content,
        serde_json::to_string(&management.operations[0].receipt).unwrap(),
        "Start receipt bytes are immutable despite terminal failure"
    );
    assert_eq!(state.generation, receipt.generation);
    assert!(
        !fixture
            .session
            .messages
            .iter()
            .any(|message| message.role == Role::Assistant
                && message
                    .content
                    .starts_with("Actual bounded answer after RunningHandle")),
        "the fallback provider script must never be admitted"
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap().unwrap(), state);
    assert_no_running_wait(&durable);
    assert_eq!(
        durable.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Failed
    );
    assert_eq!(running_pair(&durable).tool_success, Some(false));
    assert_eq!(running_pair(&durable).content, paired.content);
    let durable_start = durable
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
        })
        .collect::<Vec<_>>();
    assert_eq!(durable_start.len(), 1);
    assert_eq!(durable_start[0].content, start_pair[0].content);
    assert_eq!(durable_start[0].tool_success, Some(true));
    if matches!(sequence, RunningSequence::AfterStart) {
        assert!(crate::runtime::counted_loop::reject_recovery(&durable).is_err());
    }
    fixture.close().await;
}

#[tokio::test]
async fn self_control_returned_detached_or_driven_handle_fails_before_later_rounds() {
    for flavor in [CompletionFlavor::Detached, CompletionFlavor::Driven] {
        assert_managed_running_fails(flavor, RunningSequence::AfterStart, ResultWrapping::Raw)
            .await;
    }
}

#[tokio::test]
async fn self_control_start_same_batch_running_fails_without_admitting_iteration_or_sibling() {
    for flavor in [CompletionFlavor::Detached, CompletionFlavor::Driven] {
        assert_managed_running_fails(
            flavor,
            RunningSequence::SameBatchAsStart,
            ResultWrapping::Raw,
        )
        .await;
    }
}

#[tokio::test]
async fn self_control_running_collapse_then_forged_completed_cannot_hide_host_violation() {
    assert_managed_running_fails(
        CompletionFlavor::Driven,
        RunningSequence::AfterStart,
        ResultWrapping::CollapseThenForgeSuccess,
    )
    .await;
}

#[tokio::test]
async fn self_control_opt_in_without_start_preserves_ordinary_detached_ack() {
    let mut fixture = Fixture::new(2, 8).await;
    let config = fixture.config.as_ref().unwrap();
    let control = config.loop_control.as_ref().unwrap().clone();
    let persistence = config.persistence.clone().unwrap();
    let probe = Arc::new(RunningProbe::default());
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(IgnoresSuspensionRunningTool {
            flavor: CompletionFlavor::Detached,
            wrapping: ResultWrapping::Raw,
            expect_started: false,
            probe: probe.clone(),
            storage: fixture.storage.clone(),
        })
        .unwrap();
    let provider = Arc::new(RunningProvider {
        sequence: RunningSequence::WithoutStart,
        with_sibling: false,
        probe: probe.clone(),
        storage: fixture.storage.clone(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
        seen: Mutex::new(Vec::new()),
    });
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_with_running_tool(
            &mut fixture,
            provider.clone(),
            Arc::new(tools),
            probe.clone(),
        ),
    )
    .await
    .expect("ordinary synthetic Detached ack must remain finite");
    result.unwrap();
    // This helper is below the existing public Agent's final checkpoint. Save
    // its actual returned snapshot; do not invent or clear any runtime state.
    persistence
        .checkpoint_runtime_session(&mut fixture.session)
        .await
        .unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.kills.load(Ordering::SeqCst),
        0,
        "host opt-in alone is not cleanup authority"
    );
    assert!(!probe.cleanup_returned.load(Ordering::SeqCst));
    assert_eq!(probe.driven_drops.load(Ordering::SeqCst), 0);
    assert_eq!(*probe.observed_scope.lock().unwrap(), vec![true]);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert!(control.generation().is_none());
    assert!(control.active_counted().is_none());
    assert!(read_loop_state(&fixture.session).unwrap().is_none());
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    let paired = running_pair(&fixture.session);
    assert_eq!(paired.tool_success, Some(true));
    assert_eq!(paired.content, RUNNING_ACK);
    assert_eq!(events.iter().filter(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == RUNNING_CALL && result.success && result.result == RUNNING_ACK
    )).count(), 1);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        1
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(read_loop_state(&durable).unwrap().is_none());
    assert_eq!(running_pair(&durable).tool_success, Some(true));
    assert_eq!(running_pair(&durable).content, RUNNING_ACK);
    assert_no_running_wait(&durable);
    fixture.close().await;
}

#[tokio::test]
async fn self_control_observed_pre_start_running_refuses_start_without_changing_ordinary_result() {
    for wrapping in [
        ResultWrapping::Raw,
        ResultWrapping::CollapseThenForgeSuccess,
    ] {
        let mut fixture = Fixture::new(2, 8).await;
        let config = fixture.config.as_ref().unwrap();
        let control = config.loop_control.as_ref().unwrap().clone();
        let persistence = config.persistence.clone().unwrap();
        let probe = Arc::new(RunningProbe::default());
        let tools = bamboo_tools::BuiltinToolExecutor::new();
        tools
            .registry()
            .register(IgnoresSuspensionRunningTool {
                flavor: CompletionFlavor::Detached,
                wrapping,
                expect_started: false,
                probe: probe.clone(),
                storage: fixture.storage.clone(),
            })
            .unwrap();
        let provider = Arc::new(RunningProvider {
            sequence: RunningSequence::BeforeStart,
            with_sibling: false,
            probe: probe.clone(),
            storage: fixture.storage.clone(),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        });
        let (result, events) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_with_running_tool(
                &mut fixture,
                provider.clone(),
                Arc::new(tools),
                probe.clone(),
            ),
        )
        .await
        .expect("ordinary execution must remain usable after refusing managed Start");
        result.unwrap();
        // Same existing host-final-save boundary as the ordinary control.
        // No test code assigns or removes runtime, loop or pending state.
        persistence
            .checkpoint_runtime_session(&mut fixture.session)
            .await
            .unwrap();

        let start_pairs = fixture
            .session
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
            })
            .collect::<Vec<_>>();
        assert_eq!(start_pairs.len(), 1);
        // On 8da the prior Running is not remembered by the owner and Start
        // succeeds. The intended RED is this truthful-success mismatch, not a
        // provider panic: its later Token fallback works on both sources.
        assert_eq!(start_pairs[0].tool_success, Some(false), "{wrapping:?}: prior generic Running must conservatively refuse this activation's Start");
        let refusal: Value = serde_json::from_str(&start_pairs[0].content).unwrap();
        assert!(refusal["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()));
        assert_eq!(refusal["applied"], false);
        assert!(
            serde_json::from_str::<LoopControlReceipt>(&start_pairs[0].content).is_err(),
            "a refusal must not masquerade as an accepted operation receipt"
        );
        let start_events = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ToolComplete {
                    tool_call_id,
                    result,
                } if tool_call_id == "start" => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(start_events.len(), 1);
        assert!(!start_events[0].success);
        assert_eq!(start_events[0].result, start_pairs[0].content);

        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            probe.kills.load(Ordering::SeqCst),
            0,
            "observation before Start is not cleanup authority or a join receipt"
        );
        assert!(!probe.cleanup_returned.load(Ordering::SeqCst));
        assert_eq!(probe.driven_polls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.driven_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            probe.collapses.load(Ordering::SeqCst),
            match wrapping {
                ResultWrapping::Raw => 0,
                ResultWrapping::CollapseThenForgeSuccess => 1,
            }
        );
        assert_eq!(
            *probe.observed_scope.lock().unwrap(),
            vec![true],
            "host observation must preserve ordinary suspension permission"
        );
        let expected_result = match wrapping {
            ResultWrapping::Raw => RUNNING_ACK,
            ResultWrapping::CollapseThenForgeSuccess => {
                "FORGED_COMPLETED_SUCCESS_AFTER_RUNNING_COLLAPSE"
            }
        };
        let ordinary_pair = running_pair(&fixture.session);
        assert_eq!(ordinary_pair.tool_success, Some(true));
        assert_eq!(
            ordinary_pair.content, expected_result,
            "ordinary outcomes must not be rewritten merely by observing them"
        );
        assert_eq!(events.iter().filter(|event| matches!(event,
            AgentEvent::ToolComplete { tool_call_id, result }
                if tool_call_id == RUNNING_CALL && result.success && result.result == expected_result
        )).count(), 1);
        assert!(!events.iter().any(|event| matches!(event,
            AgentEvent::ToolToken { tool_call_id, content }
                if tool_call_id == RUNNING_CALL && (content == KILL_MARKER || content == DROP_MARKER)
        )));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
        assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::Complete { .. }))
                .count(),
            1
        );
        assert!(fixture
            .session
            .messages
            .iter()
            .any(|message| message.role == Role::Assistant
                && message.content == "Actual bounded answer after RunningHandle, round 3."));
        assert!(control.generation().is_none());
        assert!(control.active_counted().is_none());
        for snapshot in provider.seen.lock().unwrap().iter() {
            assert!(read_loop_state(&snapshot.durable).unwrap().is_none());
            assert_eq!(snapshot.cleanup.kills, 0);
            assert_no_running_wait(&snapshot.durable);
        }
        let durable = fixture
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        for session in [&fixture.session, &durable] {
            assert!(read_loop_state(session).unwrap().is_none());
            assert!(admitted_objectives(&session.messages).is_empty());
            assert_no_running_wait(session);
            let view = crate::runtime::loop_control::inspect_loop(session).unwrap();
            assert!(view.generation.is_none());
            assert!(view.status.is_none());
            assert_eq!(
                (
                    view.state_revision,
                    view.control_revision,
                    view.applied_revision
                ),
                (0, 0, 0)
            );
            assert_eq!(
                (
                    view.iterations_started,
                    view.iterations_completed,
                    view.rounds_started
                ),
                (0, 0, 0),
                "no counted budget may be installed by rejected Start"
            );
            assert_eq!(
                session.agent_runtime_state.as_ref().unwrap().status,
                bamboo_domain::AgentStatusState::Completed
            );
        }
        assert_eq!(running_pair(&durable).tool_success, Some(true));
        assert_eq!(running_pair(&durable).content, expected_result);
        let durable_start = durable
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
            })
            .collect::<Vec<_>>();
        assert_eq!(durable_start.len(), 1);
        assert_eq!(durable_start[0].tool_success, Some(false));
        assert_eq!(durable_start[0].content, start_pairs[0].content);
        fixture.close().await;
    }
}
