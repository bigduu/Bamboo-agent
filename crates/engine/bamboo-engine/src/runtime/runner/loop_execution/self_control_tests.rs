//! Synthetic-provider integration tests through the real serial tool apply path.
//! No real model, secondary executor, or manufactured ownership is used.

use super::run_agent_loop_with_config;
use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};
use crate::runtime::counted_loop::{read_loop_state, LoopState, LoopStatus};
use crate::runtime::loop_control::{LoopControlConfig, LoopControlReceipt};
use crate::session_activation::{SessionActivationRouter, SessionRunRegistration};
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Message, Role, Session};
use bamboo_domain::RuntimeSessionPersistence;
use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

const SESSION_ID: &str = "self-control-runner-fixture";
const INITIAL_PROMPT: &str = "Choose and manage a bounded loop in this session.";
const ORIGINAL_CONTEXT: &str = "Keep the original context and the verified earlier fact.";
const ORIGINAL_FACT: &str = "Earlier verified fact: the fixture has one owner.";
const FIRST_OBJECTIVE: &str = "Verify the first bounded objective.";
const SECOND_OBJECTIVE: &str = "Verify the new objective independently.";
const START_OPERATION: &str = "11111111-1111-4111-8111-111111111111";
const UPDATE_OPERATION: &str = "22222222-2222-4222-8222-222222222222";
const STOP_OPERATION: &str = "33333333-3333-4333-8333-333333333333";

#[derive(Clone, Copy)]
enum Script {
    TwoIterations,
    UpdateObjective,
    Stop,
    StatusUntilHostCap,
    CancelledUpdate,
    RetiredGeneration,
    DisabledStart,
    PublicDirect,
}

struct Observation {
    messages: Vec<Message>,
    durable_loop: Option<LoopState>,
}

struct ActiveStream(Arc<AtomicUsize>);

impl Drop for ActiveStream {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ScriptedProvider {
    script: Script,
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    seen: Mutex<Vec<Observation>>,
    barrier: Option<(usize, Arc<Notify>, Arc<Notify>)>,
    expected_tool_visibility: bool,
}

impl ScriptedProvider {
    fn new(script: Script, storage: Arc<dyn Storage>) -> Self {
        Self {
            script,
            storage,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
            barrier: None,
            expected_tool_visibility: true,
        }
    }

    fn response(&self, round: usize, messages: &[Message]) -> LLMChunk {
        match (self.script, round) {
            (Script::TwoIterations, 1) => start_call(2, 6),
            (Script::TwoIterations, 2) => LLMChunk::Token("First objective answer.".into()),
            (Script::TwoIterations, 3) => LLMChunk::Token("Second iteration answer.".into()),
            (Script::PublicDirect, 1) => start_call(2, 4),
            (Script::PublicDirect, 2) => LLMChunk::Token("Public first iteration answer.".into()),
            (Script::PublicDirect, 3) => LLMChunk::Token("Public second iteration answer.".into()),
            (Script::UpdateObjective, 1) => start_call(2, 8),
            (Script::UpdateObjective, 2) => control_call("inspect", json!({"action":"status"})),
            (Script::UpdateObjective, 3) => {
                let status = paired_result(messages, "inspect");
                control_call(
                    "update",
                    json!({
                        "action":"update",
                        "operation_id": UPDATE_OPERATION,
                        "generation": status["loop"]["generation"],
                        "expected_revision": status["loop"]["control_revision"],
                        "objective": SECOND_OBJECTIVE,
                    }),
                )
            }
            (Script::UpdateObjective, 4) => LLMChunk::Token("Old objective finished.".into()),
            (Script::UpdateObjective, 5) => LLMChunk::Token("New objective finished.".into()),
            (Script::Stop, 1) => start_call(3, 8),
            (Script::Stop, 2) => {
                let start = paired_result(messages, "start");
                control_call(
                    "stop",
                    json!({
                        "action":"stop",
                        "operation_id": STOP_OPERATION,
                        "generation": start["generation"],
                        "expected_revision": start["accepted_revision"],
                    }),
                )
            }
            (Script::StatusUntilHostCap, _) => {
                control_call(&format!("inspect-{round}"), json!({"action":"status"}))
            }
            (Script::CancelledUpdate, 1) => start_call(2, 6),
            (Script::CancelledUpdate, 2) => {
                let start = paired_result(messages, "start");
                control_call(
                    "cancelled-update",
                    json!({
                        "action":"update",
                        "operation_id": UPDATE_OPERATION,
                        "generation": start["generation"],
                        "expected_revision": start["accepted_revision"],
                        "objective": SECOND_OBJECTIVE,
                    }),
                )
            }
            (Script::RetiredGeneration, 1) => LLMChunk::Token(
                "Ordinary external prompt; do not resume a retired generation.".into(),
            ),
            (Script::DisabledStart, 1) => start_call(2, 6),
            (Script::DisabledStart, 2) => {
                LLMChunk::Token("The disabled command was refused.".into())
            }
            _ => panic!("unexpected provider call {round}; no extra summary is permitted"),
        }
    }
}

#[async_trait]
impl LLMProvider for ScriptedProvider {
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
        assert_eq!(active, 1, "one owner must never overlap provider streams");
        assert_eq!(
            tools
                .iter()
                .any(|schema| schema.function.name == "loop_control"),
            self.expected_tool_visibility,
        );
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let durable_loop = read_loop_state(&durable).unwrap();
        if matches!(self.script, Script::RetiredGeneration) {
            assert_eq!(durable_loop.as_ref().unwrap().status, LoopStatus::Failed);
        } else if round == 1 {
            assert!(
                durable_loop.is_none(),
                "host opt-in must not start a generation"
            );
        } else if let Some(state) = durable_loop.as_ref() {
            assert_eq!(
                state.rounds_started, round as u32,
                "charge admission before dispatch"
            );
            assert_eq!(
                durable
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .execution_id
                    .as_deref(),
                Some(state.owner_execution_id.as_str()),
                "runner must bind the real execution before the durable admission"
            );
        }
        self.seen.lock().unwrap().push(Observation {
            messages: messages.to_vec(),
            durable_loop,
        });
        if let Some((barrier_round, entered, release)) = self.barrier.as_ref() {
            if round == *barrier_round {
                entered.notify_one();
                release.notified().await;
            }
        }
        let output = self.response(round, messages);
        let stream = futures::stream::iter([Ok(output), Ok(LLMChunk::Done)]).map(move |chunk| {
            let _keep_stream_admitted = &guard;
            chunk
        });
        Ok(Box::pin(stream))
    }
}

fn control_call(id: &str, args: Value) -> LLMChunk {
    LLMChunk::ToolCalls(vec![ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "loop_control".into(),
            arguments: args.to_string(),
        },
    }])
}

fn start_call(iterations: u32, rounds: u32) -> LLMChunk {
    control_call(
        "start",
        json!({
            "action":"start",
            "operation_id": START_OPERATION,
            "expected_revision": 0,
            "objective": FIRST_OBJECTIVE,
            "max_iterations": iterations,
            "max_total_rounds": rounds,
        }),
    )
}

fn paired_result(messages: &[Message], call_id: &str) -> Value {
    let results = messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(call_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1, "exactly one paired result for {call_id}");
    assert_eq!(results[0].tool_success, Some(true), "{call_id}");
    serde_json::from_str(&results[0].content).unwrap()
}

fn managed_config(
    session: &Session,
    registration: &SessionRunRegistration,
    storage: Arc<dyn Storage>,
    iterations: u32,
    rounds: u32,
) -> AgentLoopConfig {
    let mut control = LoopControlConfig::new(iterations, rounds);
    control.bind_owner(registration.ownership(), session);
    AgentLoopConfig {
        model_name: Some("synthetic-only".into()),
        loop_control: Some(control),
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(rounds),
            ..Default::default()
        },
        permission_mode: Some(bamboo_config::PermissionMode::Default),
        storage: Some(storage.clone()),
        persistence: Some(Arc::new(bamboo_storage::LockedSessionStore::new(
            storage.clone(),
        ))),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..Default::default()
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    session: Session,
    config: Option<AgentLoopConfig>,
    storage: Arc<dyn Storage>,
    registration: Option<SessionRunRegistration>,
}

impl Fixture {
    async fn new(iterations: u32, rounds: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut session = Session::new(SESSION_ID, "synthetic-only");
        session.add_message(Message::system(ORIGINAL_CONTEXT));
        session.add_message(Message::assistant(ORIGINAL_FACT, None));
        let storage: Arc<dyn Storage> = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
                .await
                .unwrap(),
        );
        storage.save_session(&session).await.unwrap();
        let router = SessionActivationRouter::new();
        let registration = router
            .register_run(&session.id, "real-self-control-owner")
            .await
            .unwrap();
        let config = managed_config(&session, &registration, storage.clone(), iterations, rounds);
        assert!(config.effective_counted_loop().is_none());
        Self {
            _dir: dir,
            session,
            config: Some(config),
            storage,
            registration: Some(registration),
        }
    }

    async fn run(
        &mut self,
        provider: Arc<dyn LLMProvider>,
        cancel: CancellationToken,
    ) -> (Result<(), AgentError>, Vec<AgentEvent>) {
        let (tx, mut rx) = mpsc::channel(512);
        let tools = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        let storage = self.storage.clone();
        let config = self.config.take().expect("one activation per fixture");
        let run = run_agent_loop_with_config(
            &mut self.session,
            INITIAL_PROMPT.into(),
            tx,
            provider,
            tools,
            cancel,
            config,
        );
        tokio::pin!(run);
        let mut events = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut run => break result,
                event = rx.recv() => {
                    if let Some(event) = event {
                        Self::observe_event(storage.as_ref(), &event).await;
                        events.push(event);
                    }
                }
            }
        };
        while let Ok(event) = rx.try_recv() {
            Self::observe_event(storage.as_ref(), &event).await;
            events.push(event);
        }
        (result, events)
    }

    async fn observe_event(storage: &dyn Storage, event: &AgentEvent) {
        if let AgentEvent::ToolComplete {
            tool_call_id,
            result,
        } = event
        {
            if !result.success {
                return;
            }
            let stored = storage.load_session(SESSION_ID).await.unwrap().unwrap();
            let paired = stored
                .messages
                .iter()
                .filter(|message| {
                    message.role == Role::Tool
                        && message.tool_call_id.as_deref() == Some(tool_call_id.as_str())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                paired.len(),
                1,
                "ToolComplete requires durable pairing: {tool_call_id}"
            );
            assert_eq!(paired[0].content, result.result);
            assert_eq!(paired[0].tool_success, Some(result.success));
            let receipt: Value = serde_json::from_str(&result.result).unwrap();
            assert!(
                receipt.get("requires_owner_apply").is_none(),
                "validation-only placeholder escaped"
            );
        }
    }

    async fn close(mut self) {
        self.registration.take().unwrap().abandon().await;
    }
}

fn assert_terminal_integrity(
    fixture: &Fixture,
    provider: &ScriptedProvider,
    events: &[AgentEvent],
    calls: usize,
    completes: usize,
) {
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        completes
    );
    for call in fixture
        .session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
    {
        // A cancelled stream may have announced a call without admitting it.
        // Every published receipt must pair exactly once; normal terminal runs
        // must pair every model call as well.
        if completes == 0
            && !events.iter().any(|event| {
                matches!(event, AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == &call.id)
            })
        {
            continue;
        }
        let results = fixture
            .session
            .messages
            .iter()
            .filter(|message| {
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(call.id.as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1, "one result for {}", call.id);
        assert_eq!(events.iter().filter(|event| {
            matches!(event, AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == &call.id)
        }).count(), 1, "one ToolComplete for {}", call.id);
    }
    assert!(fixture
        .session
        .messages
        .iter()
        .any(|message| message.content == ORIGINAL_CONTEXT));
    assert!(fixture
        .session
        .messages
        .iter()
        .any(|message| message.content == ORIGINAL_FACT));
    for observation in provider.seen.lock().unwrap().iter() {
        assert!(observation
            .messages
            .iter()
            .any(|message| message.content.contains(ORIGINAL_CONTEXT)));
        assert!(observation
            .messages
            .iter()
            .any(|message| message.content == ORIGINAL_FACT));
        assert!(!observation.messages.iter().any(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("runtime_kind"))
                .and_then(Value::as_str)
                == Some("max_rounds_summary")
        }));
    }
}

fn admitted_objectives(messages: &[Message]) -> Vec<(&str, u64)> {
    messages
        .iter()
        .filter_map(|message| {
            let metadata = message.metadata.as_ref()?;
            (message.role == Role::User
                && metadata.get("runtime_kind").and_then(Value::as_str)
                    == Some("counted_loop_iteration"))
            .then(|| {
                (
                    message.content.as_str(),
                    metadata["objective_revision"].as_u64().unwrap(),
                )
            })
        })
        .collect()
}

#[tokio::test]
async fn self_control_real_runner_start_admits_two_iterations_after_request_round() {
    let mut fixture = Fixture::new(2, 6).await;
    let provider = Arc::new(ScriptedProvider::new(
        Script::TwoIterations,
        fixture.storage.clone(),
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_terminal_integrity(&fixture, &provider, &events, 3, 1);
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Completed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (2, 2, 3)
    );
    assert_eq!(
        state.owner_execution_id,
        fixture
            .session
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .execution_id
            .as_deref()
            .unwrap()
    );
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1), (FIRST_OBJECTIVE, 1)]
    );
    let start: LoopControlReceipt =
        serde_json::from_value(paired_result(&fixture.session.messages, "start")).unwrap();
    assert_eq!(
        (
            start.accepted_revision,
            start.applied_revision_at_acceptance
        ),
        (1, 0)
    );
    assert_eq!(start.effective_iteration, Some(1));
    assert!(!start.applied);
    {
        let seen = provider.seen.lock().unwrap();
        assert!(admitted_objectives(&seen[0].messages).is_empty());
        assert_eq!(
            admitted_objectives(&seen[1].messages),
            vec![(FIRST_OBJECTIVE, 1)]
        );
        assert_eq!(
            seen[1].durable_loop.as_ref().unwrap().iterations_completed,
            0,
            "the request round must not complete the model's objective"
        );
    }
    let stored = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&stored).unwrap().unwrap(), state);
    fixture.close().await;
}

#[tokio::test]
async fn self_control_real_runner_status_cas_update_applies_only_at_next_iteration() {
    let mut fixture = Fixture::new(2, 8).await;
    let provider = Arc::new(ScriptedProvider::new(
        Script::UpdateObjective,
        fixture.storage.clone(),
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_terminal_integrity(&fixture, &provider, &events, 5, 1);
    let status = paired_result(&fixture.session.messages, "inspect");
    assert_eq!(status["loop"]["control_revision"], 1);
    let update: LoopControlReceipt =
        serde_json::from_value(paired_result(&fixture.session.messages, "update")).unwrap();
    assert_eq!(
        (
            update.accepted_revision,
            update.applied_revision_at_acceptance
        ),
        (2, 1)
    );
    assert_eq!(
        (
            update.objective_revision,
            update.effective_iteration,
            update.applied
        ),
        (2, Some(2), false)
    );
    {
        let seen = provider.seen.lock().unwrap();
        assert!(
            seen[2].durable_loop.as_ref().unwrap().revision
                > status["loop"]["state_revision"].as_u64().unwrap(),
            "ordinary checkpoint changes must not invalidate control-revision CAS"
        );
        for observation in &seen[1..4] {
            assert_eq!(
                admitted_objectives(&observation.messages),
                vec![(FIRST_OBJECTIVE, 1)]
            );
            assert_eq!(
                observation
                    .durable_loop
                    .as_ref()
                    .unwrap()
                    .objective_revision,
                1
            );
        }
        let pending = seen[3].durable_loop.as_ref().unwrap();
        assert_eq!(
            pending
                .control
                .as_ref()
                .unwrap()
                .pending_objective
                .as_ref()
                .unwrap()
                .objective,
            SECOND_OBJECTIVE
        );
        assert_eq!(
            pending
                .control
                .as_ref()
                .unwrap()
                .iteration_objective_revision,
            1
        );
        assert_eq!(
            admitted_objectives(&seen[4].messages),
            vec![(FIRST_OBJECTIVE, 1), (SECOND_OBJECTIVE, 2)]
        );
        let next = seen[4].durable_loop.as_ref().unwrap();
        assert_eq!(
            (next.iterations_started, next.iterations_completed),
            (2, 1),
            "old completion cannot count as completion of the new objective"
        );
        assert_eq!(next.control.as_ref().unwrap().applied_revision, 2);
        assert!(next.control.as_ref().unwrap().pending_objective.is_none());
    }
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Completed);
    assert_eq!(
        (state.objective.as_str(), state.objective_revision),
        (SECOND_OBJECTIVE, 2)
    );
    assert_eq!((state.iterations_completed, state.rounds_started), (2, 5));
    fixture.close().await;
}

#[tokio::test]
async fn self_control_real_runner_stop_keeps_context_and_admits_no_more_provider() {
    let mut fixture = Fixture::new(3, 8).await;
    let provider = Arc::new(ScriptedProvider::new(Script::Stop, fixture.storage.clone()));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_terminal_integrity(&fixture, &provider, &events, 2, 1);
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Stopped);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (1, 0, 2)
    );
    assert!(state.control.as_ref().unwrap().stop_requested);
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1)]
    );
    let stop: LoopControlReceipt =
        serde_json::from_value(paired_result(&fixture.session.messages, "stop")).unwrap();
    assert!(stop.applied);
    assert_eq!(stop.accepted_revision, 2);
    fixture.close().await;
}

#[tokio::test]
async fn self_control_real_runner_absolute_host_cap_applies_before_any_start() {
    let mut fixture = Fixture::new(2, 2).await;
    let provider = Arc::new(ScriptedProvider::new(
        Script::StatusUntilHostCap,
        fixture.storage.clone(),
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_terminal_integrity(&fixture, &provider, &events, 2, 1);
    assert!(read_loop_state(&fixture.session).unwrap().is_none());
    assert_eq!(
        fixture
            .session
            .metadata
            .get("runtime.completion_reason")
            .map(String::as_str),
        Some("budget_exceeded")
    );
    let first = paired_result(&fixture.session.messages, "inspect-1");
    let second = paired_result(&fixture.session.messages, "inspect-2");
    assert_eq!(first["activation_rounds_spent"], 1);
    assert_eq!(second["activation_rounds_spent"], 2);
    assert_eq!(second["host_remaining_rounds"], 0);
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn self_control_real_runner_user_cancel_wins_start_and_update_admission() {
    for cancelled_round in [1, 2] {
        let mut fixture = Fixture::new(2, 6).await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut provider = ScriptedProvider::new(Script::CancelledUpdate, fixture.storage.clone());
        provider.barrier = Some((cancelled_round, entered.clone(), release.clone()));
        let provider = Arc::new(provider);
        let cancel = CancellationToken::new();
        let cancelling = async {
            entered.notified().await;
            cancel.cancel();
            release.notify_one();
        };
        let ((result, events), ()) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(fixture.run(provider.clone(), cancel.clone()), cancelling)
            })
            .await
            .expect("synthetic cancellation barrier must complete");
        assert!(matches!(result, Err(AgentError::Cancelled)));
        assert_terminal_integrity(&fixture, &provider, &events, cancelled_round, 0);
        assert!(!events.iter().any(|event| {
            matches!(event, AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "cancelled-update")
        }));
        let state = read_loop_state(&fixture.session).unwrap();
        if cancelled_round == 1 {
            assert!(
                state.is_none(),
                "cancellation must beat the first generation grant"
            );
        } else {
            let state = state.unwrap();
            assert_eq!(state.status, LoopStatus::Cancelled);
            assert_eq!(state.control.as_ref().unwrap().control_revision, 1);
            assert!(state.control.as_ref().unwrap().pending_objective.is_none());
            assert_eq!(state.objective, FIRST_OBJECTIVE);
        }
        fixture.close().await;
    }
}

struct FailStartCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    commit_before_error: bool,
    fail_terminal_checkpoint: bool,
    cancel_at_failure: Option<CancellationToken>,
    start_failed: AtomicBool,
    terminal_failed: AtomicBool,
}

#[async_trait]
impl RuntimeSessionPersistence for FailStartCheckpoint {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn load_runtime_admission_snapshot(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_admission_snapshot(id).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        let state = read_loop_state(session).unwrap();
        if state.as_ref().is_some_and(|state| {
            state.status == LoopStatus::Running
                && state.iterations_started == 0
                && state.iterations_completed == 0
                && state.control.as_ref().is_some_and(|control| {
                    control.operations.len() == 1
                        && control.operations[0].receipt.operation_id == START_OPERATION
                })
        }) && session.messages.iter().any(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
        }) && !self.start_failed.swap(true, Ordering::SeqCst)
        {
            if self.commit_before_error {
                self.inner.checkpoint_runtime_session(session).await?;
            }
            if let Some(cancel) = self.cancel_at_failure.as_ref() {
                cancel.cancel();
            }
            return Err(std::io::Error::other("synthetic start checkpoint failure"));
        }
        if self.fail_terminal_checkpoint
            && self.start_failed.load(Ordering::SeqCst)
            && state
                .as_ref()
                .is_some_and(|state| state.status == LoopStatus::Failed)
            && !self.terminal_failed.swap(true, Ordering::SeqCst)
        {
            return Err(std::io::Error::other(
                "synthetic terminal checkpoint failure",
            ));
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

fn assert_failed_start_snapshot(session: &Session) -> LoopState {
    let state = read_loop_state(session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Failed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (0, 0, 1)
    );
    let runtime = session.agent_runtime_state.as_ref().unwrap();
    assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Failed);
    assert_eq!(
        runtime.execution_id.as_deref(),
        Some(state.owner_execution_id.as_str())
    );
    assert!(admitted_objectives(&session.messages).is_empty());
    let receipt: LoopControlReceipt =
        serde_json::from_value(paired_result(&session.messages, "start")).unwrap();
    assert_eq!(receipt.generation, state.generation);
    assert!(!receipt.applied, "no objective admission was granted");
    assert_eq!(state.control.as_ref().unwrap().operations.len(), 1);
    state
}

#[tokio::test]
async fn self_control_real_runner_start_checkpoint_fault_matrix_is_conservative() {
    for commit_before_error in [false, true] {
        for fail_terminal_checkpoint in [false, true] {
            let mut fixture = Fixture::new(2, 6).await;
            let fault = Arc::new(FailStartCheckpoint {
                inner: fixture.config.as_mut().unwrap().persistence.take().unwrap(),
                commit_before_error,
                fail_terminal_checkpoint,
                cancel_at_failure: None,
                start_failed: AtomicBool::new(false),
                terminal_failed: AtomicBool::new(false),
            });
            fixture.config.as_mut().unwrap().persistence = Some(fault.clone());
            let provider = Arc::new(ScriptedProvider::new(
                Script::TwoIterations,
                fixture.storage.clone(),
            ));
            let (result, events) = fixture
                .run(provider.clone(), CancellationToken::new())
                .await;
            assert!(fault.start_failed.load(Ordering::SeqCst));
            assert_eq!(
                fault.terminal_failed.load(Ordering::SeqCst),
                fail_terminal_checkpoint
            );
            assert!(
                matches!(result, Err(AgentError::Budget(ref reason))
                if reason == "counted loop: Session checkpoint failed; execution not admitted"),
                "the original admission error must survive a later failed final save: {result:?}"
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            assert_eq!(provider.active.load(Ordering::SeqCst), 0);
            assert!(!events.iter().any(|event| matches!(event,
                AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "start")));
            assert!(!events
                .iter()
                .any(|event| matches!(event, AgentEvent::Complete { .. })));
            let failed = assert_failed_start_snapshot(&fixture.session);

            // Model the host's unconditional conservative error save, including
            // a run whose attempt to write its Failed snapshot also failed.
            fault
                .save_runtime_session(&mut fixture.session)
                .await
                .unwrap();
            let durable = fixture
                .storage
                .load_session(SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(assert_failed_start_snapshot(&durable), failed);
            assert_eq!(
                durable.agent_runtime_state,
                fixture.session.agent_runtime_state
            );

            // 0/0 has no admitted objective to replay. A fresh external owner
            // may execute ordinarily, but must not recover this generation or
            // manufacture an objective admission from its accepted receipt.
            fixture.registration.take().unwrap().abandon().await;
            let router = SessionActivationRouter::new();
            let registration = router
                .register_run(SESSION_ID, "fresh-external-owner")
                .await
                .unwrap();
            fixture.config = Some(managed_config(
                &fixture.session,
                &registration,
                fixture.storage.clone(),
                2,
                6,
            ));
            fixture.registration = Some(registration);
            let later_provider = Arc::new(ScriptedProvider::new(
                Script::RetiredGeneration,
                fixture.storage.clone(),
            ));
            let (later_result, later_events) = fixture
                .run(later_provider.clone(), CancellationToken::new())
                .await;
            later_result.unwrap();
            assert_eq!(later_provider.calls.load(Ordering::SeqCst), 1);
            assert!(!later_events
                .iter()
                .any(|event| matches!(event, AgentEvent::ToolComplete { .. })));
            assert_eq!(read_loop_state(&fixture.session).unwrap().unwrap(), failed);
            assert!(admitted_objectives(&fixture.session.messages).is_empty());
            assert_eq!(
                paired_result(&fixture.session.messages, "start")["generation"],
                failed.generation
            );
            fixture.close().await;
        }
    }
}

#[tokio::test]
async fn self_control_real_runner_disabled_tool_cannot_start_a_generation() {
    let mut fixture = Fixture::new(2, 6).await;
    fixture
        .config
        .as_mut()
        .unwrap()
        .disabled_tools
        .insert("loop_control".into());
    let mut provider = ScriptedProvider::new(Script::DisabledStart, fixture.storage.clone());
    provider.expected_tool_visibility = false;
    let provider = Arc::new(provider);
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    result.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert!(read_loop_state(&fixture.session).unwrap().is_none());
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, result } if tool_call_id == "start" && result.success)));
    let results = fixture
        .session
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some("start")
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tool_success, Some(false));
    assert!(admitted_objectives(&fixture.session.messages).is_empty());
    fixture.close().await;
}

#[test]
fn self_control_keeps_existing_static_plan_gate_even_for_readonly_status() {
    use bamboo_agent_core::{Tool, ToolMutability};
    let tool = bamboo_tools::tools::LoopControlTool::new();
    assert_eq!(
        tool.classify(&json!({"action":"status"})).mutability,
        ToolMutability::ReadOnly
    );
    assert!(
        !bamboo_tools::orchestrator::plan_mode_allows_tool("loop_control"),
        "an args-aware readonly status must not widen the existing runner Plan allowlist"
    );
}

#[tokio::test]
async fn self_control_real_runner_ultra_root_is_rejected_before_provider() {
    let mut fixture = Fixture::new(2, 6).await;
    fixture.session.set_root_orchestration_only(true).unwrap();
    fixture
        .storage
        .save_session(&fixture.session)
        .await
        .unwrap();
    let provider = Arc::new(ScriptedProvider::new(
        Script::TwoIterations,
        fixture.storage.clone(),
    ));
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(
        events.is_empty(),
        "posture refusal must precede setup and provider work"
    );
    assert!(read_loop_state(&fixture.session).unwrap().is_none());
    fixture.close().await;
}

#[tokio::test]
async fn self_control_real_runner_user_cancel_dominates_checkpoint_error_before_and_after_commit() {
    for commit_before_error in [false, true] {
        let mut fixture = Fixture::new(2, 6).await;
        let cancel = CancellationToken::new();
        let fault = Arc::new(FailStartCheckpoint {
            inner: fixture.config.as_mut().unwrap().persistence.take().unwrap(),
            commit_before_error,
            fail_terminal_checkpoint: false,
            cancel_at_failure: Some(cancel.clone()),
            start_failed: AtomicBool::new(false),
            terminal_failed: AtomicBool::new(false),
        });
        fixture.config.as_mut().unwrap().persistence = Some(fault.clone());
        let provider = Arc::new(ScriptedProvider::new(
            Script::TwoIterations,
            fixture.storage.clone(),
        ));
        let (result, events) = fixture.run(provider.clone(), cancel.clone()).await;
        assert!(fault.start_failed.load(Ordering::SeqCst));
        assert!(cancel.is_cancelled());
        assert!(matches!(result, Err(AgentError::Cancelled)),
            "user cancellation must dominate a checkpoint error, commit={commit_before_error}: {result:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.active.load(Ordering::SeqCst), 0);
        assert!(!events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolComplete { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, AgentEvent::Complete { .. })));
        let state = read_loop_state(&fixture.session).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::Cancelled);
        assert_eq!(
            (
                state.iterations_started,
                state.iterations_completed,
                state.rounds_started
            ),
            (0, 0, 1)
        );
        let runtime = fixture.session.agent_runtime_state.as_ref().unwrap();
        assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Cancelled);
        assert_eq!(
            runtime.execution_id.as_deref(),
            Some(state.owner_execution_id.as_str())
        );
        assert!(admitted_objectives(&fixture.session.messages).is_empty());
        let receipt: LoopControlReceipt =
            serde_json::from_value(paired_result(&fixture.session.messages, "start")).unwrap();
        assert!(!receipt.applied);
        fault
            .save_runtime_session(&mut fixture.session)
            .await
            .unwrap();
        let durable = fixture
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
        assert_eq!(
            durable.agent_runtime_state,
            fixture.session.agent_runtime_state
        );
        assert_eq!(
            paired_result(&durable.messages, "start"),
            serde_json::to_value(receipt).unwrap()
        );
        fixture.close().await;
    }
}

struct PublicEntryPersistence {
    inner: Arc<dyn RuntimeSessionPersistence>,
    saves: AtomicUsize,
    checkpoints: AtomicUsize,
}

#[async_trait]
impl RuntimeSessionPersistence for PublicEntryPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        self.inner.save_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.checkpoints.fetch_add(1, Ordering::SeqCst);
        self.inner.checkpoint_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn load_runtime_admission_snapshot(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_admission_snapshot(id).await
    }
}

struct PublicEntryHook {
    point: bamboo_domain::AgentHookPoint,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl bamboo_agent_core::AgentHook for PublicEntryHook {
    fn may_suspend(&self) -> bool {
        false
    }

    fn point(&self) -> bamboo_domain::AgentHookPoint {
        self.point
    }

    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        _: &bamboo_domain::HookPayload,
        _: &Session,
    ) -> bamboo_domain::HookResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        bamboo_domain::HookResult::Continue
    }
}

struct PublicEntryFixture {
    dir: tempfile::TempDir,
    session: Session,
    agent: Arc<crate::runtime::Agent>,
    provider: Arc<ScriptedProvider>,
    router: Arc<SessionActivationRouter>,
    storage: Arc<dyn Storage>,
    persistence: Arc<PublicEntryPersistence>,
    hook_calls: Arc<AtomicUsize>,
}

impl PublicEntryFixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("config");
        let workspace = dir.path().join("workspace");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let mut config = bamboo_llm::Config::from_data_dir_without_env(Some(config_dir));
        *config.memory_mut() = Some(bamboo_config::MemoryConfig {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda_injection: false,
            auto_dream_enabled: false,
            ledger_gardener_enabled: false,
            ledger_distillation_enabled: false,
            gardener_enabled: false,
            dedup_gardener_enabled: false,
            granularity_freshness_gardener_enabled: false,
            ..Default::default()
        });
        config.features.dynamic_model_routing = false;
        config.features.final_evidence_check = false;
        let flags = PromptMemoryFlags::from(config.memory().as_ref().unwrap());
        assert!(
            !flags.project_prompt_injection
                && !flags.relevant_recall
                && !flags.relevant_recall_rerank
                && !flags.project_first_dream
                && !flags.ledger_agenda
        );
        let mut session = Session::new(SESSION_ID, "synthetic-only");
        session.add_message(Message::system(ORIGINAL_CONTEXT));
        session.add_message(Message::assistant(ORIGINAL_FACT, None));
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        storage.save_session(&session).await.unwrap();
        let persistence = Arc::new(PublicEntryPersistence {
            inner: Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone())),
            saves: AtomicUsize::new(0),
            checkpoints: AtomicUsize::new(0),
        });
        let provider = Arc::new(ScriptedProvider::new(Script::PublicDirect, storage.clone()));
        let router = SessionActivationRouter::new();
        let hook_calls = Arc::new(AtomicUsize::new(0));
        let mut hook_runner = crate::runtime::HookRunner::new();
        for point in [
            bamboo_domain::AgentHookPoint::BeforeSessionSetup,
            bamboo_domain::AgentHookPoint::BeforeRound,
            bamboo_domain::AgentHookPoint::AfterSessionEnd,
        ] {
            hook_runner.register(Arc::new(PublicEntryHook {
                point,
                calls: hook_calls.clone(),
            }));
        }
        let agent = crate::runtime::Agent::builder()
            .storage(storage.clone())
            .persistence(persistence.clone())
            .attachment_reader(store)
            .skill_manager(Arc::new(bamboo_skills::SkillManager::with_config(
                bamboo_skills::SkillStoreConfig {
                    skills_dir: dir.path().join("skills"),
                    project_dir: Some(workspace),
                    active_mode: None,
                },
            )))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    dir.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(tokio::sync::RwLock::new(config)))
            .activation_router(router.clone())
            .provider(provider.clone())
            .memory_store(bamboo_memory::memory_store::MemoryStore::new(
                dir.path().join("jiandu"),
            ))
            .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::new()))
            .hook_runner(Arc::new(hook_runner))
            .build()
            .unwrap();
        Self {
            dir,
            session,
            agent: Arc::new(agent),
            provider,
            router,
            storage,
            persistence,
            hook_calls,
        }
    }

    fn request(&self, tx: mpsc::Sender<AgentEvent>) -> crate::runtime::ExecuteRequest {
        crate::runtime::ExecuteRequestBuilder::new(INITIAL_PROMPT, tx, CancellationToken::new())
            .model("synthetic-only")
            .app_data_dir(self.dir.path().join("app"))
            .run_budget(bamboo_config::RunBudgetConfig {
                max_rounds: Some(4),
                ..Default::default()
            })
            .loop_control(LoopControlConfig::new(2, 6))
            .build()
    }

    async fn execute_direct(&mut self) -> (Result<(), AgentError>, Vec<AgentEvent>) {
        let (tx, mut rx) = mpsc::channel(512);
        let request = self.request(tx);
        let agent = self.agent.clone();
        let storage = self.storage.clone();
        let run = agent.execute_direct(&mut self.session, request);
        tokio::pin!(run);
        let mut events = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut run => break result,
                event = rx.recv() => {
                    if let Some(event) = event {
                        Fixture::observe_event(storage.as_ref(), &event).await;
                        events.push(event);
                    }
                }
            }
        };
        while let Ok(event) = rx.try_recv() {
            Fixture::observe_event(storage.as_ref(), &event).await;
            events.push(event);
        }
        (result, events)
    }
}

#[tokio::test]
async fn self_control_public_agent_direct_binds_lease_and_forwards_builder_and_budget() {
    let mut fixture = PublicEntryFixture::new().await;
    let (result, events) = fixture.execute_direct().await;
    result.unwrap();
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider.active.load(Ordering::SeqCst), 0);
    assert!(fixture.hook_calls.load(Ordering::SeqCst) > 0);
    assert!(fixture.persistence.checkpoints.load(Ordering::SeqCst) > 0);
    assert!(fixture.persistence.saves.load(Ordering::SeqCst) > 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "start"))
            .count(),
        1
    );
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Completed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (2, 2, 3)
    );
    assert_eq!(
        (state.max_iterations, state.max_total_rounds),
        (2, 4),
        "the model's accepted bounds remain unchanged"
    );
    assert_eq!(
        fixture
            .session
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .max_rounds,
        Some(4),
        "the request override independently tightens the host's six-round authority"
    );
    assert_eq!(
        state.owner_execution_id,
        fixture
            .session
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .execution_id
            .as_deref()
            .unwrap()
    );
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1), (FIRST_OBJECTIVE, 1)]
    );
    let receipt: LoopControlReceipt =
        serde_json::from_value(paired_result(&fixture.session.messages, "start")).unwrap();
    assert_eq!(receipt.effective_iteration, Some(1));
    assert!(!receipt.applied);
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
    assert_eq!(
        paired_result(&durable.messages, "start"),
        serde_json::to_value(receipt).unwrap()
    );
    assert!(
        fixture.router.current_run_id(SESSION_ID).await.is_none(),
        "direct lifecycle must release its exact owner"
    );
    let seen = fixture.provider.seen.lock().unwrap();
    assert!(admitted_objectives(&seen[0].messages).is_empty());
    assert_eq!(
        admitted_objectives(&seen[1].messages),
        vec![(FIRST_OBJECTIVE, 1)]
    );
    for observation in seen.iter() {
        assert!(observation
            .messages
            .iter()
            .any(|message| message.content == ORIGINAL_FACT));
        assert!(!observation.messages.iter().any(|message| message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("runtime_kind"))
            .and_then(Value::as_str)
            == Some("max_rounds_summary")));
    }
}

#[tokio::test]
async fn self_control_public_agent_raw_execute_refuses_unbound_capability_before_effects() {
    let mut fixture = PublicEntryFixture::new().await;
    // Another real owner is deliberately present: its router registration is
    // not an authority source for this raw, unbound request.
    let registration = fixture
        .router
        .register_run(SESSION_ID, "unrelated-legitimate-owner")
        .await
        .unwrap();
    let before = serde_json::to_value(&fixture.session).unwrap();
    let durable_before = serde_json::to_value(
        fixture
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(512);
    let request = fixture.request(tx);
    let error = fixture
        .agent
        .execute(&mut fixture.session, request)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("activation owner"), "{error}");
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.hook_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.persistence.checkpoints.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.persistence.saves.load(Ordering::SeqCst), 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        serde_json::to_value(&fixture.session).unwrap(),
        before,
        "raw refusal must precede prompt append and session mutation"
    );
    assert_eq!(
        serde_json::to_value(
            fixture
                .storage
                .load_session(SESSION_ID)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        durable_before
    );
    assert_eq!(
        fixture.router.current_run_id(SESSION_ID).await.as_deref(),
        Some("unrelated-legitimate-owner")
    );
    registration.abandon().await;
}

mod output_contract_tests;
mod policy_contract_tests;

mod post_hook_contract_tests;

mod suspension_contract_tests;

mod managed_interaction_runner_tests;

mod start_hook_scope_tests;

mod pending_rounds_runner_tests;

mod managed_running_terminal_tests;
mod running_timeout_observation_tests;
