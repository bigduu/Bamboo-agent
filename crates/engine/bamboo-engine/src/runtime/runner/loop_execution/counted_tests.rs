use super::*;
use crate::runtime::config::{GoldConfig, PromptMemoryFlags};
use crate::runtime::counted_loop::{
    read_loop_state, CountedLoopConfig, LoopState, LoopStatus, LOOP_STATE_METADATA_KEY,
    MAX_LOOP_PROMPT_BYTES,
};
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
use bamboo_agent_core::{AgentHook, Message, Role};
use bamboo_domain::{HookResult, RuntimeSessionPersistence};
use bamboo_llm::{LLMChunk, LLMStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

mod recovery_tests;

const PROMPT: &str = "Inspect the next item using the accumulated context.";

#[derive(Default)]
struct FakeProvider {
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    seen: Mutex<Vec<Vec<Message>>>,
    durable: Option<Arc<dyn Storage>>,
    barrier: Option<(Arc<Notify>, Arc<Notify>)>,
    tools_forever: bool,
    task_question: bool,
    reported_usage: Option<(u64, u64)>,
    fail_at: Option<usize>,
}

#[async_trait]
impl LLMProvider for FakeProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.seen.lock().unwrap().push(messages.to_vec());
        if let Some(storage) = self.durable.as_ref() {
            let stored = storage
                .load_session("counted-fixture")
                .await
                .unwrap()
                .unwrap();
            let state = read_loop_state(&stored).unwrap().unwrap();
            assert_eq!(
                stored
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .execution_id
                    .as_deref(),
                Some(state.owner_execution_id.as_str())
            );
            assert_eq!(
                state.rounds_started, n as u32,
                "admission precedes dispatch durably"
            );
        }
        if n == 1 {
            if let Some((entered, release)) = self.barrier.as_ref() {
                entered.notify_one();
                release.notified().await;
            }
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        if self.fail_at == Some(n) {
            return Err(bamboo_llm::LLMError::Api(
                "authentication error: synthetic terminal failure".into(),
            ));
        }
        let output = if self.task_question {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: format!("task-question-{n}"),
                tool_type: "function".into(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: "Task".into(),
                    arguments: serde_json::json!({
                        "tasks": [{"id":"own", "content":"Own blocked step", "status":"blocked"}],
                        "question": {"prompt":"Own precise question"}
                    })
                    .to_string(),
                },
            }])
        } else if self.tools_forever {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: format!("read-{n}"),
                tool_type: "function".into(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: "Read".into(),
                    arguments: r#"{"file_path":"fixture"}"#.into(),
                },
            }])
        } else {
            LLMChunk::Token(format!("iteration answer {n}"))
        };
        let mut chunks = vec![Ok(output)];
        if let Some((prompt, completion)) = self.reported_usage {
            chunks.push(Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(prompt),
                output_tokens: Some(completion),
                total_tokens: Some(prompt + completion),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }));
        }
        chunks.push(Ok(LLMChunk::Done));
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

struct FakeTools;

#[async_trait]
impl ToolExecutor for FakeTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        assert_eq!(call.function.name, "Read");
        Ok(ToolResult {
            success: true,
            result: "fixture content".into(),
            display_preference: None,
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![serde_json::from_value(serde_json::json!({
            "type":"function", "function":{
                "name":"Read", "description":"Read a synthetic fixture",
                "parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}
            }
        })).unwrap()]
    }
}

fn config(iterations: u32, rounds: u32) -> AgentLoopConfig {
    AgentLoopConfig {
        model_name: Some("fake-only".into()),
        counted_loop: Some(CountedLoopConfig::new(iterations, rounds)),
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

async fn add_persistence(
    dir: &std::path::Path,
    cfg: &mut AgentLoopConfig,
    session: &Session,
) -> Arc<dyn Storage> {
    let storage: Arc<dyn Storage> = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.into())
            .await
            .unwrap(),
    );
    storage.save_session(session).await.unwrap();
    cfg.storage = Some(storage.clone());
    cfg.persistence = Some(Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    )));
    storage
}

async fn run(
    session: &mut Session,
    config: AgentLoopConfig,
    provider: Arc<FakeProvider>,
    cancel: CancellationToken,
) -> (Result<(), bamboo_agent_core::AgentError>, Vec<AgentEvent>) {
    let (tx, mut rx) = mpsc::channel(512);
    let result = run_agent_loop_with_config(
        session,
        PROMPT.into(),
        tx,
        provider,
        Arc::new(FakeTools),
        cancel,
        config,
    )
    .await;
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    (result, events)
}

fn complete_count(events: &[AgentEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, AgentEvent::Complete { .. }))
        .count()
}

#[tokio::test]
async fn counted_exact_k_accumulates_context_and_has_one_terminal_complete() {
    for k in [1, 2, 4] {
        let mut session = Session::new("counted-fixture", "fake-only");
        session.add_message(Message::system("Keep the original context."));
        let provider = Arc::new(FakeProvider::default());
        let (result, events) = run(
            &mut session,
            config(k, k),
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        result.unwrap();
        let state = read_loop_state(&session).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::Completed);
        assert_eq!(
            (
                state.iterations_started,
                state.iterations_completed,
                state.rounds_started
            ),
            (k, k, k)
        );
        assert_eq!(state.objective, PROMPT);
        assert_eq!(state.objective_revision, 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), k as usize);
        assert_eq!(
            provider.peak.load(Ordering::SeqCst),
            1,
            "one serial owner, no overlapping iterations"
        );
        assert_eq!(complete_count(&events), 1);
        let seen = provider.seen.lock().unwrap();
        for (index, transcript) in seen.iter().enumerate() {
            assert_eq!(
                transcript
                    .iter()
                    .filter(|message| message.role == Role::User && message.content == PROMPT)
                    .count(),
                index + 1
            );
            assert_eq!(
                transcript
                    .iter()
                    .filter(|message| message.role == Role::Assistant)
                    .count(),
                index
            );
            assert!(transcript
                .iter()
                .any(|message| message.content.contains("Keep the original context.")));
        }
    }
}

#[tokio::test]
async fn counted_state_is_checkpointed_before_each_dispatch_and_at_completion() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.path().into())
            .await
            .unwrap(),
    );
    let mut session = Session::new("counted-fixture", "fake-only");
    storage.save_session(&session).await.unwrap();
    let mut cfg = config(3, 3);
    cfg.storage = Some(storage.clone());
    cfg.persistence = Some(Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    )));
    let provider = Arc::new(FakeProvider {
        durable: Some(storage.clone()),
        ..Default::default()
    });
    let (result, events) = run(&mut session, cfg, provider, CancellationToken::new()).await;
    result.unwrap();
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&durable).unwrap(),
        read_loop_state(&session).unwrap()
    );
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::Completed
    );
    assert_eq!(complete_count(&events), 1);
}

struct StopAfterRound(CountedLoopConfig);

#[async_trait]
impl AgentHook for StopAfterRound {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterRound
    }
    async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
        self.0.stop();
        self.0.stop();
        HookResult::Continue
    }
}

#[tokio::test]
async fn counted_stop_wins_before_next_admission_without_leaving_a_repeat_prompt() {
    let mut cfg = config(4, 8);
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(StopAfterRound(cfg.counted_loop.clone().unwrap())));
    cfg.hook_runner = Arc::new(hooks);
    let mut session = Session::new("counted-fixture", "fake-only");
    let provider = Arc::new(FakeProvider::default());
    let (result, events) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    result.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let state = read_loop_state(&session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Stopped);
    assert_eq!(state.iterations_completed, 1);
    assert_eq!(
        session
            .messages
            .iter()
            .filter(|m| m.role == Role::User && m.content == PROMPT)
            .count(),
        1
    );
    assert_eq!(complete_count(&events), 1);
}

#[tokio::test]
async fn counted_stop_during_an_admitted_round_drains_only_that_round() {
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut cfg = config(4, 8);
    let stop = cfg.counted_loop.clone().unwrap();
    let mut session = Session::new("counted-fixture", "fake-only");
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let provider = Arc::new(FakeProvider {
        barrier: Some((entered.clone(), release.clone())),
        durable: Some(storage.clone()),
        ..Default::default()
    });
    let (outcome, ()) = tokio::join!(
        run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new()
        ),
        async {
            entered.notified().await;
            stop.stop();
            release.notify_one();
        }
    );
    outcome.0.unwrap();
    assert_eq!(
        read_loop_state(&session).unwrap().unwrap().status,
        LoopStatus::Stopped
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count(&outcome.1), 1);
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::Stopped
    );
}

#[tokio::test]
async fn counted_session_cancel_persists_cancelled_without_false_complete() {
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut cfg = config(4, 8);
    let mut session = Session::new("counted-fixture", "fake-only");
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let provider = Arc::new(FakeProvider {
        barrier: Some((entered.clone(), release.clone())),
        durable: Some(storage.clone()),
        ..Default::default()
    });
    let cancel = CancellationToken::new();
    let (outcome, ()) = tokio::join!(
        run(&mut session, cfg, provider.clone(), cancel.clone()),
        async {
            entered.notified().await;
            cancel.cancel();
            release.notify_one();
        }
    );
    assert!(outcome.0.unwrap_err().is_cancelled());
    assert_eq!(
        read_loop_state(&session).unwrap().unwrap().status,
        LoopStatus::Cancelled
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count(&outcome.1), 0);
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::Cancelled
    );
}

#[tokio::test]
async fn counted_provider_failure_persists_failed_and_never_starts_another_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(3, 6);
    let mut session = Session::new("counted-fixture", "fake-only");
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let provider = Arc::new(FakeProvider {
        fail_at: Some(2),
        durable: Some(storage.clone()),
        ..Default::default()
    });
    let (result, events) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(complete_count(&events), 0);
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    let state = read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Failed);
    assert_eq!(state.iterations_completed, 1);
}

#[tokio::test]
async fn counted_round_cap_is_hard_even_when_the_provider_keeps_calling_tools() {
    for host_cap in [Some(2), None] {
        let mut session = Session::new("counted-fixture", "fake-only");
        let provider = Arc::new(FakeProvider {
            tools_forever: true,
            ..Default::default()
        });
        let mut cfg = config(1, if host_cap.is_some() { 8 } else { 2 });
        cfg.run_budget.max_rounds = host_cap;
        let (result, events) = run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        result.unwrap();
        let state = read_loop_state(&session).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::BudgetExhausted);
        assert_eq!(
            state.max_total_rounds, 2,
            "host bound only tightens the request"
        );
        assert_eq!(state.rounds_started, 2);
        assert_eq!(state.iterations_completed, 0);
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "no extra summary/model judge"
        );
        assert_eq!(complete_count(&events), 1);
    }
}

#[tokio::test]
async fn counted_rejects_invalid_bounds_and_gold_before_model_or_transcript_mutation() {
    for (iterations, rounds, gold, cap) in [
        (0, 1, false, None),
        (101, 101, false, None),
        (1, 0, false, None),
        (1, 1001, false, None),
        (1, 1001, false, Some(2)),
        (2, 2, false, Some(1)),
        (1, 2, true, None),
    ] {
        let mut cfg = config(iterations, rounds);
        cfg.run_budget.max_rounds = cap;
        if gold {
            cfg.gold_config = Some(GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("existing Gold objective".into()),
                ..Default::default()
            });
        }
        let mut session = Session::new("counted-fixture", "fake-only");
        let provider = Arc::new(FakeProvider::default());
        let (result, events) = run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(result.is_err());
        assert!(session.messages.is_empty());
        assert!(!session.metadata.contains_key(LOOP_STATE_METADATA_KEY));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(complete_count(&events), 0);
    }
    let mut session = Session::new("counted-fixture", "fake-only");
    let provider = Arc::new(FakeProvider::default());
    let (tx, _) = mpsc::channel(10);
    let result = run_agent_loop_with_config(
        &mut session,
        "界".repeat(MAX_LOOP_PROMPT_BYTES),
        tx,
        provider.clone(),
        Arc::new(FakeTools),
        CancellationToken::new(),
        config(1, 1),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

fn interrupted_state() -> LoopState {
    LoopState {
        schema_version: 1,
        generation: uuid::Uuid::new_v4().to_string(),
        revision: 4,
        objective_revision: 1,
        owner_execution_id: "old-owner".into(),
        objective: PROMPT.into(),
        max_iterations: 3,
        max_total_rounds: 6,
        iterations_started: 1,
        iterations_completed: 0,
        rounds_started: 1,
        status: LoopStatus::Running,
    }
}

#[tokio::test]
async fn counted_restart_is_explicitly_unsupported_and_never_replays_an_admission() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.path().into())
            .await
            .unwrap(),
    );
    let mut session = Session::new("counted-fixture", "fake-only");
    session.add_message(Message::user(PROMPT));
    session.metadata.insert(
        LOOP_STATE_METADATA_KEY.into(),
        serde_json::to_string(&interrupted_state()).unwrap(),
    );
    storage.save_session(&session).await.unwrap();
    for _ in 0..2 {
        let mut reloaded = storage.load_session(&session.id).await.unwrap().unwrap();
        let router = crate::session_activation::SessionActivationRouter::new();
        let registration = router
            .register_run(&session.id, "replacement-owner")
            .await
            .unwrap();
        let persistence = bamboo_storage::LockedSessionStore::new(storage.clone());
        let refused = crate::runtime::counted_loop::admit_registered(
            &mut reloaded,
            Some(&registration.ownership()),
            Some(&router),
            &persistence,
        )
        .await
        .unwrap_err();
        assert!(refused.to_string().contains("recovery unsupported"));
        registration.abandon().await;
        let mut cfg = config(3, 6);
        cfg.counted_loop = None; // even an ordinary resume must not replay it
        cfg.persistence = Some(Arc::new(bamboo_storage::LockedSessionStore::new(
            storage.clone(),
        )));
        let provider = Arc::new(FakeProvider::default());
        let (result, _) = run(
            &mut reloaded,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("recovery unsupported"));
        assert_eq!(
            read_loop_state(&reloaded).unwrap().unwrap().status,
            LoopStatus::RecoveryUnsupported
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(reloaded.messages.len(), 1);
    }
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&durable).unwrap().unwrap().status,
        LoopStatus::RecoveryUnsupported
    );
}

#[tokio::test]
async fn counted_malformed_state_fails_closed_without_resetting_consumption() {
    let valid = serde_json::to_value(interrupted_state()).unwrap();
    let mut malformed = vec!["not-json".into()];
    for (key, value) in [
        ("schema_version", serde_json::json!(2)),
        ("revision", serde_json::json!(u64::MAX)),
        ("rounds_started", serde_json::json!(1001)),
        ("iterations_completed", serde_json::json!(2)),
        ("objective_revision", serde_json::json!(2)),
        ("unknown_field", serde_json::json!(true)),
    ] {
        let mut state = valid.clone();
        state[key] = value;
        malformed.push(state.to_string());
    }
    for raw in malformed {
        let mut session = Session::new("counted-fixture", "fake-only");
        session
            .metadata
            .insert(LOOP_STATE_METADATA_KEY.into(), raw.clone());
        let provider = Arc::new(FakeProvider::default());
        let (result, events) = run(
            &mut session,
            config(3, 6),
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(session.metadata[LOOP_STATE_METADATA_KEY], raw);
        assert!(session.messages.is_empty());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(complete_count(&events), 0);
    }
}

struct FailingPersistence;
#[async_trait]
impl RuntimeSessionPersistence for FailingPersistence {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        Err(std::io::Error::other("synthetic checkpoint failure"))
    }
}

#[tokio::test]
async fn counted_failed_checkpoint_never_dispatches_the_provider() {
    let mut cfg = config(2, 2);
    cfg.persistence = Some(Arc::new(FailingPersistence));
    let mut session = Session::new("counted-fixture", "fake-only");
    let provider = Arc::new(FakeProvider::default());
    let (result, events) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(complete_count(&events), 0);
}

struct FailInitialCheckpoints {
    inner: Arc<dyn RuntimeSessionPersistence>,
    remaining: AtomicUsize,
}

#[async_trait]
impl RuntimeSessionPersistence for FailInitialCheckpoints {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if read_loop_state(session).unwrap().is_some()
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(std::io::Error::other(
                "synthetic initial checkpoint failure",
            ));
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn counted_initial_checkpoint_failure_cannot_be_resaved_as_live() {
    for failures in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(2, 2);
        let mut session = Session::new("counted-fixture", "fake-only");
        let storage = add_persistence(dir.path(), &mut cfg, &session).await;
        let persistence = Arc::new(FailInitialCheckpoints {
            inner: cfg.persistence.take().unwrap(),
            remaining: AtomicUsize::new(failures),
        });
        cfg.persistence = Some(persistence.clone());
        let provider = Arc::new(FakeProvider::default());
        let (result, events) = run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Session checkpoint failed"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(complete_count(&events), 0);
        let state = read_loop_state(&session).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::Failed);
        assert_eq!(
            (
                state.iterations_started,
                state.iterations_completed,
                state.rounds_started
            ),
            (0, 0, 0)
        );
        assert_eq!(persistence.remaining.load(Ordering::SeqCst), 0);
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
        // Model the execute boundary's unconditional error save after the
        // transient failure, including failure of the Failed checkpoint itself.
        persistence
            .checkpoint_runtime_session(&mut session)
            .await
            .unwrap();
        let mut durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            read_loop_state(&durable).unwrap().unwrap().status,
            LoopStatus::Failed
        );
        assert_eq!(
            durable.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
        let mut ordinary = config(1, 1);
        ordinary.counted_loop = None;
        let (result, _) = run(
            &mut durable,
            ordinary,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        result.unwrap();
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            1,
            "a failed initial save cannot poison ordinary re-entry"
        );
    }
}

struct CancelAfterFirstAdmission {
    inner: Arc<dyn RuntimeSessionPersistence>,
    cancel: CancellationToken,
}

#[async_trait]
impl RuntimeSessionPersistence for CancelAfterFirstAdmission {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.checkpoint_runtime_session(session).await?;
        if read_loop_state(session)
            .unwrap()
            .is_some_and(|state| state.status == LoopStatus::Running && state.rounds_started == 1)
        {
            self.cancel.cancel();
        }
        Ok(())
    }
}

#[tokio::test]
async fn counted_cancel_after_admission_terminalizes_the_runtime_before_provider_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(2, 4);
    let mut session = Session::new("counted-fixture", "fake-only");
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let cancel = CancellationToken::new();
    cfg.persistence = Some(Arc::new(CancelAfterFirstAdmission {
        inner: cfg.persistence.take().unwrap(),
        cancel: cancel.clone(),
    }));
    let provider = Arc::new(FakeProvider::default());
    let (result, events) = run(&mut session, cfg, provider.clone(), cancel).await;
    assert!(result.unwrap_err().is_cancelled());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(complete_count(&events), 0);
    let state = read_loop_state(&session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Cancelled);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (1, 0, 1)
    );
    let runtime = session.agent_runtime_state.as_ref().unwrap();
    assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Cancelled);
    assert!(runtime.round.last_round_id.is_some());
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(read_loop_state(&durable).unwrap().unwrap(), state);
    assert_eq!(
        durable.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Cancelled
    );
    storage.save_session(&session).await.unwrap();
    let saved_again = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        saved_again.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Cancelled
    );
}

#[test]
fn counted_failure_correction_requires_the_exact_runtime_snapshot_and_preserves_waits() {
    use bamboo_domain::AgentStatusState;
    for status in [
        AgentStatusState::Initializing,
        AgentStatusState::Running,
        AgentStatusState::Finalizing,
        AgentStatusState::Completed,
        AgentStatusState::Suspended,
        AgentStatusState::Idle,
        AgentStatusState::Failed,
        AgentStatusState::Cancelled,
    ] {
        for changed in ["none", "run", "round", "execution"] {
            for cancelled in [false, true] {
                let mut session = Session::new("counted-fixture", "fake-only");
                let mut runtime = bamboo_domain::AgentRuntimeState::new(if changed == "run" {
                    "successor"
                } else {
                    "owned"
                });
                runtime.round.last_round_id = Some(
                    if changed == "round" {
                        "successor-round"
                    } else {
                        "owned-round"
                    }
                    .into(),
                );
                runtime.execution_id = Some(if changed == "execution" {
                    "successor-execution".into()
                } else {
                    "owned-execution".into()
                });
                runtime.status = status;
                session.agent_runtime_state = Some(runtime);
                let expected = (
                    "owned".into(),
                    Some("owned-round".into()),
                    Some("owned-execution".into()),
                );
                let error = if cancelled {
                    bamboo_agent_core::AgentError::Cancelled
                } else {
                    bamboo_agent_core::AgentError::LLM("synthetic failure".into())
                };
                fail_counted_runtime(&mut session, Some(&expected), &error);
                let should_correct = changed == "none"
                    && matches!(
                        status,
                        AgentStatusState::Initializing
                            | AgentStatusState::Running
                            | AgentStatusState::Finalizing
                            | AgentStatusState::Completed
                    );
                let expected_status = if should_correct {
                    if cancelled {
                        AgentStatusState::Cancelled
                    } else {
                        AgentStatusState::Failed
                    }
                } else {
                    status
                };
                assert_eq!(
                    session.agent_runtime_state.as_ref().unwrap().status,
                    expected_status,
                    "{status:?}, {changed}, cancelled={cancelled}"
                );
            }
        }
    }
}

#[tokio::test]
async fn counted_ticket_question_yield_preserves_wait_without_claiming_budget_exhaustion() {
    let dir = tempfile::tempdir().unwrap();
    let mut fixture = crate::ticket_worker_plan::tests::Fixture::new();
    let plan = Arc::new(fixture.plan());
    plan.bind_session(&mut fixture.session).unwrap();
    let mut cfg = config(2, 4);
    cfg.bind_ticket_worker_plan(plan);
    let storage = add_persistence(dir.path(), &mut cfg, &fixture.session).await;
    let provider = Arc::new(FakeProvider {
        task_question: true,
        ..Default::default()
    });
    let tools = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(bamboo_tools::TaskTool::new())
            .unwrap()
            .build(),
    );
    let (tx, mut rx) = mpsc::channel(512);
    run_agent_loop_with_config(
        &mut fixture.session,
        PROMPT.into(),
        tx,
        provider.clone(),
        tools,
        CancellationToken::new(),
        cfg,
    )
    .await
    .unwrap();
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    assert_eq!(
        complete_count(&events),
        1,
        "one activation terminal event, not a completed iteration"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::RecoveryUnsupported);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (1, 0, 1)
    );
    assert_eq!(
        fixture.session.metadata["runtime.completion_reason"],
        "ticket_question_yield"
    );
    let snapshot = fixture.service.published().unwrap().1;
    let request_id = snapshot.assignments[&fixture.assignment]
        .awaiting_request
        .as_ref()
        .unwrap();
    assert_eq!(
        snapshot.requests[request_id].kind,
        bamboo_tickets::RequestKind::Question
    );
    assert!(snapshot.submissions.is_empty());
    let mut durable = storage
        .load_session(&fixture.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap().unwrap(), state);
    assert_eq!(
        durable.metadata[crate::ticket_worker_plan::TICKET_QUESTION_YIELD_KEY],
        fixture.session.metadata[crate::ticket_worker_plan::TICKET_QUESTION_YIELD_KEY]
    );
    let mut ordinary = config(1, 1);
    ordinary.counted_loop = None;
    let (result, events) = run(
        &mut durable,
        ordinary,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(complete_count(&events), 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.service.published().unwrap().1.seq, snapshot.seq);
}

#[tokio::test]
async fn counted_public_request_reuses_activation_owner_and_rejects_overlap() {
    use crate::runtime::{Agent, ExecuteRequestBuilder};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
            .await
            .unwrap(),
    );
    let storage: Arc<dyn Storage> = store.clone();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = Arc::new(FakeProvider {
        durable: Some(storage.clone()),
        barrier: Some((entered.clone(), release.clone())),
        ..Default::default()
    });
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            dir.path().join("metrics.db"),
        )),
        7,
    );
    let agent = Agent::builder()
        .storage(storage.clone())
        .persistence(Arc::new(bamboo_storage::LockedSessionStore::new(
            storage.clone(),
        )))
        .attachment_reader(store)
        .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
        .metrics_collector(metrics)
        .config(Arc::new(tokio::sync::RwLock::new(
            bamboo_llm::Config::default(),
        )))
        .activation_router(crate::session_activation::SessionActivationRouter::new())
        .provider(provider.clone())
        .memory_store(bamboo_memory::memory_store::MemoryStore::new(
            dir.path().join("jiandu"),
        ))
        .default_tools(Arc::new(FakeTools))
        .build()
        .unwrap();
    let mut session = Session::new("counted-fixture", "fake-only");
    session.add_message(Message::user(PROMPT));
    storage.save_session(&session).await.unwrap();
    let mut conflicting = session.clone();
    let original_conflicting = serde_json::to_value(&conflicting).unwrap();
    // Earlier stop/config clones must not inherit the consumed request's owner.
    let counted = CountedLoopConfig::new(2, 2);
    let request = |tx| {
        ExecuteRequestBuilder::new(PROMPT, tx, CancellationToken::new())
            .model("fake-only")
            .counted_loop(counted.clone())
            .build()
    };
    let (raw_tx, _) = mpsc::channel(512);
    let error = agent
        .execute(&mut session, request(raw_tx))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("activation owner"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let (tx, mut rx) = mpsc::channel(512);
    let session_id = session.id.clone();
    let (result, ()) = tokio::join!(agent.execute_direct(&mut session, request(tx)), async {
        entered.notified().await;
        let (raw_tx, _) = mpsc::channel(512);
        let error = agent
            .execute(&mut conflicting, request(raw_tx))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("activation owner"));
        assert_eq!(
            serde_json::to_value(&conflicting).unwrap(),
            original_conflicting
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        // An ordinary raw request has no ownership either. Its live snapshot
        // must not fence or checkpoint the physically executing counted owner.
        let mut ordinary = storage.load_session(&session_id).await.unwrap().unwrap();
        let ordinary_before = serde_json::to_value(&ordinary).unwrap();
        let durable_before =
            serde_json::to_value(storage.load_session(&session_id).await.unwrap().unwrap())
                .unwrap();
        let (ordinary_tx, mut ordinary_rx) = mpsc::channel(512);
        let error = agent
            .execute(
                &mut ordinary,
                ExecuteRequestBuilder::new(PROMPT, ordinary_tx, CancellationToken::new()).build(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recovery unsupported"));
        assert_eq!(serde_json::to_value(&ordinary).unwrap(), ordinary_before);
        assert_eq!(
            serde_json::to_value(storage.load_session(&session_id).await.unwrap().unwrap())
                .unwrap(),
            durable_before
        );
        assert!(ordinary_rx.try_recv().is_err());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let (conflict_tx, _) = mpsc::channel(512);
        assert!(agent
            .execute_direct(&mut conflicting, request(conflict_tx))
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(&conflicting).unwrap(),
            original_conflicting
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        release.notify_one();
    });
    result.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(
        read_loop_state(&session).unwrap().unwrap().status,
        LoopStatus::Completed
    );
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    assert_eq!(complete_count(&events), 1);
}

struct FinalGuardian(AtomicUsize);
#[async_trait]
impl crate::runtime::config::GuardianSpawner for FinalGuardian {
    async fn spawn_guardian_review(
        &self,
        session: &Session,
        _: String,
        _: String,
        _: Option<std::collections::BTreeSet<String>>,
    ) -> Result<String, String> {
        let state = read_loop_state(session).unwrap().unwrap();
        assert_eq!(
            state.iterations_completed, 2,
            "intermediate iterations never spend a Guardian review"
        );
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("synthetic-reviewer".into())
    }
}

#[tokio::test]
async fn counted_final_guardian_suspension_is_preserved_without_automatic_resume() {
    let guardian = Arc::new(FinalGuardian(AtomicUsize::new(0)));
    let mut cfg = config(2, 4);
    cfg.guardian_config = Some(crate::runtime::config::GuardianConfig {
        enabled: true,
        ..Default::default()
    });
    cfg.guardian_spawner = Some(guardian.clone());
    let mut session = Session::new("counted-fixture", "fake-only");
    let provider = Arc::new(FakeProvider::default());
    let (result, _) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    result.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(guardian.0.load(Ordering::SeqCst), 1);
    assert!(session
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_some());
    assert_eq!(
        read_loop_state(&session).unwrap().unwrap().status,
        LoopStatus::RecoveryUnsupported
    );
    let (result, _) = run(
        &mut session,
        config(2, 4),
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn counted_rejects_mismatched_admitted_initial_input() {
    let mut cfg = config(2, 2);
    cfg.skip_initial_user_message = true;
    let mut session = Session::new("counted-fixture", "fake-only");
    session.add_message(Message::user("a different instruction"));
    let before = serde_json::to_value(&session).unwrap();
    let provider = Arc::new(FakeProvider::default());
    let (result, _) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

struct CancelAfterRound {
    cancel: CancellationToken,
    stop: Option<CountedLoopConfig>,
}

#[async_trait]
impl AgentHook for CancelAfterRound {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterRound
    }
    async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
        if let Some(stop) = self.stop.as_ref() {
            stop.stop();
        }
        self.cancel.cancel();
        HookResult::Continue
    }
}

#[tokio::test]
async fn counted_session_cancel_wins_simultaneous_stop_and_budget_boundaries() {
    for tools in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let mut cfg = if tools { config(1, 1) } else { config(2, 4) };
        let stop = (!tools).then(|| cfg.counted_loop.clone().unwrap());
        let mut hooks = crate::runtime::hooks::HookRunner::new();
        hooks.register(Arc::new(CancelAfterRound {
            cancel: cancel.clone(),
            stop,
        }));
        cfg.hook_runner = Arc::new(hooks);
        let mut session = Session::new("counted-fixture", "fake-only");
        let storage = add_persistence(dir.path(), &mut cfg, &session).await;
        let provider = Arc::new(FakeProvider {
            tools_forever: tools,
            durable: Some(storage.clone()),
            ..Default::default()
        });
        let (result, events) = run(&mut session, cfg, provider.clone(), cancel).await;
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(complete_count(&events), 0);
        let durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            read_loop_state(&durable).unwrap().unwrap().status,
            LoopStatus::Cancelled
        );
    }
}

struct CancelAtTerminalCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    cancel: CancellationToken,
    trigger_status: LoopStatus,
}

#[async_trait]
impl RuntimeSessionPersistence for CancelAtTerminalCheckpoint {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.checkpoint_runtime_session(session).await?;
        if read_loop_state(session)
            .unwrap()
            .is_some_and(|state| state.status == self.trigger_status)
        {
            self.cancel.cancel();
        }
        Ok(())
    }
}

#[tokio::test]
async fn counted_cancel_during_terminal_checkpoint_keeps_cancelled_error_and_no_complete() {
    for boundary in [
        LoopStatus::Stopped,
        LoopStatus::BudgetExhausted,
        LoopStatus::Finalizing,
        LoopStatus::Completed,
    ] {
        let tools = boundary == LoopStatus::BudgetExhausted;
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let mut cfg = if boundary == LoopStatus::Stopped {
            config(2, 4)
        } else {
            config(1, 1)
        };
        if boundary == LoopStatus::Stopped {
            let mut hooks = crate::runtime::hooks::HookRunner::new();
            hooks.register(Arc::new(StopAfterRound(cfg.counted_loop.clone().unwrap())));
            cfg.hook_runner = Arc::new(hooks);
        }
        let mut session = Session::new("counted-fixture", "fake-only");
        let storage = add_persistence(dir.path(), &mut cfg, &session).await;
        cfg.persistence = Some(Arc::new(CancelAtTerminalCheckpoint {
            inner: cfg.persistence.take().unwrap(),
            cancel: cancel.clone(),
            trigger_status: boundary,
        }));
        let provider = Arc::new(FakeProvider {
            tools_forever: tools,
            durable: Some(storage.clone()),
            ..Default::default()
        });
        let (result, events) = run(&mut session, cfg, provider.clone(), cancel).await;
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(complete_count(&events), 0);
        let durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            read_loop_state(&durable).unwrap().unwrap().status,
            LoopStatus::Cancelled
        );
        if boundary == LoopStatus::Completed {
            assert_eq!(
                durable.agent_runtime_state.as_ref().unwrap().status,
                bamboo_domain::AgentStatusState::Cancelled
            );
        }
    }
}

struct HoldCompletedCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    fail: bool,
}

#[async_trait]
impl RuntimeSessionPersistence for HoldCompletedCheckpoint {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if read_loop_state(session)
            .unwrap()
            .is_some_and(|state| state.status == LoopStatus::Completed)
        {
            self.entered.notify_one();
            self.release.notified().await;
            if self.fail {
                return Err(std::io::Error::other(
                    "synthetic Completed checkpoint failure",
                ));
            }
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn counted_complete_waits_for_terminal_checkpoint_and_never_publishes_failed_success() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(1, 1);
        let mut session = Session::new("counted-fixture", "fake-only");
        let storage = add_persistence(dir.path(), &mut cfg, &session).await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        cfg.persistence = Some(Arc::new(HoldCompletedCheckpoint {
            inner: cfg.persistence.take().unwrap(),
            entered: entered.clone(),
            release: release.clone(),
            fail,
        }));
        let provider = Arc::new(FakeProvider {
            durable: Some(storage.clone()),
            ..Default::default()
        });
        let (tx, mut rx) = mpsc::channel(512);
        let mut events = Vec::new();
        let (result, ()) = tokio::join!(
            run_agent_loop_with_config(
                &mut session,
                PROMPT.into(),
                tx,
                provider.clone(),
                Arc::new(FakeTools),
                CancellationToken::new(),
                cfg,
            ),
            async {
                entered.notified().await;
                while let Ok(event) = rx.try_recv() {
                    events.push(event);
                }
                assert_eq!(complete_count(&events), 0, "checkpoint is still pending");
                let stored = storage
                    .load_session("counted-fixture")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    read_loop_state(&stored).unwrap().unwrap().status,
                    LoopStatus::Finalizing
                );
                release.notify_one();
            }
        );
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let expected = if fail {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("checkpoint failed"));
            assert_eq!(complete_count(&events), 0);
            LoopStatus::Failed
        } else {
            result.unwrap();
            assert_eq!(complete_count(&events), 1);
            LoopStatus::Completed
        };
        assert_eq!(read_loop_state(&session).unwrap().unwrap().status, expected);
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            if fail {
                bamboo_domain::AgentStatusState::Failed
            } else {
                bamboo_domain::AgentStatusState::Completed
            }
        );
        let stored = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(read_loop_state(&stored).unwrap().unwrap().status, expected);
        // The host's later normal/error save cannot accidentally restore the
        // in-memory Completed value from the failed checkpoint attempt.
        storage.save_session(&session).await.unwrap();
        let saved_again = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            read_loop_state(&saved_again).unwrap().unwrap().status,
            expected
        );
    }
}

#[tokio::test]
async fn counted_complete_reports_actual_run_usage_for_success_stop_and_budget() {
    for terminal in [
        LoopStatus::Completed,
        LoopStatus::Stopped,
        LoopStatus::BudgetExhausted,
    ] {
        let tools = terminal == LoopStatus::BudgetExhausted;
        let mut cfg = if tools { config(1, 2) } else { config(2, 4) };
        if terminal == LoopStatus::Stopped {
            let mut hooks = crate::runtime::hooks::HookRunner::new();
            hooks.register(Arc::new(StopAfterRound(cfg.counted_loop.clone().unwrap())));
            cfg.hook_runner = Arc::new(hooks);
        }
        let mut session = Session::new("counted-fixture", "fake-only");
        let provider = Arc::new(FakeProvider {
            tools_forever: tools,
            reported_usage: Some((7, 3)),
            ..Default::default()
        });
        let (result, events) = run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        result.unwrap();
        let calls = if terminal == LoopStatus::Stopped {
            1
        } else {
            2
        };
        assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
        assert_eq!(read_loop_state(&session).unwrap().unwrap().status, terminal);
        assert_eq!(complete_count(&events), 1);
        let usage = events
            .iter()
            .find_map(|event| match event {
                AgentEvent::Complete { usage } => Some(usage),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            (
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens
            ),
            ((calls * 7) as u64, (calls * 3) as u64, (calls * 10) as u64)
        );
    }
}

struct AbortCountedAfterRound;

#[async_trait]
impl AgentHook for AbortCountedAfterRound {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterRound
    }
    async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
        HookResult::Abort {
            reason: "synthetic post-round failure".into(),
        }
    }
}

#[tokio::test]
async fn counted_after_round_failure_has_no_premature_complete() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(1, 1);
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(AbortCountedAfterRound));
    cfg.hook_runner = Arc::new(hooks);
    let mut session = Session::new("counted-fixture", "fake-only");
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let (result, events) = run(
        &mut session,
        cfg,
        Arc::new(FakeProvider::default()),
        CancellationToken::new(),
    )
    .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("synthetic post-round failure"));
    assert_eq!(complete_count(&events), 0);
    let stored = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&stored).unwrap().unwrap().status,
        LoopStatus::Failed
    );
}

#[derive(Clone, Copy)]
enum TerminalStateFault {
    Generation,
    Missing,
    Malformed,
}

struct ChangeFinalizingState {
    inner: Arc<dyn RuntimeSessionPersistence>,
    fault: TerminalStateFault,
}

#[async_trait]
impl RuntimeSessionPersistence for ChangeFinalizingState {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if let Some(mut state) = read_loop_state(session).unwrap() {
            if state.status == LoopStatus::Finalizing {
                match self.fault {
                    TerminalStateFault::Generation => {
                        state.generation = uuid::Uuid::new_v4().to_string();
                        session.metadata.insert(
                            LOOP_STATE_METADATA_KEY.into(),
                            serde_json::to_string(&state).unwrap(),
                        );
                    }
                    TerminalStateFault::Missing => {
                        session.metadata.remove(LOOP_STATE_METADATA_KEY);
                    }
                    TerminalStateFault::Malformed => {
                        session
                            .metadata
                            .insert(LOOP_STATE_METADATA_KEY.into(), "{bad".into());
                    }
                }
            }
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn counted_invalid_terminal_state_has_no_complete_and_host_save_keeps_failure() {
    for fault in [
        TerminalStateFault::Generation,
        TerminalStateFault::Missing,
        TerminalStateFault::Malformed,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(1, 1);
        let mut session = Session::new("counted-fixture", "fake-only");
        let storage = add_persistence(dir.path(), &mut cfg, &session).await;
        cfg.persistence = Some(Arc::new(ChangeFinalizingState {
            inner: cfg.persistence.take().unwrap(),
            fault,
        }));
        let (result, events) = run(
            &mut session,
            cfg,
            Arc::new(FakeProvider::default()),
            CancellationToken::new(),
        )
        .await;
        let expected_error = match fault {
            TerminalStateFault::Generation => "generation changed",
            TerminalStateFault::Missing => "missing terminal state",
            TerminalStateFault::Malformed => "malformed",
        };
        assert!(result.unwrap_err().to_string().contains(expected_error));
        assert_eq!(complete_count(&events), 0);
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
        storage.save_session(&session).await.unwrap();
        let stored = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            stored.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
        match fault {
            TerminalStateFault::Generation => assert_eq!(
                read_loop_state(&stored).unwrap().unwrap().status,
                LoopStatus::Finalizing
            ),
            TerminalStateFault::Missing => assert!(read_loop_state(&stored).unwrap().is_none()),
            TerminalStateFault::Malformed => assert!(read_loop_state(&stored).is_err()),
        }
    }
}

struct CountedStopHook(AtomicUsize);

#[async_trait]
impl AgentHook for CountedStopHook {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::BeforeFinalize
    }
    async fn run(&self, _: AgentHookPoint, payload: &HookPayload, session: &Session) -> HookResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            read_loop_state(session)
                .unwrap()
                .unwrap()
                .iterations_completed,
            2
        );
        match payload {
            HookPayload::Finalize {
                stop_hook_active: false,
            } => HookResult::Deny {
                reason: "verify once".into(),
            },
            HookPayload::Finalize {
                stop_hook_active: true,
            } => HookResult::Continue,
            payload => panic!("unexpected hook payload: {payload:?}"),
        }
    }
}

#[tokio::test]
async fn counted_final_stop_hook_continues_within_cap_before_one_complete() {
    let mut cfg = config(2, 3);
    let hook = Arc::new(CountedStopHook(AtomicUsize::new(0)));
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(hook.clone());
    cfg.hook_runner = Arc::new(hooks);
    let mut session = Session::new("counted-fixture", "fake-only");
    let provider = Arc::new(FakeProvider {
        reported_usage: Some((7, 3)),
        ..Default::default()
    });
    let (result, events) = run(
        &mut session,
        cfg,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;
    result.unwrap();
    let state = read_loop_state(&session).unwrap().unwrap();
    assert_eq!(
        (
            state.iterations_completed,
            state.rounds_started,
            state.status
        ),
        (2, 3, LoopStatus::Completed)
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(hook.0.load(Ordering::SeqCst), 2);
    assert_eq!(complete_count(&events), 1);
}
