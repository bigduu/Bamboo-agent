//! Phase 2 SDK runner tests (S-T2.1 .. S-T2.5).
//!
//! These exercise the canonical spawn core (`run_child_spawn`) and the ergonomic
//! `ChildRunner` facade directly at the engine level, using a mock LLM provider
//! to avoid network I/O.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};

use crate::runtime::execution::child_completion::{ChildCompletion, ChildCompletionHandler};
use crate::runtime::execution::spawn::{ExternalChildRunner, SpawnContext, SpawnJob};
use crate::sdk::runner::{ChildRunner, RunChildInput};
use crate::sdk::spawn::run_child_spawn;
use bamboo_metrics::MetricsCollector;
use bamboo_skills::SkillManager;

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// Provider that returns a single token + Done, so the child loop completes.
struct CompletedProvider;

#[async_trait]
impl LLMProvider for CompletedProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let items: Vec<bamboo_llm::provider::Result<LLMChunk>> =
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)];
        Ok(Box::pin(stream::iter(items)))
    }
}

/// Provider whose stream drips tokens forever without ever sending `Done`, so the
/// child runner never finishes on its own and the watchdog must terminate it.
///
/// The stream MUST keep yielding (rather than `stream::pending()`): the stream
/// consumer only checks the cancel token between chunks, mirroring real LLM
/// streams that emit periodic chunks.
struct HangingProvider;

#[async_trait]
impl LLMProvider for HangingProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        // A deliberately slow but *finite* stream: a handful of tokens spaced out,
        // then `Done`. Each LLM call (including any auxiliary calls that do not
        // thread the cancel token) terminates, but the main agent round takes long
        // enough that the child watchdog fires and cancels the run first — the
        // round-boundary cancel check then yields `AgentError::Cancelled`, which the
        // spawn core maps to a `timeout` terminal status (because `timeout_reason`
        // was set by the watchdog).
        let chunks: Vec<bamboo_llm::provider::Result<LLMChunk>> = vec![
            Ok(LLMChunk::Token("slow-1".to_string())),
            Ok(LLMChunk::Token("slow-2".to_string())),
            Ok(LLMChunk::Done),
        ];
        // Each chunk is paced well beyond the watchdog's total-timeout window, so
        // the watchdog deterministically fires (and cancels) while the run is still
        // streaming — never racing a fast natural completion.
        let s = stream::unfold(chunks.into_iter(), |mut it| async move {
            let next = it.next()?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            Some((next, it))
        });
        Ok(Box::pin(s))
    }
}

/// Tool executor exposing a fixed catalog of tool names.
struct CatalogToolExecutor {
    names: Vec<String>,
}

#[async_trait]
impl bamboo_agent_core::tools::ToolExecutor for CatalogToolExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        Err(ToolError::NotFound("noop".to_string()))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.names
            .iter()
            .map(|name| ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: name.clone(),
                    description: String::new(),
                    parameters: serde_json::json!({}),
                },
            })
            .collect()
    }
}

/// Test-only child runner that drives the child loop in-process. The
/// production in-process spawn path was removed (sub-agents always run as
/// actors), but `run_child_spawn`'s orchestration — event ordering, watchdog,
/// completion/status mapping, persistence — is runner-agnostic. This stand-in
/// keeps that orchestration under fast, network-free unit test without spinning
/// up a real actor worker subprocess.
struct InProcessTestRunner {
    agent: Arc<crate::Agent>,
    tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
}

#[async_trait]
impl ExternalChildRunner for InProcessTestRunner {
    async fn should_handle(&self, _session: &Session) -> bool {
        true
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        let mut builder =
            crate::runtime::ExecuteRequestBuilder::new(String::new(), event_tx, cancel_token)
                .tools(self.tools.clone())
                .model_roster(crate::runtime::ModelRoster {
                    model: Some(job.model.clone()),
                    provider_name: None,
                    provider_type: None,
                    fast: None,
                    background: None,
                    summarization: None,
                });
        if let Some(disabled) = &job.disabled_tools {
            builder = builder.disabled_tools(disabled.iter().cloned().collect());
        }
        self.agent.execute(session, builder.build()).await
    }
}

struct Harness {
    ctx: SpawnContext,
    storage: Arc<dyn Storage>,
    parent_session_id: String,
    child_session_id: String,
    parent_rx: broadcast::Receiver<AgentEvent>,
    parent_tx: broadcast::Sender<AgentEvent>,
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

async fn build_harness(
    provider: Arc<dyn LLMProvider>,
    tool_names: Vec<String>,
    child_metadata: &[(&str, &str)],
) -> Harness {
    let home = temp_dir("bamboo-sdk-test");
    tokio::fs::create_dir_all(&home).await.unwrap();

    let session_store = Arc::new(
        bamboo_storage::SessionStoreV2::new(home.clone())
            .await
            .unwrap(),
    );
    let storage_dir = home.join("storage");
    tokio::fs::create_dir_all(&storage_dir).await.unwrap();
    let jsonl = bamboo_storage::JsonlStorage::new(&storage_dir);
    jsonl.init().await.unwrap();
    let storage: Arc<dyn Storage> = Arc::new(jsonl);

    let metrics_storage = Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
        home.join("metrics.db"),
    ));
    let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

    let sessions_cache: crate::SessionCache = Arc::new(dashmap::DashMap::new());
    let agent_runners = Arc::new(RwLock::new(HashMap::new()));
    let session_event_senders = Arc::new(RwLock::new(HashMap::<
        String,
        broadcast::Sender<AgentEvent>,
    >::new()));

    let parent_session_id = "root-session".to_string();
    let child_session_id = "child-session".to_string();
    let (parent_tx, parent_rx) = broadcast::channel(1000);
    {
        let mut senders = session_event_senders.write().await;
        senders.insert(parent_session_id.clone(), parent_tx.clone());
    }

    let mut parent = Session::new(parent_session_id.clone(), "gpt-5");
    parent.title = "Root".to_string();
    storage.save_session(&parent).await.unwrap();

    let mut child = Session::new_child(
        child_session_id.clone(),
        parent_session_id.clone(),
        "gpt-5",
        "Child session",
    );
    child
        .metadata
        .insert("last_run_status".to_string(), "queued".to_string());
    for (k, v) in child_metadata {
        child.metadata.insert(k.to_string(), v.to_string());
    }
    child.add_message(Message::system("child system"));
    child.add_message(Message::user("do the task"));
    storage.save_session(&child).await.unwrap();

    let agent = Arc::new(
        crate::Agent::builder()
            .storage(storage.clone())
            .persistence(Arc::new(bamboo_storage::LockedSessionStore::new(
                storage.clone(),
            )))
            .attachment_reader(session_store.clone())
            .skill_manager(Arc::new(SkillManager::new()))
            .metrics_collector(metrics_collector)
            .config(Arc::new(RwLock::new(bamboo_llm::Config::default())))
            .provider(provider)
            .default_tools(Arc::new(CatalogToolExecutor {
                names: tool_names.clone(),
            }))
            .build()
            .expect("test agent should build"),
    );

    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(CatalogToolExecutor { names: tool_names });

    let ctx = SpawnContext {
        agent: agent.clone(),
        tools: tools.clone(),
        sessions_cache,
        agent_runners,
        session_event_senders,
        external_child_runner: Arc::new(InProcessTestRunner { agent, tools }),
        provider_router: None,
        app_data_dir: None,
        completion_handler: None,
        account_feed_inbox: None,
    };

    Harness {
        ctx,
        storage,
        parent_session_id,
        child_session_id,
        parent_rx,
        parent_tx,
    }
}

/// Collect parent events until a `SubAgentCompleted` is observed (or timeout).
async fn collect_until_completed(rx: &mut broadcast::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    collect_until_completed_with_budget(rx, Duration::from_secs(15)).await
}

/// Like [`collect_until_completed`] but with a caller-supplied budget. The
/// watchdog timeout path uses wall-clock time, which can stretch under heavy
/// parallel test load, so the timeout test grants a wider budget.
async fn collect_until_completed_with_budget(
    rx: &mut broadcast::Receiver<AgentEvent>,
    budget: Duration,
) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for SubAgentCompleted; saw: {events:?}");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(event)) => {
                let done = matches!(event, AgentEvent::SubAgentCompleted { .. });
                events.push(event);
                if done {
                    return events;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                panic!("parent channel closed before completion; saw: {events:?}")
            }
            Err(_) => panic!("timed out waiting for SubAgentCompleted; saw: {events:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// S-T2.1 — run_child_spawn integration: ordering + persisted status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_1_run_child_spawn_emits_started_event_completed_in_order() {
    let mut harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;

    // The adapter emits SubAgentStarted before enqueue; simulate that here so the
    // full ordering can be asserted.
    harness
        .parent_tx
        .send(AgentEvent::SubAgentStarted {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            title: Some("Child session".to_string()),
        })
        .unwrap();

    run_child_spawn(
        harness.ctx.clone(),
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    let events = collect_until_completed(&mut harness.parent_rx).await;

    let started_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentStarted { .. }))
        .expect("SubAgentStarted present");
    let completed_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentCompleted { .. }))
        .expect("SubAgentCompleted present");
    let event_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentEvent { .. }));

    assert!(
        started_idx < completed_idx,
        "Started must precede Completed: {events:?}"
    );
    if let Some(event_idx) = event_idx {
        assert!(
            started_idx < event_idx,
            "Started must precede SubAgentEvent"
        );
        assert!(
            event_idx < completed_idx,
            "SubAgentEvent must precede Completed"
        );
    }

    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, .. } => {
            assert_eq!(status, "completed", "child should finish completed");
        }
        other => panic!("last event must be SubAgentCompleted, got {other:?}"),
    }

    // Persisted child status reflects completion.
    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted
            .metadata
            .get("last_run_status")
            .map(String::as_str),
        Some("completed")
    );
}

// ---------------------------------------------------------------------------
// S-T2.2 — Sub-agents are full agents: ChildRunner never trims tools.
//
// There are no per-role tool policies anymore; the runner always emits
// `disabled_tools: None` regardless of the live tool catalog.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_2_run_child_never_disables_tools() {
    let tool_names = vec![
        "Read".to_string(),
        "Grep".to_string(),
        "Edit".to_string(),
        "Write".to_string(),
    ];
    let harness = build_harness(Arc::new(CompletedProvider), tool_names, &[]).await;
    let runner = ChildRunner::new(harness.ctx.clone());

    let input = RunChildInput {
        child_session_id: harness.child_session_id.clone(),
        parent_session_id: harness.parent_session_id.clone(),
        model: "gpt-5".to_string(),
    };
    let job = runner.build_job(&input);
    assert!(
        job.disabled_tools.is_none(),
        "sub-agents are full agents; the runner must never trim tools"
    );
}

// ---------------------------------------------------------------------------
// S-T2.4 — model override honored over the persisted session model
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_4_run_child_model_override_is_persisted() {
    let harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;
    let runner = ChildRunner::new(harness.ctx.clone());

    // Child session was persisted with model "gpt-5"; run with a different model.
    let override_model = "claude-3-7-sonnet";
    let mut parent_rx = harness.parent_rx.resubscribe();
    runner
        .run_child(RunChildInput {
            child_session_id: harness.child_session_id.clone(),
            parent_session_id: harness.parent_session_id.clone(),
            model: override_model.to_string(),
        })
        .await
        .unwrap();

    let _ = collect_until_completed(&mut parent_rx).await;

    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.model, override_model,
        "child session model must reflect the runner override"
    );
}

// ---------------------------------------------------------------------------
// S-T2.5 — watchdog timeout → SubAgentCompleted status=timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_5_watchdog_timeout_completes_with_timeout_status() {
    // Tight watchdog: 1s total cap, checked every 1s.
    let harness = build_harness(
        Arc::new(HangingProvider),
        Vec::new(),
        // The watchdog skips its immediate tick, so the first real check lands at
        // `check_interval_secs` (1s). By then `total_secs` already meets the 1s cap,
        // so the timeout fires on that first check — well before the slow stream
        // (first chunk at +2s) could finish, making the outcome deterministic.
        &[
            ("child_watchdog.max_total_secs", "1"),
            ("child_watchdog.max_idle_secs", "1"),
            ("child_watchdog.check_interval_secs", "1"),
        ],
    )
    .await;

    let mut parent_rx = harness.parent_rx.resubscribe();
    run_child_spawn(
        harness.ctx.clone(),
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    let events = collect_until_completed_with_budget(&mut parent_rx, Duration::from_secs(45)).await;
    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, .. } => {
            assert_eq!(status, "timeout", "watchdog must yield timeout status");
        }
        other => panic!("expected SubAgentCompleted, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Issue #546 row 1 — a panic in the child's execution task must publish a
// synthetic error completion instead of silently stranding the parent.
// ---------------------------------------------------------------------------

/// External runner double that panics instead of executing, simulating an
/// unforeseen panic anywhere in the child's execution path.
struct PanickingRunner;

#[async_trait]
impl ExternalChildRunner for PanickingRunner {
    async fn should_handle(&self, _session: &Session) -> bool {
        true
    }

    async fn execute_external_child(
        &self,
        _session: &mut Session,
        _job: &SpawnJob,
        _event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        _cancel_token: tokio_util::sync::CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        panic!("simulated child execution panic (issue #546 row 1 test)");
    }
}

#[tokio::test]
async fn s_t546_1_child_execution_panic_publishes_synthetic_error_completion() {
    let harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;
    let mut parent_rx = harness.parent_rx.resubscribe();
    // Swap in a runner that panics instead of executing — everything else
    // (storage, sessions cache, runner registry) is the real harness.
    let ctx = SpawnContext {
        external_child_runner: Arc::new(PanickingRunner),
        ..harness.ctx.clone()
    };

    run_child_spawn(
        ctx,
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    // Before the fix this event never arrived — the panic silently dropped
    // the `JoinHandle` and the parent waited forever.
    let events = collect_until_completed(&mut parent_rx).await;
    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, error, .. } => {
            assert_eq!(
                status, "error",
                "a panicked child execution task must publish a synthetic error completion"
            );
            assert!(
                error.as_deref().unwrap_or_default().contains("panic"),
                "error should reference the panic: {error:?}"
            );
        }
        other => panic!("expected SubAgentCompleted, got {other:?}"),
    }

    // The child's own storage record must also reflect the panic — not stay
    // "running" forever (which would also defeat the boot-reconciliation /
    // watchdog dead-child staleness signal for issue #546 Part B).
    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted
            .metadata
            .get("last_run_status")
            .map(String::as_str),
        Some("error")
    );
}

// ---------------------------------------------------------------------------
// Issue #546 row 3 — a panic inside the coordinator's `on_child_completed`
// must not propagate and must not prevent the broadcast `SubAgentCompleted`
// event (which is sent before the handler runs).
// ---------------------------------------------------------------------------

struct PanickingCompletionHandler;

#[async_trait]
impl ChildCompletionHandler for PanickingCompletionHandler {
    async fn on_child_completed(&self, _completion: ChildCompletion) {
        panic!("simulated coordinator panic (issue #546 row 3 test)");
    }
}

#[tokio::test]
async fn s_t546_3_completion_handler_panic_does_not_block_broadcast_or_hang() {
    let harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;
    let mut parent_rx = harness.parent_rx.resubscribe();
    let ctx = SpawnContext {
        completion_handler: Some(Arc::new(PanickingCompletionHandler)),
        ..harness.ctx.clone()
    };

    // Must complete without hanging or crashing the process — a panicking
    // handler is isolated behind its own monitored `JoinHandle`
    // (`publish_child_completion`). `collect_until_completed`'s own internal
    // timeout would fail this test if the panic somehow blocked delivery.
    run_child_spawn(
        ctx,
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    let events = collect_until_completed(&mut parent_rx).await;
    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, .. } => {
            assert_eq!(
                status, "completed",
                "the child's own real completion status is unaffected by a downstream \
                 handler panic"
            );
        }
        other => panic!("expected SubAgentCompleted, got {other:?}"),
    }
}
