use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, OnceLock};

use async_trait::async_trait;
use bamboo_agent_core::composition::CompositionExecutor;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    execute_tool_call, handle_tool_result_with_agentic_support, AgenticToolResult, FunctionCall,
    ToolCall, ToolError, ToolExecutor, ToolHandlingOutcome, ToolRegistry, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::RuntimeSessionPersistence;
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_tools::BuiltinToolExecutor;
use futures::stream;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use super::config::AgentLoopConfig;

/// Acquire a process-wide lock to serialize tests that mutate environment variables.
pub(crate) fn env_cache_lock_acquire() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn make_tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

#[test]
fn agent_loop_config_default() {
    let config = AgentLoopConfig::default();
    assert_eq!(config.max_rounds, 200);
    assert!(config.system_prompt.is_none());
    assert!(config.additional_tool_schemas.is_empty());
    assert!(config.tool_registry.is_empty());
    assert!(config.skill_manager.is_none());
    assert!(config.selected_skill_ids.is_none());
    assert!(config.disabled_tools.is_empty());
    assert!(!config.skip_initial_user_message);
    // Issue #221: no budget configured anywhere means unlimited, matching
    // every other resource knob's opt-in-only default.
    assert_eq!(config.run_budget, bamboo_config::RunBudgetConfig::default());
}

/// Issue #221 plumb-through (engine hop): `ExecuteRequestBuilder::run_budget`
/// is the exact setter the server's `agent_spawn::build_execute_request`
/// calls when the HTTP request carried an override — verify it lands
/// unmodified on the built `ExecuteRequest`, ready for
/// `AgentRuntime::execute` to merge against the config-level default.
#[test]
fn execute_request_builder_carries_run_budget_override_through_to_the_request() {
    use super::runtime::ExecuteRequestBuilder;
    use tokio_util::sync::CancellationToken;

    let (tx, _rx) = mpsc::channel(1);
    let override_budget = bamboo_config::RunBudgetConfig {
        max_total_tokens: Some(42_000),
        max_tool_calls: Some(7),
        max_subagents: None,
    };

    let request = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new())
        .run_budget(override_budget)
        .build();

    assert_eq!(request.run_budget, Some(override_budget));
}

/// Building WITHOUT `.run_budget(..)` leaves it `None` — `AgentRuntime::execute`
/// must read that as "use the config-level default", not "unlimited".
#[test]
fn execute_request_builder_defaults_run_budget_to_none() {
    use super::runtime::ExecuteRequestBuilder;
    use tokio_util::sync::CancellationToken;

    let (tx, _rx) = mpsc::channel(1);
    let request = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new()).build();

    assert!(request.run_budget.is_none());
}

#[test]
fn skip_initial_message_flag() {
    let config = AgentLoopConfig {
        skip_initial_user_message: true,
        ..Default::default()
    };
    assert!(config.skip_initial_user_message);
}

#[tokio::test]
async fn need_clarification_sends_event() {
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let mut session = Session::new("s1", "test-model");
    let tool_call = make_tool_call("call_parent", "smart_tool", "{}");
    let result = ToolResult {
        success: true,
        result: serde_json::to_string(&AgenticToolResult::NeedClarification {
            question: "Which file should I inspect?".to_string(),
            options: Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()]),
        })
        .unwrap(),
        display_preference: None,
        images: Vec::new(),
    };

    let outcome = handle_tool_result_with_agentic_support(
        &result,
        &tool_call,
        &event_tx,
        &mut session,
        tools.as_ref(),
        bamboo_agent_core::tools::ToolExecutionSessionFlags::default(),
        None,
    )
    .await;

    assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);
    let pending = session
        .pending_question
        .as_ref()
        .expect("agentic clarification should persist pending question");
    assert_eq!(pending.tool_call_id, "call_parent");
    assert_eq!(pending.tool_name, "smart_tool");
    assert_eq!(pending.question, "Which file should I inspect?");
    assert_eq!(
        pending.options,
        vec!["src/main.rs".to_string(), "src/lib.rs".to_string()]
    );
    assert_eq!(
        session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("awaiting_clarification")
    );

    let event = event_rx.recv().await.expect("missing clarification event");
    match event {
        AgentEvent::NeedClarification {
            question,
            options,
            tool_call_id,
            tool_name,
            ..
        } => {
            assert_eq!(question, "Which file should I inspect?");
            assert_eq!(
                options,
                Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()])
            );
            assert_eq!(tool_call_id.as_deref(), Some("call_parent"));
            assert_eq!(tool_name.as_deref(), Some("smart_tool"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn need_more_actions_executes_sub_actions() {
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let mut session = Session::new("s2", "test-model");
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), "workspace context\n")
        .await
        .unwrap();
    let sub_action = make_tool_call(
        "call_sub",
        "Read",
        &serde_json::json!({
            "file_path": file.path()
        })
        .to_string(),
    );
    let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
    let result = ToolResult {
        success: true,
        result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
            actions: vec![sub_action.clone()],
            reason: "Need workspace context".to_string(),
        })
        .unwrap(),
        display_preference: None,
        images: Vec::new(),
    };

    let outcome = handle_tool_result_with_agentic_support(
        &result,
        &parent_call,
        &event_tx,
        &mut session,
        tools.as_ref(),
        bamboo_agent_core::tools::ToolExecutionSessionFlags::default(),
        None,
    )
    .await;

    assert_eq!(outcome, ToolHandlingOutcome::Continue);
    assert!(session.messages.iter().any(|message| {
        message.tool_call_id.as_deref() == Some("call_sub") && !message.content.is_empty()
    }));

    let mut saw_sub_start = false;
    let mut saw_sub_complete = false;

    while let Ok(event) = event_rx.try_recv() {
        match event {
            AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == "call_sub" => {
                saw_sub_start = true;
            }
            AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "call_sub" => {
                saw_sub_complete = true;
            }
            _ => {}
        }
    }

    assert!(saw_sub_start);
    assert!(saw_sub_complete);
}

#[tokio::test]
async fn execute_tool_call_falls_back_when_composition_misses_tool() {
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let composition_executor = Arc::new(CompositionExecutor::new(Arc::new(ToolRegistry::new())));
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), "fallback\n").await.unwrap();
    let tool_call = make_tool_call(
        "call_sub",
        "Read",
        &serde_json::json!({
            "file_path": file.path(),
        })
        .to_string(),
    );

    let result = execute_tool_call(&tool_call, tools.as_ref(), Some(composition_executor))
        .await
        .expect("fallback execution should succeed");

    assert!(result.success);
    assert!(!result.result.is_empty());
}

struct CompletedTranscriptProvider;

#[async_trait]
impl LLMProvider for CompletedTranscriptProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("durable final answer".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct PartialThenTerminalErrorProvider;

#[async_trait]
impl LLMProvider for PartialThenTerminalErrorProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        // An early indexed argument fragment has neither provider id nor tool
        // name yet.  Finalizing ToolCallAccumulator would drop it; the
        // interrupted-output snapshot must retain it verbatim without inventing
        // an id or exposing it as an executable Message::tool_calls entry.
        let partial_call = ToolCall {
            id: String::new(),
            tool_type: String::new(),
            function: FunctionCall {
                name: String::new(),
                arguments: r#"{"file_path""#.to_string(),
            },
        };
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("visible before failure".to_string())),
            Ok(LLMChunk::ToolCallsIndexed(vec![(0, partial_call)])),
            Err(LLMError::Api(
                "authentication error: intentional terminal stream failure".to_string(),
            )),
        ])))
    }
}

struct NeverCalledTranscriptProvider;

#[async_trait]
impl LLMProvider for NeverCalledTranscriptProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("pre-cancelled direct execute must not call the provider")
    }
}

struct ImmediateTerminalErrorProvider;

#[async_trait]
impl LLMProvider for ImmediateTerminalErrorProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        Err(LLMError::Api(
            "authentication error: original execution failure".to_string(),
        ))
    }
}

struct MidLoopThenPartialErrorProvider {
    calls: AtomicUsize,
}

struct RetryBeforeStreamThenSuccessProvider {
    calls: AtomicUsize,
}

struct EmptyAssistantResponseProvider {
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for EmptyAssistantResponseProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(call, 0, "an empty assistant response must not be retried");
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::ResponseId("resp_empty_740".to_string())),
            Ok(LLMChunk::Token(" \n ".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct AlwaysTransientProvider {
    calls: AtomicUsize,
}

struct BootstrapStallThenSuccessProvider {
    calls: AtomicUsize,
}

struct TransportStallThenSuccessProvider {
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for AlwaysTransientProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(LLMError::Api(
            "temporary provider dispatch failure".to_string(),
        ))
    }
}

#[async_trait]
impl LLMProvider for BootstrapStallThenSuccessProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => std::future::pending::<Result<LLMStream, LLMError>>().await,
            1 => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("bootstrap retry recovered".to_string())),
                Ok(LLMChunk::Done),
            ]))),
            call => panic!("unexpected LLM call {call}"),
        }
    }
}

#[async_trait]
impl LLMProvider for TransportStallThenSuccessProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(Box::pin(stream::pending())),
            1 => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("transport retry recovered".to_string())),
                Ok(LLMChunk::Done),
            ]))),
            call => panic!("unexpected LLM call {call}"),
        }
    }
}

#[async_trait]
impl LLMProvider for RetryBeforeStreamThenSuccessProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Err(LLMError::Api(
                "temporary provider dispatch failure".to_string(),
            )),
            1 => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("retry recovered".to_string())),
                Ok(LLMChunk::Done),
            ]))),
            call => panic!("unexpected LLM call {call}"),
        }
    }
}

#[async_trait]
impl LLMProvider for MidLoopThenPartialErrorProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![make_tool_call(
                    "mid-loop-call",
                    "Read",
                    r#"{"file_path":"demo"}"#,
                )])),
                Ok(LLMChunk::Done),
            ]))),
            1 => {
                let fragment = ToolCall {
                    id: String::new(),
                    tool_type: String::new(),
                    function: FunctionCall {
                        name: String::new(),
                        arguments: "{\"next".to_string(),
                    },
                };
                Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token("second-round partial".to_string())),
                    Ok(LLMChunk::ToolCallsIndexed(vec![(0, fragment)])),
                    Err(LLMError::Api(
                        "authentication error: terminal second-round failure".to_string(),
                    )),
                ])))
            }
            call => panic!("unexpected LLM call {call}"),
        }
    }
}

struct SuccessfulReadToolExecutor;

#[async_trait]
impl ToolExecutor for SuccessfulReadToolExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        assert_eq!(call.id, "mid-loop-call");
        Ok(ToolResult {
            success: true,
            result: "mid-loop tool result".to_string(),
            display_preference: None,
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".to_string(),
            function: bamboo_agent_core::tools::FunctionSchema {
                name: "Read".to_string(),
                description: "test read".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            },
        }]
    }
}

struct FailingCheckpointPersistence {
    attempts: AtomicUsize,
}

#[async_trait]
impl RuntimeSessionPersistence for FailingCheckpointPersistence {
    async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
        Ok(())
    }

    async fn checkpoint_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("intentional checkpoint failure"))
    }
}

async fn build_direct_execute_agent(
    provider: Arc<dyn LLMProvider>,
    persistence_override: Option<Arc<dyn RuntimeSessionPersistence>>,
    tools_override: Option<Arc<dyn ToolExecutor>>,
) -> (tempfile::TempDir, crate::runtime::Agent, Arc<dyn Storage>) {
    build_direct_execute_agent_with_config(
        provider,
        persistence_override,
        tools_override,
        bamboo_llm::Config::default(),
    )
    .await
}

async fn build_direct_execute_agent_with_config(
    provider: Arc<dyn LLMProvider>,
    persistence_override: Option<Arc<dyn RuntimeSessionPersistence>>,
    tools_override: Option<Arc<dyn ToolExecutor>>,
    config: bamboo_llm::Config,
) -> (tempfile::TempDir, crate::runtime::Agent, Arc<dyn Storage>) {
    let temp = tempfile::tempdir().expect("tempdir");
    let session_store = Arc::new(
        bamboo_storage::SessionStoreV2::new(temp.path().join("sessions"))
            .await
            .expect("session store"),
    );
    let storage: Arc<dyn Storage> = session_store.clone();
    let persistence = persistence_override.unwrap_or_else(|| {
        Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()))
            as Arc<dyn RuntimeSessionPersistence>
    });
    let metrics = bamboo_metrics::MetricsCollector::spawn(
        Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
            temp.path().join("metrics.db"),
        )),
        7,
    );
    let agent = crate::runtime::Agent::builder()
        .storage(storage.clone())
        .persistence(persistence)
        .attachment_reader(session_store)
        .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
        .metrics_collector(metrics)
        .config(Arc::new(RwLock::new(config)))
        .provider(provider)
        .default_tools(tools_override.unwrap_or_else(|| Arc::new(BuiltinToolExecutor::new())))
        .build()
        .expect("direct execute agent");
    (temp, agent, storage)
}

fn direct_request(
    event_tx: mpsc::Sender<AgentEvent>,
    cancel_token: CancellationToken,
) -> crate::runtime::ExecuteRequest {
    crate::runtime::ExecuteRequestBuilder::new("", event_tx, cancel_token)
        .model("test-model")
        .build()
}

#[tokio::test]
async fn direct_execute_checkpoints_normal_completion_without_task_context() {
    let (_temp, agent, storage) =
        build_direct_execute_agent(Arc::new(CompletedTranscriptProvider), None, None).await;
    let mut session = Session::new("direct-normal-checkpoint", "test-model");
    session.add_message(Message::user("finish normally"));
    storage.save_session(&session).await.unwrap();

    let (event_tx, _event_rx) = mpsc::channel(32);
    agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect("normal execute");

    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    assert!(saved
        .messages
        .iter()
        .any(|message| message.content == "durable final answer"));
    assert!(saved.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
}

#[tokio::test]
async fn retryable_pre_stream_error_does_not_delete_existing_interrupted_tail() {
    let provider = Arc::new(RetryBeforeStreamThenSuccessProvider {
        calls: AtomicUsize::new(0),
    });
    let (_temp, agent, storage) = build_direct_execute_agent(provider.clone(), None, None).await;
    let mut session = Session::new("retry-preserves-old-interrupted", "test-model");
    session.add_message(Message::user("base"));
    let mut old_interrupted = Message::assistant("old interrupted output", None);
    old_interrupted.id = "old-interrupted".to_string();
    old_interrupted.metadata = Some(serde_json::json!({
        "runtime_kind": "interrupted_assistant_output",
        "interrupted": true,
        "interruption_kind": "llm_error",
    }));
    session.add_message(old_interrupted);
    storage.save_session(&session).await.unwrap();
    let (event_tx, _event_rx) = mpsc::channel(64);

    agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect("retry should recover");

    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    assert!(saved
        .messages
        .iter()
        .any(|message| message.id == "old-interrupted"));
    assert!(saved
        .messages
        .iter()
        .any(|message| message.content == "retry recovered"));
}

#[tokio::test]
async fn direct_execute_surfaces_and_checkpoints_empty_response_without_retry() {
    let provider = Arc::new(EmptyAssistantResponseProvider {
        calls: AtomicUsize::new(0),
    });
    let (_temp, agent, storage) = build_direct_execute_agent(provider.clone(), None, None).await;
    let mut session = Session::new("direct-empty-response", "test-model");
    session.add_message(Message::user("durable base"));
    storage.save_session(&session).await.unwrap();
    session.add_message(Message::user("checkpoint this turn"));

    let (event_tx, mut event_rx) = mpsc::channel(64);
    let result = agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await;

    assert!(matches!(
        &result,
        Err(bamboo_agent_core::AgentError::EmptyAssistantResponse {
            response_id: Some(response_id)
        }) if response_id == "resp_empty_740"
    ));
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        1,
        "the real provider pipeline must make exactly one billable call"
    );

    let terminal_event =
        crate::runtime::execution::agent_spawn::terminal_error_event_for_result(&result)
            .expect("terminal empty response must remain visible to event consumers");
    match terminal_event {
        AgentEvent::Error { message } => {
            assert!(message.contains("Empty assistant response from LLM"));
            assert!(message.contains("resp_empty_740"));
        }
        other => panic!("unexpected terminal event: {other:?}"),
    }

    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    assert!(
        saved
            .messages
            .iter()
            .any(|message| message.content == "checkpoint this turn"),
        "the shared execute boundary must checkpoint the terminal turn"
    );
    assert!(!saved.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
    }
}

#[tokio::test]
async fn direct_execute_retries_transient_llm_errors_to_existing_limit() {
    let provider = Arc::new(AlwaysTransientProvider {
        calls: AtomicUsize::new(0),
    });
    let (_temp, agent, _storage) = build_direct_execute_agent(provider.clone(), None, None).await;
    let mut session = Session::new("direct-transient-retry-limit", "test-model");
    session.add_message(Message::user("retry transient failures"));
    let (event_tx, _event_rx) = mpsc::channel(64);

    let error = agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect_err("an always-transient provider must exhaust the retry limit");

    assert!(matches!(
        error,
        bamboo_agent_core::AgentError::LLM(message)
            if message.contains("temporary provider dispatch failure")
    ));
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        3,
        "genuine transient LLM failures keep the existing three-attempt limit"
    );
}

#[tokio::test]
async fn direct_execute_retries_a_stalled_stream_bootstrap() {
    let provider = Arc::new(BootstrapStallThenSuccessProvider {
        calls: AtomicUsize::new(0),
    });
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, _storage) =
        build_direct_execute_agent_with_config(provider.clone(), None, None, config).await;
    let mut session = Session::new("direct-bootstrap-timeout-retry", "test-model");
    session.add_message(Message::user("retry a request that never returns headers"));
    let (event_tx, _event_rx) = mpsc::channel(64);

    agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect("retry-safe bootstrap timeout should recover");

    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        2,
        "the stalled bootstrap should be cancelled and replayed once"
    );
    assert!(session
        .messages
        .iter()
        .any(|message| message.content == "bootstrap retry recovered"));
}

#[tokio::test]
async fn direct_execute_retries_transport_idle_before_semantic_output() {
    let provider = Arc::new(TransportStallThenSuccessProvider {
        calls: AtomicUsize::new(0),
    });
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, _storage) =
        build_direct_execute_agent_with_config(provider.clone(), None, None, config).await;
    let mut session = Session::new("direct-transport-idle-retry", "test-model");
    session.add_message(Message::user("retry a stream with no transport frames"));
    let (event_tx, _event_rx) = mpsc::channel(64);

    agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect("retry-safe transport idle timeout should recover");

    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        2,
        "the silent stream should be discarded and replayed once"
    );
    assert!(session
        .messages
        .iter()
        .any(|message| message.content == "transport retry recovered"));
}

#[tokio::test]
async fn direct_execute_persists_partial_stream_error_without_false_completion_or_tool_replay() {
    let (_temp, agent, storage) =
        build_direct_execute_agent(Arc::new(PartialThenTerminalErrorProvider), None, None).await;
    let mut session = Session::new("direct-partial-error", "test-model");
    session.add_message(Message::user("start streaming"));
    storage.save_session(&session).await.unwrap();

    let (event_tx, mut event_rx) = mpsc::channel(64);
    let error = agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect_err("stream must fail");
    assert!(matches!(error, bamboo_agent_core::AgentError::LLM(_)));

    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    let interrupted = saved
        .messages
        .iter()
        .find(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("runtime_kind"))
                .and_then(serde_json::Value::as_str)
                == Some("interrupted_assistant_output")
        })
        .expect("interrupted partial assistant message");
    assert_eq!(interrupted.content, "visible before failure");
    assert!(
        interrupted.tool_calls.is_none(),
        "partial calls are not executable"
    );
    assert!(interrupted
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("partial_tool_calls"))
        .is_some_and(|calls| calls.is_array()));
    let fragment = interrupted
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("partial_tool_calls"))
        .and_then(serde_json::Value::as_array)
        .and_then(|calls| calls.first())
        .expect("raw partial tool-call fragment");
    assert_eq!(fragment["id"], "");
    assert_eq!(fragment["name"], "");
    assert_eq!(fragment["arguments"], r#"{"file_path""#);
    assert_eq!(fragment["index"], 0);
    assert!(!saved.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
    }
}

#[tokio::test]
async fn direct_execute_cancel_checkpoints_committed_messages_without_false_completion() {
    let (_temp, agent, storage) =
        build_direct_execute_agent(Arc::new(NeverCalledTranscriptProvider), None, None).await;
    let mut durable = Session::new("direct-cancel-checkpoint", "test-model");
    durable.add_message(Message::user("base"));
    storage.save_session(&durable).await.unwrap();

    let mut session = durable;
    let tool_call = make_tool_call("committed-call", "Read", r#"{"file_path":"x"}"#);
    session.add_message(Message::assistant("", Some(vec![tool_call])));
    session.add_message(Message::tool_result("committed-call", "committed result"));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (event_tx, mut event_rx) = mpsc::channel(32);

    let error = agent
        .execute(&mut session, direct_request(event_tx, cancel))
        .await
        .expect_err("cancelled execute");
    assert!(matches!(error, bamboo_agent_core::AgentError::Cancelled));

    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    assert!(saved
        .messages
        .iter()
        .any(|message| message.content == "committed result"));
    assert!(!saved.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
    }
}

#[tokio::test]
async fn direct_execute_mid_loop_error_persists_tool_round_and_interrupted_next_round() {
    let provider = Arc::new(MidLoopThenPartialErrorProvider {
        calls: AtomicUsize::new(0),
    });
    let (_temp, agent, storage) = build_direct_execute_agent(
        provider.clone(),
        None,
        Some(Arc::new(SuccessfulReadToolExecutor)),
    )
    .await;
    let mut session = Session::new("direct-mid-loop-error", "test-model");
    session.add_message(Message::user("run a tool then fail"));
    storage.save_session(&session).await.unwrap();
    let (event_tx, mut event_rx) = mpsc::channel(128);

    let error = agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect_err("second round must fail");
    assert!(matches!(
        error,
        bamboo_agent_core::AgentError::LLM(message)
            if message.contains("terminal second-round failure")
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

    let saved = storage
        .load_session(&session.id)
        .await
        .unwrap()
        .expect("saved session");
    let assistant_tool_call = saved
        .messages
        .iter()
        .find(|message| {
            message
                .tool_calls
                .as_ref()
                .is_some_and(|calls| calls.iter().any(|call| call.id == "mid-loop-call"))
        })
        .expect("first-round assistant tool call");
    assert!(assistant_tool_call.content.is_empty());
    assert!(saved.messages.iter().any(|message| {
        message.tool_call_id.as_deref() == Some("mid-loop-call")
            && message.content == "mid-loop tool result"
    }));
    let interrupted = saved
        .messages
        .iter()
        .find(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("runtime_kind"))
                .and_then(serde_json::Value::as_str)
                == Some("interrupted_assistant_output")
        })
        .expect("second-round interrupted output");
    assert_eq!(interrupted.content, "second-round partial");
    assert!(interrupted.tool_calls.is_none());
    let fragment = interrupted
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("partial_tool_calls"))
        .and_then(serde_json::Value::as_array)
        .and_then(|calls| calls.first())
        .expect("second-round raw tool fragment");
    assert_eq!(fragment["id"], "");
    assert_eq!(fragment["name"], "");
    assert_eq!(fragment["arguments"], "{\"next");
    assert_eq!(fragment["index"], 0);
    assert!(!saved.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
    }
}

#[tokio::test]
async fn checkpoint_failure_does_not_mask_original_execution_error() {
    let persistence = Arc::new(FailingCheckpointPersistence {
        attempts: AtomicUsize::new(0),
    });
    let (_temp, agent, _storage) = build_direct_execute_agent(
        Arc::new(ImmediateTerminalErrorProvider),
        Some(persistence.clone()),
        None,
    )
    .await;
    let mut session = Session::new("direct-checkpoint-failure", "test-model");
    session.add_message(Message::user("fail"));
    let (event_tx, _event_rx) = mpsc::channel(32);

    let error = agent
        .execute(
            &mut session,
            direct_request(event_tx, CancellationToken::new()),
        )
        .await
        .expect_err("provider error");

    assert!(matches!(
        error,
        bamboo_agent_core::AgentError::LLM(message)
            if message.contains("original execution failure")
    ));
    assert_eq!(persistence.attempts.load(Ordering::SeqCst), 1);
}
