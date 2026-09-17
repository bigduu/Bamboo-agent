use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{
    build_compression_context_blocks, build_retrieval_window_accounting_frame,
    effective_context_pressure_strategy, emit_context_pressure_notification,
    enforce_model_context_ledger_retention, mark_manual_archive_request_consumed,
    maybe_apply_host_context_compression, pending_manual_archive_request, prepare_round_context,
    surface_manual_archive_rejection, LAST_MANUAL_ARCHIVE_OCCURRENCE_KEY, LAST_PRESSURE_LEVEL_KEY,
};
use crate::runtime::config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use bamboo_agent_core::{
    AgentEvent, AgentHook, CompressionTriggerType, Message, Role, Session, TokenBudgetUsage,
};
use bamboo_compression::{
    build_retrieval_window_candidate_plan_with_token_accounting,
    effective_retrieval_window_target_tokens, BudgetStrategy, RetrievalWindowPlanError,
    RetrievalWindowPolicy, TiktokenTokenCounter, TokenBudget, TokenCounter,
};
use bamboo_config::{
    ContextManagementConfig, ContextManagementFallbackStrategy, ContextManagementStrategy,
    RetrievalWindowContextConfig,
};
use bamboo_domain::ResponseOccurrence;
use bamboo_domain::{
    provider_transcript_boundary_sha256, AgentHookPoint, CapabilityLoadingMode, ContextBlockType,
    HookPayload, HookResult, ModelContextEvent, ModelContextEventKind, ModelContextResetReason,
    ModelContextState, ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor,
    ProviderTranscriptItem, ProviderTranscriptOrigin, ProviderTranscriptResetReason,
    RetrievalWindowCheckpointOutcome, RuntimeSessionPersistence, TaskItem, TaskItemStatus,
    TaskList,
};
use bamboo_llm::models::{ContentPart, ImageUrl};
use bamboo_llm::provider::{
    LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo, ProviderVisibleToolFootprint,
    ProviderVisibleToolSegment, ProviderVisibleToolSegmentKind,
};
use bamboo_llm::{LLMChunk, LLMError};
use futures::stream;
use tokio::sync::mpsc;

fn isolate_prompt_safe_env_cache() -> MutexGuard<'static, ()> {
    let guard = crate::runtime::tests::env_cache_lock_acquire();
    let empty_data_dir = tempfile::tempdir().expect("temp dir for empty config");
    let _ = bamboo_config::Config::from_data_dir(Some(empty_data_dir.path().to_path_buf()));
    guard
}

/// A no-op LLM provider for tests that returns an empty stream.
struct NoopLlmProvider;

#[async_trait::async_trait]
impl LLMProvider for NoopLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        Ok(Box::pin(futures::stream::empty()))
    }
}

fn noop_llm() -> Arc<dyn LLMProvider> {
    Arc::new(NoopLlmProvider)
}

#[derive(Clone)]
struct RetrievalCheckpointPersistence {
    checkpoints: Arc<Mutex<Vec<Session>>>,
    fail: bool,
}

impl RetrievalCheckpointPersistence {
    fn succeeding() -> (Arc<dyn RuntimeSessionPersistence>, Arc<Mutex<Vec<Session>>>) {
        let checkpoints = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                checkpoints: Arc::clone(&checkpoints),
                fail: false,
            }),
            checkpoints,
        )
    }

    fn failing() -> (Arc<dyn RuntimeSessionPersistence>, Arc<Mutex<Vec<Session>>>) {
        let checkpoints = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                checkpoints: Arc::clone(&checkpoints),
                fail: true,
            }),
            checkpoints,
        )
    }
}

#[async_trait::async_trait]
impl RuntimeSessionPersistence for RetrievalCheckpointPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        self.checkpoint_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        if self.fail {
            return Err(io::Error::other("injected retrieval checkpoint failure"));
        }
        self.checkpoints
            .lock()
            .expect("checkpoint list lock should not be poisoned")
            .push(session.clone());
        Ok(())
    }

    async fn checkpoint_retrieval_window(
        &self,
        _expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        if self.fail {
            return Err(io::Error::other("injected retrieval checkpoint failure"));
        }
        self.checkpoints
            .lock()
            .expect("checkpoint list lock should not be poisoned")
            .push(staged.clone());
        Ok(RetrievalWindowCheckpointOutcome::Committed)
    }

    async fn checkpoint_prompt_rewrite(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.checkpoint_retrieval_window(expected_base, staged)
            .await
    }

    async fn checkpoint_manual_archive_rejection(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.checkpoint_retrieval_window(expected_base, staged)
            .await
    }

    async fn checkpoint_manual_archive_consumption(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.checkpoint_retrieval_window(expected_base, staged)
            .await
    }
}

struct DurableBaseCheckingPersistence {
    durable: Arc<Mutex<Session>>,
    runtime_checkpoints: Arc<AtomicUsize>,
    prompt_checkpoints: Arc<AtomicUsize>,
    retrieval_checkpoints: Arc<AtomicUsize>,
    manual_consumption_checkpoints: Arc<AtomicUsize>,
}

struct DurableBaseCheckingFixture {
    persistence: Arc<dyn RuntimeSessionPersistence>,
    durable: Arc<Mutex<Session>>,
    runtime_checkpoints: Arc<AtomicUsize>,
    prompt_checkpoints: Arc<AtomicUsize>,
    retrieval_checkpoints: Arc<AtomicUsize>,
    manual_consumption_checkpoints: Arc<AtomicUsize>,
}

impl DurableBaseCheckingPersistence {
    fn fixture(durable: Session) -> DurableBaseCheckingFixture {
        let durable = Arc::new(Mutex::new(durable));
        let runtime_checkpoints = Arc::new(AtomicUsize::new(0));
        let prompt_checkpoints = Arc::new(AtomicUsize::new(0));
        let retrieval_checkpoints = Arc::new(AtomicUsize::new(0));
        let manual_consumption_checkpoints = Arc::new(AtomicUsize::new(0));
        DurableBaseCheckingFixture {
            persistence: Arc::new(Self {
                durable: Arc::clone(&durable),
                runtime_checkpoints: Arc::clone(&runtime_checkpoints),
                prompt_checkpoints: Arc::clone(&prompt_checkpoints),
                retrieval_checkpoints: Arc::clone(&retrieval_checkpoints),
                manual_consumption_checkpoints: Arc::clone(&manual_consumption_checkpoints),
            }),
            durable,
            runtime_checkpoints,
            prompt_checkpoints,
            retrieval_checkpoints,
            manual_consumption_checkpoints,
        }
    }
}

#[async_trait::async_trait]
impl RuntimeSessionPersistence for DurableBaseCheckingPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        self.checkpoint_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        self.runtime_checkpoints.fetch_add(1, Ordering::SeqCst);
        *self.durable.lock().expect("durable Session lock") = session.clone();
        Ok(())
    }

    async fn checkpoint_retrieval_window(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.retrieval_checkpoints.fetch_add(1, Ordering::SeqCst);
        let mut durable = self.durable.lock().expect("durable Session lock");
        let expected_messages = serde_json::to_vec(&expected_base.messages)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let durable_messages = serde_json::to_vec(&durable.messages)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if expected_messages != durable_messages {
            *staged = durable.clone();
            return Ok(RetrievalWindowCheckpointOutcome::Rebased);
        }
        *durable = staged.clone();
        Ok(RetrievalWindowCheckpointOutcome::Committed)
    }

    async fn checkpoint_prompt_rewrite(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.prompt_checkpoints.fetch_add(1, Ordering::SeqCst);
        let mut durable = self.durable.lock().expect("durable Session lock");
        let expected_messages = serde_json::to_vec(&expected_base.messages)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let durable_messages = serde_json::to_vec(&durable.messages)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if expected_messages != durable_messages {
            *staged = durable.clone();
            return Ok(RetrievalWindowCheckpointOutcome::Rebased);
        }
        *durable = staged.clone();
        Ok(RetrievalWindowCheckpointOutcome::Committed)
    }

    async fn checkpoint_manual_archive_rejection(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.checkpoint_prompt_rewrite(expected_base, staged).await
    }

    async fn checkpoint_manual_archive_consumption(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        self.manual_consumption_checkpoints
            .fetch_add(1, Ordering::SeqCst);
        let mut durable = self.durable.lock().expect("durable Session lock");
        if serde_json::to_vec(expected_base)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            != serde_json::to_vec(&*durable)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        {
            *staged = durable.clone();
            return Ok(RetrievalWindowCheckpointOutcome::Rebased);
        }
        *durable = staged.clone();
        Ok(RetrievalWindowCheckpointOutcome::Committed)
    }
}

struct RebaseOnceRetrievalPersistence {
    calls: Arc<AtomicUsize>,
    checkpoints: Arc<Mutex<Vec<Session>>>,
    first_archive_event_id: Arc<Mutex<Option<String>>>,
}

struct RebaseOnceRetrievalFixture {
    persistence: Arc<dyn RuntimeSessionPersistence>,
    calls: Arc<AtomicUsize>,
    checkpoints: Arc<Mutex<Vec<Session>>>,
    first_archive_event_id: Arc<Mutex<Option<String>>>,
}

impl RebaseOnceRetrievalPersistence {
    fn fixture() -> RebaseOnceRetrievalFixture {
        let calls = Arc::new(AtomicUsize::new(0));
        let checkpoints = Arc::new(Mutex::new(Vec::new()));
        let first_archive_event_id = Arc::new(Mutex::new(None));
        RebaseOnceRetrievalFixture {
            persistence: Arc::new(Self {
                calls: Arc::clone(&calls),
                checkpoints: Arc::clone(&checkpoints),
                first_archive_event_id: Arc::clone(&first_archive_event_id),
            }),
            calls,
            checkpoints,
            first_archive_event_id,
        }
    }
}

#[async_trait::async_trait]
impl RuntimeSessionPersistence for RebaseOnceRetrievalPersistence {
    async fn save_runtime_session(&self, _session: &mut Session) -> io::Result<()> {
        Ok(())
    }

    async fn checkpoint_retrieval_window(
        &self,
        expected_base: &Session,
        staged: &mut Session,
    ) -> io::Result<RetrievalWindowCheckpointOutcome> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            *self
                .first_archive_event_id
                .lock()
                .expect("event id lock should not be poisoned") = staged
                .compression_events
                .last()
                .map(|event| event.id.clone());
            let mut rebased = expected_base.clone();
            let mut concurrent = Message::user("CONCURRENT_DURABLE_SUFFIX");
            concurrent.id = "concurrent-durable-suffix".to_string();
            rebased.add_message(concurrent);
            *staged = rebased;
            return Ok(RetrievalWindowCheckpointOutcome::Rebased);
        }

        self.checkpoints
            .lock()
            .expect("checkpoint list lock should not be poisoned")
            .push(staged.clone());
        Ok(RetrievalWindowCheckpointOutcome::Committed)
    }
}

fn retrieval_history_tool_schema() -> ToolSchema {
    ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "session_history_current".to_string(),
            description: "Search or read exact messages in the current Session".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string" },
                    "query": { "type": "string" }
                },
                "required": ["action"]
            }),
        },
    }
}

fn load_skill_tool_schema() -> ToolSchema {
    ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "load_skill".to_string(),
            description: "Load the explicitly selected workflow".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": { "skill_id": { "type": "string" } },
                "required": ["skill_id"]
            }),
        },
    }
}

fn retrieval_window_session(id: &str) -> Session {
    let mut session = Session::new(id, "test-model");
    session.messages.push(Message::system("retrieval system"));
    for index in 0..10 {
        session.messages.push(Message::user(format!(
            "user-{index} {}",
            "exact historical user evidence ".repeat(350)
        )));
        session.messages.push(Message::assistant(
            format!(
                "assistant-{index} {}",
                "exact historical assistant evidence ".repeat(350)
            ),
            None,
        ));
    }
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    session
}

fn retrieval_window_config(persistence: Arc<dyn RuntimeSessionPersistence>) -> AgentLoopConfig {
    AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some("retrieval system".to_string()),
        persistence: Some(persistence),
        context_management: ContextManagementConfig {
            strategy: ContextManagementStrategy::RetrievalWindow,
            retrieval_window: RetrievalWindowContextConfig {
                min_recent_user_turns: 2,
                trigger_usage_ratio: 0.45,
                target_usage_ratio: 0.30,
                history_tool_required: true,
                fallback_strategy: ContextManagementFallbackStrategy::None,
            },
        },
        ..Default::default()
    }
}

fn append_archive_context_request(session: &mut Session, call_id: &str) {
    let mut assistant = Message::assistant("", None);
    assistant.tool_calls = Some(vec![ToolCall {
        id: call_id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "archive_context".to_string(),
            arguments: "{}".to_string(),
        },
    }]);
    session.add_message(assistant);
    let mut result = Message::tool_result(call_id, "Retrieval-window archive requested");
    result.id = format!("result-{call_id}-{}", session.messages.len());
    result.tool_success = Some(true);
    session.add_message(result);
}

fn assert_archive_rejection_visible(session: &Session, call_id: &str, expected: &str) {
    let result = session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(call_id))
        .expect("archive_context result");
    assert_eq!(result.tool_success, Some(false));
    assert!(result.content.starts_with("archive_context rejected:"));
    assert!(
        result.content.contains(expected),
        "expected rejection content to contain {expected:?}, got {:?}",
        result.content
    );
}

#[test]
fn manual_archive_consumption_tracks_result_occurrence_when_call_id_is_reused() {
    let mut session = Session::new("manual-archive-reused-id", "test-model");
    session.add_message(Message::user("archive twice in separate model rounds"));

    append_archive_context_request(&mut session, "reused-call-id");
    let first = pending_manual_archive_request(&session).expect("first request");
    mark_manual_archive_request_consumed(&mut session, &first).expect("consume first request");
    assert!(pending_manual_archive_request(&session).is_none());

    let mut unrelated = Message::assistant("", None);
    unrelated.tool_calls = Some(vec![ToolCall {
        id: "reused-call-id".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "read_file".to_string(),
            arguments: r#"{"path":"README.md"}"#.to_string(),
        },
    }]);
    session.add_message(unrelated);
    let mut unrelated_result = Message::tool_result("reused-call-id", "unrelated result");
    unrelated_result.id = "result-unrelated-reused-id".to_string();
    session.add_message(unrelated_result);
    assert!(
        pending_manual_archive_request(&session).is_none(),
        "a later non-archive result reusing the ID must not revive the older archive request"
    );

    append_archive_context_request(&mut session, "reused-call-id");
    let second = pending_manual_archive_request(&session).expect("second request");
    assert_eq!(second.tool_call_id, first.tool_call_id);
    assert_ne!(second.tool_result_message_id, first.tool_result_message_id);

    mark_manual_archive_request_consumed(&mut session, &second).expect("consume second request");
    let restarted: Session = serde_json::from_slice(&serde_json::to_vec(&session).unwrap())
        .expect("occurrence marker survives restart");
    assert!(pending_manual_archive_request(&restarted).is_none());
}

#[test]
fn rejected_newest_manual_archive_remains_the_ordering_fence() {
    let mut session = Session::new("manual-archive-rejected-fence", "test-model");
    session.add_message(Message::user("try two archive requests in this turn"));

    append_archive_context_request(&mut session, "first-archive-call");
    let first = pending_manual_archive_request(&session).expect("first request");
    mark_manual_archive_request_consumed(&mut session, &first).expect("consume first request");

    append_archive_context_request(&mut session, "second-archive-call");
    let second = pending_manual_archive_request(&session).expect("second request");
    surface_manual_archive_rejection(&mut session, &second, "permanent rejection")
        .expect("surface second rejection");
    mark_manual_archive_request_consumed(&mut session, &second).expect("consume second request");

    assert!(
        pending_manual_archive_request(&session).is_none(),
        "the rejected newest occurrence must prevent the older call from replaying"
    );
    let restarted: Session = serde_json::from_slice(&serde_json::to_vec(&session).unwrap())
        .expect("rejection fence survives restart");
    assert!(pending_manual_archive_request(&restarted).is_none());
}

struct ExpandingFootprintProvider {
    projection_calls: AtomicUsize,
    expanding_projection_call: usize,
}

#[async_trait::async_trait]
impl LLMProvider for ExpandingFootprintProvider {
    async fn provider_visible_tool_footprint(
        &self,
        _ir: &bamboo_llm::prompt_ir::PromptIR,
        _tools: &[ToolSchema],
        _model: &str,
        _required_tool: Option<&str>,
    ) -> bamboo_llm::provider::Result<ProviderVisibleToolFootprint> {
        let call = self.projection_calls.fetch_add(1, Ordering::SeqCst);
        // Preflight, current-state accounting, and post-boundary accounting are
        // stable; the exact retained projection then expands unexpectedly.
        Ok(if call == self.expanding_projection_call {
            ProviderVisibleToolFootprint {
                segments: vec![ProviderVisibleToolSegment {
                    kind: ProviderVisibleToolSegmentKind::InitialFullDefinition,
                    serialized: "late expanded provider schema ".repeat(20_000),
                }],
            }
        } else {
            ProviderVisibleToolFootprint::default()
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        panic!("retrieval preparation must fail before provider dispatch")
    }
}

fn system_prompt(session: &Session) -> String {
    session
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::System))
        .map(|m| m.content.clone())
        .unwrap_or_default()
}

fn sample_task_list(session_id: &str, status: TaskItemStatus) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Compression Tasks".to_string(),
        items: vec![TaskItem {
            id: "task_1".to_string(),
            description: "Unify compression context".to_string(),
            status,
            notes: "Ensure unified context blocks reach summarization".to_string(),
            ..TaskItem::default()
        }],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

#[test]
fn historical_ledger_tokens_start_a_bounded_retention_epoch_below_event_cap() {
    let mut session = Session::new("ledger-retention-budget", "test-model");
    let large_snapshot = "historical context ".repeat(2_000);
    let events = (0..3)
        .map(|sequence| ModelContextEvent {
            id: format!("ctx-{sequence}"),
            epoch: 4,
            sequence,
            anchor_message_id: None,
            block_type: ContextBlockType::TaskSnapshot,
            revision: sequence + 1,
            supersedes_revision: (sequence > 0).then_some(sequence),
            kind: ModelContextEventKind::Snapshot,
            content_sha256: format!("digest-{sequence}"),
            rendered_text: large_snapshot.clone(),
        })
        .collect();
    session.model_context_state = Some(ModelContextState {
        state_revision: 7,
        prefix_epoch: 4,
        next_sequence: 3,
        events,
        cache_scope_sha256: Some("scope".to_string()),
        ..ModelContextState::default()
    });
    let budget = TokenBudget::with_safety_margin(8_000, 1_000, BudgetStrategy::default(), 0);
    let counter = TiktokenTokenCounter::default();

    let usage = enforce_model_context_ledger_retention(&mut session, &budget, &counter);

    assert_eq!(usage.tokens, 0);
    let state = session.model_context_state.as_ref().unwrap();
    assert_eq!(state.prefix_epoch, 5);
    assert_eq!(state.state_revision, 8);
    assert!(state.events.is_empty());
    assert_eq!(
        state.last_reset_reason,
        Some(ModelContextResetReason::RetentionLimit)
    );
}

struct RecordingLlmProvider {
    models: Arc<Mutex<Vec<String>>>,
    response: String,
}

struct StickyFallbackCatalogProvider {
    catalogs: Arc<Mutex<Vec<Vec<String>>>>,
}

#[async_trait::async_trait]
impl LLMProvider for StickyFallbackCatalogProvider {
    async fn capability_loading_mode(
        &self,
        _model: &str,
        _required_tool: Option<&str>,
    ) -> CapabilityLoadingMode {
        CapabilityLoadingMode::StickyFallback
    }

    async fn provider_visible_tool_footprint(
        &self,
        _ir: &bamboo_llm::prompt_ir::PromptIR,
        tools: &[ToolSchema],
        _model: &str,
        _required_tool: Option<&str>,
    ) -> bamboo_llm::provider::Result<ProviderVisibleToolFootprint> {
        self.catalogs.lock().expect("catalog lock").push(
            tools
                .iter()
                .map(|tool| tool.function.name.clone())
                .collect(),
        );
        Ok(ProviderVisibleToolFootprint {
            segments: vec![ProviderVisibleToolSegment {
                kind: ProviderVisibleToolSegmentKind::InitialFullDefinition,
                serialized: serde_json::to_string(tools).expect("tool schemas serialize"),
            }],
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        panic!("manual retrieval preparation must not dispatch a model request")
    }
}

#[async_trait::async_trait]
impl LLMProvider for RecordingLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.models
            .lock()
            .expect("recorded model list lock should not be poisoned")
            .push(model.to_string());

        Ok(Box::pin(stream::iter(vec![
            Ok::<LLMChunk, LLMError>(LLMChunk::Token(self.response.clone())),
            Ok::<LLMChunk, LLMError>(LLMChunk::Done),
        ])))
    }
}

fn recording_llm() -> (Arc<dyn LLMProvider>, Arc<Mutex<Vec<String>>>) {
    recording_llm_with_response("summary")
}

fn recording_llm_with_response(
    response: impl Into<String>,
) -> (Arc<dyn LLMProvider>, Arc<Mutex<Vec<String>>>) {
    let models = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(RecordingLlmProvider {
        models: Arc::clone(&models),
        response: response.into(),
    });
    (llm, models)
}

struct PromptCaptureLlmProvider {
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
}

#[async_trait::async_trait]
impl LLMProvider for PromptCaptureLlmProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.requests
            .lock()
            .expect("captured request lock should not be poisoned")
            .push(messages.to_vec());

        Ok(Box::pin(stream::iter(vec![
            Ok::<LLMChunk, LLMError>(LLMChunk::Token("summary".to_string())),
            Ok::<LLMChunk, LLMError>(LLMChunk::Done),
        ])))
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[bamboo_agent_core::tools::ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        _options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.chat_stream(messages, tools, max_output_tokens, model)
            .await
    }
}

#[allow(clippy::type_complexity)]
fn prompt_capture_llm() -> (Arc<dyn LLMProvider>, Arc<Mutex<Vec<Vec<Message>>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(PromptCaptureLlmProvider {
        requests: Arc::clone(&requests),
    });
    (llm, requests)
}

#[derive(Debug, Clone)]
struct CapturedBoundedRequest {
    messages: Vec<Message>,
    max_output_tokens: u32,
    model: String,
}

#[derive(Debug, Clone, Copy)]
enum BoundedFailureMode {
    None,
    Call(usize),
    PartialWithoutDone(usize),
    Reduce,
}

struct BoundedCompressionProvider {
    model_info: ProviderModelInfo,
    requests: Arc<Mutex<Vec<CapturedBoundedRequest>>>,
    failure_mode: BoundedFailureMode,
}

#[async_trait::async_trait]
impl LLMProvider for BoundedCompressionProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let request = CapturedBoundedRequest {
            messages: messages.to_vec(),
            max_output_tokens: max_output_tokens.unwrap_or_default(),
            model: model.to_string(),
        };
        let call_number = {
            let mut requests = self
                .requests
                .lock()
                .expect("bounded compression request lock");
            requests.push(request);
            requests.len()
        };
        let is_reduce = messages.iter().any(|message| {
            message.content.contains("final reduce stage")
                || message.content.contains("intermediate reduce stage")
        });

        match self.failure_mode {
            BoundedFailureMode::Call(failed_call) if call_number == failed_call => {
                Err(LLMError::Api(format!(
                    "injected map failure on call {call_number}"
                )))
            }
            BoundedFailureMode::Reduce if is_reduce => {
                Err(LLMError::Api("injected reduce failure".to_string()))
            }
            BoundedFailureMode::PartialWithoutDone(failed_call)
                if call_number == failed_call =>
            {
                Ok(Box::pin(stream::iter(vec![Ok::<LLMChunk, LLMError>(
                    LLMChunk::Token("partial summary that must not commit".to_string()),
                )])))
            }
            _ => Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token(format!(
                    "bounded summary part {call_number} with requirements, decisions, and test evidence"
                ))),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ]))),
        }
    }

    async fn list_model_info(&self) -> bamboo_llm::provider::Result<Vec<ProviderModelInfo>> {
        Ok(vec![self.model_info.clone()])
    }
}

#[allow(clippy::type_complexity)]
fn bounded_compression_llm(
    failure_mode: BoundedFailureMode,
) -> (
    Arc<dyn LLMProvider>,
    Arc<Mutex<Vec<CapturedBoundedRequest>>>,
) {
    bounded_compression_llm_with_limits(failure_mode, 5_000, 2_000)
}

#[allow(clippy::type_complexity)]
fn bounded_compression_llm_with_limits(
    failure_mode: BoundedFailureMode,
    max_context_tokens: u32,
    max_output_tokens: u32,
) -> (
    Arc<dyn LLMProvider>,
    Arc<Mutex<Vec<CapturedBoundedRequest>>>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(BoundedCompressionProvider {
        model_info: ProviderModelInfo {
            id: "summary-model-763".to_string(),
            max_context_tokens: Some(max_context_tokens),
            max_output_tokens: Some(max_output_tokens),
        },
        requests: Arc::clone(&requests),
        failure_mode,
    });
    (llm, requests)
}

fn bounded_compression_session(id: &str) -> Session {
    let mut session = Session::new(id, "main-model-763");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 24_000,
        max_output_tokens: 4_000,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..40 {
        let user_marker = if index >= 37 {
            format!("LATEST_PROTECTED_763_{index}")
        } else {
            format!("ARCHIVE_SOURCE_763_{index}")
        };
        session.messages.push(Message::user(format!(
            "{user_marker} {}",
            "requirement detail evidence alpha beta gamma ".repeat(40)
        )));
        session.messages.push(Message::assistant(
            format!(
                "assistant-{index} {}",
                "implementation result test output followup delta ".repeat(40)
            ),
            None,
        ));
    }
    let mut never_compress = Message::assistant(
        format!(
            "NEVER_COMPRESS_763 {}",
            "durable protected runtime state ".repeat(40)
        ),
        None,
    );
    never_compress.never_compress = true;
    session.messages.insert(5, never_compress);
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 20_000,
        total_tokens: 20_100,
        max_context_tokens: 24_000,
        budget_limit: 24_000,
        truncation_occurred: true,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });
    session.force_manual_compression = Some("Preserve concrete evidence".to_string());
    session
}

fn bounded_compression_config(summary_provider: Arc<dyn LLMProvider>) -> AgentLoopConfig {
    AgentLoopConfig {
        model_name: Some("main-model-763".to_string()),
        summarization_model_name: Some("summary-model-763".to_string()),
        summarization_model_provider: Some(summary_provider),
        summary_target_ratio: 0.20,
        summary_safe_window_percent: 80,
        ..Default::default()
    }
}

fn archive_state(session: &Session) -> Vec<(String, bool, Option<String>)> {
    session
        .messages
        .iter()
        .map(|message| {
            (
                message.id.clone(),
                message.compressed,
                message.compressed_by_event_id.clone(),
            )
        })
        .collect()
}

#[tokio::test]
async fn bounded_host_compression_uses_auxiliary_model_budget_and_exact_candidates() {
    let mut session = bounded_compression_session("bounded-success-763");
    let mut sticky_call = Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "sticky-compression-call".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME.to_string(),
                arguments: r#"{"query":"archive"}"#.to_string(),
            },
        }]),
    );
    sticky_call.never_compress = true;
    let sticky_call_id = sticky_call.id.clone();
    let mut sticky_result = Message::tool_result_with_status(
        "sticky-compression-call",
        r#"<loaded_tools>{"tools":[{"type":"function","function":{"name":"ReadArchive","description":"Read archived files","parameters":{"type":"object"}}}]}</loaded_tools>"#,
        true,
    );
    sticky_result.never_compress = true;
    let sticky_result_id = sticky_result.id.clone();
    session.messages.insert(6, sticky_call);
    session.messages.insert(7, sticky_result);
    let never_compress_id = session
        .messages
        .iter()
        .find(|message| message.content.contains("NEVER_COMPRESS_763"))
        .map(|message| message.id.clone())
        .expect("never-compress fixture");
    let latest_user_ids = session
        .messages
        .iter()
        .filter(|message| {
            matches!(message.role, Role::User) && message.content.contains("LATEST_PROTECTED_763")
        })
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();
    let (summary_llm, captured) = bounded_compression_llm(BoundedFailureMode::None);
    let config = bounded_compression_config(summary_llm);
    let main_llm = noop_llm();
    let (event_tx, mut event_rx) = mpsc::channel(128);

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "bounded-success-763",
        &[],
        &main_llm,
        Some(&event_tx),
        "pre-turn",
    )
    .await
    .expect("bounded host compression should succeed");
    assert!(applied);
    drop(event_tx);
    let progress_statuses = std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AgentEvent::ContextCompressionStatus { status, .. } => Some(status),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(progress_statuses.iter().any(|status| status == "started"));
    assert!(progress_statuses
        .iter()
        .any(|status| status.starts_with("map:")));
    assert!(progress_statuses
        .iter()
        .any(|status| status.starts_with("final_reduce:")
            || status.starts_with("intermediate_reduce:")));
    assert!(progress_statuses.iter().any(|status| status == "completed"));

    let requests = captured.lock().expect("bounded capture lock").clone();
    assert!(
        requests.len() >= 3,
        "the much smaller summary model should force multiple bounded map/reduce stages"
    );
    let counter = TiktokenTokenCounter::default();
    for request in &requests {
        assert_eq!(request.model, "summary-model-763");
        let input_tokens = counter.count_messages(&request.messages);
        assert!(
            input_tokens
                .saturating_add(request.max_output_tokens)
                .saturating_add(1_000)
                <= 4_000,
            "request exceeded auxiliary model 80% ceiling: input={input_tokens}, output={}",
            request.max_output_tokens
        );
        assert!(request.max_output_tokens <= 2_000);
    }

    let rendered_requests = requests
        .iter()
        .flat_map(|request| request.messages.iter())
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered_requests.contains("ARCHIVE_SOURCE_763_0"));
    assert!(!rendered_requests.contains("NEVER_COMPRESS_763"));
    assert!(!rendered_requests.contains("LATEST_PROTECTED_763"));

    let compressed_messages = session
        .messages
        .iter()
        .filter(|message| message.compressed)
        .collect::<Vec<_>>();
    assert!(!compressed_messages.is_empty());
    for message in compressed_messages {
        assert!(
            rendered_requests.contains(&message.id),
            "archived message {} was not represented in a raw map request",
            message.id
        );
    }
    assert!(session
        .messages
        .iter()
        .find(|message| message.id == never_compress_id)
        .is_some_and(|message| !message.compressed));
    for protected_id in [&sticky_call_id, &sticky_result_id] {
        assert!(session
            .messages
            .iter()
            .find(|message| message.id == *protected_id)
            .is_some_and(|message| !message.compressed));
    }
    let active = bamboo_compression::prepare_hybrid_context_with_fixed_tokens(
        &session,
        session
            .token_budget
            .as_ref()
            .expect("bounded session token budget"),
        &counter,
        0,
    )
    .expect("compressed context should still fit");
    for protected_id in [&sticky_call_id, &sticky_result_id] {
        assert!(
            active
                .messages
                .iter()
                .any(|message| message.id == *protected_id),
            "sticky discovery call/result must remain together in active context"
        );
    }
    for id in latest_user_ids {
        assert!(
            session
                .messages
                .iter()
                .find(|message| message.id == id)
                .is_some_and(|message| !message.compressed),
            "newest protected user message must remain active"
        );
    }

    let event = session
        .compression_events
        .last()
        .expect("successful pass should persist an event");
    assert!(event.summarization_map_calls > 1);
    assert!(event.summarization_reduce_calls >= 1);
    assert_eq!(event.model_used.as_deref(), Some("summary-model-763"));
    assert_eq!(event.summary_target_ratio, 0.20);
    assert!(event.target_summary_tokens > 0);
    assert!(event.actual_summary_tokens > 0);
    assert!(
        session
            .messages
            .iter()
            .filter(|message| message.compressed)
            .all(|message| message.compressed_by_event_id.as_deref() == Some(event.id.as_str())),
        "logical pass id should correlate requests, archive markers, and event"
    );
}

#[tokio::test]
async fn same_size_chat_and_summary_windows_still_split_near_critical_pressure() {
    let mut session = bounded_compression_session("same-window-763");
    let budget = session
        .token_budget
        .as_mut()
        .expect("main token budget fixture");
    budget.compression_target_percent = 25;
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 23_500,
        total_tokens: 23_600,
        max_context_tokens: 24_000,
        budget_limit: 24_000,
        truncation_occurred: true,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });
    let (summary_llm, captured) =
        bounded_compression_llm_with_limits(BoundedFailureMode::None, 24_000, 6_000);
    let config = bounded_compression_config(summary_llm);
    let main_llm = noop_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "same-window-763",
        &[],
        &main_llm,
        None,
        "pre-turn",
    )
    .await
    .expect("same-window bounded compression");
    assert!(applied);
    let event = session
        .compression_events
        .last()
        .expect("compression event");
    assert!(
        event.summarization_map_calls > 1,
        "near-critical source must split even when both models advertise the same context size"
    );

    let requests = captured.lock().expect("capture lock");
    let counter = TiktokenTokenCounter::default();
    for request in requests.iter() {
        assert!(
            counter
                .count_messages(&request.messages)
                .saturating_add(request.max_output_tokens)
                .saturating_add(1_000)
                <= 19_200,
            "same-window request exceeded the 80% ceiling"
        );
    }
}

async fn assert_failed_bounded_pass_is_atomic(
    failure_mode: BoundedFailureMode,
    expected_error: &str,
) {
    let mut session = bounded_compression_session("bounded-failure-763");
    let before_archive = archive_state(&session);
    let before_summary =
        serde_json::to_value(&session.conversation_summary).expect("serialize summary");
    let before_events =
        serde_json::to_value(&session.compression_events).expect("serialize events");
    let before_manual = session.force_manual_compression.clone();
    let (summary_llm, captured) = bounded_compression_llm(failure_mode);
    let config = bounded_compression_config(summary_llm);
    let main_llm = noop_llm();

    let error = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "bounded-failure-763",
        &[],
        &main_llm,
        None,
        "pre-turn",
    )
    .await
    .expect_err("injected bounded stage failure must surface");
    assert!(
        error.to_string().contains(expected_error),
        "unexpected error: {error}"
    );
    assert_eq!(archive_state(&session), before_archive);
    assert_eq!(
        serde_json::to_value(&session.conversation_summary).expect("serialize summary"),
        before_summary
    );
    assert_eq!(
        serde_json::to_value(&session.compression_events).expect("serialize events"),
        before_events
    );
    assert_eq!(session.force_manual_compression, before_manual);
    assert!(!captured.lock().expect("bounded capture lock").is_empty());
}

#[tokio::test]
async fn failed_map_chunk_leaves_archive_summary_and_events_unchanged() {
    assert_failed_bounded_pass_is_atomic(
        BoundedFailureMode::Call(2),
        "injected map failure on call 2",
    )
    .await;
}

#[tokio::test]
async fn failed_reduce_leaves_archive_summary_and_events_unchanged() {
    assert_failed_bounded_pass_is_atomic(BoundedFailureMode::Reduce, "injected reduce failure")
        .await;
}

#[tokio::test]
async fn partial_stream_leaves_archive_summary_and_events_unchanged() {
    assert_failed_bounded_pass_is_atomic(
        BoundedFailureMode::PartialWithoutDone(1),
        "without terminal completion",
    )
    .await;
}

struct CompressionInstructionHook;

#[async_trait::async_trait]
impl AgentHook for CompressionInstructionHook {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::BeforeCompression
    }

    async fn run(
        &self,
        _point: AgentHookPoint,
        payload: &HookPayload,
        _session: &Session,
    ) -> HookResult {
        assert!(matches!(
            payload,
            HookPayload::Compression {
                estimated_tokens,
                usage_percent,
                max_context_tokens: 5_000,
                trigger_context_tokens: 4_000,
                trigger,
                phase,
            } if *estimated_tokens > 0
                && *usage_percent > 0.0
                && trigger == "manual"
                && phase == "mid-turn"
        ));
        HookResult::InjectContext {
            text: "Preserve the exact build failure and its file path".to_string(),
        }
    }

    fn name(&self) -> &str {
        "compression_instructions"
    }
}

#[tokio::test]
async fn maybe_apply_host_context_compression_uses_fast_model_for_every_summary_stage() {
    let mut session = Session::new("session-cp-fast-model", "main-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 1200,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 900,
        total_tokens: 1000,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("main-model".to_string()),
        background_model_name: Some("fast-model".to_string()),
        ..Default::default()
    };
    let (llm, models) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model",
        "session-cp-fast-model",
        &[],
        &llm,
        None,
        "pre-turn",
    )
    .await
    .expect("host compression should run with fast model");

    assert!(applied, "expected pre-turn compression to be applied");

    let models = models
        .lock()
        .expect("recorded model list lock should not be poisoned");
    assert_eq!(
        models.as_slice(),
        ["fast-model", "fast-model"],
        "even a small compression candidate must route both map and reduce through the selected background model"
    );
}

#[tokio::test]
async fn host_context_compression_skips_when_no_background_model_is_configured() {
    let mut session = Session::new("session-cp-no-background-model", "test-model");
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 900,
        total_tokens: 1000,
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("main-model".to_string()),
        fast_model_name: None,
        ..Default::default()
    };
    let (llm, models) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model",
        "session-cp-no-background-model",
        &[],
        &llm,
        None,
        "pre-turn",
    )
    .await
    .expect("compression path should return cleanly when background model is absent");

    assert!(
        !applied,
        "compression should be skipped without a background model"
    );

    let models = models
        .lock()
        .expect("recorded model list lock should not be poisoned");
    assert!(
        models.is_empty(),
        "summarizer should not call the main model as fallback"
    );
}

#[tokio::test]
async fn force_overflow_context_recovery_degrades_tool_guide_before_skill_context() {
    let mut session = Session::new("session-cp-overflow-degrade", "test-model");
    session.messages.push(Message::system(
        "Base prompt\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\nskill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->".to_string(),
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-cp-overflow-degrade",
        &[],
        &llm,
        None,
    )
    .await
    .expect("overflow degradation should complete");

    assert!(applied);
    let system_prompt = session
        .messages
        .iter()
        .find(|message| matches!(message.role, Role::System))
        .map(|message| message.content.clone())
        .unwrap_or_default();
    assert!(system_prompt.contains("BAMBOO_SKILL_CONTEXT_START"));
    assert!(!system_prompt.contains("BAMBOO_TOOL_GUIDE_START"));
}

#[tokio::test]
async fn prepare_round_context_applies_placeholder_fallback_only_to_prepared_context() {
    let mut session = Session::new("session-cp-1", "test-model");
    session.messages.push(Message::user_with_parts(
        "看图",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        image_fallback: Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Placeholder,
            vision_model: None,
        }),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-1",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    let prepared_user = prepared
        .prepared_context
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("prepared user message should exist");

    assert!(prepared_user.content_parts.is_none());
    assert!(prepared_user.content.contains("[Image omitted:"));

    let persisted_user = session
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("persisted user message should exist");
    assert!(persisted_user.content_parts.is_some());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn projected_relocation_does_not_double_reserve_large_system_env_context() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let large_env = "stable environment inventory and capability detail ".repeat(900);
    let configured_system = format!(
        "system\n\n<!-- BAMBOO_ENV_CONTEXT_START -->\n{large_env}\n<!-- BAMBOO_ENV_CONTEXT_END -->"
    );
    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some(configured_system),
        ..Default::default()
    };
    let mut session = Session::new("session-cp-relocated-env", "test-model");
    let (stable_frame, sections) =
        crate::runtime::runner::session_setup::prompt_setup::build_stable_prompt_frame_with_sections(
            &session,
            &config,
            &[],
            &Default::default(),
        );
    assert!(!stable_frame
        .stable_instructions
        .contains("stable environment inventory"));
    session
        .messages
        .push(Message::system(stable_frame.stable_instructions));
    session
        .messages
        .push(Message::user("inspect the environment"));

    let counter = TiktokenTokenCounter::default();
    let env_context = sections
        .iter()
        .find(|section| section.name == "env")
        .map(|section| section.content.clone())
        .expect("relocated environment section");
    let env_message = bamboo_agent_core::ContextBlock::new(
        ContextBlockType::EnvSnapshot,
        bamboo_agent_core::ContextBlockPriority::High,
        bamboo_agent_core::ContextBlockStability::SessionStable,
        "Environment Snapshot",
        env_context,
    )
    .render_runtime_context_message();
    let fitted_prefix_tokens = counter.count_messages(&[session.messages[0].clone(), env_message]);
    let max_output_tokens = 256;
    let request_input_limit = fitted_prefix_tokens.saturating_add(768);
    session.token_budget = Some(TokenBudget::with_safety_margin(
        request_input_limit.saturating_add(max_output_tokens),
        max_output_tokens,
        BudgetStrategy::default(),
        0,
    ));

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-relocated-env",
        &[],
        &llm,
        None,
    )
    .await
    .expect("relocating an already-fitted env block must not reserve it twice");
    let projected = super::super::stream_execution::project_request_usage(
        &session,
        &prepared.prepared_context,
        &config,
        &[],
        "test-model",
        &llm,
    )
    .await
    .expect("provider-visible projection");

    assert!(projected.input_tokens <= request_input_limit);
    assert!(
        session.model_context_state.is_none(),
        "projection must stay pure"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn projected_compression_reseed_does_not_double_reserve_large_summary() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let summary_content =
        "compressed decisions requirements and verification evidence ".repeat(900);
    let mut session = Session::new("session-cp-relocated-summary", "test-model");
    session.messages.push(Message::system("system"));
    session
        .messages
        .push(Message::user("continue after compression"));
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        summary_content.clone(),
        40,
        10_000,
    ));
    session.model_context_state = Some(ModelContextState {
        state_revision: 7,
        prefix_epoch: 4,
        last_reset_reason: Some(ModelContextResetReason::Compression),
        ..ModelContextState::default()
    });

    let counter = TiktokenTokenCounter::default();
    let system_tokens = counter.count_messages(&session.messages[..1]);
    let summary_tokens =
        counter.count_messages(&[bamboo_compression::compression_summary_message(
            &summary_content,
        )]);
    let max_output_tokens = 256;
    let request_input_limit = system_tokens
        .saturating_add(summary_tokens)
        // Leave room for the ledger event envelope itself. The old total-ledger
        // feedback still fails this fixture because it adds the full summary a
        // second time on top of the fitter's summary reservation.
        .saturating_add(1_600);
    session.token_budget = Some(TokenBudget::with_safety_margin(
        request_input_limit.saturating_add(max_output_tokens),
        max_output_tokens,
        BudgetStrategy::default(),
        0,
    ));
    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some("system".to_string()),
        ..Default::default()
    };
    let pending_reset = session.model_context_state.clone();

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-relocated-summary",
        &[],
        &llm,
        None,
    )
    .await
    .expect("relocating an already-fitted summary must not reserve it twice");
    let projected = super::super::stream_execution::project_request_usage(
        &session,
        &prepared.prepared_context,
        &config,
        &[],
        "test-model",
        &llm,
    )
    .await
    .expect("provider-visible projection");

    assert!(projected.input_tokens <= request_input_limit);
    assert_eq!(
        session.model_context_state, pending_reset,
        "shadow projection must not commit the compression reseed"
    );
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn projected_refit_handles_over_limit_vision_transform_exactly_once() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-cp-vision-refit", "test-model");
    session.messages.push(Message::system("system"));
    for index in 0..20 {
        session.messages.push(Message::user(format!(
            "conversation-{index} {}",
            "bounded transcript payload ".repeat(40)
        )));
        session.messages.push(Message::assistant(
            format!(
                "answer-{index} {}",
                "implementation evidence and verification ".repeat(40)
            ),
            None,
        ));
    }
    session.messages.push(Message::user_with_parts(
        "inspect the latest image",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "https://example.com/latest.png".to_string(),
                detail: None,
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    ));
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Active ledger context".to_string(),
        items: vec![TaskItem {
            id: "task-vision-refit".to_string(),
            description: "current task state must survive the epoch reseed ".repeat(120),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    });
    session.model_context_state = Some(ModelContextState {
        state_revision: 5,
        prefix_epoch: 3,
        last_reset_reason: Some(ModelContextResetReason::RetentionLimit),
        ..ModelContextState::default()
    });
    session.token_budget = Some(TokenBudget::with_safety_margin(
        4_000,
        256,
        BudgetStrategy::default(),
        0,
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some("system".to_string()),
        image_fallback: Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("vision-test".to_string()),
        }),
        ..Default::default()
    };
    let counter = TiktokenTokenCounter::default();
    let naive = bamboo_compression::prepare_hybrid_context_with_fixed_tokens(
        &session,
        session.token_budget.as_ref().unwrap(),
        &counter,
        0,
    )
    .expect("legacy zero-reservation fit");
    let naive_message_count = naive.messages.len();
    let vision_description = "expanded vision detail with visible text and layout ".repeat(120);
    let mut expanded_candidate_messages = naive.messages.clone();
    let expanded_image = expanded_candidate_messages
        .iter_mut()
        .find(|message| message.content_parts.is_some())
        .expect("latest image must survive the initial fit");
    expanded_image.content = format!(
        "inspect the latest image\n\n[Vision description of image 1: latest.png]\n{vision_description}\n"
    );
    expanded_image.content_parts = None;
    let request_input_limit = session
        .token_budget
        .as_ref()
        .unwrap()
        .max_request_input_tokens();
    assert!(
        counter.count_messages(&expanded_candidate_messages) > request_input_limit,
        "fixture must make the transformed candidate itself exceed the input limit"
    );
    let projection_llm = noop_llm();
    let naive_projection = super::super::stream_execution::project_request_usage(
        &session,
        &naive,
        &config,
        &[],
        "test-model",
        &projection_llm,
    )
    .await
    .expect("provider-visible naive projection");
    assert!(
        naive_projection.input_tokens
            > session
                .token_budget
                .as_ref()
                .unwrap()
                .max_request_input_tokens(),
        "fixture must require a projected refit"
    );

    let (llm, models) = recording_llm_with_response(vision_description);
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-vision-refit",
        &[],
        &llm,
        None,
    )
    .await
    .expect("projected refit should reuse the transformed candidate");

    assert!(prepared.prepared_context.truncation_occurred);
    assert!(prepared.prepared_context.messages.len() < naive_message_count);
    assert!(prepared.prepared_context.messages.iter().any(|message| {
        message.content.contains("[Vision description of image 1:")
            && message.content.contains("expanded vision detail")
    }));
    assert_eq!(
        *models.lock().expect("vision call list lock"),
        vec!["vision-test".to_string()],
        "bounded projection refits must not repeat the paid vision transform"
    );
    let projected = super::super::stream_execution::project_request_usage(
        &session,
        &prepared.prepared_context,
        &config,
        &[],
        "test-model",
        &llm,
    )
    .await
    .expect("provider-visible projection");
    assert!(
        counter.count_messages(&prepared.prepared_context.messages) <= request_input_limit,
        "refit must bring the transformed message vector itself back under the limit"
    );
    assert!(projected.input_tokens <= prepared.budget.max_request_input_tokens());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn prepare_round_context_refits_for_provider_visible_tool_schemas() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut base = Session::new("session-schema-refit", "test-model");
    base.messages.push(Message::system("system"));
    for index in 0..36 {
        base.messages.push(Message::user(format!(
            "history-{index} {}",
            "bounded user transcript ".repeat(18)
        )));
        base.messages.push(Message::assistant(
            format!(
                "answer-{index} {}",
                "bounded assistant transcript ".repeat(18)
            ),
            None,
        ));
    }
    base.messages.push(Message::user("continue"));
    base.token_budget = Some(TokenBudget::with_safety_margin(
        5_000,
        256,
        BudgetStrategy::default(),
        0,
    ));
    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some("system".to_string()),
        ..Default::default()
    };
    let tool = ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "large_lookup".to_string(),
            description: "Large lookup".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "provider-visible parameter ".repeat(320)
                    }
                }
            }),
        },
    };
    let llm = noop_llm();

    let mut without_tools = base.clone();
    let without_tools_prepared = prepare_round_context(
        &mut without_tools,
        &config,
        "test-model",
        "session-schema-refit-empty",
        &[],
        &llm,
        None,
    )
    .await
    .expect("message-only context preparation");

    let mut with_tools = base;
    let with_tools_prepared = prepare_round_context(
        &mut with_tools,
        &config,
        "test-model",
        "session-schema-refit-tools",
        std::slice::from_ref(&tool),
        &llm,
        None,
    )
    .await
    .expect("schema-aware context preparation");

    assert!(
        with_tools_prepared.prepared_context.messages.len()
            < without_tools_prepared.prepared_context.messages.len(),
        "the fixed schema footprint must reserve room before fitting history"
    );
    let projected = super::super::stream_execution::project_request_usage(
        &with_tools,
        &with_tools_prepared.prepared_context,
        &config,
        std::slice::from_ref(&tool),
        "test-model",
        &llm,
    )
    .await
    .expect("schema-aware final projection");
    assert!(projected.tool_schema_input_tokens > 0);
    assert!(projected.input_tokens <= with_tools_prepared.budget.max_request_input_tokens());
}

#[tokio::test]
async fn prepare_round_context_auto_compresses_when_hard_limit_truncation_pressure_is_high() {
    let mut session = Session::new("session-cp-2", "test-model");
    session.token_budget = Some(TokenBudget::new(
        // Leave enough protected-token headroom for the invariant core
        // directives while keeping the repeated history above the automatic
        // compression trigger.
        4_500,
        200,
        BudgetStrategy::Window { size: 50 },
    ));
    session.messages.push(Message::system(
        crate::runtime::runner::prompt_context::append_core_agent_directives(
            "System prompt",
            crate::runtime::context::CORE_AGENT_DIRECTIVES,
        ),
    ));
    for index in 0..20 {
        session.messages.push(Message::user(format!(
            "Old user message {index} {}",
            "historical context under hard-limit pressure ".repeat(10)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Old assistant response {index} {}",
                "decisions implementation and verification evidence ".repeat(10)
            ),
            None,
        ));
    }
    let budget = session.token_budget.as_ref().unwrap();
    let exposure = bamboo_compression::estimate_context_compression_exposure(
        &session,
        "test-model",
        Some(budget),
    );
    let trigger_percent = (budget.compression_trigger_context_tokens() as f64
        / budget.max_context_tokens as f64)
        * 100.0;
    assert!(
        exposure.active_usage_percent >= trigger_percent,
        "fixture must cross the compression trigger: exposure={exposure:?}"
    );
    let candidate = bamboo_compression::build_forced_compression_candidate_plan(
        &session,
        "test-model",
        Some(budget),
        0.20,
        CompressionTriggerType::Auto,
    );
    assert!(
        candidate.is_ok(),
        "fixture must admit a bounded compression plan: {candidate:?}"
    );

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let (llm, _) = recording_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-2",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "high pressure hard-limit truncation should trigger host auto-compression persistence"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "host auto-compression should mark historical messages compressed"
    );
    assert!(
        prepared.prepared_context.token_usage.summary_tokens > 0,
        "prepared context should reserve summary tokens after host auto-compression"
    );
    assert!(
        prepared
            .prepared_context
            .messages
            .iter()
            .any(|m| m.content.contains("CONVERSATION_SUMMARY_START")),
        "prepared context should include the persisted compression summary"
    );
}

#[tokio::test]
async fn prepare_round_context_drops_orphan_tool_results_only_from_prepared_context() {
    let mut session = Session::new("session-cp-3", "test-model");
    session.messages.push(Message::user("Run tool"));
    session.messages.push(Message::assistant(
        "Calling tool",
        Some(vec![ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "session_note".to_string(),
                arguments: "{}".to_string(),
            },
        }]),
    ));
    session
        .messages
        .push(Message::tool_result("call_1", "ok result"));
    session
        .messages
        .push(Message::tool_result("call_orphan", "orphan result"));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-3",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    let orphan_in_prepared =
        prepared.prepared_context.messages.iter().any(|m| {
            matches!(m.role, Role::Tool) && m.tool_call_id.as_deref() == Some("call_orphan")
        });
    assert!(
        !orphan_in_prepared,
        "orphan tool result should be removed from LLM context"
    );

    let orphan_in_persisted = session
        .messages
        .iter()
        .any(|m| matches!(m.role, Role::Tool) && m.tool_call_id.as_deref() == Some("call_orphan"));
    assert!(
        orphan_in_persisted,
        "persisted session history must remain unchanged"
    );
}

#[tokio::test]
async fn prepare_round_context_prunes_unresolved_tool_calls_from_prepared_context() {
    let mut session = Session::new("session-cp-4", "test-model");
    session.messages.push(Message::user("Run tool"));
    session.messages.push(Message::assistant(
        "This text should stay",
        Some(vec![ToolCall {
            id: "call_missing".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "session_note".to_string(),
                arguments: "{}".to_string(),
            },
        }]),
    ));
    session.messages.push(Message::user("continue"));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-4",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    let unresolved_tool_call_in_prepared = prepared.prepared_context.messages.iter().any(|m| {
        m.tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.id == "call_missing"))
    });
    assert!(
        !unresolved_tool_call_in_prepared,
        "unresolved tool call should be pruned from prepared LLM context"
    );

    let assistant_text_kept = prepared
        .prepared_context
        .messages
        .iter()
        .any(|m| matches!(m.role, Role::Assistant) && m.content == "This text should stay");
    assert!(assistant_text_kept, "assistant text should be preserved");

    let unresolved_tool_call_in_persisted = session.messages.iter().any(|m| {
        m.tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.id == "call_missing"))
    });
    assert!(
        unresolved_tool_call_in_persisted,
        "persisted history must remain unchanged"
    );
}

#[tokio::test]
async fn prepare_round_context_forces_compression_when_usage_crosses_ninety_eight_percent() {
    let mut session = Session::new("session-cp-force", "test-model");
    session.token_budget = Some(TokenBudget {
        // Leave room for the fixed host-owned Session identity context while
        // keeping recorded usage above the 98% emergency threshold.
        max_context_tokens: 2000,
        max_output_tokens: 0,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 1870,
        total_tokens: 1970,
        max_context_tokens: 2000,
        budget_limit: 2000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let (llm, _) = recording_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "forced fallback should persist a compression event when usage is >= 98%"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "forced fallback should mark older messages compressed"
    );
    assert!(
        prepared.prepared_context.token_usage.usage_percentage() < 98.0,
        "prepared context should be recomputed after forced compression"
    );
}

#[tokio::test]
async fn maybe_apply_host_context_compression_supports_mid_turn_phase() {
    let mut session = Session::new("session-cp-mid-turn", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 1200,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 850,
        total_tokens: 950,
        max_context_tokens: 1200,
        budget_limit: 1000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let (llm, _) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-cp-mid-turn",
        &[],
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("mid-turn host compression should run");

    assert!(applied, "expected mid-turn compression to be applied");
    assert!(
        !session.compression_events.is_empty(),
        "mid-turn compression should persist a compression event"
    );
}

/// Put the session into active plan mode so the canonical plan-mode/plan-runtime
/// blocks (built directly from session state, not reparsed from markers) render.
fn activate_plan_mode(session: &mut Session) {
    use bamboo_domain::session::runtime_state::{AgentRuntimeState, PlanModeState, PlanModeStatus};
    session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
    session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
        entered_at: chrono::Utc::now(),
        pre_permission_mode: "default".to_string(),
        plan_file_path: None,
        status: PlanModeStatus::Designing,
    });
}

fn attach_durable_instruction_workflow(session: &mut Session) {
    const WORKFLOW_ID: &str = "compression-workflow-872";
    const WORKFLOW_REVISION: u64 = 7;

    let definition = bamboo_skills::SkillDefinition::new(
        WORKFLOW_ID,
        "Compression Workflow",
        "Private durable workflow used by the compression regression",
        "WORKFLOW_PRIVATE_INSTRUCTION_872",
    );
    let catalog_entry = bamboo_skills::WorkflowCatalogEntry {
        id: WORKFLOW_ID.to_string(),
        name: "Compression Workflow".to_string(),
        description: "Private durable workflow used by the compression regression".to_string(),
        kind: bamboo_skills::WorkflowKind::Instruction,
        source: bamboo_skills::WorkflowSource::Builtin,
        revision: WORKFLOW_REVISION,
        content_digest: "compression-workflow-digest".to_string(),
        version: "1.0.0".to_string(),
        invocation_policy: serde_json::json!({"manual": true}),
        argument_schema: serde_json::json!({"type": "object"}),
        status: bamboo_skills::WorkflowStatus::Valid,
        legacy: false,
        migration_status: None,
        last_error: None,
        winner: true,
        shadowed_candidates: Vec::new(),
    };
    let mut skills = BTreeMap::new();
    skills.insert(
        WORKFLOW_ID.to_string(),
        bamboo_skills::SkillActivationSnapshotEntry {
            definition,
            catalog_entry,
            revision: WORKFLOW_REVISION,
            resources: BTreeMap::new(),
        },
    );
    let durable = bamboo_skills::DurableWorkflowActivation {
        active: bamboo_skills::ActiveWorkflow {
            id: WORKFLOW_ID.to_string(),
            source: bamboo_skills::WorkflowSource::Builtin,
            revision: WORKFLOW_REVISION,
            kind: bamboo_skills::WorkflowKind::Instruction,
            args: serde_json::json!({"private_scope": "WORKFLOW_PRIVATE_ARG_872"}),
            invoked_by: bamboo_skills::WorkflowInvokedBy::User,
            activated_at: chrono::Utc::now(),
            status: bamboo_skills::WorkflowActivationStatus::Active,
            diagnostic: None,
            context_fingerprint: Some("workflow-context-fingerprint-872".to_string()),
            dynamic_context: Vec::new(),
        },
        snapshot: bamboo_skills::SkillActivationSnapshot {
            catalog_revision: 11,
            selected_skill_mode: None,
            skills,
        },
    };
    session.metadata.insert(
        bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY.to_string(),
        serde_json::to_string(&durable).expect("serialize durable workflow fixture"),
    );
}

#[tokio::test]
async fn mid_turn_host_context_compression_includes_unified_context_blocks_in_summary_prompt() {
    let mut session = Session::new("session-cp-mid-turn-context-blocks", "test-model");
    activate_plan_mode(&mut session);
    // External memory now rides a session field (the async refresh populates it),
    // not a system-message marker.
    session.metadata.insert(
        crate::runtime::runner::prompt_context::EXTERNAL_MEMORY_RENDERED_KEY.to_string(),
        "## External Memory (Persistent)\n\nSession memory note".to_string(),
    );
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 5000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.set_task_list(sample_task_list(&session.id, TaskItemStatus::InProgress));
    session.force_manual_compression = Some("Keep only active work".to_string());
    session.messages.push(Message::system(
        "System prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\nSession memory note\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPlan mode is active\n<!-- BAMBOO_PLAN_MODE_END -->\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\nDurable plan execution state\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->"
            .to_string(),
    ));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 4_100,
        total_tokens: 4_200,
        max_context_tokens: 5000,
        budget_limit: 5000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let mut hook_runner = crate::HookRunner::new();
    hook_runner.register(Arc::new(CompressionInstructionHook));
    let config = AgentLoopConfig {
        hook_runner: Arc::new(hook_runner),
        ..config
    };
    let (llm, requests) = prompt_capture_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-cp-mid-turn-context-blocks",
        &[],
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("mid-turn host compression should run");

    assert!(applied, "expected mid-turn compression to be applied");

    let requests = requests
        .lock()
        .expect("captured request lock should not be poisoned");
    let prompt = requests
        .last()
        .and_then(|messages| messages.iter().find(|m| matches!(m.role, Role::User)))
        .map(|message| message.content.clone())
        .expect("summary prompt user message should be captured");

    assert!(prompt.contains("## Compression Context Blocks"));
    assert!(prompt.contains("type: task_snapshot"));
    assert!(prompt.contains("type: external_memory"));
    assert!(prompt.contains("type: plan_mode_state"));
    assert!(prompt.contains("type: plan_runtime_state"));
    assert!(prompt.contains("Current Task List"));
    assert!(prompt.contains("External Memory (Persistent)"));
    assert!(prompt.contains("Plan Mode State"));
    assert!(prompt.contains("Durable Plan Execution Context"));
    assert!(prompt.contains("## Custom Compression Instructions"));
    assert!(prompt.contains("## PreCompact Hook Instructions"));
    assert!(prompt.contains("Preserve the exact build failure and its file path"));
}

#[tokio::test]
async fn pre_turn_host_context_compression_includes_available_context_blocks_in_summary_prompt() {
    let mut session = Session::new("session-cp-pre-turn-context-blocks", "test-model");
    activate_plan_mode(&mut session);
    attach_durable_instruction_workflow(&mut session);
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 5000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.set_task_list(sample_task_list(&session.id, TaskItemStatus::Pending));
    session.messages.push(Message::system(
        "System prompt\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPlan mode is active\n<!-- BAMBOO_PLAN_MODE_END -->\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\nDurable plan execution state\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->"
            .to_string(),
    ));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 4_100,
        total_tokens: 4_200,
        max_context_tokens: 5000,
        budget_limit: 5000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });
    assert!(session.messages.iter().all(|message| {
        !message.content.contains("WORKFLOW_PRIVATE_INSTRUCTION_872")
            && !message.content.contains("WORKFLOW_PRIVATE_ARG_872")
    }));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let (llm, requests) = prompt_capture_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-cp-pre-turn-context-blocks",
        &[],
        &llm,
        None,
        "pre-turn",
    )
    .await
    .expect("pre-turn host compression should run");

    assert!(applied, "expected pre-turn compression to be applied");
    assert!(session.messages.iter().all(|message| {
        !message.content.contains("WORKFLOW_PRIVATE_INSTRUCTION_872")
            && !message.content.contains("WORKFLOW_PRIVATE_ARG_872")
    }));

    let requests = requests
        .lock()
        .expect("captured request lock should not be poisoned");
    let prompt = requests
        .last()
        .and_then(|messages| messages.iter().find(|m| matches!(m.role, Role::User)))
        .map(|message| message.content.clone())
        .expect("summary prompt user message should be captured");

    assert!(prompt.contains("## Compression Context Blocks"));
    assert!(prompt.contains("type: task_snapshot"));
    assert!(prompt.contains("type: workflow_runtime"));
    assert!(prompt.contains("type: plan_mode_state"));
    assert!(prompt.contains("type: plan_runtime_state"));
    assert!(prompt.contains("Current Task List"));
    assert!(prompt.contains("Plan Mode State"));
    assert!(prompt.contains("Durable Plan Execution Context"));
    assert_eq!(
        prompt.matches("WORKFLOW_PRIVATE_INSTRUCTION_872").count(),
        1
    );
    assert_eq!(prompt.matches("WORKFLOW_PRIVATE_ARG_872").count(), 1);

    let compression_blocks = build_compression_context_blocks(&session, None);
    assert!(compression_blocks
        .iter()
        .all(|block| block.block_type != ContextBlockType::SessionIdentity));
    let workflow_blocks = compression_blocks
        .into_iter()
        .filter(|block| block.block_type == ContextBlockType::WorkflowRuntime)
        .collect::<Vec<_>>();
    assert_eq!(workflow_blocks.len(), 1);
    assert_eq!(
        workflow_blocks[0]
            .content
            .matches("WORKFLOW_PRIVATE_INSTRUCTION_872")
            .count(),
        1
    );
    assert_eq!(
        workflow_blocks[0]
            .content
            .matches("WORKFLOW_PRIVATE_ARG_872")
            .count(),
        1
    );
}

#[tokio::test]
async fn retrieval_window_second_boundary_reserves_only_incremental_marker_growth() {
    let mut session = Session::new("retrieval-second-boundary", "test-model");
    session.messages.push(Message::system("retrieval system"));

    let mut prior_event = bamboo_domain::CompressionEvent::new(
        1,
        1,
        80.0,
        30.0,
        0,
        CompressionTriggerType::Auto,
        0.0,
        None,
        0,
    );
    prior_event.kind = bamboo_domain::CompressionEventKind::RetrievalWindow;
    prior_event.retrieval_retained_recent_user_turn_count = 1;
    let mut archived = Message::user("exact archived evidence");
    archived.compressed = true;
    archived.compressed_by_event_id = Some(prior_event.id.clone());
    session.messages.push(archived);
    session.compression_events.push(prior_event);

    for index in 0..4 {
        session.messages.push(Message::user(format!(
            "second-boundary-user-{index} {}",
            "bounded evidence ".repeat(24)
        )));
        session.messages.push(Message::assistant(
            format!(
                "second-boundary-assistant-{index} {}",
                "bounded response ".repeat(24)
            ),
            None,
        ));
    }

    let (persistence, _) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, _) = recording_llm();
    let counter = TiktokenTokenCounter::default();
    let accounting_budget =
        TokenBudget::with_safety_margin(100_000, 0, BudgetStrategy::default(), 0);
    let frame = build_retrieval_window_accounting_frame(
        &session,
        &config,
        "test-model",
        &session.id,
        &tool_schemas,
        &llm,
        &accounting_budget,
        &counter,
    )
    .await
    .expect("a second retrieval boundary should be account-able");

    assert!(frame.existing_history_boundary_tokens > 0);
    let incremental_reserve = frame
        .history_boundary_reserve_tokens
        .saturating_sub(frame.existing_history_boundary_tokens);
    assert!(incremental_reserve < frame.history_boundary_reserve_tokens);
    assert_eq!(
        frame.accounting.fixed_prompt_tokens,
        frame
            .fixed_prompt_tokens_before_boundary_reserve
            .saturating_add(incremental_reserve),
        "the current boundary is already in the provider projection and must not be reserved twice"
    );

    let policy = RetrievalWindowPolicy {
        min_recent_user_turns: 1,
        target_usage_percent: 100,
    };
    let unlimited_budget =
        TokenBudget::with_safety_margin(u32::MAX, 0, BudgetStrategy::default(), 0);
    let active_tokens_before = match build_retrieval_window_candidate_plan_with_token_accounting(
        &session,
        &unlimited_budget,
        policy,
        &frame.accounting,
    ) {
        Err(RetrievalWindowPlanError::TargetAlreadySatisfied { active_tokens, .. }) => {
            active_tokens
        }
        other => panic!("unlimited target should expose active token accounting: {other:?}"),
    };
    let one_group_probe_budget = TokenBudget::with_safety_margin(
        active_tokens_before.saturating_sub(1),
        0,
        BudgetStrategy::default(),
        0,
    );
    let one_group_probe = build_retrieval_window_candidate_plan_with_token_accounting(
        &session,
        &one_group_probe_budget,
        policy,
        &frame.accounting,
    )
    .expect("one complete old group should satisfy a target just below current usage");
    assert_eq!(one_group_probe.archive_group_count, 1);

    let exact_one_group_budget = TokenBudget::with_safety_margin(
        one_group_probe.projected_active_tokens_after,
        0,
        BudgetStrategy::default(),
        0,
    );
    let exact_plan = build_retrieval_window_candidate_plan_with_token_accounting(
        &session,
        &exact_one_group_budget,
        policy,
        &frame.accounting,
    )
    .expect("the incremental boundary reserve should keep the one-group plan exact");
    assert_eq!(exact_plan.archive_group_count, 1);

    let mut double_counted = frame.accounting.clone();
    double_counted.fixed_prompt_tokens = double_counted
        .fixed_prompt_tokens
        .saturating_add(frame.existing_history_boundary_tokens);
    let double_counted_plan = build_retrieval_window_candidate_plan_with_token_accounting(
        &session,
        &exact_one_group_budget,
        policy,
        &double_counted,
    );
    assert!(
        matches!(
            &double_counted_plan,
            Ok(plan) if plan.archive_group_count > exact_plan.archive_group_count
        ) || matches!(
            &double_counted_plan,
            Err(RetrievalWindowPlanError::ProtectedContentExceedsTarget { .. })
        ),
        "double-counting the existing boundary must demonstrably over-archive or reject this exact target"
    );
}

#[tokio::test]
async fn retrieval_window_pre_turn_checkpoints_before_publication_and_never_summarizes() {
    let mut session = retrieval_window_session("retrieval-success");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp-before-archive".to_string(),
    );
    let active_before = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .count();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-success",
        &tool_schemas,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("retrieval-window preparation should succeed");

    assert!(session.conversation_summary.is_none());
    assert!(
        !session
            .metadata
            .contains_key("responses.previous_response_id"),
        "the durable archive boundary must invalidate pre-archive Responses continuation"
    );
    assert_eq!(
        session
            .compression_events
            .iter()
            .filter(|event| event.kind == bamboo_domain::CompressionEventKind::RetrievalWindow)
            .count(),
        1
    );
    let archived_ids = session
        .messages
        .iter()
        .filter(|message| message.compressed)
        .map(|message| message.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert!(!archived_ids.is_empty());
    assert!(archived_ids.len() < active_before);
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .all(|message| !archived_ids.contains(message.id.as_str())));
    assert!(prepared.prepared_context.compressed_message_ids.is_empty());
    assert!(
        crate::runtime::runner::session_setup::prompt_envelope::build_history_boundary_context_block(
            &session
        )
        .is_some()
    );
    let state = session
        .model_context_state
        .as_ref()
        .expect("archive should create a model-context reset boundary");
    assert_eq!(
        state.last_reset_reason,
        Some(ModelContextResetReason::Compression)
    );
    assert!(state.prefix_epoch > 0);

    let captured = checkpoints
        .lock()
        .expect("checkpoint list lock should not be poisoned");
    assert_eq!(captured.len(), 1);
    assert_eq!(
        serde_json::to_value(&captured[0]).unwrap(),
        serde_json::to_value(&session).unwrap(),
        "live publication must equal the durably checkpointed candidate"
    );
    drop(captured);

    drop(event_tx);
    let events: Vec<AgentEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    let archived_events = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ContextArchived {
                messages_archived,
                reset_reason,
                ..
            } => Some((*messages_archived, reset_reason.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(archived_events.len(), 1);
    assert_eq!(archived_events[0].0, archived_ids.len());
    assert_eq!(archived_events[0].1, "compression");
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::ContextSummarized { .. })));
    assert!(model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_reclaims_provider_native_transcript_before_candidate_fit() {
    let mut session = retrieval_window_session("retrieval-native-reclaim");
    let anchor = session.messages[2].id.clone();
    let provider_name = "openai-retrieval-test";
    let provider_type = "openai";
    let boundary = provider_transcript_boundary_sha256(Some(provider_name), Some(provider_type))
        .expect("provider boundary should be derived");
    session
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &boundary,
        )
        .expect("provider route should activate");
    let reasoning = ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider,
        ProviderTranscriptAuthor::Model,
        serde_json::json!({
            "type": "reasoning",
            "id": "rs_retrieval_native_reclaim",
            "status": "completed",
            "summary": [{
                "type": "summary_text",
                "text": "provider native reasoning evidence ".repeat(5_000)
            }],
            "encrypted_content": "opaque"
        }),
    )
    .expect("reasoning item should validate");
    let search_call = ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider,
        ProviderTranscriptAuthor::Model,
        serde_json::json!({
            "type": "tool_search_call",
            "id": "tsc_retrieval_native_reclaim",
            "execution": "client",
            "call_id": "search_retrieval_native_reclaim",
            "status": "completed",
            "arguments": {"query": "history"}
        }),
    )
    .expect("tool-search item should validate");
    session
        .append_provider_transcript_group(&anchor, None, vec![reasoning, search_call])
        .expect("provider-native group should append");

    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.provider_name = Some(provider_name.to_string());
    config.provider_type = Some(provider_type.to_string());
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, model_calls) = recording_llm();

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-native-reclaim",
        &tool_schemas,
        &llm,
        None,
    )
    .await
    .expect("native transcript bytes removed by the boundary must not block candidate fit");

    let event = session
        .compression_events
        .last()
        .expect("retrieval-window event should be recorded");
    assert_eq!(
        event.kind,
        bamboo_domain::CompressionEventKind::RetrievalWindow
    );
    assert!(event.retrieval_boundary_reclaimed_tokens > 0);
    assert!(event.retrieval_active_tokens_before > event.retrieval_target_tokens);
    assert!(event.retrieval_active_tokens_after <= event.retrieval_target_tokens);
    assert_eq!(
        session.provider_transcript.last_reset_reason(),
        Some(ProviderTranscriptResetReason::Compression)
    );
    assert!(session
        .provider_transcript
        .replayable_groups(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &boundary,
        )
        .is_empty());
    assert!(session.messages.iter().any(|message| message.compressed));
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert!(model_calls.lock().expect("model call lock").is_empty());

    let projected = super::super::stream_execution::project_request_usage(
        &session,
        &prepared.prepared_context,
        &config,
        &tool_schemas,
        "test-model",
        &llm,
    )
    .await
    .expect("committed request should remain projectable");
    assert!(projected.input_tokens <= event.retrieval_target_tokens);
}

#[tokio::test]
async fn retrieval_window_replans_after_concurrent_durable_suffix_before_dispatch() {
    let mut session = retrieval_window_session("retrieval-rebase");
    let fixture = RebaseOnceRetrievalPersistence::fixture();
    let config = retrieval_window_config(Arc::clone(&fixture.persistence));
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-rebase",
        &tool_schemas,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("a concurrent suffix should be rebased and replanned");

    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    assert!(session
        .messages
        .iter()
        .any(|message| message.id == "concurrent-durable-suffix" && !message.compressed));
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .any(|message| message.id == "concurrent-durable-suffix"));
    assert_eq!(
        session
            .compression_events
            .iter()
            .filter(|event| event.kind == bamboo_domain::CompressionEventKind::RetrievalWindow)
            .count(),
        1
    );
    let discarded_event_id = fixture
        .first_archive_event_id
        .lock()
        .expect("event id lock should not be poisoned")
        .clone()
        .expect("first staged archive should have an event id");
    assert!(session
        .messages
        .iter()
        .all(|message| message.compressed_by_event_id.as_deref()
            != Some(discarded_event_id.as_str())));
    let checkpoints = fixture
        .checkpoints
        .lock()
        .expect("checkpoint list lock should not be poisoned");
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        serde_json::to_value(&checkpoints[0]).unwrap(),
        serde_json::to_value(&session).unwrap()
    );
    drop(checkpoints);

    drop(event_tx);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::ContextArchived { .. }))
            .count(),
        1
    );
    assert!(model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_summary_fallback_uses_latest_rebased_session() {
    let mut session = retrieval_window_session("retrieval-rebase-fallback");
    let fixture = RebaseOnceRetrievalPersistence::fixture();
    let mut config = retrieval_window_config(Arc::clone(&fixture.persistence));
    config.context_management.retrieval_window.fallback_strategy =
        ContextManagementFallbackStrategy::Summary;
    config.background_model_name = Some("summary-model".to_string());
    let (summary_llm, summary_calls) = recording_llm();
    config.background_model_provider = Some(summary_llm);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let llm: Arc<dyn LLMProvider> = Arc::new(ExpandingFootprintProvider {
        projection_calls: AtomicUsize::new(0),
        // Preflight plus the first and rebased attempts make this the second
        // attempt's exact retained-request projection.
        expanding_projection_call: 6,
    });

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-rebase-fallback",
        &tool_schemas,
        &llm,
        None,
    )
    .await
    .expect("summary fallback should use the authoritative rebased Session");

    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert!(fixture
        .checkpoints
        .lock()
        .expect("checkpoint list lock should not be poisoned")
        .is_empty());
    assert!(session
        .messages
        .iter()
        .any(|message| message.id == "concurrent-durable-suffix"));
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .any(|message| message.id == "concurrent-durable-suffix"));
    assert!(session.conversation_summary.is_some());
    assert!(session
        .compression_events
        .iter()
        .all(|event| event.kind == bamboo_domain::CompressionEventKind::Summary));
    assert!(!summary_calls
        .lock()
        .expect("model call lock should not be poisoned")
        .is_empty());
}

#[tokio::test]
async fn retrieval_window_rejects_a_preexisting_summary_before_pressure_or_side_effects() {
    let mut session = Session::new("retrieval-existing-summary", "test-model");
    session.messages.push(Message::system("retrieval system"));
    session.messages.push(Message::user("small active request"));
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "legacy durable summary",
        4,
        80,
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-existing-summary",
        &[],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect_err("strategy transition must reject an existing summary immediately");

    assert!(error
        .to_string()
        .contains("already has a conversation summary"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn retrieval_window_missing_history_capability_fails_without_mutating_or_checkpointing() {
    let mut session = retrieval_window_session("retrieval-missing-history");
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-missing-history",
        &[],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect_err("missing exact history capability must fail closed");

    assert!(error.to_string().contains("session_history_current"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn retrieval_window_defers_archival_for_explicit_skill_activation_round() {
    let mut session = retrieval_window_session("retrieval-skill-activation");
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        "[\"review\"]".to_string(),
    );
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema(), load_skill_tool_schema()];
    let effective = super::super::stream_execution::effective_tool_schemas(&session, &tool_schemas);
    assert_eq!(
        effective
            .iter()
            .map(|schema| schema.function.name.as_str())
            .collect::<Vec<_>>(),
        vec!["load_skill"]
    );
    let (llm, model_calls) = recording_llm();

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-skill-activation",
        &tool_schemas,
        &llm,
        None,
    )
    .await
    .expect("the required load_skill setup round must remain dispatchable");

    assert!(
        crate::runtime::runner::session_setup::skill_context::explicit_activation_pending(&session)
    );
    assert!(session.messages.iter().all(|message| !message.compressed));
    assert!(session.compression_events.is_empty());
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(!prepared.prepared_context.messages.is_empty());
}

#[tokio::test]
async fn retrieval_window_below_trigger_does_not_require_history_capability() {
    let mut session = Session::new("retrieval-below-trigger", "test-model");
    session.messages.push(Message::system("retrieval system"));
    session.messages.push(Message::user("small active request"));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-below-trigger",
        &[],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("below-trigger requests must not require retrieval capability");

    assert!(!prepared.prepared_context.truncation_occurred);
    assert!(prepared.prepared_context.compressed_message_ids.is_empty());
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn retrieval_window_capped_trigger_stays_above_capped_target() {
    let budget = TokenBudget::with_safety_margin(1_000, 200, BudgetStrategy::default(), 0);
    assert_eq!(budget.max_request_input_tokens(), 800);

    let mut config = AgentLoopConfig::default();
    config.context_management = ContextManagementConfig {
        strategy: ContextManagementStrategy::RetrievalWindow,
        retrieval_window: RetrievalWindowContextConfig {
            target_usage_ratio: 0.90,
            trigger_usage_ratio: 0.95,
            ..Default::default()
        },
    };

    assert_eq!(
        config.context_management.retrieval_target_usage_percent(),
        90
    );
    assert_eq!(
        super::retrieval_window_trigger_tokens(&config, &budget),
        801
    );
}

#[tokio::test]
async fn retrieval_window_missing_history_capability_precedes_vision_fallback_dispatch() {
    let mut session = retrieval_window_session("retrieval-missing-history-image");
    session.messages.push(Message::user_with_parts(
        "latest image evidence",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "https://example.com/latest.png".to_string(),
                detail: Some("high".to_string()),
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    ));
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.image_fallback = Some(ImageFallbackConfig {
        mode: ImageFallbackMode::Vision,
        vision_model: Some("vision-test".to_string()),
    });
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-missing-history-image",
        &[],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect_err("capability preflight must fail before vision fallback dispatch");

    assert!(error.to_string().contains("session_history_current"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn retrieval_window_checkpoint_failure_leaves_live_session_byte_identical() {
    let mut session = retrieval_window_session("retrieval-checkpoint-failure");
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::failing();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-checkpoint-failure",
        &tool_schemas,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect_err("checkpoint failure must fail closed");

    assert!(error.to_string().contains("durable checkpoint failed"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn retrieval_window_post_archive_projection_failure_discards_staged_state() {
    let mut session = retrieval_window_session("retrieval-projection-failure");
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let llm: Arc<dyn LLMProvider> = Arc::new(ExpandingFootprintProvider {
        projection_calls: AtomicUsize::new(0),
        expanding_projection_call: 3,
    });
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-projection-failure",
        &tool_schemas,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect_err("expanded retained request must fail before checkpoint");

    assert!(error.to_string().contains("exact retained request exceeds"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn retrieval_window_native_image_without_complete_cost_fails_before_mutation() {
    let mut session = retrieval_window_session("retrieval-image-cost");
    session.messages.push(Message::user_with_parts(
        "latest image evidence",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "data:image/png;base64,AA==".to_string(),
                detail: Some("high".to_string()),
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    ));
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-image-cost",
        &tool_schemas,
        &llm,
        None,
    )
    .await
    .expect_err("provider-native image cost must fail closed");

    assert!(error.to_string().contains("active image message"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_compact_stays_summary_specific_while_critical_overflow_archives() {
    let mut session = retrieval_window_session("retrieval-critical-route");
    session.force_manual_compression = Some("preserve evidence".to_string());
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let (llm, model_calls) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let manual_error = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "retrieval-critical-route",
        &[retrieval_history_tool_schema()],
        &llm,
        Some(&event_tx),
        "mid-turn",
    )
    .await
    .expect_err("manual summary tool must be rejected");
    assert!(manual_error.to_string().contains("compact_context"));
    assert!(session.force_manual_compression.is_none());

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-critical-route",
        &[retrieval_history_tool_schema()],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("critical overflow should use the retrieval archive boundary");
    assert!(applied);
    assert!(session.conversation_summary.is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    let event = session
        .compression_events
        .last()
        .expect("critical archive event");
    assert_eq!(event.trigger_type, CompressionTriggerType::CriticalOverflow);
    drop(event_tx);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::ContextArchived { trigger_type, .. } if trigger_type == "critical_overflow"
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::ContextSummarized { .. })));
    assert!(model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_manual_archive_bypasses_auto_trigger_and_is_restart_idempotent() {
    let mut session = retrieval_window_session("retrieval-manual-archive");
    session.token_budget = Some(TokenBudget::with_safety_margin(
        100_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp-before-manual-archive".to_string(),
    );
    append_archive_context_request(&mut session, "call-manual-archive");
    let manual_result_id = session
        .messages
        .last()
        .expect("manual archive result")
        .id
        .clone();
    let raw_before = session
        .messages
        .iter()
        .map(|message| {
            (
                message.id.clone(),
                message.role.clone(),
                message.content.clone(),
                message.tool_calls.clone(),
                message.tool_call_id.clone(),
            )
        })
        .collect::<Vec<_>>();

    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config
        .context_management
        .retrieval_window
        .trigger_usage_ratio = 0.90;
    config
        .context_management
        .retrieval_window
        .target_usage_ratio = 0.30;
    let (llm, model_calls) = recording_llm();
    let tools = vec![retrieval_history_tool_schema()];
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-archive",
        &tools,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("manual archive should apply below the automatic trigger");

    let archive = session
        .compression_events
        .last()
        .expect("manual archive event");
    assert_eq!(archive.trigger_type, CompressionTriggerType::Manual);
    assert!(archive.retrieval_active_tokens_before < 90_000);
    assert!(archive.retrieval_active_tokens_before > archive.retrieval_target_tokens);
    assert_eq!(
        session
            .metadata
            .get(LAST_MANUAL_ARCHIVE_OCCURRENCE_KEY)
            .and_then(|value| serde_json::from_str::<ResponseOccurrence>(value).ok()),
        Some(ResponseOccurrence {
            tool_call_id: "call-manual-archive".to_string(),
            tool_result_message_id: manual_result_id,
            permission_generation: None,
        })
    );
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .all(|message| !message.compressed));
    let raw_after = session
        .messages
        .iter()
        .map(|message| {
            (
                message.id.clone(),
                message.role.clone(),
                message.content.clone(),
                message.tool_calls.clone(),
                message.tool_call_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        raw_after, raw_before,
        "manual archive must preserve raw messages"
    );
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert!(model_calls.lock().expect("model call lock").is_empty());

    let serialized = serde_json::to_vec(&session).expect("session serializes");
    let mut restarted: Session =
        serde_json::from_slice(&serialized).expect("session should reload exactly");
    assert!(pending_manual_archive_request(&restarted).is_none());
    let before_restart_pass = restarted.compression_events.len();
    prepare_round_context(
        &mut restarted,
        &config,
        "test-model",
        "retrieval-manual-archive",
        &tools,
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("a consumed manual request must not replay after restart");
    assert_eq!(restarted.compression_events.len(), before_restart_pass);
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);

    drop(event_tx);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                AgentEvent::ContextArchived { trigger_type, .. } if trigger_type == "manual"
            ))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::ContextSummarized { .. })));
}

#[tokio::test]
async fn retrieval_window_mid_turn_manual_archive_uses_sticky_request_catalog() {
    let mut session = retrieval_window_session("retrieval-manual-sticky-catalog");
    append_archive_context_request(&mut session, "call-manual-sticky-catalog");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let catalogs = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(StickyFallbackCatalogProvider {
        catalogs: Arc::clone(&catalogs),
    });
    let mut deferred = retrieval_history_tool_schema();
    deferred.function.name = "Glob".to_string();
    deferred.function.description = "deferred schema body ".repeat(20_000);
    let tools = vec![retrieval_history_tool_schema(), deferred];

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-sticky-catalog",
        &tools,
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("manual archive must account against the actual StickyFallback request catalog");

    assert!(applied);
    assert_eq!(checkpoints.lock().expect("checkpoint list").len(), 1);
    let catalogs = catalogs.lock().expect("catalog lock");
    assert!(!catalogs.is_empty());
    assert!(catalogs.iter().all(|catalog| {
        catalog
            == &vec![
                "session_history_current".to_string(),
                "discover_capabilities".to_string(),
            ]
    }));
    assert!(session.messages.iter().any(|message| message.compressed));
}

#[tokio::test]
async fn retrieval_window_manual_noop_is_consumed_durably_once() {
    let mut session = Session::new("retrieval-manual-noop", "test-model");
    session.add_message(Message::system("retrieval system"));
    session.add_message(Message::user("small request"));
    session.add_message(Message::assistant("small response", None));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        100_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    append_archive_context_request(&mut session, "call-manual-noop");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-noop",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("already-at-target manual archive should be a durable no-op");
    assert!(session.compression_events.is_empty());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert!(pending_manual_archive_request(&session).is_none());

    let mut restarted: Session = serde_json::from_slice(&serde_json::to_vec(&session).unwrap())
        .expect("no-op marker should survive restart");
    prepare_round_context(
        &mut restarted,
        &config,
        "test-model",
        "retrieval-manual-noop",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("consumed no-op must not replay");
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
}

#[tokio::test]
async fn retrieval_window_manual_noop_rebases_before_consuming_concurrent_metadata() {
    let mut session = Session::new("retrieval-manual-noop-rebase", "test-model");
    session.add_message(Message::system("retrieval system"));
    session.add_message(Message::user("small request"));
    session.add_message(Message::assistant("small response", None));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        100_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    append_archive_context_request(&mut session, "call-manual-noop-rebase");
    let fixture = DurableBaseCheckingPersistence::fixture(session.clone());
    fixture
        .durable
        .lock()
        .expect("durable Session lock")
        .set_pending_injected_messages(vec![serde_json::json!({
            "id": "bash-1",
            "status": "completed",
        })]);
    let config = retrieval_window_config(Arc::clone(&fixture.persistence));
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-noop-rebase",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("no-op consumption should rebase and retry from concurrent metadata");

    assert_eq!(fixture.runtime_checkpoints.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .manual_consumption_checkpoints
            .load(Ordering::SeqCst),
        2
    );
    assert_eq!(
        session.pending_injected_messages(),
        Some(vec![serde_json::json!({
            "id": "bash-1",
            "status": "completed",
        })])
    );
    assert!(pending_manual_archive_request(&session).is_none());
    let durable = fixture.durable.lock().expect("durable Session lock");
    assert_eq!(
        durable.pending_injected_messages(),
        Some(vec![serde_json::json!({
            "id": "bash-1",
            "status": "completed",
        })])
    );
    assert!(pending_manual_archive_request(&durable).is_none());
}

#[tokio::test]
async fn retrieval_window_manual_checkpoint_failure_remains_retryable() {
    let mut session = retrieval_window_session("retrieval-manual-retry");
    append_archive_context_request(&mut session, "call-manual-retry");
    let before = serde_json::to_vec(&session).unwrap();
    let (failing_persistence, failed_checkpoints) = RetrievalCheckpointPersistence::failing();
    let failing_config = retrieval_window_config(failing_persistence);
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    let error = prepare_round_context(
        &mut session,
        &failing_config,
        "test-model",
        "retrieval-manual-retry",
        &tools,
        &llm,
        None,
    )
    .await
    .expect_err("failed checkpoint must not consume the manual request");
    assert!(error.to_string().contains("durable checkpoint failed"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert_eq!(
        pending_manual_archive_request(&session)
            .as_ref()
            .map(|request| request.tool_call_id.as_str()),
        Some("call-manual-retry")
    );
    assert!(failed_checkpoints
        .lock()
        .expect("checkpoint list lock")
        .is_empty());

    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-retry",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("the same request should succeed at the next safe boundary");
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(
        session
            .compression_events
            .last()
            .expect("retry archive event")
            .trigger_type,
        CompressionTriggerType::Manual
    );
}

#[tokio::test]
async fn archive_context_is_rejected_and_consumed_under_summary_strategy() {
    let mut session = Session::new("summary-rejects-archive", "test-model");
    session.add_message(Message::user("ordinary summary-mode turn"));
    append_archive_context_request(&mut session, "call-summary-archive");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = AgentLoopConfig {
        persistence: Some(persistence),
        ..Default::default()
    };
    let llm = noop_llm();
    let (event_tx, mut event_rx) = mpsc::channel(4);

    let error = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "summary-rejects-archive",
        &[],
        &llm,
        Some(&event_tx),
        "mid-turn",
    )
    .await
    .expect_err("archive_context must not become a summary control");
    assert!(error
        .to_string()
        .contains("requires context_management.strategy=retrieval_window"));
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert_archive_rejection_visible(
        &session,
        "call-summary-archive",
        "requires context_management.strategy=retrieval_window",
    );
    let correction = event_rx
        .recv()
        .await
        .expect("the already-emitted tool success must receive a correction");
    match correction {
        AgentEvent::ToolComplete {
            tool_call_id,
            result,
        } => {
            assert_eq!(tool_call_id, "call-summary-archive");
            assert!(!result.success);
            assert!(result
                .result
                .contains("requires context_management.strategy=retrieval_window"));
        }
        other => panic!("unexpected archive rejection correction: {other:?}"),
    }
    let durable_result = session
        .messages
        .iter_mut()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some("call-summary-archive"))
        .expect("durable archive_context result");
    durable_result.tool_success = Some(true);
    durable_result.content = "Retrieval-window archive requested".to_string();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "summary-rejects-archive",
        &[],
        &llm,
        None,
    )
    .await
    .expect("a consumed rejection must allow the next model round");
    let visible_result = prepared
        .prepared_context
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some("call-summary-archive"))
        .expect("provider-visible archive_context result");
    assert_eq!(visible_result.tool_success, Some(false));
    assert!(visible_result
        .content
        .contains("requires context_management.strategy=retrieval_window"));
    assert!(session.compression_events.is_empty());
    assert!(session.conversation_summary.is_none());
}

#[tokio::test]
async fn archive_context_with_existing_summary_is_permanently_rejected_and_consumed() {
    let mut session = retrieval_window_session("retrieval-summary-rejects-archive");
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "existing summary",
        4,
        120,
    ));
    append_archive_context_request(&mut session, "call-existing-summary-archive");
    let before = serde_json::to_vec(&session).unwrap();
    let (failing_persistence, failed_checkpoints) = RetrievalCheckpointPersistence::failing();
    let mut failing_config = retrieval_window_config(failing_persistence);
    failing_config
        .context_management
        .retrieval_window
        .fallback_strategy = bamboo_config::ContextManagementFallbackStrategy::Summary;
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    let checkpoint_error = prepare_round_context(
        &mut session,
        &failing_config,
        "test-model",
        "retrieval-summary-rejects-archive",
        &tools,
        &llm,
        None,
    )
    .await
    .expect_err("failed rejection checkpoint must leave the request retryable");
    assert!(checkpoint_error.to_string().contains("checkpoint failed"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert_eq!(
        pending_manual_archive_request(&session)
            .as_ref()
            .map(|request| request.tool_call_id.as_str()),
        Some("call-existing-summary-archive")
    );
    assert!(failed_checkpoints
        .lock()
        .expect("checkpoint list lock")
        .is_empty());

    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.context_management.retrieval_window.fallback_strategy =
        bamboo_config::ContextManagementFallbackStrategy::Summary;

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-summary-rejects-archive",
        &tools,
        &llm,
        None,
    )
    .await
    .expect_err("a retrieval boundary cannot be layered over a summary");

    assert!(error
        .to_string()
        .contains("already has a conversation summary"));
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert_archive_rejection_visible(
        &session,
        "call-existing-summary-archive",
        "already has a conversation summary",
    );
    assert!(session.compression_events.is_empty());
    assert_eq!(
        session
            .conversation_summary
            .as_ref()
            .map(|summary| summary.content.as_str()),
        Some("existing summary")
    );
}

#[tokio::test]
async fn archive_context_existing_summary_is_consumed_before_no_fallback_gate() {
    let mut session = retrieval_window_session("retrieval-summary-no-fallback-archive");
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "existing summary",
        4,
        120,
    ));
    append_archive_context_request(&mut session, "call-summary-no-fallback");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let llm = noop_llm();

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-summary-no-fallback-archive",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect_err("manual rejection must run before the general summary gate");

    assert!(error
        .to_string()
        .contains("rejected request was surfaced and durably consumed"));
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert_archive_rejection_visible(
        &session,
        "call-summary-no-fallback",
        "already has a conversation summary",
    );
    assert!(session.compression_events.is_empty());
}

#[tokio::test]
async fn archive_context_protected_target_failure_is_consumed_after_retryable_checkpoint() {
    let mut session = Session::new("retrieval-manual-protected-reject", "test-model");
    session.add_message(Message::system("retrieval system"));
    session.add_message(Message::user("old eligible turn"));
    session.add_message(Message::assistant("old eligible response", None));
    session.add_message(Message::user("protected latest evidence ".repeat(4_000)));
    session.add_message(Message::assistant(
        "protected latest response ".repeat(4_000),
        None,
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        4_000,
        0,
        BudgetStrategy::default(),
        0,
    ));
    append_archive_context_request(&mut session, "call-protected-reject");
    let before = serde_json::to_vec(&session).unwrap();
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    let (failing_persistence, failed_checkpoints) = RetrievalCheckpointPersistence::failing();
    let mut failing_config = retrieval_window_config(failing_persistence);
    failing_config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let checkpoint_error = prepare_round_context(
        &mut session,
        &failing_config,
        "test-model",
        "retrieval-manual-protected-reject",
        &tools,
        &llm,
        None,
    )
    .await
    .expect_err("failed rejection checkpoint must keep the manual request retryable");
    assert!(checkpoint_error.to_string().contains("checkpoint failed"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert_eq!(
        pending_manual_archive_request(&session)
            .as_ref()
            .map(|request| request.tool_call_id.as_str()),
        Some("call-protected-reject")
    );
    assert!(failed_checkpoints
        .lock()
        .expect("checkpoint list lock")
        .is_empty());

    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-protected-reject",
        &tools,
        &llm,
        None,
    )
    .await
    .expect_err("protected newest content must permanently reject this manual request");

    assert!(error.to_string().contains("protected active content"));
    assert!(error.to_string().contains("durably consumed"));
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert_archive_rejection_visible(
        &session,
        "call-protected-reject",
        "protected active content",
    );
    assert!(session.compression_events.is_empty());
}

#[tokio::test]
async fn archive_context_nothing_to_archive_failure_is_consumed_once() {
    let mut session = Session::new("retrieval-manual-no-candidate", "test-model");
    session.add_message(Message::system("retrieval system"));
    session.add_message(Message::user("only protected user turn ".repeat(4_000)));
    session.add_message(Message::assistant(
        "only protected assistant turn ".repeat(4_000),
        None,
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        4_000,
        0,
        BudgetStrategy::default(),
        0,
    ));
    append_archive_context_request(&mut session, "call-no-candidate");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let llm = noop_llm();

    let error = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-manual-no-candidate",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect_err("a fully protected window has no permanent archive candidate");

    assert!(error
        .to_string()
        .contains("no eligible active logical group"));
    assert!(error.to_string().contains("durably consumed"));
    assert!(pending_manual_archive_request(&session).is_none());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert_archive_rejection_visible(
        &session,
        "call-no-candidate",
        "no eligible active logical group",
    );
    assert!(session.compression_events.is_empty());
}

#[tokio::test]
async fn retrieval_window_critical_overflow_rejects_oversized_latest_turn_transactionally() {
    let mut session = Session::new("retrieval-oversized-latest", "test-model");
    session.add_message(Message::system("retrieval system"));
    session.add_message(Message::user("old compactible turn"));
    session.add_message(Message::assistant("old response", None));
    session.add_message(Message::user("protected latest evidence ".repeat(4_000)));
    session.add_message(Message::assistant(
        "protected latest response ".repeat(4_000),
        None,
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        4_000,
        0,
        BudgetStrategy::default(),
        0,
    ));
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let (llm, model_calls) = recording_llm();

    let error = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-oversized-latest",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect_err("the newest protected turn cannot be split to satisfy the target");
    assert!(error.to_string().contains("protected active content"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_repeated_archive_survives_restart_and_preserves_prior_correlations() {
    let mut session = retrieval_window_session("retrieval-repeat-restart");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let tools = vec![retrieval_history_tool_schema()];
    let llm = noop_llm();

    prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-repeat-restart",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("first automatic boundary should commit");
    let first_event = serde_json::to_value(
        session
            .compression_events
            .first()
            .expect("first retrieval event"),
    )
    .unwrap();
    let first_correlations = session
        .messages
        .iter()
        .filter_map(|message| {
            message
                .compressed_by_event_id
                .as_ref()
                .map(|event_id| (message.id.clone(), event_id.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let first_epoch = session
        .model_context_state
        .as_ref()
        .expect("first model-context boundary")
        .prefix_epoch;
    let first_provider_epoch = session.provider_transcript.epoch();
    let first_boundary =
        crate::runtime::runner::session_setup::prompt_envelope::build_history_boundary_context_block(
            &session,
        )
        .expect("first history boundary")
        .content;

    let mut restarted: Session = serde_json::from_slice(&serde_json::to_vec(&session).unwrap())
        .expect("first boundary should survive serialization");
    assert_eq!(
        crate::runtime::runner::session_setup::prompt_envelope::build_history_boundary_context_block(
            &restarted,
        )
        .expect("restarted history boundary")
        .content,
        first_boundary
    );
    // Simulate the successful provider request between the two boundaries.
    // Seeding the pending model-context epoch is what makes a later archive a
    // distinct observable prefix epoch instead of a coalesced pre-dispatch
    // rewrite.
    let first_request_transcript = restarted
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .cloned()
        .collect::<Vec<_>>();
    super::super::context_ledger::reconcile_model_context(
        &mut restarted,
        Vec::new(),
        &first_request_transcript,
        "retrieval-repeat-scope".to_string(),
        false,
    );
    let provider_boundary =
        provider_transcript_boundary_sha256(Some("openai-repeat"), Some("openai"))
            .expect("provider boundary");
    restarted
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &provider_boundary,
        )
        .expect("provider route should activate");
    let provider_item = ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider,
        ProviderTranscriptAuthor::Model,
        serde_json::json!({
            "type": "tool_search_call",
            "id": "tsc_between_retrieval_boundaries",
            "execution": "client",
            "call_id": "search_between_retrieval_boundaries",
            "status": "completed",
            "arguments": {"query": "older evidence"}
        }),
    )
    .expect("provider item should validate");
    let provider_anchor = restarted
        .messages
        .iter()
        .rev()
        .find(|message| !message.compressed)
        .expect("active provider anchor")
        .id
        .clone();
    restarted
        .append_provider_transcript_group(&provider_anchor, None, vec![provider_item])
        .expect("provider group should append");
    for index in 0..6 {
        restarted.add_message(Message::user(format!(
            "new-user-{index} {}",
            "new exact evidence ".repeat(450)
        )));
        restarted.add_message(Message::assistant(
            format!(
                "new-assistant-{index} {}",
                "new exact response ".repeat(450)
            ),
            None,
        ));
    }
    restarted.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp-between-boundaries".to_string(),
    );

    prepare_round_context(
        &mut restarted,
        &config,
        "test-model",
        "retrieval-repeat-restart",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("second post-restart boundary should commit");

    assert_eq!(restarted.compression_events.len(), 2);
    assert_eq!(
        serde_json::to_value(&restarted.compression_events[0]).unwrap(),
        first_event,
        "a repeated boundary must not rewrite prior evidence"
    );
    for (message_id, event_id) in &first_correlations {
        let message = restarted
            .messages
            .iter()
            .find(|message| &message.id == message_id)
            .expect("prior raw message remains present");
        assert_eq!(message.compressed_by_event_id.as_ref(), Some(event_id));
    }
    assert!(restarted.messages.iter().any(|message| {
        message.compressed_by_event_id.as_deref()
            == restarted
                .compression_events
                .get(1)
                .map(|event| event.id.as_str())
    }));
    assert!(
        restarted
            .model_context_state
            .as_ref()
            .expect("second model-context boundary")
            .prefix_epoch
            > first_epoch
    );
    assert!(restarted.provider_transcript.epoch() > first_provider_epoch);
    assert!(!restarted
        .metadata
        .contains_key("responses.previous_response_id"));
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 2);
}

#[tokio::test]
async fn retrieval_window_summary_fallback_does_not_replace_automatic_mid_turn_deferral() {
    let mut session = retrieval_window_session("retrieval-fallback-deferral");
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.context_management.retrieval_window.fallback_strategy =
        ContextManagementFallbackStrategy::Summary;
    config.background_model_name = Some("summary-model".to_string());
    let tool_schemas = vec![retrieval_history_tool_schema()];
    let (llm, model_calls) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "retrieval-fallback-deferral",
        &tool_schemas,
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("automatic mid-turn pressure should defer to retrieval pre-turn");

    assert!(!applied);
    assert!(session.conversation_summary.is_none());
    assert!(session.compression_events.is_empty());
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
    assert!(model_calls.lock().expect("model call lock").is_empty());

    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "retrieval-fallback-deferral",
        &tool_schemas,
        &llm,
        None,
    )
    .await
    .expect("the next pre-turn boundary should use retrieval-window");

    assert!(session.conversation_summary.is_none());
    assert!(session
        .compression_events
        .iter()
        .any(|event| { event.kind == bamboo_domain::CompressionEventKind::RetrievalWindow }));
    assert!(session.messages.iter().any(|message| message.compressed));
    assert!(!prepared.prepared_context.messages.is_empty());
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
    assert!(model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_uses_summary_only_when_fallback_is_explicit() {
    let mut session = retrieval_window_session("retrieval-explicit-fallback");
    session.force_manual_compression = Some("preserve exact decisions".to_string());
    let (persistence, _checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.context_management.retrieval_window.fallback_strategy =
        ContextManagementFallbackStrategy::Summary;
    config.background_model_name = Some("summary-model".to_string());
    let (llm, model_calls) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "retrieval-explicit-fallback",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
        "mid-turn",
    )
    .await
    .expect("explicit summary fallback should preserve the legacy path");

    assert!(applied);
    assert!(session.conversation_summary.is_some());
    assert!(session
        .compression_events
        .iter()
        .all(|event| event.kind == bamboo_domain::CompressionEventKind::Summary));
    assert!(!model_calls.lock().expect("model call lock").is_empty());
}

#[tokio::test]
async fn prepare_round_context_auto_compresses_when_context_window_usage_crosses_trigger() {
    // Host auto-compression now uses a single rule:
    // usage(context_window) >= compression_trigger_percent.
    // Here total_tokens/context_window = 3500/4000 = 87.5% with trigger=80, so it should run.
    let mut session = Session::new("session-cp-force-context-only", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 4000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system(
        crate::runtime::runner::prompt_context::append_core_agent_directives(
            "System prompt",
            crate::runtime::context::CORE_AGENT_DIRECTIVES,
        ),
    ));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    // context_window = 4000
    // total_tokens/context_window = 3500/4000 = 87.5% >= 80%
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 3400,
        total_tokens: 3500,
        max_context_tokens: 4000,
        budget_limit: 4000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let (llm, _) = recording_llm();
    let _prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force-context-only",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        !session.compression_events.is_empty(),
        "host auto compression should run when context-window usage (87.5%) crosses trigger (80%)"
    );
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "messages should be compressed when host auto compression runs"
    );
}

#[tokio::test]
async fn prepare_round_context_skips_host_auto_compression_below_trigger() {
    let mut session = Session::new("session-cp-force-context-low", "test-model");
    session.token_budget = Some(TokenBudget {
        // The test exercises the trigger decision, so its synthetic budget
        // must also accommodate fixed, non-droppable host context.
        max_context_tokens: 2000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..4 {
        session
            .messages
            .push(Message::user(format!("User message {} short text", index)));
        session.messages.push(Message::assistant(
            format!("Assistant response {} short text", index),
            None,
        ));
    }
    // context_window = 2000, usage = 62.5%; history content is also intentionally
    // kept short so estimated usage stays below trigger (80%).
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 1150,
        total_tokens: 1250,
        max_context_tokens: 2000,
        budget_limit: 2000,
        truncation_occurred: true,
        segments_removed: 4,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let llm = noop_llm();
    let _prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "session-cp-force-context-low",
        &[],
        &llm,
        None,
    )
    .await
    .expect("prepare round context");

    assert!(
        session.compression_events.is_empty(),
        "host auto compression should stay off below trigger (80%)"
    );
    assert!(
        !session.messages.iter().any(|m| m.compressed),
        "messages should stay uncompressed below host auto-compression trigger"
    );
}

#[tokio::test]
async fn force_overflow_context_recovery_can_bypass_regular_trigger_gate() {
    let mut session = Session::new("session-cp-overflow-force", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 10_000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 95,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 780,
        total_tokens: 880,
        max_context_tokens: 10_000,
        budget_limit: 10_000,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let mut config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    config.context_management.strategy = ContextManagementStrategy::RetrievalWindow;
    config.context_management.retrieval_window.fallback_strategy =
        ContextManagementFallbackStrategy::Summary;
    let budget = session.token_budget.as_ref().expect("configured budget");
    let exposure = bamboo_compression::estimate_context_compression_exposure(
        &session,
        "test-model",
        Some(budget),
    );
    let trigger_percent = (budget.compression_trigger_context_tokens() as f64
        / budget.max_context_tokens as f64)
        * 100.0;
    assert!(
        exposure.active_usage_percent < trigger_percent,
        "fixture must stay below the ordinary summary trigger: exposure={exposure:?}"
    );
    let (llm, _) = recording_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-cp-overflow-force",
        &[],
        &llm,
        None,
    )
    .await
    .expect("forced overflow recovery should complete");

    assert!(
        applied,
        "forced overflow recovery should bypass the normal trigger gate"
    );
    assert!(!session.compression_events.is_empty());
    assert!(session.messages.iter().any(|m| m.compressed));
}

#[tokio::test]
async fn retrieval_window_provider_overflow_archives_all_eligible_groups_without_fallback() {
    for (case, context_tokens, starts_below_configured_target) in
        [("below", 200_000, true), ("above", 3_000, false)]
    {
        let session_id = format!("retrieval-provider-overflow-{case}-target");
        let mut session = Session::new(&session_id, "test-model");
        session.messages.push(Message::system("retrieval system"));
        for index in 0..4 {
            session.messages.push(Message::user(format!(
                "old user turn {index} {}",
                "small historical evidence ".repeat(8)
            )));
            session.messages.push(Message::assistant(
                format!(
                    "old assistant turn {index} {}",
                    "small historical response ".repeat(8)
                ),
                None,
            ));
        }
        session.token_budget = Some(TokenBudget::with_safety_margin(
            context_tokens,
            0,
            BudgetStrategy::default(),
            0,
        ));
        let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
        let mut config = retrieval_window_config(persistence);
        config
            .context_management
            .retrieval_window
            .min_recent_user_turns = 1;
        config
            .context_management
            .retrieval_window
            .target_usage_ratio = 0.90;
        config.context_management.retrieval_window.fallback_strategy =
            ContextManagementFallbackStrategy::None;
        let (llm, _) = recording_llm();

        let applied = super::force_overflow_context_recovery(
            &mut session,
            &config,
            "test-model",
            &session_id,
            &[retrieval_history_tool_schema()],
            &llm,
            None,
        )
        .await
        .expect("an actual provider overflow must force one retrieval archive");

        assert!(applied);
        assert_eq!(checkpoints.lock().expect("checkpoint list").len(), 1);
        let event = session
            .compression_events
            .last()
            .expect("critical retrieval boundary");
        let configured_target = effective_retrieval_window_target_tokens(
            session.token_budget.as_ref().expect("budget"),
            config.context_management.retrieval_target_usage_percent(),
        );
        assert_eq!(
            event.retrieval_active_tokens_before <= configured_target,
            starts_below_configured_target,
            "fixture must exercise its named side of the local target"
        );
        assert!(event.retrieval_target_tokens < event.retrieval_active_tokens_before);
        assert_eq!(
            event.retrieval_archived_group_count, 3,
            "an authoritative provider overflow must use the maximum safe one-shot headroom"
        );
        assert_eq!(event.retrieval_retained_user_turn_count, 1);
        assert_eq!(event.trigger_type, CompressionTriggerType::CriticalOverflow);
        assert!(session.messages.iter().any(|message| message.compressed));
        assert!(session.conversation_summary.is_none());
    }
}

/// Integration test: multi-round compress → build pressure → re-expose → compress again.
///
/// This verifies the full cycle including the anchor_index==0 fix and
/// token_usage preservation after compression.
#[tokio::test]
async fn multi_round_compression_cycle() {
    use bamboo_compression::{
        apply_compression_plan, build_forced_compression_plan_with_summary,
        estimate_context_compression_exposure,
    };

    let budget = TokenBudget {
        max_context_tokens: 2000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 50,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    };
    let mut session = Session::new("multi-round-compress", "test-model");
    session.token_budget = Some(budget.clone());
    session.add_message(Message::system("You are a helpful assistant"));

    // ---- Round 1: build pressure ----
    for idx in 0..8 {
        session.add_message(Message::user(format!(
            "User question {idx} {}",
            "alpha beta gamma delta ".repeat(10)
        )));
        session.add_message(Message::assistant(
            format!(
                "Assistant response {idx} {}",
                "analyzing files checks plans ".repeat(10)
            ),
            None,
        ));
    }

    // Simulate persisted usage from prepare_hybrid_context
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 50,
        summary_tokens: 0,
        window_tokens: 1700,
        total_tokens: 1750,
        max_context_tokens: 2000,
        budget_limit: 2000, // context_window
        truncation_occurred: true,
        segments_removed: 3,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let exposure1 = estimate_context_compression_exposure(
        &session,
        "test-model",
        session.token_budget.as_ref(),
    );
    assert!(
        exposure1.should_expose_tool,
        "should expose tool on first pressure: usage={:.1}%",
        exposure1.active_usage_percent
    );

    // ---- Compress round 1 ----
    let plan1 = build_forced_compression_plan_with_summary(
        &session,
        "test-model",
        session.token_budget.as_ref(),
        "Summary of rounds 0-7: user asked many questions, assistant analyzed files.".to_string(),
        CompressionTriggerType::Auto,
    )
    .expect("first compression plan should succeed");

    let compressed1 = apply_compression_plan(&mut session, plan1);
    assert!(compressed1 > 0, "first compression should archive messages");

    // token_usage should NOT be None after compression
    assert!(
        session.token_usage.is_some(),
        "token_usage should be preserved (re-estimated) after compression"
    );
    let usage_after_1 = session.token_usage.as_ref().unwrap();
    assert!(
        usage_after_1.budget_limit > 0,
        "budget_limit should be preserved after compression"
    );

    // ---- Round 2: build more pressure after first compression ----
    // Only one User message remains (anchor_index == 0 scenario)
    let user_count_after_1 = session
        .messages
        .iter()
        .filter(|m| !m.compressed && matches!(m.role, Role::User))
        .count();
    // Could be 1 or more depending on anchor — just verify compression happened
    assert!(
        session.messages.iter().any(|m| m.compressed),
        "some messages should be compressed after round 1"
    );

    // Add more messages to build pressure again
    for idx in 0..6 {
        session.add_message(Message::user(format!(
            "Follow-up {idx} {}",
            "more content to fill budget ".repeat(12)
        )));
        session.add_message(Message::assistant(
            format!(
                "Reply {idx} {}",
                "detailed analysis and next steps ".repeat(12)
            ),
            None,
        ));
    }

    // Simulate updated persisted usage
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 50,
        summary_tokens: 100,
        window_tokens: 1650,
        total_tokens: 1800,
        max_context_tokens: 2000,
        budget_limit: 2000,
        truncation_occurred: true,
        segments_removed: 2,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    // ---- Compress round 2 (anchor_index == 0 or small) ----
    let plan2 = build_forced_compression_plan_with_summary(
        &session,
        "test-model",
        session.token_budget.as_ref(),
        format!(
            "Updated summary: rounds 0-7 summarized earlier (user_count_after_first={}). Follow-up rounds 8-13 added.",
            user_count_after_1
        ),
        CompressionTriggerType::Auto,
    )
    .expect("second compression plan should succeed (anchor_index fix)");

    let compressed2 = apply_compression_plan(&mut session, plan2);
    assert!(
        compressed2 > 0,
        "second compression should archive more messages"
    );
    assert!(
        session.compression_events.len() >= 2,
        "should have at least 2 compression events"
    );
    assert!(
        session.token_usage.is_some(),
        "token_usage should be preserved after second compression"
    );
}

#[tokio::test]
async fn degradation_strips_system_sections_in_order() {
    // Degradation only strips sections that still live in the system message:
    // tool_guide -> skill_context -> env_context. External memory and the task
    // list ride volatile blocks now, so their markers are NOT touched here.
    let mut session = Session::new("session-5-level-degrade", "test-model");
    session.messages.push(Message::system(
        "Base prompt\n\
         <!-- BAMBOO_ENV_CONTEXT_START -->\nenv info\n<!-- BAMBOO_ENV_CONTEXT_END -->\n\
         <!-- BAMBOO_TASK_LIST_START -->\ntask items\n<!-- BAMBOO_TASK_LIST_END -->\n\
         <!-- BAMBOO_EXTERNAL_MEMORY_START -->\nmemory notes\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\
         <!-- BAMBOO_SKILL_CONTEXT_START -->\nskill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->"
            .to_string(),
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    // 1st call: strips tool_guide
    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-5-level-degrade",
        &[],
        &llm,
        None,
    )
    .await
    .expect("first degradation");
    assert!(applied);
    let prompt = system_prompt(&session);
    assert!(!prompt.contains("BAMBOO_TOOL_GUIDE"));
    assert!(prompt.contains("BAMBOO_SKILL_CONTEXT"));

    // 2nd call: strips skill_context
    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-5-level-degrade",
        &[],
        &llm,
        None,
    )
    .await
    .expect("second degradation");
    assert!(applied);
    let prompt = system_prompt(&session);
    assert!(!prompt.contains("BAMBOO_SKILL_CONTEXT"));
    assert!(prompt.contains("BAMBOO_ENV_CONTEXT"));

    // 3rd call: strips env_context. External memory + task list markers are left
    // untouched (they are no longer system-message sections).
    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-5-level-degrade",
        &[],
        &llm,
        None,
    )
    .await
    .expect("third degradation");
    assert!(applied);
    let prompt = system_prompt(&session);
    assert!(!prompt.contains("BAMBOO_ENV_CONTEXT"));
    assert!(prompt.contains("BAMBOO_EXTERNAL_MEMORY"));
    assert!(prompt.contains("BAMBOO_TASK_LIST"));
    assert!(prompt.contains("Base prompt"));
}

#[tokio::test]
async fn retrieval_window_overflow_degrades_all_sections_and_archives_in_one_recovery() {
    let mut session = retrieval_window_session("retrieval-overflow-degrade-and-archive");
    session.messages[0].content = "Base prompt\n\
         <!-- BAMBOO_ENV_CONTEXT_START -->\nenv info\n<!-- BAMBOO_ENV_CONTEXT_END -->\n\
         <!-- BAMBOO_SKILL_CONTEXT_START -->\nskill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->"
        .to_string();
    let fixture = DurableBaseCheckingPersistence::fixture(session.clone());
    let config = retrieval_window_config(Arc::clone(&fixture.persistence));
    let llm = noop_llm();
    let (event_tx, mut event_rx) = mpsc::channel(16);

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-overflow-degrade-and-archive",
        &[retrieval_history_tool_schema()],
        &llm,
        Some(&event_tx),
    )
    .await
    .expect("one retrieval recovery must finish degradation and forced archival");

    assert!(applied);
    let prompt = system_prompt(&session);
    assert!(!prompt.contains("BAMBOO_TOOL_GUIDE"));
    assert!(!prompt.contains("BAMBOO_SKILL_CONTEXT"));
    assert!(!prompt.contains("BAMBOO_ENV_CONTEXT"));
    assert_eq!(fixture.runtime_checkpoints.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.prompt_checkpoints.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.retrieval_checkpoints.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .compression_events
            .last()
            .expect("forced archive event")
            .trigger_type,
        CompressionTriggerType::CriticalOverflow
    );
    let durable = fixture.durable.lock().expect("durable Session lock");
    assert_eq!(
        durable
            .compression_events
            .last()
            .expect("durable forced archive event")
            .trigger_type,
        CompressionTriggerType::CriticalOverflow
    );
    assert!(!system_prompt(&durable).contains("BAMBOO_TOOL_GUIDE"));
    drop(durable);

    drop(event_tx);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                AgentEvent::ContextCompressionStatus { status, .. }
                    if status == "degraded_sections"
            ))
            .count(),
        3
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, AgentEvent::ContextArchived { .. })));
}

#[tokio::test]
async fn retrieval_window_overflow_degradation_checkpoint_failure_is_transactional() {
    let mut session = retrieval_window_session("retrieval-overflow-degrade-checkpoint-failure");
    session.messages[0].content = "Base prompt\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->"
        .to_string();
    let before = serde_json::to_vec(&session).unwrap();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::failing();
    let config = retrieval_window_config(persistence);
    let llm = noop_llm();

    let error = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-overflow-degrade-checkpoint-failure",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect_err("failed degradation checkpoint must leave overflow recovery retryable");

    assert!(error
        .to_string()
        .contains("prompt degradation checkpoint failed"));
    assert_eq!(serde_json::to_vec(&session).unwrap(), before);
    assert!(checkpoints.lock().expect("checkpoint list lock").is_empty());
}

#[tokio::test]
async fn retrieval_window_overflow_degradation_only_resets_provider_epoch_durably() {
    let mut session = Session::new("retrieval-overflow-degradation-only", "test-model");
    session.add_message(Message::system(
        "Base prompt\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->",
    ));
    session.add_message(Message::user("small request"));
    session.add_message(Message::assistant("small response", None));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        100_000,
        512,
        BudgetStrategy::default(),
        0,
    ));
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp-before-degradation".to_string(),
    );
    session.model_context_state = Some(ModelContextState {
        prefix_epoch: 4,
        cache_scope_sha256: Some("a".repeat(64)),
        ..ModelContextState::default()
    });
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let config = retrieval_window_config(persistence);
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-overflow-degradation-only",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect("durable prompt degradation alone should permit one provider retry");

    assert!(applied);
    assert!(session.compression_events.is_empty());
    assert!(!system_prompt(&session).contains("BAMBOO_TOOL_GUIDE"));
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
    let state = session
        .model_context_state
        .as_ref()
        .expect("prompt rewrite must reset the model-context epoch");
    assert_eq!(state.prefix_epoch, 5);
    assert_eq!(
        state.last_reset_reason,
        Some(ModelContextResetReason::ExplicitHistoryRewrite)
    );
    let checkpoints = checkpoints.lock().expect("checkpoint list lock");
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0]
            .model_context_state
            .as_ref()
            .and_then(|state| state.last_reset_reason),
        Some(ModelContextResetReason::ExplicitHistoryRewrite)
    );
}

#[tokio::test]
async fn retrieval_window_overflow_retries_degraded_prompt_when_target_is_protected() {
    let mut session = Session::new("retrieval-overflow-degraded-protected", "test-model");
    session.add_message(Message::system(
        "Base prompt\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->",
    ));
    session.add_message(Message::user("old eligible turn"));
    session.add_message(Message::assistant("old eligible response", None));
    session.add_message(Message::user("protected latest evidence ".repeat(4_000)));
    session.add_message(Message::assistant(
        "protected latest response ".repeat(4_000),
        None,
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        4_000,
        0,
        BudgetStrategy::default(),
        0,
    ));
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "retrieval-overflow-degraded-protected",
        &[retrieval_history_tool_schema()],
        &llm,
        None,
    )
    .await
    .expect("durable degradation should still permit the single provider retry");

    assert!(applied);
    assert!(session.compression_events.is_empty());
    assert!(!system_prompt(&session).contains("BAMBOO_TOOL_GUIDE"));
    assert_eq!(checkpoints.lock().expect("checkpoint list lock").len(), 1);
}

#[tokio::test]
async fn degradation_returns_none_when_all_sections_already_stripped() {
    let mut session = Session::new("session-degrade-none", "test-model");
    session
        .messages
        .push(Message::system("Just base prompt".to_string()));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    // All sections already absent — should fall through to LLM summarization path
    // but with a small session it won't have enough messages, so it returns Ok(false).
    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-degrade-none",
        &[],
        &llm,
        None,
    )
    .await
    .expect("no degradation");
    assert!(!applied);
    assert_eq!(system_prompt(&session), "Just base prompt");
}

#[tokio::test]
async fn degradation_skips_missing_sections() {
    let mut session = Session::new("session-degrade-skip", "test-model");
    // Only env_context present — tool_guide, skill, external_memory, task_list are absent
    session.messages.push(Message::system(
        "Base prompt\n\
         <!-- BAMBOO_ENV_CONTEXT_START -->\nenv info\n<!-- BAMBOO_ENV_CONTEXT_END -->"
            .to_string(),
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let llm = noop_llm();

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-degrade-skip",
        &[],
        &llm,
        None,
    )
    .await
    .expect("skip absent sections");
    assert!(applied);
    let prompt = system_prompt(&session);
    assert!(!prompt.contains("BAMBOO_ENV_CONTEXT"));
    assert!(prompt.contains("Base prompt"));
}

#[tokio::test]
async fn pre_summarization_degradation_skips_llm_for_auto_triggered_compression() {
    // When auto-triggered (non-critical, non-manual), degradation should skip
    // the expensive LLM summarization if a section can be stripped.
    // Use a large budget so actual token counting stays well below 98% critical.
    let mut session = Session::new("session-presummarize-skip", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 100_000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system(
        "Base prompt\n\
         <!-- BAMBOO_TOOL_GUIDE_START -->\nguide details\n<!-- BAMBOO_TOOL_GUIDE_END -->"
            .to_string(),
    ));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    // 85% usage with a large budget — triggers auto (80%) but NOT critical (98%).
    // Real token count of 24 short messages is ~4-5K, well under 98K.
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 80_000,
        total_tokens: 85_000,
        max_context_tokens: 100_000,
        budget_limit: 100_000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };
    let (llm, models) = recording_llm();

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-presummarize-skip",
        &[],
        &llm,
        None,
        "pre-turn",
    )
    .await
    .expect("pre-summarization degradation");

    assert!(applied, "degradation should succeed");
    assert!(
        !system_prompt(&session).contains("BAMBOO_TOOL_GUIDE"),
        "tool guide should be stripped"
    );
    let recorded = models.lock().expect("models lock");
    assert!(
        recorded.is_empty(),
        "LLM should NOT be called when degradation handles it"
    );
}

#[tokio::test]
async fn tokens_saved_is_computed_from_compressed_messages() {
    let mut session = Session::new("session-tokens-saved", "test-model");
    session.token_budget = Some(TokenBudget {
        max_context_tokens: 100_000,
        max_output_tokens: 200,
        strategy: BudgetStrategy::Hybrid {
            window_size: 20,
            enable_summarization: true,
        },
        safety_margin: 0,
        compression_trigger_percent: 80,
        compression_target_percent: 50,
        working_reserve_tokens: 0,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    });
    session.messages.push(Message::system("System prompt"));
    for index in 0..12 {
        session.messages.push(Message::user(format!(
            "User message {} {}",
            index,
            "alpha beta gamma delta epsilon zeta ".repeat(8)
        )));
        session.messages.push(Message::assistant(
            format!(
                "Assistant response {} {}",
                index,
                "analysis plan files checks and next steps ".repeat(8)
            ),
            None,
        ));
    }
    session.token_usage = Some(bamboo_agent_core::TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 80_000,
        total_tokens: 85_000,
        max_context_tokens: 100_000,
        budget_limit: 100_000,
        truncation_occurred: true,
        segments_removed: 8,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        background_model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    let (llm, _models) = recording_llm();
    let (event_tx, mut event_rx) = mpsc::channel(64);

    let applied = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "session-tokens-saved",
        &[],
        &llm,
        Some(&event_tx),
        "pre-turn",
    )
    .await
    .expect("compression");

    assert!(applied, "compression should succeed");

    // Collect events and find ContextSummarized
    drop(event_tx);
    let events: Vec<AgentEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    let summarized = events.iter().find_map(|e| match e {
        AgentEvent::ContextSummarized { tokens_saved, .. } => Some(*tokens_saved),
        _ => None,
    });
    let tokens_saved = summarized.expect("should have ContextSummarized event");
    assert!(
        tokens_saved > 0,
        "tokens_saved should be > 0, got {tokens_saved}"
    );
}

/// Build a `TokenBudgetUsage` at `total_tokens`/`max_context_tokens`, so pressure
/// is `total / max * 100` percent. `max_context_tokens` doubles as `budget_limit`.
fn pressure_usage(total_tokens: u32, max_context_tokens: u32) -> TokenBudgetUsage {
    TokenBudgetUsage {
        system_tokens: 0,
        summary_tokens: 0,
        window_tokens: 0,
        total_tokens,
        max_context_tokens,
        budget_limit: max_context_tokens,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    }
}

/// Count emitted `ContextPressureNotification` events still buffered on a
/// channel. `emit_context_pressure_notification` uses `try_send`, so draining via
/// `try_recv` needs no async runtime.
fn drain_pressure_notifications(event_rx: &mut mpsc::Receiver<AgentEvent>) -> Vec<String> {
    std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AgentEvent::ContextPressureNotification { level, .. } => Some(level),
            _ => None,
        })
        .collect()
}

#[test]
fn context_pressure_notification_fires_at_most_once_per_level_across_rounds() {
    // Acceptance test for issue #36: with pressure held at a fixed level, the
    // notification must fire exactly once — not once per round. Dedup state now
    // persists in session.metadata instead of a per-round throwaway local.
    let mut session = Session::new("session-pressure-dedup", "test-model");
    // 80% usage -> "warning" level (>= 70%).
    session.token_usage = Some(pressure_usage(80_000, 100_000));

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(64);

    // Drive 10 rounds at the same pressure level.
    for _ in 0..10 {
        emit_context_pressure_notification(
            &mut session,
            Some(&event_tx),
            ContextManagementStrategy::Summary,
        );
    }
    drop(event_tx);

    let levels = drain_pressure_notifications(&mut event_rx);
    assert_eq!(
        levels,
        vec!["warning".to_string()],
        "expected exactly one notification across 10 rounds at the same level"
    );
    // Dedup state persists across rounds in metadata.
    assert_eq!(
        session.metadata.get(LAST_PRESSURE_LEVEL_KEY),
        Some(&"summary:warning".to_string())
    );
}

#[test]
fn context_pressure_notification_refires_only_on_level_transition() {
    // Per-level-transition semantics: a level re-fires only after pressure drops
    // below the threshold and comes back (reset), or escalates to a new level.
    let mut session = Session::new("session-pressure-transition", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(64);

    // Round 1: 80% warning -> emits.
    session.token_usage = Some(pressure_usage(80_000, 100_000));
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );

    // Round 2: still 80% warning -> deduped, no re-fire.
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );

    // Round 3: drops to 50% (below threshold) -> clears stored level, no fire.
    session.token_usage = Some(pressure_usage(50_000, 100_000));
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );
    assert!(
        session.metadata.get(LAST_PRESSURE_LEVEL_KEY).is_none(),
        "stored level should be cleared once pressure drops below threshold"
    );

    // Round 4: back to 80% warning -> re-fires (reset transition).
    session.token_usage = Some(pressure_usage(80_000, 100_000));
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );

    // Round 5: escalates to 95% critical -> level transition, fires again.
    session.token_usage = Some(pressure_usage(95_000, 100_000));
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );

    // Round 6: still 95% critical -> deduped, no re-fire.
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );
    drop(event_tx);

    let levels = drain_pressure_notifications(&mut event_rx);
    // warning (r1) + warning (r4, after reset) + critical (r5) == 3 fires.
    assert_eq!(
        levels,
        vec![
            "warning".to_string(),
            "warning".to_string(),
            "critical".to_string()
        ],
        "expected fires only on level transitions, not every round"
    );
    assert_eq!(
        session.metadata.get(LAST_PRESSURE_LEVEL_KEY),
        Some(&"summary:critical".to_string())
    );
}

#[test]
fn context_pressure_notification_refires_when_strategy_changes_at_same_level() {
    let mut session = Session::new("session-pressure-strategy-transition", "test-model");
    session.token_usage = Some(pressure_usage(80_000, 100_000));
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);

    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::RetrievalWindow,
    );
    emit_context_pressure_notification(
        &mut session,
        Some(&event_tx),
        ContextManagementStrategy::RetrievalWindow,
    );

    let events = std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AgentEvent::ContextPressureNotification { message, .. } => Some(message),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert!(events[0].contains("compact_context"));
    assert!(events[1].contains("session_history_current"));
    assert_eq!(
        session.metadata.get(LAST_PRESSURE_LEVEL_KEY),
        Some(&"retrieval_window:warning".to_string())
    );
}

#[test]
fn context_pressure_notification_copy_is_strategy_aware() {
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);

    let mut summary_session = Session::new("pressure-summary-copy", "test-model");
    summary_session.token_usage = Some(pressure_usage(80_000, 100_000));
    emit_context_pressure_notification(
        &mut summary_session,
        Some(&event_tx),
        ContextManagementStrategy::Summary,
    );
    let AgentEvent::ContextPressureNotification {
        message: summary_message,
        ..
    } = event_rx.try_recv().expect("summary pressure event")
    else {
        panic!("expected summary pressure event");
    };
    assert_eq!(
        summary_message,
        "Context window filling up (~80%). Consider using compact_context to compress older \
         conversation history before auto-compression triggers."
    );

    for (total_tokens, expected_level) in [(80_000, "warning"), (95_000, "critical")] {
        let mut retrieval_session =
            Session::new(format!("pressure-retrieval-{expected_level}"), "test-model");
        retrieval_session.token_usage = Some(pressure_usage(total_tokens, 100_000));
        emit_context_pressure_notification(
            &mut retrieval_session,
            Some(&event_tx),
            ContextManagementStrategy::RetrievalWindow,
        );
        let AgentEvent::ContextPressureNotification { level, message, .. } =
            event_rx.try_recv().expect("retrieval pressure event")
        else {
            panic!("expected retrieval pressure event");
        };
        assert_eq!(level, expected_level);
        assert!(message.contains("archiv"));
        assert!(message.contains("session_history_current"));
        assert!(message.contains("session_note"));
        assert!(message.contains("decisions, paths, progress, and blockers"));
        assert!(message.contains("Do not copy raw transcript"));
        assert!(!message.contains("compact_context"));
        assert!(!message.contains("Auto-compression"));
    }
}

#[test]
fn context_pressure_notification_reports_explicit_summary_fallback_for_summarized_session() {
    let mut session = Session::new("pressure-summary-fallback", "test-model");
    session.token_usage = Some(pressure_usage(80_000, 100_000));
    session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
        "Existing summary",
        8,
        2_000,
    ));
    let context_management = ContextManagementConfig {
        strategy: ContextManagementStrategy::RetrievalWindow,
        retrieval_window: RetrievalWindowContextConfig {
            fallback_strategy: ContextManagementFallbackStrategy::Summary,
            ..RetrievalWindowContextConfig::default()
        },
    };
    let effective_strategy = effective_context_pressure_strategy(&session, &context_management);
    assert_eq!(effective_strategy, ContextManagementStrategy::Summary);

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
    emit_context_pressure_notification(&mut session, Some(&event_tx), effective_strategy);
    let AgentEvent::ContextPressureNotification { message, .. } = event_rx
        .try_recv()
        .expect("summary fallback pressure event")
    else {
        panic!("expected summary fallback pressure event");
    };
    assert!(message.contains("auto-compression"));
    assert!(message.contains("compact_context"));
    assert!(!message.contains("Retrieval-window"));
    assert!(!message.contains("session_history_current"));
}
