use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::{
    build_compression_context_blocks, emit_context_pressure_notification,
    enforce_model_context_ledger_retention, maybe_apply_host_context_compression,
    prepare_round_context, LAST_PRESSURE_LEVEL_KEY,
};
use crate::runtime::config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::{
    AgentEvent, AgentHook, CompressionTriggerType, Message, Role, Session, TokenBudgetUsage,
};
use bamboo_compression::{BudgetStrategy, TiktokenTokenCounter, TokenBudget, TokenCounter};
use bamboo_domain::{
    AgentHookPoint, ContextBlockType, HookPayload, HookResult, ModelContextEvent,
    ModelContextEventKind, ModelContextResetReason, ModelContextState, TaskItem, TaskItemStatus,
    TaskList,
};
use bamboo_llm::models::{ContentPart, ImageUrl};
use bamboo_llm::provider::{LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo};
use bamboo_llm::{LLMChunk, LLMError};
use futures::stream;
use tokio::sync::mpsc;

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

#[tokio::test]
async fn projected_relocation_does_not_double_reserve_large_system_env_context() {
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
    );

    assert!(projected.input_tokens <= request_input_limit);
    assert!(
        session.model_context_state.is_none(),
        "projection must stay pure"
    );
}

#[tokio::test]
async fn projected_compression_reseed_does_not_double_reserve_large_summary() {
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
    );

    assert!(projected.input_tokens <= request_input_limit);
    assert_eq!(
        session.model_context_state, pending_reset,
        "shadow projection must not commit the compression reseed"
    );
}

#[tokio::test]
async fn projected_refit_handles_over_limit_vision_transform_exactly_once() {
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
    let naive_projection = super::super::stream_execution::project_request_usage(
        &session,
        &naive,
        &config,
        &[],
        "test-model",
    );
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
    );
    assert!(
        counter.count_messages(&prepared.prepared_context.messages) <= request_input_limit,
        "refit must bring the transformed message vector itself back under the limit"
    );
    assert!(projected.input_tokens <= prepared.budget.max_request_input_tokens());
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
        max_context_tokens: 1200,
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
        window_tokens: 1078,
        total_tokens: 1178,
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

    let workflow_blocks = build_compression_context_blocks(&session, None)
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
    for index in 0..4 {
        session
            .messages
            .push(Message::user(format!("User message {} short text", index)));
        session.messages.push(Message::assistant(
            format!("Assistant response {} short text", index),
            None,
        ));
    }
    // context_window = 1200, usage = 62.5%; history content is also intentionally
    // kept short so estimated usage stays below trigger (80%).
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 100,
        summary_tokens: 0,
        window_tokens: 650,
        total_tokens: 750,
        max_context_tokens: 1200,
        budget_limit: 1200,
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
        max_context_tokens: 1200,
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
        max_context_tokens: 1200,
        budget_limit: 1200,
        truncation_occurred: false,
        segments_removed: 0,
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

    let applied = super::force_overflow_context_recovery(
        &mut session,
        &config,
        "test-model",
        "session-cp-overflow-force",
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
        emit_context_pressure_notification(&mut session, Some(&event_tx));
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
        Some(&"warning".to_string())
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
    emit_context_pressure_notification(&mut session, Some(&event_tx));

    // Round 2: still 80% warning -> deduped, no re-fire.
    emit_context_pressure_notification(&mut session, Some(&event_tx));

    // Round 3: drops to 50% (below threshold) -> clears stored level, no fire.
    session.token_usage = Some(pressure_usage(50_000, 100_000));
    emit_context_pressure_notification(&mut session, Some(&event_tx));
    assert!(
        session.metadata.get(LAST_PRESSURE_LEVEL_KEY).is_none(),
        "stored level should be cleared once pressure drops below threshold"
    );

    // Round 4: back to 80% warning -> re-fires (reset transition).
    session.token_usage = Some(pressure_usage(80_000, 100_000));
    emit_context_pressure_notification(&mut session, Some(&event_tx));

    // Round 5: escalates to 95% critical -> level transition, fires again.
    session.token_usage = Some(pressure_usage(95_000, 100_000));
    emit_context_pressure_notification(&mut session, Some(&event_tx));

    // Round 6: still 95% critical -> deduped, no re-fire.
    emit_context_pressure_notification(&mut session, Some(&event_tx));
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
        Some(&"critical".to_string())
    );
}
