use std::sync::{Arc, Mutex};

use crate::runtime::task_context::{TaskLoopContext, TaskLoopItem, ToolCallRecord};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentEvent, Message};
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream};
use chrono::Utc;
use futures::stream;
use tokio::sync::mpsc;

use super::message_builder::format_recent_tools;
use super::{build_task_evaluation_messages, evaluate_task_progress};

fn create_test_context() -> TaskLoopContext {
    let mut session = bamboo_agent_core::Session::new("test", "test-model");
    let task_list = TaskList {
        session_id: "test".to_string(),
        title: "Test Tasks".to_string(),
        items: vec![TaskItem {
            id: "1".to_string(),
            description: "Fix bug in authentication".to_string(),
            status: TaskItemStatus::InProgress,
            depends_on: Vec::new(),
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");
    context.items = vec![TaskLoopItem {
        id: "1".to_string(),
        description: "Fix bug in authentication".to_string(),
        status: TaskItemStatus::InProgress,
        depends_on: Vec::new(),
        notes: String::new(),
        active_form: None,
        parent_id: None,
        phase: bamboo_domain::task::TaskPhase::Execution,
        priority: bamboo_domain::task::TaskPriority::Medium,
        completion_criteria: Vec::new(),
        evidence: Vec::new(),
        blockers: Vec::new(),
        transitions: Vec::new(),
        tool_calls: vec![
            ToolCallRecord {
                round: 0,
                tool_name: "read_file".to_string(),
                success: true,
                timestamp: Utc::now(),
            },
            ToolCallRecord {
                round: 1,
                tool_name: "write_file".to_string(),
                success: true,
                timestamp: Utc::now(),
            },
        ],
        started_at_round: Some(0),
        completed_at_round: None,
    }];

    context
}

#[test]
fn build_evaluation_messages_contains_context_and_rules() {
    let context = create_test_context();
    let session = bamboo_agent_core::Session::new("test", "test-model");

    let messages = build_task_evaluation_messages(&context, &session);

    assert_eq!(messages.len(), 2);
    assert!(messages[0].content.contains("task progress evaluator"));
    assert!(messages[1].content.contains("Fix bug in authentication"));
}

#[test]
fn format_recent_tools_includes_symbols_and_tool_names() {
    let context = create_test_context();
    let output = format_recent_tools(&context, 5);

    assert!(output.contains("read_file"));
    assert!(output.contains("write_file"));
    assert!(output.contains("✓"));
}

#[test]
fn in_progress_items_require_evaluation() {
    let mut context = create_test_context();

    assert!(context
        .items
        .iter()
        .any(|item| matches!(item.status, TaskItemStatus::InProgress)));

    context.items[0].status = TaskItemStatus::Completed;

    assert!(!context
        .items
        .iter()
        .any(|item| matches!(item.status, TaskItemStatus::InProgress)));
}

#[derive(Clone, Default)]
struct RecordingFailingProvider {
    requested_models: Arc<Mutex<Vec<String>>>,
}

impl RecordingFailingProvider {
    fn last_requested_model(&self) -> Option<String> {
        self.requested_models
            .lock()
            .ok()
            .and_then(|models| models.last().cloned())
    }
}

#[async_trait]
impl LLMProvider for RecordingFailingProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        if let Ok(mut models) = self.requested_models.lock() {
            models.push(model.to_string());
        }

        Err(LLMError::Api("intentional provider failure".to_string()))
    }
}

#[tokio::test]
async fn task_evaluation_uses_explicit_model_parameter_for_provider_request() {
    let context = create_test_context();
    let session = bamboo_agent_core::Session::new("test-session", "session-model");
    let provider = Arc::new(RecordingFailingProvider::default());
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(4);

    let result = evaluate_task_progress(
        &context,
        &session,
        llm,
        &event_tx,
        "test-session",
        "evaluation-model",
        None,
    )
    .await
    .expect("evaluation should gracefully handle provider failure");

    assert_eq!(
        provider.last_requested_model().as_deref(),
        Some("evaluation-model")
    );
    assert!(!result.needs_evaluation);
    assert!(result.updates.is_empty());
    assert!(result.reasoning.contains("Evaluation failed:"));
    assert!(result.reasoning.contains("intentional provider failure"));
}

#[derive(Clone, Default)]
struct RecordingRequestOptionsProvider {
    requested_reasoning: Arc<Mutex<Vec<Option<ReasoningEffort>>>>,
    requested_max_tokens: Arc<Mutex<Vec<Option<u32>>>>,
}

impl RecordingRequestOptionsProvider {
    fn last_requested_reasoning(&self) -> Option<Option<ReasoningEffort>> {
        self.requested_reasoning
            .lock()
            .ok()
            .and_then(|values| values.last().copied())
    }
}

#[async_trait]
impl LLMProvider for RecordingRequestOptionsProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        Ok(Box::pin(stream::iter(vec![
            Ok::<LLMChunk, LLMError>(LLMChunk::Token(
                "{\"item_id\":\"1\",\"status\":\"completed\"}".to_string(),
            )),
            Ok::<LLMChunk, LLMError>(LLMChunk::Done),
        ])))
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        if let Ok(mut values) = self.requested_reasoning.lock() {
            values.push(options.and_then(|o| o.reasoning_effort));
        }
        if let Ok(mut values) = self.requested_max_tokens.lock() {
            values.push(max_output_tokens);
        }
        self.chat_stream(messages, tools, max_output_tokens, model)
            .await
    }
}

#[tokio::test]
async fn task_evaluation_caps_reasoning_effort_to_high_for_lightweight_request() {
    let context = create_test_context();
    let session = bamboo_agent_core::Session::new("test-session", "session-model");
    let provider = Arc::new(RecordingRequestOptionsProvider::default());
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(4);

    let _ = evaluate_task_progress(
        &context,
        &session,
        llm,
        &event_tx,
        "test-session",
        "gpt-5-mini",
        Some(ReasoningEffort::Xhigh),
    )
    .await
    .expect("evaluation request should succeed");

    assert_eq!(
        provider.last_requested_reasoning(),
        Some(Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn task_evaluation_sufficient_max_tokens_for_high_reasoning() {
    let context = create_test_context();
    let session = bamboo_agent_core::Session::new("test-session", "session-model");
    let provider = Arc::new(RecordingRequestOptionsProvider::default());
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(4);

    let _ = evaluate_task_progress(
        &context,
        &session,
        llm,
        &event_tx,
        "test-session",
        "gpt-5-mini",
        Some(ReasoningEffort::High),
    )
    .await
    .expect("evaluation request should succeed");

    let captured = provider
        .requested_max_tokens
        .lock()
        .expect("lock should not be poisoned");
    let max_tokens = captured[0].expect("max_output_tokens should be set");
    // ReasoningEffort::High targets 4096 thinking budget; max_tokens must leave room for output.
    assert!(
        max_tokens > 4096,
        "max_output_tokens ({}) must exceed thinking budget (4096) to avoid truncation",
        max_tokens
    );
}
