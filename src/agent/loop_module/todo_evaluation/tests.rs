use std::sync::{Arc, Mutex};

use crate::agent::core::todo::{TodoItem, TodoList};
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentEvent, Message, TodoItemStatus};
use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
use crate::agent::loop_module::todo_context::{TodoLoopContext, TodoLoopItem, ToolCallRecord};
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;

use super::message_builder::format_recent_tools;
use super::{build_todo_evaluation_messages, evaluate_todo_progress};

fn create_test_context() -> TodoLoopContext {
    let mut session = crate::agent::core::Session::new("test", "test-model");
    let todo_list = TodoList {
        session_id: "test".to_string(),
        title: "Test Tasks".to_string(),
        items: vec![TodoItem {
            id: "1".to_string(),
            description: "Fix bug in authentication".to_string(),
            status: TodoItemStatus::InProgress,
            depends_on: Vec::new(),
            notes: String::new(),
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_todo_list(todo_list);

    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");
    context.items = vec![TodoLoopItem {
        id: "1".to_string(),
        description: "Fix bug in authentication".to_string(),
        status: TodoItemStatus::InProgress,
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
    let session = crate::agent::core::Session::new("test", "test-model");

    let messages = build_todo_evaluation_messages(&context, &session);

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
        .any(|item| matches!(item.status, TodoItemStatus::InProgress)));

    context.items[0].status = TodoItemStatus::Completed;

    assert!(!context
        .items
        .iter()
        .any(|item| matches!(item.status, TodoItemStatus::InProgress)));
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
    ) -> crate::agent::llm::provider::Result<LLMStream> {
        if let Ok(mut models) = self.requested_models.lock() {
            models.push(model.to_string());
        }

        Err(LLMError::Api("intentional provider failure".to_string()))
    }
}

#[tokio::test]
async fn todo_evaluation_uses_explicit_model_parameter_for_provider_request() {
    let context = create_test_context();
    let session = crate::agent::core::Session::new("test-session", "session-model");
    let provider = Arc::new(RecordingFailingProvider::default());
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(4);

    let result = evaluate_todo_progress(
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
