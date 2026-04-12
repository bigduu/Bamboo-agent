use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::prepare_round;
use crate::agent::core::{AgentError, Message, Role, Session};
use crate::agent::core::{TaskItem, TaskItemStatus, TaskList};
use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::tools::BuiltinToolExecutor;

#[derive(Clone)]
struct NoopProvider;

#[async_trait]
impl LLMProvider for NoopProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[crate::agent::core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        Ok(Box::pin(stream::empty()))
    }
}

fn sample_task_list(session_id: &str, status: TaskItemStatus) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Tasks".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Test item".to_string(),
            status,
            depends_on: Vec::new(),
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn prepare_round_updates_task_context_and_returns_round_id() {
    let mut session = Session::new("session-prelude", "test-model");
    session.set_task_list(sample_task_list("session-prelude", TaskItemStatus::Pending));
    let mut task_context = TaskLoopContext::from_session(&session);
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);

    let round_id = prepare_round(
        &mut session,
        &mut task_context,
        2,
        7,
        &CancellationToken::new(),
        None,
        "session-prelude",
        "test-model",
        false,
        &config,
        llm.clone(),
        &tools,
    )
    .await
    .expect("round should prepare");

    assert_eq!(round_id, "session-prelude-round-3");
    let ctx = task_context.expect("task context should exist");
    assert_eq!(ctx.current_round, 2);
    assert_eq!(ctx.max_rounds, 7);
    assert!(session
        .messages
        .iter()
        .any(|msg| matches!(msg.role, Role::System) && msg.content.contains("Current Task List")));
}

#[tokio::test]
async fn prepare_round_refreshes_prompt_metadata_for_round_sections() {
    let mut session = Session::new("session-round-metadata", "test-model");
    session.add_message(Message::system("Base prompt"));
    session.set_task_list(sample_task_list(
        "session-round-metadata",
        TaskItemStatus::InProgress,
    ));
    let mut task_context = TaskLoopContext::from_session(&session);
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);

    let _round_id = prepare_round(
        &mut session,
        &mut task_context,
        0,
        5,
        &CancellationToken::new(),
        None,
        "session-round-metadata",
        "test-model",
        false,
        &config,
        llm.clone(),
        &tools,
    )
    .await
    .expect("round should prepare");

    let flags = session
        .metadata
        .get("runtime_prompt_component_flags")
        .map(String::as_str)
        .unwrap_or_default();
    let lengths = session
        .metadata
        .get("runtime_prompt_component_lengths")
        .map(String::as_str)
        .unwrap_or_default();
    let layout = session
        .metadata
        .get("runtime_prompt_section_layout")
        .map(String::as_str)
        .unwrap_or_default();

    assert!(flags.contains("workspace=0"));
    assert!(flags.contains("external_memory="));
    assert!(flags.contains("task_list=1"));
    assert!(lengths.contains("external_memory="));
    assert!(lengths.contains("task_list="));
    assert!(lengths.contains("final="));
    assert!(layout.contains("round_base_prompt:core_static:static:1:"));
    assert!(layout.contains("task_list:environment_workspace:dynamic:1:"));

    let observability = session
        .metadata
        .get("runtime_prompt_memory_observability")
        .and_then(|raw| {
            serde_json::from_str::<crate::agent::core::PromptMemoryObservability>(raw).ok()
        })
        .expect("observability should be recorded");
    assert_eq!(observability.session_notes_status, "empty");
    assert!(!observability.relevant_recall_rerank_enabled);
    assert!(observability.external_memory_section_chars > 0);
}

#[tokio::test]
async fn prepare_round_preserves_shared_prompt_snapshot_static_fields() {
    let mut session = Session::new("session-round-snapshot", "test-model");
    session.add_message(Message::system("Base prompt"));
    session
        .metadata
        .insert("base_system_prompt".to_string(), "Base prompt".to_string());
    session
        .metadata
        .insert("enhance_prompt".to_string(), "Extra guidance".to_string());
    session.metadata.insert(
        "workspace_path".to_string(),
        "/tmp/session-round-snapshot".to_string(),
    );
    super::super::session_setup::prompt_setup::persist_prompt_snapshot_metadata(
        &mut session,
        crate::agent::core::PromptSnapshot {
            base_system_prompt: "Base prompt".to_string(),
            enhancement_prompt: Some("Extra guidance".to_string()),
            workspace_context: Some("Workspace path: /tmp/session-round-snapshot".to_string()),
            instruction_context: Some("Instruction block".to_string()),
            env_context: Some("Env block".to_string()),
            skill_context: Some("Skill block".to_string()),
            tool_guide_context: Some("Tool guide block".to_string()),
            dream_notebook: Some("Dream block".to_string()),
            session_memory_note: Some("Session note block".to_string()),
            project_memory_index: None,
            relevant_durable_memories: None,
            project_dream: None,
            global_dream_fallback: None,
            prompt_memory_observability: None,
            external_memory: None,
            task_list: None,
            effective_system_prompt: "Base prompt".to_string(),
        },
    );
    session.set_task_list(sample_task_list(
        "session-round-snapshot",
        TaskItemStatus::InProgress,
    ));
    let mut task_context = TaskLoopContext::from_session(&session);
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);

    let _round_id = prepare_round(
        &mut session,
        &mut task_context,
        0,
        5,
        &CancellationToken::new(),
        None,
        "session-round-snapshot",
        "test-model",
        false,
        &config,
        llm.clone(),
        &tools,
    )
    .await
    .expect("round should prepare");

    let snapshot =
        super::super::session_setup::prompt_setup::read_prompt_snapshot_metadata(&session)
            .expect("prompt snapshot should exist after round refresh");
    assert_eq!(snapshot.base_system_prompt, "Base prompt");
    assert_eq!(
        snapshot.enhancement_prompt.as_deref(),
        Some("Extra guidance")
    );
    assert_eq!(snapshot.skill_context.as_deref(), Some("Skill block"));
    assert!(snapshot
        .task_list
        .as_deref()
        .unwrap_or_default()
        .contains("Current Task List"));
    assert!(snapshot
        .effective_system_prompt
        .contains("Current Task List"));
    assert!(snapshot.prompt_memory_observability.is_some());
}

#[tokio::test]
async fn prepare_round_returns_cancelled_error_when_token_cancelled() {
    let mut session = Session::new("session-cancelled", "test-model");
    let mut task_context = None;
    let cancel_token = CancellationToken::new();
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();
    let llm: Arc<dyn LLMProvider> = Arc::new(NoopProvider);
    cancel_token.cancel();

    let result = prepare_round(
        &mut session,
        &mut task_context,
        0,
        5,
        &cancel_token,
        None,
        "session-cancelled",
        "test-model",
        false,
        &config,
        llm.clone(),
        &tools,
    )
    .await;

    assert!(matches!(result, Err(AgentError::Cancelled)));
}
