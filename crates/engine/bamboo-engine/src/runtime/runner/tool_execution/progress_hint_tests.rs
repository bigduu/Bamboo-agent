use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};

fn hint_count(messages: &[Message]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata["runtime_kind"] == "observation_progress_hint")
        })
        .count()
}

struct ObservationProvider {
    calls: AtomicUsize,
    rounds: usize,
    tool_name: &'static str,
    call_namespace: usize,
    hint_counts: Mutex<Vec<usize>>,
}

#[async_trait]
impl LLMProvider for ObservationProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        self.hint_counts.lock().unwrap().push(hint_count(messages));
        let chunks = if round < self.rounds {
            vec![
                Ok(LLMChunk::ToolCalls(
                    (0..2)
                        .map(|offset| ToolCall {
                            id: format!("observation-{}-{round}-{offset}", self.call_namespace),
                            tool_type: "function".into(),
                            function: FunctionCall {
                                name: self.tool_name.into(),
                                arguments: if round % 2 == 0 {
                                    r#"{"path":"file","offset":1}"#.into()
                                } else {
                                    r#"{"offset":1,"path":"file"}"#.into()
                                },
                            },
                        })
                        .collect(),
                )),
                Ok(LLMChunk::Done),
            ]
        } else {
            vec![
                Ok(LLMChunk::Token("Done using the collected evidence.".into())),
                Ok(LLMChunk::Done),
            ]
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

struct ObservationExecutor {
    calls: AtomicUsize,
    tool_name: &'static str,
    changed_round: Option<usize>,
}

#[async_trait]
impl ToolExecutor for ObservationExecutor {
    async fn execute(
        &self,
        _: &ToolCall,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let output = if self.changed_round == Some(call / 2) {
            "new evidence"
        } else {
            "same evidence"
        };
        Ok(ToolResult::text(true, output))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".into(),
            function: FunctionSchema {
                name: self.tool_name.into(),
                description: "Observation progress probe".into(),
                parameters: serde_json::json!({"type":"object","properties":{}}),
            },
        }]
    }
}

async fn run_observation_loop(
    session: &mut Session,
    tool_name: &'static str,
    changed_round: Option<usize>,
) -> Vec<usize> {
    let provider = Arc::new(ObservationProvider {
        calls: AtomicUsize::new(0),
        rounds: 6,
        tool_name,
        call_namespace: session.messages.len(),
        hint_counts: Mutex::new(Vec::new()),
    });
    let executor = Arc::new(ObservationExecutor {
        calls: AtomicUsize::new(0),
        tool_name,
        changed_round,
    });
    let config = AgentLoopConfig {
        system_prompt: Some("Complete the bounded task using collected evidence.".into()),
        model_name: Some("model".into()),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    crate::runtime::runner::run_agent_loop_with_config(
        session,
        "Inspect the file and finish the task.".into(),
        tx,
        provider.clone(),
        executor.clone(),
        CancellationToken::new(),
        config,
    )
    .await
    .unwrap();
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        12,
        "the hint never skips a real call"
    );
    let mut completes = 0;
    while let Ok(event) = rx.try_recv() {
        if matches!(event, bamboo_agent_core::AgentEvent::Complete { .. }) {
            completes += 1;
        }
    }
    assert_eq!(
        completes, 1,
        "the advisory leaves completion behavior unchanged"
    );
    let hints = provider.hint_counts.lock().unwrap().clone();
    hints
}

#[tokio::test]
async fn observation_progress_real_loop_hints_after_paired_results_and_resets_on_new_run() {
    let mut session = Session::new("observation-progress", "model");
    assert_eq!(
        run_observation_loop(&mut session, "Read", None).await,
        [0, 0, 0, 1, 1, 1, 1]
    );
    let hint_index = session
        .messages
        .iter()
        .position(|message| hint_count(std::slice::from_ref(message)) == 1)
        .unwrap();
    assert_eq!(session.messages[hint_index - 1].role, Role::Tool);
    assert_eq!(
        session.messages[hint_index - 1].tool_call_id.as_deref(),
        Some("observation-0-2-1")
    );
    assert_eq!(
        session.messages[hint_index].metadata.as_ref().unwrap()["hidden_from_ui"],
        true
    );
    assert_eq!(
        run_observation_loop(&mut session, "Read", None).await,
        [1, 1, 1, 2, 2, 2, 2]
    );
    assert_eq!(hint_count(&session.messages), 2);
}

#[tokio::test]
async fn observation_progress_real_loop_resets_when_output_changes_and_leaves_polling_alone() {
    let mut session = Session::new("observation-progress-change", "model");
    assert_eq!(
        run_observation_loop(&mut session, "Read", Some(2)).await,
        [0, 0, 0, 0, 0, 0, 1]
    );
    let mut polling = Session::new("observation-progress-polling", "model");
    assert_eq!(
        run_observation_loop(&mut polling, "BashOutput", None).await,
        [0; 7]
    );
    let mut mutation = Session::new("observation-progress-mutation", "model");
    assert_eq!(
        run_observation_loop(&mut mutation, "Write", None).await,
        [0; 7]
    );
}
