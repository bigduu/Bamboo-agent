//! Exercise repeated tool advice through the runner and its next provider request.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::stream;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};

use super::policy::REPEATED_TOOL_ADVISORY_PREFIX;

const FINAL_ANSWER: &str = "I can use the evidence already collected.";

struct ModelProgressProvider {
    calls: AtomicUsize,
    advisory_rounds: Mutex<Vec<usize>>,
    tool_name: &'static str,
    finish_after: usize,
}

#[async_trait]
impl LLMProvider for ModelProgressProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        let advisory = messages
            .iter()
            .enumerate()
            .filter(|(_, message)| message.content.starts_with(REPEATED_TOOL_ADVISORY_PREFIX))
            .collect::<Vec<_>>();
        assert!(advisory.len() <= 1);
        if let Some(&(index, advice)) = advisory.first() {
            assert!(advice.content.contains("3 times in a row"));
            assert!(advice.content.contains("trying a different approach"));
            assert_eq!(messages[index - 1].role, Role::Tool);
            assert_eq!(
                messages[index - 1].tool_call_id.as_deref(),
                Some(format!("model-progress-{}", round - 1).as_str())
            );
            assert_eq!(messages[index - 1].content, "unchanged raw evidence");
            self.advisory_rounds.lock().unwrap().push(round);
        }
        let chunks = if round == self.finish_after {
            vec![Ok(LLMChunk::Token(FINAL_ANSWER.into())), Ok(LLMChunk::Done)]
        } else {
            vec![
                Ok(LLMChunk::ToolCalls(vec![ToolCall {
                    id: format!("model-progress-{round}"),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: self.tool_name.into(),
                        arguments: if self.tool_name == "browser_eval" {
                            json!({
                                "code":"document.title",
                                "expected_epoch":17,
                                "expected_url":"about:blank",
                            })
                            .to_string()
                        } else {
                            json!({"path":"same-file"}).to_string()
                        },
                    },
                }])),
                Ok(LLMChunk::Done),
            ]
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

struct UnchangedToolExecutor {
    name: &'static str,
    calls: AtomicUsize,
}

#[async_trait]
impl ToolExecutor for UnchangedToolExecutor {
    async fn execute(
        &self,
        _: &ToolCall,
    ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(true, "unchanged raw evidence"))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".into(),
            function: FunctionSchema {
                name: self.name.into(),
                description: "Return the same observation".into(),
                parameters: json!({"type":"object"}),
            },
        }]
    }
}

async fn run_model_feedback(
    tool_name: &'static str,
    finish_after: usize,
) -> (Session, Arc<ModelProgressProvider>, usize, Vec<AgentEvent>) {
    let mut session = Session::new("model-progress-contract", "model");
    let provider = Arc::new(ModelProgressProvider {
        calls: AtomicUsize::new(0),
        advisory_rounds: Mutex::new(Vec::new()),
        tool_name,
        finish_after,
    });
    let executor = Arc::new(UnchangedToolExecutor {
        name: tool_name,
        calls: AtomicUsize::new(0),
    });
    let config = AgentLoopConfig {
        system_prompt: Some(
            "Read only what is useful; change approach when evidence stops changing.".into(),
        ),
        model_name: Some("model".into()),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..Default::default()
    };
    assert!(config.run_budget.max_rounds.is_none());
    let (tx, mut rx) = mpsc::channel(256);
    crate::runtime::runner::run_agent_loop_with_config(
        &mut session,
        "Inspect useful evidence and finish.".into(),
        tx,
        provider.clone(),
        executor.clone(),
        CancellationToken::new(),
        config,
    )
    .await
    .unwrap();
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    (
        session,
        provider,
        executor.calls.load(Ordering::SeqCst),
        events,
    )
}

fn assert_no_new_progress_question(session: &Session, events: &[AgentEvent]) {
    assert!(session.pending_question.is_none());
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    for call in session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
    {
        let result = session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(call.id.as_str()))
            .expect("every original call has its real tool response");
        assert_eq!(result.content, "unchanged raw evidence");
        assert_eq!(result.tool_success, Some(true));
    }
}

#[tokio::test]
async fn browser_repetition_advice_reaches_the_next_request_with_the_real_result() {
    let (session, provider, tools, events) = run_model_feedback("browser_eval", 3).await;
    assert_no_new_progress_question(&session, &events);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert_eq!(tools, 3);
    assert_eq!(*provider.advisory_rounds.lock().unwrap(), [3]);
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains(REPEATED_TOOL_ADVISORY_PREFIX)));
}

#[tokio::test]
async fn repeated_reads_can_continue_past_the_old_recovery_cutoff() {
    let (session, provider, tools, events) = run_model_feedback("Read", 10).await;
    assert_no_new_progress_question(&session, &events);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 11);
    assert_eq!(tools, 10);
    assert_eq!(*provider.advisory_rounds.lock().unwrap(), [3]);
    assert!(!session.metadata.contains_key("runtime.completion_reason"));
    assert_eq!(session.messages.last().unwrap().content, FINAL_ANSWER);
    assert_eq!(
        session.model_context_state.as_ref().unwrap().prefix_epoch,
        0,
        "request-only advice must not reset the durable transcript epoch"
    );
}
