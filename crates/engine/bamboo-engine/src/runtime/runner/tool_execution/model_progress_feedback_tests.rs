//! Current run API contract: repeated observations produce bounded model
//! feedback, never a new Human question. All providers/executors are local.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema};
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::stream;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};

const FEEDBACK_PREFIX: &str = "[runtime_progress_feedback_v1]\n";

struct ModelProgressProvider {
    calls: AtomicUsize,
    feedback: Mutex<Vec<Value>>,
    stop_on_feedback: bool,
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
        assert!(round < 16, "the feedback recovery must have a finite bound");
        let packets = messages.iter().filter_map(|message| {
            let offset = message.content.find(FEEDBACK_PREFIX)? + FEEDBACK_PREFIX.len();
            serde_json::Deserializer::from_str(&message.content[offset..])
                .into_iter::<Value>()
                .next()
                .map(Result::unwrap)
        }).collect::<Vec<_>>();
        assert!(packets.len() <= 1, "a request contains one current feedback packet");
        if let Some(packet) = packets.first() {
            assert_eq!(packet["schema"], "runtime_progress_feedback_v1");
            assert_eq!(packet["source"], "runtime");
            assert_eq!(packet["reason"], "unchanged_file_observations");
            assert_eq!(packet["recovery"]["episode_limit"], 3);
            assert_eq!(packet["recovery"]["round_limit"], 4);
            assert!(messages.iter().any(|message| message.role == Role::Tool),
                "feedback follows real paired tool results");
            self.feedback.lock().unwrap().push(packet.clone());
        }
        let chunks = if self.stop_on_feedback && !packets.is_empty() {
            vec![Ok(LLMChunk::Token("I can use the observed evidence without another identical read.".into())), Ok(LLMChunk::Done)]
        } else {
            vec![Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: format!("model-progress-{round}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Read".into(),
                    arguments: json!({"path":"same-file"}).to_string(),
                },
            }])), Ok(LLMChunk::Done)]
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

struct UnchangedReadExecutor(AtomicUsize);

#[async_trait]
impl ToolExecutor for UnchangedReadExecutor {
    async fn execute(&self, _: &ToolCall) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(true, "unchanged raw evidence"))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".into(),
            function: FunctionSchema {
                name: "Read".into(),
                description: "Deterministic existing observation".into(),
                parameters: json!({"type":"object","properties":{"path":{"type":"string"}}}),
            },
        }]
    }
}

async fn run_model_feedback(stop_on_feedback: bool) -> (Session, Arc<ModelProgressProvider>, usize, Vec<AgentEvent>) {
    let mut session = Session::new("model-progress-contract", "model");
    let provider = Arc::new(ModelProgressProvider {
        calls: AtomicUsize::new(0), feedback: Mutex::new(Vec::new()), stop_on_feedback,
    });
    let executor = Arc::new(UnchangedReadExecutor(AtomicUsize::new(0)));
    let config = AgentLoopConfig {
        system_prompt: Some("Read only what is useful; change approach when evidence stops changing.".into()),
        model_name: Some("model".into()),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false, relevant_recall: false,
            relevant_recall_rerank: false, project_first_dream: false, ledger_agenda: false,
        },
        ..Default::default()
    };
    assert!(config.run_budget.max_rounds.is_none());
    let (tx, mut rx) = mpsc::channel(256);
    crate::runtime::runner::run_agent_loop_with_config(
        &mut session, "Inspect useful evidence and finish.".into(), tx,
        provider.clone(), executor.clone(), CancellationToken::new(), config,
    ).await.unwrap();
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() { events.push(event); }
    (session, provider, executor.0.load(Ordering::SeqCst), events)
}

fn assert_no_new_progress_question(session: &Session, events: &[AgentEvent]) {
    assert!(session.pending_question.is_none());
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    assert!(!events.iter().any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    for call in session.messages.iter().filter_map(|message| message.tool_calls.as_ref()).flatten() {
        let result = session.messages.iter().find(|message| message.tool_call_id.as_deref() == Some(call.id.as_str()))
            .expect("every original call has its real tool response");
        assert_eq!(result.content, "unchanged raw evidence");
        assert_eq!(result.tool_success, Some(true));
    }
}

#[tokio::test]
async fn model_feedback_lets_same_llm_use_evidence_without_a_human_popup() {
    let (session, provider, tools, events) = run_model_feedback(true).await;
    assert_no_new_progress_question(&session, &events);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert_eq!(tools, 3);
    assert_eq!(provider.feedback.lock().unwrap().len(), 1);
    assert!(session.messages.iter().all(|message| !message.content.contains(FEEDBACK_PREFIX)));
}

#[tokio::test]
async fn model_feedback_ignored_with_default_unlimited_rounds_stops_without_another_dispatch() {
    let (session, provider, tools, events) = run_model_feedback(false).await;
    assert_no_new_progress_question(&session, &events);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 7);
    assert_eq!(tools, 7);
    assert_eq!(provider.feedback.lock().unwrap().len(), 4);
    assert_eq!(session.metadata.get("runtime.completion_reason").map(String::as_str),
        Some("observation_progress_exhausted"));
    let summaries = session.messages.iter().filter(|message| message.metadata.as_ref()
        .is_some_and(|metadata| metadata["runtime_kind"] == "observation_progress_exhausted"))
        .collect::<Vec<_>>();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].role, Role::Assistant);
    assert!(summaries[0].content.contains("not confirmed complete"));
}
