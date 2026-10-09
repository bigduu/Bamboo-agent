use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, PromptIR};
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};
use crate::session_app::child_session::{
    apply_child_session_update, run_child_action, update_child_action, ChildRunnerInfo,
    ChildSessionEntry, ChildSessionError, ChildSessionPort, ChildSessionUpdate, DeleteChildResult,
};

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

fn provider_hint_count(messages: &[Message]) -> usize {
    // Advisory User context is model-only. Check actual guidance text.
    messages
        .iter()
        .filter(|message| message.role == Role::User)
        .map(|message| message.content.matches(HINT_TEXT).count())
        .sum()
}

const HINT_TEXT: &str = "The same successful file observations returned unchanged information for three consecutive rounds.";

struct ObservationProvider {
    calls: AtomicUsize,
    rounds: usize,
    tool_name: &'static str,
    call_namespace: usize,
    hint_counts: Mutex<Vec<usize>>,
    fail_on_hint_once: bool,
    hint_retry_failed: AtomicBool,
    steering: Option<(Arc<dyn bamboo_domain::SessionInboxPort>, String, bool)>,
}

#[async_trait]
impl LLMProvider for ObservationProvider {
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        _: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        // Exercise the real Anthropic structured-system lowering, which ignores
        // ordinary System body messages when these system blocks are present.
        assert!(!ir.system_blocks.is_empty());
        let body = bamboo_llm::providers::anthropic::build_anthropic_request_with_cache_blocks(
            &ir.body_chat(),
            &ir.system_blocks,
            tools,
            model,
            max_output_tokens.unwrap_or(1024),
            true,
            None,
            None,
            Some(&ir.cache),
            false,
        );
        let wire_blocks = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["content"].as_array().unwrap())
            .collect::<Vec<_>>();
        let wire_count = wire_blocks
            .iter()
            .filter_map(|block| block["text"].as_str())
            .map(|text| text.matches(HINT_TEXT).count())
            .sum::<usize>();
        for (index, block) in wire_blocks.iter().enumerate() {
            if block["text"]
                .as_str()
                .is_some_and(|text| text.contains(HINT_TEXT))
            {
                assert!(index >= 2);
                for offset in [2, 1] {
                    assert_eq!(wire_blocks[index - offset]["type"], "tool_result");
                }
                let second_result_id = wire_blocks[index - 1]["tool_use_id"].as_str().unwrap();
                let pair_prefix = second_result_id.strip_suffix("-1").unwrap();
                assert_eq!(
                    wire_blocks[index - 2]["tool_use_id"],
                    format!("{pair_prefix}-0")
                );
            }
        }
        let flat = ir.flatten();
        assert_eq!(wire_count, provider_hint_count(&flat));
        self.chat_stream(&flat, tools, max_output_tokens, model)
            .await
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let hints = provider_hint_count(messages);
        self.hint_counts.lock().unwrap().push(hints);
        if self.fail_on_hint_once
            && hints == 1
            && !self.hint_retry_failed.swap(true, Ordering::SeqCst)
        {
            return Err(LLMError::Api(
                "temporary provider failure before output".into(),
            ));
        }
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        if round == 4 {
            if let Some((inbox, session_id, wrapped)) = self.steering.as_ref() {
                let mut envelope = bamboo_domain::SessionMessageEnvelope::user_input(
                    session_id,
                    "Use a different approach, then finish",
                );
                if *wrapped {
                    envelope = envelope
                        .with_root_chat_prompt("Task instructions".into())
                        .unwrap();
                }
                let receipt = inbox.deliver(&envelope).await.unwrap();
                inbox
                    .mark_activation_eligible(
                        session_id,
                        receipt.generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                    .unwrap();
            }
        }
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
    run_observation_loop_with_retry(session, tool_name, changed_round, false).await
}

async fn run_observation_loop_with_retry(
    session: &mut Session,
    tool_name: &'static str,
    changed_round: Option<usize>,
    fail_on_hint_once: bool,
) -> Vec<usize> {
    let provider = Arc::new(ObservationProvider {
        calls: AtomicUsize::new(0),
        rounds: 6,
        tool_name,
        call_namespace: session.messages.len(),
        hint_counts: Mutex::new(Vec::new()),
        fail_on_hint_once,
        hint_retry_failed: AtomicBool::new(false),
        steering: None,
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
    let mut questions = 0;
    while let Ok(event) = rx.try_recv() {
        if matches!(event, bamboo_agent_core::AgentEvent::Complete { .. }) {
            completes += 1;
        }
        if matches!(
            event,
            bamboo_agent_core::AgentEvent::NeedClarification { .. }
        ) {
            questions += 1;
        }
    }
    assert_eq!(
        completes, 1,
        "the existing terminal event is sent for completed and suspended runs"
    );
    let should_pause = session.kind == bamboo_domain::SessionKind::Root
        && tool_name == "Read"
        && changed_round.is_none();
    assert_eq!(questions, usize::from(should_pause));
    if should_pause {
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            6,
            "no seventh provider request after pause"
        );
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Suspended
        );
        let pending = session
            .pending_question
            .as_ref()
            .expect("runtime progress question");
        assert!(crate::session_app::no_progress::is_no_progress_question(
            session, pending
        ));
        assert!(pending.allow_custom);
        assert_eq!(pending.options, ["Continue", "Stop"]);
        // A pause must neither replace real observations nor manufacture tool
        // execution. Every original call keeps its own successful result.
        for call in session
            .messages
            .iter()
            .filter_map(|message| message.tool_calls.as_ref())
            .flatten()
        {
            assert_eq!(call.function.name, "Read");
            let result = session
                .messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some(call.id.as_str()))
                .unwrap();
            assert_eq!(result.content, "same evidence");
            assert_eq!(result.tool_success, Some(true));
        }
    }
    let hints = provider.hint_counts.lock().unwrap().clone();
    hints
}

#[tokio::test]
async fn observation_progress_request_retry_keeps_the_same_one_shot_hint() {
    let mut session = Session::new("observation-progress-retry", "model");
    assert_eq!(
        run_observation_loop_with_retry(&mut session, "Read", None, true).await,
        [0, 0, 0, 1, 1, 0, 0]
    );
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains(HINT_TEXT)));
    assert_eq!(hint_count(&session.messages), 0);
}

#[tokio::test]
async fn observation_progress_real_loop_hints_then_pauses_after_paired_results_and_resets_on_user_message(
) {
    let mut session = Session::new("observation-progress", "model");
    assert_eq!(
        run_observation_loop(&mut session, "Read", None).await,
        [0, 0, 0, 1, 0, 0]
    );
    assert_eq!(hint_count(&session.messages), 0);
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains(HINT_TEXT)));
    assert_eq!(
        run_observation_loop(&mut session, "Read", None).await,
        [0, 0, 0, 1, 0, 0]
    );
    assert_eq!(hint_count(&session.messages), 0);
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains(HINT_TEXT)));
}

/// Storage seam only; retry/update logic uses the production child actions.
struct ObservationChildPort(Mutex<Session>);

#[async_trait]
impl ChildSessionPort for ObservationChildPort {
    async fn load_root_session(&self, _: &str) -> Result<Session, ChildSessionError> {
        unreachable!("retry/update receives its verified parent")
    }

    async fn load_child_for_parent(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<Session, ChildSessionError> {
        let child = self.0.lock().unwrap();
        assert_eq!(child.parent_session_id.as_deref(), Some(parent_id));
        assert_eq!(child.id, child_id);
        Ok(child.clone())
    }

    async fn save_child_session(&self, child: &mut Session) -> Result<(), ChildSessionError> {
        *self.0.lock().unwrap() = child.clone();
        Ok(())
    }

    async fn update_child_session(
        &self,
        parent_id: &str,
        child_id: &str,
        update: ChildSessionUpdate,
    ) -> Result<(Session, usize), ChildSessionError> {
        let mut child = self.0.lock().unwrap();
        assert_eq!(child.parent_session_id.as_deref(), Some(parent_id));
        assert_eq!(child.id, child_id);
        let removed = apply_child_session_update(&mut child, update)?;
        Ok((child.clone(), removed))
    }

    async fn save_child_session_authoritative_flags(
        &self,
        _: &mut Session,
    ) -> Result<(), ChildSessionError> {
        unreachable!("no resident reuse")
    }

    async fn is_child_running(&self, _: &str) -> bool {
        false
    }

    async fn list_children(&self, _: &str) -> Vec<ChildSessionEntry> {
        unreachable!("no listing")
    }

    async fn enqueue_child_run(&self, _: &Session, _: &Session) -> Result<(), ChildSessionError> {
        unreachable!("retry only prepares the next run")
    }

    async fn cancel_child_run_and_wait(&self, _: &str) -> Result<(), ChildSessionError> {
        unreachable!("no cancellation")
    }

    async fn delete_child_session(
        &self,
        _: &str,
        _: &str,
    ) -> Result<DeleteChildResult, ChildSessionError> {
        unreachable!("no deletion")
    }

    async fn get_child_runner_info(&self, _: &str) -> Option<ChildRunnerInfo> {
        unreachable!("no inspection")
    }

    async fn register_parent_wait_for_child(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<(), ChildSessionError> {
        unreachable!("no parent wait")
    }

    async fn register_parent_wait_for_children(
        &self,
        _: &str,
        _: &[String],
        _: bamboo_domain::session::runtime_state::ChildWaitPolicy,
    ) -> Result<usize, ChildSessionError> {
        unreachable!("no parent wait")
    }

    async fn active_child_ids(&self, _: &str) -> Vec<String> {
        unreachable!("no child selection")
    }

    async fn find_resident_child(&self, _: &str, _: &str) -> Option<String> {
        unreachable!("no resident reuse")
    }

    async fn ensure_child_indexed(&self, _: &str) {
        unreachable!("no child creation")
    }
}

#[tokio::test]
async fn observation_progress_child_retry_and_update_keep_the_assignment_anchor() {
    let parent = Session::new("observation-parent", "model");
    let mut child = Session::new_child("observation-child", &parent.id, "model", "Inspect file");
    child
        .metadata
        .insert("responsibility".into(), "Inspect a bounded file".into());
    child
        .metadata
        .insert("subagent_type".into(), "worker".into());
    assert_eq!(
        run_observation_loop(&mut child, "Read", None).await,
        [0, 0, 0, 1, 0, 0, 0]
    );
    assert_eq!(hint_count(&child.messages), 0);
    assert!(child
        .messages
        .iter()
        .all(|message| !message.content.contains(HINT_TEXT)));
    let assignment_index = child
        .messages
        .iter()
        .rposition(|message| message.role == Role::User)
        .unwrap();
    let assignment = child.messages[assignment_index].clone();
    assert_eq!(assignment.content, "Inspect the file and finish the task.");
    child
        .metadata
        .insert("assignment_prompt".into(), assignment.content.clone());
    let tail_len = child.messages.len() - assignment_index - 1;
    assert!(
        tail_len > 1,
        "the real loop emitted tool history while giving the model a hint"
    );

    let retry_port = ObservationChildPort(Mutex::new(child.clone()));
    // SubAgent.run defaults to resetting after the latest user assignment.
    let result = run_child_action(&retry_port, &parent, child.id.clone(), None)
        .await
        .unwrap();
    assert_eq!(result["messages_removed"], tail_len);
    let retried = retry_port.0.lock().unwrap().clone();
    assert_eq!(retried.messages.len(), assignment_index + 1);
    assert_eq!(retried.messages.last().unwrap().id, assignment.id);
    assert_eq!(retried.messages.last().unwrap().content, assignment.content);
    assert_eq!(hint_count(&retried.messages), 0);

    let update_port = ObservationChildPort(Mutex::new(child.clone()));
    let result = update_child_action(
        &update_port,
        &parent.id,
        child.id.clone(),
        None,
        None,
        Some("Inspect the other file; preserve the bounded scope.".into()),
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(result["messages_removed"], tail_len);
    let updated = update_port.0.lock().unwrap();
    assert_eq!(updated.messages.len(), assignment_index + 1);
    let updated_assignment = updated.messages.last().unwrap();
    assert_eq!(updated_assignment.id, assignment.id);
    assert_eq!(updated_assignment.role, Role::User);
    assert!(updated_assignment
        .content
        .contains("Inspect the other file; preserve the bounded scope."));
    assert_eq!(hint_count(&updated.messages), 0);
    assert!(updated
        .messages
        .iter()
        .all(|message| message.role != Role::Tool));
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

#[tokio::test]
async fn observation_progress_new_human_input_resets_streak_before_pause() {
    for wrapped in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().into())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn bamboo_agent_core::storage::Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn bamboo_domain::SessionInboxPort> =
            Arc::new(bamboo_storage::FileSessionInbox::new(
                store,
                bamboo_domain::SessionInboxLimits::default(),
            ));
        let mut session = Session::new("progress-fresh-input", "model");
        storage.save_session(&session).await.unwrap();
        let provider = Arc::new(ObservationProvider {
            calls: AtomicUsize::new(0),
            rounds: 6,
            tool_name: "Read",
            call_namespace: 0,
            hint_counts: Mutex::new(Vec::new()),
            fail_on_hint_once: false,
            hint_retry_failed: AtomicBool::new(false),
            steering: Some((inbox.clone(), session.id.clone(), wrapped)),
        });
        let executor = Arc::new(ObservationExecutor {
            calls: AtomicUsize::new(0),
            tool_name: "Read",
            changed_round: None,
        });
        let config = AgentLoopConfig {
            model_name: Some("model".into()),
            system_prompt: Some("Complete bounded task".into()),
            storage: Some(storage),
            persistence: Some(persistence),
            session_inbox: Some(inbox),
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::channel(256);
        crate::runtime::runner::run_agent_loop_with_config(
            &mut session,
            "Inspect file".into(),
            tx,
            provider.clone(),
            executor,
            CancellationToken::new(),
            config,
        )
        .await
        .unwrap();
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            7,
            "fresh Human direction breaks the old streak before round six"
        );
        assert!(session.pending_question.is_none());
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Completed
        );
        assert!(session
            .messages
            .iter()
            .any(|message| message.content == "Use a different approach, then finish"));
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(
                event,
                bamboo_agent_core::AgentEvent::NeedClarification { .. }
            ));
        }
    }
}

#[tokio::test]
async fn observation_progress_hidden_resume_keeps_question_with_zero_model_dispatch() {
    let mut session = Session::new("progress-hidden-resume", "model");
    run_observation_loop(&mut session, "Read", None).await;
    let question_id = session
        .pending_question
        .as_ref()
        .unwrap()
        .tool_call_id
        .clone();
    let evidence = serde_json::to_value(&session.messages).unwrap();
    let mut notification = Message::user("A background task completed");
    notification.metadata = Some(
        serde_json::json!({"hidden_from_ui": true, "runtime_kind": "child_completion_resume"}),
    );
    session.add_message(notification);
    let provider = Arc::new(ObservationProvider {
        calls: AtomicUsize::new(0),
        rounds: 0,
        tool_name: "Read",
        call_namespace: 0,
        hint_counts: Mutex::new(Vec::new()),
        fail_on_hint_once: false,
        hint_retry_failed: AtomicBool::new(false),
        steering: None,
    });
    let executor = Arc::new(ObservationExecutor {
        calls: AtomicUsize::new(0),
        tool_name: "Read",
        changed_round: None,
    });
    let (tx, _rx) = mpsc::channel(256);
    crate::runtime::runner::run_agent_loop_with_config(
        &mut session,
        "A background task completed".into(),
        tx,
        provider.clone(),
        executor.clone(),
        CancellationToken::new(),
        AgentLoopConfig {
            skip_initial_user_message: true,
            model_name: Some("model".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert!(provider.hint_counts.lock().unwrap().is_empty());
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        session.pending_question.as_ref().unwrap().tool_call_id,
        question_id
    );
    assert_eq!(
        session.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Suspended
    );
    let original_count = evidence.as_array().unwrap().len();
    // System prompt setup may refresh its own body; the original real tool
    // pairs and runtime question remain byte-for-byte identical.
    for original in evidence
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] != "system")
    {
        let id = original["id"].as_str().unwrap();
        let actual = session
            .messages
            .iter()
            .find(|message| message.id == id)
            .unwrap();
        assert_eq!(serde_json::to_value(actual).unwrap(), *original);
    }
    assert_eq!(session.messages.len(), original_count + 1);
}
