use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

use bamboo_agent_core::{
    BashCompletionInfo, BashCompletionSink, FunctionCall, ToolCall, ToolCtx, ToolOutcome,
    ToolSchema,
};
use bamboo_llm::{LLMChunk, LLMError, LLMStream};
use futures::stream;
use serde_json::{json, Value};

// Release the command even when an assertion unwinds before the explicit poll.
struct ReleaseGate(PathBuf);

struct SideProcessCleanup(String);
impl Drop for SideProcessCleanup {
    fn drop(&mut self) {
        if let Some(shell) = bamboo_tools::tools::bash_runtime::get_shell(&self.0) {
            let _ = futures::FutureExt::now_or_never(shell.kill());
        }
    }
}

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"release");
    }
}

fn gated_command(gate: &Path) -> String {
    let gate = gate.to_string_lossy().replace('\'', "'\\''");
    format!(
        "printf 'exec-started\\n'; while [ ! -f '{gate}' ]; do sleep 0.01; done; \
         printf 'exec-finished\\n'; exit 7"
    )
}

struct IgnoredQuietReminder {
    command: String,
    calls: AtomicUsize,
    handle: Mutex<Option<String>>,
    side_handle: String,
    release_on_feedback: Option<PathBuf>,
}

#[async_trait::async_trait]
impl LLMProvider for IgnoredQuietReminder {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let calls = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![tool_call(
                "quiet-start",
                "exec_command",
                json!({"cmd":self.command,"yield_time_ms":250}),
            )],
            1 => {
                let started = result_for(messages, "quiet-start");
                assert_eq!(started["status"], "running");
                let handle = started["session_id"].as_str().unwrap().to_owned();
                *self.handle.lock().unwrap() = Some(handle.clone());
                // Virtual time advances only the real capture/poll clocks, not
                // the gated OS child or its actual wait/exit status.
                tokio::time::advance(Duration::from_secs(90)).await;
                let mut polls: Vec<_> = (0..8)
                    .map(|index| {
                        tool_call(
                            &format!("ignored-poll-{index}"),
                            "write_stdin",
                            json!({"session_id":handle,"chars":"","yield_time_ms":250+index,
                        "max_output_tokens":100+index}),
                        )
                    })
                    .collect();
                polls.push(tool_call("other-input", "write_stdin", json!({"session_id":self.side_handle,"chars":"side-input","eof":true,"yield_time_ms":250})));
                polls.push(tool_call(
                    "other-operation",
                    "exec_command",
                    json!({"cmd":"printf independent","yield_time_ms":250}),
                ));
                polls
            }
            round => {
                let handle = self.handle.lock().unwrap().clone().unwrap();
                if self.release_on_feedback.is_some() && round == 3 {
                    let result = result_for(messages, "feedback-final");
                    assert_eq!(result["status"], "completed");
                    assert_eq!(result["exit_code"], 7);
                    assert_eq!(result["output"], "quiet-finished");
                    return Ok(Box::pin(stream::iter([
                        Ok(LLMChunk::Token("Observed command exit 7 after runtime feedback.".into())),
                        Ok(LLMChunk::Done),
                    ])));
                }
                assert!(messages.iter().any(|message| message.content.contains("[runtime_progress_feedback_v1]")),
                    "the same model receives structured facts instead of a Human question");
                if let Some(gate) = self.release_on_feedback.as_ref() {
                    assert_eq!(round, 2);
                    tokio::time::resume();
                    std::fs::write(gate, b"release").unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while bamboo_tools::tools::bash_runtime::get_shell(&handle).unwrap().status() == "running" {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }).await.unwrap();
                    vec![tool_call("feedback-final", "write_stdin",
                        json!({"session_id":handle,"yield_time_ms":30_000}))]
                } else {
                    assert!(round < 6, "finite recovery ends before a seventh provider request");
                    vec![tool_call(&format!("feedback-poll-{round}"), "write_stdin",
                        json!({"session_id":handle,"yield_time_ms":250}))]
                }
            },
        };
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::ToolCalls(calls)),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct ReadQuietCompletion {
    handle: String,
    calls: AtomicUsize,
    gate: PathBuf,
    wait_before_release: bool,
}

#[async_trait::async_trait]
impl LLMProvider for ReadQuietCompletion {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.wait_before_release && round == 0 {
            return Ok(Box::pin(stream::iter([
                Ok(LLMChunk::ToolCalls(
                    (0..3)
                        .map(|index| {
                            tool_call(
                                &format!("continued-wait-{index}"),
                                "write_stdin",
                                json!({"session_id":self.handle,"yield_time_ms":250}),
                            )
                        })
                        .collect(),
                )),
                Ok(LLMChunk::Done),
            ])));
        }
        if self.wait_before_release && round == 1 {
            for index in 0..3 {
                let result = result_for(messages, &format!("continued-wait-{index}"));
                assert_eq!(result["status"], "running");
                assert_eq!(result["polling"]["exit_observed"], false);
                assert!(result["polling"].get("poll_suppressed").is_none());
            }
            std::fs::write(&self.gate, b"release").unwrap();
        }
        let chunks = match round - usize::from(self.wait_before_release) {
            0 => vec![
                Ok(LLMChunk::ToolCalls(vec![tool_call(
                    "quiet-final",
                    "write_stdin",
                    json!({"session_id":self.handle,"yield_time_ms":30_000}),
                )])),
                Ok(LLMChunk::Done),
            ],
            1 => {
                let result = result_for(messages, "quiet-final");
                assert_eq!(result["status"], "completed");
                assert_eq!(result["exit_code"], 7);
                assert_eq!(result["output"], "quiet-finished");
                assert!(result.get("session_id").is_none());
                vec![
                    Ok(LLMChunk::Token("Observed command exit 7.".into())),
                    Ok(LLMChunk::Done),
                ]
            }
            round => panic!("unexpected continuation request {round}"),
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

#[tokio::test(start_paused = true)]
async fn ignored_quiet_polling_ends_bounded_recovery_without_question_or_killing() {
    quiet_feedback_fixture(false).await;
}

#[tokio::test(start_paused = true)]
async fn output_and_exit_after_feedback_are_read_in_the_same_real_execution() {
    quiet_feedback_fixture(true).await;
}

async fn quiet_feedback_fixture(finish_after_feedback: bool) {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let quoted_gate = gate.to_string_lossy().replace('\'', "'\\''");
    let (mut session, config, mut state) = fixture(if finish_after_feedback {
        "quiet-exit-race"
    } else {
        "ignored-quiet-polling"
    });
    let executor = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let mut side_ctx = ToolCtx::none("side-start");
    side_ctx.session_id = Some(Arc::from(session.id.as_str()));
    let ToolOutcome::Completed(side) = executor
        .registry()
        .get("exec_command")
        .unwrap()
        .invoke(
            json!({"cmd":"cat","interactive":true,"yield_time_ms":250}),
            side_ctx.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("side command must use the ordinary exec protocol");
    };
    let side: Value = serde_json::from_str(&side.result).unwrap();
    let _side_cleanup = SideProcessCleanup(side["session_id"].as_str().unwrap().to_owned());
    let provider = Arc::new(IgnoredQuietReminder {
        command: format!(
            "while [ ! -f '{quoted_gate}' ]; do sleep 0.01; done; printf quiet-finished; exit 7"
        ),
        calls: AtomicUsize::new(0),
        handle: Mutex::new(None),
        side_handle: side["session_id"].as_str().unwrap().to_owned(),
        release_on_feedback: finish_after_feedback.then(|| gate.clone()),
    });
    let (tx, mut rx) = mpsc::channel(256);
    run_pipeline(
        &mut session,
        &tx,
        provider.clone(),
        executor.clone(),
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), if finish_after_feedback { 4 } else { 6 });
    assert!(session.pending_question.is_none());
    assert_ne!(state.runtime_state.status, bamboo_domain::AgentStatusState::Suspended);
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    assert_eq!(session.metadata.get("runtime.completion_reason").map(String::as_str),
        if finish_after_feedback { None } else { Some("observation_progress_exhausted") });
    let handle = provider.handle.lock().unwrap().clone().unwrap();
    let shell = bamboo_tools::tools::bash_runtime::get_shell(&handle);
    if finish_after_feedback {
        assert!(shell.is_none(), "the real final capture consumed the completed handle");
    } else {
        assert_eq!(shell.as_ref().unwrap().status(), "running");
    }
    let mut suppressed = 0;
    for index in 0..8 {
        let result = result_for(&session.messages, &format!("ignored-poll-{index}"));
        assert_eq!(result["status"], "running");
        assert_eq!(result["session_id"], handle);
        assert!(result.get("exit_code").is_none());
        suppressed += usize::from(result["polling"]["poll_suppressed"] == true);
    }
    assert!(
        suppressed > 0,
        "same-batch redundant waits are suppressed and every call stays paired"
    );
    let mut questions = 0;
    let mut dispatched_polls = 0;
    while let Ok(event) = rx.try_recv() {
        if matches!(&event, bamboo_agent_core::AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id.starts_with("ignored-poll-"))
        {
            dispatched_polls += 1;
        }
        if matches!(
            event,
            bamboo_agent_core::AgentEvent::NeedClarification { .. }
        ) {
            questions += 1;
        }
    }
    assert_eq!(questions, 0);
    if !finish_after_feedback {
        for round in 2..6 {
            let actual = result_for(&session.messages, &format!("feedback-poll-{round}"));
            assert_eq!(actual["status"], "running");
            assert!(actual["polling"].get("poll_suppressed").is_none(),
                "every post-feedback read reaches real capture, rather than synthetic running");
        }
    }
    for id in ["other-input", "other-operation"] {
        let other = result_for(&session.messages, id);
        assert!(other["polling"].get("poll_suppressed").is_none());
    }
    assert_eq!(
        dispatched_polls + suppressed,
        8,
        "suppressed polls never enter the executor"
    );
    // No synthetic completion or process control is needed to resume. Restore
    // real time before letting the real OS child produce its ordinary exit.
    if !finish_after_feedback {
        tokio::time::resume();
    }
    for id in ["other-input", "other-operation"] {
        let initial = result_for(&session.messages, id);
        let mut output = initial["output"].as_str().unwrap().to_owned();
        if let Some(handle) = initial["session_id"].as_str() {
            let ToolOutcome::Completed(last) = executor
                .registry()
                .get("write_stdin")
                .unwrap()
                .invoke(
                    json!({"session_id":handle,"yield_time_ms":5000}),
                    side_ctx.clone(),
                )
                .await
                .unwrap()
            else {
                panic!("side command poll must stay ordinary");
            };
            let last: Value = serde_json::from_str(&last.result).unwrap();
            assert_eq!(last["exit_code"], 0);
            output.push_str(last["output"].as_str().unwrap());
        }
        assert_eq!(
            output,
            if id == "other-input" {
                "side-input"
            } else {
                "independent"
            }
        );
    }
    if finish_after_feedback {
        return;
    }
    if !finish_after_feedback {
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(
            shell.as_ref().unwrap().status(),
            "running",
            "silence is not a short process timeout"
        );
        tokio::time::resume();
    }
    session.add_message(Message::user(
        "Read the still-pending command and collect its real result.",
    ));
    let resumed = Arc::new(ReadQuietCompletion {
        handle: handle.clone(),
        calls: AtomicUsize::new(0),
        gate,
        wait_before_release: true,
    });
    run_pipeline(
        &mut session,
        &tx,
        resumed.clone(),
        executor,
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .unwrap();
    assert!(session.pending_question.is_none());
    assert_eq!(
        resumed.calls.load(Ordering::SeqCst),
        3
    );
    assert!(bamboo_tools::tools::bash_runtime::get_shell(&handle).is_none());
}

struct CancelledPollProvider {
    handle: String,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl LLMProvider for CancelledPollProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::ToolCalls(vec![tool_call(
                "cancel-poll",
                "write_stdin",
                json!({"session_id":self.handle,"yield_time_ms":30_000}),
            )])),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn cancelling_the_runner_during_a_poll_keeps_the_delivered_command_readable() {
    let (mut session, config, mut state) = fixture("cancel-quiet-poll");
    let executor = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let mut ctx = ToolCtx::none("cancel-start");
    ctx.session_id = Some(Arc::from(session.id.as_str()));
    let ToolOutcome::Completed(start) = executor
        .registry()
        .get("exec_command")
        .unwrap()
        .invoke(
            json!({"cmd":"cat","interactive":true,"yield_time_ms":250}),
            ctx.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("expected an ordinary start result");
    };
    let start: Value = serde_json::from_str(&start.result).unwrap();
    let handle = start["session_id"].as_str().unwrap().to_owned();
    let _cleanup = SideProcessCleanup(handle.clone());
    let provider = Arc::new(CancelledPollProvider {
        handle: handle.clone(),
        calls: AtomicUsize::new(0),
    });
    let (tx, mut rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let observer = async {
        while let Some(event) = rx.recv().await {
            if matches!(event, bamboo_agent_core::AgentEvent::ToolStart { ref tool_call_id, .. } if tool_call_id == "cancel-poll")
            {
                cancel.cancel();
                return;
            }
        }
        panic!("poll did not enter the real runner");
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_pipeline(
                &mut session,
                &tx,
                provider.clone(),
                executor.clone(),
                &cancel,
                &config,
                &mut state
            ),
            observer
        )
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(AgentError::Cancelled)));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        bamboo_tools::tools::bash_runtime::get_shell(&handle)
            .unwrap()
            .status(),
        "running"
    );
    assert!(session
        .messages
        .iter()
        .filter(|m| m.tool_call_id.as_deref() == Some("cancel-poll"))
        .all(|m| m.tool_success != Some(true)));
    let ToolOutcome::Completed(final_result) = executor
        .registry()
        .get("write_stdin")
        .unwrap()
        .invoke(
            json!({"session_id":handle,"chars":"survived","eof":true,"yield_time_ms":5000}),
            ctx,
        )
        .await
        .unwrap()
    else {
        panic!("explicit continuation returns its actual result");
    };
    let final_result: Value = serde_json::from_str(&final_result.result).unwrap();
    assert_eq!(final_result["exit_code"], 0);
    assert_eq!(final_result["output"], "survived");
    assert!(bamboo_tools::tools::bash_runtime::get_shell(&handle).is_none());
}

fn tool_call(id: &str, name: &str, args: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: args.to_string(),
        },
    }
}

fn result_for(messages: &[Message], id: &str) -> Value {
    let message = messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(id))
        .expect("the next provider request contains the paired tool result");
    assert_eq!(message.role, Role::Tool);
    assert_eq!(message.tool_success, Some(true));
    serde_json::from_str(&message.content).expect("exec results retain their JSON protocol")
}

#[derive(Default)]
struct CompletionRecorder(AtomicUsize);

impl BashCompletionSink for CompletionRecorder {
    fn on_bash_completed(&self, _: BashCompletionInfo) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ResumeRecorder(AtomicUsize);

impl crate::runtime::config::BashResumeHook for ResumeRecorder {
    fn arrange_bash_self_resume(&self, _: String, _: Vec<String>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct NoopPersistence;

#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for NoopPersistence {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        Ok(())
    }
}

fn fixture(session_id: &str) -> (Session, AgentLoopConfig, LoopRunState) {
    let mut session = Session::new(session_id, "model");
    session.add_message(Message::user("Run the command and pull its result."));
    let config = AgentLoopConfig {
        model_name: Some("model".into()),
        persistence: Some(Arc::new(NoopPersistence)),
        prompt_memory_flags: crate::runtime::config::PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(4),
            ..Default::default()
        },
        ..Default::default()
    };
    let state = LoopRunState {
        session_id: session.id.clone(),
        execution_id: "exec-session-execution".into(),
        current_inputs: None,
        model_name: "model".into(),
        metrics_collector: None,
        debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
        task_context: None,
        overflow_recovery: Default::default(),
        task_evaluation: Default::default(),
        gold_evaluation: Default::default(),
        auxiliary_models: Default::default(),
        runtime_state: AgentRuntimeState::new(&session.id),
    };
    (session, config, state)
}

struct PullResultsProvider {
    command: String,
    gate: PathBuf,
    calls: AtomicUsize,
    handle: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl LLMProvider for PullResultsProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert!(messages.iter().all(|message| {
            message.role != Role::User
                || !message
                    .content
                    .contains("Runtime notification: background shell")
        }));
        let chunks = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                for name in ["exec_command", "write_stdin"] {
                    assert!(tools.iter().any(|tool| tool.function.name == name));
                }
                vec![
                    Ok(LLMChunk::ToolCalls(vec![tool_call(
                        "start-command",
                        "exec_command",
                        json!({"cmd": self.command, "yield_time_ms": 250}),
                    )])),
                    Ok(LLMChunk::Done),
                ]
            }
            1 => {
                let result = result_for(messages, "start-command");
                assert_eq!(result["status"], "running");
                let handle = result["session_id"].as_str().unwrap().to_string();
                *self.handle.lock().unwrap() = Some(handle.clone());
                tokio::fs::write(&self.gate, b"release").await.unwrap();
                vec![
                    Ok(LLMChunk::ToolCalls(vec![tool_call(
                        "pull-result",
                        "write_stdin",
                        json!({"session_id": handle, "chars": "", "yield_time_ms": 30_000}),
                    )])),
                    Ok(LLMChunk::Done),
                ]
            }
            2 => {
                assert_eq!(result_for(messages, "start-command")["status"], "running");
                let result = result_for(messages, "pull-result");
                assert_eq!(result["status"], "completed");
                assert_eq!(result["exit_code"], 7);
                assert!(result["output"].as_str().unwrap().contains("exec-finished"));
                vec![
                    Ok(LLMChunk::Token("Observed exit code 7.".into())),
                    Ok(LLMChunk::Done),
                ]
            }
            round => panic!("unexpected provider request {round}"),
        };
        Ok(Box::pin(stream::iter(chunks)))
    }
}

#[tokio::test]
async fn model_pulls_exec_completion_as_tool_result_without_a_runtime_user_notification() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let provider = Arc::new(PullResultsProvider {
        command: gated_command(&gate),
        gate,
        calls: AtomicUsize::new(0),
        handle: Mutex::new(None),
    });
    let (mut session, mut config, mut state) = fixture("model-pulls-exec-result");
    let resume = Arc::new(ResumeRecorder::default());
    let completion = Arc::new(CompletionRecorder::default());
    config.bash_resume_hook = Some(resume.clone());
    config.bash_completion_sink = Some(completion.clone());
    let executor = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let (tx, _rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let complete = tokio::time::timeout(
        Duration::from_secs(10),
        run_pipeline(
            &mut session,
            &tx,
            provider.clone(),
            executor,
            &cancel,
            &config,
            &mut state,
        ),
    )
    .await
    .expect("gated command and explicit poll finish promptly")
    .unwrap();

    assert!(complete);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert!(provider.handle.lock().unwrap().is_some());
    assert!(state.runtime_state.waiting_for_bash.is_none());
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    assert_eq!(resume.0.load(Ordering::SeqCst), 0);
    assert_eq!(completion.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn outstanding_model_polled_exec_does_not_arm_legacy_bash_wait_or_resume() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let (mut session, mut config, mut state) = fixture("exec-excluded-from-bash-wait");
    let resume = Arc::new(ResumeRecorder::default());
    let completion = Arc::new(CompletionRecorder::default());
    config.bash_resume_hook = Some(resume.clone());
    let executor = bamboo_tools::BuiltinToolExecutor::new();
    let exec = executor.registry().get("exec_command").unwrap();
    let poll = executor.registry().get("write_stdin").unwrap();
    let mut ctx = ToolCtx::none("start-command");
    ctx.session_id = Some(Arc::from(session.id.as_str()));
    ctx.can_async_resume = true;
    ctx.bash_completion_sink = Some(completion.clone());
    let ToolOutcome::Completed(started) = exec
        .invoke(
            json!({"cmd": gated_command(&gate), "yield_time_ms": 250}),
            ctx.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("yield returns a completed protocol request");
    };
    let started: Value = serde_json::from_str(&started.result).unwrap();
    let handle = started["session_id"].as_str().unwrap();
    let shell = bamboo_tools::tools::bash_runtime::get_shell(handle).unwrap();
    assert_eq!(shell.status(), "running");

    let outcome =
        maybe_suspend_for_outstanding_bash(&mut session, &config, &mut state.runtime_state).await;
    assert!(outcome.is_none());
    assert!(state.runtime_state.waiting_for_bash.is_none());
    assert_eq!(resume.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        shell.status(),
        "running",
        "the terminal gate does not kill a pull session"
    );

    tokio::fs::write(&gate, b"release").await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        poll.invoke(
            json!({"session_id": handle, "chars": "", "yield_time_ms": 30_000}),
            ctx,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let ToolOutcome::Completed(result) = result else {
        panic!("poll returns its final tool result directly");
    };
    let result: Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(result["exit_code"], 7);
    assert_eq!(completion.0.load(Ordering::SeqCst), 0);
}
