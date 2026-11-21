use super::*;
use bamboo_agent_core::{storage::Storage, AgentHook};
use bamboo_domain::{
    SessionActivationPolicy, SessionInboxLimits, SessionInboxPort, SessionMessageBody,
    SessionMessageContent, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource, SessionRuntimeInstruction,
};

#[derive(Default)]
struct BackgroundHook {
    jobs: Mutex<Vec<bamboo_tools::tools::exec_command::BackgroundExecJob>>,
}

impl crate::runtime::config::BashResumeHook for BackgroundHook {
    fn arrange_bash_self_resume(&self, _: String, _: Vec<String>) {
        panic!("exec handoff must use its capable hook");
    }

    fn supports_background_exec(&self) -> bool {
        true
    }

    fn arrange_exec_self_resume(
        &self,
        job: bamboo_tools::tools::exec_command::BackgroundExecJob,
        _: chrono::DateTime<Utc>,
    ) {
        self.jobs.lock().unwrap().push(job);
    }
}

struct QuietBackgroundProvider {
    command: String,
    calls: AtomicUsize,
    handle: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl LLMProvider for QuietBackgroundProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let calls = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![tool_call(
                "start-background",
                "exec_command",
                json!({"cmd": self.command, "yield_time_ms": 250}),
            )],
            1 => {
                let handle = result_for(messages, "start-background")["session_id"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                *self.handle.lock().unwrap() = Some(handle.clone());
                tokio::time::advance(Duration::from_secs(90)).await;
                (0..8)
                    .map(|i| {
                        tool_call(
                            &format!("quiet-background-{i}"),
                            "write_stdin",
                            json!({"session_id": handle, "chars": "", "yield_time_ms": 250}),
                        )
                    })
                    .collect()
            }
            round => panic!("background handoff must not spend another provider request: {round}"),
        };
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::ToolCalls(calls)),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[derive(Default)]
struct ConversationProvider(AtomicUsize);

#[async_trait::async_trait]
impl LLMProvider for ConversationProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(self.0.fetch_add(1, Ordering::SeqCst), 0);
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::Token(
                "We can continue discussing the next step.".into(),
            )),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test(start_paused = true)]
async fn background_exec_handoff_releases_the_conversation_and_keeps_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let quoted = gate.to_string_lossy().replace('\'', "'\\''");
    let provider = Arc::new(QuietBackgroundProvider {
        command: format!(
            "while [ ! -f '{quoted}' ]; do sleep 0.01; done; printf background-finished; exit 7"
        ),
        calls: AtomicUsize::new(0),
        handle: Mutex::new(None),
    });
    let (mut session, mut config, mut state) = fixture("quiet-background-session");
    config.run_budget.max_rounds = None;
    let hook = Arc::new(BackgroundHook::default());
    config.bash_resume_hook = Some(hook.clone());
    let executor = Arc::new(yielding_executor());
    let (tx, mut rx) = mpsc::channel(256);
    let complete = run_pipeline(
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
    assert!(!complete, "the foreground is suspended, not completed");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(session.pending_question.is_none());
    assert!(!session.metadata.contains_key("runtime.completion_reason"));
    let handle = provider.handle.lock().unwrap().clone().unwrap();
    assert_eq!(
        state
            .runtime_state
            .waiting_for_bash
            .as_ref()
            .unwrap()
            .bash_ids,
        vec![handle.clone()]
    );
    assert_eq!(
        state.runtime_state.status,
        bamboo_domain::AgentStatusState::Suspended
    );
    assert!(session.messages.iter().any(|m| m
        .metadata
        .as_ref()
        .is_some_and(|meta| meta["runtime_kind"] == "background_exec_wait")));
    assert!(!session
        .messages
        .iter()
        .any(|m| m.content.contains("finite diagnostic budget is exhausted")));
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(event, AgentEvent::NeedClarification { .. }));
    }
    let job = hook.jobs.lock().unwrap().pop().unwrap();
    assert_eq!(job.id(), handle);
    assert_eq!(
        bamboo_tools::tools::bash_runtime::get_shell(&handle)
            .unwrap()
            .status(),
        "running"
    );

    session.add_message(Message::user(
        "Discuss the next step while that command runs.",
    ));
    let mut next = fixture(&session.id).2;
    next.runtime_state = state.runtime_state.clone();
    next.runtime_state.status = bamboo_domain::AgentStatusState::Running;
    let conversation = Arc::new(ConversationProvider::default());
    run_pipeline(
        &mut session,
        &tx,
        conversation.clone(),
        executor.clone(),
        &CancellationToken::new(),
        &config,
        &mut next,
    )
    .await
    .unwrap();
    assert_eq!(conversation.0.load(Ordering::SeqCst), 1);
    assert!(next.runtime_state.waiting_for_bash.is_some());
    assert!(session
        .messages
        .iter()
        .any(|m| m.content == "We can continue discussing the next step."));
    assert_eq!(
        bamboo_tools::tools::bash_runtime::get_shell(&handle)
            .unwrap()
            .status(),
        "running"
    );

    tokio::time::resume();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let info = tokio::time::timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(info.exit_code, Some(7));
    assert!(info.output_tail.contains("background-finished"));
    let mut ctx = ToolCtx::none("collect-background");
    ctx.session_id = Some(Arc::from(session.id.as_str()));
    let ToolOutcome::Completed(result) = executor
        .registry()
        .get("write_stdin")
        .unwrap()
        .invoke(json!({"session_id": handle}), ctx)
        .await
        .unwrap()
    else {
        panic!("ordinary result");
    };
    let result: Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(result["exit_code"], 7);
    assert_eq!(result["output"], "background-finished");
}

#[tokio::test(start_paused = true)]
async fn quiet_exec_wait_ceiling_requires_a_fresh_empty_poll_to_wait_again() {
    use bamboo_tools::tools::{bash_runtime::get_shell, exec_command::reserve_quiet_exec_job};

    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let quoted = gate.to_string_lossy().replace('\'', "'\\''");
    let command = format!("while [ ! -f '{quoted}' ]; do sleep 0.01; done; exit 7");
    let executor = yielding_executor();
    let start = executor.registry().get("exec_command").unwrap();
    let poll = executor.registry().get("write_stdin").unwrap();
    let owner = "bounded-quiet-exec-wait";
    let context = |id: &str| {
        let mut ctx = ToolCtx::none(id);
        ctx.session_id = Some(Arc::from(owner));
        ctx
    };
    let ToolOutcome::Completed(started) = start
        .invoke(
            json!({"cmd": command, "yield_time_ms": 250}),
            context("start"),
        )
        .await
        .unwrap()
    else {
        panic!("expected running result");
    };
    let result: Value = serde_json::from_str(&started.result).unwrap();
    let handle = result["session_id"].as_str().unwrap();
    let poll_args = json!({"session_id": handle, "yield_time_ms": 250});
    for n in 0..4 {
        poll.invoke(poll_args.clone(), context(&format!("quiet-{n}")))
            .await
            .unwrap();
    }

    let job = reserve_quiet_exec_job(owner, handle).await.unwrap();
    let started = tokio::time::Instant::now();
    let message = wait_for_exec(
        job,
        &CancellationToken::new(),
        Some(QUIET_EXEC_WAIT_TIMEOUT),
    )
    .await
    .unwrap();
    assert!(started.elapsed() >= QUIET_EXEC_WAIT_TIMEOUT);
    assert!(message.content.contains("reached its wait ceiling"));
    assert!(message.content.contains("Completion is not confirmed"));
    assert!(message.content.contains(handle));
    assert_eq!(get_shell(handle).unwrap().status(), "running");
    assert!(reserve_quiet_exec_job(owner, handle).await.is_none());

    let ToolOutcome::Completed(poll_after_ceiling) = poll
        .invoke(poll_args.clone(), context("poll-after-ceiling"))
        .await
        .unwrap()
    else {
        panic!("expected running result");
    };
    let result: Value = serde_json::from_str(&poll_after_ceiling.result).unwrap();
    assert_eq!(result["status"], "running");
    assert_eq!(result["session_id"], handle);
    assert!(result.get("exit_code").is_none());
    assert!(reserve_quiet_exec_job("another-owner", handle)
        .await
        .is_none());

    let job = reserve_quiet_exec_job(owner, handle).await.unwrap();
    poll.invoke(poll_args.clone(), context("poll-during-wait"))
        .await
        .unwrap();
    assert!(reserve_quiet_exec_job(owner, handle).await.is_none());
    tokio::time::resume();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let message = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_exec(
            job,
            &CancellationToken::new(),
            Some(QUIET_EXEC_WAIT_TIMEOUT),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(message.content.contains(r#""exit_code":7"#));

    let ToolOutcome::Completed(collected) = poll
        .invoke(poll_args, context("collect-terminal-result"))
        .await
        .unwrap()
    else {
        panic!("expected terminal result");
    };
    let result: Value = serde_json::from_str(&collected.result).unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(result["exit_code"], 7);
}

struct PollMonitoredExec {
    handle: String,
    calls: AtomicUsize,
    expect_completion: bool,
}

#[async_trait::async_trait]
impl LLMProvider for PollMonitoredExec {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.expect_completion && round > 0 {
            assert_eq!(round, 1);
            assert!(messages
                .iter()
                .any(|message| message.content.contains("monitor-finished")));
            assert!(!messages
                .iter()
                .any(|message| { message.content.contains("[runtime_progress_feedback_v1]") }));
            return Ok(Box::pin(stream::iter([
                Ok(LLMChunk::Token("Observed exit 7.".into())),
                Ok(LLMChunk::Done),
            ])));
        }
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::ToolCalls(
                (0..8)
                    .map(|index| {
                        tool_call(
                            &format!("monitor-followup-{round}-{index}"),
                            "write_stdin",
                            json!({"session_id": self.handle, "yield_time_ms": 250}),
                        )
                    })
                    .collect(),
            )),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct DeliverExecAfterPolling {
    monitor: Arc<BackgroundHook>,
    gate: PathBuf,
    inbox: Arc<dyn SessionInboxPort>,
    persistence: Arc<bamboo_storage::LockedSessionStore>,
}

#[async_trait::async_trait]
impl AgentHook for DeliverExecAfterPolling {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterRound
    }

    async fn run(&self, _: AgentHookPoint, payload: &HookPayload, session: &Session) -> HookResult {
        if !matches!(payload, HookPayload::Round { round: 1 }) {
            return HookResult::Continue;
        }
        // The quiet batch is paired. Release the actual process and admit
        // its terminal capture before the next round can spend diagnostics.
        let job = self.monitor.jobs.lock().unwrap().pop().unwrap();
        tokio::time::resume();
        tokio::fs::write(&self.gate, b"release").await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), job.completion())
            .await
            .unwrap();
        let data = json!({
            "session_id": result.session_id,
            "exec_session_id": result.bash_id,
            "status": result.status,
            "exit_code": result.exit_code,
            "result": result.output_tail,
        });
        assert_eq!(data["exit_code"], 7);
        let completion = SessionMessageEnvelope {
            id: SessionMessageId::stable("background_exec_completion", &data),
            target_session_id: session.id.clone(),
            source: SessionMessageSource::Runtime {
                subsystem: "background_exec".into(),
            },
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: "background_exec_completed".into(),
                content: Some(SessionMessageContent::text(result.output_tail)),
                data: Some(data),
                provider_message: None,
            }),
            correlation_id: Some(result.bash_id),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
        };
        let receipt = self
            .inbox
            .deliver_runtime_for_target(&completion, session.created_at)
            .await
            .unwrap();
        self.inbox
            .mark_activation_eligible(
                &session.id,
                receipt.generation,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .unwrap();
        self.persistence
            .update_runtime_config(&session.id, |durable| {
                durable
                    .agent_runtime_state
                    .as_mut()
                    .unwrap()
                    .waiting_for_bash = None;
                durable.metadata.remove("runtime.background_exec_id");
            })
            .await
            .unwrap()
            .unwrap();
        HookResult::Continue
    }
}

#[tokio::test(start_paused = true)]
async fn background_exec_quiet_followup_reuses_the_existing_wait() {
    quiet_followup(false).await;
}

#[tokio::test(start_paused = true)]
async fn background_exec_completion_at_the_quiet_boundary_reaches_the_model() {
    quiet_followup(true).await;
}

async fn quiet_followup(completes_at_boundary: bool) {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let quoted = gate.to_string_lossy().replace('\'', "'\\''");
    let provider = Arc::new(QuietBackgroundProvider {
        command: format!(
            "while [ ! -f '{quoted}' ]; do sleep 0.01; done; printf monitor-finished; exit 7"
        ),
        calls: AtomicUsize::new(0),
        handle: Mutex::new(None),
    });
    let (mut session, mut config, mut state) = fixture("quiet-background-followup");
    config.run_budget.max_rounds = None;
    let hook = Arc::new(BackgroundHook::default());
    config.bash_resume_hook = Some(hook.clone());
    let executor = Arc::new(yielding_executor());
    let (tx, _rx) = mpsc::channel(256);
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
    let handle = provider.handle.lock().unwrap().clone().unwrap();
    let wait = state.runtime_state.waiting_for_bash.clone().unwrap();
    session.metadata.remove("runtime.suspend_reason");
    session.add_message(Message::user("Keep waiting for the existing command."));
    state.runtime_state.status = bamboo_domain::AgentStatusState::Running;
    state.runtime_state.suspension = None;
    if completes_at_boundary {
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        state_bridge::write_runtime_state(&mut session, &state.runtime_state);
        store.save_session(&session).await.unwrap();
        let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(store.clone()));
        let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let mut hooks = crate::runtime::HookRunner::new();
        hooks.register(Arc::new(DeliverExecAfterPolling {
            monitor: hook.clone(),
            gate: gate.clone(),
            inbox: inbox.clone(),
            persistence: persistence.clone(),
        }));
        config.storage = Some(store);
        config.persistence = Some(persistence);
        config.session_inbox = Some(inbox);
        config.hook_runner = Arc::new(hooks);
    }
    let followup = Arc::new(PollMonitoredExec {
        handle: handle.clone(),
        calls: AtomicUsize::new(0),
        expect_completion: completes_at_boundary,
    });
    let complete = run_pipeline(
        &mut session,
        &tx,
        followup.clone(),
        executor,
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .unwrap();

    assert!(!session.metadata.contains_key("runtime.completion_reason"));
    if completes_at_boundary {
        assert!(complete);
        assert_eq!(followup.calls.load(Ordering::SeqCst), 2);
        assert!(state.runtime_state.waiting_for_bash.is_none());
        assert!(hook.jobs.lock().unwrap().is_empty());
        assert!(session
            .messages
            .iter()
            .any(|message| message.content == "Observed exit 7."));
        return;
    }
    assert!(!complete);
    assert_eq!(followup.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.runtime_state.waiting_for_bash, Some(wait));
    assert_eq!(
        state.runtime_state.status,
        bamboo_domain::AgentStatusState::Suspended
    );
    let job = {
        let mut jobs = hook.jobs.lock().unwrap();
        assert_eq!(jobs.len(), 1, "retain the original monitor");
        jobs.pop().unwrap()
    };
    assert_eq!(job.id(), handle);
    tokio::time::resume();
    tokio::fs::write(&gate, b"release").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), job.completion())
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(7));
    assert!(result.output_tail.contains("monitor-finished"));
}

struct FailedBackgroundCheckpoint;

#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for FailedBackgroundCheckpoint {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        Err(std::io::Error::other("controlled checkpoint rejection"))
    }
}

#[tokio::test]
async fn background_exec_checkpoint_failure_or_wrong_owner_cannot_publish_a_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let (mut session, mut config, mut state) = fixture("failed-background-checkpoint");
    let hook = Arc::new(BackgroundHook::default());
    config.bash_resume_hook = Some(hook.clone());
    config.persistence = Some(Arc::new(FailedBackgroundCheckpoint));
    let executor = bamboo_tools::BuiltinToolExecutor::new();
    let mut ctx = ToolCtx::none("start-checkpoint-command");
    ctx.session_id = Some(Arc::from(session.id.as_str()));
    let ToolOutcome::Completed(started) = executor
        .registry()
        .get("exec_command")
        .unwrap()
        .invoke(
            json!({"cmd": gated_command(&gate), "yield_time_ms": 250}),
            ctx.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("ordinary exec");
    };
    let started: Value = serde_json::from_str(&started.result).unwrap();
    let handle = started["session_id"].as_str().unwrap();
    let before = serde_json::to_value(&session).unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    let mut other = Session::new("wrong-background-owner", "model");
    let mut other_runtime = AgentRuntimeState::new(&other.id);
    assert!(!maybe_suspend_for_background_exec(
        &mut other,
        &config,
        &mut other_runtime,
        &tx,
        handle
    )
    .await
    .unwrap());
    assert!(maybe_suspend_for_background_exec(
        &mut session,
        &config,
        &mut state.runtime_state,
        &tx,
        handle
    )
    .await
    .is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert!(state.runtime_state.waiting_for_bash.is_none());
    assert!(hook.jobs.lock().unwrap().is_empty());
    assert!(rx.try_recv().is_err());
    let reservation =
        bamboo_tools::tools::exec_command::reserve_background_job(&session.id, handle).unwrap();
    assert_eq!(
        bamboo_tools::tools::bash_runtime::get_shell(handle)
            .unwrap()
            .status(),
        "running"
    );
    drop(reservation);
    let handles = [handle.to_owned()];
    let cancel = CancellationToken::new();
    let (waiting, released) = tokio::join!(
        observe_explicit_exec_waits(
            &mut session,
            &config,
            &mut state.runtime_state,
            &tx,
            &handles,
            &cancel,
        ),
        tokio::fs::write(&gate, b"release"),
    );
    released.unwrap();
    assert!(
        !waiting.unwrap(),
        "the current run observes the original process"
    );
    assert!(hook.jobs.lock().unwrap().is_empty());
    assert!(state.runtime_state.waiting_for_bash.is_none());
    let message = session.messages.last().unwrap();
    assert!(message.content.contains(r#""exit_code":7"#));
    assert!(message.content.contains("exec-finished"));
    let ToolOutcome::Completed(pulled) = executor
        .registry()
        .get("write_stdin")
        .unwrap()
        .invoke(json!({"session_id": handle, "yield_time_ms": 30_000}), ctx)
        .await
        .unwrap()
    else {
        panic!("ordinary exec result");
    };
    let pulled: Value = serde_json::from_str(&pulled.result).unwrap();
    assert_eq!(pulled["status"], "completed");
    assert!(pulled["output"].as_str().unwrap().contains("exec-finished"));
}

#[derive(Default)]
struct MixedWaitProvider(AtomicUsize);

#[async_trait::async_trait]
impl LLMProvider for MixedWaitProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(self.0.fetch_add(1, Ordering::SeqCst), 0);
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::ToolCalls(vec![
                tool_call(
                    "wait-exec",
                    "SubAgent",
                    json!({"intent":"wait", "target":"exec-handle"}),
                ),
                tool_call("ask-format", "choose_output_format", json!({})),
            ])),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct MixedWaitTool(&'static str);

#[async_trait::async_trait]
impl bamboo_agent_core::tools::Tool for MixedWaitTool {
    fn name(&self) -> &str {
        self.0
    }

    fn description(&self) -> &str {
        "Synthetic wait boundary."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }

    async fn invoke(
        &self,
        _: Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, bamboo_agent_core::tools::ToolError> {
        use bamboo_agent_core::tools::ToolResult;

        if self.0 == "SubAgent" {
            let mut receipt = ToolResult::text(
                true,
                json!({"status":"wait_requested", "session_id":"exec-handle"}).to_string(),
            );
            receipt.display_preference =
                Some(bamboo_tools::tools::exec_command::WAIT_FOR_EXEC_DISPLAY.into());
            return Ok(ToolOutcome::Completed(receipt));
        }
        Ok(ToolOutcome::NeedsHuman {
            question: bamboo_agent_core::PendingQuestion {
                tool_call_id: ctx.tool_call_id.to_string(),
                tool_name: self.0.into(),
                question: "Which output format?".into(),
                options: vec!["Text".into(), "JSON".into()],
                allow_custom: false,
                source: bamboo_agent_core::PendingQuestionSource::PauseTool,
            },
            result: ToolResult::text(true, "Waiting for the output format."),
        })
    }
}

#[tokio::test]
async fn background_exec_explicit_wait_is_reported_as_deferred_by_a_same_batch_question() {
    let (mut session, config, mut state) = fixture("mixed-exec-wait");
    let provider = Arc::new(MixedWaitProvider::default());
    let executor = Arc::new(yielding_executor());
    executor.register_tool(MixedWaitTool("SubAgent")).unwrap();
    executor
        .register_tool(MixedWaitTool("choose_output_format"))
        .unwrap();
    let (tx, _rx) = mpsc::channel(128);
    assert!(!run_pipeline(
        &mut session,
        &tx,
        provider.clone(),
        executor,
        &CancellationToken::new(),
        &config,
        &mut state,
    )
    .await
    .unwrap());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        result_for(&session.messages, "wait-exec")["status"],
        "wait_requested"
    );
    assert_eq!(session.pending_question.unwrap().tool_call_id, "ask-format");
    assert!(state.runtime_state.waiting_for_bash.is_none());
    let deferred = session
        .messages
        .iter()
        .find(|message| {
            message.metadata.as_ref().is_some_and(|metadata| {
                metadata["runtime_kind"] == "exec_wait_deferred"
                    && metadata["hidden_from_ui"] == true
            })
        })
        .unwrap();
    assert!(deferred.content.contains("exec-handle"));
}
