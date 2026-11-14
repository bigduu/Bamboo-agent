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
    assert!(
        !maybe_background_quiet_exec(&mut other, &config, &mut other_runtime, &tx, handle)
            .await
            .unwrap()
    );
    assert!(maybe_background_quiet_exec(
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
    tokio::fs::write(&gate, b"release").await.unwrap();
    executor
        .registry()
        .get("write_stdin")
        .unwrap()
        .invoke(json!({"session_id": handle, "yield_time_ms": 30_000}), ctx)
        .await
        .unwrap();
}
