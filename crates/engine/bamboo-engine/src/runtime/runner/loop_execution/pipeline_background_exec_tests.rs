use super::*;

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
    let executor = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
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
