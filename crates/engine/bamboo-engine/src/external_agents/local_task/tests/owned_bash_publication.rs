//! Real SDK -> Agent -> BuiltinToolExecutor -> exec_command ownership, including the
//! final publication barrier. The controlled provider only chooses the tool;
//! it neither launches nor terminates the process on behalf of the runtime.
use super::*;
use bamboo_agent_core::tools::FunctionCall;

struct BashCeiling;
#[async_trait]
impl NativeToolCeilingSource for BashCeiling {
    async fn observe(&self, _: &Session) -> Result<Vec<String>, String> {
        Ok(vec!["exec_command".into(), "write_stdin".into()])
    }
}

struct BashProvider {
    command: String,
    workspace: PathBuf,
    calls: AtomicUsize,
}
#[async_trait]
impl LLMProvider for BashProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "unexpected retry after tool result: {:?}",
            messages
                .iter()
                .rev()
                .find(|message| message.role == Role::Tool)
                .map(|message| message.content.chars().take(600).collect::<String>())
        );
        assert!(tools
            .iter()
            .any(|tool| tool.function.name == "exec_command"));
        assert!(!tools.iter().any(|tool| tool.function.name == "Bash"));
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: "owned-builtin-bash".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "exec_command".into(),
                    arguments: serde_json::json!({
                        "cmd": self.command,
                        "workdir": self.workspace,
                        "yield_time_ms": 1000,
                    })
                    .to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn sdk_owned_bash_stops_before_publication(shutdown: bool) {
    let home = tempfile::tempdir().unwrap();
    let workspace = home.path().join("bash-workspace");
    std::fs::create_dir(&workspace).unwrap();
    // The descendant is independently observable and keeps trying to mutate
    // the workspace. Releasing it AFTER the terminal event catches a detached
    // descendant even if its last heartbeat happened before cancellation.
    let provider = Arc::new(BashProvider {
        command: "printf '%s\\n' \"$$\" > leader.pid; \
            (i=0; while [ \"$i\" -lt 2000 ]; do \
              printf x >> heartbeat; \
              if [ -f release-late-write ]; then printf forbidden > late-write; exit; fi; \
              i=$((i + 1)); sleep 0.02; \
            done) & printf '%s\\n' \"$!\" > descendant.pid; wait"
            .into(),
        workspace: workspace.clone(),
        calls: AtomicUsize::new(0),
    });
    let mut harness = Harness::open_with_tools(
        home.path(),
        &root_id(),
        provider.clone(),
        1,
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        Arc::new(BashCeiling),
    )
    .await;
    let mut child = harness
        .child("owned-bash", "Run the owned shell command.")
        .await;
    child.set_workspace_path_meta(workspace.to_string_lossy());
    harness.store.save_session(&child).await.unwrap();
    bamboo_agent_core::workspace_state::set_workspace(&child.id, workspace.canonicalize().unwrap());
    let mut events = {
        let mut senders = harness.ctx.session_event_senders.write().await;
        let sender = senders[&harness.parent.id].clone();
        senders.insert(child.id.clone(), sender.clone());
        sender.subscribe()
    };
    harness.launch(&child).await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if workspace.join("leader.pid").exists()
                && workspace.join("descendant.pid").exists()
                && std::fs::metadata(workspace.join("heartbeat"))
                    .is_ok_and(|metadata| metadata.len() >= 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the production SDK Agent must actually start the Bash builtin");
    let read_pid = |name| {
        std::fs::read_to_string(workspace.join(name))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap()
    };
    let leader = read_pid("leader.pid");
    let descendant = read_pid("descendant.pid");
    assert!(leader > 0 && descendant > 0 && leader != descendant);
    // Signal 0 is only a liveness observation, never stop proof or a request
    // to terminate. The production owner is the only code that stops Bash.
    assert_eq!(unsafe { libc::kill(leader, 0) }, 0);
    assert_eq!(unsafe { libc::kill(descendant, 0) }, 0);
    let execution = lock(&harness.runner.executions)[&child.id].clone();
    assert!(lock(&execution.stop).is_none());
    assert!(*lock(&execution.launch) == LaunchState::Running);
    if shutdown {
        tokio::time::timeout(WAIT, harness.runner.shutdown())
            .await
            .expect("shutdown must join the active Agent and its Bash resources")
            .unwrap();
    } else {
        harness.ctx.agent_runners.read().await[&child.id]
            .cancel_token
            .cancel();
    }
    tokio::time::timeout(WAIT, async {
        let mut history_committed = false;
        loop {
            match events.recv().await.unwrap() {
                AgentEvent::SessionHistoryCommitted { session_id } => {
                    assert_eq!(session_id, child.id);
                    let durable = harness
                        .store
                        .load_session(&child.id)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(durable.metadata["runtime.local_task.state"], "stopped");
                    let source =
                        crate::execution::ChildCompletionSource::from_committed_session(&durable)
                            .expect("history publication requires the committed terminal source");
                    assert_eq!(source.status, "cancelled");
                    assert_eq!(source.activation_run_id, execution.owner.fence.run_id);
                    // The direct child must be reaped already, not merely sent
                    // a cancellation/kill request that will settle later.
                    assert_eq!(unsafe { libc::kill(leader, 0) }, -1);
                    assert_eq!(
                        std::io::Error::last_os_error().raw_os_error(),
                        Some(libc::ESRCH)
                    );
                    history_committed = true;
                }
                AgentEvent::SubAgentCompleted {
                    child_session_id,
                    status,
                    error,
                    ..
                } => {
                    assert_eq!(child_session_id, child.id);
                    assert_eq!(status, "cancelled", "{error:?}");
                    assert!(
                        history_committed,
                        "history/source must precede parent completion"
                    );
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("cancelled Bash must cross the actual SDK publication barrier");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "cancelled", "{error:?}");
    let saved = harness.assert_terminal(&child, "cancelled").await;
    let stop = lock(&execution.stop)
        .clone()
        .expect("real join/drain stop proof");
    assert_eq!(
        saved.metadata["runtime.local_task.instance"],
        stop.instance_id.to_string()
    );
    assert_eq!(stop.instance_id, execution.resources.identity().owner_id);
    assert!(execution.task.lock().await.is_none());
    assert!(execution.heartbeat.lock().await.is_none());
    assert!(lock(&execution.slot).is_none());
    assert!(
        execution.resources.spawn_owned(async {}).is_err(),
        "closed owner must reject new work"
    );
    assert_eq!(harness.runner.concurrency.available_permits(), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let heartbeat = std::fs::read(workspace.join("heartbeat")).unwrap();
    std::fs::write(workspace.join("release-late-write"), b"go").unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        std::fs::read(workspace.join("heartbeat")).unwrap(),
        heartbeat
    );
    assert!(
        !workspace.join("late-write").exists(),
        "no descendant may write after terminal publication"
    );
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{descendant}/stat")) {
        // Orphaned zombies depend on the container's init for reaping. They
        // cannot execute; a running/sleeping descendant is still a failure.
        let state = stat.rsplit_once(") ").unwrap().1.as_bytes()[0];
        assert!(
            state == b'Z' || state == b'X',
            "descendant is still executable: {stat}"
        );
    }
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_sdk_cancel_reaps_real_bash_before_terminal_publication() {
    sdk_owned_bash_stops_before_publication(false).await;
}

#[tokio::test]
async fn production_sdk_shutdown_reaps_real_bash_before_terminal_publication() {
    sdk_owned_bash_stops_before_publication(true).await;
}

struct QuietCommandProvider(AtomicUsize);

#[async_trait]
impl LLMProvider for QuietCommandProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        let last_result = || {
            let result = messages
                .iter()
                .rev()
                .find(|msg| msg.role == Role::Tool)
                .unwrap();
            serde_json::from_str::<serde_json::Value>(&result.content).unwrap()
        };
        let (name, args) = match call {
            0 => (
                "exec_command",
                serde_json::json!({
                    "cmd": "i=0; while [ ! -f release ] && [ \"$i\" -lt 280 ]; do i=$((i + 1)); sleep 0.05; done; test -f release && printf verified",
                    "yield_time_ms": 250,
                }),
            ),
            1 => {
                let result = last_result();
                assert_eq!(result["status"], "running");
                (
                    "write_stdin",
                    serde_json::json!({
                        "session_id": result["session_id"],
                        "chars": "",
                        "yield_time_ms": 10000,
                    }),
                )
            }
            2 => {
                let result = last_result();
                assert_eq!(result["status"], "completed");
                assert_eq!(result["exit_code"], 0);
                assert_eq!(result["output"], "verified");
                return Ok(Box::pin(futures::stream::iter(vec![
                    Ok(LLMChunk::Token("The command was verified.".into())),
                    Ok(LLMChunk::Done),
                ])));
            }
            _ => panic!("the same command must finish without another launch or poll"),
        };
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: format!("quiet-command-{call}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: name.into(),
                    arguments: args.to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn production_sdk_quiet_owned_command_outlives_child_idle_limit() {
    let home = tempfile::tempdir().unwrap();
    let workspace = home.path().join("quiet-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let provider = Arc::new(QuietCommandProvider(AtomicUsize::new(0)));
    let mut harness = Harness::open_with_tools(
        home.path(),
        &root_id(),
        provider.clone(),
        1,
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        Arc::new(BashCeiling),
    )
    .await;
    let mut child = harness
        .child("quiet-command", "Wait for the command result.")
        .await;
    child.set_workspace_path_meta(workspace.to_string_lossy());
    child
        .metadata
        .insert("child_watchdog.check_interval_secs".into(), "1".into());
    child
        .metadata
        .insert("child_watchdog.max_idle_secs".into(), "2".into());
    harness.store.save_session(&child).await.unwrap();
    bamboo_agent_core::workspace_state::set_workspace(&child.id, workspace.canonicalize().unwrap());
    let mut events = harness.child_events(&child).await;
    harness.launch(&child).await.unwrap();

    tokio::time::timeout(WAIT, async {
        loop {
            if matches!(events.recv().await.unwrap(), AgentEvent::ToolStart { tool_name, .. } if tool_name == "write_stdin") {
                break;
            }
        }
    })
    .await
    .expect("the Child must poll its original process");
    let runner = harness.ctx.agent_runners.read().await[&child.id].clone();
    // Hold a silent, real process past the Child watchdog's idle window and
    // at least one watchdog tick, then let the same command exit normally.
    tokio::time::sleep(Duration::from_millis(3400)).await;
    let cancelled_while_waiting = runner.cancel_token.is_cancelled();
    let sample = chrono::Utc::now();
    let live_wait =
        harness
            .runner
            .shell_wait_activity_at(&child.id, &runner.run_id, child.created_at, sample);
    let foreign_wait =
        harness
            .runner
            .shell_wait_activity_at(&child.id, "different-run", child.created_at, sample);
    std::fs::write(workspace.join("release"), []).unwrap();
    let (id, status, error) = harness.completion().await;
    harness.runner.shutdown().await.unwrap();

    assert_eq!(id, child.id);
    assert_eq!(live_wait, Some(sample));
    assert_eq!(foreign_wait, None);
    assert!(
        !cancelled_while_waiting,
        "idle watchdog cancelled the command owner: {error:?}"
    );
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(provider.0.load(Ordering::SeqCst), 3);
    harness.assert_terminal(&child, "completed").await;
}
