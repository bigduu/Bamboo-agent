//! Real SDK -> Agent -> BuiltinToolExecutor -> Bash ownership, including the
//! final publication barrier. The controlled provider only chooses the tool;
//! it neither launches nor terminates the process on behalf of the runtime.
use super::*;
use bamboo_agent_core::tools::{FunctionCall, ToolExecutionContext, ToolOutcome};

const LEGACY_BASH_FIXTURE: &str = "legacy_bash_host_fixture";

/// The model catalog hides Bash. This test-only host compatibility adapter
/// preserves coverage of its actual SDK cancellation/publication lifecycle.
/// Only the exposed fixture name changes; the builtin receives the same call
/// arguments and execution context, including its LocalTask resource owner.
struct LegacyBashHostFixture(bamboo_tools::BuiltinToolExecutor);

impl LegacyBashHostFixture {
    fn builtin_call(call: &ToolCall) -> Result<ToolCall, ToolError> {
        if call.function.name != LEGACY_BASH_FIXTURE {
            return Err(ToolError::NotFound(call.function.name.clone()));
        }
        let mut builtin_call = call.clone();
        builtin_call.function.name = "Bash".into();
        Ok(builtin_call)
    }
}

#[async_trait]
impl ToolExecutor for LegacyBashHostFixture {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.execute(&Self::builtin_call(call)?).await
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.0
            .execute_with_context_outcome(&Self::builtin_call(call)?, ctx)
            .await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.0
            .list_tools()
            .into_iter()
            .filter(|schema| schema.function.name == "Bash")
            .map(|mut schema| {
                schema.function.name = LEGACY_BASH_FIXTURE.into();
                schema
            })
            .collect()
    }
}

struct BashCeiling;
#[async_trait]
impl NativeToolCeilingSource for BashCeiling {
    async fn observe(&self, _: &Session) -> Result<Vec<String>, String> {
        Ok(vec![LEGACY_BASH_FIXTURE.into()])
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
        _: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        assert!(tools
            .iter()
            .any(|tool| tool.function.name == LEGACY_BASH_FIXTURE));
        assert!(!tools.iter().any(|tool| tool.function.name == "Bash"));
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: "owned-builtin-bash".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: LEGACY_BASH_FIXTURE.into(),
                    arguments: serde_json::json!({
                        "command": self.command,
                        "workdir": self.workspace,
                        "run_in_background": false,
                        "timeout": 60000,
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
        Arc::new(LegacyBashHostFixture(
            bamboo_tools::BuiltinToolExecutor::new(),
        )),
        Arc::new(BashCeiling),
    )
    .await;
    let child = harness
        .child("owned-bash", "Run the owned Bash command.")
        .await;
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
