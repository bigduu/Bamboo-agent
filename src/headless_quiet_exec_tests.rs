use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::{AgentEvent, ToolSchema};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};

use super::*;

const LAST_POLL: &str = "poll-4";
const OUTPUT: &str = "quiet command completed";
const CLI_TIMEOUT_SECS: u64 = 8;

struct QuietExecProvider {
    gate: PathBuf,
    calls: AtomicUsize,
}

fn tool_result(messages: &[Message], id: &str) -> Value {
    let message = messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(id))
        .expect("the previous tool call must have a result");
    serde_json::from_str(&message.content).unwrap()
}

fn call(id: &str, name: &str, arguments: Value) -> LLMChunk {
    LLMChunk::ToolCalls(vec![ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    }])
}

#[async_trait::async_trait]
impl LLMProvider for QuietExecProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        for name in ["exec_command", "write_stdin"] {
            assert!(tools.iter().any(|tool| tool.function.name == name));
        }
        assert!(!tools.iter().any(|tool| tool.function.name == "bash"));
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        let chunk = match round {
            0 => {
                let gate = self.gate.to_string_lossy().replace('\'', "'\\''");
                let command = format!(
                    "n=0; while [ ! -f '{gate}' ] && [ \"$n\" -lt 800 ]; do \
                     sleep 0.05; n=$((n+1)); done; [ -f '{gate}' ] || exit 90; \
                     printf '%s' '{OUTPUT}'; exit 7"
                );
                call(
                    "start",
                    "exec_command",
                    json!({"cmd": command, "yield_time_ms": 250}),
                )
            }
            1..=4 => {
                let start = tool_result(messages, "start");
                assert_eq!(start["status"], "running");
                if round > 1 {
                    let previous = tool_result(messages, &format!("poll-{}", round - 1));
                    assert_eq!(previous["status"], "running");
                    assert_eq!(previous["output"], "");
                    assert_eq!(previous["polling"]["advisory"].is_string(), round == 4);
                }
                call(
                    &format!("poll-{round}"),
                    "write_stdin",
                    json!({"session_id": start["session_id"], "chars": "", "yield_time_ms": 250}),
                )
            }
            5 => {
                let result = tool_result(messages, LAST_POLL);
                assert_eq!(result["status"], "running");
                let notification = messages
                    .iter()
                    .find(|m| {
                        m.content
                            .starts_with("Runtime notification: background exec command")
                    })
                    .expect("runner must deliver terminal evidence before calling the model");
                assert!(notification.content.contains(r#""exit_code":7"#));
                assert!(notification.content.contains(OUTPUT));
                LLMChunk::Token("Received the terminal result.".into())
            }
            _ => panic!("unexpected provider round {round}"),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn run_quiet_exec(should_time_out: bool) {
    let dir = tempfile::tempdir().unwrap();
    let session_id = format!("headless-quiet-exec-{}", uuid::Uuid::new_v4());
    let provider = Arc::new(QuietExecProvider {
        gate: dir.path().join("release"),
        calls: AtomicUsize::new(0),
    });
    let config = crate::model_entrypoint_routing_tests::config(true, "chat-a");
    let state = AppState::new_with_provider_and_memory_store(
        dir.path().to_path_buf(),
        config.clone(),
        provider.clone(),
        bamboo_memory::memory_store::MemoryStore::new(dir.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = config;
    let registry = ProviderRegistry::new(
        [("chat-a".into(), provider.clone() as Arc<dyn LLMProvider>)].into(),
        "chat-a".into(),
    );
    registry.set_runtime_models([("chat-a".into(), vec!["main-a".into()])].into());
    state.provider_registry.replace_with(registry);

    let mut session = Session::new(&session_id, "main-a");
    bamboo_engine::session_app::provider_model::persist_model_ref(
        &mut session,
        &bamboo_domain::ProviderModelRef::new("chat-a", "main-a"),
    );
    state.storage.save_session(&session).await.unwrap();
    state.session_store.save_session(&session).await.unwrap();
    let sender = state.get_session_event_sender(&session_id).await;
    let mut events = sender.subscribe();
    let started = Instant::now();
    let run = run_with_state(
        HeadlessArgs {
            prompt: "Run the synthetic quiet command.".into(),
            session: Some(session_id.clone()),
            model: None,
            workspace: None,
            project_id: None,
            data_dir: dir.path().to_path_buf(),
            permission_mode: Some("auto".into()),
            stream_json: true,
            reasoning_effort: None,
            skill_mode: None,
            provider: None,
            timeout_secs: should_time_out.then_some(CLI_TIMEOUT_SECS),
        },
        state,
    );
    tokio::pin!(run);
    let fixture_timeout = Duration::from_secs(CLI_TIMEOUT_SECS + 20);
    let result = tokio::time::timeout(fixture_timeout, async {
        let mut handle = None;
        loop {
            let event = tokio::select! {
                result = &mut run => panic!("headless exited before waiting: {result:?}"),
                event = events.recv() => event.unwrap(),
            };
            match event {
                AgentEvent::ToolStart {
                    tool_call_id,
                    arguments,
                    ..
                } if tool_call_id == LAST_POLL => {
                    handle = arguments["session_id"].as_str().map(str::to_owned);
                }
                AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == LAST_POLL => break,
                _ => {}
            }
        }
        tokio::select! {
            result = &mut run => panic!("headless exited with a live command: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(750)) => {},
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
        if !should_time_out {
            tokio::fs::write(&provider.gate, b"release").await.unwrap();
        }
        let result = run.await;
        // The fixture owns the command and reaps it even when the CLI exits early.
        if let Some(handle) = handle {
            if let Some(job) =
                bamboo_tools::tools::exec_command::reserve_background_job(&session_id, &handle)
            {
                tokio::fs::write(&provider.gate, b"release").await.unwrap();
                tokio::time::timeout(Duration::from_secs(3), job.completion())
                    .await
                    .unwrap();
            }
        }
        result
    })
    .await
    .expect("headless run should finish");
    if should_time_out {
        assert!(result.unwrap_err().contains("cancelled"));
        assert!(started.elapsed() >= Duration::from_secs(CLI_TIMEOUT_SECS));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
    } else {
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 6);
    }
}

#[tokio::test]
async fn headless_quiet_exec_waits_for_the_real_exit_after_the_poll_returns() {
    run_quiet_exec(false).await;
}

#[tokio::test]
async fn headless_quiet_exec_wait_respects_cli_timeout() {
    run_quiet_exec(true).await;
}
