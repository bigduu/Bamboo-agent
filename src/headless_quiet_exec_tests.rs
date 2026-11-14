use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::{AgentEvent, ToolSchema};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};

use super::*;

const BASH_CALL: &str = "foreground-command";
const OUTPUT: &str = "quiet command completed";
const CLI_TIMEOUT_SECS: u64 = 8;

struct QuietExecProvider {
    gate: PathBuf,
    calls: AtomicUsize,
}

fn tool_result(messages: &[Message], id: &str) -> String {
    let message = messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(id))
        .expect("the previous tool call must have a result");
    message.content.clone()
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
        assert!(tools.iter().any(|tool| tool.function.name == "bash"));
        assert!(!tools
            .iter()
            .any(|tool| matches!(tool.function.name.as_str(), "exec_command" | "write_stdin")));
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        let chunk = match round {
            0 => {
                let gate = self.gate.to_string_lossy().replace('\'', "'\\''");
                let command = format!(
                    "n=0; while [ ! -f '{gate}' ] && [ \"$n\" -lt 800 ]; do \
                     sleep 0.05; n=$((n+1)); done; [ -f '{gate}' ] || exit 90; \
                     printf '%s' '{OUTPUT}'; exit 7"
                );
                call(BASH_CALL, "bash", json!({"command": command}))
            }
            1 => {
                let result = tool_result(messages, BASH_CALL);
                assert!(result.starts_with(OUTPUT), "{result}");
                assert!(result.contains("Command exited with code 7"), "{result}");
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
        loop {
            let event = tokio::select! {
                result = &mut run => panic!("headless exited before waiting: {result:?}"),
                event = events.recv() => event.unwrap(),
            };
            match event {
                AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == BASH_CALL => break,
                _ => {}
            }
        }
        tokio::select! {
            result = &mut run => panic!("headless exited with a live command: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(750)) => {},
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        if !should_time_out {
            tokio::fs::write(&provider.gate, b"release").await.unwrap();
        }
        run.await
    })
    .await
    .expect("headless run should finish");
    if should_time_out {
        assert!(result.unwrap_err().contains("cancelled"));
        assert!(started.elapsed() >= Duration::from_secs(CLI_TIMEOUT_SECS));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    } else {
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn headless_foreground_bash_waits_and_returns_the_real_exit() {
    run_quiet_exec(false).await;
}

#[tokio::test]
async fn headless_foreground_bash_respects_cli_timeout() {
    run_quiet_exec(true).await;
}
