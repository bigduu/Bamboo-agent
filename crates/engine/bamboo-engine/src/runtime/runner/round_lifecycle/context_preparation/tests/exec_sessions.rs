#![allow(clippy::await_holding_lock)]

use super::super::super::stream_execution::{
    build_exec_session_continuation_context_block, execute_llm_stream, project_request_usage,
    LlmStreamFrame,
};
use super::super::maybe_apply_summary_context_compression_with_budget;
use super::*;
use bamboo_agent_core::tools::{Tool, ToolCtx, ToolOutcome};
use bamboo_agent_core::{BashCompletionInfo, BashCompletionSink};
use bamboo_tools::tools::{bash_runtime, ExecCommandTool, WriteStdinTool};
use serde_json::{json, Value};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

struct ReleaseGate(PathBuf);

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"release");
    }
}

#[derive(Default)]
struct CompletionRecorder(AtomicUsize);

impl BashCompletionSink for CompletionRecorder {
    fn on_bash_completed(&self, _: BashCompletionInfo) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn tool_context(owner: &str, completion: Arc<CompletionRecorder>) -> ToolCtx {
    let mut ctx = ToolCtx::none("exec-continuity-test");
    ctx.session_id = Some(Arc::from(owner));
    ctx.bash_completion_sink = Some(completion);
    ctx
}

fn tool_payload(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("pull sessions use ordinary completed tool requests");
    };
    assert!(result.success);
    serde_json::from_str(&result.result).unwrap()
}

async fn start_gated_shell(
    owner: &str,
    gate: &std::path::Path,
    completion: Arc<CompletionRecorder>,
) -> (String, Value, Value) {
    let quoted_gate = gate.to_string_lossy().replace('\'', "'\\''");
    let args = json!({
        "cmd": format!(
            "printf '%s' '{}'; while [ ! -f '{quoted_gate}' ]; do sleep 0.01; done; printf 'terminal-output'; exit 7",
            "PRIVATE_SHELL_OUTPUT ".repeat(1_000),
        ),
        "yield_time_ms": 250,
        "max_output_tokens": 8192,
    });
    let mut result = tool_payload(
        ExecCommandTool::new()
            .invoke(args.clone(), tool_context(owner, completion))
            .await
            .unwrap(),
    );
    assert_eq!(result["status"], "running");
    let handle = result["session_id"].as_str().unwrap().to_owned();
    // Keep the current model slice's JSON feedback shape in the archived fixture.
    result["hook_feedback"] = json!(["PRIVATE_POST_TOOL_FEEDBACK"]);
    (handle, args, result)
}

fn shell_schemas() -> Vec<ToolSchema> {
    vec![
        ExecCommandTool::new().to_schema(),
        WriteStdinTool::new().to_schema(),
    ]
}

fn shell_history(owner: &str, args: &Value, result: &Value) -> Session {
    let mut session = Session::new(owner, "test-model");
    session.token_budget = Some(TokenBudget::with_safety_margin(
        24_000,
        128,
        BudgetStrategy::default(),
        0,
    ));
    session.add_message(Message::system("system"));
    session.add_message(Message::user(
        "Run this command and retrieve its terminal outcome.",
    ));
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "start-exec".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "exec_command".into(),
                arguments: args.to_string(),
            },
        }]),
    ));
    session.add_message(Message::tool_result("start-exec", result.to_string()));
    session
}

struct PullFromContextProvider {
    gate: PathBuf,
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
}

#[async_trait::async_trait]
impl LLMProvider for PullFromContextProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == "write_stdin"));
        assert!(messages.iter().all(|message| message.role != Role::Tool));
        let snapshot = messages
            .iter()
            .find(|message| {
                message.metadata.as_ref().is_some_and(|metadata| {
                    metadata["bamboo_context_block"]["title"]
                        == "Shell sessions awaiting a terminal result"
                })
            })
            .expect("provider receives the live continuation snapshot after archival/trimming");
        let ids: Vec<String> = serde_json::from_str(
            snapshot
                .content
                .split("Session IDs: ")
                .nth(1)
                .unwrap()
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ids.len(), 1);
        assert!(!snapshot.content.contains("PRIVATE_SHELL_OUTPUT"));
        assert!(!snapshot.content.contains("PRIVATE_POST_TOOL_FEEDBACK"));
        self.requests.lock().unwrap().push(messages.to_vec());
        tokio::fs::write(&self.gate, b"release").await.unwrap();
        Ok(Box::pin(stream::iter(vec![
            Ok::<_, LLMError>(LLMChunk::ToolCalls(vec![ToolCall {
                id: "explicit-final-poll".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "write_stdin".into(),
                    arguments: json!({"session_id":ids[0],"chars":"","yield_time_ms":30_000})
                        .to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn dispatch_and_pull_from_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    schemas: &[ToolSchema],
    gate: &std::path::Path,
    handle: &str,
    completion: Arc<CompletionRecorder>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let llm: Arc<dyn LLMProvider> = Arc::new(PullFromContextProvider {
        gate: gate.to_owned(),
        requests: requests.clone(),
    });
    let owner = session.id.clone();
    let prepared =
        prepare_round_context(session, config, "test-model", &owner, schemas, &llm, None)
            .await
            .unwrap();
    let projected = project_request_usage(
        session,
        &prepared.prepared_context,
        config,
        schemas,
        "test-model",
        &llm,
    )
    .await
    .unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let block_tokens = TiktokenTokenCounter::default().count_message(
        &build_exec_session_continuation_context_block(session, schemas)
            .unwrap()
            .render_runtime_context_message(),
    );
    let mut rejected = session.clone();
    let error = execute_llm_stream(
        &mut rejected,
        config,
        &llm,
        &prepared.prepared_context,
        schemas,
        &LlmStreamFrame {
            event_tx: &tx,
            cancel_token: &CancellationToken::new(),
            session_id: &owner,
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: projected.input_tokens.saturating_sub(block_tokens) + 128,
            max_output_tokens: 128,
            observation_progress_hint: None,
            prompt_memory_exposure: None,
        },
    )
    .await
    .err()
    .expect("the continuation snapshot cannot bypass the final input cap");
    assert!(error
        .to_string()
        .contains("final known provider-visible request exceeds"));
    assert!(requests.lock().unwrap().is_empty());
    // Exactly fit the projected input plus output reservation. The dynamic
    // snapshot must participate in both projection and the final dispatch gate.
    let (output, _, _) = execute_llm_stream(
        session,
        config,
        &llm,
        &prepared.prepared_context,
        schemas,
        &LlmStreamFrame {
            event_tx: &tx,
            cancel_token: &CancellationToken::new(),
            session_id: &owner,
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: projected.input_tokens + 128,
            max_output_tokens: 128,
            observation_progress_hint: None,
            prompt_memory_exposure: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert_eq!(output.tool_calls.len(), 1);
    let call = &output.tool_calls[0];
    assert_eq!(call.function.name, "write_stdin");
    let args: Value = serde_json::from_str(&call.function.arguments).unwrap();
    assert_eq!(args["session_id"], handle);
    let terminal = tool_payload(
        WriteStdinTool::new()
            .invoke(args, tool_context(&owner, completion.clone()))
            .await
            .unwrap(),
    );
    assert_eq!(terminal["status"], "completed");
    assert_eq!(terminal["exit_code"], 7);
    assert!(terminal["output"]
        .as_str()
        .unwrap()
        .contains("terminal-output"));
    assert!(bash_runtime::uncollected_polled_shells_for_session(&owner).is_empty());
    assert!(build_exec_session_continuation_context_block(session, schemas).is_none());
    assert!(
        session
            .model_context_state
            .as_ref()
            .unwrap()
            .events
            .iter()
            .all(|event| { !event.rendered_text.contains(handle) }),
        "the live handle inventory is never copied into the durable context ledger"
    );
    assert_eq!(completion.0.load(Ordering::SeqCst), 0);
    let (after_llm, after_requests) = prompt_capture_llm();
    let after = prepare_round_context(
        session,
        config,
        "test-model",
        &owner,
        schemas,
        &after_llm,
        None,
    )
    .await
    .unwrap();
    let projected = project_request_usage(
        session,
        &after.prepared_context,
        config,
        schemas,
        "test-model",
        &after_llm,
    )
    .await
    .unwrap();
    execute_llm_stream(
        session,
        config,
        &after_llm,
        &after.prepared_context,
        schemas,
        &LlmStreamFrame {
            event_tx: &tx,
            cancel_token: &CancellationToken::new(),
            session_id: &owner,
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: projected.input_tokens + 128,
            max_output_tokens: 128,
            observation_progress_hint: None,
            prompt_memory_exposure: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(after_requests.lock().unwrap().len(), 1);
    assert!(
        after_requests.lock().unwrap()[0]
            .iter()
            .all(|message| !message.content.contains(handle)),
        "a consumed handle is absent from the actual next provider request"
    );
}

#[tokio::test]
async fn exec_handles_survive_both_summary_strategies_and_reach_actual_provider() {
    let _env_lock = isolate_prompt_safe_env_cache();
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
    ] {
        for manual in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let gate = dir.path().join("release");
            let _release = ReleaseGate(gate.clone());
            let owner = format!("summary-continuity-{strategy:?}-{manual}");
            let completion = Arc::new(CompletionRecorder::default());
            let (handle, args, result) = start_gated_shell(&owner, &gate, completion.clone()).await;
            let mut session = shell_history(&owner, &args, &result);
            if manual {
                session.force_manual_compression = Some(String::new());
            } else {
                session.token_usage = Some(pressure_usage(21_000, 24_000));
            }
            let schemas = shell_schemas();
            let block_tokens = TiktokenTokenCounter::default().count_message(
                &build_exec_session_continuation_context_block(&session, &schemas)
                    .unwrap()
                    .render_runtime_context_message(),
            );
            let (summary_llm, summary_requests) = prompt_capture_llm();
            let mut config = bounded_compression_config(summary_llm);
            config.context_management.strategy = strategy;
            let budget = session.token_budget.clone().unwrap();
            assert!(maybe_apply_summary_context_compression_with_budget(
                &mut session,
                &config,
                "test-model",
                &owner,
                &schemas,
                &noop_llm(),
                &budget,
                None,
                "continuity-test",
                None,
            )
            .await
            .unwrap());
            let stored_result = session
                .messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some("start-exec"))
                .unwrap();
            assert!(stored_result.compressed);
            assert_eq!(stored_result.content, result.to_string());
            assert_eq!(
                session
                    .compression_events
                    .last()
                    .unwrap()
                    .fixed_prompt_tokens,
                block_tokens
            );
            if strategy == ContextManagementStrategy::MessageSummary {
                let sent = serde_json::to_string(&*summary_requests.lock().unwrap()).unwrap();
                assert!(!sent.contains("PRIVATE_SHELL_OUTPUT"));
                assert!(!sent.contains("PRIVATE_POST_TOOL_FEEDBACK"));
                assert!(!sent.contains(&handle));
            }
            dispatch_and_pull_from_context(
                &mut session,
                &config,
                &schemas,
                &gate,
                &handle,
                completion,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn exec_handles_survive_hard_history_trimming_without_summary() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let owner = "hard-trim-exec-continuity";
    let completion = Arc::new(CompletionRecorder::default());
    let (handle, args, result) = start_gated_shell(owner, &gate, completion.clone()).await;
    let mut session = shell_history(owner, &args, &result);
    session.token_budget.as_mut().unwrap().max_context_tokens = 4_000;
    let config = AgentLoopConfig::default();
    let schemas = shell_schemas();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        owner,
        &schemas,
        &noop_llm(),
        None,
    )
    .await
    .unwrap();
    assert!(prepared.prepared_context.truncation_occurred);
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .all(|message| message.role != Role::Tool));
    assert!(session.compression_events.is_empty());
    dispatch_and_pull_from_context(&mut session, &config, &schemas, &gate, &handle, completion)
        .await;
}

#[tokio::test]
async fn exec_snapshot_retains_unread_terminal_and_respects_owner_and_exact_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("release");
    let _release = ReleaseGate(gate.clone());
    let owner = "unread-terminal-exec-continuity";
    let completion = Arc::new(CompletionRecorder::default());
    let (handle, _, _) = start_gated_shell(owner, &gate, completion.clone()).await;
    let shell = bash_runtime::get_shell(&handle).unwrap();
    tokio::fs::write(&gate, b"release").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while shell.status() == "running" {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        bash_runtime::uncollected_polled_shells_for_session(owner),
        vec![handle.clone()]
    );
    assert!(bash_runtime::uncollected_polled_shells_for_session("another-owner").is_empty());
    let legacy =
        bash_runtime::spawn_background("exit 0", None, None, Some(owner.into()), false, None)
            .await
            .unwrap();
    let unowned = bash_runtime::spawn_background("exit 0", None, None, None, false, None)
        .await
        .unwrap();
    assert_eq!(
        bash_runtime::uncollected_polled_shells_for_session(owner),
        vec![handle.clone()]
    );
    assert!(bash_runtime::uncollected_polled_shells_for_session("").is_empty());
    bash_runtime::remove_shell(&legacy.id);
    bash_runtime::remove_shell(&unowned.id);
    let mut session = Session::new(owner, "test-model");
    let schemas = shell_schemas();
    assert!(
        build_exec_session_continuation_context_block(&session, &schemas)
            .unwrap()
            .content
            .contains(&handle)
    );
    for name in [
        "vendor::write_stdin",
        "default::write_stdin",
        "BashOutput",
        "Read",
    ] {
        let mut foreign = schemas[1].clone();
        foreign.function.name = name.into();
        assert!(
            build_exec_session_continuation_context_block(&session, &[foreign]).is_none(),
            "{name}"
        );
    }
    assert!(build_exec_session_continuation_context_block(&session, &[]).is_none());
    // An explicit load_skill-only request must not inherit continuation guidance
    // from a broader pre-projection schema list.
    let mut required = session.clone();
    required.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.into(),
        "explicit".into(),
    );
    required.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        "[\"review\"]".into(),
    );
    assert!(build_exec_session_continuation_context_block(&required, &schemas).is_none());
    session.set_root_orchestration_only(true).unwrap();
    assert!(build_exec_session_continuation_context_block(&session, &schemas).is_none());
    session.set_root_orchestration_only(false).unwrap();
    let terminal = tool_payload(
        WriteStdinTool::new()
            .invoke(
                json!({"session_id":handle,"chars":"","yield_time_ms":250}),
                tool_context(owner, completion.clone()),
            )
            .await
            .unwrap(),
    );
    assert_eq!(terminal["exit_code"], 7);
    assert!(build_exec_session_continuation_context_block(&session, &schemas).is_none());
    assert_eq!(completion.0.load(Ordering::SeqCst), 0);
}
