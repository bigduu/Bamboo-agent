use super::*;
use bamboo_domain::{SessionMessageEnvelope, SupervisedRootOutcomeResult};

pub(super) const INPUT: &str = "Verify the held background command and report its result.";
const COMMAND: &str = "start-managed-background-exec";
const OUTPUT: &str = "managed-background-exec-finished";
const RELEASE_FILE: &str = "managed-exec-release";
const CRASH_STAGE: &str = "after-native-exec-final";
const MARKER_ENV: &str = "BAMBOO_2662_MARKER";
const TEST_NAME: &str = "app_state::observer_subscriptions::tests::background_exec::subscribed_background_exec_delivers_once_after_owned_host_restart";

struct ReleaseGate(std::path::PathBuf);

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"release");
    }
}

fn callback(message: &Message) -> Option<SessionMessageEnvelope> {
    let marker = message.metadata.as_ref()?.get("session_message")?;
    (marker["body"]["instruction"] == "background_exec_completed")
        .then(|| serde_json::from_value(marker.clone()).unwrap())
}

pub(super) async fn stream(probe: &Probe, messages: &[Message]) -> Result<LLMStream, LLMError> {
    probe.source_calls.fetch_add(1, Ordering::SeqCst);
    assert!(messages
        .iter()
        .any(|message| { message.content.contains("Managed Session Delivery Summary") }));
    let tool = |id: String, name: &str, args: Value| ToolCall {
        id,
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: args.to_string(),
        },
    };
    let chunk = if let Some(completion) = messages.iter().find_map(callback) {
        let bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) = completion.body
        else {
            unreachable!()
        };
        let data = instruction.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["exit_code"], 0);
        let result: Value = serde_json::from_str(data["result"].as_str().unwrap()).unwrap();
        assert_eq!(result["output"], OUTPUT);
        LLMChunk::Token(format!(
            "<!-- bamboo-handoff:v1 -->\n- **Task** Verify held command\n- **Conclusion** {HANDOFF_HEAD}\n- **Evidence** `{HANDOFF_EVIDENCE}`\n- **Risk** {HANDOFF_RISK}\n<!-- /bamboo-handoff:v1 -->"
        ))
    } else if let Some(result) = messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(COMMAND))
    {
        let started: Value = serde_json::from_str(&result.content).unwrap();
        let handle = started["session_id"].as_str().unwrap();
        assert_eq!(started["status"], "running");
        assert!(!messages
            .iter()
            .any(|message| { message.tool_call_id.as_deref() == Some("quiet-background-0") }));
        // Age the actual tool's monotonic observations. The process stays
        // held on the filesystem gate throughout the normal poll pipeline.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(90)).await;
        tokio::time::resume();
        LLMChunk::ToolCalls(
            (0..8)
                .map(|index| {
                    tool(
                        format!("quiet-background-{index}"),
                        "write_stdin",
                        json!({
                            "session_id":handle, "chars":"", "yield_time_ms":250,
                        }),
                    )
                })
                .collect(),
        )
    } else {
        let gate = probe
            .diagnostic_home
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .join(RELEASE_FILE);
        let gate = gate.to_string_lossy().replace('\'', "'\\''");
        let command = format!("while [ ! -f '{gate}' ]; do sleep 0.01; done; printf {OUTPUT}");
        LLMChunk::ToolCalls(vec![tool(
            COMMAND.into(),
            "exec_command",
            json!({
                "cmd":command, "yield_time_ms":250,
            }),
        )])
    };
    Ok(Box::pin(futures::stream::iter([
        Ok(chunk),
        Ok(LLMChunk::Done),
    ])))
}

async fn worker_before_delivery() {
    let f = Fixture::new_with_native_source(true).await;
    let gate = ReleaseGate(f.home.path().join(RELEASE_FILE));
    let mut source = load(&f.state, ROOT_A).await;
    source
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .set_permission_mode(SessionPermissionMode::Bypass);
    f.state.storage.save_session(&source).await.unwrap();
    let target = managed_target(&f.state, &f.reference, ROOT_A).await;
    let admitted = invoke_managed_control(
        &f.state,
        &f.reference,
        "managed-background-exec",
        json!({"action":"followup", "target_session_id":ROOT_A, "target_ref":target,
            "operation_id":"managed-background-exec", "message":INPUT, "notify_on_material_event":true}),
    ).await;
    assert_eq!(admitted["notify_on_material_event"], true);

    // The real quiet poll advances Tokio time; keep this fixture's outer
    // deadline on wall time while waiting for the actual Actor checkpoint.
    let deadline = std::time::Instant::now() + WAIT;
    let (original_run, handle) = loop {
        let source = load(&f.state, ROOT_A).await;
        assert!(source.pending_question.is_none());
        if let Some(fact) = source
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.supervised_root_outcomes.as_ref())
            .and_then(|outcomes| outcomes.facts.last())
            .filter(|fact| fact.result == Some(SupervisedRootOutcomeResult::Suspended))
        {
            let wait = source.messages.last().unwrap().metadata.as_ref().unwrap();
            assert_eq!(wait["runtime_kind"], "background_exec_wait");
            assert_eq!(wait["run_id"], fact.run_id);
            break (
                fact.run_id.clone(),
                wait["exec_session_id"].as_str().unwrap().to_owned(),
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "native background command did not park"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        bamboo_tools::tools::bash_runtime::get_shell(&handle)
            .unwrap()
            .status(),
        "running"
    );
    drive_material_notices(&f, 1).await;
    settle_material_notices(&f).await;

    std::fs::write(&gate.0, b"release").unwrap();
    let source = settled(&f.state, ROOT_A).await;
    let outcomes = source
        .runtime_metadata
        .as_ref()
        .unwrap()
        .supervised_root_outcomes
        .as_ref()
        .unwrap();
    let [initial, terminal] = outcomes.facts.as_slice() else {
        panic!("expected the suspended and callback Runs");
    };
    assert_eq!(initial.run_id, original_run);
    assert_eq!(terminal.resumed_from, Some(initial.sequence));
    assert_eq!(
        terminal.result,
        Some(SupervisedRootOutcomeResult::Completed)
    );
    assert_eq!(terminal.inputs.len(), 1);
    let original = &initial.inputs[0];
    assert_eq!(
        original.receipt_id.as_str(),
        admitted["receipt_id"].as_str().unwrap()
    );
    assert_eq!(
        outcomes.material_inputs(terminal).collect::<Vec<_>>(),
        vec![original]
    );
    let [completion] = source
        .messages
        .iter()
        .filter_map(callback)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    assert_eq!(terminal.inputs[0].receipt_id, completion.id);
    assert!(f
        .state
        .session_inbox
        .was_admitted(ROOT_A, &completion.id)
        .await
        .unwrap());
    let final_message = terminal.verified_final_message(&source).unwrap();
    assert_eq!(f.probe.source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);

    let marker = std::path::PathBuf::from(std::env::var_os(MARKER_ENV).unwrap());
    write_crash_marker(
        &marker,
        &json!({
            "boundary":CRASH_STAGE, "pid":std::process::id(), "home":f.home.path(),
            "reference":f.reference, "terminal_run":terminal.run_id, "final_message":final_message.message_id,
            "callback_id":completion.id, "callback_generation":terminal.inputs[0].generation,
        }),
    );
    std::future::pending::<()>().await;
    drop(f);
}

#[actix_web::test]
async fn subscribed_background_exec_delivers_once_after_owned_host_restart() {
    if std::env::var(CRASH_WORKER_ENV).as_deref() == Ok(CRASH_STAGE) {
        worker_before_delivery().await;
        return;
    }
    let outer = tempfile::tempdir().unwrap();
    let temporary = outer.path().join("tmp");
    std::fs::create_dir(&temporary).unwrap();
    let marker = outer.path().join("exec-final-marker.json");
    let stdout = outer.path().join("exec-final-stdout.log");
    let stderr = outer.path().join("exec-final-stderr.log");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env(CRASH_WORKER_ENV, CRASH_STAGE)
        .env(MARKER_ENV, &marker)
        .env("HOME", outer.path())
        .env("TMPDIR", &temporary)
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("RUST_MIN_STACK", "8388608")
        .current_dir(outer.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .kill_on_drop(true);
    for name in [
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    let signal = kill_owned_at_marker(&mut command, &marker, &stdout, &stderr, CRASH_STAGE).await;
    let home = std::fs::canonicalize(signal["home"].as_str().unwrap()).unwrap();
    assert!(home.starts_with(std::fs::canonicalize(&temporary).unwrap()));
    let probe = Arc::new(Probe::default());
    let state = host(&home, probe.clone()).await;
    let reference = serde_json::from_value(signal["reference"].clone()).unwrap();
    let f = Fixture {
        home: outer,
        state,
        probe,
        reference,
    };

    let source = load(&f.state, ROOT_A).await;
    let completion = source.messages.iter().find_map(callback).unwrap();
    let replay = f
        .state
        .session_inbox
        .deliver_runtime_for_target(&completion, source.created_at)
        .await
        .unwrap();
    assert_eq!(replay.id.as_str(), signal["callback_id"].as_str().unwrap());
    assert_eq!(
        replay.generation,
        signal["callback_generation"].as_u64().unwrap()
    );
    drive_material_notices(&f, 1).await;
    settle_material_notices(&f).await;
    let parent = load(&f.state, &f.reference.session_id).await;
    let terminal_run = signal["terminal_run"].as_str().unwrap();
    let notices: Vec<_> = parent
        .messages
        .iter()
        .filter(|message| {
            message.content.starts_with("# Managed Session handoff")
                && message.content.contains(terminal_run)
        })
        .collect();
    let [notice] = notices.as_slice() else {
        panic!("exactly one terminal handoff after restart");
    };
    assert!(notice.content.contains(HANDOFF_HEAD));
    assert!(notice
        .content
        .contains(signal["final_message"].as_str().unwrap()));
    let receipt = bamboo_domain::SessionMessageId::parse(notice.id.clone()).unwrap();
    assert!(f
        .state
        .session_inbox
        .was_admitted(&f.reference.session_id, &receipt)
        .await
        .unwrap());
    assert_eq!(f.probe.parent_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.probe.source_calls.load(Ordering::SeqCst), 0);
    f.state.shutdown().await;
}
