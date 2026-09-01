//! Manual e2e against the real Codex CLI. It is ignored in normal CI.
//!
//! Run with:
//! `cargo test --test e2e_codex_cli_manual -- --ignored --nocapture`

use std::process::Command;
use std::time::Duration;

use bamboo_agent::codex_cli_executor::{
    resolve_codex_permission_config, CodexAuthConfig, CodexExecutor,
};
use bamboo_subagent::codex_discovery::discover_codex_cli;
use bamboo_subagent::executor::{ChildExecutor, EventSink, SteerInbox};
use bamboo_subagent::proto::{RunSpec, TerminalStatus};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires an installed Codex CLI >= 0.144 on PATH"]
async fn real_codex_discovery_reports_path_version_and_actionable_missing_error() {
    let discovered = discover_codex_cli(None)
        .await
        .expect("installed Codex passes the shared discovery preflight");
    println!(
        "CODEX_DISCOVERY path={} version={}",
        discovered.path, discovered.version
    );
    assert!(!discovered.path.is_empty());
    assert!(discovered.version.contains("codex-cli"));

    let missing = discover_codex_cli(Some("/definitely/missing/codex"))
        .await
        .expect_err("missing override must be rejected");
    assert!(missing.contains("npm i -g @openai/codex"));
    assert!(missing.contains("codex_binary"));
}

#[tokio::test]
#[ignore = "requires a logged-in Codex CLI >= 0.144 on PATH"]
async fn real_codex_completes_trivial_turn_and_reports_bootstrap_metadata() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(workspace.path())
        .status()
        .expect("git is installed");
    assert!(status.success());

    let executor = CodexExecutor::new(
        None,
        None,
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        Vec::new(),
        CodexAuthConfig::inherit(),
        resolve_codex_permission_config(
            Some("read-only"),
            Some("never"),
            false,
            false,
            Some("read-only".to_string()),
            false,
            false,
        )
        .unwrap(),
    )
    .await
    .expect("Codex preflight succeeds");
    let (sink, mut rx) = EventSink::channel();
    let outcome = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: "Reply with exactly PONG and nothing else.".to_string(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            },
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("real Codex turn timed out");

    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert_eq!(outcome.result.as_deref().map(str::trim), Some("PONG"));
    let mut bootstrap = None;
    let mut usage = None;
    while let Ok(event) = rx.try_recv() {
        println!("EVENT: {event}");
        if event["executor"] == "codex" {
            bootstrap = Some(event);
        } else if event["type"] == "complete" {
            usage = Some(event["usage"].clone());
        }
    }
    let bootstrap = bootstrap.expect("thread.started emitted bootstrap metadata");
    assert!(bootstrap["binary"]
        .as_str()
        .is_some_and(|path| !path.is_empty()));
    assert!(bootstrap["version"]
        .as_str()
        .is_some_and(|version| version.contains("codex-cli")));
    assert_eq!(bootstrap["sandbox"], "read-only");
    assert_eq!(bootstrap["approval_policy"], "never");
    assert_eq!(bootstrap["policy_invocation"], "explicit");
    assert_eq!(bootstrap["auth_mode"], "inherit");
    assert_eq!(bootstrap["codex_home_mode"], "inherit");
    assert_eq!(bootstrap["forward_env"], json!([]));
    assert!(usage
        .as_ref()
        .and_then(|value| value["total_tokens"].as_u64())
        .is_some_and(|total| total > 0));
}

#[tokio::test]
#[ignore = "requires a logged-in Codex CLI >= 0.144 on PATH"]
async fn real_workspace_write_denies_outside_write_and_emits_tool_error() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(workspace.path())
        .status()
        .expect("git is installed");
    assert!(status.success());

    let outside = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .expect("HOME is set")
        .join(format!(
            ".bamboo-codex-sandbox-e2e-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let _ = std::fs::remove_file(&outside);
    let inside = workspace.path().join("inside-marker.txt");
    let command = format!(
        "printf inside > ./inside-marker.txt && printf outside > {}",
        outside.display()
    );

    let executor = CodexExecutor::new(
        None,
        None,
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        Vec::new(),
        CodexAuthConfig::inherit(),
        resolve_codex_permission_config(
            Some("workspace-write"),
            Some("never"),
            false,
            false,
            None,
            false,
            false,
        )
        .unwrap(),
    )
    .await
    .expect("Codex preflight succeeds");
    let (sink, mut receiver) = EventSink::channel();
    let outcome = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: format!(
                    "Run this exact shell command now: /bin/sh -c '{command}'\nThen report the sandbox failure, including its exit status and exact stderr."
                ),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            },
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("real Codex sandbox turn timed out");

    let outside_written = outside.exists();
    if outside_written {
        let _ = std::fs::remove_file(&outside);
    }
    assert_eq!(outcome.status, TerminalStatus::Completed, "{outcome:?}");
    assert!(inside.exists(), "workspace write did not succeed");
    assert!(
        !outside_written,
        "sandbox allowed an outside-workspace write"
    );

    let events = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
    assert!(
        events.iter().any(|event| event["type"] == "tool_error"),
        "outside denial was not surfaced as a tool error: {events:?}"
    );
}

#[tokio::test]
#[ignore = "requires a logged-in Codex CLI >= 0.144 on PATH"]
async fn real_codex_second_activation_resumes_and_recalls_native_context() {
    const NONCE: &str = "BAMBOO_RESUME_572_NONCE_9F3A";

    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(workspace.path())
        .status()
        .expect("git is installed");
    assert!(status.success());

    let executor = CodexExecutor::new(
        None,
        None,
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        Vec::new(),
        CodexAuthConfig::inherit(),
        resolve_codex_permission_config(
            Some("read-only"),
            Some("never"),
            false,
            false,
            Some("read-only".to_string()),
            false,
            false,
        )
        .unwrap(),
    )
    .await
    .expect("Codex preflight succeeds");

    let (sink, _events) = EventSink::channel();
    let first = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: format!(
                    "Remember this nonce for the next turn: {NONCE}. Reply with exactly STORED."
                ),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            },
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("first real Codex turn timed out");
    assert_eq!(first.status, TerminalStatus::Completed, "{first:?}");
    assert_eq!(first.result.as_deref().map(str::trim), Some("STORED"));

    let state_path = state.path().join("codex-session.json");
    let first_state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert!(first_state["thread_id"]
        .as_str()
        .is_some_and(|thread_id| !thread_id.is_empty()));

    let current_task = "What exact nonce did I ask you to remember? Reply with that nonce only.";
    let (sink, mut events) = EventSink::channel();
    let second = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: current_task.to_string(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                // Deliberately ship only the current message. This keeps the
                // activation discriminant non-empty but gives fallback
                // rehydration no copy of NONCE, so only native resume can pass.
                messages: vec![json!({"role": "user", "content": current_task})],
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            },
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("resumed real Codex turn timed out");
    assert_eq!(second.status, TerminalStatus::Completed, "{second:?}");
    assert_eq!(second.result.as_deref().map(str::trim), Some(NONCE));

    let emitted = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    assert!(
        !emitted
            .iter()
            .any(|event| event["phase"] == "resume_fallback"),
        "real resume unexpectedly used fallback: {emitted:?}"
    );
    let second_state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert!(second_state["thread_id"]
        .as_str()
        .is_some_and(|thread_id| !thread_id.is_empty()));
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires a logged-in Codex CLI >= 0.144 on PATH"]
async fn real_cancellation_kills_the_process_group_and_the_session_can_resume() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(workspace.path())
        .status()
        .expect("git is installed");
    assert!(status.success());

    let executor = CodexExecutor::new(
        None,
        None,
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        Vec::new(),
        CodexAuthConfig::inherit(),
        resolve_codex_permission_config(
            Some("workspace-write"),
            Some("never"),
            false,
            false,
            None,
            false,
            false,
        )
        .unwrap(),
    )
    .await
    .expect("Codex preflight succeeds");

    let pid_file = workspace.path().join("cancel-child.pid");
    let cancel = CancellationToken::new();
    let cancel_for_run = cancel.clone();
    let (sink, _events) = EventSink::channel();
    let first = executor.run(
        RunSpec {
            assignment: "Run this exact command now and wait for it to finish: /bin/sh -c 'echo $$ > ./cancel-child.pid; sleep 120'".to_string(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        },
        sink,
        SteerInbox::disconnected(),
        cancel_for_run,
    );
    tokio::pin!(first);

    let shell_pid = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::select! {
            pid = async {
                loop {
                    if let Ok(raw) = tokio::fs::read_to_string(&pid_file).await {
                        if let Ok(pid) = raw.trim().parse::<i32>() {
                            break pid;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            } => pid,
            outcome = &mut first => {
                panic!("Codex ended before starting the long-running descendant: {outcome:?}")
            }
        }
    })
    .await
    .expect("Codex did not start the long-running descendant in time");
    cancel.cancel();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), &mut first)
        .await
        .expect("cancelled Codex process did not exit in time");
    assert_eq!(cancelled.status, TerminalStatus::Cancelled, "{cancelled:?}");

    let descendant_gone = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // SAFETY: signal 0 performs an existence/permission probe and does
            // not deliver a signal or dereference memory.
            if unsafe { libc::kill(shell_pid, 0) } != 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        descendant_gone.is_ok(),
        "cancelled Codex descendant process {shell_pid} is still alive"
    );

    let assignment = "Reply with exactly RECOVERED and nothing else.";
    let (sink, _events) = EventSink::channel();
    let resumed = tokio::time::timeout(
        Duration::from_secs(180),
        executor.run(
            RunSpec {
                assignment: assignment.to_string(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: vec![json!({"role": "user", "content": assignment})],
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            },
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("Codex did not resume after cancellation");
    assert_eq!(resumed.status, TerminalStatus::Completed, "{resumed:?}");
    assert_eq!(resumed.result.as_deref().map(str::trim), Some("RECOVERED"));
}
