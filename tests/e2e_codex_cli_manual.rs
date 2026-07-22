//! Manual e2e against the real Codex CLI. It is ignored in normal CI.
//!
//! Run with:
//! `cargo test --test e2e_codex_cli_manual -- --ignored --nocapture`

use std::process::Command;
use std::time::Duration;

use bamboo_agent::codex_cli_executor::{
    resolve_codex_permission_config, CodexAuthConfig, CodexExecutor,
};
use bamboo_subagent::executor::{ChildExecutor, EventSink, SteerInbox};
use bamboo_subagent::proto::{RunSpec, TerminalStatus};
use serde_json::json;
use tokio_util::sync::CancellationToken;

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
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
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
    while let Ok(event) = rx.try_recv() {
        println!("EVENT: {event}");
        if event["executor"] == "codex" {
            bootstrap = Some(event);
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
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
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
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
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
                reasoning_effort: None,
                permission_policy: None,
                // Deliberately ship only the current message. This keeps the
                // activation discriminant non-empty but gives fallback
                // rehydration no copy of NONCE, so only native resume can pass.
                messages: vec![json!({"role": "user", "content": current_task})],
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
