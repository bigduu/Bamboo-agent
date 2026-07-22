//! Manual e2e against the real Codex CLI. It is ignored in normal CI.
//!
//! Run with:
//! `cargo test --test e2e_codex_cli_manual -- --ignored --nocapture`

use std::process::Command;
use std::time::Duration;

use bamboo_agent::codex_cli_executor::CodexExecutor;
use bamboo_subagent::executor::{ChildExecutor, EventSink, SteerInbox};
use bamboo_subagent::proto::{RunSpec, TerminalStatus};
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
        Some("read-only".to_string()),
        Some(workspace.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        false,
        Vec::new(),
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
}
