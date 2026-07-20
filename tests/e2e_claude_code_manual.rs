//! MANUAL e2e against the REAL `claude` CLI — never run in CI (all `#[ignore]`).
//! Run: cargo test --test e2e_claude_code_manual -- --ignored --nocapture --test-threads=1
//! Requires a logged-in Claude Code install on PATH.
//!
//! ## Findings baked into this file (issue #443)
//!
//! - **Default mode is `auto`, not `default`.** Against claude 2.1.207, the
//!   `system.init` frame reports `permissionMode: "auto"` when no
//!   `--permission-mode` flag is passed at all — the CLI self-approves every
//!   tool and never emits a `can_use_tool` ask. `ClaudeCodeExecutor::build_command`
//!   now always passes an EXPLICIT mode (the configured value, or `default`)
//!   so `bypassPermissions`/`acceptEdits` are opt-in, not the silent norm.
//! - **Sandbox-safe-echo caveat.** Even in `default` mode, the CLI's own
//!   sandbox auto-runs plain read-only commands (e.g. `echo`) without asking
//!   — a test exercising the approval relay MUST use a command with a side
//!   effect (a file write) to actually force a permission ask; see
//!   `real_claude_approval_relay_via_host_bridge` below.
//! - **Billing is subscription, not API-key.** `apiKeySource: "none"` was
//!   observed with no env key forwarded — confirms the CLI bills the logged-in
//!   subscription by default. `ClaudeCodeExecutor::build_command` now runs the
//!   child under `env_clear()` + a fixed allowlist (HOME/PATH/SHELL/TERM/LANG/
//!   LC_*/TMPDIR/USER/LOGNAME); forwarding `ANTHROPIC_API_KEY` via
//!   `forward_env` is an explicit opt-in that flips billing to the API key.

use std::time::Duration;

use bamboo_agent::claude_code_executor::ClaudeCodeExecutor;
use bamboo_subagent::executor::{ChildExecutor, EventSink, HostBridge, SteerInbox};
use bamboo_subagent::proto::{RunSpec, TerminalStatus};
use tokio_util::sync::CancellationToken;

fn spec(assignment: &str, messages: Vec<serde_json::Value>) -> RunSpec {
    RunSpec {
        assignment: assignment.to_string(),
        reasoning_effort: None,
        permission_policy: None,
        messages,
    }
}

async fn drain_events(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut all = Vec::new();
    while let Some(ev) = rx.recv().await {
        println!("EVENT: {}", serde_json::to_string(&ev).unwrap());
        all.push(ev);
    }
    all
}

#[tokio::test]
#[ignore]
async fn real_claude_fresh_then_resume() {
    let ws = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let exec = ClaudeCodeExecutor::new(
        None,
        None,
        Some("bypassPermissions".to_string()),
        Some(ws.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        // Default isolation (issue #443): the child does NOT load this
        // machine's ~/.claude MCP servers/skills/settings, and forwards no
        // extra env — matches production defaults.
        false,
        Vec::new(),
    );

    // Turn 1: fresh session.
    let (sink, rx) = EventSink::channel();
    let events = tokio::spawn(drain_events(rx));
    let outcome = tokio::time::timeout(
        Duration::from_secs(180),
        exec.run(
            spec("Reply with exactly the word PONG and nothing else.", vec![]),
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("turn 1 timed out");
    println!("OUTCOME1: {outcome:?}");
    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert!(outcome.result.as_deref().unwrap_or("").contains("PONG"));
    let _ = events.await;

    let state_file = state.path().join("claude-code-session.json");
    let raw = std::fs::read_to_string(&state_file).expect("state file written");
    println!("STATE: {raw}");

    // Turn 2: reactivation (messages non-empty) → must --resume and remember.
    let (sink, rx) = EventSink::channel();
    let events = tokio::spawn(drain_events(rx));
    let history = vec![serde_json::json!({
        "role": "user",
        "content": "Reply with exactly the word PONG and nothing else."
    })];
    let outcome = tokio::time::timeout(
        Duration::from_secs(180),
        exec.run(
            spec(
                "Earlier in this same conversation I asked you to reply with one specific \
                 uppercase word. Answer with just that word again.",
                history,
            ),
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("turn 2 timed out");
    println!("OUTCOME2: {outcome:?}");
    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert!(
        outcome.result.as_deref().unwrap_or("").contains("PONG"),
        "resume did not carry context: {:?}",
        outcome.result
    );
    let _ = events.await;
}

#[tokio::test]
#[ignore]
async fn real_claude_approval_relay_via_host_bridge() {
    let ws = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    // EXPLICIT default mode: claude 2.1.207's headless stream-json default is
    // "auto" (auto-approve, never asks) — discovered in this e2e; only an
    // explicit --permission-mode default makes Bash gated and relayed. (As
    // of #443 the executor always passes an explicit mode anyway, so `None`
    // here would now behave identically — kept explicit for clarity.)
    let exec = ClaudeCodeExecutor::new(
        None,
        None,
        Some("default".to_string()),
        Some(ws.path().to_string_lossy().into_owned()),
        Some(state.path().to_path_buf()),
        false,
        Vec::new(),
    );

    let (bridge, mut req_rx) = HostBridge::channel();
    let approvals = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(req) = req_rx.recv().await {
            println!(
                "APPROVAL_REQ: {}",
                serde_json::to_string(&req.body).unwrap()
            );
            seen.push(req.body.clone());
            let _ = req.reply.send(serde_json::json!({"approved": true}));
        }
        seen
    });

    let (sink, rx) = EventSink::channel();
    let sink = sink.with_host_bridge(bridge);
    let events = tokio::spawn(drain_events(rx));
    let outcome = tokio::time::timeout(
        Duration::from_secs(240),
        exec.run(
            spec(
                "Use the Bash tool to run exactly: touch approval-marker.txt && echo \
                 wrote-approval-marker — then report the command's output back verbatim. \
                 (A plain echo is auto-sandboxed as safe by the CLI; the file write is \
                 what forces a permission ask.)",
                vec![],
            ),
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("approval turn timed out");
    println!("OUTCOME: {outcome:?}");
    let _ = events.await;
    let seen = approvals.await.unwrap();
    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert!(
        outcome
            .result
            .as_deref()
            .unwrap_or("")
            .contains("wrote-approval-marker"),
        "bash output missing: {:?}",
        outcome.result
    );
    assert!(
        !seen.is_empty(),
        "no approval request ever reached the host bridge"
    );
}
