//! End-to-end: spawn a real actor *subprocess*, discover it via the file fabric, connect over a
//! real WebSocket, assign a task, and stream events + terminal back. No mocks, no in-process cheat.

use std::path::Path;
use std::time::Duration;

use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec, ProvisionSpec};
use bamboo_subagent::transport::ChildClient;
use tempfile::TempDir;

#[tokio::test]
async fn spawn_discover_run_stream_terminal() {
    let worker = Path::new(env!("CARGO_BIN_EXE_subagent-demo-worker"));
    let dir = TempDir::new().unwrap();
    let fabric = dir.path().join("agents");

    // 1. spawn subprocess, provision it over stdin, wait for Tier-1 self-registration
    let spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "c1".into(),
            parent_id: Some("p1".into()),
            project_key: None,
            role: "demo".into(),
            depth: 0,
        },
        ExecutorSpec::Echo,
        fabric.to_string_lossy().into_owned(),
    );
    let spawned = spawn_worker(worker, &[], &spec, Duration::from_secs(15))
        .await
        .expect("worker should spawn and register");
    assert_eq!(spawned.record.agent_id, "c1");
    assert!(spawned.record.endpoint.starts_with("ws://127.0.0.1:"));
    assert_ne!(spawned.pid(), None);

    // 2. connect over WebSocket to the discovered endpoint, assign a task
    let mut client = ChildClient::connect(&spawned.record.endpoint)
        .await
        .expect("connect to child");
    client
        .send(ParentFrame::Run(RunSpec {
            assignment: "hello world".into(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .unwrap();

    // 3. collect streamed events until the terminal frame
    let mut events = Vec::new();
    let mut terminal = None;
    while let Some(frame) = client.next_frame().await.unwrap() {
        match frame {
            ChildFrame::Event { event } => events.push(event),
            ChildFrame::EventBatch { batch } => events.extend(batch.events),
            ChildFrame::ApprovalRequest { .. } => {}
            ChildFrame::SessionMessageAdmitted { confirmation } => {
                panic!("echo run emitted unexpected SessionInbox confirmation: {confirmation:?}")
            }
            ChildFrame::Terminal { status, result, .. } => {
                terminal = Some((status, result));
                break;
            }
        }
    }

    let (status, result) = terminal.expect("should receive a terminal frame");
    assert_eq!(status, TerminalStatus::Completed);
    assert_eq!(result.as_deref(), Some("echo: hello world"));
    // streamed one token per word + a complete event
    assert!(events.iter().any(|e| e["content"] == "hello "));
    assert!(events.iter().any(|e| e["content"] == "world "));
    assert!(events.iter().any(|e| e["type"] == "complete"));

    let _ = client.close().await;
    spawned.kill().await;
}

/// Reuse path: ONE reusable subprocess serves two sequential assignments. The
/// parent reconnects for the second run (as the pool does) and the worker — same
/// process, still registered — handles it. This is the process-collapse that
/// makes N sibling sub-agents cost a few processes instead of N.
#[tokio::test]
async fn reusable_worker_serves_two_sequential_assignments_same_process() {
    let worker = Path::new(env!("CARGO_BIN_EXE_subagent-demo-worker"));
    let dir = TempDir::new().unwrap();
    let fabric = dir.path().join("agents");

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "warm".into(),
            parent_id: Some("p1".into()),
            project_key: None,
            role: "demo".into(),
            depth: 0,
        },
        ExecutorSpec::Echo,
        fabric.to_string_lossy().into_owned(),
    );
    spec.reusable = true;
    spec.limits.idle_timeout_secs = Some(30);

    let spawned = spawn_worker(worker, &[], &spec, Duration::from_secs(15))
        .await
        .expect("reusable worker should spawn and register");
    let pid = spawned.pid().expect("worker pid");
    let endpoint = spawned.record.endpoint.clone();

    // Helper: connect, assign, drain to terminal, close (mirrors a pool checkout).
    async fn run_assignment(endpoint: &str, assignment: &str) -> Option<String> {
        let mut client = ChildClient::connect(endpoint).await.expect("connect");
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: assignment.into(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            }))
            .await
            .unwrap();
        let mut result = None;
        while let Some(frame) = client.next_frame().await.unwrap() {
            if let ChildFrame::Terminal {
                status, result: r, ..
            } = frame
            {
                assert_eq!(status, TerminalStatus::Completed);
                result = r;
                break;
            }
        }
        let _ = client.close().await;
        result
    }

    // First assignment.
    assert_eq!(
        run_assignment(&endpoint, "first").await.as_deref(),
        Some("echo: first")
    );

    // The worker is still alive and registered (renewing its lease) between runs.
    assert!(
        Fabric::at(&fabric).resolve("warm").await.unwrap().is_some(),
        "reusable worker should remain registered between runs"
    );

    // Second assignment on the SAME process/endpoint — the reuse.
    assert_eq!(
        run_assignment(&endpoint, "second").await.as_deref(),
        Some("echo: second")
    );

    // Same PID served both: no second process was spawned.
    assert_eq!(spawned.pid(), Some(pid));

    spawned.kill().await;
}
