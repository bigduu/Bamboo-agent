//! End-to-end: spawn a real actor *subprocess*, discover it via the file fabric, connect over a
//! real WebSocket, assign a task, and stream events + terminal back. No mocks, no in-process cheat.

use std::path::Path;
use std::time::Duration;

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
            reasoning_effort: None,
        }))
        .await
        .unwrap();

    // 3. collect streamed events until the terminal frame
    let mut events = Vec::new();
    let mut terminal = None;
    while let Some(frame) = client.next_frame().await.unwrap() {
        match frame {
            ChildFrame::Event { event } => events.push(event),
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
