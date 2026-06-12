//! End-to-end against the REAL `bamboo` binary: spawn `bamboo subagent-worker` as a
//! subprocess, provision it over stdin (echo executor — no API key needed), discover it
//! via the file fabric, run a task over a real WebSocket, and collect the stream.
//!
//! This proves the production subcommand path (clap → worker run → factory → WS serve →
//! self-register → withdraw) works end-to-end; the BambooRuntime executor swaps in via
//! the same factory with a provisioned credential.

use std::path::Path;
use std::time::Duration;

use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec, ProvisionSpec};
use bamboo_subagent::transport::ChildClient;
use tempfile::TempDir;

#[tokio::test]
async fn real_bamboo_binary_serves_a_subagent_run() {
    let bamboo_bin = Path::new(env!("CARGO_BIN_EXE_bamboo"));
    let dir = TempDir::new().unwrap();
    let fabric = dir.path().join("agents");

    let spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "real-c1".into(),
            parent_id: Some("p1".into()),
            project_key: None,
            role: "smoke".into(),
        },
        ExecutorSpec::Echo,
        fabric.to_string_lossy().into_owned(),
    );

    // Spawn the production binary with the `subagent-worker` subcommand.
    let spawned = spawn_worker(
        bamboo_bin,
        &["subagent-worker".to_string()],
        &spec,
        Duration::from_secs(20),
    )
    .await
    .expect("real bamboo worker should spawn and self-register");
    assert_eq!(spawned.record.agent_id, "real-c1");
    assert_eq!(spawned.record.role, "smoke");

    let mut client = ChildClient::connect(&spawned.record.endpoint)
        .await
        .expect("connect to real worker");
    client
        .send(ParentFrame::Run(RunSpec {
            assignment: "ping pong".into(),
            reasoning_effort: None,
        }))
        .await
        .unwrap();

    let mut saw_token = false;
    let mut terminal = None;
    while let Some(frame) = client.next_frame().await.unwrap() {
        match frame {
            ChildFrame::Event { event } => {
                if event["type"] == "token" {
                    saw_token = true;
                }
            }
            ChildFrame::Terminal { status, result, .. } => {
                terminal = Some((status, result));
                break;
            }
        }
    }

    let (status, result) = terminal.expect("terminal frame from real worker");
    assert_eq!(status, TerminalStatus::Completed);
    assert_eq!(result.as_deref(), Some("echo: ping pong"));
    assert!(saw_token, "should have streamed token events");

    let _ = client.close().await;
    spawned.kill().await;
}
