//! End-to-end against the REAL `bamboo` binary: spawn `bamboo subagent-worker` as a
//! subprocess, provision it over stdin (echo executor — no API key needed), discover it
//! via the file fabric, run a task over a real WebSocket, and collect the stream.
//!
//! This proves the production subcommand path (clap → worker run → factory → WS serve →
//! self-register → withdraw) works end-to-end; the BambooRuntime executor swaps in via
//! the same factory with a provisioned credential.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{BrokerCore, BrokerServer};
#[cfg(unix)]
use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::fleet::{spawn_worker, spawn_worker_on_bus};
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
#[cfg(unix)]
use bamboo_subagent::provision::WorkerOwner;
use bamboo_subagent::provision::{BusEndpoint, ChildIdentity, ExecutorSpec, ProvisionSpec};
use bamboo_subagent::transport::ChildClient;
use tempfile::TempDir;
use tokio::net::TcpListener;

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// Failure-only cleanup for the exact worker PID learned from its Fabric
/// registration. Never targets a process group or an unresolved child handle.
#[cfg(unix)]
struct ExactPidCleanup(Option<u32>);

#[cfg(unix)]
impl Drop for ExactPidCleanup {
    fn drop(&mut self) {
        if let Some(pid) = self.0.take() {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

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
            depth: 0,
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
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
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
            ChildFrame::ApprovalRequest { .. } => {}
            ChildFrame::SessionMessageAdmitted { .. } => {
                panic!("worker must not confirm an empty initial SessionInbox batch")
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

/// A production worker must not survive abrupt loss of the Bamboo process that
/// physically spawned it. The shell deliberately has another command after the
/// worker (`; :`) so it stays the direct parent instead of exec-optimizing the
/// worker into the shell process.
#[cfg(unix)]
#[tokio::test]
async fn real_bamboo_worker_exits_when_its_direct_owner_is_sigkilled() {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    use tokio::process::Command;

    let bamboo_bin = Path::new(env!("CARGO_BIN_EXE_bamboo"));
    let dir = TempDir::new().expect("tempdir");
    let fabric_dir = dir.path().join("agents");
    tokio::fs::create_dir_all(&fabric_dir)
        .await
        .expect("fabric dir");
    let mut shell = Command::new("/bin/sh")
        .arg("-c")
        .arg("\"$1\" subagent-worker; :")
        .arg("owner-shell")
        .arg(bamboo_bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn direct owner shell");
    let shell_pid = shell.id().expect("owner shell pid");

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "owner-loss-e2e".into(),
            parent_id: Some("owner-session".into()),
            project_key: None,
            role: "owner-loss".into(),
            depth: 0,
        },
        ExecutorSpec::Echo,
        fabric_dir.to_string_lossy().into_owned(),
    );
    spec.owner = Some(WorkerOwner {
        process_id: shell_pid,
        instance_id: "owner-loss-e2e-instance".into(),
        process_start_id: None,
        session_id: Some("owner-session".into()),
        worker_spawned_at: chrono::Utc::now(),
    });
    let spec_json = spec.to_json().expect("encode provision spec");
    let mut shell_stdin = shell.stdin.take().expect("owner shell stdin");
    shell_stdin
        .write_all(spec_json.as_bytes())
        .await
        .expect("write provision spec");
    shell_stdin.shutdown().await.expect("close provision stdin");
    drop(shell_stdin);

    let fabric = Fabric::at(&fabric_dir);
    let record = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(record) = fabric
                .resolve("owner-loss-e2e")
                .await
                .expect("resolve worker")
            {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("production worker registers in Fabric");
    let worker_pid = record.pid;
    assert!(worker_pid > 1, "worker must report a killable process PID");
    assert_ne!(worker_pid, shell_pid, "shell must not exec the worker");
    assert_ne!(
        worker_pid,
        std::process::id(),
        "never target the test process"
    );
    assert!(
        shell.try_wait().expect("query owner shell").is_none(),
        "direct owner shell must still be waiting on the worker"
    );
    let mut worker_cleanup = ExactPidCleanup(Some(worker_pid));

    let killed = unsafe { libc::kill(shell_pid as libc::pid_t, libc::SIGKILL) };
    assert_eq!(killed, 0, "SIGKILL the direct owner shell");
    tokio::time::timeout(Duration::from_secs(5), shell.wait())
        .await
        .expect("owner shell is reaped")
        .expect("wait for owner shell");

    tokio::time::timeout(Duration::from_secs(10), async {
        while process_exists(worker_pid) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker exits promptly after owner loss");
    worker_cleanup.0 = None;
}

#[tokio::test]
async fn real_bamboo_bus_worker_exits_after_true_idle_timeout_and_leaves_presence() {
    let bamboo_bin = Path::new(env!("CARGO_BIN_EXE_bamboo"));
    let dir = TempDir::new().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path().join("broker")));
    let token = "idle-e2e-token";
    let server = Arc::new(BrokerServer::new(core.clone(), token));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let address = listener.local_addr().expect("broker address");
    let server_task = tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{address}");

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "bus-idle-e2e".into(),
            parent_id: Some("bus-idle-parent".into()),
            project_key: None,
            role: "bus-idle".into(),
            depth: 0,
        },
        ExecutorSpec::Echo,
        dir.path().join("fabric").to_string_lossy().into_owned(),
    );
    spec.bus = Some(BusEndpoint {
        endpoint,
        token: token.into(),
    });
    spec.reusable = true;
    spec.limits.idle_timeout_secs = Some(1);
    let mut spawned = spawn_worker_on_bus(bamboo_bin, &["subagent-worker".to_string()], &spec)
        .await
        .expect("spawn production bus worker");

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if core
                .connected_by_role("bus-idle")
                .await
                .iter()
                .any(|id| id == "bus-idle-e2e")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("production worker announces broker presence");

    tokio::time::timeout(Duration::from_secs(10), async {
        while spawned.is_alive() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("idle production worker process exits");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !core
                .connected_by_role("bus-idle")
                .await
                .iter()
                .any(|id| id == "bus-idle-e2e")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("idle worker presence is removed after disconnect");

    spawned.kill().await;
    server_task.abort();
}
