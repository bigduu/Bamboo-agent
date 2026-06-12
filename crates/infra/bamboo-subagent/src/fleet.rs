//! Parent-side fleet helpers: spawn an actor subprocess, provision it over stdin, discover it.
//!
//! Bootstrap protocol:
//! 1. spawn the worker binary with **no arguments** (nothing secret in argv/env),
//! 2. write one [`ProvisionSpec`] JSON document to its stdin and close the pipe,
//! 3. poll the Tier-1 fabric until the worker self-registers under `identity.child_id`.
//!
//! The real engine adapter (`SubprocessChildRunner`) builds on these primitives.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

use crate::discovery::Fabric;
use crate::proto::AgentRecord;
use crate::provision::ProvisionSpec;
use crate::transport::{TransportError, TransportResult};

/// A spawned actor subprocess plus its discovered record. Killed on drop (`kill_on_drop`).
pub struct SpawnedChild {
    pub record: AgentRecord,
    process: Child,
}

impl SpawnedChild {
    /// Terminate the child process.
    pub async fn kill(mut self) {
        let _ = self.process.kill().await;
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.id()
    }
}

/// Spawn `worker_bin worker_args…`, provision it with `spec` over stdin, then poll the fabric
/// until the worker self-registers (or `wait` elapses). On timeout the process is killed.
///
/// `worker_args` carries fixed subcommand/flag arguments (e.g. `["subagent-worker"]` for the
/// main `bamboo` binary) — never per-child data, which all rides in the spec.
pub async fn spawn_worker(
    worker_bin: &Path,
    worker_args: &[String],
    spec: &ProvisionSpec,
    wait: Duration,
) -> TransportResult<SpawnedChild> {
    let fabric_dir = Path::new(&spec.fabric_dir);
    tokio::fs::create_dir_all(fabric_dir).await.ok();

    let spec_json = spec
        .to_json()
        .map_err(|e| TransportError::Protocol(format!("provision spec encode: {e}")))?;

    let mut cmd = Command::new(worker_bin);
    cmd.args(worker_args);
    cmd.stdin(Stdio::piped());
    cmd.kill_on_drop(true);
    let mut process = cmd.spawn().map_err(TransportError::Io)?;

    // Feed the spec, then close stdin so the worker sees EOF.
    {
        let mut stdin = process
            .stdin
            .take()
            .ok_or_else(|| TransportError::Protocol("worker stdin unavailable".to_string()))?;
        stdin
            .write_all(spec_json.as_bytes())
            .await
            .map_err(TransportError::Io)?;
        stdin.shutdown().await.map_err(TransportError::Io)?;
    }

    let child_id = spec.identity.child_id.clone();
    let fab = Fabric::at(fabric_dir);
    let deadline = Instant::now() + wait;
    loop {
        if let Ok(Some(record)) = fab.resolve(&child_id).await {
            return Ok(SpawnedChild { record, process });
        }
        // bail early if the worker died before registering
        if let Ok(Some(status)) = process.try_wait() {
            return Err(TransportError::Protocol(format!(
                "worker exited before registering: {status}"
            )));
        }
        if Instant::now() >= deadline {
            let _ = process.kill().await;
            return Err(TransportError::Protocol(format!(
                "worker '{child_id}' did not register within {wait:?}"
            )));
        }
        sleep(Duration::from_millis(20)).await;
    }
}
