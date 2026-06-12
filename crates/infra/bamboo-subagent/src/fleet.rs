//! Parent-side fleet helpers: spawn an actor subprocess and discover it.
//!
//! This is the minimal `SubagentFleet` surface for the demo/e2e: launch the worker binary, wait
//! for it to self-register into the Tier-1 fabric, and hand back its [`AgentRecord`] + process
//! handle. The real engine adapter (`SubprocessChildRunner`) will build on the same primitives.

use std::path::Path;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

use crate::discovery::Fabric;
use crate::proto::AgentRecord;
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

/// Spawn `worker_bin <child_id> <fabric_dir> <role>`, then poll the fabric until the child
/// self-registers (or `wait` elapses). On timeout the process is killed and an error returned.
pub async fn spawn_worker(
    worker_bin: &Path,
    fabric_dir: &Path,
    child_id: &str,
    role: &str,
    wait: Duration,
) -> TransportResult<SpawnedChild> {
    tokio::fs::create_dir_all(fabric_dir).await.ok();

    let mut cmd = Command::new(worker_bin);
    cmd.arg(child_id).arg(fabric_dir).arg(role);
    cmd.kill_on_drop(true);
    let mut process = cmd.spawn().map_err(TransportError::Io)?;

    let fab = Fabric::at(fabric_dir);
    let deadline = Instant::now() + wait;
    loop {
        if let Ok(Some(record)) = fab.resolve(child_id).await {
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
