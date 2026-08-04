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
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

use crate::discovery::Fabric;
use crate::proto::AgentRecord;
use crate::provision::{ProvisionSpec, WorkerOwner};
use crate::transport::{TransportError, TransportResult};

fn local_owner_instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID
        .get_or_init(|| uuid::Uuid::new_v4().to_string())
        .as_str()
}

/// Stamp physical owner metadata at the last possible moment so a cloned or
/// cached spec cannot carry a stale PID/instance into a later spawn.
fn provision_for_local_spawn(spec: &ProvisionSpec) -> ProvisionSpec {
    let mut provisioned = spec.clone();
    provisioned.owner = Some(WorkerOwner {
        process_id: std::process::id(),
        instance_id: local_owner_instance_id().to_string(),
        session_id: spec.identity.parent_id.clone(),
        worker_spawned_at: chrono::Utc::now(),
    });
    provisioned
}

/// A launched actor plus its discovered record.
///
/// `process` is `Some` for a locally-spawned subprocess (killed on drop via
/// `kill_on_drop`) and `None` for a remote resident worker reached over the
/// network (remote-actor-plan P1, #181): a `ConnectLauncher` owns no OS process,
/// so `kill()` is a no-op and `pid()` returns `None`. The parent reclaims a
/// remote worker by closing the connection + the worker's own idle timeout, not
/// by killing a process it does not own.
pub struct SpawnedChild {
    pub record: AgentRecord,
    process: Option<Child>,
}

impl SpawnedChild {
    /// Build a record-only handle for a worker this process does not own (a
    /// remote resident worker connected to, not spawned). `kill()`/`pid()` are
    /// inert.
    pub fn remote(record: AgentRecord) -> Self {
        Self {
            record,
            process: None,
        }
    }

    /// Terminate the child process. A no-op for a remote (process-less) worker.
    pub async fn kill(mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill().await;
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.as_ref().and_then(|p| p.id())
    }

    /// Whether the owned child process is still running (best-effort, non-blocking
    /// `try_wait`). A remote (process-less) handle reports `false` — it is never
    /// pool-owned. Used by the warm pool to skip + reap a worker that exited while
    /// parked before handing it out for reuse.
    pub fn is_alive(&mut self) -> bool {
        match self.process.as_mut() {
            Some(p) => matches!(p.try_wait(), Ok(None)),
            None => false,
        }
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

    let spec_json = provision_for_local_spawn(spec)
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
            return Ok(SpawnedChild {
                record,
                process: Some(process),
            });
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

/// Spawn a worker that dials the mailbox bus (`spec.bus`) instead of listening
/// for a direct WS connection. Returns as soon as the process is spawned + fed
/// its spec — there is no file-discovery rendezvous to wait for; the worker
/// dials the bus asynchronously and the broker queues any `Run` until it
/// subscribes. The returned [`SpawnedChild`] carries a synthetic record (the
/// worker is addressed by mailbox id, not a listen endpoint) + the kill-on-drop
/// process handle.
pub async fn spawn_worker_on_bus(
    worker_bin: &Path,
    worker_args: &[String],
    spec: &ProvisionSpec,
) -> TransportResult<SpawnedChild> {
    let spec_json = provision_for_local_spawn(spec)
        .to_json()
        .map_err(|e| TransportError::Protocol(format!("provision spec encode: {e}")))?;

    let mut cmd = Command::new(worker_bin);
    cmd.args(worker_args);
    cmd.stdin(Stdio::piped());
    cmd.kill_on_drop(true);
    let mut process = cmd.spawn().map_err(TransportError::Io)?;
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

    let record = AgentRecord {
        agent_id: spec.identity.child_id.clone(),
        role: spec.identity.role.clone(),
        labels: Vec::new(),
        endpoint: spec
            .bus
            .as_ref()
            .map(|b| b.endpoint.clone())
            .unwrap_or_default(),
        pid: process.id().unwrap_or(0),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: chrono::Utc::now(),
        lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
    };
    Ok(SpawnedChild {
        record,
        process: Some(process),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provision::{ChildIdentity, ExecutorSpec};

    #[test]
    fn local_spawn_stamps_current_process_instance_and_parent_session() {
        let spec = ProvisionSpec::new(
            ChildIdentity {
                child_id: "child".into(),
                parent_id: Some("parent-session".into()),
                project_key: None,
                role: "worker".into(),
                depth: 1,
            },
            ExecutorSpec::Echo,
            "fabric".into(),
        );

        let first = provision_for_local_spawn(&spec);
        let second = provision_for_local_spawn(&spec);
        assert!(spec.owner.is_none(), "caller-owned spec remains reusable");
        let first_owner = first.owner.expect("physical owner metadata");
        let second_owner = second.owner.expect("physical owner metadata");
        assert_eq!(first_owner.process_id, std::process::id());
        assert_eq!(first_owner.instance_id, second_owner.instance_id);
        assert_eq!(first_owner.session_id.as_deref(), Some("parent-session"));
        assert!(second_owner.worker_spawned_at >= first_owner.worker_spawned_at);
    }
}
