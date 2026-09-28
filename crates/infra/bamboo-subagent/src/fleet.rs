//! Parent-side fleet helpers: spawn an actor subprocess, provision it over stdin, discover it.
//!
//! Bootstrap protocol:
//! 1. for typed read-only work, probe the worker's non-secret capability document,
//! 2. spawn the worker with only fixed configured arguments (nothing per-child or secret),
//! 3. write one [`ProvisionSpec`] JSON document to its stdin and close the pipe,
//! 4. poll the Tier-1 fabric until the worker self-registers under `identity.child_id`.
//!
//! The real engine adapter (`SubprocessChildRunner`) builds on these primitives.

use std::path::Path;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout, Instant};

use crate::discovery::Fabric;
use crate::proto::AgentRecord;
use crate::provision::{
    ProvisionSpec, WorkerCapabilityReport, WorkerOwner, REQUIRED_CHILD_CONTEXT_WORKER_CAPABILITY,
    TYPED_READ_ONLY_WORKER_CAPABILITY,
};
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
    provisioned.owner = Some(WorkerOwner::for_current_process(
        local_owner_instance_id().to_string(),
        spec.identity.parent_id.clone(),
    ));
    provisioned
}

const WORKER_CAPABILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
fn validate_worker_capability_report(output: &[u8]) -> TransportResult<()> {
    validate_required_capability(output, TYPED_READ_ONLY_WORKER_CAPABILITY)
}

fn validate_required_capability(output: &[u8], capability: &str) -> TransportResult<()> {
    let report: WorkerCapabilityReport = serde_json::from_slice(output).map_err(|_| {
        TransportError::Protocol("worker capability probe returned invalid JSON".into())
    })?;
    if (matches!(
        capability,
        REQUIRED_CHILD_CONTEXT_WORKER_CAPABILITY
            | crate::provision::CHILD_CREATION_IDENTITY_WORKER_CAPABILITY
    ) && report.provision_version != crate::provision::PROVISION_VERSION)
        || !report.supports(capability)
    {
        return Err(TransportError::Protocol(format!(
            "worker does not acknowledge required capability '{capability}'"
        )));
    }
    Ok(())
}

/// Verify the worker understands the authority-bearing fields before sending
/// it a typed read-only provision. An older Bamboo binary exits on the unknown
/// flag; a custom worker must implement the same explicit acknowledgement.
async fn require_typed_read_only_worker_capability(
    worker_bin: &Path,
    worker_args: &[String],
) -> TransportResult<()> {
    require_worker_capability(worker_bin, worker_args, TYPED_READ_ONLY_WORKER_CAPABILITY).await
}

/// Probe a trusted local worker before creating a required-context child.
/// Capability support is protocol compatibility, not loaded-build attestation.
pub async fn require_worker_capability(
    worker_bin: &Path,
    worker_args: &[String],
    capability: &str,
) -> TransportResult<()> {
    let mut probe = Command::new(worker_bin);
    probe.args(worker_args);
    probe.arg("--print-capabilities");
    probe.stdin(Stdio::null());
    probe.kill_on_drop(true);
    let output = timeout(WORKER_CAPABILITY_PROBE_TIMEOUT, probe.output())
        .await
        .map_err(|_| {
            TransportError::Protocol(format!(
                "worker capability probe timed out after {WORKER_CAPABILITY_PROBE_TIMEOUT:?}"
            ))
        })?
        .map_err(TransportError::Io)?;
    if !output.status.success() {
        return Err(TransportError::Protocol(format!(
            "worker capability probe failed with status {}; refusing required capability activation",
            output.status
        )));
    }
    validate_required_capability(&output.stdout, capability)
}

async fn ensure_provision_capabilities(
    worker_bin: &Path,
    worker_args: &[String],
    spec: &ProvisionSpec,
) -> TransportResult<()> {
    if spec.capabilities.native_tool_ceiling_required {
        require_worker_capability(
            worker_bin,
            worker_args,
            crate::provision::NATIVE_TOOL_CEILING_WORKER_CAPABILITY,
        )
        .await?;
    }
    if spec.capabilities.child_creation_identity {
        require_worker_capability(
            worker_bin,
            worker_args,
            crate::provision::CHILD_CREATION_IDENTITY_WORKER_CAPABILITY,
        )
        .await?;
    }
    if spec.capabilities.read_only_enforced() {
        require_typed_read_only_worker_capability(worker_bin, worker_args).await?;
    }
    if spec.capabilities.required_child_context {
        require_worker_capability(
            worker_bin,
            worker_args,
            REQUIRED_CHILD_CONTEXT_WORKER_CAPABILITY,
        )
        .await?;
    }
    Ok(())
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
    ensure_provision_capabilities(worker_bin, worker_args, spec).await?;
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
    ensure_provision_capabilities(worker_bin, worker_args, spec).await?;
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
    fn typed_read_only_worker_requires_explicit_capability_acknowledgement() {
        let accepted = serde_json::to_vec(&WorkerCapabilityReport::current()).unwrap();
        validate_worker_capability_report(&accepted).unwrap();

        let legacy = serde_json::to_vec(&WorkerCapabilityReport {
            provision_version: 1,
            capabilities: Vec::new(),
        })
        .unwrap();
        let error = validate_worker_capability_report(&legacy).unwrap_err();
        assert!(error
            .to_string()
            .contains(TYPED_READ_ONLY_WORKER_CAPABILITY));

        assert!(validate_worker_capability_report(b"not-json").is_err());
    }

    #[test]
    fn child_creation_requires_exact_schema_and_explicit_capability() {
        let capability = crate::provision::CHILD_CREATION_IDENTITY_WORKER_CAPABILITY;
        let mut report = WorkerCapabilityReport::current();
        validate_required_capability(&serde_json::to_vec(&report).unwrap(), capability).unwrap();
        report.capabilities.retain(|entry| entry != capability);
        assert!(
            validate_required_capability(&serde_json::to_vec(&report).unwrap(), capability)
                .is_err()
        );
        report.capabilities.push(capability.into());
        report.provision_version += 1;
        assert!(
            validate_required_capability(&serde_json::to_vec(&report).unwrap(), capability)
                .is_err()
        );
        assert!(validate_required_capability(b"not-json", capability).is_err());
    }

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
        #[cfg(windows)]
        assert!(first_owner.process_start_id.is_some());
        #[cfg(not(windows))]
        assert!(first_owner.process_start_id.is_none());
        assert_eq!(first_owner.process_start_id, second_owner.process_start_id);
        assert!(second_owner.worker_spawned_at >= first_owner.worker_spawned_at);
    }
}
