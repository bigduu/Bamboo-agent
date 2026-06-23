//! `WorkerLauncher`: the seam between "spawn a local subprocess" and "connect to
//! a remote worker" (see `docs/remote-actor-plan.md` §3.1).
//!
//! Phase 0 ships only [`LocalSubprocessLauncher`], a zero-behavior-change wrapper
//! over [`crate::fleet::spawn_worker`]. Remote launchers (connect to a resident
//! `wss://` worker, or schedule one via a control plane) plug in later behind the
//! same trait, so the rest of the fleet/runner code never branches on *where* a
//! worker runs — only on this abstraction.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use crate::fleet::{spawn_worker, SpawnedChild};
use crate::proto::AgentRecord;
use crate::provision::{Placement, ProvisionSpec};
use crate::transport::{ChildClient, TransportError, TransportResult};

/// Brings up (or connects to) one actor worker for a [`ProvisionSpec`] and waits
/// up to `wait` for it to become reachable (self-registered in discovery).
///
/// Abstracts *how* a worker comes to exist so callers stay placement-agnostic.
#[async_trait]
pub trait WorkerLauncher: Send + Sync {
    async fn launch(&self, spec: &ProvisionSpec, wait: Duration) -> TransportResult<SpawnedChild>;
}

/// The current, default behavior: spawn the worker binary as a local OS
/// subprocess and provision it over stdin. A direct, zero-behavior-change
/// wrapper over [`spawn_worker`].
pub struct LocalSubprocessLauncher {
    pub worker_bin: PathBuf,
    pub worker_args: Vec<String>,
}

impl LocalSubprocessLauncher {
    pub fn new(worker_bin: impl Into<PathBuf>, worker_args: Vec<String>) -> Self {
        Self {
            worker_bin: worker_bin.into(),
            worker_args,
        }
    }
}

#[async_trait]
impl WorkerLauncher for LocalSubprocessLauncher {
    async fn launch(&self, spec: &ProvisionSpec, wait: Duration) -> TransportResult<SpawnedChild> {
        spawn_worker(&self.worker_bin, &self.worker_args, spec, wait).await
    }
}

/// Connect to an already-running resident worker instead of spawning one
/// (remote-actor-plan §3.1 / P1). Owns no OS process: the returned
/// [`SpawnedChild`] carries `process: None`, so its `kill()` is a no-op and the
/// parent reclaims the worker by closing the connection + the worker's own idle
/// timeout.
///
/// The launcher reads everything it needs from the spec:
/// - `spec.placement` MUST be [`Placement::Remote`] — it supplies the endpoint.
/// - `spec.secrets.worker_auth_token` supplies the bearer used for the
///   connectivity probe (it is NEVER copied into the discovery `AgentRecord`).
pub struct ConnectLauncher;

#[async_trait]
impl WorkerLauncher for ConnectLauncher {
    async fn launch(&self, spec: &ProvisionSpec, wait: Duration) -> TransportResult<SpawnedChild> {
        let endpoint = match &spec.placement {
            Placement::Remote { endpoint } => endpoint.clone(),
            other => {
                return Err(TransportError::Protocol(format!(
                    "ConnectLauncher requires Placement::Remote, got {other:?}"
                )))
            }
        };

        // Connectivity probe: connect (with the bearer if any) and close again,
        // bounded by `wait`, so an unreachable / mis-authenticated worker fails
        // fast here instead of at the first `Run`. The token authenticates the
        // probe but never leaves this scope.
        let token = spec.secrets.worker_auth_token.as_deref();
        let probe = ChildClient::connect_with_auth(&endpoint, token);
        match tokio::time::timeout(wait, probe).await {
            Ok(Ok(client)) => {
                let _ = client.close().await;
            }
            Ok(Err(e)) => {
                return Err(TransportError::Protocol(format!(
                    "remote worker '{}' unreachable at {endpoint}: {e}",
                    spec.identity.child_id
                )))
            }
            Err(_) => {
                return Err(TransportError::Protocol(format!(
                    "remote worker '{}' did not accept a connection at {endpoint} within {wait:?}",
                    spec.identity.child_id
                )))
            }
        }

        // Synthesize a record from the spec's identity + the remote endpoint.
        // No owned process (pid 0) and NO token (tokens never go in discovery
        // records).
        let record = AgentRecord {
            agent_id: spec.identity.child_id.clone(),
            role: spec.identity.role.clone(),
            labels: Vec::new(),
            endpoint,
            pid: 0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            started_at: Utc::now(),
            lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
        };
        Ok(SpawnedChild::remote(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provision::{ChildIdentity, ExecutorSpec};

    /// The launcher must be object-safe so it can be stored as
    /// `Arc<dyn WorkerLauncher>` (how the fleet will hold the chosen placement).
    #[test]
    fn local_subprocess_launcher_is_a_trait_object() {
        let launcher = LocalSubprocessLauncher::new("/bin/true", vec!["subagent-worker".into()]);
        let _dyn: &dyn WorkerLauncher = &launcher;
        assert_eq!(launcher.worker_bin, PathBuf::from("/bin/true"));
        assert_eq!(launcher.worker_args, vec!["subagent-worker".to_string()]);
    }

    #[test]
    fn connect_launcher_is_a_trait_object() {
        let _dyn: &dyn WorkerLauncher = &ConnectLauncher;
    }

    /// A non-Remote placement is a clear error (no spawn, no connect).
    #[tokio::test]
    async fn connect_launcher_rejects_non_remote_placement() {
        let spec = ProvisionSpec::new(
            ChildIdentity {
                child_id: "c1".into(),
                parent_id: None,
                project_key: None,
                role: "demo".into(),
                depth: 0,
            },
            ExecutorSpec::Echo,
            "/tmp/fabric".into(),
        );
        // Default placement is Local.
        let result = ConnectLauncher
            .launch(&spec, Duration::from_millis(100))
            .await;
        match result {
            Err(TransportError::Protocol(m)) if m.contains("Placement::Remote") => {}
            Err(other) => panic!("expected a clear Placement::Remote error, got {other:?}"),
            Ok(_) => panic!("Local placement must be rejected"),
        }
    }
}
