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

use crate::fleet::{spawn_worker, SpawnedChild};
use crate::provision::ProvisionSpec;
use crate::transport::TransportResult;

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The launcher must be object-safe so it can be stored as
    /// `Arc<dyn WorkerLauncher>` (how the fleet will hold the chosen placement).
    #[test]
    fn local_subprocess_launcher_is_a_trait_object() {
        let launcher = LocalSubprocessLauncher::new("/bin/true", vec!["subagent-worker".into()]);
        let _dyn: &dyn WorkerLauncher = &launcher;
        assert_eq!(launcher.worker_bin, PathBuf::from("/bin/true"));
        assert_eq!(launcher.worker_args, vec!["subagent-worker".to_string()]);
    }
}
