//! Owned bootstrap permission, before any native browser writer is admitted.
//! This does not recover an old launch or prove exit of an admitted writer.

use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use tokio::process::Child;

use super::{
    profile, read_host_line, Arc, AsyncWriteExt, BrowserCleanup, BrowserError, BrowserLifecycle,
    BrowserReaper, BufReader, Instant, Mutex,
};

const PREFIX: &[u8] = b"BAMBOO_BROWSER_LAUNCH_V1 ";
const MAX_FRAME_BYTES: usize = 16_384;

#[derive(Deserialize)]
enum ReadyKind {
    #[serde(rename = "bamboo_browser_launch_ready")]
    Ready,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    #[serde(rename = "kind")]
    _kind: ReadyKind,
    version: u8,
    attempt_id: uuid::Uuid,
}

fn unavailable() -> BrowserError {
    BrowserError::Unavailable("browser launch admission unavailable".into())
}

pub(super) fn verify_host(path: &Path) -> Result<std::fs::File, BrowserError> {
    // Compatibility only, under the existing packaged-host integrity contract.
    // Do not turn a string marker into authentication or OS containment proof.
    const HEADER: &[u8] = b"#!/usr/bin/env node\n// BAMBOO_BROWSER_STORAGE_INSPECTION_V1\n// BAMBOO_BROWSER_LAUNCH_ADMISSION_V1\n";
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.len() > 8 * 1024 * 1024 {
        return Err(unavailable());
    }
    let mut header = vec![0; HEADER.len()];
    file.read_exact(&mut header).map_err(|_| unavailable())?;
    if header != HEADER {
        return Err(unavailable());
    }
    Ok(file)
}

/// A compatibility marker, not a permission or storage-containment proof.
pub(super) fn verify_clean_host(path: &Path) -> Result<std::fs::File, BrowserError> {
    const HEADER: &[u8] = b"// BAMBOO_BROWSER_EPHEMERAL_CLEAN_V1\n";
    let mut file = verify_host(path)?;
    let mut header = vec![0; HEADER.len()];
    file.read_exact(&mut header).map_err(|_| unavailable())?;
    if header != HEADER {
        return Err(unavailable());
    }
    Ok(file)
}

fn valid_ready(line: &[u8], attempt: uuid::Uuid) -> bool {
    if line.len() > MAX_FRAME_BYTES {
        return false;
    }
    let Some(json) = line.strip_prefix(PREFIX) else {
        return false;
    };
    let Ok(ready) = serde_json::from_slice::<Ready>(json) else {
        return false;
    };
    ready.version == 1 && ready.attempt_id == attempt
}

pub(super) struct Admitted {
    pub(super) child: Child,
    pub(super) lease: profile::Lease,
    pub(super) scratch: tempfile::TempDir,
    pub(super) cleanup: Arc<BrowserCleanup>,
}

struct BootstrapOwner {
    child: Option<Child>,
    lease: Option<profile::Lease>,
    scratch: Option<tempfile::TempDir>,
    cleanup: Arc<BrowserCleanup>,
    // Set BEFORE writing a durable fence or attempting any grant bytes. A
    // cancellation/partial write thereafter can never take the unadmitted path.
    native_writer_possible: bool,
}

impl BootstrapOwner {
    fn mark_native_writer_possible(&mut self) {
        // Monotonic state transition, also consumed by Drop on early errors.
        self.native_writer_possible = true;
    }
}

impl Drop for BootstrapOwner {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let process_group = child.id().and_then(|id| i32::try_from(id).ok());
        let lease = self.lease.take();
        let marked = !self.native_writer_possible
            || lease
                .as_ref()
                .is_some_and(|lease| lease.begin_close().is_ok());
        let can_detach = lease.is_none() || marked;
        let resources = BrowserReaper {
            stdin: child.stdin.take(),
            child: Arc::new(Mutex::new(child)),
            cleanup: self.cleanup.clone(),
            lease,
            temp_dir: self.scratch.take(),
            process_group,
            group_exit_required: true,
            // No completed runtime handshake exists after an ambiguous grant.
            // An exit0 alone must not convert that into a confirmed close.
            graceful: !self.native_writer_possible,
            marked,
            clean_host_exit: false,
            native_launch_unadmitted: !self.native_writer_possible,
        };
        if can_detach {
            resources.spawn();
        } else {
            // The same durable-marker failure rule as ordinary owner Drop.
            resources.reap();
        }
    }
}

pub(super) fn admit(
    child: Child,
    lease: profile::Lease,
    scratch: tempfile::TempDir,
    lifecycle: &BrowserLifecycle,
    attempt: uuid::Uuid,
    deadline: Instant,
) -> impl std::future::Future<Output = Result<Admitted, BrowserError>> + '_ {
    // Construct the guard before returning the future: even cancellation before
    // its first poll must reap the child before releasing its lease/scratch.
    let mut owner = BootstrapOwner {
        child: Some(child),
        lease: Some(lease),
        scratch: Some(scratch),
        cleanup: BrowserCleanup::register(lifecycle),
        native_writer_possible: false,
    };
    async move {
        let mut stdin = owner
            .child
            .as_mut()
            .unwrap()
            .stdin
            .take()
            .ok_or_else(unavailable)?;
        let stdout = owner
            .child
            .as_mut()
            .unwrap()
            .stdout
            .take()
            .ok_or_else(unavailable)?;
        let mut reader = BufReader::new(stdout);
        let ready = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            read_host_line(&mut reader),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?
        .ok_or_else(unavailable)?;
        if !valid_ready(&ready, attempt) || !reader.buffer().is_empty() {
            // A host violating the bootstrap contract is not a proven safe failure.
            owner.mark_native_writer_possible();
            return Err(unavailable());
        }
        lifecycle.ensure_writer_admission()?;
        owner.mark_native_writer_possible();
        let generation = owner.lease.as_mut().unwrap().begin_launch()?.to_owned();
        let mut grant = PREFIX.to_vec();
        grant.extend(
            serde_json::to_vec(&serde_json::json!({
                "kind": "bamboo_browser_launch_grant", "version": 1,
                "attempt_id": attempt, "launch_generation": generation,
            }))
            .map_err(|_| unavailable())?,
        );
        grant.push(b'\n');
        tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), async {
            stdin.write_all(&grant).await?;
            stdin.flush().await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        // The child cannot produce normal RPC before the grant. No buffered line
        // may be discarded when handing its original pipes to BrowserSession.
        let mut child = owner.child.take().unwrap();
        child.stdin = Some(stdin);
        child.stdout = Some(reader.into_inner());
        Ok(Admitted {
            child,
            lease: owner.lease.take().unwrap(),
            scratch: owner.scratch.take().unwrap(),
            cleanup: owner.cleanup.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_is_exact_bounded_and_bound_to_the_native_attempt() {
        let attempt = uuid::Uuid::new_v4();
        let line = format!("BAMBOO_BROWSER_LAUNCH_V1 {{\"kind\":\"bamboo_browser_launch_ready\",\"version\":1,\"attempt_id\":\"{attempt}\"}}\n");
        assert!(valid_ready(line.as_bytes(), attempt));
        assert!(!valid_ready(line.as_bytes(), uuid::Uuid::new_v4()));
        assert!(!valid_ready(
            line.replace("\"version\":1", "\"version\":2").as_bytes(),
            attempt
        ));
        assert!(!valid_ready(
            line.replace("\"version\":1", "\"version\":1,\"confirmed\":true")
                .as_bytes(),
            attempt
        ));
        assert!(!valid_ready(&vec![b'x'; MAX_FRAME_BYTES + 1], attempt));
    }

    #[test]
    fn older_host_is_rejected_before_bootstrap() {
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("host.cjs");
        std::fs::write(
            &script,
            b"#!/usr/bin/env node\n// BAMBOO_BROWSER_STORAGE_INSPECTION_V1\n",
        )
        .unwrap();
        assert!(verify_host(&script).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unpolled_prepermission_cancellation_still_reaps_owned_resources() {
        use std::process::Stdio;

        let root = tempfile::tempdir().unwrap();
        let profiles = root.path().join("profiles");
        let lease = profile::acquire_shared(&profiles).unwrap();
        let control = lease.path.clone();
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().to_path_buf();
        let mut command = tokio::process::Command::new("/bin/cat");
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let lifecycle = BrowserLifecycle::default();
        let pending = admit(
            command.spawn().unwrap(),
            lease,
            scratch,
            &lifecycle,
            uuid::Uuid::new_v4(),
            Instant::now() + super::super::BROWSER_COMMAND_DEADLINE,
        );
        drop(pending); // Intentionally never polled.
        let owners = lifecycle.owners.lock().unwrap().clone();
        assert_eq!(owners.len(), 1);
        for owner in owners {
            tokio::time::timeout(super::super::BROWSER_REAP_DEADLINE, owner.wait())
                .await
                .unwrap();
            assert!(!owner.is_quarantined());
        }
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(control).unwrap().count(), 1);
        assert!(profile::acquire_shared(&profiles).is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn known_prepermission_exit_does_not_persist_an_unknown_writer() {
        use std::process::Stdio;
        let root = tempfile::tempdir().unwrap();
        let profiles = root.path().join("profiles");
        let lease = profile::acquire_shared(&profiles).unwrap();
        let control = lease.path.clone();
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().to_path_buf();
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("exit 1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0);
        let lifecycle = BrowserLifecycle::default();
        let result = admit(
            command.spawn().unwrap(),
            lease,
            scratch,
            &lifecycle,
            uuid::Uuid::new_v4(),
            Instant::now() + super::super::BROWSER_COMMAND_DEADLINE,
        )
        .await;
        assert!(result.is_err());
        let owners = lifecycle.owners.lock().unwrap().clone();
        for owner in owners {
            tokio::time::timeout(super::super::BROWSER_REAP_DEADLINE, owner.wait())
                .await
                .unwrap();
            assert!(!owner.is_quarantined());
        }
        assert!(!lifecycle
            .unconfirmed_writers
            .load(super::super::Ordering::Acquire));
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(&control).unwrap().count(), 1);
        assert!(profile::acquire_shared(&profiles).is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonzero_exit_after_permission_still_quarantines_the_native_writer() {
        use std::process::Stdio;

        let root = tempfile::tempdir().unwrap();
        let profiles = root.path().join("profiles");
        let lease = profile::acquire_shared(&profiles).unwrap();
        let control = lease.path.clone();
        let scratch = tempfile::tempdir().unwrap();
        let scratch_path = scratch.path().to_path_buf();
        let attempt = uuid::Uuid::new_v4();
        let ready = format!("BAMBOO_BROWSER_LAUNCH_V1 {{\"kind\":\"bamboo_browser_launch_ready\",\"version\":1,\"attempt_id\":\"{attempt}\"}}");
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(format!(
                "printf '%s\\n' '{ready}'; IFS= read -r grant; exit 1"
            ))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let lifecycle = BrowserLifecycle {
            bounded_storage: true,
            ..BrowserLifecycle::default()
        };
        let admitted = admit(
            command.spawn().unwrap(),
            lease,
            scratch,
            &lifecycle,
            attempt,
            Instant::now() + super::super::BROWSER_COMMAND_DEADLINE,
        )
        .await
        .unwrap();
        let cleanup = admitted.cleanup.clone();
        let session = super::super::BrowserSession::from_child_owned(
            admitted.child,
            Some(admitted.scratch),
            &lifecycle,
            Some(admitted.lease),
            Some(admitted.cleanup),
        )
        .unwrap();
        tokio::time::timeout(super::super::BROWSER_CLOSE_DEADLINE, cleanup.wait())
            .await
            .unwrap();
        assert!(cleanup.is_quarantined());
        assert!(control.join(".bamboo-profile-launch-active-v1").is_file());
        assert!(control
            .join(".bamboo-profile-writer-exit-unconfirmed")
            .is_file());
        assert!(scratch_path.is_dir());
        assert!(profile::acquire_shared(&profiles).is_err());
        drop(session);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_ready_then_exit_zero_still_quarantines_the_unknown_contract() {
        use std::process::Stdio;

        let root = tempfile::tempdir().unwrap();
        let profiles = root.path().join("profiles");
        let lease = profile::acquire_shared(&profiles).unwrap();
        let control = lease.path.clone();
        let scratch = tempfile::tempdir_in(root.path()).unwrap();
        let scratch_path = scratch.path().to_path_buf();
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("printf '{}\\n'; exit 0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let lifecycle = BrowserLifecycle::default();
        let result = admit(
            command.spawn().unwrap(),
            lease,
            scratch,
            &lifecycle,
            uuid::Uuid::new_v4(),
            Instant::now() + super::super::BROWSER_COMMAND_DEADLINE,
        )
        .await;
        assert!(result.is_err());
        let owners = lifecycle.owners.lock().unwrap().clone();
        for owner in owners {
            tokio::time::timeout(super::super::BROWSER_REAP_DEADLINE, owner.wait())
                .await
                .unwrap();
            assert!(owner.is_quarantined());
        }
        assert!(control
            .join(".bamboo-profile-writer-exit-unconfirmed")
            .is_file());
        assert!(scratch_path.is_dir());
        assert!(profile::acquire_shared(&profiles).is_err());
    }
}
