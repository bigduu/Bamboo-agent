//! Error model + atomic write helper shared by `store` and `mailbox`.

use std::path::{Path, PathBuf};

/// Errors from the persistent store / mailbox layer.
///
/// Invariant: authoritative data lives in `session.json` files; index and mailbox files are
/// caches/queues — a corrupt one is recoverable (rebuild / quarantine), never fatal.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("decode {path}: {source}")]
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("corrupt index {path}, rebuild required")]
    CorruptIndex { path: PathBuf },
    #[error("not found: {0}")]
    NotFound(String),
    /// A `ProvisionSpec` violated a cross-field invariant before being shipped
    /// to a worker (e.g. both `mcp` and `mcp_proxy` set).
    #[error("invalid provision spec: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

impl StoreError {
    pub(crate) fn io(path: impl AsRef<Path>, source: std::io::Error) -> Self {
        StoreError::Io {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
    pub(crate) fn decode(path: impl AsRef<Path>, source: serde_json::Error) -> Self {
        StoreError::Decode {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
}

/// Write `bytes` to `path` atomically: a temp file in the same directory + `rename`.
///
/// Readers never observe a half-written file. Parent directories are created as needed.
/// The temp name is hidden (`.`-prefixed) and unique so concurrent writers and directory
/// scanners (e.g. mailbox `drain`) skip it.
pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    match atomic_write_with_gate(path, bytes, None).await? {
        bamboo_domain::AdmissionCommit::Committed(()) => Ok(()),
        _ => unreachable!("ungated atomic write always commits"),
    }
}

struct HiddenTempCleanup(std::path::PathBuf);

impl Drop for HiddenTempCleanup {
    fn drop(&mut self) {
        // The gated writer owns this guard for its entire blocking write, even
        // if the awaiting async future is dropped. Hidden temp files are never
        // delivered messages.
        let _ = std::fs::remove_file(&self.0);
    }
}

pub(crate) async fn atomic_write_with_gate(
    path: &Path,
    bytes: &[u8],
    gate: Option<&bamboo_domain::AdmissionGate>,
) -> Result<bamboo_domain::AdmissionCommit<()>> {
    atomic_write_with_gate_inner(path, bytes, gate, None).await
}

async fn atomic_write_with_gate_inner(
    path: &Path,
    bytes: &[u8],
    gate: Option<&bamboo_domain::AdmissionGate>,
    before_commit: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
) -> Result<bamboo_domain::AdmissionCommit<()>> {
    use tokio::io::AsyncWriteExt;

    let dir = path
        .parent()
        .ok_or_else(|| StoreError::NotFound(format!("no parent dir for {}", path.display())))?;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| StoreError::io(dir, e))?;

    let stem = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let tmp = dir.join(format!(".{stem}.tmp.{}", uuid::Uuid::new_v4()));
    if let Some(gate) = gate {
        let gate = bamboo_domain::AdmissionGate::clone(gate);
        let path = path.to_path_buf();
        let bytes = bytes.to_vec();
        // The task owns both temp creation and cleanup. Dropping the awaiting
        // future cannot race an in-flight async create/write that recreates a
        // hidden temp after its cleanup guard has already run.
        return tokio::task::spawn_blocking(move || {
            write_hidden_temp_blocking(&path, &tmp, &bytes, Some(&gate), |phase, _| {
                if phase == "maildir_replace" {
                    if let Some(before_commit) = &before_commit {
                        before_commit();
                    }
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| StoreError::Invalid(format!("inbox write task failed: {error}")))?;
    }

    let _cleanup = HiddenTempCleanup(tmp.clone());
    {
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| StoreError::io(&tmp, e))?;
        file.write_all(bytes)
            .await
            .map_err(|e| StoreError::io(&tmp, e))?;
        file.sync_all().await.map_err(|e| StoreError::io(&tmp, e))?;
    }
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| StoreError::io(path, e))?;
    Ok(bamboo_domain::AdmissionCommit::Committed(()))
}

/// Complete synchronous Maildir writer for callers already inside their owned job.
/// The gate linearizes permission only; the caller supplies physical ownership.
pub(crate) fn atomic_write_with_gate_blocking(
    path: &Path,
    bytes: &[u8],
    gate: Option<&bamboo_domain::AdmissionGate>,
    observe: impl Fn(&str, &Path) -> std::io::Result<()>,
) -> Result<bamboo_domain::AdmissionCommit<()>> {
    let dir = path
        .parent()
        .ok_or_else(|| StoreError::NotFound(format!("no parent dir for {}", path.display())))?;
    std::fs::create_dir_all(dir).map_err(|e| StoreError::io(dir, e))?;
    let stem = path.file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let temp = dir.join(format!(".{stem}.tmp.{}", uuid::Uuid::new_v4()));
    write_hidden_temp_blocking(path, &temp, bytes, gate, observe)
}

fn write_hidden_temp_blocking(
    path: &Path,
    temp: &Path,
    bytes: &[u8],
    gate: Option<&bamboo_domain::AdmissionGate>,
    observe: impl Fn(&str, &Path) -> std::io::Result<()>,
) -> Result<bamboo_domain::AdmissionCommit<()>> {
    use std::io::Write;
    let cleanup = HiddenTempCleanup(temp.to_owned());
    let result = (|| {
        if gate.is_some_and(bamboo_domain::AdmissionGate::is_cancelled) {
            return Ok(bamboo_domain::AdmissionCommit::Cancelled);
        }
        let mut file = std::fs::File::create(temp).map_err(|e| StoreError::io(temp, e))?;
        file.write_all(bytes).map_err(|e| StoreError::io(temp, e))?;
        file.sync_all().map_err(|e| StoreError::io(temp, e))?;
        drop(file);
        observe("maildir_replace", path).map_err(|e| StoreError::io(path, e))?;
        let rename = || std::fs::rename(temp, path).map_err(|e| StoreError::io(path, e));
        match gate {
            Some(gate) => gate.commit(rename),
            None => rename().map(bamboo_domain::AdmissionCommit::Committed),
        }
    })();
    // Observation cannot replace a primary error or suppress actual cleanup.
    let _ = observe("maildir_cleanup", path);
    drop(cleanup);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex};

    #[tokio::test]
    async fn cancelled_after_fsync_before_rename_cleans_hidden_temp_after_awaiter_abort() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("visible.json");
        let gate = Arc::new(bamboo_domain::AdmissionGate::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let before_commit: Arc<dyn Fn() + Send + Sync> = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move || {
                entered.notify_one();
                let (lock, condvar) = &*release;
                let released = lock.lock().unwrap();
                drop(condvar.wait_while(released, |released| !*released).unwrap());
            })
        };
        let writing = {
            let gate = gate.clone();
            let target = target.clone();
            tokio::spawn(async move {
                atomic_write_with_gate_inner(
                    &target,
                    b"durable bytes",
                    Some(&gate),
                    Some(before_commit),
                )
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("writer must flush the hidden temp before rename");
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);

        gate.cancel_if_pending();
        writing.abort();
        assert!(writing.await.unwrap_err().is_cancelled());
        let (lock, condvar) = &*release;
        *lock.lock().unwrap() = true;
        condvar.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while std::fs::read_dir(temp.path()).unwrap().count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached writer must clean its hidden temp");
        assert!(!target.exists());
        assert!(gate.is_cancelled());
    }
}
