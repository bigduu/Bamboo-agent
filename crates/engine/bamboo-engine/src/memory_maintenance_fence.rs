//! Cross-process serialization for long-running memory maintenance workflows.
//!
//! Jiandu already serializes each individual scope mutation. Auto-Dream history
//! rewrites need a wider fence, however: the replacement lineage is frozen
//! before provider calls and applied only after every replacement sink succeeds.
//! Every lineage-changing writer must therefore participate so none can create
//! or mutate a descendant in between.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use fs2::FileExt;
use tokio::sync::{Mutex, OwnedMutexGuard};

use bamboo_memory::memory_store::MemoryStore;

const MEMORY_MAINTENANCE_FENCE_FILE: &str = ".bamboo-memory-maintenance.lock";

fn local_fences() -> &'static StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>> {
    static FENCES: OnceLock<StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    FENCES.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn local_fence(path: &Path) -> Arc<Mutex<()>> {
    let mut fences = local_fences().lock().expect("memory fence lock poisoned");
    fences.retain(|_, fence| fence.strong_count() > 0);
    if let Some(fence) = fences.get(path).and_then(Weak::upgrade) {
        return fence;
    }
    let fence = Arc::new(Mutex::new(()));
    fences.insert(path.to_path_buf(), Arc::downgrade(&fence));
    fence
}

pub struct MemoryMaintenanceFenceGuard {
    file: File,
    _local: OwnedMutexGuard<()>,
}

impl Drop for MemoryMaintenanceFenceGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Serialize Auto-Dream extraction/rewrite transactions with every
/// lineage-changing memory mutation, including across Bamboo processes sharing
/// a Jiandu data root.
///
/// Callers must hold the returned guard across the complete read-modify-write
/// operation. Ordinary creation that cannot merge into an existing document
/// does not need this wider transaction fence.
pub async fn acquire_memory_maintenance_fence(
    memory: &MemoryStore,
) -> Result<MemoryMaintenanceFenceGuard, String> {
    let path = memory.memory_root_dir().join(MEMORY_MAINTENANCE_FENCE_FILE);
    let local = local_fence(&path).lock_owned().await;
    let lock_path = path.clone();
    let file = tokio::task::spawn_blocking(move || -> io::Result<File> {
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        FileExt::lock_exclusive(&file)?;
        Ok(file)
    })
    .await
    .map_err(|error| format!("memory maintenance fence task failed: {error}"))?
    .map_err(|error| {
        format!(
            "failed to acquire memory maintenance fence '{}': {error}",
            path.display()
        )
    })?;

    Ok(MemoryMaintenanceFenceGuard {
        file,
        _local: local,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    #[tokio::test]
    async fn fence_serializes_distinct_stores_for_the_same_data_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first_store = MemoryStore::new(temp.path());
        let second_store = MemoryStore::new(temp.path());
        let first = acquire_memory_maintenance_fence(&first_store)
            .await
            .expect("first fence");

        let waiter = tokio::spawn(async move {
            acquire_memory_maintenance_fence(&second_store)
                .await
                .expect("second fence")
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiter.is_finished(), "same-root maintenance must wait");

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should acquire after release")
            .expect("waiter task");
        drop(second);
    }
}
