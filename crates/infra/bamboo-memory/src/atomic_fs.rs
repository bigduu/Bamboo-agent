//! Crash-safe file writes for the memory store.
//!
//! A plain [`tokio::fs::write`] truncates the target and then streams the new
//! bytes, so a crash (OOM, panic, power loss) mid-write can leave a user memory
//! document half-written or empty — the worst-impact corruption case tracked in
//! #35. These helpers instead write to a unique sibling temp file, fsync it, and
//! atomically rename it over the target, so a concurrent reader (or the next
//! startup) always observes either the old content or the complete new content,
//! never a torn write. Follow-up to #35, tracked in #166.
//!
//! bamboo-memory does not depend on bamboo-storage (where the sibling
//! `atomic_write` for session JSONL lives), so this is a self-contained local
//! helper rather than a shared one — deliberately keeping the crate boundary.
//! [`plan_store`](crate::plan_store) has a matching *synchronous* atomic writer
//! for the plan artifacts; the two stay split because that path is sync and this
//! one is async, but they share [`unique_temp_path`] so temp naming can't drift.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Process-wide monotonic counter so two writers to the *same* target within a
/// single clock tick still get distinct temp names — `nanos` alone can collide.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A unique sibling temp path (`.<name>.<pid>.<nanos>.<seq>.tmp`) so concurrent
/// writers to the same target never collide on the temp file. Shared with
/// [`crate::plan_store`] so both atomic writers name temps identically.
pub(crate) fn unique_temp_path(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    path.with_file_name(format!(
        ".{file_name}.{}.{nanos}.{seq}.tmp",
        std::process::id()
    ))
}

/// Write `bytes` to `tmp` and fsync so the bytes are durable before any rename
/// publishes them.
async fn write_and_sync(tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = fs::File::create(tmp).await?;
    file.write_all(bytes).await?;
    file.sync_all().await
}

/// Atomically rename `from` over `to`. On Unix this is a single atomic syscall
/// even when `to` exists; on Windows rename cannot overwrite, so fall back to a
/// remove-then-rename (which has a small non-atomic window). A genuine Unix
/// error (e.g. cross-device) is returned as-is and never deletes `to`.
async fn atomic_rename(from: &Path, to: &Path) -> io::Result<()> {
    match fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(_err) if cfg!(windows) && to.exists() => {
            let _ = fs::remove_file(to).await;
            fs::rename(from, to).await
        }
        Err(err) => Err(err),
    }
}

/// fsync the parent directory so the rename entry itself is durable. Without
/// this, a power loss just after the rename can still lose the new directory
/// entry and revert to the old content (never torn, but stale). Best-effort:
/// some platforms disallow opening a directory for fsync.
async fn sync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }
}

/// Write `bytes` to `path` atomically: `create_dir_all` the parent, write+fsync
/// a temp sibling, atomically rename it over `path`, then fsync the directory.
/// If any step before the rename fails, the temp is removed and the existing
/// target is left untouched — a failed write leaves neither corruption nor
/// litter.
pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = unique_temp_path(path);
    if let Err(err) = write_and_sync(&tmp, bytes).await {
        let _ = fs::remove_file(&tmp).await;
        return Err(err);
    }
    if let Err(err) = atomic_rename(&tmp, path).await {
        let _ = fs::remove_file(&tmp).await;
        return Err(err);
    }
    sync_parent_dir(path).await;
    Ok(())
}

/// Best-effort removal of every staged temp. Temps already renamed into place
/// are gone, so `remove_file` is a harmless no-op for those.
async fn cleanup_temps(staged: &[(PathBuf, PathBuf)]) {
    for (tmp, _) in staged {
        let _ = fs::remove_file(tmp).await;
    }
}

/// Commit multiple derived files with *staged* atomicity: every temp is written
/// and fsync'd FIRST, and only once all have staged successfully are they
/// renamed into place. If staging any file fails (serialization was done by the
/// caller, so this is an I/O failure), nothing is published and every staged
/// temp is cleaned up — so a failure partway through a multi-file refresh never
/// leaves a half-updated, mutually-inconsistent index set.
///
/// A crash *between* the renames can still leave some files from the new
/// generation and some from the old, but every file is individually complete
/// (never torn) and the whole set is fully derived from source, so the next
/// refresh regenerates it — the recoverable/rebuildable state #166 asks for. A
/// per-file generation manifest would give strict cross-file atomicity but is
/// over-engineering for indexes that are always rebuildable from the documents.
pub(crate) async fn atomic_write_batch(writes: Vec<(PathBuf, Vec<u8>)>) -> io::Result<()> {
    let mut staged: Vec<(PathBuf, PathBuf)> = Vec::with_capacity(writes.len());

    // Phase 1: stage every temp. On ANY failure, roll back all staged temps so a
    // failed batch leaves neither corruption nor litter.
    for (path, bytes) in &writes {
        if let Some(parent) = path.parent() {
            if let Err(err) = fs::create_dir_all(parent).await {
                cleanup_temps(&staged).await;
                return Err(err);
            }
        }
        let tmp = unique_temp_path(path);
        if let Err(err) = write_and_sync(&tmp, bytes).await {
            let _ = fs::remove_file(&tmp).await;
            cleanup_temps(&staged).await;
            return Err(err);
        }
        staged.push((tmp, path.clone()));
    }

    // Phase 2: publish. A rename of an already-fsync'd temp is the only fallible
    // step left (atomic on Unix); if one fails, sweep the temps not yet renamed
    // (already-renamed ones are gone, so cleanup skips them harmlessly).
    for (idx, (tmp, path)) in staged.iter().enumerate() {
        if let Err(err) = atomic_rename(tmp, path).await {
            cleanup_temps(&staged[idx..]).await;
            return Err(err);
        }
    }

    // Phase 3: fsync each distinct parent directory once.
    let mut synced: Vec<PathBuf> = Vec::new();
    for (_, path) in &staged {
        if let Some(parent) = path.parent() {
            if !synced.iter().any(|p| p.as_path() == parent) {
                sync_parent_dir(path).await;
                synced.push(parent.to_path_buf());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Names of leftover `*.tmp` staging files in `dir` (should always be empty
    /// after a write settles — no litter).
    fn temp_litter(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(".tmp"))
            .collect()
    }

    #[tokio::test]
    async fn atomic_write_creates_file_and_leaves_no_temp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested").join("doc.md");
        atomic_write(&path, b"hello").await.unwrap();
        assert_eq!(fs::read(&path).await.unwrap(), b"hello");
        assert!(temp_litter(path.parent().unwrap()).is_empty());
    }

    #[tokio::test]
    async fn atomic_write_overwrites_existing_without_temp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("doc.md");
        atomic_write(&path, b"v1").await.unwrap();
        atomic_write(&path, b"v2-longer").await.unwrap();
        assert_eq!(fs::read(&path).await.unwrap(), b"v2-longer");
        assert!(temp_litter(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn atomic_write_failure_preserves_target_and_cleans_temp() {
        let dir = tempdir().unwrap();
        // Target is an existing (non-empty) directory: renaming a file over it
        // fails, exercising the rename-error cleanup path.
        let path = dir.path().join("busy");
        fs::create_dir(&path).await.unwrap();
        fs::write(path.join("child"), b"keep").await.unwrap();

        let err = atomic_write(&path, b"payload").await;
        assert!(
            err.is_err(),
            "writing a file over a non-empty dir must fail"
        );
        // The directory (and its child) survive, and no temp is left behind.
        assert!(path.is_dir());
        assert_eq!(fs::read(path.join("child")).await.unwrap(), b"keep");
        assert!(temp_litter(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn atomic_write_batch_commits_all_and_leaves_no_temp() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("idx").join("a.json");
        let b = dir.path().join("idx").join("b.json");
        let c = dir.path().join("views").join("c.md");
        atomic_write_batch(vec![
            (a.clone(), b"{\"a\":1}".to_vec()),
            (b.clone(), b"{\"b\":2}".to_vec()),
            (c.clone(), b"# c".to_vec()),
        ])
        .await
        .unwrap();

        assert_eq!(fs::read(&a).await.unwrap(), b"{\"a\":1}");
        assert_eq!(fs::read(&b).await.unwrap(), b"{\"b\":2}");
        assert_eq!(fs::read(&c).await.unwrap(), b"# c");
        assert!(temp_litter(a.parent().unwrap()).is_empty());
        assert!(temp_litter(c.parent().unwrap()).is_empty());
    }

    #[tokio::test]
    async fn atomic_write_batch_failure_publishes_nothing_and_leaves_no_temp() {
        let dir = tempdir().unwrap();
        let first = dir.path().join("first.json");
        // A pre-existing regular file; the second batch entry's parent IS this
        // file, so `create_dir_all` fails mid-batch after `first` has staged.
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, b"keep").await.unwrap();
        let bad = blocker.join("child.json");

        let result =
            atomic_write_batch(vec![(first.clone(), b"1".to_vec()), (bad, b"2".to_vec())]).await;

        assert!(result.is_err(), "staging under a file-parent must fail");
        // Rollback: the first entry was staged but never published, and no temp
        // is left behind; the pre-existing file is untouched.
        assert!(!first.exists(), "a failed batch must publish nothing");
        assert_eq!(fs::read(&blocker).await.unwrap(), b"keep");
        assert!(temp_litter(dir.path()).is_empty());
    }
}
