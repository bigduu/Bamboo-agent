use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::Notify;

const MAX_TRACKED_SESSIONS: usize = 2_000;
const STABLE_READ_ATTEMPTS: usize = 3;
pub(crate) const MAX_TRACKED_FILE_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadState {
    Unread,
    Stale,
    Fresh,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileSnapshot {
    size_bytes: u64,
    modified_ns: Option<u128>,
    content_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrackedSnapshot {
    Present(FileSnapshot),
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrackedEntry {
    generation: u64,
    snapshot: TrackedSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BaselineAdvance {
    Advanced,
    AlreadyCurrent,
    Conflict,
}

#[derive(Debug, Clone)]
pub(crate) struct MutationSlot {
    session_id: String,
    normalized_path: String,
    session_reads: Arc<Mutex<SessionReads>>,
    generation: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct StableFileRead {
    bytes: Vec<u8>,
    snapshot: FileSnapshot,
}

impl StableFileRead {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedFileRead {
    slot: MutationSlot,
    bytes: Vec<u8>,
}

impl ValidatedFileRead {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn slot(&self) -> &MutationSlot {
        &self.slot
    }
}

#[derive(Debug, Default)]
struct SessionReads {
    files: HashMap<String, TrackedEntry>,
    next_generation: u64,
    last_touched: Option<Instant>,
}

impl SessionReads {
    fn store(&mut self, path: String, snapshot: TrackedSnapshot) {
        self.next_generation = self.next_generation.checked_add(1).unwrap_or(1);
        self.files.insert(
            path,
            TrackedEntry {
                generation: self.next_generation,
                snapshot,
            },
        );
    }
}

fn tracker() -> &'static DashMap<String, Arc<Mutex<SessionReads>>> {
    static TRACKER: OnceLock<DashMap<String, Arc<Mutex<SessionReads>>>> = OnceLock::new();
    TRACKER.get_or_init(DashMap::new)
}

async fn normalize_path(path: &str) -> String {
    let original = PathBuf::from(path);
    if let Ok(canonical) = tokio::fs::canonicalize(&original).await {
        return canonical.to_string_lossy().into_owned();
    }

    // A missing target cannot itself be canonicalized. Canonicalize the nearest
    // existing ancestor and append the unresolved suffix so the key is stable
    // before and after creation (notably `/tmp` -> `/private/tmp` on macOS).
    let mut ancestor: &Path = &original;
    let mut suffix: Vec<OsString> = Vec::new();
    loop {
        if let Ok(canonical) = tokio::fs::canonicalize(ancestor).await {
            let mut normalized = canonical;
            for component in suffix.iter().rev() {
                normalized.push(component);
            }
            return normalized.to_string_lossy().into_owned();
        }

        let Some(name) = ancestor.file_name() else {
            break;
        };
        suffix.push(name.to_os_string());
        let Some(parent) = ancestor.parent() else {
            break;
        };
        ancestor = parent;
    }

    original.to_string_lossy().into_owned()
}

fn modified_ns(metadata: &std::fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn metadata_matches(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.len() == right.len()
        && left.file_type() == right.file_type()
        && modified_ns(left) == modified_ns(right)
}

fn snapshot_from_bytes(metadata: &std::fs::Metadata, bytes: &[u8]) -> FileSnapshot {
    let content_digest = Sha256::digest(bytes).into();

    FileSnapshot {
        size_bytes: metadata.len(),
        modified_ns: modified_ns(metadata),
        content_digest,
    }
}

async fn read_bounded(path: &str, max_bytes: u64) -> io::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = file.take(max_bytes.saturating_add(1));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;

    if bytes.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds maximum tracked size of {max_bytes} bytes"),
        ));
    }

    Ok(bytes)
}

/// Read one coherent version of a path.
///
/// Metadata-only checks miss same-size rewrites and a single `metadata -> read`
/// sequence can pair bytes from one version with metadata from another. Two
/// byte-identical reads bracketed by unchanged metadata give the tracker a
/// content-backed baseline. A writer can still arrive after this function
/// returns; that is safe because the next validation compares both metadata and
/// the digest and rejects the now-stale baseline.
pub(crate) async fn stable_read(path: &str) -> io::Result<StableFileRead> {
    for _ in 0..STABLE_READ_ATTEMPTS {
        let before = tokio::fs::metadata(path).await?;
        if !before.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tracked path is not a regular file",
            ));
        }
        if before.len() > MAX_TRACKED_FILE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file exceeds maximum tracked size of {} bytes",
                    MAX_TRACKED_FILE_SIZE
                ),
            ));
        }

        let first = read_bounded(path, MAX_TRACKED_FILE_SIZE).await?;
        let middle = tokio::fs::metadata(path).await?;
        if !metadata_matches(&before, &middle) || first.len() as u64 != middle.len() {
            continue;
        }

        let second = read_bounded(path, MAX_TRACKED_FILE_SIZE).await?;
        let after = tokio::fs::metadata(path).await?;
        if metadata_matches(&middle, &after)
            && second.len() as u64 == after.len()
            && first == second
        {
            return Ok(StableFileRead {
                snapshot: snapshot_from_bytes(&after, &second),
                bytes: second,
            });
        }
    }

    Err(io::Error::other(
        "file changed while a stable snapshot was being captured",
    ))
}

async fn cleanup_if_needed() {
    let map = tracker();
    if map.len() <= MAX_TRACKED_SESSIONS {
        return;
    }

    let mut oldest: Option<(String, Instant)> = None;
    for entry in map.iter() {
        let key = entry.key().clone();
        let session = entry.value().clone();
        let touched = session.lock().await.last_touched.unwrap_or(Instant::now());
        match oldest {
            Some((_, ts)) if touched >= ts => {}
            _ => oldest = Some((key, touched)),
        }
    }

    if let Some((key, _)) = oldest {
        map.remove(&key);
    }
}

async fn store_snapshot(session_id: &str, path: &str, snapshot: TrackedSnapshot) {
    let normalized = normalize_path(path).await;
    let entry = tracker()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(SessionReads::default())))
        .clone();

    {
        let mut guard = entry.lock().await;
        guard.last_touched = Some(Instant::now());
        guard.store(normalized, snapshot);
    }

    cleanup_if_needed().await;
}

pub(crate) async fn mark_stable_read(session_id: &str, path: &str, stable: &StableFileRead) {
    store_snapshot(
        session_id,
        path,
        TrackedSnapshot::Present(stable.snapshot.clone()),
    )
    .await;
}

pub async fn mark_read(session_id: &str, path: &str) -> io::Result<()> {
    let snapshot = match stable_read(path).await {
        Ok(stable) => TrackedSnapshot::Present(stable.snapshot),
        Err(error) if error.kind() == io::ErrorKind::NotFound => TrackedSnapshot::Missing,
        Err(error) => return Err(error),
    };
    store_snapshot(session_id, path, snapshot).await;
    Ok(())
}

pub async fn has_read(session_id: &str, path: &str) -> bool {
    let normalized = normalize_path(path).await;
    let Some(entry) = tracker().get(session_id).map(|value| value.clone()) else {
        return false;
    };

    let mut guard = entry.lock().await;
    guard.last_touched = Some(Instant::now());
    guard.files.contains_key(&normalized)
}

pub async fn read_state(session_id: &str, path: &str) -> ReadState {
    match read_if_fresh(session_id, path).await {
        Ok(_) => ReadState::Fresh,
        Err(state) => state,
    }
}

pub(crate) async fn read_if_fresh(
    session_id: &str,
    path: &str,
) -> Result<ValidatedFileRead, ReadState> {
    let normalized = normalize_path(path).await;
    let Some(entry) = tracker().get(session_id).map(|value| value.clone()) else {
        return Err(ReadState::Unread);
    };

    let baseline = {
        let mut guard = entry.lock().await;
        guard.last_touched = Some(Instant::now());
        match guard.files.get(&normalized) {
            Some(TrackedEntry {
                generation,
                snapshot: TrackedSnapshot::Present(snapshot),
            }) => (*generation, snapshot.clone()),
            Some(TrackedEntry {
                snapshot: TrackedSnapshot::Missing,
                ..
            }) => return Err(ReadState::Stale),
            None => return Err(ReadState::Unread),
        }
    };

    let current = stable_read(path).await.map_err(|_| ReadState::Stale)?;
    if baseline.1 != current.snapshot {
        return Err(ReadState::Stale);
    }

    Ok(ValidatedFileRead {
        slot: MutationSlot {
            session_id: session_id.to_string(),
            normalized_path: normalized,
            session_reads: entry,
            generation: Some(baseline.0),
        },
        bytes: current.bytes,
    })
}

/// Capture the tracker slot before creating a path. The generation is a CAS
/// token even when the slot is absent, so a concurrent same-session Read cannot
/// be silently overwritten by the writer's post-write baseline update.
pub(crate) async fn capture_write_slot(session_id: &str, path: &str) -> MutationSlot {
    let normalized = normalize_path(path).await;
    let entry = tracker()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(SessionReads::default())))
        .clone();
    let generation = {
        let mut guard = entry.lock().await;
        guard.last_touched = Some(Instant::now());
        guard.files.get(&normalized).map(|value| value.generation)
    };

    cleanup_if_needed().await;
    MutationSlot {
        session_id: session_id.to_string(),
        normalized_path: normalized,
        session_reads: entry,
        generation,
    }
}

/// Advance a session baseline only after the path stably reads back as the
/// exact bytes the tool committed. The pre-mutation slot generation is a CAS
/// token for both existing and newly-created files. If a concurrent Read has
/// already registered the exact verified version, the operation is idempotently
/// successful; any other generation change is a conflict and leaves that newer
/// baseline untouched.
pub(crate) async fn advance_after_verified_write(
    path: &str,
    slot: &MutationSlot,
    expected_bytes: &[u8],
) -> BaselineAdvance {
    let normalized = normalize_path(path).await;
    if slot.normalized_path != normalized {
        return BaselineAdvance::Conflict;
    }

    let Ok(current) = stable_read(path).await else {
        return BaselineAdvance::Conflict;
    };
    if current.bytes != expected_bytes {
        return BaselineAdvance::Conflict;
    }

    #[cfg(test)]
    pause_before_advance_if_requested(slot).await;

    let Some(active_entry) = tracker().get(&slot.session_id).map(|value| value.clone()) else {
        return BaselineAdvance::Conflict;
    };
    if !Arc::ptr_eq(&active_entry, &slot.session_reads) {
        return BaselineAdvance::Conflict;
    }

    let mut guard = slot.session_reads.lock().await;
    guard.last_touched = Some(Instant::now());
    let slot_is_unchanged = match (slot.generation, guard.files.get(&normalized)) {
        (None, None) => true,
        (Some(expected), Some(entry)) => entry.generation == expected,
        _ => false,
    };

    if slot_is_unchanged {
        guard.store(
            normalized,
            TrackedSnapshot::Present(current.snapshot.clone()),
        );
        return BaselineAdvance::Advanced;
    }

    if guard.files.get(&normalized).is_some_and(|entry| {
        matches!(
            &entry.snapshot,
            TrackedSnapshot::Present(snapshot) if snapshot == &current.snapshot
        )
    }) {
        BaselineAdvance::AlreadyCurrent
    } else {
        BaselineAdvance::Conflict
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestAdvancePause {
    session_id: String,
    normalized_path: String,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

#[cfg(test)]
fn test_advance_pauses() -> &'static std::sync::Mutex<Vec<TestAdvancePause>> {
    static PAUSES: OnceLock<std::sync::Mutex<Vec<TestAdvancePause>>> = OnceLock::new();
    PAUSES.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

#[cfg(test)]
pub(crate) async fn pause_next_advance_for_test(
    session_id: &str,
    path: &str,
) -> (Arc<Notify>, Arc<Notify>) {
    let pause = TestAdvancePause {
        session_id: session_id.to_string(),
        normalized_path: normalize_path(path).await,
        reached: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let handles = (pause.reached.clone(), pause.resume.clone());
    test_advance_pauses().lock().unwrap().push(pause);
    handles
}

#[cfg(test)]
async fn pause_before_advance_if_requested(slot: &MutationSlot) {
    let pause = {
        let mut pauses = test_advance_pauses().lock().unwrap();
        pauses
            .iter()
            .position(|pause| {
                pause.session_id == slot.session_id && pause.normalized_path == slot.normalized_path
            })
            .map(|position| pauses.swap_remove(position))
    };

    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_id(label: &str) -> String {
        format!("read-tracker-{label}-{}", uuid::Uuid::new_v4())
    }

    #[tokio::test]
    async fn read_state_transitions_from_fresh_to_stale_after_external_change() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("fresh-stale");

        mark_read(&session, &path).await.unwrap();
        assert!(has_read(&session, &path).await);
        assert_eq!(read_state(&session, &path).await, ReadState::Fresh);

        tokio::fs::write(file.path(), "v2 changed").await.unwrap();
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn same_size_rewrite_is_stale_even_when_mtime_is_restored() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "aaaa").await.unwrap();
        let original_modified = std::fs::metadata(file.path()).unwrap().modified().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("same-size-digest");

        mark_read(&session, &path).await.unwrap();
        tokio::fs::write(file.path(), "bbbb").await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(file.path())
            .unwrap()
            .set_modified(original_modified)
            .unwrap();

        let rewritten = std::fs::metadata(file.path()).unwrap();
        assert_eq!(rewritten.len(), 4);
        assert_eq!(rewritten.modified().unwrap(), original_modified);
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn verified_write_advances_the_exact_previous_baseline() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "before").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("advance");

        mark_read(&session, &path).await.unwrap();
        let validated = read_if_fresh(&session, &path).await.unwrap();
        tokio::fs::write(file.path(), "after").await.unwrap();

        assert_eq!(
            advance_after_verified_write(&path, validated.slot(), b"after").await,
            BaselineAdvance::Advanced
        );
        assert_eq!(read_state(&session, &path).await, ReadState::Fresh);
    }

    #[tokio::test]
    async fn post_write_verification_mismatch_does_not_advance_baseline() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "before").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("verify-mismatch");

        mark_read(&session, &path).await.unwrap();
        let validated = read_if_fresh(&session, &path).await.unwrap();
        tokio::fs::write(file.path(), "external").await.unwrap();

        assert_eq!(
            advance_after_verified_write(&path, validated.slot(), b"intended").await,
            BaselineAdvance::Conflict
        );
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn concurrent_same_session_read_of_verified_version_is_idempotent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "before").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("concurrent-read-cas");

        mark_read(&session, &path).await.unwrap();
        let old_validation = read_if_fresh(&session, &path).await.unwrap();

        tokio::fs::write(file.path(), "after").await.unwrap();
        let concurrent_read = stable_read(&path).await.unwrap();
        mark_stable_read(&session, &path, &concurrent_read).await;

        assert_eq!(
            advance_after_verified_write(&path, old_validation.slot(), b"after").await,
            BaselineAdvance::AlreadyCurrent
        );
        assert_eq!(read_state(&session, &path).await, ReadState::Fresh);
    }

    #[tokio::test]
    async fn concurrent_same_session_read_of_other_version_is_a_conflict() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "before").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("concurrent-other-cas");

        mark_read(&session, &path).await.unwrap();
        let old_validation = read_if_fresh(&session, &path).await.unwrap();

        tokio::fs::write(file.path(), "other").await.unwrap();
        let concurrent_read = stable_read(&path).await.unwrap();
        mark_stable_read(&session, &path, &concurrent_read).await;
        tokio::fs::write(file.path(), "intended").await.unwrap();

        assert_eq!(
            advance_after_verified_write(&path, old_validation.slot(), b"intended").await,
            BaselineAdvance::Conflict
        );
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn missing_file_marked_read_is_treated_as_stale_for_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.txt");
        let path_str = path.to_string_lossy().to_string();
        let session = session_id("missing");

        mark_read(&session, &path_str).await.unwrap();
        assert!(has_read(&session, &path_str).await);
        assert_eq!(read_state(&session, &path_str).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn failed_non_missing_mark_does_not_overwrite_the_prior_baseline() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "v1").await.unwrap();
        let path = file.path().to_string_lossy().to_string();
        let session = session_id("failed-mark");

        mark_read(&session, &path).await.unwrap();
        let normalized = normalize_path(&path).await;
        let entry = tracker().get(&session).unwrap().clone();
        let before_generation = entry.lock().await.files[&normalized].generation;

        std::fs::OpenOptions::new()
            .write(true)
            .open(file.path())
            .unwrap()
            .set_len(MAX_TRACKED_FILE_SIZE + 1)
            .unwrap();
        let error = mark_read(&session, &path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let after_generation = entry.lock().await.files[&normalized].generation;
        assert_eq!(after_generation, before_generation);
        assert_eq!(read_state(&session, &path).await, ReadState::Stale);
    }

    #[tokio::test]
    async fn normalize_path_canonicalizes_real_and_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("real.txt");
        tokio::fs::write(&file_path, "hello").await.unwrap();

        // A real path is canonicalized to the resolved absolute form.
        let raw = file_path.to_string_lossy().to_string();
        let normalized = normalize_path(&raw).await;
        let canonical = tokio::fs::canonicalize(&file_path).await.unwrap();
        assert_eq!(normalized, canonical.to_string_lossy().to_string());

        // A missing target uses its canonical parent plus unresolved suffix.
        let missing = dir.path().join("does_not_exist.txt");
        let missing_str = missing.to_string_lossy().to_string();
        assert_eq!(
            normalize_path(&missing_str).await,
            canonical
                .parent()
                .unwrap()
                .join("does_not_exist.txt")
                .to_string_lossy()
                .into_owned()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_path_key_is_stable_across_creation_through_tmp_alias() {
        let dir = tempfile::Builder::new()
            .prefix("bamboo-read-tracker-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("created.txt");
        let path_str = path.to_string_lossy().to_string();

        let before = normalize_path(&path_str).await;
        tokio::fs::write(&path, "created").await.unwrap();
        let after = normalize_path(&path_str).await;

        assert_eq!(before, after);
    }
}
