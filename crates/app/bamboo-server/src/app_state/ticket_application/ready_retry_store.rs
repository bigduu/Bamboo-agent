//! Outer filesystem adapter for retry metadata; never an execution authority.
//! Each blocking transaction retains the same lock through file and directory
//! synchronization, including when its async caller is cancelled.
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bamboo_orchestration::ready_recovery::{
    validate_ready_retry_record, validate_ready_retry_transition, ReadyRetryKey, ReadyRetryRecord,
    ReadyRetryStore, VersionedReadyRetry,
};
use bamboo_tickets::{canonical_bytes, content_hash, Error, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

const MAX_ENTRIES: usize = 1024;
const MAX_BYTES: u64 = 8 * 1024 * 1024;
const STATE: &str = "state.json";
const LOCK: &str = "ready-retry-v1.lock";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyRetryFilePhase {
    BeforeFileSync,
    BeforeReplace,
    AfterReplace,
    BeforeDirectorySync,
    AfterDirectorySync,
}

/// Trusted-host/test injection only; no Worker, JSON command or HTTP setter.
pub type ReadyRetryPublicationFault =
    Arc<dyn Fn(ReadyRetryFilePhase) -> io::Result<()> + Send + Sync>;

#[derive(Clone)]
pub struct FileReadyRetryStore {
    directory: PathBuf,
    fault: Option<ReadyRetryPublicationFault>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    key: ReadyRetryKey,
    value: VersionedReadyRetry,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    schema: u32,
    revision: u64,
    entries: BTreeMap<String, Entry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    state: State,
    sha256: String,
}

fn unavailable(reason: &str) -> Error {
    Error::AuthorityUnavailable(format!("Ready retry storage: {reason}"))
}

fn key_hash(key: &ReadyRetryKey) -> Result<String> {
    if key.scope_id.is_empty()
        || key.scope_id.len() > 1024
        || key.work_id.is_empty()
        || key.work_id.len() > 256
    {
        return Err(unavailable("invalid stable scope/Work key"));
    }
    Ok(content_hash(&canonical_bytes(key)?))
}

struct TransactionLock(File);
impl Drop for TransactionLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

impl FileReadyRetryStore {
    /// No file is created until an explicitly installed Host policy reads or
    /// writes retries. Startup never recovers a policy from these bytes.
    pub fn new(data_root: &Path) -> Self {
        Self {
            directory: data_root.join("ready-retry-v1"),
            fault: None,
        }
    }

    pub fn with_publication_fault(data_root: &Path, fault: ReadyRetryPublicationFault) -> Self {
        Self {
            directory: data_root.join("ready-retry-v1"),
            fault: Some(fault),
        }
    }

    fn lock(&self) -> Result<(TransactionLock, bool)> {
        if !cfg!(any(target_os = "macos", target_os = "linux")) {
            return Err(unavailable("durable filesystem platform not validated"));
        }
        let parent = self
            .directory
            .parent()
            .ok_or_else(|| unavailable("missing data root"))?;
        // Bootstrap shares the same stable lock as every later transaction.
        // A losing first reader must not leave an uninitializable empty store.
        let path = parent.join(LOCK);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(unavailable("retry lock is not a plain file"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive().map_err(|_| {
            Error::ResourceBlocked("another Ready retry metadata transaction is active".into())
        })?;
        let lock = TransactionLock(file);
        let created = match fs::create_dir(&self.directory) {
            Ok(()) => {
                File::open(parent)?.sync_all()?;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(error.into()),
        };
        let metadata = fs::symlink_metadata(&self.directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(unavailable("retry directory is not a plain directory"));
        }
        Ok((lock, created))
    }

    fn read(&self, freshly_created: bool) -> Result<State> {
        let path = self.directory.join(STATE);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound && freshly_created => {
                let state = State {
                    schema: 1,
                    revision: 0,
                    entries: BTreeMap::new(),
                };
                self.publish(&state)?;
                return Ok(state);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(unavailable(
                    "initialized retry metadata is missing; explicit repair required",
                ))
            }
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_BYTES {
            return Err(unavailable("invalid or oversized retry metadata file"));
        }
        let mut bytes = Vec::new();
        File::open(path)?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(unavailable("retry metadata exceeds its byte limit"));
        }
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        if envelope.state.schema != 1
            || envelope.state.entries.len() > MAX_ENTRIES
            || content_hash(&canonical_bytes(&envelope.state)?) != envelope.sha256
        {
            return Err(unavailable(
                "retry metadata schema/checksum/entry limit mismatch",
            ));
        }
        for (hash, entry) in &envelope.state.entries {
            if key_hash(&entry.key)? != *hash
                || entry.value.revision == 0
                || entry.value.revision > envelope.state.revision
            {
                return Err(unavailable("retry metadata identity/revision mismatch"));
            }
            validate_ready_retry_record(&entry.value.record)?;
        }
        Ok(envelope.state)
    }

    fn phase(&self, phase: ReadyRetryFilePhase) -> Result<()> {
        if let Some(fault) = &self.fault {
            fault(phase)?;
        }
        Ok(())
    }

    fn publish(&self, state: &State) -> Result<()> {
        let sha256 = content_hash(&canonical_bytes(state)?);
        let bytes = serde_json::to_vec(&Envelope {
            state: State {
                schema: state.schema,
                revision: state.revision,
                entries: state
                    .entries
                    .iter()
                    .map(|(key, entry)| {
                        (
                            key.clone(),
                            Entry {
                                key: entry.key.clone(),
                                value: entry.value.clone(),
                            },
                        )
                    })
                    .collect(),
            },
            sha256,
        })?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(unavailable("retry metadata byte capacity exhausted"));
        }
        let temporary = self
            .directory
            .join(format!("pending-{}.json", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            self.phase(ReadyRetryFilePhase::BeforeFileSync)?;
            file.sync_all()?;
            drop(file);
            self.phase(ReadyRetryFilePhase::BeforeReplace)?;
            fs::rename(&temporary, self.directory.join(STATE))?;
            self.phase(ReadyRetryFilePhase::AfterReplace)?;
            self.phase(ReadyRetryFilePhase::BeforeDirectorySync)?;
            File::open(&self.directory)?.sync_all()?;
            self.phase(ReadyRetryFilePhase::AfterDirectorySync)?;
            Ok(())
        })();
        // Never roll back the published record after an uncertain directory
        // sync. Its charged reservation remains authoritative for retry policy.
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result
    }

    fn load_blocking(&self, key: &ReadyRetryKey) -> Result<Option<VersionedReadyRetry>> {
        let hash = key_hash(key)?;
        let (_lock, created) = self.lock()?;
        let state = self.read(created)?;
        Ok(state.entries.get(&hash).map(|entry| entry.value.clone()))
    }

    fn exchange_blocking(
        &self,
        key: &ReadyRetryKey,
        expected: Option<u64>,
        next: ReadyRetryRecord,
    ) -> Result<VersionedReadyRetry> {
        validate_ready_retry_record(&next)?;
        let hash = key_hash(key)?;
        let (_lock, created) = self.lock()?;
        let mut state = self.read(created)?;
        let previous = state.entries.get(&hash);
        if previous.map(|entry| entry.value.revision) != expected {
            return Err(Error::RevisionConflict);
        }
        if let Some(previous) = previous {
            validate_ready_retry_transition(&previous.value.record, &next)?;
        }
        if previous.is_none() && state.entries.len() >= MAX_ENTRIES {
            return Err(unavailable(
                "retry metadata entry capacity exhausted; no live record evicted",
            ));
        }
        let value = VersionedReadyRetry {
            revision: expected
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| unavailable("retry revision overflow"))?,
            record: next,
        };
        state.revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| unavailable("retry store revision overflow"))?;
        state.entries.insert(
            hash,
            Entry {
                key: key.clone(),
                value: value.clone(),
            },
        );
        self.publish(&state)?;
        Ok(value)
    }
}

#[async_trait::async_trait]
impl ReadyRetryStore for FileReadyRetryStore {
    async fn load(&self, key: &ReadyRetryKey) -> Result<Option<VersionedReadyRetry>> {
        let (store, key) = (self.clone(), key.clone());
        tokio::task::spawn_blocking(move || store.load_blocking(&key))
            .await
            .map_err(|error| unavailable(&format!("retry load task failed: {error}")))?
    }
    async fn compare_exchange(
        &self,
        key: &ReadyRetryKey,
        expected: Option<u64>,
        next: ReadyRetryRecord,
    ) -> Result<VersionedReadyRetry> {
        let (store, key) = (self.clone(), key.clone());
        tokio::task::spawn_blocking(move || store.exchange_blocking(&key, expected, next))
            .await
            .map_err(|error| unavailable(&format!("retry CAS task failed: {error}")))?
    }
}
