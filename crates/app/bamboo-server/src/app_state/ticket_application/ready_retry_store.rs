//! Outer filesystem adapter for retry metadata; never an execution authority.
//! Each blocking transaction retains the same lock through file and directory
//! synchronization, including when its async caller is cancelled.
//! Each transaction also independently locks the stable Host root directory, so
//! replacing and restoring the named lock cannot admit a competing publisher.
//! Clones pin one file descriptor; observed inode/link loss fails closed. Trusted
//! root repair and mixed-version access must drain live writers first.
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use bamboo_orchestration::ready_recovery::{
    validate_ready_retry_record, validate_ready_retry_transition, ReadyRetryKey, ReadyRetryRecord,
    ReadyRetryStore, VersionedReadyRetry,
};
use bamboo_tickets::{canonical_bytes, content_hash, Error, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

// Only the historical schema-1 map is bounded by this count. New stable keys
// use individual bounded records and never evict another Work's retry budget.
const MAX_LEGACY_ENTRIES: usize = 1024;
const MAX_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RECORD_BYTES: u64 = 64 * 1024;
const STATE: &str = "state.json";
const LOCK: &str = "ready-retry-v1.lock";
const RECORDS: &str = "entries";
const MEMBERSHIP: &str = "membership";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyRetryFilePhase {
    BeforeFileSync,
    BeforeReplace,
    AfterReplace,
    BeforeDirectorySync,
    AfterDirectorySync,
    BeforeFormatSync,
    AfterFormatSync,
    BeforeMembershipSync,
    AfterMembershipSync,
}

/// Trusted-host/test injection only; no Worker, JSON command or HTTP setter.
pub type ReadyRetryPublicationFault =
    Arc<dyn Fn(ReadyRetryFilePhase) -> io::Result<()> + Send + Sync>;

#[derive(Clone)]
pub struct FileReadyRetryStore {
    directory: PathBuf,
    fault: Option<ReadyRetryPublicationFault>,
    lock_file: Arc<Mutex<Option<File>>>,
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordState {
    schema: u32,
    key: ReadyRetryKey,
    value: VersionedReadyRetry,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordEnvelope {
    state: RecordState,
    sha256: String,
}

#[derive(Serialize)]
struct Publication<'a, T> {
    state: &'a T,
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

struct TransactionLock<'a> {
    descriptor: MutexGuard<'a, Option<File>>,
    path: PathBuf,
    root: File,
    root_path: PathBuf,
}

impl TransactionLock<'_> {
    fn verify(&self) -> Result<()> {
        let opened_root = self.root.metadata()?;
        let named_root = fs::metadata(&self.root_path)
            .map_err(|_| unavailable("Host root is missing or inaccessible"))?;
        if !opened_root.is_dir() || !named_root.is_dir() {
            return Err(unavailable("Host root is not a directory"));
        }
        let file = self
            .descriptor
            .as_ref()
            .ok_or_else(|| unavailable("retry lock descriptor is unavailable"))?;
        let opened = file.metadata()?;
        let named = fs::symlink_metadata(&self.path)
            .map_err(|_| unavailable("retry lock is missing or inaccessible"))?;
        if !opened.is_file() || named.file_type().is_symlink() || !named.is_file() {
            return Err(unavailable("retry lock is not a plain file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if opened_root.dev() != named_root.dev()
                || opened_root.ino() != named_root.ino()
                || opened_root.nlink() == 0
                || named_root.nlink() == 0
            {
                return Err(unavailable(
                    "Host root identity changed; explicit repair required",
                ));
            }
            if opened.dev() != named.dev()
                || opened.ino() != named.ino()
                || opened.nlink() != 1
                || named.nlink() != 1
            {
                return Err(unavailable(
                    "retry lock identity changed; explicit repair required",
                ));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(unavailable("durable filesystem platform not validated"))
        }
    }
}

impl Drop for TransactionLock<'_> {
    fn drop(&mut self) {
        if let Some(file) = self.descriptor.as_ref() {
            let _ = FileExt::unlock(file);
        }
        let _ = FileExt::unlock(&self.root);
    }
}

impl FileReadyRetryStore {
    /// No file is created until an explicitly installed Host policy reads or
    /// writes retries. Startup never recovers a policy from these bytes.
    pub fn new(data_root: &Path) -> Self {
        Self {
            directory: data_root.join("ready-retry-v1"),
            fault: None,
            lock_file: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_publication_fault(data_root: &Path, fault: ReadyRetryPublicationFault) -> Self {
        Self {
            directory: data_root.join("ready-retry-v1"),
            fault: Some(fault),
            lock_file: Arc::new(Mutex::new(None)),
        }
    }

    fn lock(&self) -> Result<(TransactionLock<'_>, bool)> {
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
        // Clones retain the original descriptor. A process-local guard is
        // necessary because flock on that same descriptor is reentrant.
        let mut descriptor = self.lock_file.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => {
                Error::ResourceBlocked("another Ready retry metadata transaction is active".into())
            }
            std::sync::TryLockError::Poisoned(_) => unavailable("retry lock guard is poisoned"),
        })?;
        // A separately opened directory descriptor is the publication mutex.
        // Its inode does not change when LOCK is removed/replaced/restored.
        // Acquire it before caching a named descriptor: a losing contender must
        // not retain a transient replacement file for its next transaction.
        let root = File::open(parent)?;
        if !root.metadata()?.is_dir() {
            return Err(unavailable("Host root is not a directory"));
        }
        FileExt::try_lock_exclusive(&root).map_err(|_| {
            Error::ResourceBlocked("another Ready retry metadata transaction is active".into())
        })?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(unavailable("retry lock is not a plain file"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if descriptor.is_none() {
            *descriptor = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)?,
            );
        }
        let file = descriptor
            .as_ref()
            .ok_or_else(|| unavailable("retry lock descriptor is unavailable"))?;
        file.try_lock_exclusive().map_err(|_| {
            Error::ResourceBlocked("another Ready retry metadata transaction is active".into())
        })?;
        let lock = TransactionLock {
            descriptor,
            path,
            root,
            root_path: parent.to_path_buf(),
        };
        lock.verify()?;
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
        lock.verify()?;
        Ok((lock, created))
    }

    fn read(&self, freshly_created: bool, lock: &TransactionLock<'_>) -> Result<State> {
        let path = self.directory.join(STATE);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound && freshly_created => {
                let state = State {
                    schema: 1,
                    revision: 0,
                    entries: BTreeMap::new(),
                };
                self.publish(&state, lock)?;
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
        if !matches!(envelope.state.schema, 1..=3)
            || envelope.state.entries.len() > MAX_LEGACY_ENTRIES
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
        if envelope.state.schema >= 2 {
            Self::plain_directory(&self.directory.join(RECORDS))?;
        }
        if envelope.state.schema == 3 {
            Self::plain_directory(&self.directory.join(MEMBERSHIP))?;
        } else if envelope.state.schema == 2 {
            // Format 2 has no independent proof that an absent key is new.
            // Do not scan surviving records and silently bless that absence.
            match fs::symlink_metadata(self.directory.join(MEMBERSHIP)) {
                Ok(_) => {
                    return Err(unavailable(
                        "schema-2 store has unexpected membership; explicit repair required",
                    ))
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        lock.verify()?;
        Ok(envelope.state)
    }

    fn plain_directory(path: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(unavailable(
                "retry record directory is not a plain directory",
            ));
        }
        Ok(())
    }

    fn record_directory(&self, hash: &str) -> PathBuf {
        self.directory.join(RECORDS).join(hash)
    }

    fn key_marker(&self, hash: &str) -> PathBuf {
        self.directory.join(RECORDS).join(format!("{hash}.key"))
    }

    fn membership(&self, hash: &str) -> PathBuf {
        self.directory.join(MEMBERSHIP).join(hash)
    }

    fn membership_exists(&self, hash: &str) -> Result<bool> {
        match fs::symlink_metadata(self.directory.join(MEMBERSHIP)) {
            Ok(_) => Self::plain_directory(&self.directory.join(MEMBERSHIP))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        match fs::symlink_metadata(self.membership(hash)) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != 0 {
                    return Err(unavailable(
                        "protected membership is not an empty plain file",
                    ));
                }
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn key_marker_exists(&self, hash: &str) -> Result<bool> {
        match fs::symlink_metadata(self.key_marker(hash)) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != 0 {
                    return Err(unavailable("retry key marker is not an empty plain file"));
                }
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn record_directory_exists(&self, hash: &str) -> Result<bool> {
        match fs::symlink_metadata(self.record_directory(hash)) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn value(
        &self,
        state: &State,
        key: &ReadyRetryKey,
        hash: &str,
    ) -> Result<Option<VersionedReadyRetry>> {
        let directory_exists = self.record_directory_exists(hash)?;
        let marker_exists = self.key_marker_exists(hash)?;
        let membership_exists = self.membership_exists(hash)?;
        if let Some(entry) = state.entries.get(hash) {
            if directory_exists || marker_exists || membership_exists {
                return Err(unavailable(
                    "duplicate legacy and individual retry identity",
                ));
            }
            return Ok(Some(entry.value.clone()));
        }
        if state.schema == 1 {
            if directory_exists || marker_exists || membership_exists {
                return Err(unavailable(
                    "individual retry record without schema-2 activation",
                ));
            }
            return Ok(None);
        }
        if directory_exists != marker_exists
            || (state.schema == 3 && directory_exists != membership_exists)
        {
            return Err(unavailable(
                "initialized retry membership, marker or directory is missing",
            ));
        }
        if !directory_exists {
            if state.schema == 2 {
                return Err(unavailable(
                    "schema-2 key absence is unproven; explicit trusted repair required",
                ));
            }
            return Ok(None);
        }
        let directory = self.record_directory(hash);
        Self::plain_directory(&directory)?;
        let path = directory.join(STATE);
        // An existing key directory is a durable initialization boundary. A
        // missing/corrupt file is never an absent key or a fresh retry budget.
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_RECORD_BYTES
        {
            return Err(unavailable("invalid or oversized individual retry record"));
        }
        let mut bytes = Vec::new();
        File::open(path)?
            .take(MAX_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(unavailable(
                "individual retry record exceeds its byte limit",
            ));
        }
        let envelope: RecordEnvelope = serde_json::from_slice(&bytes)?;
        if envelope.state.schema != state.schema
            || envelope.state.key != *key
            || key_hash(&envelope.state.key)? != hash
            || envelope.state.value.revision == 0
            || content_hash(&canonical_bytes(&envelope.state)?) != envelope.sha256
        {
            return Err(unavailable(
                "individual retry identity/revision/checksum mismatch",
            ));
        }
        validate_ready_retry_record(&envelope.state.value.record)?;
        Ok(Some(envelope.state.value))
    }

    fn activate_individual_records(
        &self,
        state: &mut State,
        lock: &TransactionLock<'_>,
    ) -> Result<()> {
        if matches!(state.schema, 2 | 3) {
            // A previous activation can have returned an uncertain error after
            // rename but before directory sync. Observing the format alone is
            // not proof that its old-reader rejection is durable. Re-establish
            // that barrier before any individual record may be published.
            self.phase(ReadyRetryFilePhase::BeforeFormatSync, lock)?;
            File::open(self.directory.join(STATE))?.sync_all()?;
            File::open(&self.directory)?.sync_all()?;
            return self.phase(ReadyRetryFilePhase::AfterFormatSync, lock);
        }
        for name in [RECORDS, MEMBERSHIP] {
            let directory = self.directory.join(name);
            match fs::create_dir(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            Self::plain_directory(&directory)?;
        }
        // Also sync an existing directory from an interrupted activation.
        File::open(&self.directory)?.sync_all()?;
        // Both earlier readers reject schema 3 in this same state.json. A
        // separate marker alone could be ignored and refund a stored budget.
        state.schema = 3;
        self.publish(state, lock)
    }

    fn phase(&self, phase: ReadyRetryFilePhase, lock: &TransactionLock<'_>) -> Result<()> {
        lock.verify()?;
        if let Some(fault) = &self.fault {
            fault(phase)?;
        }
        lock.verify()
    }

    fn publish(&self, state: &State, lock: &TransactionLock<'_>) -> Result<()> {
        self.publish_state(&self.directory, state, MAX_BYTES, lock)
    }

    fn publish_state<T: Serialize>(
        &self,
        directory: &Path,
        state: &T,
        limit: u64,
        lock: &TransactionLock<'_>,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(&Publication {
            state,
            sha256: content_hash(&canonical_bytes(state)?),
        })?;
        if bytes.len() as u64 > limit {
            return Err(unavailable("retry metadata byte capacity exhausted"));
        }
        let temporary = directory.join(format!("pending-{}.json", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            self.phase(ReadyRetryFilePhase::BeforeFileSync, lock)?;
            file.sync_all()?;
            drop(file);
            self.phase(ReadyRetryFilePhase::BeforeReplace, lock)?;
            fs::rename(&temporary, directory.join(STATE))?;
            self.phase(ReadyRetryFilePhase::AfterReplace, lock)?;
            self.phase(ReadyRetryFilePhase::BeforeDirectorySync, lock)?;
            File::open(directory)?.sync_all()?;
            self.phase(ReadyRetryFilePhase::AfterDirectorySync, lock)?;
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
        let (lock, created) = self.lock()?;
        let state = self.read(created, &lock)?;
        let value = self.value(&state, key, &hash)?;
        lock.verify()?;
        Ok(value)
    }

    fn exchange_blocking(
        &self,
        key: &ReadyRetryKey,
        expected: Option<u64>,
        next: ReadyRetryRecord,
    ) -> Result<VersionedReadyRetry> {
        validate_ready_retry_record(&next)?;
        let hash = key_hash(key)?;
        let (lock, created) = self.lock()?;
        let mut state = self.read(created, &lock)?;
        let previous = self.value(&state, key, &hash)?;
        lock.verify()?;
        if previous.as_ref().map(|value| value.revision) != expected {
            return Err(Error::RevisionConflict);
        }
        if let Some(previous) = previous.as_ref() {
            validate_ready_retry_transition(&previous.record, &next)?;
        }
        let value = VersionedReadyRetry {
            revision: expected
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| unavailable("retry revision overflow"))?,
            record: next,
        };
        if !state.entries.contains_key(&hash) {
            self.activate_individual_records(&mut state, &lock)?;
            let directory = self.record_directory(&hash);
            if previous.is_none() {
                if state.schema != 3 {
                    return Err(unavailable("new retry keys require protected membership"));
                }
                // This index is outside both per-key artifacts in entries/.
                // It is durable before either artifact or the charge appears;
                // cancellation/error never removes an initialized membership.
                let membership = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(self.membership(&hash))?;
                self.phase(ReadyRetryFilePhase::BeforeMembershipSync, &lock)?;
                membership.sync_all()?;
                drop(membership);
                File::open(self.directory.join(MEMBERSHIP))?.sync_all()?;
                self.phase(ReadyRetryFilePhase::AfterMembershipSync, &lock)?;
                // The permanent sibling marker also detects loss of the whole
                // key directory. Neither marker nor directory is ever cleaned
                // up after an error; a partial initialization must fail closed.
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(self.key_marker(&hash))?
                    .sync_all()?;
                File::open(self.directory.join(RECORDS))?.sync_all()?;
                fs::create_dir(&directory)?;
                File::open(self.directory.join(RECORDS))?.sync_all()?;
            }
            let record = RecordState {
                schema: state.schema,
                key: key.clone(),
                value: value.clone(),
            };
            self.publish_state(&directory, &record, MAX_RECORD_BYTES, &lock)?;
            lock.verify()?;
            return Ok(value);
        }
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
        self.publish(&state, &lock)?;
        lock.verify()?;
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
