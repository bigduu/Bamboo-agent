//! Durable, revisioned JSON persistence and immutable live section snapshots.
//!
//! This module is intentionally independent from the compatibility [`crate::Config`]
//! facade. A section owns its file, validation, revision and last-known-good
//! snapshot; callers only publish a new snapshot after the durable commit succeeds.

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_BACKUP_GENERATIONS: usize = 3;

#[derive(Debug, Error)]
pub enum ConfigStoreError {
    #[error("configuration I/O failed")]
    Io(#[from] std::io::Error),
    #[error("configuration document is invalid")]
    Json(#[from] serde_json::Error),
    #[error("configuration revision conflict: expected {expected}, actual {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("configuration validation failed: {0}")]
    Validation(String),
    #[error("configuration watch failed")]
    Watch(#[from] notify::Error),
}

pub type ConfigStoreResult<T> = Result<T, ConfigStoreError>;

/// Low-level atomic file writer shared by versioned stores and legacy JSON
/// sidecars. It serializes writers across processes with an advisory lock.
#[derive(Debug, Clone)]
pub struct AtomicFileStore {
    path: PathBuf,
    sensitive: bool,
    backup_generations: usize,
}

impl AtomicFileStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            sensitive: false,
            backup_generations: DEFAULT_BACKUP_GENERATIONS,
        }
    }

    pub fn sensitive(mut self, sensitive: bool) -> Self {
        self.sensitive = sensitive;
        self
    }

    pub fn backup_generations(mut self, generations: usize) -> Self {
        self.backup_generations = generations;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write_json<T>(&self, value: &T) -> ConfigStoreResult<()>
    where
        T: Serialize + DeserializeOwned,
    {
        let bytes = serde_json::to_vec_pretty(value)?;
        let _lock = self.lock()?;
        self.commit_bytes_locked(&bytes, |previous| {
            let parsed: T = serde_json::from_slice(previous)?;
            Ok(serde_json::to_vec_pretty(&parsed)?)
        })
    }

    pub fn write_bytes_without_backup(&self, bytes: &[u8]) -> ConfigStoreResult<()> {
        let _lock = self.lock()?;
        self.install_bytes(bytes)
    }

    fn lock(&self) -> ConfigStoreResult<FileLock> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        create_dir(parent, self.sensitive)?;
        let lock_path = sibling_suffix(&self.path, "lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        file.lock_exclusive()?;
        Ok(FileLock(file))
    }

    fn commit_bytes_locked<F>(&self, bytes: &[u8], sanitize_previous: F) -> ConfigStoreResult<()>
    where
        F: FnOnce(&[u8]) -> ConfigStoreResult<Vec<u8>>,
    {
        if self.path.exists() {
            let previous = std::fs::read(&self.path)?;
            if let Ok(sanitized) = sanitize_previous(&previous) {
                self.rotate_backups()?;
                self.install_at_with_mode(
                    &self.backup_path(0),
                    &sanitized,
                    existing_file_mode(&self.path),
                )?;
            }
        }
        self.install_bytes(bytes)
    }

    fn install_bytes(&self, bytes: &[u8]) -> ConfigStoreResult<()> {
        self.install_at(&self.path, bytes)
    }

    fn install_at(&self, target: &Path, bytes: &[u8]) -> ConfigStoreResult<()> {
        self.install_at_with_mode(target, bytes, None)
    }

    fn install_at_with_mode(
        &self,
        target: &Path,
        bytes: &[u8],
        inherited_mode: Option<u32>,
    ) -> ConfigStoreResult<()> {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        create_dir(parent, self.sensitive)?;
        #[cfg(unix)]
        let preserved_mode = if self.sensitive {
            None
        } else {
            match (existing_file_mode(target), inherited_mode) {
                (Some(existing), Some(inherited)) => Some(existing & inherited),
                (Some(existing), None) => Some(existing),
                (None, inherited) => inherited,
            }
        };
        #[cfg(not(unix))]
        let _ = inherited_mode;
        let file_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("config.json");
        let temp_path = parent.join(format!(".{file_name}.tmp.{}", Uuid::new_v4()));
        let mut cleanup = TempCleanup(Some(temp_path.clone()));

        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // 0666 lets the process umask choose permissions for a new ordinary
            // file. Sensitive files are always owner-only.
            options.mode(if self.sensitive { 0o600 } else { 0o666 });
        }
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);

        #[cfg(unix)]
        set_file_mode(&temp_path, self.sensitive, preserved_mode)?;

        replace_path(&temp_path, target)?;
        cleanup.0 = None;
        sync_parent(parent)?;
        Ok(())
    }

    fn rotate_backups(&self) -> ConfigStoreResult<()> {
        if self.backup_generations == 0 {
            return Ok(());
        }
        for generation in (1..self.backup_generations).rev() {
            let from = self.backup_path(generation - 1);
            let to = self.backup_path(generation);
            if from.exists() {
                replace_path(&from, &to)?;
            }
        }
        Ok(())
    }

    fn backup_path(&self, generation: usize) -> PathBuf {
        if generation == 0 {
            sibling_suffix(&self.path, "bak")
        } else {
            sibling_suffix(&self.path, &format!("bak.{generation}"))
        }
    }

    fn quarantine_primary_locked(&self, primary: &[u8]) -> ConfigStoreResult<PathBuf> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("config.json");
        let prefix = format!("{file_name}.corrupt.");
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix))
                && std::fs::read(entry.path()).ok().as_deref() == Some(primary)
            {
                return Ok(entry.path());
            }
        }
        let quarantine = sibling_suffix(&self.path, &format!("corrupt.{}", Uuid::new_v4()));
        self.install_at_with_mode(&quarantine, primary, existing_file_mode(&self.path))?;
        Ok(quarantine)
    }
}

struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

struct TempCleanup(Option<PathBuf>);

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RevisionedDocument<T> {
    schema_version: u32,
    revision: u64,
    data: T,
}

#[derive(Debug, Clone)]
pub struct StoredValue<T> {
    pub data: T,
    pub revision: u64,
    /// The exact primary or backup file that supplied `data`.
    pub source_path: PathBuf,
    pub recovered_from_backup: bool,
    pub quarantine_path: Option<PathBuf>,
    /// True when an external edit reused/went behind the live revision and the
    /// store durably rewrote it with a fresh monotonic revision.
    pub normalized_external_revision: bool,
}

/// A versioned JSON store with compare-and-swap writes.
#[derive(Debug, Clone)]
pub struct AtomicJsonStore<T> {
    file: AtomicFileStore,
    schema_version: u32,
    _marker: std::marker::PhantomData<T>,
}

impl<T> AtomicJsonStore<T>
where
    T: Clone + Serialize + DeserializeOwned,
{
    pub fn new(path: impl Into<PathBuf>, schema_version: u32) -> Self {
        Self {
            file: AtomicFileStore::new(path),
            schema_version,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn sensitive(mut self, sensitive: bool) -> Self {
        self.file = self.file.sensitive(sensitive);
        self
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }

    pub fn load(&self) -> ConfigStoreResult<Option<StoredValue<T>>> {
        self.load_validated(|_| Ok(()))
    }

    pub fn load_validated<F>(&self, validate: F) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        let _lock = self.file.lock()?;
        self.load_validated_locked(&validate)
    }

    /// Load a revisioned document while accepting the pre-section-store raw
    /// `T` shape as revision zero. This is intentionally opt-in: callers use
    /// it only while migrating a legacy sidecar in place.
    pub fn load_validated_allowing_unversioned<F>(
        &self,
        validate: F,
    ) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        let _lock = self.file.lock()?;
        self.load_validated_compatible_locked(&validate)
    }

    fn load_validated_compatible_locked<F>(
        &self,
        validate: &F,
    ) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        if !self.path().exists() {
            return Ok(None);
        }
        let primary = std::fs::read(self.path())?;
        match self
            .read_compatible_document_bytes(&primary)
            .and_then(|document| {
                validate(&document.data)
                    .map_err(ConfigStoreError::Validation)
                    .map(|_| document)
            }) {
            Ok(document) => Ok(Some(StoredValue {
                data: document.data,
                revision: document.revision,
                source_path: self.path().to_path_buf(),
                recovered_from_backup: false,
                quarantine_path: None,
                normalized_external_revision: false,
            })),
            Err(primary_error) => {
                let quarantine_path = Some(self.file.quarantine_primary_locked(&primary)?);
                for generation in 0..self.file.backup_generations {
                    let backup = self.file.backup_path(generation);
                    if let Ok(document) = std::fs::read(&backup)
                        .map_err(ConfigStoreError::Io)
                        .and_then(|bytes| self.read_compatible_document_bytes(&bytes))
                        .and_then(|document| {
                            validate(&document.data)
                                .map_err(ConfigStoreError::Validation)
                                .map(|_| document)
                        })
                    {
                        return Ok(Some(StoredValue {
                            data: document.data,
                            revision: document.revision,
                            source_path: backup,
                            recovered_from_backup: true,
                            quarantine_path,
                            normalized_external_revision: false,
                        }));
                    }
                }
                Err(primary_error)
            }
        }
    }

    fn load_validated_locked<F>(&self, validate: &F) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        if !self.path().exists() {
            return Ok(None);
        }
        let primary = std::fs::read(self.path())?;
        match self.read_document_bytes(&primary).and_then(|document| {
            validate(&document.data)
                .map_err(ConfigStoreError::Validation)
                .map(|_| document)
        }) {
            Ok(document) => Ok(Some(StoredValue {
                data: document.data,
                revision: document.revision,
                source_path: self.path().to_path_buf(),
                recovered_from_backup: false,
                quarantine_path: None,
                normalized_external_revision: false,
            })),
            Err(primary_error) => {
                let quarantine_path = Some(self.file.quarantine_primary_locked(&primary)?);
                for generation in 0..self.file.backup_generations {
                    let backup = self.file.backup_path(generation);
                    if let Ok(document) = self.read_document(&backup).and_then(|document| {
                        validate(&document.data)
                            .map_err(ConfigStoreError::Validation)
                            .map(|_| document)
                    }) {
                        return Ok(Some(StoredValue {
                            data: document.data,
                            revision: document.revision,
                            source_path: backup,
                            recovered_from_backup: true,
                            quarantine_path,
                            normalized_external_revision: false,
                        }));
                    }
                }
                Err(primary_error)
            }
        }
    }

    /// Load a candidate for an already-live section. External edits that reuse
    /// or move backwards from the current revision are durably normalized to a
    /// fresh monotonic revision before the candidate is returned.
    pub fn load_validated_for_reload<F>(
        &self,
        current_revision: u64,
        current_data: &T,
        validate: F,
    ) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        let _lock = self.file.lock()?;
        let Some(mut stored) = self.load_validated_locked(&validate)? else {
            return Ok(None);
        };
        if stored.recovered_from_backup {
            return Ok(Some(stored));
        }

        let content_changed =
            serde_json::to_value(&stored.data)? != serde_json::to_value(current_data)?;
        if stored.revision < current_revision
            || (stored.revision == current_revision && content_changed)
        {
            let revision = current_revision.checked_add(1).ok_or_else(|| {
                ConfigStoreError::Validation("revision counter exhausted".to_string())
            })?;
            let document = RevisionedDocument {
                schema_version: self.schema_version,
                revision,
                data: stored.data.clone(),
            };
            let bytes = serde_json::to_vec_pretty(&document)?;
            self.file.commit_bytes_locked(&bytes, |previous| {
                let parsed: RevisionedDocument<T> = serde_json::from_slice(previous)?;
                Ok(serde_json::to_vec_pretty(&parsed)?)
            })?;
            stored.revision = revision;
            stored.normalized_external_revision = true;
        }
        Ok(Some(stored))
    }

    /// Legacy-compatible counterpart of [`Self::load_validated_for_reload`].
    /// A changed raw sidecar is upgraded durably to the revisioned envelope
    /// before it can become the next live snapshot.
    pub fn load_validated_for_reload_allowing_unversioned<F>(
        &self,
        current_revision: u64,
        current_data: &T,
        validate: F,
    ) -> ConfigStoreResult<Option<StoredValue<T>>>
    where
        F: Fn(&T) -> Result<(), String>,
    {
        let _lock = self.file.lock()?;
        let Some(mut stored) = self.load_validated_compatible_locked(&validate)? else {
            return Ok(None);
        };
        if stored.recovered_from_backup {
            return Ok(Some(stored));
        }

        let content_changed =
            serde_json::to_value(&stored.data)? != serde_json::to_value(current_data)?;
        if stored.revision < current_revision
            || (stored.revision == current_revision && content_changed)
        {
            let revision = current_revision.checked_add(1).ok_or_else(|| {
                ConfigStoreError::Validation("revision counter exhausted".to_string())
            })?;
            self.commit_document_locked(revision, stored.data.clone(), true)?;
            stored.revision = revision;
            stored.normalized_external_revision = true;
        }
        Ok(Some(stored))
    }

    pub fn commit<F>(
        &self,
        expected_revision: u64,
        candidate: T,
        validate: F,
    ) -> ConfigStoreResult<u64>
    where
        F: FnOnce(&T) -> Result<(), String>,
    {
        validate(&candidate).map_err(ConfigStoreError::Validation)?;
        let _lock = self.file.lock()?;
        let actual = if self.path().exists() {
            self.read_document(self.path())?.revision
        } else {
            0
        };
        if actual != expected_revision {
            return Err(ConfigStoreError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        let revision = actual.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("revision counter exhausted".to_string())
        })?;
        let document = RevisionedDocument {
            schema_version: self.schema_version,
            revision,
            data: candidate,
        };
        let bytes = serde_json::to_vec_pretty(&document)?;
        self.file.commit_bytes_locked(&bytes, |previous| {
            let parsed: RevisionedDocument<T> = serde_json::from_slice(previous)?;
            Ok(serde_json::to_vec_pretty(&parsed)?)
        })?;
        Ok(revision)
    }

    /// CAS commit that treats an existing pre-envelope raw `T` document as
    /// revision zero and upgrades it atomically on the first successful write.
    pub fn commit_allowing_unversioned<F>(
        &self,
        expected_revision: u64,
        candidate: T,
        validate: F,
    ) -> ConfigStoreResult<u64>
    where
        F: FnOnce(&T) -> Result<(), String>,
    {
        validate(&candidate).map_err(ConfigStoreError::Validation)?;
        let _lock = self.file.lock()?;
        let actual = if self.path().exists() {
            let bytes = std::fs::read(self.path())?;
            self.read_compatible_document_bytes(&bytes)?.revision
        } else {
            0
        };
        if actual != expected_revision {
            return Err(ConfigStoreError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        let revision = actual.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("revision counter exhausted".to_string())
        })?;
        self.commit_document_locked(revision, candidate, true)?;
        Ok(revision)
    }

    fn commit_document_locked(
        &self,
        revision: u64,
        candidate: T,
        allow_unversioned_backup: bool,
    ) -> ConfigStoreResult<()> {
        let document = RevisionedDocument {
            schema_version: self.schema_version,
            revision,
            data: candidate,
        };
        let bytes = serde_json::to_vec_pretty(&document)?;
        self.file.commit_bytes_locked(&bytes, |previous| {
            let parsed = if allow_unversioned_backup {
                self.read_compatible_document_bytes(previous)?
            } else {
                self.read_document_bytes(previous)?
            };
            Ok(serde_json::to_vec_pretty(&parsed)?)
        })?;
        Ok(())
    }

    fn read_document(&self, path: &Path) -> ConfigStoreResult<RevisionedDocument<T>> {
        let bytes = std::fs::read(path)?;
        self.read_document_bytes(&bytes)
    }

    fn read_document_bytes(&self, bytes: &[u8]) -> ConfigStoreResult<RevisionedDocument<T>> {
        let document: RevisionedDocument<T> = serde_json::from_slice(bytes)?;
        if document.schema_version > self.schema_version {
            return Err(ConfigStoreError::Validation(
                "document schema is newer than this runtime".to_string(),
            ));
        }
        Ok(document)
    }

    fn read_compatible_document_bytes(
        &self,
        bytes: &[u8],
    ) -> ConfigStoreResult<RevisionedDocument<T>> {
        let shape: serde_json::Value = serde_json::from_slice(bytes)?;
        let has_envelope_marker = shape.as_object().is_some_and(|object| {
            object.contains_key("schema_version")
                || object.contains_key("revision")
                || object.contains_key("data")
        });
        if has_envelope_marker {
            self.read_document_bytes(bytes)
        } else {
            Ok(RevisionedDocument {
                schema_version: self.schema_version,
                revision: 0,
                data: serde_json::from_slice(bytes)?,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectionStatus {
    Healthy,
    Missing,
    Degraded,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectionSourceKind {
    File,
    Backup,
    Default,
}

#[derive(Debug, Clone)]
pub struct SectionSnapshot<T> {
    pub data: Arc<T>,
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SectionEnvelope<T> {
    pub data: T,
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

impl<T: Clone> SectionSnapshot<T> {
    pub fn envelope(&self) -> SectionEnvelope<T> {
        SectionEnvelope {
            data: self.data.as_ref().clone(),
            revision: self.revision,
            loaded_at: self.loaded_at,
            source_path: self.source_path.clone(),
            source_kind: self.source_kind,
            status: self.status,
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConfigSectionEvent {
    #[serde(rename = "config.changed")]
    Changed { section: String, revision: u64 },
    #[serde(rename = "config.invalid")]
    Invalid { section: String, revision: u64 },
    #[serde(rename = "config.recovered")]
    Recovered { section: String, revision: u64 },
}

type Validator<T> = Arc<dyn Fn(&T) -> Result<(), String> + Send + Sync>;

/// A section's immutable, last-known-good process snapshot.
pub struct LiveSection<T> {
    name: String,
    store: AtomicJsonStore<T>,
    operation_lock: Mutex<()>,
    snapshot: RwLock<Arc<SectionSnapshot<T>>>,
    validate: Validator<T>,
}

impl<T> LiveSection<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn open<F>(
        name: impl Into<String>,
        store: AtomicJsonStore<T>,
        default: T,
        validate: F,
    ) -> ConfigStoreResult<Self>
    where
        F: Fn(&T) -> Result<(), String> + Send + Sync + 'static,
    {
        let validate: Validator<T> = Arc::new(validate);
        let (data, revision, source_kind, status, last_error) =
            match store.load_validated(|value| validate(value)) {
                Ok(Some(stored)) => {
                    let kind = if stored.recovered_from_backup {
                        SectionSourceKind::Backup
                    } else {
                        SectionSourceKind::File
                    };
                    let status = if stored.recovered_from_backup {
                        SectionStatus::Degraded
                    } else {
                        SectionStatus::Healthy
                    };
                    let error = stored.recovered_from_backup.then(|| {
                        "primary document invalid; using last-known-good backup".to_string()
                    });
                    (stored.data, stored.revision, kind, status, error)
                }
                Ok(None) => (
                    default,
                    0,
                    SectionSourceKind::Default,
                    SectionStatus::Missing,
                    None,
                ),
                Err(_) => (
                    default,
                    0,
                    SectionSourceKind::Default,
                    SectionStatus::Invalid,
                    Some("section could not be parsed or read".to_string()),
                ),
            };
        let snapshot = SectionSnapshot {
            data: Arc::new(data),
            revision,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind,
            status,
            last_error,
        };
        Ok(Self {
            name: name.into(),
            store,
            operation_lock: Mutex::new(()),
            snapshot: RwLock::new(Arc::new(snapshot)),
            validate,
        })
    }

    pub fn snapshot(&self) -> Arc<SectionSnapshot<T>> {
        self.snapshot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn commit(
        &self,
        expected_revision: u64,
        candidate: T,
    ) -> ConfigStoreResult<ConfigSectionEvent> {
        self.commit_inner(expected_revision, candidate, || {})
    }

    fn commit_inner<F>(
        &self,
        expected_revision: u64,
        candidate: T,
        before_publish: F,
    ) -> ConfigStoreResult<ConfigSectionEvent>
    where
        F: FnOnce(),
    {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let revision = self
            .store
            .commit(expected_revision, candidate.clone(), |value| {
                (self.validate)(value)
            })?;
        before_publish();
        self.publish(SectionSnapshot {
            data: Arc::new(candidate),
            revision,
            loaded_at: Utc::now(),
            source_path: self.store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Healthy,
            last_error: None,
        });
        Ok(ConfigSectionEvent::Changed {
            section: self.name.clone(),
            revision,
        })
    }

    pub fn reload(&self) -> ConfigSectionEvent {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.snapshot();
        match self.store.load_validated_for_reload(
            current.revision,
            current.data.as_ref(),
            |value| (self.validate)(value),
        ) {
            Ok(Some(stored)) => {
                if stored.recovered_from_backup {
                    self.publish_error(
                        SectionStatus::Degraded,
                        "primary document invalid; retaining last-known-good snapshot",
                    );
                    return ConfigSectionEvent::Invalid {
                        section: self.name.clone(),
                        revision: current.revision,
                    };
                }
                let recovered = current.status != SectionStatus::Healthy;
                let status = SectionStatus::Healthy;
                let source_kind = SectionSourceKind::File;
                self.publish(SectionSnapshot {
                    data: Arc::new(stored.data),
                    revision: stored.revision,
                    loaded_at: Utc::now(),
                    source_path: self.store.path().to_path_buf(),
                    source_kind,
                    status,
                    last_error: None,
                });
                if recovered && status == SectionStatus::Healthy {
                    ConfigSectionEvent::Recovered {
                        section: self.name.clone(),
                        revision: stored.revision,
                    }
                } else {
                    ConfigSectionEvent::Changed {
                        section: self.name.clone(),
                        revision: stored.revision,
                    }
                }
            }
            Ok(None) => {
                self.publish_error(SectionStatus::Missing, "section file is missing");
                ConfigSectionEvent::Invalid {
                    section: self.name.clone(),
                    revision: current.revision,
                }
            }
            _ => {
                self.publish_error(
                    SectionStatus::Invalid,
                    "section could not be parsed or read",
                );
                ConfigSectionEvent::Invalid {
                    section: self.name.clone(),
                    revision: current.revision,
                }
            }
        }
    }

    fn publish_error(&self, status: SectionStatus, message: &str) {
        let current = self.snapshot();
        self.publish(SectionSnapshot {
            data: current.data.clone(),
            revision: current.revision,
            loaded_at: Utc::now(),
            source_path: current.source_path.clone(),
            source_kind: current.source_kind,
            status,
            last_error: Some(message.to_string()),
        });
    }

    fn publish(&self, snapshot: SectionSnapshot<T>) {
        *self
            .snapshot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(snapshot);
    }
}

/// Debounced parent-directory watcher. Temp, lock, backup and quarantine files
/// are filtered; callers map the returned canonical paths to section reloads.
pub struct ConfigDirectoryWatcher {
    _watcher: RecommendedWatcher,
    changes: mpsc::Receiver<Vec<PathBuf>>,
    self_writes: SelfWriteMarkers,
}

/// Cloneable marker used by async section applicators after an internal
/// normalization write. Suppression remains content-exact, so a later external
/// edit of the same path is never hidden.
#[derive(Clone)]
pub struct ConfigSelfWriteMarker {
    self_writes: SelfWriteMarkers,
}

impl ConfigSelfWriteMarker {
    pub fn mark_self_write(&self, path: impl Into<PathBuf>) {
        mark_self_write(&self.self_writes, path.into());
    }
}

type SelfWriteMarker = (Instant, Option<Vec<u8>>);
type SelfWriteMarkers = Arc<Mutex<HashMap<PathBuf, SelfWriteMarker>>>;

impl ConfigDirectoryWatcher {
    pub fn watch(directory: impl AsRef<Path>, debounce: Duration) -> ConfigStoreResult<Self> {
        let (raw_tx, raw_rx) = mpsc::channel::<notify::Result<Event>>();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = raw_tx.send(event);
        })?;
        watcher.watch(directory.as_ref(), RecursiveMode::NonRecursive)?;

        let (changes_tx, changes) = mpsc::channel();
        let self_writes: SelfWriteMarkers = Arc::new(Mutex::new(HashMap::new()));
        let worker_self_writes = Arc::clone(&self_writes);
        std::thread::spawn(move || {
            while let Ok(first) = raw_rx.recv() {
                let mut paths = BTreeSet::new();
                collect_event_paths(first, &mut paths);
                let until = Instant::now() + debounce;
                while let Some(remaining) = until.checked_duration_since(Instant::now()) {
                    match raw_rx.recv_timeout(remaining) {
                        Ok(event) => collect_event_paths(event, &mut paths),
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                }
                paths.retain(|path| !is_internal_store_path(path));
                paths = paths.into_iter().map(normalize_path).collect();
                let mut markers = worker_self_writes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                markers.retain(|_, (marked, _)| marked.elapsed() < Duration::from_secs(2));
                paths.retain(|path| {
                    let Some((_, fingerprint)) = markers.get(path) else {
                        return true;
                    };
                    match (fingerprint, std::fs::read(path).ok()) {
                        (Some(expected), Some(actual)) => expected != &actual,
                        (None, _) => false,
                        _ => true,
                    }
                });
                drop(markers);
                if !paths.is_empty() && changes_tx.send(paths.into_iter().collect()).is_err() {
                    return;
                }
            }
        });

        Ok(Self {
            _watcher: watcher,
            changes,
            self_writes,
        })
    }

    pub fn mark_self_write(&self, path: impl Into<PathBuf>) {
        mark_self_write(&self.self_writes, path.into());
    }

    pub fn self_write_marker(&self) -> ConfigSelfWriteMarker {
        ConfigSelfWriteMarker {
            self_writes: Arc::clone(&self.self_writes),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<PathBuf>, mpsc::RecvTimeoutError> {
        self.changes.recv_timeout(timeout)
    }
}

fn mark_self_write(markers: &SelfWriteMarkers, path: PathBuf) {
    let path = normalize_path(path);
    let fingerprint = std::fs::read(&path).ok();
    markers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(path, (Instant::now(), fingerprint));
}

fn collect_event_paths(event: notify::Result<Event>, paths: &mut BTreeSet<PathBuf>) {
    if let Ok(event) = event {
        paths.extend(event.paths);
    }
}

fn is_internal_store_path(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    name.starts_with('.')
        || name.contains(".tmp.")
        || name.ends_with(".lock")
        || name.contains(".bak")
        || name.contains(".corrupt.")
}

fn normalize_path(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn sibling_suffix(path: &Path, suffix: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.json");
    path.with_file_name(format!("{name}.{suffix}"))
}

fn create_dir(path: &Path, sensitive: bool) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    if sensitive {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn existing_file_mode(path: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions().mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

#[cfg(unix)]
fn set_file_mode(path: &Path, sensitive: bool, preserved_mode: Option<u32>) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if sensitive || preserved_mode.is_some() {
        let mode = if sensitive {
            0o600
        } else {
            preserved_mode.expect("checked above")
        };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

fn replace_path(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        // Same-filesystem rename atomically replaces the destination on Unix.
        std::fs::rename(from, to)
    }
    #[cfg(not(unix))]
    {
        match std::fs::rename(from, to) {
            Ok(()) => Ok(()),
            Err(error) if to.exists() => {
                let file_name = to
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("config.json");
                let displaced =
                    to.with_file_name(format!(".{file_name}.replace-old.{}", Uuid::new_v4()));
                std::fs::rename(to, &displaced)?;
                match std::fs::rename(from, to) {
                    Ok(()) => {
                        let _ = std::fs::remove_file(displaced);
                        Ok(())
                    }
                    Err(_) => {
                        std::fs::rename(displaced, to)?;
                        Err(error)
                    }
                }
            }
            Err(error) => Err(error),
        }
    }
}

fn sync_parent(parent: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Example {
        value: String,
    }

    #[test]
    fn cas_prevents_lost_updates() {
        let dir = TempDir::new().unwrap();
        let store = AtomicJsonStore::new(dir.path().join("example.json"), 1);
        assert_eq!(
            store
                .commit(
                    0,
                    Example {
                        value: "one".into()
                    },
                    |_| Ok(())
                )
                .unwrap(),
            1
        );
        let error = store
            .commit(
                0,
                Example {
                    value: "stale".into(),
                },
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict {
                expected: 0,
                actual: 1
            }
        ));
        assert_eq!(store.load().unwrap().unwrap().data.value, "one");
    }

    #[test]
    fn legacy_revision_zero_has_exactly_one_concurrent_cas_winner() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        std::fs::write(&path, br#"{"value":"legacy"}"#).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut joins = Vec::new();
        for value in ["one", "two"] {
            let store = AtomicJsonStore::new(&path, 1);
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                store.commit_allowing_unversioned(
                    0,
                    Example {
                        value: value.to_string(),
                    },
                    |_| Ok(()),
                )
            }));
        }
        barrier.wait();
        let results = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(ConfigStoreError::Conflict {
                        expected: 0,
                        actual: 1
                    })
                ))
                .count(),
            1
        );
    }

    #[test]
    fn compatible_load_never_downgrades_envelope_errors_to_legacy() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let store: AtomicJsonStore<Example> = AtomicJsonStore::new(&path, 1);

        std::fs::write(
            &path,
            br#"{"schema_version":99,"revision":7,"data":{"value":"future"}}"#,
        )
        .unwrap();
        assert!(matches!(
            store.load_validated_allowing_unversioned(|_| Ok(())),
            Err(ConfigStoreError::Validation(message))
                if message == "document schema is newer than this runtime"
        ));

        std::fs::write(
            &path,
            br#"{"schema_version":1,"data":{"value":"missing-revision"}}"#,
        )
        .unwrap();
        assert!(matches!(
            store.load_validated_allowing_unversioned(|_| Ok(())),
            Err(ConfigStoreError::Json(_))
        ));
    }

    #[test]
    fn invalid_candidate_never_changes_disk_or_snapshot() {
        let dir = TempDir::new().unwrap();
        let section = LiveSection::open(
            "example",
            AtomicJsonStore::new(dir.path().join("example.json"), 1),
            Example {
                value: "default".into(),
            },
            |value| {
                if value.value.is_empty() {
                    Err("empty".to_string())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        section
            .commit(
                0,
                Example {
                    value: "good".into(),
                },
            )
            .unwrap();
        let before = section.snapshot();
        assert!(section
            .commit(
                1,
                Example {
                    value: String::new()
                }
            )
            .is_err());
        let after = section.snapshot();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.data.value, "good");
    }

    #[test]
    fn io_failure_never_publishes_candidate() {
        let dir = TempDir::new().unwrap();
        let blocked_parent = dir.path().join("blocked");
        let section = LiveSection::open(
            "example",
            AtomicJsonStore::new(blocked_parent.join("example.json"), 1),
            Example {
                value: "default".into(),
            },
            |_| Ok(()),
        )
        .unwrap();
        std::fs::remove_file(sibling_suffix(section.store.path(), "lock")).unwrap();
        std::fs::remove_dir(&blocked_parent).unwrap();
        std::fs::write(&blocked_parent, b"not a directory").unwrap();
        assert!(section
            .commit(
                0,
                Example {
                    value: "candidate".into(),
                },
            )
            .is_err());
        let snapshot = section.snapshot();
        assert_eq!(snapshot.revision, 0);
        assert_eq!(snapshot.data.value, "default");
    }

    #[test]
    fn envelope_and_event_have_typed_public_contract() {
        let dir = TempDir::new().unwrap();
        let section = LiveSection::open(
            "providers",
            AtomicJsonStore::new(dir.path().join("providers.json"), 1),
            Example {
                value: "default".into(),
            },
            |_| Ok(()),
        )
        .unwrap();
        let event = section
            .commit(
                0,
                Example {
                    value: "configured".into(),
                },
            )
            .unwrap();
        let event_json = serde_json::to_value(event).unwrap();
        assert_eq!(event_json["type"], "config.changed");
        let envelope = serde_json::to_value(section.snapshot().envelope()).unwrap();
        assert_eq!(envelope["revision"], 1);
        assert_eq!(envelope["status"], "healthy");
        assert_eq!(envelope["data"]["value"], "configured");
    }

    #[test]
    fn corrupt_primary_keeps_last_known_good_and_recovers_after_repair() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let section = LiveSection::open(
            "example",
            AtomicJsonStore::new(&path, 1),
            Example {
                value: "default".into(),
            },
            |_| Ok(()),
        )
        .unwrap();
        section
            .commit(
                0,
                Example {
                    value: "one".into(),
                },
            )
            .unwrap();
        section
            .commit(
                1,
                Example {
                    value: "two".into(),
                },
            )
            .unwrap();
        std::fs::write(&path, b"{broken").unwrap();
        assert!(matches!(
            section.reload(),
            ConfigSectionEvent::Invalid { revision: 2, .. }
        ));
        let degraded = section.snapshot();
        assert_eq!(degraded.status, SectionStatus::Degraded);
        assert_eq!(degraded.revision, 2);
        assert_eq!(degraded.data.value, "two");
        section
            .store
            .file
            .write_bytes_without_backup(
                &serde_json::to_vec_pretty(&RevisionedDocument {
                    schema_version: 1,
                    revision: 3,
                    data: Example {
                        value: "fixed".into(),
                    },
                })
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            section.reload(),
            ConfigSectionEvent::Recovered { revision: 3, .. }
        ));
        assert_eq!(section.snapshot().data.value, "fixed");
    }

    #[test]
    fn repeated_invalid_load_reuses_content_identical_quarantine() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let store = AtomicJsonStore::new(&path, 1);
        store
            .commit(
                0,
                Example {
                    value: "one".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        store
            .commit(
                1,
                Example {
                    value: "two".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        std::fs::write(&path, b"{same broken bytes").unwrap();

        let first = store.load().unwrap().unwrap().quarantine_path.unwrap();
        let second = store.load().unwrap().unwrap().quarantine_path.unwrap();
        assert_eq!(first, second);
        let count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("example.json.corrupt."))
            })
            .count();
        assert_eq!(count, 1);
        assert_eq!(std::fs::read(first).unwrap(), b"{same broken bytes");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(second).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn validated_load_waits_for_the_store_lock() {
        let dir = TempDir::new().unwrap();
        let store = AtomicJsonStore::new(dir.path().join("example.json"), 1);
        store
            .commit(
                0,
                Example {
                    value: "one".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        let held = store.file.lock().unwrap();
        let reader = store.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            tx.send(reader.load().map(|value| value.unwrap().revision))
                .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        drop(held);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(), 1);
        join.join().unwrap();
    }

    #[test]
    fn schema_invalid_primary_recovers_from_validated_backup_on_startup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let store = AtomicJsonStore::new(&path, 1);
        store
            .commit(
                0,
                Example {
                    value: "last-known-good".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        store
            .commit(
                1,
                Example {
                    value: "newer".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        store
            .file
            .write_bytes_without_backup(
                &serde_json::to_vec_pretty(&RevisionedDocument {
                    schema_version: 1,
                    revision: 3,
                    data: Example {
                        value: String::new(),
                    },
                })
                .unwrap(),
            )
            .unwrap();

        let section = LiveSection::open(
            "example",
            store,
            Example {
                value: "default".into(),
            },
            |value| {
                if value.value.is_empty() {
                    Err("empty".to_string())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        let snapshot = section.snapshot();
        assert_eq!(snapshot.status, SectionStatus::Degraded);
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.data.value, "last-known-good");
    }

    #[test]
    fn concurrent_writers_have_one_winner() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let store = AtomicJsonStore::new(&path, 1);
        store
            .commit(
                0,
                Example {
                    value: "base".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        let first = store.clone();
        let second = store.clone();
        let a =
            std::thread::spawn(move || first.commit(1, Example { value: "a".into() }, |_| Ok(())));
        let b =
            std::thread::spawn(move || second.commit(1, Example { value: "b".into() }, |_| Ok(())));
        let results = [a.join().unwrap(), b.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    }

    #[test]
    fn backup_rotation_replaces_existing_oldest_generation() {
        let dir = TempDir::new().unwrap();
        let store = AtomicJsonStore::new(dir.path().join("example.json"), 1);
        for expected in 0..5 {
            store
                .commit(
                    expected,
                    Example {
                        value: format!("value-{}", expected + 1),
                    },
                    |_| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(
            store
                .read_document(&store.file.backup_path(0))
                .unwrap()
                .revision,
            4
        );
        assert_eq!(
            store
                .read_document(&store.file.backup_path(1))
                .unwrap()
                .revision,
            3
        );
        assert_eq!(
            store
                .read_document(&store.file.backup_path(2))
                .unwrap()
                .revision,
            2
        );
    }

    #[test]
    fn external_content_edit_gets_new_durable_revision_and_stales_old_cas() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let store = AtomicJsonStore::new(&path, 1);
        let section = LiveSection::open(
            "example",
            store.clone(),
            Example {
                value: "default".into(),
            },
            |_| Ok(()),
        )
        .unwrap();
        section
            .commit(
                0,
                Example {
                    value: "api".into(),
                },
            )
            .unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&RevisionedDocument {
                schema_version: 1,
                revision: 1,
                data: Example {
                    value: "external".into(),
                },
            })
            .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            section.reload(),
            ConfigSectionEvent::Changed { revision: 2, .. }
        ));
        assert_eq!(section.snapshot().data.value, "external");
        assert_eq!(store.load().unwrap().unwrap().revision, 2);
        assert!(matches!(
            section.commit(
                1,
                Example {
                    value: "stale".into(),
                }
            ),
            Err(ConfigStoreError::Conflict {
                expected: 1,
                actual: 2
            })
        ));
    }

    #[test]
    fn reload_cannot_overtake_commit_between_durable_write_and_publish() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let section = Arc::new(
            LiveSection::open(
                "example",
                AtomicJsonStore::new(&path, 1),
                Example {
                    value: "default".into(),
                },
                |_| Ok(()),
            )
            .unwrap(),
        );
        section
            .commit(
                0,
                Example {
                    value: "initial".into(),
                },
            )
            .unwrap();

        let (durable_tx, durable_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let committing = Arc::clone(&section);
        let commit = std::thread::spawn(move || {
            committing.commit_inner(
                1,
                Example {
                    value: "commit".into(),
                },
                || {
                    durable_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                },
            )
        });
        durable_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Simulate an editor write after commit's durable store operation but
        // before its in-memory publication.
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&RevisionedDocument {
                schema_version: 1,
                revision: 2,
                data: Example {
                    value: "external".into(),
                },
            })
            .unwrap(),
        )
        .unwrap();
        let (reload_tx, reload_rx) = std::sync::mpsc::channel();
        let reloading = Arc::clone(&section);
        let reload = std::thread::spawn(move || {
            reload_tx.send(reloading.reload()).unwrap();
        });
        assert!(
            reload_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "reload must wait until commit has published"
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            commit.join().unwrap().unwrap(),
            ConfigSectionEvent::Changed { revision: 2, .. }
        ));
        assert!(matches!(
            reload_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            ConfigSectionEvent::Changed { revision: 3, .. }
        ));
        reload.join().unwrap();

        let snapshot = section.snapshot();
        let disk = section.store.load().unwrap().unwrap();
        assert_eq!(snapshot.revision, 3);
        assert_eq!(snapshot.revision, disk.revision);
        assert_eq!(snapshot.data.value, "external");
        assert_eq!(snapshot.data.as_ref(), &disk.data);
    }

    #[test]
    fn concurrent_live_commits_publish_only_the_cas_winner() {
        let dir = TempDir::new().unwrap();
        let section = Arc::new(
            LiveSection::open(
                "example",
                AtomicJsonStore::new(dir.path().join("example.json"), 1),
                Example {
                    value: "default".into(),
                },
                |_| Ok(()),
            )
            .unwrap(),
        );
        section
            .commit(
                0,
                Example {
                    value: "initial".into(),
                },
            )
            .unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let spawn = |value: &'static str| {
            let section = Arc::clone(&section);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                (
                    value,
                    section.commit(
                        1,
                        Example {
                            value: value.into(),
                        },
                    ),
                )
            })
        };
        let first = spawn("first");
        let second = spawn("second");
        barrier.wait();
        let results = [first.join().unwrap(), second.join().unwrap()];
        let winner = results
            .iter()
            .find_map(|(value, result)| result.as_ref().ok().map(|_| *value))
            .unwrap();
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_err()).count(),
            1
        );

        let snapshot = section.snapshot();
        let disk = section.store.load().unwrap().unwrap();
        assert_eq!(snapshot.revision, 2);
        assert_eq!(snapshot.revision, disk.revision);
        assert_eq!(snapshot.data.value, winner);
        assert_eq!(snapshot.data.as_ref(), &disk.data);
    }

    #[test]
    fn watcher_coalesces_atomic_rename_and_ignores_self_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let watcher = ConfigDirectoryWatcher::watch(dir.path(), Duration::from_millis(50)).unwrap();
        let temp = dir.path().join(".example.json.editor-tmp");
        std::fs::write(&temp, b"{}\n").unwrap();
        std::fs::rename(temp, &path).unwrap();
        let changed = watcher.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(changed, vec![path.canonicalize().unwrap()]);

        std::fs::write(&path, b"{\"self\":true}\n").unwrap();
        watcher.mark_self_write(&path);
        assert!(watcher.recv_timeout(Duration::from_millis(250)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn sensitive_store_enforces_directory_and_file_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new().unwrap();
        let dir = root.path().join("private");
        let path = dir.join("credentials.json");
        let store = AtomicJsonStore::new(&path, 1).sensitive(true);
        store
            .commit(
                0,
                Example {
                    value: "secret".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_replace_preserves_stricter_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("example.json");
        let mut store = AtomicJsonStore::new(&path, 1);
        store.file.backup_generations = 1;
        store
            .commit(
                0,
                Example {
                    value: "one".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        store
            .commit(
                1,
                Example {
                    value: "two".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(store.file.backup_path(0))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        std::fs::set_permissions(
            store.file.backup_path(0),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        store
            .commit(
                2,
                Example {
                    value: "three".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(
            std::fs::metadata(store.file.backup_path(0))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_new_file_respects_process_umask() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("umask.json");
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(
                "umask 077; exec \"$BAMBOO_CONFIG_TEST_BIN\" --exact \
                 config_store::tests::ordinary_new_file_umask_child",
            )
            .env("BAMBOO_CONFIG_TEST_BIN", std::env::current_exe().unwrap())
            .env("BAMBOO_CONFIG_UMASK_TEST_PATH", &path)
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_new_file_umask_child() {
        let Some(path) = std::env::var_os("BAMBOO_CONFIG_UMASK_TEST_PATH") else {
            return;
        };
        AtomicJsonStore::new(PathBuf::from(path), 1)
            .commit(
                0,
                Example {
                    value: "child".into(),
                },
                |_| Ok(()),
            )
            .unwrap();
    }
}
