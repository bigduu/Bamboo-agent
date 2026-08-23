//! Durable idempotency records for `POST /api/v1/sessions`.
//!
//! Only SHA-256 digests of the caller key and canonical request payload are
//! persisted. In particular, filenames, records, and tracing correlation IDs
//! never contain the raw idempotency key, title, prompt, workspace path, or
//! provider configuration.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

/// A bounded client-visible recovery window lets startup cleanup prune
/// terminal receipts. Pending truth is intentionally exempt. Clients must
/// finish ambiguous-create recovery within this window; see
/// `docs/session-create-idempotency.md`.
pub(crate) const RETENTION_HOURS: i64 = 24;
const STORE_VERSION: u8 = 1;
const LOCK_SHARDS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoredOperationStatus {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredOperationError {
    pub code: String,
    pub message: String,
    pub http_status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionCreateOperationRecord {
    version: u8,
    /// Full digest, repeated inside the record so misplaced/corrupt files fail
    /// closed instead of being accepted under another caller's key.
    pub key_digest: String,
    pub payload_fingerprint: String,
    pub session_id: String,
    pub status: StoredOperationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<StoredOperationError>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Pending reservations never expire: discarding one could let the same
    /// logical action allocate a second UUID after a long outage. Terminal
    /// receipts receive a bounded expiry when they become terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

impl SessionCreateOperationRecord {
    pub(crate) fn pending(
        key_digest: String,
        payload_fingerprint: String,
        session_id: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            version: STORE_VERSION,
            key_digest,
            payload_fingerprint,
            session_id,
            status: StoredOperationStatus::Pending,
            error: None,
            created_at: now,
            updated_at: now,
            expires_at: None,
        }
    }

    pub(crate) fn mark_succeeded(&mut self) {
        self.status = StoredOperationStatus::Succeeded;
        self.error = None;
        self.updated_at = Utc::now();
        self.expires_at = Some(self.updated_at + Duration::hours(RETENTION_HOURS));
    }

    pub(crate) fn mark_failed(&mut self, error: StoredOperationError) {
        self.status = StoredOperationStatus::Failed;
        self.error = Some(error);
        self.updated_at = Utc::now();
        self.expires_at = Some(self.updated_at + Duration::hours(RETENTION_HOURS));
    }

    pub(crate) fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.status != StoredOperationStatus::Pending
            && self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

/// Process-scoped operation store. A fixed shard set bounds lock memory while
/// serializing every concurrent request that hashes to the same key.
pub(crate) struct SessionCreateOperationStore {
    root: PathBuf,
    locks: Vec<Arc<Mutex<()>>>,
}

pub(crate) struct SessionCreateOperationGuard {
    _process: OwnedMutexGuard<()>,
    file: std::fs::File,
}

impl Drop for SessionCreateOperationGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl SessionCreateOperationStore {
    pub(crate) fn new(app_data_dir: &Path) -> Self {
        Self {
            root: app_data_dir.join("session-create-operations").join("v1"),
            locks: (0..LOCK_SHARDS).map(|_| Arc::new(Mutex::new(()))).collect(),
        }
    }

    pub(crate) async fn lock(&self, key_digest: &str) -> io::Result<SessionCreateOperationGuard> {
        validate_digest(key_digest)?;
        let shard = self.lock_shard(key_digest);
        let process = self.locks[shard].clone().lock_owned().await;
        let lock_dir = self.root.join("locks");
        tokio::fs::create_dir_all(&lock_dir).await?;
        // Use the same fixed shard selection across processes. This bounds the
        // lock directory at 64 files while preserving same-key exclusion.
        let lock_path = lock_dir.join(format!("{shard:02}.lock"));
        let file = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(lock_path)?;
            FileExt::lock_exclusive(&file)?;
            Ok::<_, io::Error>(file)
        })
        .await
        .map_err(|error| io::Error::other(format!("join operation lock task: {error}")))??;
        Ok(SessionCreateOperationGuard {
            _process: process,
            file,
        })
    }

    /// Try to acquire the same process + OS claim used by [`Self::lock`]
    /// without waiting. Status lookup uses this to report an actively-owned
    /// pending operation immediately instead of racing its creator's recovery
    /// projections.
    pub(crate) async fn try_lock(
        &self,
        key_digest: &str,
    ) -> io::Result<Option<SessionCreateOperationGuard>> {
        validate_digest(key_digest)?;
        let shard = self.lock_shard(key_digest);
        let process = match self.locks[shard].clone().try_lock_owned() {
            Ok(process) => process,
            Err(_) => return Ok(None),
        };
        let lock_dir = self.root.join("locks");
        tokio::fs::create_dir_all(&lock_dir).await?;
        let lock_path = lock_dir.join(format!("{shard:02}.lock"));
        let file = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(lock_path)?;
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => Ok(Some(file)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|error| io::Error::other(format!("join operation try-lock task: {error}")))??;
        Ok(file.map(|file| SessionCreateOperationGuard {
            _process: process,
            file,
        }))
    }

    fn lock_shard(&self, key_digest: &str) -> usize {
        key_digest
            .as_bytes()
            .iter()
            .take(2)
            .fold(0usize, |acc, byte| acc.wrapping_mul(31) + *byte as usize)
            % self.locks.len()
    }

    #[cfg(test)]
    pub(crate) async fn load(
        &self,
        key_digest: &str,
    ) -> io::Result<Option<SessionCreateOperationRecord>> {
        let Some(record) = self.load_raw(key_digest).await? else {
            return Ok(None);
        };
        // Status reads intentionally do not acquire the POST claim. Do not
        // unlink an expired snapshot here: a concurrent POST may already have
        // replaced it with a new pending generation for the reused key.
        Ok((!record.is_expired(Utc::now())).then_some(record))
    }

    /// Read the durable receipt without applying client-visible expiry. GET
    /// uses this to distinguish an expired known operation from a never-seen
    /// key; only a claimed POST may prune and reuse expired truth.
    pub(crate) async fn load_for_status(
        &self,
        key_digest: &str,
    ) -> io::Result<Option<SessionCreateOperationRecord>> {
        self.load_raw(key_digest).await
    }

    /// Load while the caller owns [`Self::lock`], pruning an expired terminal
    /// receipt without racing a concurrent replacement.
    pub(crate) async fn load_claimed(
        &self,
        key_digest: &str,
    ) -> io::Result<Option<SessionCreateOperationRecord>> {
        let Some(record) = self.load_raw(key_digest).await? else {
            return Ok(None);
        };
        if record.is_expired(Utc::now()) {
            let path = self.path_for(key_digest);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            return Ok(None);
        }
        Ok(Some(record))
    }

    async fn load_raw(&self, key_digest: &str) -> io::Result<Option<SessionCreateOperationRecord>> {
        validate_digest(key_digest)?;
        let path = self.path_for(key_digest);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let record: SessionCreateOperationRecord = serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if record.version != STORE_VERSION || record.key_digest != key_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session-create operation record identity mismatch",
            ));
        }
        Ok(Some(record))
    }

    pub(crate) async fn save(&self, record: &SessionCreateOperationRecord) -> io::Result<()> {
        validate_digest(&record.key_digest)?;
        tokio::fs::create_dir_all(&self.root).await?;
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        atomic_write(&self.path_for(&record.key_digest), &bytes).await
    }

    /// Prune terminal receipts that were already expired when this store was
    /// opened. Candidates are discovered by digest-shaped filenames only,
    /// then re-read while holding the same claim used by POST before deletion.
    /// This makes startup cleanup safe alongside a rolling second process.
    ///
    /// Corrupt records are deliberately retained for manual recovery and
    /// reported only by their non-sensitive digest correlation prefix. Unknown
    /// filenames and atomic-write temporaries are outside this store's
    /// namespace and are ignored without logging their caller-controlled text.
    pub(crate) async fn prune_expired(&self) -> io::Result<usize> {
        let mut entries = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };
        let mut candidates = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(digest) = name.strip_suffix(".json") else {
                continue;
            };
            if validate_digest(digest).is_ok() {
                candidates.push(digest.to_ascii_lowercase());
            }
        }

        let mut deleted = 0;
        for digest in candidates {
            match self.load_raw(&digest).await {
                Ok(Some(record)) if record.is_expired(Utc::now()) => {
                    // Only an observed expired candidate may wait for a claim;
                    // a long-running pending POST must not delay startup.
                    let Some(_claim) = self.try_lock(&digest).await? else {
                        continue;
                    };
                    if self
                        .load_raw(&digest)
                        .await?
                        .is_some_and(|current| current.is_expired(Utc::now()))
                    {
                        match tokio::fs::remove_file(self.path_for(&digest)).await {
                            Ok(()) => deleted += 1,
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    tracing::warn!(
                        target: "bamboo.session_create",
                        correlation_id = correlation_id(&digest),
                        phase = "retention_cleanup",
                        outcome = "corrupt_retained",
                        "retaining corrupt session-create operation receipt for manual recovery"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(deleted)
    }

    fn path_for(&self, key_digest: &str) -> PathBuf {
        self.root.join(format!("{key_digest}.json"))
    }

    #[cfg(test)]
    pub(crate) fn root_for_test(&self) -> &Path {
        &self.root
    }
}

pub(crate) fn validate_key(raw: &str) -> Result<(), &'static str> {
    if raw.is_empty() {
        return Err("Idempotency-Key must not be empty");
    }
    if raw.len() > 128 {
        return Err("Idempotency-Key must be at most 128 bytes");
    }
    if !raw
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(
            "Idempotency-Key may contain only ASCII letters, digits, hyphen, underscore, dot, or colon",
        );
    }
    Ok(())
}

pub(crate) fn key_digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

pub(crate) fn correlation_id(key_digest: &str) -> &str {
    key_digest.get(..16).unwrap_or(key_digest)
}

/// Hash a recursively canonicalized JSON representation. The canonical bytes
/// live only in memory; the durable record contains this digest alone.
pub(crate) fn payload_fingerprint<T: Serialize>(value: &T) -> io::Result<String> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    canonicalize_json(&mut value);
    let bytes = serde_json::to_vec(&value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = std::mem::take(map).into_iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut child) in entries {
                canonicalize_json(&mut child);
                map.insert(key, child);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                canonicalize_json(child);
            }
        }
        _ => {}
    }
}

fn validate_digest(digest: &str) -> io::Result<()> {
    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid session-create operation digest",
        ))
    }
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "operation path has no parent")
    })?;
    tokio::fs::create_dir_all(parent).await?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("record");
    let temp = parent.join(format!(".{file_name}.tmp.{}", Uuid::new_v4()));

    let write_result = async {
        let mut file = tokio::fs::File::create(&temp).await?;
        file.write_all(bytes).await?;
        file.sync_all().await
    }
    .await;
    if let Err(error) = write_result {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    if let Err(error) = bamboo_skills::legacy::atomic_replace_file(&temp, path).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    if let Ok(directory) = tokio::fs::File::open(parent).await {
        let _ = directory.sync_all().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_bounded_path_safe_ascii_keys() {
        assert!(validate_key("create:550e8400-e29b-41d4-a716-446655440000").is_ok());
        assert!(validate_key("").is_err());
        assert!(validate_key("contains space").is_err());
        assert!(validate_key(&"x".repeat(129)).is_err());
    }

    #[test]
    fn canonical_payload_hash_ignores_object_key_order() {
        let left = serde_json::json!({"nested": {"b": 2, "a": 1}});
        let right = serde_json::json!({"nested": {"a": 1, "b": 2}});
        assert_eq!(
            payload_fingerprint(&left).unwrap(),
            payload_fingerprint(&right).unwrap()
        );
    }

    #[tokio::test]
    async fn durable_record_never_contains_raw_key_or_payload() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionCreateOperationStore::new(dir.path());
        let raw_key = "secret-operation-key";
        let raw_title = "private session title";
        let raw_prompt = "private system prompt";
        let raw_gold = "private gold evaluator";
        let raw_workspace = "/private/workspace/path";
        let raw_provider = "private-provider-instance";
        let payload = serde_json::json!({
            "title": raw_title,
            "system_prompt": raw_prompt,
            "gold_config": {"evaluator": raw_gold},
            "workspace_path": raw_workspace,
            "provider": raw_provider,
        });
        let digest = key_digest(raw_key);
        let record = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&payload).unwrap(),
            Uuid::new_v4().to_string(),
        );
        store.save(&record).await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(store.root_for_test())
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .collect();
        assert_eq!(entries.len(), 1);
        let file_name = entries[0].file_name().to_string_lossy().into_owned();
        let contents = std::fs::read_to_string(entries[0].path()).unwrap();
        assert!(!file_name.contains(raw_key));
        assert!(!contents.contains(raw_key));
        for secret in [raw_title, raw_prompt, raw_gold, raw_workspace, raw_provider] {
            assert!(
                !contents.contains(secret),
                "durable operation record exposed request payload: {secret}"
            );
        }
        assert!(file_name.starts_with(&digest));
    }

    #[tokio::test]
    async fn independent_store_instances_serialize_the_same_digest() {
        let dir = tempfile::tempdir().unwrap();
        let first = SessionCreateOperationStore::new(dir.path());
        let second = Arc::new(SessionCreateOperationStore::new(dir.path()));
        let digest = key_digest("cross-process-shape");
        let first_guard = first.lock(&digest).await.unwrap();
        assert!(
            second.try_lock(&digest).await.unwrap().is_none(),
            "status try-lock must never wait behind the active claim"
        );

        let digest_for_second = digest.clone();
        let second_waiter = Arc::clone(&second);
        let waiter = tokio::spawn(async move {
            let _guard = second_waiter.lock(&digest_for_second).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        assert!(
            !waiter.is_finished(),
            "the file lock must block an independent store instance"
        );
        drop(first_guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("second claimant should proceed after unlock")
            .unwrap();
        assert!(second.try_lock(&digest).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn pending_never_expires_but_terminal_receipts_do() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionCreateOperationStore::new(dir.path());
        let digest = key_digest("retention-shape");
        let mut record = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "retained"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        store.save(&record).await.unwrap();
        assert!(store.load(&digest).await.unwrap().is_some());
        assert!(
            record.expires_at.is_none(),
            "pending reservations are durable"
        );

        record.mark_succeeded();
        record.expires_at = Some(Utc::now() - Duration::seconds(1));
        store.save(&record).await.unwrap();
        assert!(store.load(&digest).await.unwrap().is_none());
        assert!(store.path_for(&digest).exists());
        let _claim = store.lock(&digest).await.unwrap();
        assert!(store.load_claimed(&digest).await.unwrap().is_none());
        assert!(!store.path_for(&digest).exists());
    }

    #[tokio::test]
    async fn startup_prune_deletes_unrelated_expired_receipts_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionCreateOperationStore::new(dir.path());

        let expired_digest = key_digest("expired-unrelated-key");
        let mut expired = SessionCreateOperationRecord::pending(
            expired_digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "expired"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        expired.mark_succeeded();
        expired.expires_at = Some(Utc::now() - Duration::seconds(1));
        store.save(&expired).await.unwrap();

        let pending_digest = key_digest("pending-unrelated-key");
        let mut pending = SessionCreateOperationRecord::pending(
            pending_digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "pending"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        // Even malformed historical metadata must not expire pending truth.
        pending.expires_at = Some(Utc::now() - Duration::seconds(1));
        store.save(&pending).await.unwrap();

        let live_digest = key_digest("live-unrelated-key");
        let mut live = SessionCreateOperationRecord::pending(
            live_digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "live"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        live.mark_failed(StoredOperationError {
            code: "safe".to_string(),
            message: "safe".to_string(),
            http_status: 400,
        });
        store.save(&live).await.unwrap();

        let corrupt_digest = key_digest("corrupt-unrelated-key");
        tokio::fs::write(store.path_for(&corrupt_digest), b"not-json")
            .await
            .unwrap();

        assert_eq!(store.prune_expired().await.unwrap(), 1);
        assert!(!store.path_for(&expired_digest).exists());
        assert!(store.path_for(&pending_digest).exists());
        assert!(store.path_for(&live_digest).exists());
        assert!(store.path_for(&corrupt_digest).exists());
    }

    #[tokio::test]
    async fn startup_prune_skips_a_busy_expired_claim_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let owner = SessionCreateOperationStore::new(dir.path());
        let cleaner = SessionCreateOperationStore::new(dir.path());
        let digest = key_digest("busy-expired-startup-key");
        let mut expired = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "busy"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        expired.mark_succeeded();
        expired.expires_at = Some(Utc::now() - Duration::seconds(1));
        owner.save(&expired).await.unwrap();
        let claim = owner.lock(&digest).await.unwrap();

        let deleted = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            cleaner.prune_expired(),
        )
        .await
        .expect("startup cleanup must skip, not wait for, a busy expired claim")
        .unwrap();
        assert_eq!(deleted, 0);
        assert!(owner.path_for(&digest).exists());

        drop(claim);
        assert_eq!(cleaner.prune_expired().await.unwrap(), 1);
        assert!(!owner.path_for(&digest).exists());
    }

    #[tokio::test]
    async fn saving_a_terminal_update_atomically_replaces_pending_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionCreateOperationStore::new(dir.path());
        let digest = key_digest("portable-overwrite");
        let mut record = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "replace"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        store.save(&record).await.unwrap();
        record.mark_succeeded();
        store.save(&record).await.unwrap();

        let latest = store.load(&digest).await.unwrap().unwrap();
        assert_eq!(latest.status, StoredOperationStatus::Succeeded);
        assert!(latest.expires_at.is_some());
    }
}
