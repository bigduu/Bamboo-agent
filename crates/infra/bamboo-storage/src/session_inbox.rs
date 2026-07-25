//! Durable SessionInbox adapter backed by the proven sub-agent Maildir.
//!
//! Every inbox lives beside the authoritative logical session:
//! `sessions/<root>[/children/<child>]/inbox/`. The address is always
//! `Session.id`; the current worker/process/placement never enters the path.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use bamboo_domain::{
    SessionActivationPolicy, SessionInboxBacklog, SessionInboxClaim, SessionInboxError,
    SessionInboxLimits, SessionInboxPort, SessionInboxReceipt, SessionMessageEnvelope,
    SessionMessageId, SessionMessageSource,
};
use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, Mailbox, MsgId};
use base64::Engine;
use chrono::{TimeZone, Utc};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::v2::atomic_write;
use crate::SessionStoreV2;

const INBOX_DIR: &str = "inbox";
const GENERATION_FILE: &str = "generation";
const ACTIVATION_GENERATION_FILE: &str = "activation-generation";
const INTERRUPT_GENERATION_FILE: &str = "interrupt-generation";
const ADMITTED_DIR: &str = "admitted";
const OPERATION_LOCK_FILE: &str = ".session-inbox.lock";

struct FileOperationLock(File);

impl Drop for FileOperationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

/// Filesystem SessionInbox implementation. Clone/share one instance per
/// runtime so concurrent senders serialize only the small generation/backlog
/// transaction for their target session.
#[derive(Clone)]
pub struct FileSessionInbox {
    sessions: Arc<SessionStoreV2>,
    limits: SessionInboxLimits,
    /// Runtime-owned path registry. Clones of this adapter share it, while
    /// independent AppState/SDK runtimes remain fully isolated.
    operation_locks: Arc<Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>>,
}

impl FileSessionInbox {
    pub fn new(sessions: Arc<SessionStoreV2>, limits: SessionInboxLimits) -> Self {
        Self {
            sessions,
            limits,
            operation_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn limits(&self) -> SessionInboxLimits {
        self.limits
    }

    async fn lock_process(&self, dir: &Path) -> OwnedMutexGuard<()> {
        let key = dir.to_path_buf();
        let lock = {
            let mut locks = self
                .operation_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(&key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    locks.insert(key, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    async fn lock_file(dir: &Path) -> Result<FileOperationLock, SessionInboxError> {
        tokio::fs::create_dir_all(dir).await.map_err(|error| {
            SessionInboxError::Storage(format!(
                "create session inbox directory {}: {error}",
                dir.display()
            ))
        })?;
        let path = dir.join(OPERATION_LOCK_FILE);
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|error| {
                    SessionInboxError::Storage(format!(
                        "open session inbox lock {}: {error}",
                        path.display()
                    ))
                })?;
            file.lock_exclusive().map_err(|error| {
                SessionInboxError::Storage(format!(
                    "lock session inbox {}: {error}",
                    path.display()
                ))
            })?;
            Ok(FileOperationLock(file))
        })
        .await
        .map_err(|error| SessionInboxError::Storage(format!("join inbox lock task: {error}")))?
    }

    async fn lock_operation(
        &self,
        dir: &Path,
    ) -> Result<(OwnedMutexGuard<()>, FileOperationLock), SessionInboxError> {
        // Advisory locks do not reliably serialize two file descriptors in one
        // process on every platform. The path-keyed process mutex covers that
        // case; the file lock coordinates separate Bamboo processes.
        let process = self.lock_process(dir).await;
        let file = Self::lock_file(dir).await?;
        Ok((process, file))
    }

    async fn lock_lifecycle(
        &self,
    ) -> Result<crate::v2::SessionLifecycleReadGuard, SessionInboxError> {
        self.sessions
            .lock_session_lifecycle_shared()
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!(
                    "lock session lifecycle for inbox operation: {error}"
                ))
            })
    }

    async fn inbox_dir(&self, session_id: &str) -> Result<PathBuf, SessionInboxError> {
        let rel = self
            .sessions
            .resolve_rel_path(session_id)
            .await
            .ok_or_else(|| SessionInboxError::TargetNotFound(session_id.to_string()))?;
        let session_dir = self.sessions.bamboo_home_dir().join(rel);
        match tokio::fs::try_exists(session_dir.join("session.json")).await {
            Ok(true) => Ok(session_dir.join(INBOX_DIR)),
            Ok(false) => Err(SessionInboxError::TargetNotFound(session_id.to_string())),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "validate SessionInbox target {session_id}: {error}"
            ))),
        }
    }

    async fn read_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        let path = dir.join(GENERATION_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw.trim().parse::<u64>().map_err(|error| {
                SessionInboxError::Storage(format!(
                    "decode inbox generation {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read inbox generation {}: {error}",
                path.display()
            ))),
        }
    }

    async fn next_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        let next = Self::read_generation(dir).await?.saturating_add(1);
        atomic_write(&dir.join(GENERATION_FILE), next.to_string().as_bytes())
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist inbox generation: {error}"))
            })?;
        Ok(next)
    }

    async fn read_activation_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        let path = dir.join(ACTIVATION_GENERATION_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw.trim().parse::<u64>().map_err(|error| {
                SessionInboxError::Storage(format!(
                    "decode inbox activation generation {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read inbox activation generation {}: {error}",
                path.display()
            ))),
        }
    }

    async fn read_interrupt_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        let path = dir.join(INTERRUPT_GENERATION_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw.trim().parse::<u64>().map_err(|error| {
                SessionInboxError::Storage(format!(
                    "decode inbox interrupt generation {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read inbox interrupt generation {}: {error}",
                path.display()
            ))),
        }
    }

    async fn oldest_backlog_generation(dir: &Path) -> Result<Option<u64>, SessionInboxError> {
        let mut oldest = None;
        for queue in ["new", "cur"] {
            for (generation, _, _) in Self::valid_queue_entries(dir, queue).await? {
                oldest = Some(oldest.map_or(generation, |current: u64| current.min(generation)));
            }
        }
        Ok(oldest)
    }

    fn wrapper(envelope: &SessionMessageEnvelope, generation: u64) -> InboxMessage {
        let from = match &envelope.source {
            SessionMessageSource::User => AgentRef {
                session_id: "user".to_string(),
                role: None,
            },
            SessionMessageSource::Session { session_id } => AgentRef {
                session_id: session_id.clone(),
                role: None,
            },
            SessionMessageSource::Runtime { subsystem } => AgentRef {
                session_id: format!("runtime:{subsystem}"),
                role: None,
            },
        };
        // The Maildir filename sorts on this transport timestamp. The original
        // sender timestamp remains intact inside the typed envelope body.
        let transport_time = Utc.timestamp_nanos(generation.min(i64::MAX as u64) as i64);
        InboxMessage {
            // Maildir filenames include MsgId. Hashing keeps the filename well
            // below NAME_MAX even when the accepted logical id is 256 bytes.
            // The original id remains authoritative inside the envelope.
            id: MsgId(format!(
                "sm-{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(Sha256::digest(envelope.id.as_str().as_bytes()))
            )),
            from,
            kind: InboxKind::SessionEnvelope,
            body: serde_json::to_value(envelope).unwrap_or(serde_json::Value::Null),
            created_at: transport_time,
            correlation_id: envelope
                .correlation_id
                .as_ref()
                .map(|value| MsgId(value.clone())),
        }
    }

    fn claim_generation(claim_id: &str) -> Result<u64, SessionInboxError> {
        claim_id
            .split_once('-')
            .and_then(|(prefix, _)| prefix.parse::<u64>().ok())
            .ok_or_else(|| {
                SessionInboxError::InvalidClaim(format!(
                    "claim filename has no ordered generation: {claim_id}"
                ))
            })
    }

    fn admitted_path(dir: &Path, id: &SessionMessageId) -> PathBuf {
        // Fixed-size digest avoids an admitted tombstone filename exceeding
        // NAME_MAX for a valid maximum-length id. The receipt body retains and
        // verifies the original id, so a theoretical hash collision fails
        // closed rather than silently aliasing a receipt.
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(id.as_str().as_bytes()));
        dir.join(ADMITTED_DIR).join(format!("{encoded}.json"))
    }

    /// Digest the idempotency-defining semantics of an envelope.
    ///
    /// `created_at` and `attempt` are deliberately excluded: a crash retry
    /// (notably deterministic legacy-queue migration) may reconstruct those
    /// transport/retry attributes while still representing the same logical
    /// delivery. Target, source, kind, body and correlation/thread edges are
    /// immutable; reusing an id with any of those changed fails closed.
    fn semantic_digest(envelope: &SessionMessageEnvelope) -> Result<String, SessionInboxError> {
        let canonical = bamboo_domain::canonical_json_bytes(&envelope.idempotency_semantics());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(canonical)))
    }

    async fn quarantine_claim(
        dir: &Path,
        claim_path: &Path,
        reason: &str,
    ) -> Result<(), SessionInboxError> {
        let name = claim_path
            .file_name()
            .ok_or_else(|| SessionInboxError::InvalidClaim("claim has no filename".to_string()))?;
        let corrupt_dir = dir.join("corrupt");
        tokio::fs::create_dir_all(&corrupt_dir)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let target = corrupt_dir.join(name);
        match tokio::fs::rename(claim_path, &target).await {
            Ok(()) => {
                tracing::warn!(
                    path = %target.display(),
                    reason,
                    "quarantined malformed typed session inbox envelope"
                );
                Ok(())
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "quarantine malformed claim {}: {error}",
                claim_path.display()
            ))),
        }
    }

    /// Enumerate transport entries with a parseable ordered generation.
    ///
    /// A malformed `*.json` filename is neither a valid backlog item nor a
    /// reason to poison every later delivery/claim. Move it to the durable
    /// corruption quarantine while the inbox operation lock is held.
    async fn valid_queue_entries(
        dir: &Path,
        queue: &str,
    ) -> Result<Vec<(u64, String, PathBuf)>, SessionInboxError> {
        let queue_dir = dir.join(queue);
        let mut reader = match tokio::fs::read_dir(&queue_dir).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(SessionInboxError::Storage(format!(
                    "read SessionInbox queue {}: {error}",
                    queue_dir.display()
                )));
            }
        };
        let mut valid = Vec::new();
        while let Some(entry) = reader.next_entry().await.map_err(|error| {
            SessionInboxError::Storage(format!(
                "scan SessionInbox queue {}: {error}",
                queue_dir.display()
            ))
        })? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || !name.ends_with(".json") {
                continue;
            }
            match Self::claim_generation(&name) {
                Ok(generation) => valid.push((generation, name, entry.path())),
                Err(error) => {
                    Self::quarantine_claim(dir, &entry.path(), &error.to_string()).await?;
                }
            }
        }
        Ok(valid)
    }

    #[cfg(test)]
    async fn count_json(dir: &Path) -> Result<usize, SessionInboxError> {
        let mut reader = match tokio::fs::read_dir(dir).await {
            Ok(reader) => reader,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(SessionInboxError::Storage(format!(
                    "read inbox directory {}: {error}",
                    dir.display()
                )));
            }
        };
        let mut count = 0;
        while let Some(entry) = reader.next_entry().await.map_err(|error| {
            SessionInboxError::Storage(format!("scan inbox directory {}: {error}", dir.display()))
        })? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with('.') && name.ends_with(".json") {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Look up only the permanent admitted tombstone.
    ///
    /// This is deliberately distinct from [`existing_receipt`], which also
    /// scans pending/claimed queues for enqueue idempotency. Ack retry may
    /// succeed after `cur/` disappeared only with this permanent proof.
    async fn admitted_receipt(
        dir: &Path,
        requested: &SessionMessageEnvelope,
    ) -> Result<Option<SessionInboxReceipt>, SessionInboxError> {
        let id = &requested.id;
        let requested_digest = Self::semantic_digest(requested)?;
        let admitted_path = Self::admitted_path(dir, id);
        match tokio::fs::read(&admitted_path).await {
            Ok(bytes) => {
                let receipt: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
                let stored_id = receipt["id"].as_str().ok_or_else(|| {
                    SessionInboxError::Storage(format!(
                        "admitted receipt {} has no message id",
                        admitted_path.display()
                    ))
                })?;
                if stored_id != id.as_str() {
                    return Err(SessionInboxError::InvalidClaim(format!(
                        "admitted receipt digest collision: requested {}, stored {}",
                        id, stored_id
                    )));
                }
                let generation = receipt["generation"].as_u64().ok_or_else(|| {
                    SessionInboxError::Storage(format!(
                        "admitted receipt {} has no generation",
                        admitted_path.display()
                    ))
                })?;
                let stored_digest = receipt["semantic_digest"].as_str().ok_or_else(|| {
                    SessionInboxError::InvalidClaim(format!(
                        "admitted receipt has no semantic digest for {}",
                        id
                    ))
                })?;
                if stored_digest != requested_digest {
                    return Err(SessionInboxError::InvalidClaim(format!(
                        "message id {} was reused with different delivery semantics",
                        id
                    )));
                }
                return Ok(Some(SessionInboxReceipt {
                    id: id.clone(),
                    generation,
                }));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(SessionInboxError::Storage(format!(
                    "read admitted receipt {}: {error}",
                    admitted_path.display()
                )));
            }
        }
        Ok(None)
    }

    async fn existing_receipt(
        dir: &Path,
        requested: &SessionMessageEnvelope,
    ) -> Result<Option<SessionInboxReceipt>, SessionInboxError> {
        if let Some(receipt) = Self::admitted_receipt(dir, requested).await? {
            return Ok(Some(receipt));
        }
        let id = &requested.id;
        let requested_digest = Self::semantic_digest(requested)?;
        for queue in ["new", "cur"] {
            for (generation, _name, path) in Self::valid_queue_entries(dir, queue).await? {
                let Ok(bytes) = tokio::fs::read(path).await else {
                    continue;
                };
                let Ok(wrapper) = serde_json::from_slice::<InboxMessage>(&bytes) else {
                    continue;
                };
                if wrapper.kind != InboxKind::SessionEnvelope {
                    continue;
                }
                let Ok(envelope) = serde_json::from_value::<SessionMessageEnvelope>(wrapper.body)
                else {
                    continue;
                };
                if &envelope.id == id {
                    if Self::semantic_digest(&envelope)? != requested_digest {
                        return Err(SessionInboxError::InvalidClaim(format!(
                            "message id {} was reused with different delivery semantics",
                            id
                        )));
                    }
                    return Ok(Some(SessionInboxReceipt {
                        id: id.clone(),
                        generation,
                    }));
                }
            }
        }
        Ok(None)
    }

    fn validate_claim_name(claim_id: &str) -> Result<(), SessionInboxError> {
        let path = Path::new(claim_id);
        if claim_id.is_empty()
            || path.components().count() != 1
            || claim_id.contains('/')
            || claim_id.contains('\\')
            || !claim_id.ends_with(".json")
        {
            return Err(SessionInboxError::InvalidClaim(claim_id.to_string()));
        }
        Ok(())
    }
}

#[async_trait]
impl SessionInboxPort for FileSessionInbox {
    async fn deliver(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        envelope
            .validate()
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let payload = serde_json::to_vec(envelope)
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        if payload.len() > self.limits.max_payload_bytes {
            return Err(SessionInboxError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.limits.max_payload_bytes,
            });
        }

        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(&envelope.target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        // Enqueue idempotency is independent from consumer admission dedupe.
        // This closes the legacy-migration crash window (deliver succeeded,
        // source clear did not) without letting deterministic retries fill the
        // bounded backlog.
        if let Some(receipt) = Self::existing_receipt(&dir, envelope).await? {
            return Ok(receipt);
        }
        let mailbox = Mailbox::at(&dir);
        let current = Self::valid_queue_entries(&dir, "new").await?.len()
            + Self::valid_queue_entries(&dir, "cur").await?.len();
        if current >= self.limits.max_backlog {
            return Err(SessionInboxError::BacklogFull {
                current,
                limit: self.limits.max_backlog,
            });
        }

        let generation = Self::next_generation(&dir).await?;
        mailbox
            .deliver(&Self::wrapper(envelope, generation))
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        Ok(SessionInboxReceipt {
            id: envelope.id.clone(),
            generation,
        })
    }

    async fn mark_activation_eligible(
        &self,
        target_session_id: &str,
        generation: u64,
        policy: SessionActivationPolicy,
    ) -> Result<(), SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        let delivered_generation = Self::read_generation(&dir).await?;
        if generation == 0 || generation > delivered_generation {
            return Err(SessionInboxError::InvalidClaim(format!(
                "activation generation {generation} is outside delivered range 1..={delivered_generation}"
            )));
        }
        // Publish the interrupt policy before the activation watermark. The
        // two values live in separate atomic files, so a crash may expose
        // `interrupt > activation`; it must never expose a newly eligible
        // explicit steering prefix as RespectSpecificWait.
        if policy == SessionActivationPolicy::InterruptSpecificWait {
            let current_interrupt = Self::read_interrupt_generation(&dir).await?;
            if generation > current_interrupt {
                atomic_write(
                    &dir.join(INTERRUPT_GENERATION_FILE),
                    generation.to_string().as_bytes(),
                )
                .await
                .map_err(|error| {
                    SessionInboxError::Storage(format!(
                        "persist inbox interrupt generation: {error}"
                    ))
                })?;
            }
        }
        let current = Self::read_activation_generation(&dir).await?;
        if generation > current {
            atomic_write(
                &dir.join(ACTIVATION_GENERATION_FILE),
                generation.to_string().as_bytes(),
            )
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist inbox activation generation: {error}"))
            })?;
        }
        Ok(())
    }

    async fn claim(
        &self,
        target_session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        let mailbox = Mailbox::at(&dir);
        mailbox
            .ensure_dirs()
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let activation_generation = Self::read_activation_generation(&dir).await?;
        let limit = limit.min(self.limits.max_claim_batch);
        if activation_generation == 0 || limit == 0 {
            return Ok(Vec::new());
        }

        // Claim only the prefix explicitly authorized by a durable producer
        // watermark. In particular, recovering `cur/` after a crash must not
        // let a newer, merely staged generation hitch a ride on an older
        // activation. Unauthorized files stay exactly where they are.
        let mut eligible = Vec::new();
        for queue in ["cur", "new"] {
            for (generation, name, _) in Self::valid_queue_entries(&dir, queue).await? {
                if generation <= activation_generation {
                    eligible.push((generation, name, queue == "cur"));
                }
            }
        }
        eligible.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                // A recovered canonical claim wins over an impossible duplicate
                // filename in `new/`.
                .then_with(|| right.2.cmp(&left.2))
        });

        let mut claims = Vec::new();
        for (_generation, claim_id, already_claimed) in eligible {
            if claims.len() >= limit {
                break;
            }
            let cur_path = dir.join("cur").join(&claim_id);
            if !already_claimed {
                let new_path = dir.join("new").join(&claim_id);
                match tokio::fs::rename(&new_path, &cur_path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(SessionInboxError::Storage(format!(
                            "claim SessionInbox message {}: {error}",
                            new_path.display()
                        )));
                    }
                }
            }
            let decoded = (|| {
                let bytes = std::fs::read(&cur_path).map_err(|error| error.to_string())?;
                let wrapper = serde_json::from_slice::<InboxMessage>(&bytes)
                    .map_err(|error| error.to_string())?;
                if wrapper.kind != InboxKind::SessionEnvelope {
                    return Err(format!("unexpected inbox kind {:?}", wrapper.kind));
                }
                let generation =
                    Self::claim_generation(&claim_id).map_err(|error| error.to_string())?;
                let envelope = serde_json::from_value::<SessionMessageEnvelope>(wrapper.body)
                    .map_err(|error| error.to_string())?;
                envelope.validate().map_err(|error| error.to_string())?;
                if envelope.target_session_id != target_session_id {
                    return Err(format!(
                        "claim target {} does not match inbox {target_session_id}",
                        envelope.target_session_id
                    ));
                }
                Ok(SessionInboxClaim {
                    envelope,
                    generation,
                    claim_id,
                })
            })();
            match decoded {
                Ok(claim) => claims.push(claim),
                Err(reason) => {
                    Self::quarantine_claim(&dir, &cur_path, &reason).await?;
                }
            }
        }
        Ok(claims)
    }

    async fn was_admitted(
        &self,
        target_session_id: &str,
        id: &SessionMessageId,
    ) -> Result<bool, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let path = Self::admitted_path(&dir, id);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let receipt: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
                let stored_id = receipt["id"].as_str().ok_or_else(|| {
                    SessionInboxError::Storage(format!(
                        "admitted receipt {} has no message id",
                        path.display()
                    ))
                })?;
                if stored_id != id.as_str() {
                    return Err(SessionInboxError::InvalidClaim(format!(
                        "admitted receipt digest collision: requested {}, stored {}",
                        id, stored_id
                    )));
                }
                Ok(true)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read admitted receipt {}: {error}",
                path.display()
            ))),
        }
    }

    async fn ack(
        &self,
        target_session_id: &str,
        claim: &SessionInboxClaim,
    ) -> Result<(), SessionInboxError> {
        Self::validate_claim_name(&claim.claim_id)?;
        if claim.envelope.target_session_id != target_session_id {
            return Err(SessionInboxError::InvalidClaim(
                "claim target mismatch".to_string(),
            ));
        }
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        let cur_path = dir.join("cur").join(&claim.claim_id);
        let bytes = match tokio::fs::read(&cur_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Idempotent retry only when the exact permanent receipt is
                // already present. A missing claim without that proof is stale.
                return match Self::admitted_receipt(&dir, &claim.envelope).await? {
                    Some(receipt) if receipt.generation == claim.generation => Ok(()),
                    _ => Err(SessionInboxError::InvalidClaim(format!(
                        "canonical claim no longer exists: {}",
                        claim.claim_id
                    ))),
                };
            }
            Err(error) => {
                return Err(SessionInboxError::Storage(format!(
                    "read claimed message {}: {error}",
                    cur_path.display()
                )));
            }
        };
        let wrapper: InboxMessage = serde_json::from_slice(&bytes).map_err(|error| {
            SessionInboxError::InvalidClaim(format!(
                "decode canonical claim {}: {error}",
                claim.claim_id
            ))
        })?;
        if wrapper.kind != InboxKind::SessionEnvelope {
            return Err(SessionInboxError::InvalidClaim(format!(
                "canonical claim {} has kind {:?}",
                claim.claim_id, wrapper.kind
            )));
        }
        let persisted: SessionMessageEnvelope =
            serde_json::from_value(wrapper.body).map_err(|error| {
                SessionInboxError::InvalidClaim(format!(
                    "decode canonical envelope {}: {error}",
                    claim.claim_id
                ))
            })?;
        let filename_generation = Self::claim_generation(&claim.claim_id)?;
        if filename_generation != claim.generation
            || persisted.id != claim.envelope.id
            || persisted.target_session_id != target_session_id
            || persisted != claim.envelope
        {
            return Err(SessionInboxError::InvalidClaim(format!(
                "canonical claim mismatch for {}",
                claim.claim_id
            )));
        }

        let admitted_path = Self::admitted_path(&dir, &claim.envelope.id);
        if let Some(existing) = Self::admitted_receipt(&dir, &claim.envelope).await? {
            if existing.generation != claim.generation {
                return Err(SessionInboxError::InvalidClaim(format!(
                    "admitted receipt generation mismatch for {}",
                    claim.envelope.id
                )));
            }
        }
        let receipt = serde_json::to_vec_pretty(&serde_json::json!({
            "id": claim.envelope.id,
            "generation": claim.generation,
            "semantic_digest": Self::semantic_digest(&claim.envelope)?,
            "admitted_at": Utc::now(),
        }))
        .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let admitted_dir = admitted_path.parent().ok_or_else(|| {
            SessionInboxError::Storage(format!(
                "admitted receipt has no parent: {}",
                admitted_path.display()
            ))
        })?;
        tokio::fs::create_dir_all(admitted_dir)
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!(
                    "create admitted receipt directory {}: {error}",
                    admitted_dir.display()
                ))
            })?;
        atomic_write(&admitted_path, &receipt)
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist admitted receipt: {error}"))
            })?;

        match tokio::fs::remove_file(&cur_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "remove claimed message {}: {error}",
                cur_path.display()
            ))),
        }
    }

    async fn inspect(
        &self,
        target_session_id: &str,
    ) -> Result<SessionInboxBacklog, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        let pending = Self::valid_queue_entries(&dir, "new").await?.len();
        let claimed = Self::valid_queue_entries(&dir, "cur").await?.len();
        Ok(SessionInboxBacklog {
            pending,
            claimed,
            generation: Self::read_generation(&dir).await?,
            activation_generation: Self::read_activation_generation(&dir).await?,
            interrupt_generation: Self::read_interrupt_generation(&dir).await?,
            oldest_generation: Self::oldest_backlog_generation(&dir).await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{Session, Storage};
    use tempfile::TempDir;

    async fn fixture(
        limits: SessionInboxLimits,
    ) -> (TempDir, Arc<SessionStoreV2>, FileSessionInbox) {
        let temp = TempDir::new().unwrap();
        let sessions = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        sessions
            .save_session(&Session::new("session-1", "model"))
            .await
            .unwrap();
        let inbox = FileSessionInbox::new(sessions.clone(), limits);
        (temp, sessions, inbox)
    }

    async fn authorize_latest(inbox: &FileSessionInbox) {
        let generation = inbox.inspect("session-1").await.unwrap().generation;
        inbox
            .mark_activation_eligible(
                "session-1",
                generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_delivery_is_ordered_and_survives_reopen() {
        let (_temp, sessions, _fixture_inbox) = fixture(SessionInboxLimits::default()).await;
        // Construct independent adapters over the same durable store. They do
        // not share the runtime-owned mutex registry, so this exercises the
        // per-session file lock that coordinates separate runtime adapters (and
        // separate processes), not merely Arc clones of one adapter.
        let first = Arc::new(FileSessionInbox::new(
            sessions.clone(),
            SessionInboxLimits::default(),
        ));
        let second = Arc::new(FileSessionInbox::new(
            sessions.clone(),
            SessionInboxLimits::default(),
        ));
        let mut tasks = Vec::new();
        for index in 0..40 {
            let inbox = if index % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            tasks.push(tokio::spawn(async move {
                let mut envelope =
                    SessionMessageEnvelope::user_input("session-1", format!("message-{index}"));
                envelope.id = SessionMessageId::parse(format!("id-{index}")).unwrap();
                inbox.deliver(&envelope).await.unwrap()
            }));
        }
        let mut receipts = Vec::new();
        for task in tasks {
            receipts.push(task.await.unwrap());
        }
        receipts.sort_by_key(|receipt| receipt.generation);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.generation)
                .collect::<Vec<_>>(),
            (1..=40).collect::<Vec<_>>()
        );
        second
            .mark_activation_eligible(
                "session-1",
                40,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();

        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        let claims = reopened.claim("session-1", 100).await.unwrap();
        assert_eq!(claims.len(), 40);
        assert_eq!(
            claims
                .iter()
                .map(|claim| claim.generation)
                .collect::<Vec<_>>(),
            (1..=40).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn activation_watermark_is_monotonic_and_tracks_oldest_backlog_across_reopen() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let first = SessionMessageEnvelope::user_input("session-1", "first");
        let second = SessionMessageEnvelope::user_input("session-1", "second");
        let first_receipt = inbox.deliver(&first).await.unwrap();
        let second_receipt = inbox.deliver(&second).await.unwrap();

        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending, 2);
        assert_eq!(backlog.claimed, 0);
        assert_eq!(backlog.oldest_generation, Some(first_receipt.generation));
        assert_eq!(backlog.activation_generation, 0);
        assert!(!backlog.activation_pending());

        inbox
            .mark_activation_eligible(
                "session-1",
                first_receipt.generation,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .unwrap();
        assert!(inbox
            .inspect("session-1")
            .await
            .unwrap()
            .activation_pending());

        // Only the authorized prefix moves to `cur`; the newer staged item
        // remains inert in `new/`, including across reopen.
        let first_claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        let claimed = inbox.inspect("session-1").await.unwrap();
        assert_eq!(claimed.pending, 1);
        assert_eq!(claimed.claimed, 1);
        assert_eq!(claimed.oldest_generation, Some(first_receipt.generation));
        assert!(claimed.activation_pending());

        inbox.ack("session-1", &first_claim).await.unwrap();
        let after_ack = inbox.inspect("session-1").await.unwrap();
        assert_eq!(after_ack.pending, 1);
        assert_eq!(after_ack.claimed, 0);
        assert_eq!(after_ack.oldest_generation, Some(second_receipt.generation));
        assert_eq!(after_ack.activation_generation, first_receipt.generation);
        assert!(
            !after_ack.activation_pending(),
            "a stale activation watermark cannot wake a newer staged item"
        );
        assert!(inbox.claim("session-1", 1).await.unwrap().is_empty());

        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        let reopened_backlog = reopened.inspect("session-1").await.unwrap();
        assert_eq!(reopened_backlog, after_ack);
        assert!(reopened.claim("session-1", 1).await.unwrap().is_empty());

        // Lower/equal retries are idempotent and cannot move the watermark
        // backward; authorizing the newer prefix makes the remaining claim
        // restart-eligible.
        reopened
            .mark_activation_eligible(
                "session-1",
                first_receipt.generation,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .unwrap();
        assert_eq!(
            reopened
                .inspect("session-1")
                .await
                .unwrap()
                .activation_generation,
            first_receipt.generation
        );
        reopened
            .mark_activation_eligible(
                "session-1",
                second_receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let eligible = reopened.inspect("session-1").await.unwrap();
        assert_eq!(eligible.activation_generation, second_receipt.generation);
        assert!(eligible.activation_pending());
        assert_eq!(eligible.interrupt_generation, second_receipt.generation);
        assert!(eligible.interrupt_pending());
        let second_claim = reopened.claim("session-1", 1).await.unwrap().remove(0);
        assert_eq!(second_claim.generation, second_receipt.generation);
        assert!(reopened
            .mark_activation_eligible(
                "session-1",
                second_receipt.generation + 1,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn interrupt_policy_is_durable_before_activation_publish_failure() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let receipt = inbox
            .deliver(&SessionMessageEnvelope::user_input(
                "session-1",
                "explicit steering",
            ))
            .await
            .unwrap();
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let activation_path = dir.join(ACTIVATION_GENERATION_FILE);

        // Force the second atomic publication to fail. The first publication
        // (interrupt policy) must already be durable, while no readable
        // activation watermark is exposed to a restarting process.
        tokio::fs::create_dir(&activation_path).await.unwrap();
        assert!(inbox
            .mark_activation_eligible(
                "session-1",
                receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .is_err());
        assert_eq!(
            FileSessionInbox::read_interrupt_generation(&dir)
                .await
                .unwrap(),
            receipt.generation
        );
        assert!(
            !activation_path.is_file(),
            "a failed activation publish must not expose a downgraded prefix"
        );

        // Restart/retry completes publication. Any visible activation prefix
        // now has an interrupt watermark at least as new.
        tokio::fs::remove_dir(&activation_path).await.unwrap();
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        reopened
            .mark_activation_eligible(
                "session-1",
                receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let backlog = reopened.inspect("session-1").await.unwrap();
        assert_eq!(backlog.activation_generation, receipt.generation);
        assert!(backlog.interrupt_generation >= backlog.activation_generation);
        assert!(backlog.interrupt_pending());
    }

    #[tokio::test]
    async fn admitted_receipt_is_permanent_across_restart_and_duplicate_filename() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "first");
        envelope.id = SessionMessageId::parse("same-id").unwrap();
        inbox.deliver(&envelope).await.unwrap();
        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        inbox.ack("session-1", &claim).await.unwrap();

        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        assert!(reopened
            .was_admitted("session-1", &envelope.id)
            .await
            .unwrap());
        envelope.created_at = Utc::now();
        let duplicate_receipt = reopened.deliver(&envelope).await.unwrap();
        assert_eq!(duplicate_receipt.generation, claim.generation);
        assert!(reopened.claim("session-1", 1).await.unwrap().is_empty());
        assert_eq!(reopened.inspect("session-1").await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn repeated_delivery_is_idempotent_before_claim_and_after_ack() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "same");
        envelope.id = SessionMessageId::parse("stable-id").unwrap();

        let first = inbox.deliver(&envelope).await.unwrap();
        let second = inbox.deliver(&envelope).await.unwrap();
        assert_eq!(first, second);
        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending, 1);
        assert_eq!(backlog.generation, 1);

        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        inbox.ack("session-1", &claim).await.unwrap();
        let third = inbox.deliver(&envelope).await.unwrap();
        assert_eq!(third, first);
        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        assert_eq!(backlog.generation, 1);
    }

    #[tokio::test]
    async fn reordered_nested_json_uses_the_same_semantic_receipt() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut first_inner = serde_json::Map::new();
        first_inner.insert("z".to_string(), serde_json::json!({"b": 2, "a": 1}));
        first_inner.insert("a".to_string(), serde_json::json!([{"y": 2, "x": 1}]));
        let mut second_nested = serde_json::Map::new();
        second_nested.insert("x".to_string(), serde_json::json!(1));
        second_nested.insert("y".to_string(), serde_json::json!(2));
        let mut second_inner = serde_json::Map::new();
        second_inner.insert(
            "a".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::Object(second_nested)]),
        );
        second_inner.insert("z".to_string(), serde_json::json!({"a": 1, "b": 2}));

        let make = |data| SessionMessageEnvelope {
            id: SessionMessageId::parse("nested-canonical-id").unwrap(),
            source: SessionMessageSource::Runtime {
                subsystem: "canonical-test".to_string(),
            },
            target_session_id: "session-1".to_string(),
            kind: bamboo_domain::SessionMessageKind::RuntimeInstruction,
            body: bamboo_domain::SessionMessageBody::RuntimeInstruction(
                bamboo_domain::SessionRuntimeInstruction {
                    instruction: "nested".to_string(),
                    content: None,
                    data: Some(data),
                    provider_message: None,
                },
            ),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: Some("canonical-json".to_string()),
        };
        let first = make(serde_json::Value::Object(first_inner));
        let mut second = make(serde_json::Value::Object(second_inner));
        second.created_at = Utc::now();
        second.attempt = Some(9);

        let first_receipt = inbox.deliver(&first).await.unwrap();
        let retry_receipt = inbox.deliver(&second).await.unwrap();
        assert_eq!(retry_receipt, first_receipt);
        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.generation, 1);
        assert_eq!(backlog.pending, 1);
    }

    #[tokio::test]
    async fn reused_id_with_changed_semantics_fails_before_claim_and_after_ack_restart() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut original = SessionMessageEnvelope::user_input("session-1", "original");
        original.id = SessionMessageId::parse("semantic-id").unwrap();
        inbox.deliver(&original).await.unwrap();

        let mut changed = original.clone();
        changed.body = bamboo_domain::SessionMessageBody::Content(
            bamboo_domain::SessionMessageContent::text("different"),
        );
        assert!(matches!(
            inbox.deliver(&changed).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        assert_eq!(inbox.inspect("session-1").await.unwrap().pending, 1);

        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        inbox.ack("session-1", &claim).await.unwrap();
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        assert!(matches!(
            reopened.deliver(&changed).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        assert_eq!(
            reopened.inspect("session-1").await.unwrap().pending
                + reopened.inspect("session-1").await.unwrap().claimed,
            0
        );
    }

    #[tokio::test]
    async fn deterministic_legacy_retry_ignores_only_retry_metadata() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let value = serde_json::json!({"content": "legacy retry"});
        let mut original = SessionMessageEnvelope {
            id: SessionMessageId::legacy("session-1", 0, &value),
            source: SessionMessageSource::Runtime {
                subsystem: "legacy_pending_injected_messages".to_string(),
            },
            target_session_id: "session-1".to_string(),
            kind: bamboo_domain::SessionMessageKind::RuntimeInstruction,
            body: bamboo_domain::SessionMessageBody::RuntimeInstruction(
                bamboo_domain::SessionRuntimeInstruction {
                    instruction: "legacy_pending_injected_message".to_string(),
                    content: Some(bamboo_domain::SessionMessageContent::text("legacy retry")),
                    data: Some(value),
                    provider_message: None,
                },
            ),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: Some("legacy_pending_injected_messages".to_string()),
        };
        let first = inbox.deliver(&original).await.unwrap();
        original.created_at += chrono::Duration::seconds(5);
        original.attempt = Some(2);
        assert_eq!(inbox.deliver(&original).await.unwrap(), first);

        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        inbox.ack("session-1", &claim).await.unwrap();
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        original.created_at += chrono::Duration::seconds(5);
        original.attempt = Some(3);
        assert_eq!(reopened.deliver(&original).await.unwrap(), first);
    }

    #[tokio::test]
    async fn maximum_length_id_uses_bounded_transport_and_receipt_names() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "long id");
        envelope.id = SessionMessageId::parse("x".repeat(256)).unwrap();
        inbox.deliver(&envelope).await.unwrap();
        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);
        inbox.ack("session-1", &claim).await.unwrap();
        assert!(inbox.was_admitted("session-1", &envelope.id).await.unwrap());
    }

    #[tokio::test]
    async fn admitted_digest_mismatch_fails_closed() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let requested = SessionMessageId::parse("requested").unwrap();
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let path = FileSessionInbox::admitted_path(&dir, &requested);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        atomic_write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "id": "different",
                "generation": 1,
            }))
            .unwrap()
            .as_slice(),
        )
        .await
        .unwrap();

        assert!(matches!(
            inbox.was_admitted("session-1", &requested).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "collision");
        envelope.id = requested;
        assert!(matches!(
            inbox.deliver(&envelope).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
    }

    #[tokio::test]
    async fn ack_rejects_mismatched_id_and_generation_without_deleting_claim() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "exact");
        envelope.id = SessionMessageId::parse("exact-id").unwrap();
        inbox.deliver(&envelope).await.unwrap();
        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);

        let mut wrong_generation = claim.clone();
        wrong_generation.generation += 1;
        assert!(matches!(
            inbox.ack("session-1", &wrong_generation).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));

        let mut wrong_id = claim.clone();
        wrong_id.envelope.id = SessionMessageId::parse("other-id").unwrap();
        assert!(matches!(
            inbox.ack("session-1", &wrong_id).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.claimed, 1);
        assert!(!inbox.was_admitted("session-1", &envelope.id).await.unwrap());

        inbox.ack("session-1", &claim).await.unwrap();
        assert!(inbox.was_admitted("session-1", &envelope.id).await.unwrap());
    }

    #[tokio::test]
    async fn ack_cannot_treat_matching_unclaimed_new_entry_as_permanent_proof() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "still pending");
        envelope.id = SessionMessageId::parse("pending-is-not-admitted").unwrap();
        let receipt = inbox.deliver(&envelope).await.unwrap();
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let mut entries = tokio::fs::read_dir(dir.join("new")).await.unwrap();
        let claim_id = loop {
            let entry = entries.next_entry().await.unwrap().unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".json") && !name.starts_with('.') {
                break name;
            }
        };
        let fabricated = SessionInboxClaim {
            envelope: envelope.clone(),
            generation: receipt.generation,
            claim_id,
        };

        assert!(matches!(
            inbox.ack("session-1", &fabricated).await,
            Err(SessionInboxError::InvalidClaim(_))
        ));
        let backlog = inbox.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending, 1);
        assert_eq!(backlog.claimed, 0);
        assert!(!inbox.was_admitted("session-1", &envelope.id).await.unwrap());
    }

    #[tokio::test]
    async fn ack_completes_receipt_then_remove_crash_window_idempotently() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut envelope = SessionMessageEnvelope::user_input("session-1", "exactly once");
        envelope.id = SessionMessageId::parse("receipt-before-remove").unwrap();
        inbox.deliver(&envelope).await.unwrap();
        authorize_latest(&inbox).await;
        let claim = inbox.claim("session-1", 1).await.unwrap().remove(0);

        // Simulate a crash after the permanent receipt was committed but before
        // the canonical cur file was removed.
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let admitted_path = FileSessionInbox::admitted_path(&dir, &envelope.id);
        tokio::fs::create_dir_all(admitted_path.parent().unwrap())
            .await
            .unwrap();
        let receipt = serde_json::to_vec(&serde_json::json!({
            "id": envelope.id,
            "generation": claim.generation,
            "semantic_digest": FileSessionInbox::semantic_digest(&envelope).unwrap(),
            "admitted_at": Utc::now(),
        }))
        .unwrap();
        atomic_write(&admitted_path, &receipt).await.unwrap();
        assert_eq!(inbox.inspect("session-1").await.unwrap().claimed, 1);

        inbox.ack("session-1", &claim).await.unwrap();
        assert_eq!(inbox.inspect("session-1").await.unwrap().claimed, 0);
        assert!(inbox.was_admitted("session-1", &envelope.id).await.unwrap());
        // Retrying after both steps completed is also an exact no-op.
        inbox.ack("session-1", &claim).await.unwrap();
    }

    #[tokio::test]
    async fn malformed_typed_envelope_is_quarantined_without_poisoning_drain() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let mailbox = Mailbox::at(&dir);
        mailbox
            .deliver(&InboxMessage {
                id: MsgId("malformed".to_string()),
                from: AgentRef {
                    session_id: "runtime:test".to_string(),
                    role: None,
                },
                kind: InboxKind::SessionEnvelope,
                body: serde_json::json!({"not": "an envelope"}),
                created_at: Utc.timestamp_nanos(0),
                correlation_id: None,
            })
            .await
            .unwrap();
        let valid = SessionMessageEnvelope::user_input("session-1", "valid");
        inbox.deliver(&valid).await.unwrap();
        authorize_latest(&inbox).await;

        let claims = inbox.claim("session-1", 10).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, valid.id);
        assert_eq!(
            FileSessionInbox::count_json(&dir.join("corrupt"))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn malformed_json_filename_is_quarantined_without_blocking_backlog_or_claim() {
        let (_temp, _sessions, inbox) = fixture(SessionInboxLimits {
            max_payload_bytes: 256 * 1024,
            max_backlog: 1,
            max_claim_batch: 128,
        })
        .await;
        let dir = inbox.inbox_dir("session-1").await.unwrap();
        let new_dir = dir.join("new");
        tokio::fs::create_dir_all(&new_dir).await.unwrap();
        tokio::fs::write(new_dir.join("not-a-generation.json"), b"{}")
            .await
            .unwrap();

        // Inspection quarantines the malformed transport artifact, so it is
        // neither the oldest generation nor a capacity-consuming message.
        let empty = inbox.inspect("session-1").await.unwrap();
        assert_eq!(empty.pending + empty.claimed, 0);
        assert_eq!(empty.oldest_generation, None);
        let valid = SessionMessageEnvelope::user_input("session-1", "valid after poison");
        let receipt = inbox.deliver(&valid).await.unwrap();
        inbox
            .mark_activation_eligible(
                "session-1",
                receipt.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let claims = inbox.claim("session-1", 10).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, valid.id);
        assert_eq!(
            FileSessionInbox::count_json(&dir.join("corrupt"))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn payload_and_backlog_limits_are_explicit() {
        let limits = SessionInboxLimits {
            max_payload_bytes: 512,
            max_backlog: 1,
            max_claim_batch: 1,
        };
        let (_temp, _sessions, inbox) = fixture(limits).await;
        let oversized = SessionMessageEnvelope::user_input("session-1", "x".repeat(1024));
        assert!(matches!(
            inbox.deliver(&oversized).await,
            Err(SessionInboxError::PayloadTooLarge { .. })
        ));
        inbox
            .deliver(&SessionMessageEnvelope::user_input("session-1", "one"))
            .await
            .unwrap();
        assert!(matches!(
            inbox
                .deliver(&SessionMessageEnvelope::user_input("session-1", "two"))
                .await,
            Err(SessionInboxError::BacklogFull {
                current: 1,
                limit: 1
            })
        ));
    }
}
