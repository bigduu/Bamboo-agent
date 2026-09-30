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
    SessionActivationPolicy, SessionInboxActivationIntent, SessionInboxBacklog, SessionInboxClaim,
    SessionInboxError, SessionInboxLimits, SessionInboxPort, SessionInboxReceipt,
    SessionMessageEnvelope, SessionMessageId, SessionMessageSource,
};
use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, Mailbox, MsgId};
use base64::Engine;
use chrono::{TimeZone, Utc};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::v2::atomic_write;
use crate::SessionStoreV2;

const INBOX_DIR: &str = "inbox";
const GENERATION_FILE: &str = "generation";
const ACTIVATION_GENERATION_FILE: &str = "activation-generation";
const INTERRUPT_GENERATION_FILE: &str = "interrupt-generation";
const ADMITTED_DIR: &str = "admitted";
const OPERATION_LOCK_FILE: &str = ".session-inbox.lock";
const ACTIVATION_INTENT_KEY: &str = "session_inbox_activation_intent";
const MAX_INTENT_TRANSPORT_BYTES: usize = 32 * 1024 * 1024;

#[path = "session_inbox_owned.rs"]
mod owned;
pub(crate) use owned::OwnedFilesystem;
use owned::{AckAuthority, InboxAuthority, StoredLease};

struct StoredInboxReceipt {
    delivery: SessionInboxReceipt,
    intent: Option<SessionInboxActivationIntent>,
    lease: Option<StoredLease>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedActivationWatermark {
    version: u32,
    generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    interrupt_snapshot: Option<u64>,
}

struct FileOperationLock(File);

impl Drop for FileOperationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

/// Holds the same cross-process Inbox lock as claim/ACK while an Actor-fenced
/// pre-dispatch context seed verifies the physical `cur/` entries and writes
/// main. It does not admit or acknowledge those entries.
pub(crate) struct UnownedActorClaimGuard {
    _process: OwnedMutexGuard<()>,
    _file: FileOperationLock,
    dir: PathBuf,
    target: String,
    max_transport_bytes: usize,
}

impl UnownedActorClaimGuard {
    pub(crate) fn directory(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn verify(&self, claim: &SessionInboxClaim) -> Result<(), SessionInboxError> {
        use std::io::Read;

        FileSessionInbox::validate_claim_name(&claim.claim_id)?;
        if FileSessionInbox::claim_generation(&claim.claim_id)? != claim.generation
            || claim.envelope.target_session_id != self.target
        {
            return Err(SessionInboxError::InvalidClaim(
                "Actor claim identity changed before dispatch".into(),
            ));
        }
        let path = self.dir.join("cur").join(&claim.claim_id);
        let file = File::open(path)
            .map_err(|_| SessionInboxError::InvalidClaim("Actor claim disappeared".into()))?;
        if !file
            .metadata()
            .map_err(|_| SessionInboxError::InvalidClaim("Actor claim unreadable".into()))?
            .is_file()
        {
            return Err(SessionInboxError::InvalidClaim(
                "Actor claim is not a regular file".into(),
            ));
        }
        let mut bytes = Vec::new();
        file.take(self.max_transport_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SessionInboxError::InvalidClaim("Actor claim unreadable".into()))?;
        if bytes.len() > self.max_transport_bytes {
            return Err(SessionInboxError::InvalidClaim(
                "Actor claim exceeds transport limit".into(),
            ));
        }
        let wrapper: InboxMessage = serde_json::from_slice(&bytes)
            .map_err(|_| SessionInboxError::InvalidClaim("Actor claim is invalid".into()))?;
        if wrapper.kind != InboxKind::SessionEnvelope {
            return Err(SessionInboxError::InvalidClaim(
                "Actor claim kind changed".into(),
            ));
        }
        let envelope: SessionMessageEnvelope = serde_json::from_value(wrapper.body)
            .map_err(|_| SessionInboxError::InvalidClaim("Actor envelope is invalid".into()))?;
        envelope
            .validate()
            .map_err(|_| SessionInboxError::InvalidClaim("Actor envelope is invalid".into()))?;
        if envelope != claim.envelope {
            return Err(SessionInboxError::InvalidClaim(
                "Actor claim envelope changed".into(),
            ));
        }
        Ok(())
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
    #[cfg(test)]
    followup_authority_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    admission_commit_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    activation_write_failure: bool,
    #[cfg(test)]
    intent_scan_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    owned_after_write_failure: bool,
    #[cfg(test)]
    owned_renew_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    owned_ack_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    owned_ack_after_receipt_failure: bool,
    #[cfg(test)]
    owned_after_header_failure: bool,
    #[cfg(test)]
    owned_fs_hook: Option<owned::FilesystemHook>,
    #[cfg(test)]
    owned_scope_drop: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl FileSessionInbox {
    pub fn new(sessions: Arc<SessionStoreV2>, limits: SessionInboxLimits) -> Self {
        Self {
            sessions,
            limits,
            operation_locks: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            followup_authority_pause: None,
            #[cfg(test)]
            admission_commit_pause: None,
            #[cfg(test)]
            activation_write_failure: false,
            #[cfg(test)]
            intent_scan_pause: None,
            #[cfg(test)]
            owned_after_write_failure: false,
            #[cfg(test)]
            owned_renew_pause: None,
            #[cfg(test)]
            owned_ack_pause: None,
            #[cfg(test)]
            owned_ack_after_receipt_failure: false,
            #[cfg(test)]
            owned_after_header_failure: false,
            #[cfg(test)]
            owned_fs_hook: None,
            #[cfg(test)]
            owned_scope_drop: None,
        }
    }

    /// Storage-only opt-in; this result is not provider or worker admission.
    pub async fn checkpoint_actor_input(
        &self,
        request: crate::ActorInputCheckpoint,
    ) -> Result<crate::ActorInputCheckpointResult, crate::ActorInputCheckpointError> {
        self.sessions
            .checkpoint_owned_input(self.clone(), request)
            .await
    }

    pub(crate) async fn actor_input_filesystem(
        &self,
        target: &str,
        guards: Arc<crate::v2::ActorInputGuards>,
    ) -> Result<(PathBuf, OwnedFilesystem), SessionInboxError> {
        self.filesystem_with_authority(target, InboxAuthority::Actor { _guard: guards })
            .await
    }

    pub(crate) async fn lock_unowned_actor_claims(
        &self,
        target: &str,
    ) -> Result<UnownedActorClaimGuard, SessionInboxError> {
        let dir = self.inbox_dir(target).await?;
        let (process, file) = self.lock_operation(&dir).await?;
        if Self::owned_enabled(&dir).await? {
            return Err(SessionInboxError::InvalidClaim(
                "unowned Actor claim required".into(),
            ));
        }
        Ok(UnownedActorClaimGuard {
            _process: process,
            _file: file,
            dir,
            target: target.to_owned(),
            max_transport_bytes: self.max_transport_bytes(),
        })
    }

    pub fn limits(&self) -> SessionInboxLimits {
        self.limits
    }

    fn max_transport_bytes(&self) -> usize {
        self.limits
            .max_payload_bytes
            .saturating_mul(8)
            .saturating_add(4096)
            .min(MAX_INTENT_TRANSPORT_BYTES)
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

    async fn next_generation(
        dir: &Path,
        filesystem: &OwnedFilesystem,
    ) -> Result<u64, SessionInboxError> {
        let next = Self::read_generation(dir).await?.saturating_add(1);
        filesystem
            .write(&dir.join(GENERATION_FILE), next.to_string().as_bytes())
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist inbox generation: {error}"))
            })?;
        Ok(next)
    }

    async fn read_activation_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        Self::read_activation_watermark(dir)
            .await
            .map(|(generation, _)| generation)
    }

    async fn read_activation_watermark(dir: &Path) -> Result<(u64, bool), SessionInboxError> {
        let path = dir.join(ACTIVATION_GENERATION_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => {
                if let Ok(generation) = raw.trim().parse::<u64>() {
                    return Ok((generation, false));
                }
                let watermark: VersionedActivationWatermark =
                    serde_json::from_str(&raw).map_err(|_| {
                        SessionInboxError::Storage("invalid inbox activation watermark".into())
                    })?;
                if !matches!(watermark.version, 2 | 3) {
                    return Err(SessionInboxError::Storage(
                        "unsupported inbox activation watermark version".into(),
                    ));
                }
                if watermark.version == 3 && watermark.interrupt_snapshot.is_none() {
                    return Err(SessionInboxError::Storage(
                        "owned watermark lacks interrupt snapshot".into(),
                    ));
                }
                Ok((watermark.generation, true))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok((0, false)),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read inbox activation generation {}: {error}",
                path.display()
            ))),
        }
    }

    async fn write_activation_watermark(
        dir: &Path,
        generation: u64,
        versioned: bool,
        filesystem: &OwnedFilesystem,
    ) -> Result<(), SessionInboxError> {
        let owned = Self::owned_enabled(dir).await?;
        let bytes = if versioned {
            serde_json::to_vec(&VersionedActivationWatermark {
                version: if owned { 3 } else { 2 },
                generation,
                interrupt_snapshot: if owned {
                    Some(Self::read_interrupt_generation(dir).await?)
                } else {
                    None
                },
            })
            .map_err(|_| SessionInboxError::Storage("encode inbox activation watermark".into()))?
        } else {
            generation.to_string().into_bytes()
        };
        filesystem
            .write(&dir.join(ACTIVATION_GENERATION_FILE), &bytes)
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist inbox activation generation: {error}"))
            })
    }

    async fn read_interrupt_generation(dir: &Path) -> Result<u64, SessionInboxError> {
        let path = dir.join(INTERRUPT_GENERATION_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => {
                if let Ok(generation) = raw.trim().parse::<u64>() {
                    return Ok(Self::activation_interrupt_snapshot(dir)
                        .await?
                        .unwrap_or(generation));
                }
                let watermark: VersionedActivationWatermark =
                    serde_json::from_str(&raw).map_err(|_| {
                        SessionInboxError::Storage("invalid inbox interrupt watermark".into())
                    })?;
                if watermark.version != 3 {
                    return Err(SessionInboxError::Storage(
                        "unsupported inbox interrupt watermark version".into(),
                    ));
                }
                Ok(watermark.generation)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Ok(Self::activation_interrupt_snapshot(dir).await?.unwrap_or(0))
            }
            Err(error) => Err(SessionInboxError::Storage(format!(
                "read inbox interrupt generation {}: {error}",
                path.display()
            ))),
        }
    }

    async fn oldest_backlog_generation(
        dir: &Path,
        filesystem: &OwnedFilesystem,
    ) -> Result<Option<u64>, SessionInboxError> {
        let mut oldest = None;
        for queue in ["new", "cur"] {
            for (generation, _, _) in Self::owned_queue_entries(dir, queue, filesystem).await? {
                oldest = Some(oldest.map_or(generation, |current: u64| current.min(generation)));
            }
        }
        Ok(oldest)
    }

    fn wrapper(
        envelope: &SessionMessageEnvelope,
        generation: u64,
        intent: Option<SessionInboxActivationIntent>,
    ) -> InboxMessage {
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
        let mut body = serde_json::to_value(envelope).unwrap_or(serde_json::Value::Null);
        if let (Some(intent), Some(fields)) = (intent, body.as_object_mut()) {
            // Provider admission strips this transport-owned field. Maildir
            // publishes permission with the message in the same rename.
            fields.insert(
                ACTIVATION_INTENT_KEY.into(),
                serde_json::to_value(intent).expect("activation intent serialization"),
            );
        }
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
            body,
            created_at: transport_time,
            correlation_id: envelope
                .correlation_id
                .as_ref()
                .map(|value| MsgId(value.clone())),
        }
    }

    fn activation_intent(
        body: &serde_json::Value,
    ) -> Result<Option<SessionInboxActivationIntent>, SessionInboxError> {
        let Some(raw) = body.get(ACTIVATION_INTENT_KEY) else {
            return Ok(None);
        };
        let intent: SessionInboxActivationIntent =
            serde_json::from_value(raw.clone()).map_err(|_| {
                SessionInboxError::InvalidClaim("invalid SessionInbox activation intent".into())
            })?;
        intent.policy()?;
        Ok(Some(intent))
    }

    /// An immediate grant applies to one message, never an earlier sibling.
    fn eligible(
        generation: u64,
        prefix: u64,
        intent: Option<SessionInboxActivationIntent>,
    ) -> bool {
        generation > 0 && (generation <= prefix || intent.is_some())
    }

    fn effective_activation_policy(
        generation: u64,
        prefix: u64,
        interrupt_prefix: u64,
        intent: Option<SessionInboxActivationIntent>,
    ) -> Result<SessionActivationPolicy, SessionInboxError> {
        let own_interrupt = intent.map(|intent| intent.policy()).transpose()?
            == Some(SessionActivationPolicy::InterruptSpecificWait);
        Ok(
            if own_interrupt || (generation <= prefix && generation <= interrupt_prefix) {
                SessionActivationPolicy::InterruptSpecificWait
            } else {
                SessionActivationPolicy::RespectSpecificWait
            },
        )
    }

    async fn queue_intent(
        &self,
        path: &Path,
        versioned: bool,
    ) -> Result<Option<SessionInboxActivationIntent>, SessionInboxError> {
        let file = tokio::fs::File::open(path).await.map_err(|error| {
            SessionInboxError::Storage(format!("read inbox activation intent: {error}"))
        })?;
        let limit = self.max_transport_bytes();
        let size = file
            .metadata()
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("inspect inbox activation bytes: {error}"))
            })?
            .len();
        if size > limit as u64 {
            return Err(SessionInboxError::InvalidClaim(
                "inbox activation scan exceeds byte limit".into(),
            ));
        }
        #[cfg(test)]
        if let Some((entered, release)) = &self.intent_scan_pause {
            entered.notify_one();
            release.notified().await;
        }
        // Metadata is only a precheck: growth after stat must not bypass the
        // bound, even when the transport subsequently fails JSON decoding.
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("read inbox activation bytes: {error}"))
            })?;
        if bytes.len() > limit {
            return Err(SessionInboxError::InvalidClaim(
                "inbox activation scan exceeds byte limit".into(),
            ));
        }
        // Corrupt legacy transports remain claim-time quarantine work. Their
        // presence alone must never grant immediate execution permission.
        let Ok((wrapper, lease)) = Self::decode_owned_wrapper(&bytes) else {
            return Ok(None);
        };
        if lease.is_some()
            && !Self::owned_enabled(path.parent().and_then(Path::parent).ok_or_else(|| {
                SessionInboxError::InvalidClaim("invalid inbox queue path".into())
            })?)
            .await?
        {
            return Err(SessionInboxError::InvalidClaim(
                "owned lease requires v3 watermarks".into(),
            ));
        }
        if wrapper.kind != InboxKind::SessionEnvelope {
            return Ok(None);
        }
        let intent = Self::activation_intent(&wrapper.body)?;
        if intent.is_some() && !versioned {
            return Err(SessionInboxError::InvalidClaim(
                "activation intent requires its v2 watermark".into(),
            ));
        }
        Ok(intent)
    }

    fn claim_generation(claim_id: &str) -> Result<u64, SessionInboxError> {
        claim_id
            .split_once('-')
            .and_then(|(prefix, _)| prefix.parse::<u64>().ok())
            .filter(|generation| *generation > 0)
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
        Self::queue_entries(dir, queue, None).await
    }

    async fn owned_queue_entries(
        dir: &Path,
        queue: &str,
        filesystem: &OwnedFilesystem,
    ) -> Result<Vec<(u64, String, PathBuf)>, SessionInboxError> {
        Self::queue_entries(dir, queue, Some(filesystem)).await
    }

    async fn queue_entries(
        dir: &Path,
        queue: &str,
        filesystem: Option<&OwnedFilesystem>,
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
                Err(error) => match filesystem {
                    Some(filesystem) => {
                        filesystem
                            .quarantine(dir, &entry.path(), &error.to_string())
                            .await?
                    }
                    None => Self::quarantine_claim(dir, &entry.path(), &error.to_string()).await?,
                },
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
    ) -> Result<Option<StoredInboxReceipt>, SessionInboxError> {
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
                return Ok(Some(StoredInboxReceipt {
                    delivery: SessionInboxReceipt {
                        id: id.clone(),
                        generation,
                    },
                    intent: Self::activation_intent(&receipt)?,
                    lease: StoredLease::from_value(&receipt)?,
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
        requested_intent: Option<SessionInboxActivationIntent>,
        filesystem: &OwnedFilesystem,
    ) -> Result<Option<SessionInboxReceipt>, SessionInboxError> {
        if let Some(receipt) = Self::admitted_receipt(dir, requested).await? {
            if receipt.intent != requested_intent {
                return Err(SessionInboxError::InvalidClaim(
                    "message id was reused with different activation intent".into(),
                ));
            }
            return Ok(Some(receipt.delivery));
        }
        let id = &requested.id;
        let requested_digest = Self::semantic_digest(requested)?;
        for queue in ["new", "cur", "cancelled", "dead"] {
            for (generation, _name, path) in
                Self::owned_queue_entries(dir, queue, filesystem).await?
            {
                let Ok(bytes) = tokio::fs::read(path).await else {
                    continue;
                };
                let Ok((wrapper, lease)) = Self::decode_owned_wrapper(&bytes) else {
                    continue;
                };
                if lease.is_some() && !Self::owned_enabled(dir).await? {
                    return Err(SessionInboxError::InvalidClaim(
                        "owned lease requires v3 watermarks".into(),
                    ));
                }
                if wrapper.kind != InboxKind::SessionEnvelope {
                    continue;
                }
                let intent = Self::activation_intent(&wrapper.body)?;
                let Ok(envelope) = serde_json::from_value::<SessionMessageEnvelope>(wrapper.body)
                else {
                    continue;
                };
                if &envelope.id == id {
                    if Self::semantic_digest(&envelope)? != requested_digest
                        || intent != requested_intent
                    {
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

    async fn deliver_with_lifecycle_held(
        &self,
        envelope: &SessionMessageEnvelope,
        authority: InboxAuthority,
        gate: Option<&bamboo_domain::AdmissionGate>,
        intent: Option<SessionInboxActivationIntent>,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = owned::ScopeDrop(self.owned_scope_drop.clone());
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

        let (dir, filesystem) = self
            .filesystem_with_authority(&envelope.target_session_id, authority)
            .await?;
        // Enqueue idempotency is independent from consumer admission dedupe.
        // This closes the legacy-migration crash window (deliver succeeded,
        // source clear did not) without letting deterministic retries fill the
        // bounded backlog.
        if let Some(receipt) = Self::existing_receipt(&dir, envelope, intent, &filesystem).await? {
            if intent.is_some() && !Self::read_activation_watermark(&dir).await?.1 {
                return Err(SessionInboxError::InvalidClaim(
                    "activation intent requires its v2 watermark".into(),
                ));
            }
            return Ok(receipt);
        }
        if let Some(gate) = gate {
            if gate.is_cancelled() {
                return Err(SessionInboxError::AdmissionCancelled);
            }
            if gate.is_committed() {
                return Err(SessionInboxError::Storage(
                    "committed inbox admission has no matching durable receipt".into(),
                ));
            }
        }
        let current = Self::owned_queue_entries(&dir, "new", &filesystem)
            .await?
            .len()
            + Self::owned_queue_entries(&dir, "cur", &filesystem)
                .await?
                .len();
        if current >= self.limits.max_backlog {
            return Err(SessionInboxError::BacklogFull {
                current,
                limit: self.limits.max_backlog,
            });
        }

        let generation = Self::next_generation(&dir, &filesystem).await?;
        let wrapper = Self::wrapper(envelope, generation, intent);
        if intent.is_some() {
            let bytes = serde_json::to_vec_pretty(&wrapper).map_err(|_| {
                SessionInboxError::Storage("encode immediate inbox transport".into())
            })?;
            if bytes.len() > self.max_transport_bytes() {
                return Err(SessionInboxError::PayloadTooLarge {
                    actual: bytes.len(),
                    limit: self.max_transport_bytes(),
                });
            }
            let (prefix, versioned) = Self::read_activation_watermark(&dir).await?;
            if !versioned {
                // Fence v1-only readers/writers before publishing any intent.
                // This preserves the existing coordinator permission exactly;
                // cancellation may leave this safe upgrade without a message.
                Self::write_activation_watermark(&dir, prefix, true, &filesystem).await?;
            }
        }
        #[cfg(test)]
        if gate.is_some() {
            if let Some((entered, release)) = &self.admission_commit_pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        match filesystem
            .deliver(&dir, &wrapper, gate)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?
        {
            bamboo_domain::AdmissionCommit::Committed(_) => {}
            bamboo_domain::AdmissionCommit::Cancelled => {
                return Err(SessionInboxError::AdmissionCancelled)
            }
            bamboo_domain::AdmissionCommit::AlreadyCommitted => {
                return Err(SessionInboxError::Storage(
                    "committed inbox admission has no matching durable receipt".into(),
                ))
            }
        }
        Ok(SessionInboxReceipt {
            id: envelope.id.clone(),
            generation,
        })
    }

    async fn ack_unlocked(
        &self,
        dir: &Path,
        target_session_id: &str,
        claim: &SessionInboxClaim,
        authority: AckAuthority<'_>,
    ) -> Result<(), SessionInboxError> {
        let expected_lease = authority.lease();
        let cur_path = dir.join("cur").join(&claim.claim_id);
        let bytes = match tokio::fs::read(&cur_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Idempotent retry only when the exact permanent receipt is
                // already present. A missing claim without that proof is stale.
                return match Self::admitted_receipt(dir, &claim.envelope).await? {
                    Some(receipt)
                        if receipt.delivery.generation == claim.generation
                            && StoredLease::same_optional_identity(
                                receipt.lease.as_ref(),
                                expected_lease,
                            ) =>
                    {
                        Ok(())
                    }
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
        let (wrapper, stored_lease) = Self::decode_owned_wrapper(&bytes)?;
        if !StoredLease::same_optional_identity(stored_lease.as_ref(), expected_lease) {
            return Err(SessionInboxError::InvalidClaim(
                "Inbox lease identity mismatch".into(),
            ));
        }
        if wrapper.kind != InboxKind::SessionEnvelope {
            return Err(SessionInboxError::InvalidClaim(format!(
                "canonical claim {} has kind {:?}",
                claim.claim_id, wrapper.kind
            )));
        }
        let intent = Self::activation_intent(&wrapper.body)?;
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

        let admitted_path = Self::admitted_path(dir, &claim.envelope.id);
        if let Some(existing) = Self::admitted_receipt(dir, &claim.envelope).await? {
            if existing.delivery.generation != claim.generation
                || existing.intent != intent
                || !StoredLease::same_optional_identity(existing.lease.as_ref(), expected_lease)
            {
                return Err(SessionInboxError::InvalidClaim(format!(
                    "admitted receipt generation mismatch for {}",
                    claim.envelope.id
                )));
            }
        }
        let mut receipt = serde_json::json!({
            "id": claim.envelope.id,
            "generation": claim.generation,
            "semantic_digest": Self::semantic_digest(&claim.envelope)?,
            "admitted_at": Utc::now(),
        });
        if let Some(intent) = intent {
            receipt.as_object_mut().expect("receipt object").insert(
                ACTIVATION_INTENT_KEY.into(),
                serde_json::to_value(intent).expect("activation intent serialization"),
            );
        }
        if let Some(lease) = expected_lease {
            receipt.as_object_mut().expect("receipt object").insert(
                owned::LEASE_KEY.into(),
                serde_json::to_value(lease).expect("lease serialization"),
            );
        }
        let receipt = serde_json::to_vec_pretty(&receipt)
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let admitted_dir = admitted_path.parent().ok_or_else(|| {
            SessionInboxError::Storage(format!(
                "admitted receipt has no parent: {}",
                admitted_path.display()
            ))
        })?;
        authority.create_dir(admitted_dir).await.map_err(|error| {
            SessionInboxError::Storage(format!(
                "create admitted receipt directory {}: {error}",
                admitted_dir.display()
            ))
        })?;
        authority
            .write(&admitted_path, &receipt)
            .await
            .map_err(|error| {
                SessionInboxError::Storage(format!("persist admitted receipt: {error}"))
            })?;

        #[cfg(test)]
        if expected_lease.is_some() && self.owned_ack_after_receipt_failure {
            return Err(SessionInboxError::Storage(
                "injected post-receipt ACK failure".into(),
            ));
        }

        match authority.remove(&cur_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SessionInboxError::Storage(format!(
                "remove claimed message {}: {error}",
                cur_path.display()
            ))),
        }
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
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    async fn claim_owned(
        &self,
        target: &str,
        limit: usize,
        run: Option<&str>,
        request: &bamboo_domain::SessionInboxLeaseRequest,
    ) -> Result<Vec<bamboo_domain::SessionInboxOwnedClaim>, SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        let run = run.map(str::to_owned);
        let request = request.clone();
        owned::complete_owned(async move {
            inbox
                .claim_owned_impl(&target, limit, run.as_deref(), &request)
                .await
        })
        .await
    }

    async fn renew_owned(
        &self,
        target: &str,
        claim: &bamboo_domain::SessionInboxOwnedClaim,
        request: &bamboo_domain::SessionInboxLeaseRequest,
    ) -> Result<bamboo_domain::SessionInboxOwnedClaim, SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        let claim = claim.clone();
        let request = request.clone();
        owned::complete_owned(
            async move { inbox.renew_owned_impl(&target, &claim, &request).await },
        )
        .await
    }

    async fn ack_owned(
        &self,
        target: &str,
        claim: &bamboo_domain::SessionInboxOwnedClaim,
        now: chrono::DateTime<Utc>,
    ) -> Result<(), SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        let claim = claim.clone();
        owned::complete_owned(async move { inbox.ack_owned_impl(&target, &claim, now).await }).await
    }

    async fn inspect_owned_leases(
        &self,
        target: &str,
        limit: usize,
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<bamboo_domain::SessionInboxLeaseInspection>, SessionInboxError> {
        self.inspect_owned_impl(target, limit, now).await
    }

    async fn inspect_wake_readiness(
        &self,
        target: &str,
        now: chrono::DateTime<Utc>,
    ) -> Result<bamboo_domain::SessionInboxWakeReadiness, SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        owned::complete_owned(async move { inbox.inspect_wake_readiness_impl(&target, now).await })
            .await
    }

    async fn fail_owned(
        &self,
        target: &str,
        claim: &bamboo_domain::SessionInboxOwnedClaim,
        report: &bamboo_domain::SessionInboxFailureReport,
    ) -> Result<bamboo_domain::SessionInboxFailureOutcome, SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        let claim = claim.clone();
        let report = report.clone();
        owned::complete_owned(async move { inbox.fail_owned_impl(&target, &claim, &report).await })
            .await
    }

    async fn inspect_dead_letters(
        &self,
        target: &str,
        limit: usize,
        principal: &bamboo_domain::SessionInboxAdministrationPrincipal,
    ) -> Result<Vec<bamboo_domain::SessionInboxDeadLetterInspection>, SessionInboxError> {
        self.inspect_dead_letters_impl(target, limit, principal)
            .await
    }

    async fn retry_dead_letter(
        &self,
        target: &str,
        id: &SessionMessageId,
        generation: u64,
        now: chrono::DateTime<Utc>,
        principal: &bamboo_domain::SessionInboxAdministrationPrincipal,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        let inbox = self.clone();
        let target = target.to_owned();
        let id = id.clone();
        let principal = principal.clone();
        owned::complete_owned(async move {
            inbox
                .retry_dead_letter_impl(&target, &id, generation, now, &principal)
                .await
        })
        .await
    }

    async fn deliver(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        let lifecycle = self.lock_lifecycle().await?;
        self.deliver_with_lifecycle_held(
            envelope,
            InboxAuthority::Lifecycle { _guard: lifecycle },
            None,
            None,
        )
        .await
    }

    async fn deliver_with_gate(
        &self,
        envelope: &SessionMessageEnvelope,
        gate: &bamboo_domain::AdmissionGate,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        let lifecycle = self.lock_lifecycle().await?;
        self.deliver_with_lifecycle_held(
            envelope,
            InboxAuthority::Lifecycle { _guard: lifecycle },
            Some(gate),
            None,
        )
        .await
    }

    async fn deliver_with_activation_intent(
        &self,
        envelope: &SessionMessageEnvelope,
        policy: SessionActivationPolicy,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        let lifecycle = self.lock_lifecycle().await?;
        self.deliver_with_lifecycle_held(
            envelope,
            InboxAuthority::Lifecycle { _guard: lifecycle },
            gate,
            Some(SessionInboxActivationIntent::new(policy)),
        )
        .await
    }

    async fn deliver_supervisor_followup(
        &self,
        supervisor: &bamboo_domain::SupervisorReference,
        envelope: &SessionMessageEnvelope,
    ) -> Result<SessionInboxReceipt, SessionInboxError> {
        if envelope.source
            != (SessionMessageSource::Session {
                session_id: supervisor.session_id.clone(),
            })
            || envelope.kind != bamboo_domain::SessionMessageKind::PeerMessage
        {
            return Err(SessionInboxError::InvalidClaim(
                "Supervisor followup requires its own typed Session peer origin".into(),
            ));
        }
        let authority = self
            .sessions
            .lock_supervisor_followup(supervisor, &envelope.target_session_id)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        #[cfg(test)]
        if let Some((entered, release)) = &self.followup_authority_pause {
            entered.notify_one();
            release.notified().await;
        }
        // Never call public deliver here: a queued lifecycle writer would make
        // that nested shared acquisition deadlock. This is the same adapter,
        // operation lock, semantic receipt and Maildir transaction as deliver.
        self.deliver_with_lifecycle_held(
            envelope,
            InboxAuthority::Supervisor { _guard: authority },
            None,
            Some(SessionInboxActivationIntent::new(
                SessionActivationPolicy::RespectSpecificWait,
            )),
        )
        .await
    }

    async fn mark_activation_eligible(
        &self,
        target_session_id: &str,
        generation: u64,
        policy: SessionActivationPolicy,
    ) -> Result<(), SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = owned::ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target_session_id).await?;
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
                Self::write_interrupt_watermark(&dir, generation, &filesystem).await?;
            }
        }
        let (current, versioned) = Self::read_activation_watermark(&dir).await?;
        if generation > current {
            #[cfg(test)]
            if self.activation_write_failure {
                return Err(SessionInboxError::Storage(
                    "injected activation watermark write failure".into(),
                ));
            }
            Self::write_activation_watermark(&dir, generation, versioned, &filesystem).await?;
        }
        Ok(())
    }

    async fn coordinator_activation_generation(
        &self,
        target_session_id: &str,
    ) -> Result<u64, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        Self::read_activation_generation(&dir).await
    }

    async fn claim(
        &self,
        target_session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        self.claim_for_turn(target_session_id, limit, None).await
    }

    async fn claim_for_turn(
        &self,
        target_session_id: &str,
        limit: usize,
        active_run_id: Option<&str>,
    ) -> Result<Vec<SessionInboxClaim>, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        if Self::owned_enabled(&dir).await? {
            return Err(SessionInboxError::InvalidClaim(
                "owned Inbox claim API required".into(),
            ));
        }
        let mailbox = Mailbox::at(&dir);
        mailbox
            .ensure_dirs()
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let (activation_generation, versioned) = Self::read_activation_watermark(&dir).await?;
        let limit = limit.min(self.limits.max_claim_batch);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let interrupt_prefix = Self::read_interrupt_generation(&dir).await?;

        // The coordinator grants a prefix; an immediate producer grants only
        // its own message. Recovery cannot promote an earlier staged sibling.
        let mut eligible = Vec::new();
        for queue in ["cur", "new"] {
            for (generation, name, path) in Self::valid_queue_entries(&dir, queue).await? {
                let intent = self.queue_intent(&path, versioned).await?;
                if Self::eligible(generation, activation_generation, intent) {
                    if let Some(run_id) = active_run_id {
                        let path = dir.join(queue).join(&name);
                        let bytes = tokio::fs::read(&path)
                            .await
                            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
                        if let Ok(wrapper) = serde_json::from_slice::<InboxMessage>(&bytes) {
                            if let Ok(envelope) =
                                serde_json::from_value::<SessionMessageEnvelope>(wrapper.body)
                            {
                                if envelope.waits_for_successor_of_run(run_id) {
                                    continue;
                                }
                            }
                        }
                    }
                    eligible.push((
                        generation,
                        name,
                        queue == "cur",
                        Self::effective_activation_policy(
                            generation,
                            activation_generation,
                            interrupt_prefix,
                            intent,
                        )?,
                    ));
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
        for (_generation, claim_id, already_claimed, activation_policy) in eligible {
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
                    activation_policy,
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
                "claim target mismatch".into(),
            ));
        }
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target_session_id).await?;
        let _guard = self.lock_operation(&dir).await?;
        if Self::owned_enabled(&dir).await? {
            return Err(SessionInboxError::InvalidClaim(
                "owned Inbox ACK API required".into(),
            ));
        }
        self.ack_unlocked(&dir, target_session_id, claim, AckAuthority::Legacy)
            .await
    }

    async fn pending_guidance(
        &self,
        target_session_id: &str,
    ) -> Result<Vec<SessionMessageEnvelope>, SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = owned::ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target_session_id).await?;
        let mut entries = Self::owned_queue_entries(&dir, "new", &filesystem).await?;
        entries.sort_by_key(|entry| entry.0);
        let mut result = Vec::new();
        for (_, _, path) in entries {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            let wrapper: InboxMessage = serde_json::from_slice(&bytes)
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(wrapper.body)
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            if envelope.is_guidance() && envelope.target_session_id == target_session_id {
                result.push(envelope);
            }
        }
        Ok(result)
    }

    async fn cancel_guidance(
        &self,
        target_session_id: &str,
        id: &SessionMessageId,
    ) -> Result<bool, SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = owned::ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target_session_id).await?;
        // The same operation lock protects claim and withdrawal across adapters.
        // Retain the original envelope as a permanent deduplication tombstone.
        for (_, name, path) in Self::owned_queue_entries(&dir, "new", &filesystem).await? {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            let wrapper: InboxMessage = serde_json::from_slice(&bytes)
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(wrapper.body)
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            if &envelope.id == id
                && envelope.is_guidance()
                && envelope.target_session_id == target_session_id
            {
                filesystem
                    .cancel(&dir, &path, &name)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn inspect(
        &self,
        target_session_id: &str,
    ) -> Result<SessionInboxBacklog, SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = owned::ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target_session_id).await?;
        let pending = Self::owned_queue_entries(&dir, "new", &filesystem)
            .await?
            .len();
        let claimed = Self::owned_queue_entries(&dir, "cur", &filesystem)
            .await?
            .len();
        let (prefix, versioned) = Self::read_activation_watermark(&dir).await?;
        let interrupt_prefix = Self::read_interrupt_generation(&dir).await?;
        let mut activation_generation = prefix;
        let mut interrupt_generation = 0;
        for queue in ["new", "cur"] {
            for (generation, _, path) in Self::owned_queue_entries(&dir, queue, &filesystem).await?
            {
                let intent = self.queue_intent(&path, versioned).await?;
                if Self::eligible(generation, prefix, intent) {
                    activation_generation = activation_generation.max(generation);
                    if Self::effective_activation_policy(
                        generation,
                        prefix,
                        interrupt_prefix,
                        intent,
                    )? == SessionActivationPolicy::InterruptSpecificWait
                    {
                        interrupt_generation = interrupt_generation.max(generation);
                    }
                }
            }
        }
        Ok(SessionInboxBacklog {
            pending,
            claimed,
            generation: Self::read_generation(&dir).await?,
            activation_generation,
            coordinator_generation: prefix,
            interrupt_generation,
            oldest_generation: Self::oldest_backlog_generation(&dir, &filesystem).await?,
        })
    }
}

#[cfg(test)]
#[path = "session_inbox_supervisor_tests.rs"]
mod supervisor_tests;

#[cfg(test)]
#[path = "session_inbox_activation_tests.rs"]
mod activation_intent_tests;

#[cfg(test)]
#[path = "session_inbox_owned_tests.rs"]
mod owned_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        Session, SessionChildOutcome, SessionMessageBody, SessionMessageKind, SessionMessageSource,
        Storage,
    };
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
    async fn cancellation_at_inbox_commit_publishes_no_message_after_restart() {
        let (_temp, sessions, mut inbox) = fixture(SessionInboxLimits::default()).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        inbox.admission_commit_pause = Some((entered.clone(), release.clone()));
        let inbox = Arc::new(inbox);
        let envelope = SessionMessageEnvelope::user_input("session-1", "cancel me");
        let gate = Arc::new(bamboo_domain::AdmissionGate::default());
        let delivery = {
            let inbox = inbox.clone();
            let gate = gate.clone();
            tokio::spawn(async move { inbox.deliver_with_gate(&envelope, &gate).await })
        };
        entered.notified().await;
        gate.cancel_if_pending();
        release.notify_one();
        assert!(matches!(
            delivery.await.unwrap(),
            Err(SessionInboxError::AdmissionCancelled)
        ));
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        let backlog = reopened.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        assert_eq!(backlog.activation_generation, 0);
        let dir = reopened.inbox_dir("session-1").await.unwrap().join("new");
        let mut entries = tokio::fs::read_dir(dir).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn committed_gate_reuses_exact_receipt_after_retry_and_restart() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let envelope = SessionMessageEnvelope::user_input("session-1", "once");
        let gate = bamboo_domain::AdmissionGate::default();
        let first = inbox.deliver_with_gate(&envelope, &gate).await.unwrap();
        gate.cancel_if_pending();
        assert!(gate.is_committed());
        assert_eq!(
            inbox.deliver_with_gate(&envelope, &gate).await.unwrap(),
            first
        );
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        assert_eq!(
            reopened.deliver_with_gate(&envelope, &gate).await.unwrap(),
            first
        );
        let backlog = reopened.inspect("session-1").await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 1);
        assert_eq!(backlog.generation, first.generation);
    }

    #[tokio::test]
    async fn deferred_guidance_keeps_its_run_fence_across_retries_and_reopen() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut deferred = SessionMessageEnvelope::user_input("session-1", "after task");
        deferred.correlation_id = Some("session-guidance-after-run:run-a".into());
        let original = inbox.deliver(&deferred).await.unwrap();
        let mut later = SessionMessageEnvelope::user_input("session-1", "after round");
        later.correlation_id = Some("session-guidance".into());
        inbox.deliver(&later).await.unwrap();
        authorize_latest(&inbox).await;
        let claims = inbox
            .claim_for_turn("session-1", 128, Some("run-a"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, later.id);
        inbox.ack("session-1", &claims[0]).await.unwrap();
        let mut retry = deferred.clone();
        retry.correlation_id = Some("session-guidance-after-run:run-b".into());
        assert_eq!(inbox.deliver(&retry).await.unwrap(), original);
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        assert_eq!(
            reopened.pending_guidance("session-1").await.unwrap()[0],
            deferred
        );
        assert!(reopened
            .claim_for_turn("session-1", 128, Some("run-a"))
            .await
            .unwrap()
            .is_empty());
        let claims = reopened
            .claim_for_turn("session-1", 128, Some("run-b"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, deferred.id);
        reopened.ack("session-1", &claims[0]).await.unwrap();
        assert_eq!(reopened.deliver(&retry).await.unwrap(), original);
        assert!(!reopened
            .inspect("session-1")
            .await
            .unwrap()
            .activation_pending());
    }

    #[tokio::test]
    async fn child_outcome_admitted_during_run_is_claimable_only_by_successor() {
        let (_temp, sessions, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut outcome = SessionMessageEnvelope {
            id: bamboo_domain::SessionMessageId::parse("grandchild-outcome").unwrap(),
            source: SessionMessageSource::Runtime {
                subsystem: "child_completion_coordinator".into(),
            },
            target_session_id: "session-1".into(),
            kind: SessionMessageKind::ChildOutcome,
            body: SessionMessageBody::ChildOutcome(SessionChildOutcome {
                child_session_id: "grandchild".into(),
                status: "completed".into(),
                result: Some("done".into()),
                error: None,
                provider_message: None,
            }),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: Some("child_completion_after_run:old-run".into()),
        };
        let first = inbox.deliver(&outcome).await.unwrap();
        authorize_latest(&inbox).await;
        let reopened = FileSessionInbox::new(sessions, SessionInboxLimits::default());
        assert!(reopened
            .claim_for_turn("session-1", 1, Some("old-run"))
            .await
            .unwrap()
            .is_empty());
        outcome.correlation_id = Some("child_completion_after_run:new-run".into());
        assert_eq!(reopened.deliver(&outcome).await.unwrap(), first);
        let claims = reopened
            .claim_for_turn("session-1", 1, Some("new-run"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, outcome.id);
        assert_eq!(
            claims[0].envelope.correlation_id.as_deref(),
            Some("child_completion_after_run:old-run")
        );
    }

    #[tokio::test]
    async fn withdrawal_is_durable_and_never_retracts_a_claim() {
        let (_temp, store, inbox) = fixture(SessionInboxLimits::default()).await;
        let mut message = SessionMessageEnvelope::user_input("session-1", "guidance");
        message.correlation_id = Some("session-guidance".into());
        inbox.deliver(&message).await.unwrap();
        assert_eq!(inbox.pending_guidance("session-1").await.unwrap().len(), 1);
        assert!(inbox
            .cancel_guidance("session-1", &message.id)
            .await
            .unwrap());
        let reopened = FileSessionInbox::new(store, SessionInboxLimits::default());
        reopened.deliver(&message).await.unwrap();
        assert!(reopened
            .pending_guidance("session-1")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            reopened
                .inspect("session-1")
                .await
                .unwrap()
                .oldest_generation,
            None
        );
        message.id = SessionMessageId::new();
        reopened.deliver(&message).await.unwrap();
        authorize_latest(&reopened).await;
        let claims = reopened.claim("session-1", 1).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert!(!reopened
            .cancel_guidance("session-1", &message.id)
            .await
            .unwrap());
        assert_eq!(reopened.inspect("session-1").await.unwrap().claimed, 1);
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
            activation_policy: SessionActivationPolicy::RespectSpecificWait,
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

#[cfg(test)]
#[path = "session_inbox_owned_lifetime_tests.rs"]
mod owned_lifetime_tests;

#[cfg(test)]
#[path = "session_inbox_producer_lifetime_tests.rs"]
mod producer_lifetime_tests;
