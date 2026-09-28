//! Explicit storage leases; no engine consumer opts in through this module.
use super::*;
use bamboo_domain::{
    SessionInboxLeaseInspection, SessionInboxLeaseRequest, SessionInboxLeaseToken,
    SessionInboxOwnedClaim,
};
use chrono::DateTime;
use serde::{Deserialize, Serialize};

pub(super) const LEASE_KEY: &str = "session_inbox_lease";
const OWNED_KIND: &str = "session_envelope_owned_v3";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredLease {
    version: u32,
    token: SessionInboxLeaseToken,
    policy: SessionActivationPolicy,
}

impl StoredLease {
    pub(super) fn from_value(value: &serde_json::Value) -> Result<Option<Self>, SessionInboxError> {
        let Some(raw) = value.get(LEASE_KEY) else {
            return Ok(None);
        };
        let lease: Self =
            serde_json::from_value(raw.clone()).map_err(|_| invalid("invalid Inbox lease"))?;
        if lease.version != 1
            || lease.token.epoch == 0
            || lease.token.consumer.as_str().is_empty()
            || lease.token.consumer.as_str().len() > 128
            || uuid::Uuid::parse_str(&lease.token.incarnation).is_err()
        {
            return Err(invalid("invalid Inbox lease"));
        }
        Ok(Some(lease))
    }

    fn same_identity(&self, token: &SessionInboxLeaseToken) -> bool {
        self.token.consumer == token.consumer
            && self.token.epoch == token.epoch
            && self.token.incarnation == token.incarnation
            && self.token.expires_at == token.expires_at
    }

    pub(super) fn same_optional_identity(left: Option<&Self>, right: Option<&Self>) -> bool {
        match (left, right) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                left.same_identity(&right.token) && left.policy == right.policy
            }
            _ => false,
        }
    }
}

fn invalid(message: &str) -> SessionInboxError {
    SessionInboxError::InvalidClaim(message.into())
}

// Field order releases Inbox FD, process, then the complete original authority
// (reverse sorted Sessions, Task and lifecycle for Supervisor followups).
pub(super) enum InboxAuthority {
    Lifecycle {
        _guard: crate::v2::SessionLifecycleReadGuard,
    },
    Actor {
        _guard: Arc<crate::v2::ActorInputGuards>,
    },
    Supervisor {
        _guard: crate::v2::SupervisorFollowupGuard,
    },
}
struct OwnedScope {
    _process: OwnedMutexGuard<()>,
    _authority: InboxAuthority,
}
struct OwnedGuards {
    _file: FileOperationLock,
    _scope: Arc<OwnedScope>,
}

#[cfg(test)]
pub(super) type FilesystemHook = Arc<dyn Fn(&str, &Path) -> std::io::Result<()> + Send + Sync>;

/// Private mutation capability: constructed only from the actual acquired guards.
#[derive(Clone)]
pub(crate) struct OwnedFilesystem {
    guards: Arc<OwnedGuards>,
    #[cfg(test)]
    hook: Option<FilesystemHook>,
}

impl OwnedFilesystem {
    pub(crate) async fn job<T: Send + 'static>(
        &self,
        event: &'static str,
        path: &Path,
        job: impl FnOnce(&Self, &Path) -> std::io::Result<T> + Send + 'static,
    ) -> std::io::Result<T> {
        let filesystem = self.clone();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            // This capture outlives cancellation of the async transaction itself.
            let _guards = &filesystem.guards;
            filesystem.observe(event, &path)?;
            job(&filesystem, &path)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    fn observe(&self, _event: &str, _path: &Path) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook(_event, _path)?;
        }
        Ok(())
    }

    pub(super) async fn create_dir(&self, path: &Path) -> std::io::Result<()> {
        self.job("mkdir", path, |_, path| std::fs::create_dir_all(path))
            .await
    }

    pub(super) async fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let bytes = bytes.to_vec();
        self.job("write", path, move |filesystem, path| {
            use std::io::Write;
            let temp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
            let result = (|| {
                let mut file = File::create(&temp)?;
                filesystem.observe("write_temp", path)?;
                file.write_all(&bytes)?;
                file.sync_all()
            })();
            if let Err(error) = result {
                // Keep the primary error and the existing best-effort cleanup contract.
                #[cfg(test)]
                let _ = filesystem.observe("write_cleanup", path);
                let _ = std::fs::remove_file(&temp);
                return Err(error);
            }
            filesystem.observe("replace", path)?;
            replace(&temp, path)?;
            filesystem.observe("after_replace", path)
        })
        .await
    }

    pub(super) async fn remove(&self, path: &Path) -> std::io::Result<()> {
        self.job("remove", path, |_, path| std::fs::remove_file(path))
            .await
    }

    pub(super) async fn deliver(
        &self,
        dir: &Path,
        wrapper: &InboxMessage,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> std::io::Result<bamboo_domain::AdmissionCommit<MsgId>> {
        let wrapper = wrapper.clone();
        let gate = gate.cloned();
        self.job("maildir", dir, move |filesystem, dir| {
            Mailbox::at(dir)
                .deliver_blocking(&wrapper, gate.as_ref(), |phase, path| {
                    filesystem.observe(phase, path)
                })
                .map_err(std::io::Error::other)
        })
        .await
    }

    pub(super) async fn cancel(&self, dir: &Path, path: &Path, name: &str) -> std::io::Result<()> {
        let cancelled = dir.join("cancelled");
        let to = cancelled.join(name);
        self.job("cancel", path, move |_, path| {
            std::fs::create_dir_all(cancelled)?;
            std::fs::rename(path, to)
        })
        .await
    }

    async fn rotate(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let to = to.to_owned();
        self.job("rotate", from, move |_, from| std::fs::rename(from, to))
            .await
    }

    pub(super) async fn quarantine(
        &self,
        dir: &Path,
        path: &Path,
        reason: &str,
    ) -> Result<(), SessionInboxError> {
        let name = path
            .file_name()
            .ok_or_else(|| invalid("claim has no filename"))?;
        let corrupt = dir.join("corrupt");
        let to = corrupt.join(name);
        let reason = reason.to_owned();
        self.job("quarantine", path, move |_, from| {
            std::fs::create_dir_all(&corrupt)?;
            match std::fs::rename(from, &to) {
                Ok(()) => {
                    tracing::warn!(path = %to.display(), reason, "quarantined malformed typed session inbox envelope");
                    Ok(())
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        }).await.map_err(|error| SessionInboxError::Storage(error.to_string()))
    }
}

// Same replacement and residue contract as v2::atomic_write; no parent-dir fsync.
fn replace(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(not(windows))]
    {
        std::fs::rename(from, to)
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };
        let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
        let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
        let result = unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// The exact owned lease cannot be dispatched without its actual mutation scope.
pub(super) enum AckAuthority<'a> {
    Legacy,
    Owned {
        lease: &'a StoredLease,
        filesystem: &'a OwnedFilesystem,
    },
}
impl AckAuthority<'_> {
    pub(super) fn lease(&self) -> Option<&StoredLease> {
        match self {
            Self::Legacy => None,
            Self::Owned { lease, .. } => Some(lease),
        }
    }
    pub(super) async fn create_dir(&self, path: &Path) -> std::io::Result<()> {
        match self {
            Self::Legacy => tokio::fs::create_dir_all(path).await,
            Self::Owned { filesystem, .. } => filesystem.create_dir(path).await,
        }
    }
    pub(super) async fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Legacy => atomic_write(path, bytes).await,
            Self::Owned { filesystem, .. } => filesystem.write(path, bytes).await,
        }
    }
    pub(super) async fn remove(&self, path: &Path) -> std::io::Result<()> {
        match self {
            Self::Legacy => tokio::fs::remove_file(path).await,
            Self::Owned { filesystem, .. } => filesystem.remove(path).await,
        }
    }
}

#[cfg(test)]
pub(super) struct ScopeDrop(pub(super) Option<Arc<std::sync::atomic::AtomicBool>>);
#[cfg(test)]
impl Drop for ScopeDrop {
    fn drop(&mut self) {
        if let Some(dropped) = &self.0 {
            dropped.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// Dropping the caller's JoinHandle leaves this job running with its locks.
/// Runtime shutdown may stop later phases; each started filesystem job owns its locks.
pub(super) async fn complete_owned<T: Send + 'static>(
    job: impl std::future::Future<Output = Result<T, SessionInboxError>> + Send + 'static,
) -> Result<T, SessionInboxError> {
    tokio::spawn(job)
        .await
        .map_err(|_| invalid("owned Inbox transaction outcome unconfirmed"))?
}

impl FileSessionInbox {
    pub(super) async fn owned_filesystem(
        &self,
        target: &str,
    ) -> Result<(PathBuf, OwnedFilesystem), SessionInboxError> {
        let lifecycle = self.lock_lifecycle().await?;
        self.filesystem_with_authority(target, InboxAuthority::Lifecycle { _guard: lifecycle })
            .await
    }

    pub(super) async fn filesystem_with_authority(
        &self,
        target: &str,
        authority: InboxAuthority,
    ) -> Result<(PathBuf, OwnedFilesystem), SessionInboxError> {
        let dir = self.inbox_dir(target).await?;
        let process = self.lock_process(&dir).await;
        let scope = Arc::new(OwnedScope {
            _process: process,
            _authority: authority,
        });
        let path = dir.clone();
        #[cfg(test)]
        let hook = self.owned_fs_hook.clone();
        let guards = tokio::task::spawn_blocking(move || {
            // Acquisition itself owns the already-acquired L/process scope.
            let _scope = &scope;
            #[cfg(test)]
            if let Some(hook) = &hook {
                hook("acquire", &path)?;
            }
            std::fs::create_dir_all(&path)?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path.join(OPERATION_LOCK_FILE))?;
            file.lock_exclusive()?;
            Ok::<_, std::io::Error>(Arc::new(OwnedGuards {
                _file: FileOperationLock(file),
                _scope: scope,
            }))
        })
        .await
        .map_err(|error| {
            SessionInboxError::Storage(format!("join owned inbox lock task: {error}"))
        })?
        .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        Ok((
            dir,
            OwnedFilesystem {
                guards,
                #[cfg(test)]
                hook: self.owned_fs_hook.clone(),
            },
        ))
    }

    async fn watermark_version(dir: &Path, file: &str) -> Result<u32, SessionInboxError> {
        match tokio::fs::read_to_string(dir.join(file)).await {
            Ok(raw) => {
                if raw.trim().parse::<u64>().is_ok() {
                    return Ok(0);
                }
                let value: VersionedActivationWatermark =
                    serde_json::from_str(&raw).map_err(|_| invalid("invalid Inbox watermark"))?;
                if !matches!(value.version, 2 | 3) {
                    return Err(invalid("unsupported Inbox watermark version"));
                }
                if file == ACTIVATION_GENERATION_FILE
                    && value.version == 3
                    && value.interrupt_snapshot.is_none()
                {
                    return Err(invalid("owned watermark lacks interrupt snapshot"));
                }
                Ok(value.version)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            // The legacy two-publication fault boundary may deliberately make
            // ACT a directory. Preserve its preceding INT publication; actual
            // ACT reads still fail, and INT3 below forbids owned downgrade.
            Err(error)
                if file == ACTIVATION_GENERATION_FILE
                    && error.kind() == ErrorKind::IsADirectory =>
            {
                Ok(0)
            }
            Err(error) => Err(SessionInboxError::Storage(error.to_string())),
        }
    }

    pub(super) async fn owned_enabled(dir: &Path) -> Result<bool, SessionInboxError> {
        let activation = Self::watermark_version(dir, ACTIVATION_GENERATION_FILE).await?;
        let interrupt = Self::watermark_version(dir, INTERRUPT_GENERATION_FILE).await?;
        if interrupt == 3 && activation != 3 {
            return Err(invalid("owned activation watermark downgraded"));
        }
        Ok(activation == 3)
    }

    pub(super) async fn activation_interrupt_snapshot(
        dir: &Path,
    ) -> Result<Option<u64>, SessionInboxError> {
        if Self::watermark_version(dir, ACTIVATION_GENERATION_FILE).await? != 3 {
            return Ok(None);
        }
        let bytes = tokio::fs::read(dir.join(ACTIVATION_GENERATION_FILE))
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let header: VersionedActivationWatermark =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid owned watermark"))?;
        header
            .interrupt_snapshot
            .map(Some)
            .ok_or_else(|| invalid("owned watermark lacks interrupt snapshot"))
    }

    async fn ensure_owned_format(
        &self,
        dir: &Path,
        filesystem: &OwnedFilesystem,
    ) -> Result<(), SessionInboxError> {
        let enabled = Self::owned_enabled(dir).await?;
        if enabled && Self::watermark_version(dir, INTERRUPT_GENERATION_FILE).await? == 3 {
            return Ok(());
        }
        if !enabled {
            for queue in ["cur", "new"] {
                for (_, _, path) in Self::owned_queue_entries(dir, queue, filesystem).await? {
                    let (_, lease) = self.read_owned_transport(&path).await?;
                    if lease.is_some() {
                        return Err(invalid("owned lease requires v3 watermarks"));
                    }
                }
            }
        }
        let (generation, _) = Self::read_activation_watermark(dir).await?;
        let interrupt = Self::read_interrupt_generation(dir).await?;
        // Commit both authorities in ACT3 first. During the upgrade window,
        // only its snapshot supplies interrupt authority; an old Interrupt
        // writer may change the legacy integer but cannot change this proof.
        for (file, generation) in [
            (ACTIVATION_GENERATION_FILE, generation),
            (INTERRUPT_GENERATION_FILE, interrupt),
        ] {
            let bytes = serde_json::to_vec(&VersionedActivationWatermark {
                version: 3,
                generation,
                interrupt_snapshot: (file == ACTIVATION_GENERATION_FILE).then_some(interrupt),
            })
            .map_err(|_| invalid("encode Inbox watermark"))?;
            filesystem
                .write(&dir.join(file), &bytes)
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            #[cfg(test)]
            if file == ACTIVATION_GENERATION_FILE && self.owned_after_header_failure {
                return Err(invalid("injected Inbox header upgrade failure"));
            }
        }
        Ok(())
    }

    pub(super) async fn write_interrupt_watermark(
        dir: &Path,
        generation: u64,
        filesystem: &OwnedFilesystem,
    ) -> Result<(), SessionInboxError> {
        let bytes = if Self::owned_enabled(dir).await? {
            serde_json::to_vec(&VersionedActivationWatermark {
                version: 3,
                generation,
                interrupt_snapshot: None,
            })
            .map_err(|_| invalid("encode Inbox watermark"))?
        } else {
            generation.to_string().into_bytes()
        };
        filesystem
            .write(&dir.join(INTERRUPT_GENERATION_FILE), &bytes)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))
    }

    pub(super) fn decode_owned_wrapper(
        bytes: &[u8],
    ) -> Result<(InboxMessage, Option<StoredLease>), SessionInboxError> {
        let mut value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| invalid("invalid Inbox wrapper"))?;
        let lease = StoredLease::from_value(&value)?;
        let owned = value.get("kind").and_then(serde_json::Value::as_str) == Some(OWNED_KIND);
        if owned != lease.is_some() {
            return Err(invalid("Inbox wrapper lease format mismatch"));
        }
        if owned {
            value["kind"] = serde_json::json!("session_envelope");
        }
        let wrapper =
            serde_json::from_value(value).map_err(|_| invalid("invalid Inbox wrapper"))?;
        Ok((wrapper, lease))
    }

    async fn read_owned_transport(
        &self,
        path: &Path,
    ) -> Result<(InboxMessage, Option<StoredLease>), SessionInboxError> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let limit = self.max_transport_bytes();
        if file
            .metadata()
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?
            .len()
            > limit as u64
        {
            return Err(invalid("Inbox lease transport exceeds byte limit"));
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        if bytes.len() > limit {
            return Err(invalid("Inbox lease transport exceeds byte limit"));
        }
        Self::decode_owned_wrapper(&bytes)
    }

    async fn write_owned_transport(
        &self,
        path: &Path,
        wrapper: &InboxMessage,
        lease: &StoredLease,
        filesystem: &OwnedFilesystem,
    ) -> Result<(), SessionInboxError> {
        let mut value =
            serde_json::to_value(wrapper).map_err(|_| invalid("encode Inbox wrapper"))?;
        value["kind"] = serde_json::json!(OWNED_KIND);
        value[LEASE_KEY] =
            serde_json::to_value(lease).map_err(|_| invalid("encode Inbox lease"))?;
        let bytes = serde_json::to_vec_pretty(&value).map_err(|_| invalid("encode Inbox lease"))?;
        if bytes.len() > self.max_transport_bytes() {
            return Err(invalid("Inbox lease transport exceeds byte limit"));
        }
        filesystem
            .write(path, &bytes)
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))
    }

    fn owned_name(generation: u64, token: &SessionInboxLeaseToken) -> String {
        format!(
            "{generation:020}-owned-{}-{}.json",
            token.epoch, token.incarnation
        )
    }

    fn owned_claim(
        wrapper: InboxMessage,
        generation: u64,
        claim_id: String,
        lease: StoredLease,
        target: &str,
    ) -> Result<SessionInboxOwnedClaim, SessionInboxError> {
        if wrapper.kind != InboxKind::SessionEnvelope {
            return Err(invalid("unexpected Inbox kind"));
        }
        let envelope: SessionMessageEnvelope =
            serde_json::from_value(wrapper.body).map_err(|_| invalid("invalid Inbox envelope"))?;
        envelope
            .validate()
            .map_err(|_| invalid("invalid Inbox envelope"))?;
        if envelope.target_session_id != target {
            return Err(invalid("Inbox target mismatch"));
        }
        Ok(SessionInboxOwnedClaim {
            claim: SessionInboxClaim {
                envelope,
                generation,
                activation_policy: lease.policy,
                claim_id,
            },
            lease: lease.token,
        })
    }

    pub(super) async fn claim_owned_impl(
        &self,
        target: &str,
        limit: usize,
        run: Option<&str>,
        request: &SessionInboxLeaseRequest,
    ) -> Result<Vec<SessionInboxOwnedClaim>, SessionInboxError> {
        let expires_at = request.expires_at()?;
        #[cfg(test)]
        let _scope_drop = ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target).await?;
        for queue in ["new", "cur", "corrupt"] {
            filesystem
                .create_dir(&dir.join(queue))
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        }
        self.ensure_owned_format(&dir, &filesystem).await?;
        let prefix = Self::read_activation_generation(&dir).await?;
        let interrupt = Self::read_interrupt_generation(&dir).await?;
        let mut entries = Self::owned_queue_entries(&dir, "cur", &filesystem).await?;
        entries.extend(Self::owned_queue_entries(&dir, "new", &filesystem).await?);
        entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        let mut result = Vec::new();
        for (generation, _, path) in entries {
            if result.len() >= limit.min(self.limits.max_claim_batch) {
                break;
            }
            let (wrapper, stored) = self.read_owned_transport(&path).await?;
            let intent = Self::activation_intent(&wrapper.body)?;
            if !Self::eligible(generation, prefix, intent) {
                continue;
            }
            let envelope: SessionMessageEnvelope = serde_json::from_value(wrapper.body.clone())
                .map_err(|_| invalid("invalid Inbox envelope"))?;
            envelope
                .validate()
                .map_err(|_| invalid("invalid Inbox envelope"))?;
            if envelope.target_session_id != target {
                return Err(invalid("Inbox target mismatch"));
            }
            // Receipt publication is terminal even if a process stopped before
            // removing cur. Never mint a successor over this admitted input.
            if let Some(receipt) = Self::admitted_receipt(&dir, &envelope).await? {
                if receipt.delivery.generation != generation
                    || receipt.intent != intent
                    || !StoredLease::same_optional_identity(receipt.lease.as_ref(), stored.as_ref())
                {
                    return Err(invalid("Inbox terminal lease mismatch"));
                }
                filesystem
                    .remove(&path)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
                continue;
            }
            if run.is_some_and(|run| envelope.guidance_waits_for_run(run)) {
                continue;
            }
            let lease = match stored {
                Some(lease) if lease.token.expires_at > request.now => {
                    if lease.token.consumer != request.consumer {
                        continue;
                    }
                    lease
                }
                previous => StoredLease {
                    version: 1,
                    policy: match previous.as_ref() {
                        Some(lease) => lease.policy,
                        None => Self::effective_activation_policy(
                            generation, prefix, interrupt, intent,
                        )?,
                    },
                    token: SessionInboxLeaseToken {
                        consumer: request.consumer.clone(),
                        epoch: previous
                            .as_ref()
                            .map_or(Some(1), |lease| lease.token.epoch.checked_add(1))
                            .ok_or_else(|| invalid("Inbox lease epoch exhausted"))?,
                        expires_at,
                        incarnation: uuid::Uuid::new_v4().to_string(),
                    },
                },
            };
            let name = Self::owned_name(generation, &lease.token);
            let canonical = dir.join("cur").join(&name);
            // The unknown kind first fences an old already-held ACK at the
            // old path. Rename then fences it at the path boundary as well.
            self.write_owned_transport(&path, &wrapper, &lease, &filesystem)
                .await?;
            #[cfg(test)]
            if self.owned_after_write_failure {
                return Err(invalid("injected Inbox lease rename failure"));
            }
            if path != canonical {
                if tokio::fs::try_exists(&canonical)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?
                {
                    return Err(invalid("Inbox lease incarnation already exists"));
                }
                filesystem
                    .rotate(&path, &canonical)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            }
            result.push(Self::owned_claim(wrapper, generation, name, lease, target)?);
        }
        Ok(result)
    }

    // Called only inside a complete std job that owns the actual joint guard.
    // Existing v3 decoder/caps and lease identity remain the authority.
    pub(crate) fn locked_input_claim(
        &self,
        dir: &Path,
        target: &str,
        claim: &SessionInboxOwnedClaim,
        now: DateTime<Utc>,
    ) -> Result<(SessionMessageEnvelope, Option<SessionInboxActivationIntent>), SessionInboxError>
    {
        use std::io::Read;
        let storage = |error: std::io::Error| SessionInboxError::Storage(error.to_string());
        let read = |path: &Path| -> Result<Vec<u8>, SessionInboxError> {
            let file = File::open(path).map_err(storage)?;
            let limit = self.max_transport_bytes();
            if !file.metadata().map_err(storage)?.is_file() {
                return Err(invalid("invalid owned input file"));
            }
            let mut bytes = Vec::new();
            file.take(limit as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            if bytes.len() > limit {
                return Err(invalid("Inbox lease transport exceeds byte limit"));
            }
            Ok(bytes)
        };
        Self::validate_claim_name(&claim.claim.claim_id)?;
        let activation: VersionedActivationWatermark =
            serde_json::from_slice(&read(&dir.join(ACTIVATION_GENERATION_FILE))?)
                .map_err(|_| invalid("invalid owned activation watermark"))?;
        let interrupt: VersionedActivationWatermark =
            serde_json::from_slice(&read(&dir.join(INTERRUPT_GENERATION_FILE))?)
                .map_err(|_| invalid("invalid owned interrupt watermark"))?;
        if activation.version != 3
            || activation.interrupt_snapshot.is_none()
            || interrupt.version != 3
        {
            return Err(invalid("owned input requires v3 watermarks"));
        }
        let (wrapper, stored) =
            Self::decode_owned_wrapper(&read(&dir.join("cur").join(&claim.claim.claim_id))?)?;
        let stored = stored.ok_or_else(|| invalid("Inbox lease missing"))?;
        let intent = Self::activation_intent(&wrapper.body)?;
        if !stored.same_identity(&claim.lease)
            || stored.policy != claim.claim.activation_policy
            || stored.token.expires_at <= now
            || Self::owned_name(claim.claim.generation, &stored.token) != claim.claim.claim_id
            || !Self::eligible(claim.claim.generation, activation.generation, intent)
        {
            return Err(invalid("Inbox claim lost, expired or ineligible"));
        }
        let actual = Self::owned_claim(
            wrapper,
            claim.claim.generation,
            claim.claim.claim_id.clone(),
            stored,
            target,
        )?;
        if actual.claim != claim.claim {
            return Err(invalid("Inbox claim mismatch"));
        }
        match std::fs::symlink_metadata(Self::admitted_path(dir, &actual.claim.envelope.id)) {
            Ok(_) => return Err(invalid("Inbox claim is already terminal")),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
        }
        Ok((actual.claim.envelope, intent))
    }

    async fn current_owned(
        &self,
        dir: &Path,
        target: &str,
        claim: &SessionInboxOwnedClaim,
        now: DateTime<Utc>,
    ) -> Result<(InboxMessage, StoredLease), SessionInboxError> {
        Self::validate_claim_name(&claim.claim.claim_id)?;
        if claim.claim.envelope.target_session_id != target {
            return Err(invalid("Inbox target mismatch"));
        }
        if !Self::owned_enabled(dir).await? {
            return Err(invalid("owned lease requires v3 watermarks"));
        }
        let (wrapper, stored) = self
            .read_owned_transport(&dir.join("cur").join(&claim.claim.claim_id))
            .await?;
        let stored = stored.ok_or_else(|| invalid("Inbox lease missing"))?;
        if !stored.same_identity(&claim.lease)
            || stored.policy != claim.claim.activation_policy
            || stored.token.expires_at <= now
            || Self::owned_name(claim.claim.generation, &stored.token) != claim.claim.claim_id
        {
            return Err(invalid("Inbox lease lost or expired"));
        }
        let actual = Self::owned_claim(
            wrapper.clone(),
            claim.claim.generation,
            claim.claim.claim_id.clone(),
            stored.clone(),
            target,
        )?;
        if actual.claim != claim.claim {
            return Err(invalid("Inbox claim mismatch"));
        }
        Ok((wrapper, stored))
    }

    pub(super) async fn renew_owned_impl(
        &self,
        target: &str,
        claim: &SessionInboxOwnedClaim,
        request: &SessionInboxLeaseRequest,
    ) -> Result<SessionInboxOwnedClaim, SessionInboxError> {
        let expires_at = request.expires_at()?;
        if request.consumer != claim.lease.consumer {
            return Err(invalid("Inbox lease consumer mismatch"));
        }
        #[cfg(test)]
        let _scope_drop = ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target).await?;
        let (wrapper, mut stored) = self.current_owned(&dir, target, claim, request.now).await?;
        #[cfg(test)]
        if let Some((entered, release)) = &self.owned_renew_pause {
            entered.notify_one();
            release.notified().await;
        }
        stored.token.expires_at = stored.token.expires_at.max(expires_at);
        self.write_owned_transport(
            &dir.join("cur").join(&claim.claim.claim_id),
            &wrapper,
            &stored,
            &filesystem,
        )
        .await?;
        Self::owned_claim(
            wrapper,
            claim.claim.generation,
            claim.claim.claim_id.clone(),
            stored,
            target,
        )
    }

    pub(super) async fn ack_owned_impl(
        &self,
        target: &str,
        claim: &SessionInboxOwnedClaim,
        now: DateTime<Utc>,
    ) -> Result<(), SessionInboxError> {
        Self::validate_claim_name(&claim.claim.claim_id)?;
        if claim.claim.envelope.target_session_id != target {
            return Err(invalid("Inbox target mismatch"));
        }
        #[cfg(test)]
        let _scope_drop = ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target).await?;
        // Check the terminal proof first; an exact ACK retry remains terminal
        // even after the lease would expire. A stale epoch cannot borrow it.
        if let Some(receipt) = Self::admitted_receipt(&dir, &claim.claim.envelope).await? {
            if receipt.delivery.generation != claim.claim.generation
                || receipt.lease.as_ref().is_none_or(|lease| {
                    !lease.same_identity(&claim.lease)
                        || lease.policy != claim.claim.activation_policy
                })
                || Self::owned_name(claim.claim.generation, &claim.lease) != claim.claim.claim_id
            {
                return Err(invalid("Inbox terminal lease mismatch"));
            }
            let lease = receipt.lease.as_ref().expect("validated terminal lease");
            return self
                .ack_unlocked(
                    &dir,
                    target,
                    &claim.claim,
                    AckAuthority::Owned {
                        lease,
                        filesystem: &filesystem,
                    },
                )
                .await;
        }
        let (_, stored) = self.current_owned(&dir, target, claim, now).await?;
        #[cfg(test)]
        if let Some((entered, release)) = &self.owned_ack_pause {
            entered.notify_one();
            release.notified().await;
        }
        self.ack_unlocked(
            &dir,
            target,
            &claim.claim,
            AckAuthority::Owned {
                lease: &stored,
                filesystem: &filesystem,
            },
        )
        .await
    }

    pub(super) async fn inspect_owned_impl(
        &self,
        target: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<SessionInboxLeaseInspection>, SessionInboxError> {
        #[cfg(test)]
        let _scope_drop = ScopeDrop(self.owned_scope_drop.clone());
        let (dir, filesystem) = self.owned_filesystem(target).await?;
        let mut entries = Self::owned_queue_entries(&dir, "cur", &filesystem).await?;
        entries.extend(Self::owned_queue_entries(&dir, "new", &filesystem).await?);
        entries.sort_by_key(|entry| entry.0);
        let mut result = Vec::new();
        for (generation, _, path) in entries {
            if result.len() >= limit.min(self.limits.max_claim_batch) {
                break;
            }
            let (_, lease) = self.read_owned_transport(&path).await?;
            if let Some(lease) = lease {
                if !Self::owned_enabled(&dir).await? {
                    return Err(invalid("owned lease requires v3 watermarks"));
                }
                result.push(SessionInboxLeaseInspection {
                    generation,
                    epoch: lease.token.epoch,
                    expires_at: lease.token.expires_at,
                    expired: lease.token.expires_at <= now,
                    reclaim_count: lease.token.epoch - 1,
                });
            }
        }
        Ok(result)
    }
}
