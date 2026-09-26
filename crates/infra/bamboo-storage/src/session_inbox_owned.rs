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

/// Dropping the caller's JoinHandle leaves this job running with its locks.
/// The Tokio runtime itself must outlive the job (see #1341).
pub(super) async fn complete_owned<T: Send + 'static>(
    job: impl std::future::Future<Output = Result<T, SessionInboxError>> + Send + 'static,
) -> Result<T, SessionInboxError> {
    tokio::spawn(job)
        .await
        .map_err(|_| invalid("owned Inbox transaction outcome unconfirmed"))?
}

impl FileSessionInbox {
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

    async fn ensure_owned_format(&self, dir: &Path) -> Result<(), SessionInboxError> {
        let enabled = Self::owned_enabled(dir).await?;
        if enabled && Self::watermark_version(dir, INTERRUPT_GENERATION_FILE).await? == 3 {
            return Ok(());
        }
        if !enabled {
            for queue in ["cur", "new"] {
                for (_, _, path) in Self::valid_queue_entries(dir, queue).await? {
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
            atomic_write(&dir.join(file), &bytes)
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
        atomic_write(&dir.join(INTERRUPT_GENERATION_FILE), &bytes)
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
        atomic_write(path, &bytes)
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
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target).await?;
        let _guard = self.lock_operation(&dir).await?;
        Mailbox::at(&dir)
            .ensure_dirs()
            .await
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        self.ensure_owned_format(&dir).await?;
        let prefix = Self::read_activation_generation(&dir).await?;
        let interrupt = Self::read_interrupt_generation(&dir).await?;
        let mut entries = Self::valid_queue_entries(&dir, "cur").await?;
        entries.extend(Self::valid_queue_entries(&dir, "new").await?);
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
                tokio::fs::remove_file(&path)
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
            self.write_owned_transport(&path, &wrapper, &lease).await?;
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
                tokio::fs::rename(&path, &canonical)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            }
            result.push(Self::owned_claim(wrapper, generation, name, lease, target)?);
        }
        Ok(result)
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
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target).await?;
        let _guard = self.lock_operation(&dir).await?;
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
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target).await?;
        let _guard = self.lock_operation(&dir).await?;
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
            return self
                .ack_unlocked(&dir, target, &claim.claim, receipt.lease.as_ref())
                .await;
        }
        let (_, stored) = self.current_owned(&dir, target, claim, now).await?;
        #[cfg(test)]
        if let Some((entered, release)) = &self.owned_ack_pause {
            entered.notify_one();
            release.notified().await;
        }
        self.ack_unlocked(&dir, target, &claim.claim, Some(&stored))
            .await
    }

    pub(super) async fn inspect_owned_impl(
        &self,
        target: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<SessionInboxLeaseInspection>, SessionInboxError> {
        let _lifecycle = self.lock_lifecycle().await?;
        let dir = self.inbox_dir(target).await?;
        let _guard = self.lock_operation(&dir).await?;
        let mut entries = Self::valid_queue_entries(&dir, "cur").await?;
        entries.extend(Self::valid_queue_entries(&dir, "new").await?);
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
