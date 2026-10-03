//! Versioned offline authority transfer. The source HEAD is retired before a
//! copy exists; destination publication stays read-only until the source HEAD
//! durably consumes that exact activation. No runtime or network under locks.
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::{canonical_bytes, content_hash, service::validate_snapshot, store::FileStore, *};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationRequest {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_commit: String,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub source_root: String,
    pub destination_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_snapshot_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStage {
    SourceRetired,
    SourceTransferred,
    DestinationActivated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationReceipt {
    pub request: MigrationRequest,
    pub principal: String,
    pub request_hash: String,
    pub stage: MigrationStage,
    pub retired_seq: u64,
    pub source_retired_commit: Option<String>,
    pub activation_commit: Option<String>,
    pub activated_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_snapshot: Option<Artifact>,
}

/// Only the trusted embedding Host may construct this after acquiring its
/// stopped-host lifecycle guard and checking canonical runtime ownership.
/// A client `stopped: true`, expired slot, or lease is never a proof.
#[derive(Clone, Debug)]
pub struct StoppedScopeProof {
    binding: ScopeBinding,
    supervisor_snapshot: Option<Vec<u8>>,
}

impl StoppedScopeProof {
    pub fn from_verified_host(binding: ScopeBinding) -> Self {
        Self {
            binding,
            supervisor_snapshot: None,
        }
    }
    pub fn with_verified_supervisor_snapshot(mut self, bytes: Vec<u8>) -> Self {
        self.supervisor_snapshot = Some(bytes);
        self
    }
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn validate_migration(snapshot: &Snapshot) -> Result<()> {
    let files = snapshot
        .assignments
        .values()
        .any(|a| a.effects.values().any(|e| e.artifact.is_some()));
    if files && snapshot.schema != 3 {
        return Err(Error::AuthorityUnavailable(
            "file effect ledger requires schema 3".into(),
        ));
    }
    match (&snapshot.migration, snapshot.schema) {
        (None, 1)
            if snapshot.legacy_attachment.is_none()
                && !snapshot.tickets.values().any(|w| {
                    w.import_source
                        .as_ref()
                        .is_some_and(|s| s.artifact.is_some())
                }) => {}
        (None, 2)
            if snapshot.legacy_attachment.is_some()
                || snapshot.tickets.values().any(|w| {
                    w.import_source
                        .as_ref()
                        .is_some_and(|s| s.artifact.is_some())
                }) => {}
        (None, 3) if files => {}
        (Some(r), 2 | 3) => {
            let activation_seq = r
                .retired_seq
                .checked_add(1)
                .ok_or_else(|| Error::AuthorityUnavailable("migration sequence overflow".into()))?;
            if r.request.binding != snapshot.binding
                || r.principal.is_empty()
                || !valid_hash(&r.request.expected_commit)
                || r.request.operation_id.is_empty()
                || r.request.operation_id.len() > 256
                || r.request_hash != content_hash(&canonical_bytes(&r.request)?)
                || r.retired_seq
                    != r.request
                        .expected_seq
                        .checked_add(1)
                        .ok_or(Error::RevisionConflict)?
                || r.request.source_root == r.request.destination_root
                || !Path::new(&r.request.source_root).is_absolute()
                || !Path::new(&r.request.destination_root).is_absolute()
                || r.supervisor_snapshot.as_ref().map(|a| &a.sha256)
                    != r.request.supervisor_snapshot_hash.as_ref()
            {
                return Err(Error::AuthorityUnavailable(
                    "invalid migration receipt".into(),
                ));
            }
            let receipt = snapshot
                .receipts
                .get(&r.request.operation_id)
                .ok_or_else(|| {
                    Error::AuthorityUnavailable(
                        "migration lacks immutable operation receipt".into(),
                    )
                })?;
            if receipt.principal != r.principal
                || receipt.request_hash != r.request_hash
                || receipt.committed_seq != r.retired_seq
                || receipt.canonical_request.as_bytes() != canonical_bytes(&r.request)?
            {
                return Err(Error::AuthorityUnavailable(
                    "migration operation receipt mismatch".into(),
                ));
            }
            match r.stage {
                MigrationStage::SourceRetired
                    if snapshot.seq == r.retired_seq
                        && snapshot.authority_epoch == r.request.expected_epoch
                        && r.source_retired_commit.is_none()
                        && r.activation_commit.is_none()
                        && r.activated_seq.is_none() => {}
                MigrationStage::SourceTransferred
                    if snapshot.seq == activation_seq
                        && snapshot.authority_epoch == r.request.expected_epoch
                        && r.source_retired_commit.as_deref().is_some_and(valid_hash)
                        && r.activation_commit.as_deref().is_some_and(valid_hash)
                        && r.activated_seq == Some(activation_seq) => {}
                MigrationStage::DestinationActivated
                    if snapshot.seq >= activation_seq
                        && snapshot.authority_epoch > r.request.expected_epoch
                        && r.source_retired_commit.as_deref().is_some_and(valid_hash)
                        && r.activation_commit.is_none()
                        && r.activated_seq == Some(activation_seq) => {}
                _ => {
                    return Err(Error::AuthorityUnavailable(
                        "migration stage/header mismatch".into(),
                    ))
                }
            }
        }
        _ => {
            return Err(Error::AuthorityUnavailable(
                "unsupported or incomplete snapshot schema".into(),
            ))
        }
    }
    for source in snapshot
        .tickets
        .values()
        .filter_map(|w| w.import_source.as_ref())
        .chain(snapshot.legacy_attachment.iter())
    {
        if let Some(a) = &source.artifact {
            if !valid_hash(&source.snapshot_hash)
                || a.sha256 != source.snapshot_hash
                || a.uri
                    != format!(
                        "{}{hash}",
                        crate::store::MANAGED_ARTIFACT_PREFIX,
                        hash = source.snapshot_hash
                    )
            {
                return Err(Error::AuthorityUnavailable(
                    "legacy source Artifact/hash mismatch".into(),
                ));
            }
        }
    }
    if let Some(a) = snapshot
        .migration
        .as_ref()
        .and_then(|r| r.supervisor_snapshot.as_ref())
    {
        if !valid_hash(&a.sha256)
            || a.uri != format!("{}{}", crate::store::MANAGED_ARTIFACT_PREFIX, a.sha256)
        {
            return Err(Error::AuthorityUnavailable(
                "Supervisor snapshot Artifact invalid".into(),
            ));
        }
    }
    Ok(())
}

fn require_stopped(snapshot: &Snapshot) -> Result<()> {
    if snapshot.assignments.values().any(|a| {
        !a.process_stopped
            || a.effects
                .values()
                .any(|e| matches!(e.state, EffectState::Started | EffectState::OutcomeUnknown))
    }) {
        return Err(Error::ResourceBlocked(
            "owned executions or external effects are not confirmed stopped/reconciled".into(),
        ));
    }
    Ok(())
}

fn require_subject(
    authority: &Authority,
    binding: &ScopeBinding,
    proof: &StoppedScopeProof,
) -> Result<()> {
    authority.user()?;
    if &authority.binding != binding || &proof.binding != binding {
        return Err(Error::ScopeDenied(
            "offline migration binding/subject".into(),
        ));
    }
    Ok(())
}

fn verify_receipt(
    r: &MigrationReceipt,
    authority: &Authority,
    request: &MigrationRequest,
) -> Result<()> {
    if r.principal != authority.identity() {
        return Err(Error::ScopeDenied("migration receipt subject".into()));
    }
    if r.request != *request {
        return Err(Error::IdempotencyConflict);
    }
    Ok(())
}

/// Resolve a new destination without accepting traversal or a symlink final
/// component. The parent must already exist; migration never creates ancestors.
pub fn canonical_migration_destination(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .ok_or_else(|| Error::InvalidTransition("destination needs a directory name".into()))?;
    if path.exists() {
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(Error::ScopeDenied(
                "migration destination is a symlink".into(),
            ));
        }
        return Ok(path.canonicalize()?.to_string_lossy().into_owned());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(parent
        .canonicalize()?
        .join(name)
        .to_string_lossy()
        .into_owned())
}

impl TicketService {
    /// Inspection takes the writer OS lock but grants no runtime permission,
    /// increments no epoch, and requires an existing verified HEAD.
    pub fn open_offline(root: impl AsRef<Path>, binding: ScopeBinding) -> Result<Self> {
        if !root.as_ref().join("HEAD").exists() {
            return Err(Error::AuthorityUnavailable(
                "offline inspection requires existing HEAD".into(),
            ));
        }
        let mut store = FileStore::open(root.as_ref(), binding)?;
        let (_, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified offline snapshot".into()))?;
        validate_snapshot(snapshot)?;
        store.offline_original_writable = store.health == Health::Writable;
        store.health = Health::ReadOnly {
            reason: "offline inspection; no runtime permission".into(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
        })
    }

    pub fn retire_for_migration(
        &self,
        authority: &Authority,
        request: &MigrationRequest,
        proof: &StoppedScopeProof,
    ) -> Result<OperationReceipt> {
        let mut store = self.inner.lock().expect("store mutex");
        let (commit, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no published snapshot".into()))?;
        require_subject(authority, &snapshot.binding, proof)?;
        if request.binding != snapshot.binding
            || request.source_root != store.root().to_string_lossy()
        {
            return Err(Error::ScopeDenied("migration source/binding".into()));
        }
        let request_hash = content_hash(&canonical_bytes(request)?);
        if let Some(receipt) = snapshot.receipts.get(&request.operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("migration receipt subject".into()));
            }
            return if receipt.request_hash == request_hash {
                Ok(receipt.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if snapshot
            .migration
            .as_ref()
            .is_some_and(|r| r.stage != MigrationStage::DestinationActivated)
        {
            return Err(Error::AuthorityUnavailable(
                "authority already retired".into(),
            ));
        }
        if !store.offline_original_writable {
            return Err(Error::AuthorityUnavailable(
                "source is not a stopped writable original".into(),
            ));
        }
        if request.expected_commit != *commit
            || request.expected_seq != snapshot.seq
            || request.expected_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        snapshot.seq.checked_add(2).ok_or(Error::RevisionConflict)?;
        snapshot
            .authority_epoch
            .checked_add(1)
            .ok_or(Error::RevisionConflict)?;
        if request.destination_root
            != canonical_migration_destination(Path::new(&request.destination_root))?
            || request.destination_root == request.source_root
            || Path::new(&request.destination_root).exists()
        {
            return Err(Error::InvalidTransition(
                "migration requires one new canonical destination".into(),
            ));
        }
        require_stopped(snapshot)?;
        if request.supervisor_snapshot_hash.as_ref()
            != proof
                .supervisor_snapshot
                .as_ref()
                .map(|b| content_hash(b))
                .as_ref()
        {
            return Err(Error::ScopeDenied(
                "Supervisor snapshot proof/hash mismatch".into(),
            ));
        }
        let mut next = snapshot.clone();
        next.seq = next.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
        next.schema = next.schema.max(2); // Never downgrade a file effect ledger.
        let supervisor_snapshot = if let Some(bytes) = &proof.supervisor_snapshot {
            store.health = Health::Writable;
            let result = store.store_artifact(bytes);
            store.health = Health::ReadOnly {
                reason: "offline snapshot staging".into(),
            };
            Some(result?)
        } else {
            None
        };
        let record = MigrationReceipt {
            request: request.clone(),
            principal: authority.identity(),
            request_hash: request_hash.clone(),
            stage: MigrationStage::SourceRetired,
            retired_seq: next.seq,
            source_retired_commit: None,
            activation_commit: None,
            activated_seq: None,
            supervisor_snapshot,
        };
        let receipt = OperationReceipt {
            operation_id: request.operation_id.clone(),
            principal: authority.identity(),
            request_hash,
            canonical_request: String::from_utf8(canonical_bytes(request)?)
                .expect("canonical JSON"),
            committed_seq: next.seq,
            ids: Default::default(),
        };
        next.receipts
            .insert(request.operation_id.clone(), receipt.clone());
        next.migration = Some(record);
        validate_snapshot(&next)?;
        store.publish_offline(next)?;
        store.offline_original_writable = false;
        Ok(receipt)
    }

    /// Copies only the exact retired scope. Retry verifies every existing blob;
    /// an incomplete copy never becomes writable and corruption is not repaired.
    pub fn export_retired_migration(
        &self,
        authority: &Authority,
        request: &MigrationRequest,
        proof: &StoppedScopeProof,
    ) -> Result<String> {
        let store = self.inner.lock().expect("store mutex");
        let (_, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no published snapshot".into()))?;
        require_subject(authority, &snapshot.binding, proof)?;
        let r = snapshot
            .migration
            .as_ref()
            .ok_or_else(|| Error::InvalidTransition("source must be retired first".into()))?;
        verify_receipt(r, authority, request)?;
        if r.stage != MigrationStage::SourceRetired
            || request.source_root != store.root().to_string_lossy()
        {
            return Err(Error::InvalidTransition(
                "migration copy is already consumed or has wrong source".into(),
            ));
        }
        require_stopped(snapshot)?;
        store.export_migration(Path::new(&request.destination_root), &r.request_hash)
    }

    /// Both process locks remain held. Publish the new epoch behind the backup
    /// marker, consume that exact activation in source HEAD, then release marker.
    /// A crash at any boundary leaves no two writable authorities.
    pub fn activate_migrated_copy(
        &self,
        source: &TicketService,
        authority: &Authority,
        request: &MigrationRequest,
        proof: &StoppedScopeProof,
    ) -> Result<MigrationReceipt> {
        if Arc::ptr_eq(&self.inner, &source.inner) {
            return Err(Error::ScopeDenied("source equals destination".into()));
        }
        let mut original = source.inner.lock().expect("source store mutex");
        let (source_commit, source_snapshot) = original
            .published
            .clone()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified original".into()))?;
        require_subject(authority, &source_snapshot.binding, proof)?;
        let r = source_snapshot
            .migration
            .as_ref()
            .ok_or_else(|| Error::InvalidTransition("original was not retired".into()))?;
        verify_receipt(r, authority, request)?;
        if r.stage == MigrationStage::DestinationActivated
            || request.source_root != original.root().to_string_lossy()
        {
            return Err(Error::ScopeDenied(
                "original retirement proof required".into(),
            ));
        }
        require_stopped(&source_snapshot)?;
        let mut destination = self.inner.lock().expect("destination store mutex");
        if request.destination_root != destination.root().to_string_lossy() {
            return Err(Error::ScopeDenied(
                "copy is not the bound destination".into(),
            ));
        }
        let (mut commit, mut snapshot) = destination
            .published
            .clone()
            .ok_or_else(|| Error::AuthorityUnavailable("copy incomplete".into()))?;
        require_subject(authority, &snapshot.binding, proof)?;
        let copied = snapshot
            .migration
            .as_ref()
            .ok_or_else(|| Error::InvalidTransition("ordinary backups remain read-only".into()))?;
        verify_receipt(copied, authority, request)?;
        let retired_commit = r.source_retired_commit.clone().unwrap_or(source_commit);
        if copied.stage == MigrationStage::SourceRetired {
            if r.stage != MigrationStage::SourceRetired
                || commit != retired_commit
                || !destination.root().join("BACKUP_READ_ONLY").exists()
            {
                return Err(Error::ScopeDenied(
                    "copy is not the full retired HEAD".into(),
                ));
            }
            require_stopped(&snapshot)?;
            snapshot.seq = snapshot.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
            snapshot.authority_epoch = snapshot
                .authority_epoch
                .checked_add(1)
                .ok_or(Error::RevisionConflict)?;
            let mut activated = r.clone();
            activated.stage = MigrationStage::DestinationActivated;
            activated.source_retired_commit = Some(retired_commit.clone());
            activated.activated_seq = Some(snapshot.seq);
            snapshot.migration = Some(activated);
            for q in snapshot.requests.values_mut() {
                if matches!(q.kind, RequestKind::Approval { .. })
                    && matches!(q.status, RequestStatus::Open | RequestStatus::Approved)
                {
                    q.status = RequestStatus::Expired;
                    q.updated_seq = snapshot.seq;
                }
            }
            validate_snapshot(&snapshot)?;
            commit = destination.publish_offline(snapshot.clone())?;
        } else if copied.stage != MigrationStage::DestinationActivated
            || copied.source_retired_commit.as_ref() != Some(&retired_commit)
        {
            return Err(Error::ScopeDenied("copy activation mismatch".into()));
        }
        let activation_commit = if r.stage == MigrationStage::SourceTransferred {
            let frozen = r
                .activation_commit
                .as_ref()
                .ok_or_else(|| Error::AuthorityUnavailable("missing frozen activation".into()))?;
            let mut ancestor = Some(commit.clone());
            while ancestor.as_ref().is_some_and(|c| c != frozen) {
                ancestor = destination.parent_commit(ancestor.as_ref().expect("ancestor"))?;
            }
            if ancestor.is_none() {
                return Err(Error::ScopeDenied(
                    "activation is not original consumed lineage".into(),
                ));
            }
            frozen.clone()
        } else {
            commit.clone()
        };
        if r.stage == MigrationStage::SourceRetired {
            let mut transferred = source_snapshot;
            transferred.seq += 1;
            let receipt = transferred.migration.as_mut().expect("retirement receipt");
            receipt.stage = MigrationStage::SourceTransferred;
            receipt.source_retired_commit = Some(retired_commit);
            receipt.activation_commit = Some(activation_commit);
            receipt.activated_seq = Some(
                snapshot
                    .migration
                    .as_ref()
                    .expect("activated receipt")
                    .retired_seq
                    + 1,
            );
            validate_snapshot(&transferred)?;
            original.publish_offline(transferred)?;
        }
        destination.release_backup()?;
        Ok(snapshot.migration.expect("activated receipt"))
    }
}
