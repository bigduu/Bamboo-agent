//! SessionStoreV2's durable per-Session actor activation authority.
//!
//! The ordinary Session remains the identity/transcript source of truth. This
//! small sidecar is a versioned, serialized CAS record for execution attempts.
//! It is never derived from a physical worker or broker mailbox.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::{
    ActorActivation, ActorActivationClaim, ActorActivationFence, ActorActivationFinish,
    ActorActivationStatus, ActorAncestorObservation, ActorControlPlaneBinding, ActorDirectoryEntry,
    ActorDirectoryError, ActorDirectoryPort, ActorLogicalState, ActorSession, ProjectId, Session,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use super::{
    durable_atomic_write_blocking, validate_session_id, ActorTreeWriteGuard,
    RuntimeTaskTransactionReadGuard, SessionLifecycleReadGuard, SessionStoreV2, SessionWriteGuard,
};

/// Every started filesystem job keeps the same physical and in-process locks
/// alive, even when its async caller or Tokio runtime stops waiting. Field
/// order releases the locks in reverse acquisition order.
struct ActorAuthorityGuards {
    _tree: std::sync::Mutex<Option<ActorTreeWriteGuard>>,
    tree_root: std::sync::Mutex<Option<bamboo_domain::Session>>,
    tree_bumped: std::sync::atomic::AtomicBool,
    _session: SessionWriteGuard,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: SessionLifecycleReadGuard,
}

const ACTOR_AUTHORITY_FILE: &str = "actor-authority.json";
const ACTOR_INITIALIZED_FILE: &str = "actor-authority.initialized.json";

/// Independent evidence that an authority record was fully initialized before
/// the first claim. Losing the mutable sidecar must never restart attempt 0.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorInitializedMarker {
    schema_version: u32,
    actor_id: String,
    session_created_at: DateTime<Utc>,
}

/// Pure census validation only; never initialize, refresh or claim an Actor.
pub(super) fn validate_census_witnesses(
    record: &[u8],
    marker: &[u8],
    session: &Session,
    project: Option<&ProjectId>,
) -> io::Result<()> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid census Actor witness");
    let record: ActorDirectoryEntry = serde_json::from_slice(record).map_err(|_| invalid())?;
    let marker: ActorInitializedMarker = serde_json::from_slice(marker).map_err(|_| invalid())?;
    if record.validate().is_err()
        || !record.actor.matches_session(session)
        || record.actor.project_id.as_deref() != project.map(ProjectId::as_str)
        || marker.schema_version != bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION
        || marker.actor_id != session.id
        || marker.session_created_at != session.created_at
    {
        return Err(invalid());
    }
    Ok(())
}

fn storage(error: io::Error) -> ActorDirectoryError {
    ActorDirectoryError::Storage(error.to_string())
}

fn checked_next(value: u64) -> Result<u64, ActorDirectoryError> {
    value
        .checked_add(1)
        .ok_or(ActorDirectoryError::CounterOverflow)
}

pub(super) fn current_live<'a>(
    entry: &'a ActorDirectoryEntry,
    fence: &ActorActivationFence,
    now: DateTime<Utc>,
) -> Result<&'a ActorActivation, ActorDirectoryError> {
    let activation = entry
        .activation
        .as_ref()
        .ok_or(ActorDirectoryError::StaleFence)?;
    if entry.actor.state != ActorLogicalState::Active
        || !activation.status.is_live()
        || activation.lease_expires_at <= now
        || !activation.matches_fence(fence)
    {
        return Err(ActorDirectoryError::StaleFence);
    }
    Ok(activation)
}

struct Mutation<T> {
    value: T,
    changed: bool,
}

impl<T> Mutation<T> {
    fn changed(value: T) -> Self {
        Self {
            value,
            changed: true,
        }
    }

    fn unchanged(value: T) -> Self {
        Self {
            value,
            changed: false,
        }
    }
}

impl SessionStoreV2 {
    async fn regular_actor_directory(path: &Path) -> Result<bool, ActorDirectoryError> {
        match fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
            Ok(_) => Err(ActorDirectoryError::Corrupt),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }

    async fn regular_actor_file(path: &Path) -> Result<bool, ActorDirectoryError> {
        match fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_file() => Ok(true),
            Ok(_) => Err(ActorDirectoryError::Corrupt),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }

    /// Verify each path component before trusting an index hint. A missing
    /// main file may be a stale rebuildable index entry; a symlink is corrupt.
    async fn indexed_actor_rel_path(
        &self,
        actor_id: &str,
        rel: &str,
    ) -> Result<Option<String>, ActorDirectoryError> {
        let (kind, root_id) =
            Self::copy_source_identity_from_rel(actor_id, rel).map_err(storage)?;
        if !Self::regular_actor_directory(&self.sessions_dir).await?
            || !Self::regular_actor_directory(&self.sessions_dir.join(&root_id)).await?
        {
            return Ok(None);
        }
        if kind == bamboo_domain::SessionKind::Child
            && (!Self::regular_actor_directory(&self.sessions_dir.join(&root_id).join("children"))
                .await?
                || !Self::regular_actor_directory(&self.abs_path_from_rel(rel)).await?)
        {
            return Ok(None);
        }
        let main = self.abs_path_from_rel(rel).join("session.json");
        if !Self::regular_actor_file(&main).await? {
            return Ok(None);
        }
        Ok(Some(rel.to_string()))
    }

    async fn scan_actor_rel_path(&self, actor_id: &str) -> Result<String, ActorDirectoryError> {
        if !Self::regular_actor_directory(&self.sessions_dir).await? {
            return Err(ActorDirectoryError::NotFound(actor_id.to_string()));
        }
        let mut candidates = Vec::new();
        let root_rel = Self::root_rel_path(actor_id);
        if self
            .indexed_actor_rel_path(actor_id, &root_rel)
            .await?
            .is_some()
        {
            candidates.push(root_rel);
        }
        let mut roots = fs::read_dir(&self.sessions_dir).await.map_err(storage)?;
        while let Some(root) = roots.next_entry().await.map_err(storage)? {
            if !root.file_type().await.map_err(storage)?.is_dir() {
                continue;
            }
            let Some(root_id) = root.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if validate_session_id(&root_id).is_err() {
                continue;
            }
            let child_rel = Self::child_rel_path(&root_id, actor_id);
            if self
                .indexed_actor_rel_path(actor_id, &child_rel)
                .await?
                .is_some()
            {
                candidates.push(child_rel);
                if candidates.len() > 1 {
                    return Err(ActorDirectoryError::Corrupt);
                }
            }
        }
        match candidates.pop() {
            Some(rel) => Ok(rel),
            None => Err(ActorDirectoryError::NotFound(actor_id.to_string())),
        }
    }

    /// Resolve every valid on-disk candidate while holding the actor's file
    /// lock. Cached and global indexes can each point to one of two physical
    /// Sessions with the same id; trusting either would split activation
    /// authority between Store instances. The scan rejects that ambiguity.
    pub(super) async fn actor_authority_location(
        &self,
        actor_id: &str,
    ) -> Result<(String, PathBuf), ActorDirectoryError> {
        validate_session_id(actor_id).map_err(storage)?;
        let rel = self.scan_actor_rel_path(actor_id).await?;
        let path = self.abs_path_from_rel(&rel).join(ACTOR_AUTHORITY_FILE);
        Ok((rel, path))
    }

    /// A Child's own Session file is insufficient authority: deleting a
    /// middle Child currently leaves its nested descendants on disk. Verify
    /// the complete parent chain, root identity, depth and Project before
    /// publishing or accepting any activation sidecar for the descendant.
    pub(super) async fn validate_actor_lineage(
        &self,
        actor: &ActorSession,
    ) -> Result<Vec<ActorAncestorObservation>, ActorDirectoryError> {
        let mut current = actor.clone();
        let mut lineage = Vec::new();
        while let Some(parent_id) = current.parent_actor_id.clone() {
            let root_id = &current.root_actor_id;
            let (expected_rel, kind) = if parent_id == *root_id {
                (
                    Self::root_rel_path(&parent_id),
                    bamboo_domain::SessionKind::Root,
                )
            } else {
                (
                    Self::child_rel_path(root_id, &parent_id),
                    bamboo_domain::SessionKind::Child,
                )
            };
            let actual_rel = self.scan_actor_rel_path(&parent_id).await?;
            if actual_rel != expected_rel {
                return Err(ActorDirectoryError::InvalidIdentity);
            }
            let parent = self
                .load_session_from_dir_strict(
                    &self.abs_path_from_rel(&expected_rel),
                    &parent_id,
                    kind,
                    root_id,
                )
                .await
                .map_err(storage)?
                .ok_or_else(|| ActorDirectoryError::NotFound(parent_id.clone()))?;
            let parent = ActorSession::from_session(&parent)?;
            if parent.root_actor_id != *root_id
                || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
                || parent.project_id != current.project_id
                || parent.session_created_at > current.session_created_at
            {
                return Err(ActorDirectoryError::InvalidIdentity);
            }
            lineage.push(ActorAncestorObservation {
                actor_id: parent.actor_id.clone(),
                session_created_at: parent.session_created_at,
                metadata_version: parent.observed_metadata_version,
            });
            current = parent;
        }
        if current.actor_id != actor.root_actor_id || current.spawn_depth != 0 {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        Ok(lineage)
    }

    async fn read_or_create_actor_entry(
        &self,
        actor_id: &str,
        rel: &str,
        path: &Path,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        let (kind, root_id) =
            Self::copy_source_identity_from_rel(actor_id, rel).map_err(storage)?;
        let directory = self.abs_path_from_rel(rel);
        let session = self
            .load_session_from_dir_strict(&directory, actor_id, kind, &root_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| ActorDirectoryError::NotFound(actor_id.to_string()))?;
        if session.id != actor_id {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let mut expected = ActorSession::from_session(&session)?;
        expected.ancestor_observations = self.validate_actor_lineage(&expected).await?;
        let marker_path = directory.join(ACTOR_INITIALIZED_FILE);
        let marker = match Self::regular_actor_file(&marker_path).await? {
            true => {
                let bytes = fs::read(&marker_path).await.map_err(storage)?;
                let marker: ActorInitializedMarker =
                    serde_json::from_slice(&bytes).map_err(|_| ActorDirectoryError::Corrupt)?;
                if marker.schema_version != bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION
                    || marker.actor_id != actor_id
                    || marker.session_created_at != expected.session_created_at
                {
                    return Err(ActorDirectoryError::Corrupt);
                }
                Some(marker)
            }
            false => None,
        };
        let mut entry = match Self::regular_actor_file(path).await? {
            true => {
                let raw = fs::read(path).await.map_err(storage)?;
                let entry: ActorDirectoryEntry =
                    serde_json::from_slice(&raw).map_err(|_| ActorDirectoryError::Corrupt)?;
                entry.validate()?;
                if !entry.actor.matches_session(&session) {
                    return Err(ActorDirectoryError::InvalidIdentity);
                }
                if marker.is_none() {
                    // Only the fully inert first publication can survive a
                    // crash before marker durability. A prior claim without
                    // independent evidence must never be silently adopted.
                    if entry.actor.current_attempt != 0
                        || entry.actor.state != ActorLogicalState::Cold
                        || entry.activation.is_some()
                    {
                        return Err(ActorDirectoryError::Corrupt);
                    }
                    self.write_actor_marker(&marker_path, &expected, guards)
                        .await?;
                }
                entry
            }
            false if marker.is_none() => {
                // The Session was durably visible before this publication. A
                // crash before the marker leaves an inert Cold actor. No claim
                // is returned until BOTH files have been durably published.
                let entry = ActorDirectoryEntry::new(expected.clone());
                self.write_actor_entry(path, &entry, guards).await?;
                self.write_actor_marker(&marker_path, &expected, guards)
                    .await?;
                entry
            }
            false => return Err(ActorDirectoryError::Corrupt),
        };
        let project_changed = entry.actor.project_id != expected.project_id;
        if kind == bamboo_domain::SessionKind::Child && project_changed {
            // Unlike a Root, a saved Child cannot be rebound to another
            // Project after its identity was first published.
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let observed = entry.actor.observed_metadata_version;
        let current = expected.observed_metadata_version;
        if current < observed
            || entry.actor.ancestor_observations.len() != expected.ancestor_observations.len()
        {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let mut lineage_changed = false;
        let mut lineage_gap = false;
        for (previous, latest) in entry
            .actor
            .ancestor_observations
            .iter()
            .zip(&expected.ancestor_observations)
        {
            if previous.actor_id != latest.actor_id
                || previous.session_created_at != latest.session_created_at
                || latest.metadata_version < previous.metadata_version
            {
                return Err(ActorDirectoryError::InvalidIdentity);
            }
            let delta = latest.metadata_version - previous.metadata_version;
            lineage_changed |= delta > 0;
            lineage_gap |= delta >= 2;
        }
        if entry.actor.state == ActorLogicalState::Active
            && (project_changed || current - observed >= 2 || lineage_gap)
        {
            // A same-Project gap of two revisions could conceal an A→B→A
            // rebind in this Actor or any ancestor. One unseen revision
            // cannot conceal that round trip when save boundaries enforce
            // version increments for Project changes.
            return Err(ActorDirectoryError::ProjectTransitionBlocked);
        }
        if project_changed || current != observed || lineage_changed {
            entry.actor.project_id = expected.project_id;
            entry.actor.observed_metadata_version = current;
            entry.actor.ancestor_observations = expected.ancestor_observations;
            entry.revision = checked_next(entry.revision)?;
            self.write_actor_entry(path, &entry, guards).await?;
        }
        Ok(entry)
    }

    async fn write_actor_marker(
        &self,
        path: &Path,
        actor: &ActorSession,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        let marker = ActorInitializedMarker {
            schema_version: bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION,
            actor_id: actor.actor_id.clone(),
            session_created_at: actor.session_created_at,
        };
        let bytes = serde_json::to_vec(&marker).map_err(|_| ActorDirectoryError::Corrupt)?;
        self.write_actor_bytes(path, bytes, guards).await
    }

    async fn write_actor_entry(
        &self,
        path: &Path,
        entry: &ActorDirectoryEntry,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        entry.validate()?;
        let bytes = serde_json::to_vec_pretty(entry).map_err(|_| ActorDirectoryError::Corrupt)?;
        self.write_actor_bytes(path, bytes, guards).await
    }

    async fn write_actor_bytes(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        if !guards
            .tree_bumped
            .load(std::sync::atomic::Ordering::Acquire)
        {
            let root = guards
                .tree_root
                .lock()
                .expect("Actor tree Root")
                .clone()
                .ok_or(ActorDirectoryError::Corrupt)?;
            self.bump_actor_tree_revision(&root)
                .await
                .map_err(storage)?;
            guards
                .tree_bumped
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let path = path.to_path_buf();
        let guards = Arc::clone(guards);
        #[cfg(test)]
        let hook = self.actor_write_hook.lock().unwrap().clone();
        #[cfg(test)]
        let transcript_hook = self.transcript_write_hook.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            let _guards = guards;
            durable_atomic_write_blocking(&path, &bytes, |phase| {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    return hook.visit(&path, phase);
                }
                #[cfg(test)]
                if let Some(hook) = &transcript_hook {
                    hook.visit(phase)?;
                }
                let _ = phase;
                Ok(())
            })
        })
        .await
        .map_err(|error| storage(io::Error::other(format!("join actor write: {error}"))))?
        .map_err(storage)
    }

    async fn actor_transaction<T, F>(
        &self,
        actor_id: &str,
        operation: F,
    ) -> Result<T, ActorDirectoryError>
    where
        F: FnOnce(&mut ActorDirectoryEntry) -> Result<Mutation<T>, ActorDirectoryError> + Send,
        T: Send,
    {
        // Same lock order as strict Session control-plane writes: lifecycle,
        // Task sidecar, then the exact Session maintenance/file lock. The last
        // lock spans read/CAS/durable rename, including independent processes.
        let lifecycle = self
            .lock_session_lifecycle_shared()
            .await
            .map_err(storage)?;
        let task = self
            .lock_runtime_task_sidecar_shared()
            .await
            .map_err(storage)?;
        let session = self
            .acquire_session_maintenance_lock(actor_id)
            .await
            .map_err(storage)?;
        let guards = Arc::new(ActorAuthorityGuards {
            _tree: std::sync::Mutex::new(None),
            tree_root: std::sync::Mutex::new(None),
            tree_bumped: std::sync::atomic::AtomicBool::new(false),
            _session: session,
            _task: task,
            _lifecycle: lifecycle,
        });
        let (rel, path) = self.actor_authority_location(actor_id).await?;
        let (_, root_id) = Self::copy_source_identity_from_rel(actor_id, &rel).map_err(storage)?;
        let tree = self
            .acquire_actor_tree_write_guard(&root_id)
            .await
            .map_err(storage)?;
        *guards._tree.lock().expect("Actor tree writer guard") = Some(tree);
        let root = self
            .canonical_actor_tree_root(&root_id)
            .await
            .map_err(storage)?;
        *guards.tree_root.lock().expect("Actor tree Root") = Some(root);
        let mut entry = self
            .read_or_create_actor_entry(actor_id, &rel, &path, &guards)
            .await?;
        let Mutation { value, changed } = operation(&mut entry)?;
        if changed {
            entry.revision = checked_next(entry.revision)?;
            self.write_actor_entry(&path, &entry, &guards).await?;
        }
        Ok(value)
    }

    /// Retire a resident only while its exact physical activation is still
    /// the Directory owner. A delayed launcher/renewal cleanup may run after
    /// expiry and replacement; it must not retire that successor by ActorId.
    pub async fn retire_actor_if_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
    ) -> Result<bool, ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            if entry.actor.state == ActorLogicalState::Retired
                || !entry
                    .activation
                    .as_ref()
                    .is_some_and(|activation| activation.matches_fence(fence))
            {
                return Ok(Mutation::unchanged(false));
            }
            if let Some(activation) = entry.activation.as_mut() {
                if activation.status.is_live() {
                    activation.lease_epoch = checked_next(activation.lease_epoch)?;
                    activation.status = ActorActivationStatus::Cancelled;
                    activation.finished_at = Some(now);
                    activation.lease_expires_at = now;
                }
            }
            entry.actor.state = ActorLogicalState::Retired;
            Ok(Mutation::changed(true))
        })
        .await
    }
}

#[async_trait]
impl ActorDirectoryPort for SessionStoreV2 {
    async fn ensure_actor(
        &self,
        actor_id: &str,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        self.actor_transaction(actor_id, |entry| Ok(Mutation::unchanged(entry.clone())))
            .await
    }

    async fn inspect_actor(
        &self,
        actor_id: &str,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        self.ensure_actor(actor_id).await
    }

    async fn bind_control_plane(
        &self,
        actor_id: &str,
        binding: ActorControlPlaneBinding,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        if binding
            .placement_intent
            .as_ref()
            .is_some_and(|intent| !intent.is_valid())
        {
            return Err(ActorDirectoryError::InvalidTransition);
        }
        let ActorControlPlaneBinding {
            policy_revision,
            placement_intent,
        } = binding;
        self.actor_transaction(actor_id, |entry| {
            let policy_changed = policy_revision
                .is_some_and(|revision| entry.actor.policy_revision != Some(revision));
            let placement_changed = placement_intent
                .as_ref()
                .is_some_and(|intent| entry.actor.placement_intent.as_ref() != Some(intent));
            if !policy_changed && !placement_changed {
                return Ok(Mutation::unchanged(entry.clone()));
            }
            if placement_changed && entry.actor.placement_intent.is_some() {
                return Err(ActorDirectoryError::PlacementIntentConflict);
            }
            if policy_changed
                && entry.actor.policy_revision.is_some_and(|previous| {
                    policy_revision.is_none_or(|revision| revision <= previous)
                })
            {
                return Err(ActorDirectoryError::PolicyTransitionBlocked);
            }
            if matches!(
                entry.actor.state,
                ActorLogicalState::Active | ActorLogicalState::Retired
            ) {
                return Err(if policy_changed {
                    ActorDirectoryError::PolicyTransitionBlocked
                } else {
                    ActorDirectoryError::PlacementIntentConflict
                });
            }
            if let Some(revision) = policy_revision {
                entry.actor.policy_revision = Some(revision);
            }
            if let Some(intent) = placement_intent {
                entry.actor.placement_intent = Some(intent);
            }
            let mut returned = entry.clone();
            returned.revision = checked_next(entry.revision)?;
            Ok(Mutation::changed(returned))
        })
        .await
    }

    async fn claim_activation(
        &self,
        claim: &ActorActivationClaim,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        if claim.run_id.trim().is_empty()
            || claim.lease_owner.trim().is_empty()
            || claim.lease_expires_at <= claim.now
        {
            return Err(ActorDirectoryError::InvalidTransition);
        }
        self.actor_transaction(&claim.actor_id, |entry| {
            if entry.actor.state == ActorLogicalState::Retired {
                return Err(ActorDirectoryError::InvalidTransition);
            }
            if let Some(active) = entry.activation.as_ref() {
                if active.status.is_live() && active.lease_expires_at > claim.now {
                    if active.run_id == claim.run_id
                        && active.lease_owner == claim.lease_owner
                        && active.lease_expires_at == claim.lease_expires_at
                        && active.inbox_generation == claim.inbox_generation
                        && active.placement_ref == claim.placement_ref
                    {
                        return Ok(Mutation::unchanged(active.clone()));
                    }
                    return Err(ActorDirectoryError::Busy);
                }
            }
            let attempt = checked_next(entry.actor.current_attempt)?;
            let lease_epoch = checked_next(
                entry
                    .activation
                    .as_ref()
                    .map_or(0, |previous| previous.lease_epoch),
            )?;
            let activation = ActorActivation {
                schema_version: bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION,
                actor_id: entry.actor.actor_id.clone(),
                activation_id: Uuid::new_v4().to_string(),
                attempt,
                run_id: claim.run_id.clone(),
                lease_owner: claim.lease_owner.clone(),
                lease_epoch,
                lease_expires_at: claim.lease_expires_at,
                project_id: entry.actor.project_id.clone(),
                inbox_generation: claim.inbox_generation,
                placement_ref: claim.placement_ref.clone(),
                status: ActorActivationStatus::Reserved,
                checkpoint_revision: 0,
                started_at: None,
                finished_at: None,
            };
            entry.actor.current_attempt = attempt;
            entry.actor.state = ActorLogicalState::Active;
            entry.activation = Some(activation.clone());
            Ok(Mutation::changed(activation))
        })
        .await
    }

    async fn start_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            let status = current_live(entry, fence, now)?.status;
            let activation = entry
                .activation
                .as_mut()
                .ok_or(ActorDirectoryError::StaleFence)?;
            if status == ActorActivationStatus::Running {
                return Ok(Mutation::unchanged(activation.clone()));
            }
            activation.status = ActorActivationStatus::Running;
            activation.started_at = Some(now);
            Ok(Mutation::changed(activation.clone()))
        })
        .await
    }

    async fn renew_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            let old_expiry = current_live(entry, fence, now)?.lease_expires_at;
            if lease_expires_at <= old_expiry {
                return Err(ActorDirectoryError::InvalidTransition);
            }
            let activation = entry
                .activation
                .as_mut()
                .ok_or(ActorDirectoryError::StaleFence)?;
            activation.lease_expires_at = lease_expires_at;
            Ok(Mutation::changed(activation.clone()))
        })
        .await
    }

    async fn checkpoint_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        expected_checkpoint_revision: u64,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            let active = current_live(entry, fence, now)?;
            if active.status != ActorActivationStatus::Running {
                return Err(ActorDirectoryError::InvalidTransition);
            }
            if active.checkpoint_revision != expected_checkpoint_revision {
                return Err(ActorDirectoryError::StaleFence);
            }
            let next = checked_next(expected_checkpoint_revision)?;
            let activation = entry
                .activation
                .as_mut()
                .ok_or(ActorDirectoryError::StaleFence)?;
            activation.checkpoint_revision = next;
            Ok(Mutation::changed(activation.clone()))
        })
        .await
    }

    async fn finish_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        outcome: ActorActivationFinish,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            let current_status = current_live(entry, fence, now)?.status;
            if outcome == ActorActivationFinish::Succeeded
                && current_status != ActorActivationStatus::Running
            {
                return Err(ActorDirectoryError::InvalidTransition);
            }
            let activation = entry
                .activation
                .as_mut()
                .ok_or(ActorDirectoryError::StaleFence)?;
            activation.status = match outcome {
                ActorActivationFinish::Succeeded => ActorActivationStatus::Succeeded,
                ActorActivationFinish::Failed => ActorActivationStatus::Failed,
                ActorActivationFinish::Cancelled => ActorActivationStatus::Cancelled,
            };
            activation.finished_at = Some(now);
            entry.actor.state = if outcome == ActorActivationFinish::Failed {
                ActorLogicalState::Failed
            } else {
                ActorLogicalState::Cold
            };
            Ok(Mutation::changed(activation.clone()))
        })
        .await
    }

    async fn retire_actor(
        &self,
        actor_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        self.actor_transaction(actor_id, |entry| {
            if entry.actor.state == ActorLogicalState::Retired {
                return Ok(Mutation::unchanged(entry.clone()));
            }
            if let Some(activation) = entry.activation.as_mut() {
                if activation.status.is_live() {
                    activation.lease_epoch = checked_next(activation.lease_epoch)?;
                    activation.status = ActorActivationStatus::Cancelled;
                    activation.finished_at = Some(now);
                    activation.lease_expires_at = now;
                }
            }
            entry.actor.state = ActorLogicalState::Retired;
            // The returned snapshot reflects the new persisted revision.
            let mut returned = entry.clone();
            returned.revision = checked_next(entry.revision)?;
            Ok(Mutation::changed(returned))
        })
        .await
    }

    async fn validate_fence(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
    ) -> Result<(), ActorDirectoryError> {
        self.actor_transaction(&fence.actor_id, |entry| {
            current_live(entry, fence, now)?;
            Ok(Mutation::unchanged(()))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bamboo_domain::{
        record_permission_audit, resolve_permission_mode, ActorControlPlaneBinding,
        ActorDirectoryPort, ActorPlacementClass, ActorPlacementIntent, ActorPlacementRef,
        PermissionAuditSeed, PermissionMode, Session, SessionPermissionMode, Storage,
    };
    use chrono::{Duration, Utc};

    use super::*;

    fn claim(
        actor_id: &str,
        run_id: &str,
        owner: &str,
        now: DateTime<Utc>,
    ) -> ActorActivationClaim {
        ActorActivationClaim {
            actor_id: actor_id.into(),
            run_id: run_id.into(),
            lease_owner: owner.into(),
            lease_expires_at: now + Duration::minutes(5),
            inbox_generation: 7,
            placement_ref: Some(ActorPlacementRef {
                class: ActorPlacementClass::Local,
                lease_id: "local-lease".into(),
                slot_epoch: None,
            }),
            now,
        }
    }

    fn stamp_policy(session: &mut Session, policy_revision: u64) {
        record_permission_audit(
            &mut session.metadata,
            &PermissionAuditSeed::bamboo_runtime(
                policy_revision,
                resolve_permission_mode(SessionPermissionMode::Default, PermissionMode::Default),
            ),
            None,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn host_policy_and_intent_survive_reassignment_and_physical_replacement(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("actor-control-plane", "model");
        stamp_policy(&mut root, 7);
        store.save_session(&root).await?;
        let cold = store.ensure_actor(&root.id).await?;
        // Even a complete saved audit is not by itself proof that its revision
        // came from the Host's effective configuration.
        assert_eq!(cold.actor.policy_revision, None);
        assert_eq!(cold.actor.placement_intent, None);

        let intent = ActorPlacementIntent::Pool {
            pool_id: "workers".into(),
        };
        let binding = ActorControlPlaneBinding {
            policy_revision: Some(7),
            placement_intent: Some(intent.clone()),
        };
        let bound = store.bind_control_plane(&root.id, binding.clone()).await?;
        assert_eq!(bound.actor.policy_revision, Some(7));
        assert_eq!(bound.actor.placement_intent, Some(intent.clone()));
        assert_eq!(
            store.bind_control_plane(&root.id, binding).await?.revision,
            bound.revision
        );
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&root.id, "run-1", "host-a", now))
            .await?;
        assert_eq!(
            activation.placement_ref.as_ref().unwrap().class,
            ActorPlacementClass::Local
        );
        assert_eq!(
            store.inspect_actor(&root.id).await?.actor.placement_intent,
            Some(intent.clone())
        );
        store.start_activation(&activation.fence(), now).await?;
        store
            .finish_activation(
                &activation.fence(),
                now + Duration::seconds(1),
                ActorActivationFinish::Succeeded,
            )
            .await?;

        root.set_project_id_meta("another-project");
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let after = reopened.inspect_actor(&root.id).await?;
        assert_eq!(after.actor.project_id.as_deref(), Some("another-project"));
        assert_eq!(after.actor.policy_revision, Some(7));
        assert_eq!(after.actor.placement_intent, Some(intent));
        let sidecar = home
            .path()
            .join("sessions/actor-control-plane/actor-authority.json");
        let before_conflict = fs::read(&sidecar).await?;
        assert_eq!(
            reopened
                .bind_control_plane(
                    &root.id,
                    ActorControlPlaneBinding {
                        policy_revision: None,
                        placement_intent: Some(ActorPlacementIntent::Local),
                    }
                )
                .await
                .unwrap_err(),
            ActorDirectoryError::PlacementIntentConflict
        );
        assert_eq!(fs::read(&sidecar).await?, before_conflict);
        let advanced = reopened
            .bind_control_plane(
                &root.id,
                ActorControlPlaneBinding {
                    policy_revision: Some(8),
                    placement_intent: None,
                },
            )
            .await?;
        assert_eq!(advanced.actor.policy_revision, Some(8));
        assert_eq!(
            advanced.actor.placement_intent,
            after.actor.placement_intent
        );
        Ok(())
    }

    #[tokio::test]
    async fn only_host_binding_sets_policy_and_live_or_regressive_changes_are_rejected(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("policy-refresh", "model");
        stamp_policy(&mut root, 99);
        root.metadata.insert(
            "placement".into(),
            r#"{"kind":"remote","host":"other"}"#.into(),
        );
        store.save_session(&root).await?;
        assert_eq!(
            store.ensure_actor(&root.id).await?.actor.policy_revision,
            None
        );
        assert_eq!(
            store.inspect_actor(&root.id).await?.actor.placement_intent,
            None
        );

        let now = Utc::now();
        let active = store
            .claim_activation(&claim(&root.id, "run", "host", now))
            .await?;
        let sidecar = home
            .path()
            .join("sessions/policy-refresh/actor-authority.json");
        let before_conflict = fs::read(&sidecar).await?;
        assert_eq!(
            store.inspect_actor(&root.id).await?.actor.policy_revision,
            None
        );
        assert_eq!(
            store
                .bind_control_plane(
                    &root.id,
                    ActorControlPlaneBinding {
                        policy_revision: Some(4),
                        placement_intent: None,
                    }
                )
                .await
                .unwrap_err(),
            ActorDirectoryError::PolicyTransitionBlocked
        );
        assert_eq!(fs::read(&sidecar).await?, before_conflict);
        store.validate_fence(&active.fence(), now).await?;
        store.start_activation(&active.fence(), now).await?;
        store
            .finish_activation(
                &active.fence(),
                now + Duration::seconds(1),
                ActorActivationFinish::Succeeded,
            )
            .await?;
        let bound = store
            .bind_control_plane(
                &root.id,
                ActorControlPlaneBinding {
                    policy_revision: Some(4),
                    placement_intent: None,
                },
            )
            .await?;
        assert_eq!(bound.actor.policy_revision, Some(4));
        assert_eq!(bound.actor.placement_intent, None);

        stamp_policy(&mut root, 1000);
        store.save_runtime_state(&root).await?;
        assert_eq!(
            store.inspect_actor(&root.id).await?.actor.policy_revision,
            Some(4)
        );
        let before_conflict = fs::read(&sidecar).await?;
        assert_eq!(
            store
                .bind_control_plane(
                    &root.id,
                    ActorControlPlaneBinding {
                        policy_revision: Some(3),
                        placement_intent: None,
                    }
                )
                .await
                .unwrap_err(),
            ActorDirectoryError::PolicyTransitionBlocked
        );
        assert_eq!(fs::read(&sidecar).await?, before_conflict);
        Ok(())
    }

    #[tokio::test]
    async fn persisted_session_precedes_claim_and_attempt_survives_restart(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let now = Utc::now();
        assert_eq!(
            store
                .claim_activation(&claim("actor-root", "run-1", "host-a", now))
                .await
                .unwrap_err(),
            ActorDirectoryError::NotFound("actor-root".into())
        );
        assert!(!home.path().join("sessions/actor-root").exists());

        let root = Session::new("actor-root", "model");
        store.save_session(&root).await?;
        let cold = store.ensure_actor("actor-root").await?;
        assert_eq!(cold.actor.actor_id, root.id);
        assert_eq!(cold.actor.root_actor_id, root.id);
        assert_eq!(cold.actor.current_attempt, 0);
        assert!(home
            .path()
            .join("sessions/actor-root/actor-authority.json")
            .is_file());

        let first = store
            .claim_activation(&claim("actor-root", "run-1", "host-a", now))
            .await?;
        assert_eq!(first.attempt, 1);
        assert_eq!(first.lease_epoch, 1);
        assert_eq!(first.status, ActorActivationStatus::Reserved);
        assert_eq!(
            store
                .claim_activation(&claim("actor-root", "run-1", "host-a", now))
                .await?,
            first
        );
        assert_eq!(
            store
                .claim_activation(&claim("actor-root", "other-run", "host-b", now))
                .await
                .unwrap_err(),
            ActorDirectoryError::Busy
        );
        assert_eq!(
            store
                .finish_activation(
                    &first.fence(),
                    now + Duration::seconds(1),
                    ActorActivationFinish::Succeeded,
                )
                .await
                .unwrap_err(),
            ActorDirectoryError::InvalidTransition
        );
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let recovered = reopened.inspect_actor("actor-root").await?;
        assert_eq!(recovered.activation.as_ref(), Some(&first));

        let started = reopened
            .start_activation(&first.fence(), now + Duration::seconds(1))
            .await?;
        assert_eq!(started.status, ActorActivationStatus::Running);
        let renewed = reopened
            .renew_activation(
                &first.fence(),
                now + Duration::seconds(2),
                first.lease_expires_at + Duration::minutes(1),
            )
            .await?;
        assert_eq!(renewed.fence(), first.fence());
        assert!(renewed.lease_expires_at > first.lease_expires_at);
        let checkpoint = reopened
            .checkpoint_activation(&first.fence(), now + Duration::seconds(2), 0)
            .await?;
        assert_eq!(checkpoint.checkpoint_revision, 1);
        assert_eq!(
            reopened
                .checkpoint_activation(&first.fence(), now + Duration::seconds(3), 0)
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        reopened
            .finish_activation(
                &first.fence(),
                now + Duration::seconds(4),
                ActorActivationFinish::Succeeded,
            )
            .await?;
        assert_eq!(
            reopened
                .validate_fence(&first.fence(), now + Duration::seconds(5))
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );

        let retried = reopened
            .claim_activation(&claim(
                "actor-root",
                "run-2",
                "host-b",
                now + Duration::seconds(6),
            ))
            .await?;
        assert_eq!(retried.actor_id, first.actor_id);
        assert_eq!(retried.attempt, 2);
        assert_eq!(retried.lease_epoch, 2);
        assert_ne!(retried.activation_id, first.activation_id);
        assert_eq!(
            reopened
                .start_activation(&first.fence(), now + Duration::seconds(7))
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        Ok(())
    }

    #[tokio::test]
    async fn independent_stores_serialize_competing_owners(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let first = Arc::new(SessionStoreV2::new(home.path().to_path_buf()).await?);
        first
            .save_session(&Session::new("contended", "model"))
            .await?;
        let second = Arc::new(SessionStoreV2::new(home.path().to_path_buf()).await?);
        let now = Utc::now();
        let a = claim("contended", "run-a", "host-a", now);
        let b = claim("contended", "run-b", "host-b", now);
        let (left, right) = tokio::join!(first.claim_activation(&a), second.claim_activation(&b));
        let winners = usize::from(left.is_ok()) + usize::from(right.is_ok());
        assert_eq!(winners, 1);
        assert!(
            matches!(left, Err(ActorDirectoryError::Busy))
                || matches!(right, Err(ActorDirectoryError::Busy))
        );
        let recovered = second.inspect_actor("contended").await?;
        assert_eq!(recovered.actor.current_attempt, 1);
        assert_eq!(
            recovered.activation.unwrap().status,
            ActorActivationStatus::Reserved
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_owner_cannot_checkpoint_or_finish_new_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        store
            .save_session(&Session::new("expired", "model"))
            .await?;
        let now = Utc::now();
        let old = store
            .claim_activation(&claim("expired", "run-old", "host-old", now))
            .await?;
        let after_expiry = now + Duration::minutes(6);
        let next = store
            .claim_activation(&claim("expired", "run-next", "host-next", after_expiry))
            .await?;
        assert_eq!(next.attempt, old.attempt + 1);
        assert_eq!(next.lease_epoch, old.lease_epoch + 1);
        assert_eq!(
            store
                .finish_activation(&old.fence(), after_expiry, ActorActivationFinish::Failed)
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        assert_eq!(
            store
                .checkpoint_activation(&old.fence(), after_expiry, 0)
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        assert_eq!(
            store
                .validate_fence(&old.fence(), after_expiry)
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        Ok(())
    }

    #[tokio::test]
    async fn delayed_resident_cleanup_cannot_retire_replacement_activation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let old_store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        old_store
            .save_session(&Session::new("resident-replacement", "model"))
            .await?;
        let now = Utc::now();
        let old = old_store
            .claim_activation(&claim("resident-replacement", "old", "old-host", now))
            .await?;
        old_store.start_activation(&old.fence(), now).await?;

        let replacement_store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let after_expiry = now + Duration::minutes(6);
        let replacement = replacement_store
            .claim_activation(&claim(
                "resident-replacement",
                "replacement",
                "new-host",
                after_expiry,
            ))
            .await?;
        replacement_store
            .start_activation(&replacement.fence(), after_expiry)
            .await?;

        assert!(
            !old_store
                .retire_actor_if_activation(&old.fence(), after_expiry)
                .await?
        );
        let current = old_store.inspect_actor("resident-replacement").await?;
        assert_eq!(current.actor.state, ActorLogicalState::Active);
        assert_eq!(current.activation.unwrap().fence(), replacement.fence());
        replacement_store
            .validate_fence(&replacement.fence(), after_expiry)
            .await?;

        assert!(
            old_store
                .retire_actor_if_activation(&replacement.fence(), after_expiry)
                .await?
        );
        let retired = replacement_store
            .inspect_actor("resident-replacement")
            .await?;
        assert_eq!(retired.actor.state, ActorLogicalState::Retired);
        assert_eq!(
            retired.activation.unwrap().status,
            ActorActivationStatus::Cancelled
        );
        Ok(())
    }

    #[tokio::test]
    async fn retirement_fences_owner_and_preserves_identity(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let root = Session::new("tree-root", "model");
        let child = Session::new_child_of("tree-child", &root, "model", "Child");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        let now = Utc::now();
        let active = store
            .claim_activation(&claim("tree-child", "child-run", "host-a", now))
            .await?;
        let retired = store
            .retire_actor("tree-child", now + Duration::seconds(1))
            .await?;
        assert_eq!(retired.actor.actor_id, child.id);
        assert_eq!(
            retired.actor.parent_actor_id.as_deref(),
            Some(root.id.as_str())
        );
        assert_eq!(retired.actor.root_actor_id, root.id);
        assert_eq!(retired.actor.state, ActorLogicalState::Retired);
        assert_eq!(
            retired.activation.as_ref().unwrap().status,
            ActorActivationStatus::Cancelled
        );
        assert_eq!(
            store
                .validate_fence(&active.fence(), now + Duration::seconds(1))
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        assert_eq!(
            store
                .claim_activation(&claim(
                    "tree-child",
                    "later",
                    "host-b",
                    now + Duration::seconds(2)
                ))
                .await
                .unwrap_err(),
            ActorDirectoryError::InvalidTransition
        );
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(reopened.inspect_actor("tree-child").await?, retired);
        assert!(reopened.load_session("tree-child").await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_or_mismatched_authority_fails_closed_without_repair(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        store
            .save_session(&Session::new("damaged", "model"))
            .await?;
        let entry = store.ensure_actor("damaged").await?;
        let path = home.path().join("sessions/damaged/actor-authority.json");

        fs::write(&path, b"{invalid").await?;
        assert_eq!(
            store.inspect_actor("damaged").await.unwrap_err(),
            ActorDirectoryError::Corrupt
        );
        assert_eq!(fs::read(&path).await?, b"{invalid");

        let mut wrong_version = entry.clone();
        wrong_version.schema_version += 1;
        fs::write(&path, serde_json::to_vec(&wrong_version)?).await?;
        assert_eq!(
            store.inspect_actor("damaged").await.unwrap_err(),
            ActorDirectoryError::Corrupt
        );

        let mut wrong_identity = entry;
        wrong_identity.actor.session_created_at += Duration::seconds(1);
        fs::write(&path, serde_json::to_vec(&wrong_identity)?).await?;
        assert_eq!(
            store.inspect_actor("damaged").await.unwrap_err(),
            ActorDirectoryError::InvalidIdentity
        );
        assert_eq!(
            store
                .claim_activation(&claim("damaged", "run", "host", Utc::now()))
                .await
                .unwrap_err(),
            ActorDirectoryError::InvalidIdentity
        );
        Ok(())
    }

    #[tokio::test]
    async fn store_opened_before_creation_discovers_root_and_child_from_durable_files(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let writer = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let stale_reader = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let root = Session::new("late-root", "model");
        let child = Session::new_child_of("late-child", &root, "model", "Child");
        writer.save_session(&root).await?;
        writer.save_session(&child).await?;
        assert!(stale_reader.get_index_entry("late-child").await.is_none());
        assert_eq!(
            stale_reader.ensure_actor("late-root").await?.actor.actor_id,
            root.id
        );
        assert_eq!(
            stale_reader
                .ensure_actor("late-child")
                .await?
                .actor
                .parent_actor_id,
            Some(root.id.clone())
        );

        // Simulate a crash after a new Session's files became durable but
        // before the global rebuildable index was published.
        let orphan = Session::new_child_of("late-orphan", &root, "model", "Orphan");
        writer.save_session(&orphan).await?;
        let index_path = home.path().join("sessions.json");
        let mut index: serde_json::Value = serde_json::from_slice(&fs::read(&index_path).await?)?;
        index["sessions"]
            .as_object_mut()
            .unwrap()
            .remove(&orphan.id);
        fs::write(index_path, serde_json::to_vec(&index)?).await?;
        assert_eq!(
            stale_reader.ensure_actor(&orphan.id).await?.actor.actor_id,
            orphan.id
        );
        let activation = stale_reader
            .claim_activation(&claim(&orphan.id, "run", "host", Utc::now()))
            .await?;
        assert_eq!(activation.attempt, 1);
        Ok(())
    }

    #[tokio::test]
    async fn orphaned_nested_child_cannot_publish_activation_authority(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let root = Session::new("lineage-root", "model");
        let child = Session::new_child_of("lineage-child", &root, "model", "Child");
        let grandchild = Session::new_child_of("lineage-grandchild", &child, "model", "Grandchild");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        store.save_session(&grandchild).await?;

        assert!(store.delete_session(&child.id).await?);
        assert!(store.load_session(&child.id).await?.is_none());
        assert!(store.load_session(&grandchild.id).await?.is_some());
        assert_eq!(
            store
                .claim_activation(&claim(&grandchild.id, "run", "host", Utc::now()))
                .await
                .unwrap_err(),
            ActorDirectoryError::NotFound(child.id.clone())
        );
        assert!(!home
            .path()
            .join("sessions/lineage-root/children/lineage-grandchild/actor-authority.json")
            .exists());
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_physical_actor_id_fails_closed_across_stale_stores(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let first = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let origin = Session::new("origin", "model");
        let child = Session::new_child_of("same", &origin, "model", "Child");
        first.save_session(&origin).await?;
        first.save_session(&child).await?;
        let stale = SessionStoreV2::new(home.path().to_path_buf()).await?;
        first.save_session(&Session::new("same", "model")).await?;

        for store in [&first, &stale] {
            assert_eq!(
                store
                    .claim_activation(&claim("same", "run", "host", Utc::now()))
                    .await
                    .unwrap_err(),
                ActorDirectoryError::Corrupt
            );
        }
        assert!(!home
            .path()
            .join("sessions/origin/children/same/actor-authority.json")
            .exists());
        assert!(!home
            .path()
            .join("sessions/same/actor-authority.json")
            .exists());
        Ok(())
    }

    #[tokio::test]
    async fn child_project_must_match_its_current_parent() -> Result<(), Box<dyn std::error::Error>>
    {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("project-lineage-root", "model");
        root.set_project_id_meta("project-a");
        let mut child = Session::new_child_of("project-lineage-child", &root, "model", "Child");
        child.set_project_id_meta("project-b");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        assert_eq!(
            store
                .claim_activation(&claim(&child.id, "run", "host", Utc::now()))
                .await
                .unwrap_err(),
            ActorDirectoryError::InvalidIdentity
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_grandchild_cannot_observe_middle_parent_project_rebind(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("aba-root", "model");
        root.set_project_id_meta("project-a");
        let mut child = Session::new_child_of("aba-child", &root, "model", "Child");
        child.set_project_id_meta("project-a");
        let mut grandchild = Session::new_child_of("aba-grandchild", &child, "model", "Grandchild");
        grandchild.set_project_id_meta("project-a");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        store.save_session(&grandchild).await?;
        let stale_store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&grandchild.id, "run", "host", now))
            .await?;
        child.set_project_id_meta("project-b");
        child.metadata_version += 1;
        assert_eq!(
            stale_store
                .save_runtime_state(&child)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            store.save_session(&child).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            store
                .load_session(&child.id)
                .await?
                .unwrap()
                .project_id_meta()
                .as_deref(),
            Some("project-a")
        );
        let restarted = SessionStoreV2::new(home.path().to_path_buf()).await?;
        restarted.validate_fence(&activation.fence(), now).await?;
        Ok(())
    }

    #[tokio::test]
    async fn active_child_cannot_rebind_its_own_project() -> Result<(), Box<dyn std::error::Error>>
    {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("own-aba-root", "model");
        root.set_project_id_meta("project-a");
        let mut child = Session::new_child_of("own-aba-child", &root, "model", "Child");
        child.set_project_id_meta("project-a");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&child.id, "run", "host", now))
            .await?;
        child.set_project_id_meta("project-b");
        child.metadata_version += 1;
        assert_eq!(
            store.save_runtime_state(&child).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        store.validate_fence(&activation.fence(), now).await?;
        Ok(())
    }

    #[tokio::test]
    async fn recreated_middle_parent_cannot_revive_grandchild_fence(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let root = Session::new("birth-root", "model");
        let child = Session::new_child_of("birth-child", &root, "model", "Child");
        let grandchild = Session::new_child_of("birth-grandchild", &child, "model", "Grandchild");
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        store.save_session(&grandchild).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&grandchild.id, "run", "host", now))
            .await?;
        assert!(store.delete_session(&child.id).await?);
        let mut replacement = Session::new_child_of(&child.id, &root, "model", "Replacement");
        replacement.created_at = child.created_at + Duration::seconds(1);
        store.save_session(&replacement).await?;
        assert_eq!(
            store
                .validate_fence(&activation.fence(), now)
                .await
                .unwrap_err(),
            ActorDirectoryError::InvalidIdentity
        );
        Ok(())
    }

    #[tokio::test]
    async fn root_project_rebinds_only_when_no_activation_is_live(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("project-root", "model");
        store.save_session(&root).await?;
        let original = store.ensure_actor(&root.id).await?;
        root.set_project_id_meta(" project-first ");
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        let bound = store.inspect_actor(&root.id).await?;
        assert_eq!(bound.actor.project_id.as_deref(), Some("project-first"));
        assert_eq!(bound.revision, original.revision + 1);
        root.set_project_id_meta("project-second");
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        let reassigned = store.inspect_actor(&root.id).await?;
        assert_eq!(
            reassigned.actor.project_id.as_deref(),
            Some("project-second")
        );
        assert_eq!(reassigned.revision, bound.revision + 1);
        let now = Utc::now();
        let active = store
            .claim_activation(&claim(&root.id, "run", "host", now))
            .await?;
        assert_eq!(active.project_id.as_deref(), Some("project-second"));

        root.set_project_id_meta("project-third");
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        let sidecar = home
            .path()
            .join("sessions/project-root/actor-authority.json");
        let before = fs::read(&sidecar).await?;
        assert_eq!(
            store.inspect_actor(&root.id).await.unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        assert_eq!(
            store
                .validate_fence(&active.fence(), now)
                .await
                .unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        assert_eq!(fs::read(&sidecar).await?, before);
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(
            reopened.inspect_actor(&root.id).await.unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        Ok(())
    }

    #[tokio::test]
    async fn root_project_aba_cannot_revive_an_old_active_fence(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("project-aba", "model");
        root.set_project_id_meta("project-a");
        store.save_session(&root).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&root.id, "run", "host", now))
            .await?;
        let sidecar = home
            .path()
            .join("sessions/project-aba/actor-authority.json");
        let before = fs::read(&sidecar).await?;

        for project in ["project-b", "project-a"] {
            root.set_project_id_meta(project);
            root.metadata_version += 1;
            store.save_runtime_state(&root).await?;
        }
        assert_eq!(
            store
                .validate_fence(&activation.fence(), now)
                .await
                .unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        assert_eq!(fs::read(&sidecar).await?, before);
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(
            reopened.inspect_actor(&root.id).await.unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_root_allows_one_ui_revision_but_blocks_ambiguous_ui_gap(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut root = Session::new("project-ui", "model");
        root.set_project_id_meta("project-a");
        store.save_session(&root).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&claim(&root.id, "run", "host", now))
            .await?;

        root.title = "One title change".into();
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        store.validate_fence(&activation.fence(), now).await?;
        let observed = store.inspect_actor(&root.id).await?;
        assert_eq!(observed.actor.observed_metadata_version, 1);
        let sidecar = home.path().join("sessions/project-ui/actor-authority.json");
        let before = fs::read(&sidecar).await?;

        root.pinned = true;
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        root.title = "Another title change".into();
        root.metadata_version += 1;
        store.save_runtime_state(&root).await?;
        assert_eq!(
            store
                .validate_fence(&activation.fence(), now)
                .await
                .unwrap_err(),
            ActorDirectoryError::ProjectTransitionBlocked
        );
        assert_eq!(fs::read(&sidecar).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn initialized_marker_prevents_lost_sidecar_from_resetting_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        store
            .save_session(&Session::new("marker-root", "model"))
            .await?;
        let sidecar = home
            .path()
            .join("sessions/marker-root/actor-authority.json");
        let marker = home
            .path()
            .join("sessions/marker-root/actor-authority.initialized.json");
        store.ensure_actor("marker-root").await?;
        assert!(sidecar.is_file() && marker.is_file());

        // Interrupted initial publication may leave only an inert Cold file.
        fs::remove_file(&marker).await?;
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(
            reopened
                .ensure_actor("marker-root")
                .await?
                .actor
                .current_attempt,
            0
        );
        assert!(marker.is_file());
        reopened
            .claim_activation(&claim("marker-root", "run", "host", Utc::now()))
            .await?;
        let marker_bytes = fs::read(&marker).await?;
        fs::remove_file(&marker).await?;
        assert_eq!(
            reopened.inspect_actor("marker-root").await.unwrap_err(),
            ActorDirectoryError::Corrupt
        );
        fs::write(&marker, marker_bytes).await?;
        fs::remove_file(&sidecar).await?;
        let lost = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(
            lost.inspect_actor("marker-root").await.unwrap_err(),
            ActorDirectoryError::Corrupt
        );
        assert_eq!(
            lost.claim_activation(&claim("marker-root", "retry", "host", Utc::now()))
                .await
                .unwrap_err(),
            ActorDirectoryError::Corrupt
        );
        assert!(!sidecar.exists());

        store
            .save_session(&Session::new("direct-claim", "model"))
            .await?;
        let direct = store
            .claim_activation(&claim("direct-claim", "run", "host", Utc::now()))
            .await?;
        assert_eq!(direct.attempt, 1);
        assert!(home
            .path()
            .join("sessions/direct-claim/actor-authority.initialized.json")
            .is_file());
        Ok(())
    }
}
