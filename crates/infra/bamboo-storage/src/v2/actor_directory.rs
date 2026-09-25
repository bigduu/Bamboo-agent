//! SessionStoreV2's durable per-Session actor activation authority.
//!
//! The ordinary Session remains the identity/transcript source of truth. This
//! small sidecar is a versioned, serialized CAS record for execution attempts.
//! It is never derived from a physical worker or broker mailbox.

use std::io;
use std::path::Path;

use async_trait::async_trait;
use bamboo_domain::{
    ActorActivation, ActorActivationClaim, ActorActivationFence, ActorActivationFinish,
    ActorActivationStatus, ActorDirectoryEntry, ActorDirectoryError, ActorDirectoryPort,
    ActorLogicalState, ActorSession,
};
use chrono::{DateTime, Utc};
use tokio::fs;
use uuid::Uuid;

use super::{durable_atomic_write, SessionStoreV2};

const ACTOR_AUTHORITY_FILE: &str = "actor-authority.json";

fn storage(error: io::Error) -> ActorDirectoryError {
    ActorDirectoryError::Storage(error.to_string())
}

fn checked_next(value: u64) -> Result<u64, ActorDirectoryError> {
    value
        .checked_add(1)
        .ok_or(ActorDirectoryError::CounterOverflow)
}

fn current_live<'a>(
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
    async fn actor_authority_path(
        &self,
        actor_id: &str,
    ) -> Result<std::path::PathBuf, ActorDirectoryError> {
        let rel = self
            .resolve_rel_path(actor_id)
            .await
            .ok_or_else(|| ActorDirectoryError::NotFound(actor_id.to_string()))?;
        Ok(self.abs_path_from_rel(&rel).join(ACTOR_AUTHORITY_FILE))
    }

    async fn read_or_create_actor_entry(
        &self,
        actor_id: &str,
        path: &Path,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError> {
        let index = self
            .get_index_entry(actor_id)
            .await
            .ok_or_else(|| ActorDirectoryError::NotFound(actor_id.to_string()))?;
        let (kind, root_id) =
            Self::copy_source_identity_from_rel(actor_id, &index.rel_path).map_err(storage)?;
        let directory = self.abs_path_from_rel(&index.rel_path);
        let session = self
            .load_session_from_dir_strict(&directory, actor_id, kind, &root_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| ActorDirectoryError::NotFound(actor_id.to_string()))?;
        if session.id != actor_id {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let expected = ActorSession::from_session(&session)?;
        let entry = match fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_file() => {
                let raw = fs::read(path).await.map_err(storage)?;
                let entry: ActorDirectoryEntry =
                    serde_json::from_slice(&raw).map_err(|_| ActorDirectoryError::Corrupt)?;
                entry.validate()?;
                if !entry.actor.matches_session(&session) {
                    return Err(ActorDirectoryError::InvalidIdentity);
                }
                entry
            }
            Ok(_) => return Err(ActorDirectoryError::Corrupt),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // The Session was durably visible before this publication. A
                // crash here leaves an inert Cold actor, never an unowned Run.
                let entry = ActorDirectoryEntry::new(expected);
                self.write_actor_entry(path, &entry).await?;
                entry
            }
            Err(error) => return Err(storage(error)),
        };
        Ok(entry)
    }

    async fn write_actor_entry(
        &self,
        path: &Path,
        entry: &ActorDirectoryEntry,
    ) -> Result<(), ActorDirectoryError> {
        entry.validate()?;
        let bytes = serde_json::to_vec_pretty(entry).map_err(|_| ActorDirectoryError::Corrupt)?;
        durable_atomic_write(path, &bytes).await.map_err(storage)
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
        let _lifecycle = self
            .lock_session_lifecycle_shared()
            .await
            .map_err(storage)?;
        let _task = self
            .lock_runtime_task_sidecar_shared()
            .await
            .map_err(storage)?;
        let _session = self
            .acquire_session_maintenance_lock(actor_id)
            .await
            .map_err(storage)?;
        let path = self.actor_authority_path(actor_id).await?;
        let mut entry = self.read_or_create_actor_entry(actor_id, &path).await?;
        let Mutation { value, changed } = operation(&mut entry)?;
        if changed {
            entry.revision = checked_next(entry.revision)?;
            self.write_actor_entry(&path, &entry).await?;
        }
        Ok(value)
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
        ActorDirectoryPort, ActorPlacementClass, ActorPlacementRef, Session, Storage,
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
            }),
            now,
        }
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
}
