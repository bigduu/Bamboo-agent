//! Durable logical actor identity and activation authority contracts.
//!
//! A Session is the source of actor identity and history. This record only
//! stores the activation state needed to fence replaceable execution owners.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Session, SessionKind};
use crate::ProjectId;

pub const ACTOR_DIRECTORY_SCHEMA_VERSION: u32 = 2;

/// A stable logical address. It is always exactly Session.id.
pub type ActorId = String;

fn valid_actor_id(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && !value.contains('/')
        && !value.contains('\\')
        && !value.contains("..")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorLogicalState {
    Cold,
    Active,
    Failed,
    Retired,
}

/// A placement preference contains no endpoint, token, PID, or mailbox id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorPlacementIntent {
    Local,
    Pool { pool_id: String },
    PinnedHost { host_ref: String },
}

/// Policy and placement selected by the Host control plane for a logical Actor.
/// Missing fields leave an already-bound value unchanged. Neither field may
/// be populated from a worker event, physical lease, or UI placement badge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorControlPlaneBinding {
    pub policy_revision: Option<u64>,
    pub placement_intent: Option<ActorPlacementIntent>,
}

impl ActorPlacementIntent {
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Local => true,
            Self::Pool { pool_id } => !pool_id.trim().is_empty() && pool_id.trim() == pool_id,
            Self::PinnedHost { host_ref } => {
                !host_ref.trim().is_empty() && host_ref.trim() == host_ref
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorPlacementClass {
    Local,
    Docker,
    Ssh,
    Remote,
    Schedulable,
}

/// An internal opaque placement lease reference, never a network address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorPlacementRef {
    pub class: ActorPlacementClass,
    pub lease_id: String,
    /// Slot incarnation within one WorkerHost generation. Older local and
    /// deployment records deserialize without it; remote admission requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_epoch: Option<u64>,
}

/// One saved ancestor identity and revision in a Child's authority chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorAncestorObservation {
    pub actor_id: ActorId,
    pub session_created_at: DateTime<Utc>,
    pub metadata_version: u64,
}

/// Versioned projection of durable Session identity plus logical lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorSession {
    pub schema_version: u32,
    pub actor_id: ActorId,
    /// Fences an explicitly deleted/recreated Session with the same public id.
    pub session_created_at: DateTime<Utc>,
    pub parent_actor_id: Option<ActorId>,
    pub root_actor_id: ActorId,
    pub project_id: Option<String>,
    /// Last observed revision of this Session's own metadata. A Project
    /// change advances this revision exactly once at the Session boundary.
    pub observed_metadata_version: u64,
    /// Direct parent first, Root last. Storage stamps this from the strict
    /// durable lineage before publishing a Child's authority record.
    pub ancestor_observations: Vec<ActorAncestorObservation>,
    pub spawn_depth: u32,
    pub state: ActorLogicalState,
    pub current_attempt: u64,
    /// Effective permission-policy revision bound from Host configuration.
    pub policy_revision: Option<u64>,
    /// Host-resolved logical placement preference, independent of the current
    /// physical activation's placement lease.
    pub placement_intent: Option<ActorPlacementIntent>,
}

impl ActorSession {
    /// Derive stable actor identity and current Project context from a saved Session.
    pub fn from_session(session: &Session) -> Result<Self, ActorDirectoryError> {
        let actor_id = session.id.as_str();
        let root_id = session.root_session_id.as_str();
        if !valid_actor_id(actor_id) || !valid_actor_id(root_id) {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let parent_actor_id = match session.kind {
            SessionKind::Root => {
                if session.parent_session_id.is_some()
                    || session.spawn_depth != 0
                    || root_id != actor_id
                {
                    return Err(ActorDirectoryError::InvalidIdentity);
                }
                None
            }
            SessionKind::Child => {
                let Some(parent) = session.parent_session_id.as_ref() else {
                    return Err(ActorDirectoryError::InvalidIdentity);
                };
                if !valid_actor_id(parent)
                    || parent == actor_id
                    || root_id == actor_id
                    || session.spawn_depth == 0
                {
                    return Err(ActorDirectoryError::InvalidIdentity);
                }
                Some(parent.clone())
            }
        };
        let project_id = session
            .project_id_meta()
            .map(|id| {
                id.trim()
                    .parse::<ProjectId>()
                    .map(ProjectId::into_string)
                    .map_err(|_| ActorDirectoryError::InvalidIdentity)
            })
            .transpose()?;
        Ok(Self {
            schema_version: ACTOR_DIRECTORY_SCHEMA_VERSION,
            actor_id: actor_id.to_string(),
            session_created_at: session.created_at,
            parent_actor_id,
            root_actor_id: root_id.to_string(),
            project_id,
            observed_metadata_version: session.metadata_version,
            ancestor_observations: Vec::new(),
            spawn_depth: session.spawn_depth,
            state: ActorLogicalState::Cold,
            current_attempt: 0,
            policy_revision: None,
            placement_intent: None,
        })
    }

    pub fn matches_session(&self, session: &Session) -> bool {
        Self::from_session(session).is_ok_and(|expected| {
            self.schema_version == expected.schema_version
                && self.actor_id == expected.actor_id
                && self.session_created_at == expected.session_created_at
                && self.parent_actor_id == expected.parent_actor_id
                && self.root_actor_id == expected.root_actor_id
                && self.spawn_depth == expected.spawn_depth
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorActivationStatus {
    Reserved,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl ActorActivationStatus {
    pub fn is_live(self) -> bool {
        matches!(self, Self::Reserved | Self::Running)
    }
}

/// One execution attempt. The owner reference is internal control-plane data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorActivation {
    pub schema_version: u32,
    pub actor_id: ActorId,
    pub activation_id: String,
    pub attempt: u64,
    pub run_id: String,
    pub lease_owner: String,
    pub lease_epoch: u64,
    pub lease_expires_at: DateTime<Utc>,
    /// Project identity at claim time; a later cold Root reassignment does not
    /// relabel the completed activation's authority.
    pub project_id: Option<String>,
    pub inbox_generation: u64,
    pub placement_ref: Option<ActorPlacementRef>,
    pub status: ActorActivationStatus,
    pub checkpoint_revision: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl ActorActivation {
    pub fn fence(&self) -> ActorActivationFence {
        ActorActivationFence {
            schema_version: self.schema_version,
            actor_id: self.actor_id.clone(),
            activation_id: self.activation_id.clone(),
            attempt: self.attempt,
            run_id: self.run_id.clone(),
            lease_owner: self.lease_owner.clone(),
            lease_epoch: self.lease_epoch,
        }
    }

    pub fn matches_fence(&self, fence: &ActorActivationFence) -> bool {
        self.fence() == *fence
    }
}

/// The exact authority required before a writer emits an event, mutates the
/// transcript, cancels an activation, or acknowledges its inbox claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorActivationFence {
    pub schema_version: u32,
    pub actor_id: ActorId,
    pub activation_id: String,
    pub attempt: u64,
    pub run_id: String,
    pub lease_owner: String,
    pub lease_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorDirectoryEntry {
    pub schema_version: u32,
    pub revision: u64,
    pub actor: ActorSession,
    pub activation: Option<ActorActivation>,
}

impl ActorDirectoryEntry {
    pub fn new(actor: ActorSession) -> Self {
        Self {
            schema_version: ACTOR_DIRECTORY_SCHEMA_VERSION,
            revision: 1,
            actor,
            activation: None,
        }
    }

    pub fn validate(&self) -> Result<(), ActorDirectoryError> {
        if self.schema_version != ACTOR_DIRECTORY_SCHEMA_VERSION
            || self.actor.schema_version != ACTOR_DIRECTORY_SCHEMA_VERSION
            || self.revision == 0
            || !valid_actor_id(&self.actor.actor_id)
            || !valid_actor_id(&self.actor.root_actor_id)
            || self
                .actor
                .project_id
                .as_ref()
                .is_some_and(|id| id.parse::<ProjectId>().is_err())
            || match self.actor.parent_actor_id.as_deref() {
                None => {
                    self.actor.spawn_depth != 0
                        || self.actor.root_actor_id != self.actor.actor_id
                        || !self.actor.ancestor_observations.is_empty()
                }
                Some(parent) => {
                    !valid_actor_id(parent)
                        || parent == self.actor.actor_id
                        || self.actor.root_actor_id == self.actor.actor_id
                        || self.actor.spawn_depth == 0
                        || self.actor.ancestor_observations.len() != self.actor.spawn_depth as usize
                        || self
                            .actor
                            .ancestor_observations
                            .first()
                            .is_none_or(|ancestor| ancestor.actor_id != parent)
                        || self
                            .actor
                            .ancestor_observations
                            .last()
                            .is_none_or(|ancestor| ancestor.actor_id != self.actor.root_actor_id)
                }
            }
            || self
                .actor
                .placement_intent
                .as_ref()
                .is_some_and(|intent| !intent.is_valid())
        {
            return Err(ActorDirectoryError::Corrupt);
        }
        match &self.activation {
            None if self.actor.current_attempt == 0
                && self.actor.state == ActorLogicalState::Cold => {}
            None if self.actor.state == ActorLogicalState::Retired => {}
            None => return Err(ActorDirectoryError::Corrupt),
            Some(activation) => {
                if activation.actor_id != self.actor.actor_id
                    || activation.schema_version != ACTOR_DIRECTORY_SCHEMA_VERSION
                    || activation.attempt == 0
                    || activation.attempt != self.actor.current_attempt
                    || activation.lease_epoch == 0
                    || activation.lease_epoch < activation.attempt
                    || activation.activation_id.is_empty()
                    || activation.run_id.trim().is_empty()
                    || activation.lease_owner.trim().is_empty()
                    || activation
                        .project_id
                        .as_ref()
                        .is_some_and(|id| id.parse::<ProjectId>().is_err())
                    || (activation.status.is_live()
                        && activation.project_id != self.actor.project_id)
                    || activation.placement_ref.as_ref().is_some_and(|placement| {
                        placement.lease_id.trim().is_empty()
                            || (matches!(
                                placement.class,
                                ActorPlacementClass::Remote | ActorPlacementClass::Schedulable
                            ) && activation.status.is_live()
                                && !matches!(placement.slot_epoch, Some(epoch) if epoch > 0))
                    })
                {
                    return Err(ActorDirectoryError::Corrupt);
                }
                let expected_state = match activation.status {
                    ActorActivationStatus::Reserved | ActorActivationStatus::Running => {
                        ActorLogicalState::Active
                    }
                    ActorActivationStatus::Succeeded | ActorActivationStatus::Cancelled => {
                        ActorLogicalState::Cold
                    }
                    ActorActivationStatus::Failed => ActorLogicalState::Failed,
                };
                if self.actor.state != expected_state
                    && self.actor.state != ActorLogicalState::Retired
                {
                    return Err(ActorDirectoryError::Corrupt);
                }
                if self.actor.state == ActorLogicalState::Retired && activation.status.is_live() {
                    return Err(ActorDirectoryError::Corrupt);
                }
                if activation.status.is_live() && activation.finished_at.is_some() {
                    return Err(ActorDirectoryError::Corrupt);
                }
                if !activation.status.is_live() && activation.finished_at.is_none() {
                    return Err(ActorDirectoryError::Corrupt);
                }
                if activation.status == ActorActivationStatus::Reserved
                    && activation.started_at.is_some()
                {
                    return Err(ActorDirectoryError::Corrupt);
                }
                if matches!(
                    activation.status,
                    ActorActivationStatus::Running | ActorActivationStatus::Succeeded
                ) && activation.started_at.is_none()
                {
                    return Err(ActorDirectoryError::Corrupt);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ActorActivationClaim {
    pub actor_id: ActorId,
    pub run_id: String,
    pub lease_owner: String,
    pub lease_expires_at: DateTime<Utc>,
    pub inbox_generation: u64,
    pub placement_ref: Option<ActorPlacementRef>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorActivationFinish {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ActorDirectoryError {
    #[error("actor Session does not exist: {0}")]
    NotFound(String),
    #[error("actor Session identity is invalid or changed")]
    InvalidIdentity,
    #[error("Root Project context changed or cannot be proven stable while an activation is live")]
    ProjectTransitionBlocked,
    #[error("effective actor policy revision conflicts with the Host's previously bound revision")]
    PolicyTransitionBlocked,
    #[error("actor placement intent conflicts with the Host's previously bound intent")]
    PlacementIntentConflict,
    #[error("actor authority record is malformed, unsupported, or inconsistent")]
    Corrupt,
    #[error("actor activation is already owned by another live attempt")]
    Busy,
    #[error("actor activation fence is stale or expired")]
    StaleFence,
    #[error("actor lifecycle transition is invalid")]
    InvalidTransition,
    #[error("actor authority counter cannot advance")]
    CounterOverflow,
    #[error("actor authority storage failed: {0}")]
    Storage(String),
}

/// Narrow storage/runtime port. Implementations must serialize each mutation
/// across processes and verify the persisted Session before any activation.
#[async_trait]
pub trait ActorDirectoryPort: Send + Sync {
    async fn ensure_actor(
        &self,
        actor_id: &str,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError>;
    async fn inspect_actor(
        &self,
        actor_id: &str,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError>;
    /// Bind the effective revision and logical placement selected by trusted
    /// Host configuration before activation. Physical worker claims and
    /// Session display metadata are never sources for this binding. Existing
    /// values survive Project reassignment and physical replacement.
    async fn bind_control_plane(
        &self,
        actor_id: &str,
        binding: ActorControlPlaneBinding,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError>;
    async fn claim_activation(
        &self,
        claim: &ActorActivationClaim,
    ) -> Result<ActorActivation, ActorDirectoryError>;
    async fn start_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
    ) -> Result<ActorActivation, ActorDirectoryError>;
    async fn renew_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<ActorActivation, ActorDirectoryError>;
    async fn checkpoint_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        expected_checkpoint_revision: u64,
    ) -> Result<ActorActivation, ActorDirectoryError>;
    async fn finish_activation(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
        outcome: ActorActivationFinish,
    ) -> Result<ActorActivation, ActorDirectoryError>;
    async fn retire_actor(
        &self,
        actor_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ActorDirectoryEntry, ActorDirectoryError>;
    /// Observe the current owner. This observation expires as soon as the
    /// operation returns; transcript writes and Inbox acks need an integrated
    /// final mutation under the same authority lock, not check-then-write.
    async fn validate_fence(
        &self,
        fence: &ActorActivationFence,
        now: DateTime<Utc>,
    ) -> Result<(), ActorDirectoryError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_identity_is_the_session_tree_identity() {
        let root = Session::new("root", "model");
        let child = Session::new_child_of("child", &root, "model", "Child");
        let grandchild = Session::new_child_of("grandchild", &child, "model", "Grandchild");
        let actor = ActorSession::from_session(&grandchild).unwrap();
        assert_eq!(actor.actor_id, grandchild.id);
        assert_eq!(actor.root_actor_id, root.id);
        assert_eq!(actor.parent_actor_id.as_deref(), Some(child.id.as_str()));
        assert_eq!(actor.spawn_depth, 2);
        assert!(actor.matches_session(&grandchild));

        let mut recreated = grandchild.clone();
        recreated.created_at += chrono::Duration::seconds(1);
        assert!(!actor.matches_session(&recreated));
        let mut moved = grandchild.clone();
        moved.parent_session_id = Some("sibling".into());
        assert!(!actor.matches_session(&moved));
        let mut rebound = grandchild.clone();
        rebound.set_project_id_meta("project-a");
        assert!(actor.matches_session(&rebound));
    }

    #[test]
    fn malformed_actor_state_is_rejected_before_use() {
        let root = Session::new("root", "model");
        let mut entry = ActorDirectoryEntry::new(ActorSession::from_session(&root).unwrap());
        assert!(entry.validate().is_ok());
        entry.actor.state = ActorLogicalState::Active;
        assert_eq!(entry.validate(), Err(ActorDirectoryError::Corrupt));
        entry.actor.state = ActorLogicalState::Cold;
        entry.schema_version += 1;
        assert_eq!(entry.validate(), Err(ActorDirectoryError::Corrupt));

        let mut malformed_child = Session::new_child_of("child", &root, "model", "Child");
        malformed_child.parent_session_id = None;
        assert_eq!(
            ActorSession::from_session(&malformed_child),
            Err(ActorDirectoryError::InvalidIdentity)
        );
    }

    #[test]
    fn terminal_legacy_remote_placement_without_slot_epoch_remains_readable() {
        for class in [
            ActorPlacementClass::Remote,
            ActorPlacementClass::Schedulable,
        ] {
            let root = Session::new("legacy-remote-root", "model");
            let now = chrono::Utc::now();
            let mut entry = ActorDirectoryEntry::new(ActorSession::from_session(&root).unwrap());
            entry.actor.current_attempt = 1;
            entry.actor.state = ActorLogicalState::Failed;
            entry.activation = Some(ActorActivation {
                schema_version: ACTOR_DIRECTORY_SCHEMA_VERSION,
                actor_id: root.id.clone(),
                activation_id: "legacy-activation".into(),
                attempt: 1,
                run_id: "legacy-run".into(),
                lease_owner: "legacy-host".into(),
                lease_epoch: 1,
                lease_expires_at: now + chrono::Duration::minutes(1),
                project_id: None,
                inbox_generation: 0,
                placement_ref: Some(ActorPlacementRef {
                    class,
                    lease_id: "legacy-placement".into(),
                    slot_epoch: None,
                }),
                status: ActorActivationStatus::Failed,
                checkpoint_revision: 0,
                started_at: Some(now),
                finished_at: Some(now),
            });
            let saved = serde_json::to_value(&entry).unwrap();
            assert!(saved["activation"]["placement_ref"]
                .get("slot_epoch")
                .is_none());
            let mut restored: ActorDirectoryEntry = serde_json::from_value(saved).unwrap();
            assert_eq!(restored.validate(), Ok(()));

            restored.actor.state = ActorLogicalState::Active;
            let activation = restored.activation.as_mut().unwrap();
            activation.status = ActorActivationStatus::Running;
            activation.finished_at = None;
            assert_eq!(restored.validate(), Err(ActorDirectoryError::Corrupt));
        }
    }
}
