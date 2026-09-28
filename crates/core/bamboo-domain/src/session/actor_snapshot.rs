//! Read-only actor subtree projection. This is a view identity, not an event cursor.

use async_trait::async_trait;
use serde::Serialize;

use super::{
    ActorActivationFence, ActorActivationStatus, ActorLogicalState, ActorPlacementClass,
    SessionKind,
};

pub const ACTOR_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Constructed by trusted host/runtime code, never decoded from a public request.
/// A selector or a Session ID alone cannot confer actor authority.
pub struct ActorSnapshotPrincipal(Principal);

pub enum ActorSnapshotAuthority<'a> {
    HostOwner,
    LiveActor(&'a ActorActivationFence),
}

enum Principal {
    HostOwner,
    LiveActor(ActorActivationFence),
}

impl ActorSnapshotPrincipal {
    /// The gateway must first prove LocalBypass or authenticated host credentials.
    pub fn host_owner() -> Self {
        Self(Principal::HostOwner)
    }

    /// Internal runtime callers supply the complete current activation fence.
    pub fn live_actor(fence: ActorActivationFence) -> Self {
        Self(Principal::LiveActor(fence))
    }

    pub fn authority(&self) -> ActorSnapshotAuthority<'_> {
        match &self.0 {
            Principal::HostOwner => ActorSnapshotAuthority::HostOwner,
            Principal::LiveActor(fence) => ActorSnapshotAuthority::LiveActor(fence),
        }
    }
}

/// Callers may tighten these ceilings; adapters reject requests to raise them.
#[derive(Clone, Copy, Debug)]
pub struct ActorSnapshotLimits {
    pub nodes: usize,
    pub directory_entries: usize,
    pub file_reads: usize,
    pub file_bytes: usize,
    pub aggregate_read_bytes: usize,
    pub response_bytes: usize,
}

impl Default for ActorSnapshotLimits {
    fn default() -> Self {
        Self {
            nodes: 256,
            directory_entries: 4096,
            file_reads: 2048,
            file_bytes: 512 * 1024,
            aggregate_read_bytes: 8 * 1024 * 1024,
            response_bytes: 256 * 1024,
        }
    }
}

impl ActorSnapshotLimits {
    pub fn is_bounded(self) -> bool {
        let ceiling = Self::default();
        [
            (self.nodes, ceiling.nodes),
            (self.directory_entries, ceiling.directory_entries),
            (self.file_reads, ceiling.file_reads),
            (self.file_bytes, ceiling.file_bytes),
            (self.aggregate_read_bytes, ceiling.aggregate_read_bytes),
            (self.response_bytes, ceiling.response_bytes),
        ]
        .into_iter()
        .all(|(value, max)| value > 0 && value <= max)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PublicActorActivation {
    pub activation_id: String,
    pub attempt: u64,
    pub status: ActorActivationStatus,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PublicActorRevision {
    pub session_metadata_version: u64,
    /// None means no proven actor directory row exists; zero is never synthesized.
    pub actor_directory_revision: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PublicActorSnapshotNode {
    pub actor_id: String,
    pub parent_actor_id: Option<String>,
    pub root_actor_id: String,
    pub depth: u32,
    /// A bounded projection of the already public Session title.
    pub title: String,
    pub role: SessionKind,
    pub logical_state: Option<ActorLogicalState>,
    pub placement_class: Option<ActorPlacementClass>,
    pub revision: PublicActorRevision,
    pub activation: Option<PublicActorActivation>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PublicActorSubtreeSnapshot {
    pub schema_version: u32,
    pub root_actor_id: String,
    pub subtree_actor_id: String,
    /// Opaque equality identity only. It provides no ordering or replay position.
    pub snapshot_id: String,
    pub stream_cursor: Option<String>,
    pub nodes: Vec<PublicActorSnapshotNode>,
}

/// Static diagnostic codes carry no paths, private authority, or source contents.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorSnapshotError {
    #[error("invalid_selector")]
    InvalidSelector,
    #[error("not_found")]
    NotFound,
    #[error("unauthorized_scope")]
    UnauthorizedScope,
    #[error("stale_authority")]
    StaleAuthority,
    #[error("inconsistent_authority")]
    InconsistentAuthority,
    #[error("pending_transaction")]
    PendingTransaction,
    #[error("budget_exceeded")]
    BudgetExceeded,
    #[error("unsupported_authority")]
    UnsupportedAuthority,
    #[error("storage_unavailable")]
    StorageUnavailable,
}

#[async_trait]
pub trait ActorSnapshotPort: Send + Sync {
    async fn actor_subtree_snapshot(
        &self,
        principal: ActorSnapshotPrincipal,
        root_id: &str,
        subtree_id: &str,
        limits: ActorSnapshotLimits,
    ) -> Result<PublicActorSubtreeSnapshot, ActorSnapshotError>;
}
