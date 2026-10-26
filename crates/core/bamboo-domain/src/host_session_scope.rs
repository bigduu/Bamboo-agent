//! Finite Host installation data. Serialized observations never grant access.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ActorAncestorObservation, ActorSession, ProjectId, Session, SessionAuthorityIdentity,
    SessionCallerReference, SessionKind,
};

/// The existing Supervisor logical operation address. Scope installation or
/// a new delivery intent cannot allocate another message for the same call.
/// This data is not authorization; the final writer still validates birth,
/// current grants and the complete original caller under physical guards.
pub fn supervisor_followup_message_id(
    supervisor: &crate::SupervisorReference,
    target_session_id: &str,
    operation_id: &str,
) -> crate::SessionMessageId {
    crate::SessionMessageId::stable(
        "supervisor_followup",
        &serde_json::json!({
            "supervisor": supervisor,
            "target_session_id": target_session_id,
            "operation_id": operation_id,
        }),
    )
}

#[derive(Debug, Clone, Copy)]
enum HostSessionPeerProtocol {
    PeerV1,
    SupervisorFollowup,
}

/// Pure wire/idempotency projection, never a grant. The physical writer must
/// recompute it from the original caller whose canonical identity it checks.
#[derive(Debug, Clone)]
pub struct HostSessionPeerIdentity {
    pub message_id: crate::SessionMessageId,
    pub thread_id: Option<String>,
    protocol: HostSessionPeerProtocol,
}

impl HostSessionPeerIdentity {
    pub fn for_caller(
        caller: &SessionCallerReference,
        target_id: &str,
        target_created_at: DateTime<Utc>,
        operation_id: &str,
    ) -> Self {
        let (message_id, protocol) = match &caller.authority_identity {
            SessionAuthorityIdentity::Supervisor { incarnation_id } => (
                supervisor_followup_message_id(
                    &crate::SupervisorReference {
                        session_id: caller.session_id.clone(),
                        incarnation_id: *incarnation_id,
                    },
                    target_id,
                    operation_id,
                ),
                HostSessionPeerProtocol::SupervisorFollowup,
            ),
            SessionAuthorityIdentity::Ordinary => (
                crate::session_peer_message_id(
                    &caller.session_id,
                    caller.created_at,
                    target_id,
                    target_created_at,
                    operation_id,
                ),
                HostSessionPeerProtocol::PeerV1,
            ),
        };
        let thread_id = match protocol {
            HostSessionPeerProtocol::PeerV1 => Some(message_id.to_string()),
            HostSessionPeerProtocol::SupervisorFollowup => None,
        };
        Self {
            message_id,
            thread_id,
            protocol,
        }
    }

    pub fn activation_intent(
        &self,
        delivery: crate::SessionMessageDeliveryIntent,
    ) -> crate::SessionInboxActivationIntent {
        match (self.protocol, delivery) {
            // Retain the existing Supervisor trigger's exact durable wire.
            // QueueOnly still has its own inert intent at this same address,
            // so changing it later is a conflict rather than a second send.
            (
                HostSessionPeerProtocol::SupervisorFollowup,
                crate::SessionMessageDeliveryIntent::TriggerTurn,
            ) => crate::SessionInboxActivationIntent::new(
                crate::SessionActivationPolicy::RespectSpecificWait,
            ),
            _ => crate::SessionInboxActivationIntent::with_delivery(delivery),
        }
    }
}

pub const HOST_SESSION_SCOPE_VERSION: u32 = 1;
pub const MAX_HOST_SESSION_TARGETS: usize = 128;
pub const MAX_HOST_SESSION_CREATES: usize = 32;
pub const MAX_HOST_SESSION_ANCESTORS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostSessionRight {
    ListMetadata,
    InspectBoundedProgress,
    /// Fixed immutable status/brief views, including bounded Task previews.
    /// Separate from the original progress/assistant-excerpt projection.
    ReadContextView,
    SendPeer,
}

/// Stable scope identity, independent of one conversation or tool invocation.
/// Keep this explicit projection separate from `SessionCallerReference`: later
/// execution observations may add generation fields, which must not revoke a
/// Host grant when an idle conversation resets without changing its identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionScopeIdentity {
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub kind: SessionKind,
    pub root_session_id: String,
    pub parent_session_id: Option<String>,
    pub parent_created_at: Option<DateTime<Utc>>,
    pub spawn_depth: u32,
    pub authority_identity: SessionAuthorityIdentity,
    pub root_tool_authority_revision: u64,
    pub session_version: Option<u32>,
    pub workspace: Option<String>,
}

impl From<&SessionCallerReference> for HostSessionScopeIdentity {
    fn from(caller: &SessionCallerReference) -> Self {
        Self {
            session_id: caller.session_id.clone(),
            created_at: caller.created_at,
            kind: caller.kind,
            root_session_id: caller.root_session_id.clone(),
            parent_session_id: caller.parent_session_id.clone(),
            parent_created_at: caller.parent_created_at,
            spawn_depth: caller.spawn_depth,
            authority_identity: caller.authority_identity.clone(),
            root_tool_authority_revision: caller.root_tool_authority_revision,
            session_version: caller.session_version,
            workspace: caller.workspace.clone(),
        }
    }
}

impl HostSessionScopeIdentity {
    /// Stable identity only. The final operation must separately validate the
    /// original caller's complete conversation generation under its own locks.
    pub fn matches_caller(&self, caller: &SessionCallerReference) -> bool {
        self == &Self::from(caller)
    }
}

/// Original scope/lineage data, without conversation state or tool arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostScopeSessionObservation {
    pub identity: HostSessionScopeIdentity,
    pub metadata_version: u64,
    pub project_id: Option<ProjectId>,
    /// Direct parent first, Root last, just like the canonical Actor directory.
    pub ancestors: Vec<ActorAncestorObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostSessionScopeError {
    #[error("host_session_scope_invalid: explicit finite scope is malformed")]
    Invalid,
    #[error("host_session_scope_stale: original Session identity/Project changed")]
    Stale,
    #[error("host_session_scope_denied: this exact operation is not granted")]
    Denied,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.trim() == id && !id.contains(['/', '\\']) && !id.contains("..")
}

impl HostScopeSessionObservation {
    /// A Host supplies the complete canonically read ancestor observations.
    /// This captures data only; final storage still checks the complete chain.
    pub fn capture(
        session: &Session,
        ancestors: Vec<ActorAncestorObservation>,
    ) -> Result<Self, HostSessionScopeError> {
        session
            .validate_conversation_state()
            .map_err(|_| HostSessionScopeError::Invalid)?;
        let actor =
            ActorSession::from_session(session).map_err(|_| HostSessionScopeError::Invalid)?;
        let observation = Self {
            identity: HostSessionScopeIdentity::from(&SessionCallerReference::from(session)),
            metadata_version: session.metadata_version,
            project_id: actor
                .project_id
                .map(|id| id.parse().map_err(|_| HostSessionScopeError::Invalid))
                .transpose()?,
            ancestors,
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn validate(&self) -> Result<(), HostSessionScopeError> {
        let caller = &self.identity;
        if !valid_id(&caller.session_id)
            || !valid_id(&caller.root_session_id)
            || self.ancestors.len() > MAX_HOST_SESSION_ANCESTORS
            || self.ancestors.len() != caller.spawn_depth as usize
        {
            return Err(HostSessionScopeError::Invalid);
        }
        let mut ids = BTreeSet::from([caller.session_id.as_str()]);
        if self.ancestors.iter().any(|a| {
            !valid_id(&a.actor_id)
                || !ids.insert(&a.actor_id)
                || a.session_created_at > caller.created_at
        }) {
            return Err(HostSessionScopeError::Invalid);
        }
        match caller.kind {
            SessionKind::Root
                if caller.root_session_id == caller.session_id
                    && caller.parent_session_id.is_none()
                    && caller.parent_created_at.is_none()
                    && caller.spawn_depth == 0 => {}
            SessionKind::Child
                if caller.spawn_depth > 0
                    && self.ancestors.first().is_some_and(|p| {
                        Some(p.actor_id.as_str()) == caller.parent_session_id.as_deref()
                            && Some(p.session_created_at) == caller.parent_created_at
                    })
                    && self
                        .ancestors
                        .last()
                        .is_some_and(|root| root.actor_id == caller.root_session_id) => {}
            _ => return Err(HostSessionScopeError::Invalid),
        }
        Ok(())
    }

    pub fn check_session(&self, current: &Session) -> Result<(), HostSessionScopeError> {
        self.validate()?;
        current
            .validate_conversation_state()
            .map_err(|_| HostSessionScopeError::Stale)?;
        let project = ActorSession::from_session(current)
            .map_err(|_| HostSessionScopeError::Stale)?
            .project_id;
        if !self
            .identity
            .matches_caller(&SessionCallerReference::from(current))
            || self.metadata_version != current.metadata_version
            || self.project_id.as_ref().map(ProjectId::as_str) != project.as_deref()
        {
            return Err(HostSessionScopeError::Stale);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionTargetScope {
    pub observation: HostScopeSessionObservation,
    pub rights: BTreeSet<HostSessionRight>,
}

/// One explicit creation operation. Workspace bytes are already bound by Host;
/// the existing canonical workspace resolver must recheck them before creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionCreateScope {
    pub operation_id: Uuid,
    pub project_id: ProjectId,
    pub workspace_path: String,
    /// Empty by default. Creation/receipts do not imply follow-up permission.
    #[serde(default)]
    pub created_target_rights: BTreeSet<HostSessionRight>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionScopeBinding {
    pub schema_version: u32,
    pub grant_id: Uuid,
    pub revision: u64,
    pub caller: HostScopeSessionObservation,
    pub targets: BTreeMap<String, HostSessionTargetScope>,
    pub creates: BTreeMap<Uuid, HostSessionCreateScope>,
}

impl HostSessionScopeBinding {
    pub fn validate(&self) -> Result<(), HostSessionScopeError> {
        self.caller.validate()?;
        if self.schema_version != HOST_SESSION_SCOPE_VERSION
            || self.grant_id.is_nil()
            || self.revision == 0
            || self.targets.len() > MAX_HOST_SESSION_TARGETS
            || self.creates.len() > MAX_HOST_SESSION_CREATES
            || (self.targets.is_empty() && self.creates.is_empty())
        {
            return Err(HostSessionScopeError::Invalid);
        }
        for (id, target) in &self.targets {
            target.observation.validate()?;
            if id != &target.observation.identity.session_id
                || id == &self.caller.identity.session_id
                || target.observation.project_id.is_none()
                || target.rights.is_empty()
            {
                return Err(HostSessionScopeError::Invalid);
            }
        }
        for (id, create) in &self.creates {
            let path = Path::new(&create.workspace_path);
            if id.is_nil()
                || id != &create.operation_id
                || !path.is_absolute()
                || create.workspace_path.trim() != create.workspace_path
                || path
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
            {
                return Err(HostSessionScopeError::Invalid);
            }
        }
        Ok(())
    }

    pub fn target(
        &self,
        id: &str,
        right: HostSessionRight,
    ) -> Result<&HostSessionTargetScope, HostSessionScopeError> {
        self.validate()?;
        self.targets
            .get(id)
            .filter(|target| target.rights.contains(&right))
            .ok_or(HostSessionScopeError::Denied)
    }

    pub fn create(
        &self,
        operation_id: Uuid,
        project: &ProjectId,
        workspace: &str,
    ) -> Result<&HostSessionCreateScope, HostSessionScopeError> {
        self.validate()?;
        self.creates
            .get(&operation_id)
            .filter(|scope| &scope.project_id == project && scope.workspace_path == workspace)
            .ok_or(HostSessionScopeError::Denied)
    }
}

/// Non-deserializable Host installation identity. Another issuer cannot mint a
/// token for a store's installed authority, even by copying all serialized data.
#[derive(Debug, Clone)]
pub struct HostSessionScopeIssuer(Arc<Uuid>);

#[derive(Debug, Clone)]
pub struct HostSessionScopeToken {
    issuer: Arc<Uuid>,
    binding: Arc<HostSessionScopeBinding>,
}

impl Default for HostSessionScopeIssuer {
    fn default() -> Self {
        Self(Arc::new(Uuid::new_v4()))
    }
}

impl HostSessionScopeIssuer {
    /// Trusted in-process Host API only. A registry must install the returned
    /// token and retain its publication fence through every final operation.
    pub fn issue(
        &self,
        binding: HostSessionScopeBinding,
    ) -> Result<HostSessionScopeToken, HostSessionScopeError> {
        binding.validate()?;
        Ok(HostSessionScopeToken {
            issuer: Arc::clone(&self.0),
            binding: Arc::new(binding),
        })
    }
    pub fn recognizes(&self, token: &HostSessionScopeToken) -> bool {
        Arc::ptr_eq(&self.0, &token.issuer)
    }
}

impl HostSessionScopeToken {
    pub fn binding(&self) -> &HostSessionScopeBinding {
        &self.binding
    }
    pub fn same_installation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.issuer, &other.issuer) && Arc::ptr_eq(&self.binding, &other.binding)
    }
}

#[cfg(test)]
#[path = "host_session_scope_tests.rs"]
mod tests;
