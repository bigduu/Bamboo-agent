//! Current-application-user management observations, never reusable grants.
//! Storage validates the canonical Supervisor in its own namespace on every
//! operation. A model-provided ID, cursor or copy of these bytes grants nothing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ProjectId, Session, SessionCallerReference, SessionKind, SupervisorReference};

pub const MAX_SUPERVISOR_DIRECTORY_PAGE: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorCurrentUserScope {
    pub supervisor: SupervisorReference,
    pub created_at: DateTime<Utc>,
    pub orchestration_revision: u64,
    /// Retained compatibility/Observer CAS state; it does not restrict default
    /// Supervisor management to the old Project list or attachment table.
    pub legacy_state_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorCurrentUserTarget {
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub kind: SessionKind,
    pub root_session_id: String,
    pub parent_session_id: Option<String>,
    /// Commitment to the full original identity/generation/workspace, without
    /// exporting its reset receipt ledger or workspace bytes into every page.
    /// A hash is an observation, not a grant; the final writer recomputes it.
    pub authority_sha256: String,
    /// Commitment to the canonical ancestor births and observations selected
    /// with this target. A Child cannot outlive an observed parent generation.
    pub ancestry_sha256: String,
    pub metadata_version: u64,
    pub project_id: Option<ProjectId>,
}

impl SupervisorCurrentUserTarget {
    pub fn capture(session: &Session) -> Result<Self, &'static str> {
        Self::capture_with_ancestors(session, &[])
    }

    pub fn capture_with_ancestors(
        session: &Session,
        ancestors: &[crate::ActorAncestorObservation],
    ) -> Result<Self, &'static str> {
        session
            .validate_conversation_state()
            .map_err(|_| "invalid target conversation authority")?;
        crate::ActorSession::from_session(session)
            .map_err(|_| "invalid canonical target identity")?;
        if !session.authority_identity.is_ordinary() {
            return Err("current-user target must have ordinary Session authority");
        }
        crate::HostScopeSessionObservation::capture(session, ancestors.to_vec())
            .map_err(|_| "invalid canonical target ancestry")?;
        Ok(Self {
            session_id: session.id.clone(),
            created_at: session.created_at,
            kind: session.kind,
            root_session_id: session.root_session_id.clone(),
            parent_session_id: session.parent_session_id.clone(),
            authority_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(&SessionCallerReference::from(session))
                    .map_err(|_| "invalid target authority encoding")?,
            )),
            ancestry_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(ancestors).map_err(|_| "invalid target ancestry encoding")?,
            )),
            metadata_version: session.metadata_version,
            project_id: session
                .project_id_meta()
                .map(|id| id.parse().map_err(|_| "invalid target Project"))
                .transpose()?,
        })
    }

    pub fn matches(&self, session: &Session) -> bool {
        Self::capture(session).is_ok_and(|current| current == *self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorCurrentUserPage {
    pub scope: SupervisorCurrentUserScope,
    pub targets: Vec<SupervisorCurrentUserTarget>,
    /// A locator only, validated together with the caller and request. It does
    /// not authorize a target or promise a frozen global directory snapshot.
    pub next_after: Option<String>,
}

/// Bind new followups to the actual target lifetime while retaining the old
/// logical operation convention. Old v1 receipts remain at their old address.
pub fn supervisor_current_user_message_id(
    supervisor: &SupervisorReference,
    target: &SupervisorCurrentUserTarget,
    operation_id: &str,
) -> crate::SessionMessageId {
    crate::SessionMessageId::stable(
        "supervisor_current_user_followup_v1",
        &serde_json::json!({
            "supervisor":supervisor,
            "target_id":target.session_id,
            "target_created_at":target.created_at,
            "operation_id":operation_id,
        }),
    )
}
