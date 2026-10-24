//! Role-neutral observation of the Session that admitted an operation.
//!
//! This is identity, never a grant. Hosts must still revalidate their current
//! Project/target scope, permissions and writer capability on every operation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Session, SessionAuthorityIdentity, SessionKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCallerReference {
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
    /// Original operation generation; scope permissions use a separate stable identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_reset: Option<super::ConversationResetState>,
}

impl From<&Session> for SessionCallerReference {
    /// Capture from the original executing snapshot before dispatch. Loading a
    /// replacement Session later cannot reconstruct the original observation.
    fn from(session: &Session) -> Self {
        Self {
            session_id: session.id.clone(),
            created_at: session.created_at,
            kind: session.kind,
            root_session_id: session.root_session_id.clone(),
            parent_session_id: session.parent_session_id.clone(),
            parent_created_at: session.parent_created_at,
            spawn_depth: session.spawn_depth,
            authority_identity: session.authority_identity.clone(),
            root_tool_authority_revision: session.root_tool_authority_revision,
            session_version: session.session_version,
            workspace: session.workspace.clone(),
            conversation_reset: session.conversation_reset.clone(),
        }
    }
}

impl SessionCallerReference {
    pub fn matches_session(&self, session: &Session) -> bool {
        session.validate_conversation_state().is_ok() && self == &Self::from(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};

    #[test]
    fn role_neutral_observation_preserves_each_caller_without_a_grant() {
        let root = Session::new("ordinary", "model");
        let child = Session::new_child_of("child", &root, "model", "short task");
        let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: uuid::Uuid::new_v4(),
        };
        for session in [root, child, supervisor] {
            let before = serde_json::to_value(&session).unwrap();
            let reference = SessionCallerReference::from(&session);
            assert!(reference.matches_session(&session));
            assert_eq!(serde_json::to_value(&session).unwrap(), before);
            let restored: SessionCallerReference =
                serde_json::from_value(serde_json::to_value(reference).unwrap()).unwrap();
            assert!(restored.matches_session(&session));
        }
    }

    #[test]
    fn observation_rejects_recreated_lifetime_ancestry_workspace_or_tool_revision() {
        let session = Session::new("same-id", "model");
        let reference = SessionCallerReference::from(&session);
        let mut changed = session.clone();
        changed.created_at += chrono::Duration::seconds(1);
        assert!(!reference.matches_session(&changed));
        let mut changed = session.clone();
        changed.parent_session_id = Some("another-parent".into());
        assert!(!reference.matches_session(&changed));
        let mut changed = session.clone();
        changed.workspace = Some("/another-workspace".into());
        assert!(!reference.matches_session(&changed));
        let mut changed = session.clone();
        changed.root_tool_authority_revision += 1;
        assert!(!reference.matches_session(&changed));
        let mut cosmetic = session.clone();
        cosmetic.title = "renamed without changing authority".into();
        cosmetic.metadata_version += 1;
        assert!(reference.matches_session(&cosmetic));
    }
}
