//! Trusted Session identity. Cosmetic metadata never grants this identity.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Reserved location for the default Supervisor. The ID alone grants no authority.
pub const DEFAULT_SUPERVISOR_SESSION_ID: &str = "bamboo-default-supervisor";

/// A snapshot cannot be committed against the durable Session authority.
/// Persistence publishers must not cache a rejected identity, including when
/// they normally publish runtime state after an unrelated I/O failure.
#[derive(Debug, thiserror::Error)]
#[error("Session authority conflict: {0}")]
pub struct SessionAuthorityConflict(pub String);

/// Persisted authority assigned only through the trusted bootstrap storage port.
/// Ordinary constructors, copies and children must not inherit Supervisor identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionAuthorityIdentity {
    #[default]
    Ordinary,
    Supervisor {
        incarnation_id: Uuid,
    },
}

impl SessionAuthorityIdentity {
    pub fn is_ordinary(&self) -> bool {
        matches!(self, Self::Ordinary)
    }
}

/// Small bootstrap result, deliberately excluding Session history/control-plane
/// snapshots so callers cannot install a history-free snapshot in a session cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorBootstrapReceipt {
    pub session_id: String,
    pub incarnation_id: Uuid,
    pub created: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Session;

    #[test]
    fn legacy_metadata_and_child_constructors_do_not_grant_authority() {
        let mut root = Session::new("root", "model");
        root.metadata
            .insert("authority_identity".into(), "supervisor".into());
        root.metadata.insert("role".into(), "supervisor".into());
        let raw = serde_json::to_value(&root).unwrap();
        assert!(raw.get("authority_identity").is_none());
        let loaded: Session = serde_json::from_value(raw).unwrap();
        assert!(loaded.authority_identity.is_ordinary());

        root.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: Uuid::new_v4(),
        };
        let child = Session::new_child_of("child", &root, "model", "child");
        let nested = Session::new_child_of("nested", &child, "model", "nested");
        assert!(child.authority_identity.is_ordinary());
        assert!(nested.authority_identity.is_ordinary());
        assert_eq!(nested.root_session_id, root.id);
        assert!(Session::new_child("flat", root.id, "model", "flat")
            .authority_identity
            .is_ordinary());
    }

    #[test]
    fn supervisor_identity_round_trips_without_a_separate_role_field() {
        let mut session = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        session.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: Uuid::new_v4(),
        };
        let raw = serde_json::to_value(&session).unwrap();
        assert_eq!(raw["authority_identity"]["kind"], "supervisor");
        let restored: Session = serde_json::from_value(raw).unwrap();
        assert_eq!(restored.authority_identity, session.authority_identity);
        for raw in [
            serde_json::json!({"kind":"unknown"}),
            serde_json::json!({"kind":"supervisor"}),
            serde_json::json!({"kind":"supervisor","incarnation_id":"invalid"}),
        ] {
            assert!(serde_json::from_value::<SessionAuthorityIdentity>(raw).is_err());
        }
    }
}
