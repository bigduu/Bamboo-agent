//! Bounded host-managed relationships, separate from immutable Session identity.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ProjectId, SupervisorBootstrapReceipt, DEFAULT_SUPERVISOR_SESSION_ID};

pub const SUPERVISOR_MANAGEMENT_SCHEMA_VERSION: u32 = 1;
pub const SUPERVISOR_OBSERVER_SCHEMA_VERSION: u32 = 2;
pub const MAX_SUPERVISOR_OBSERVERS: usize = 8;
pub const MAX_SUPERVISOR_OBSERVER_TARGETS: usize = 8;
pub const MAX_SUPERVISOR_PROJECTS: usize = 64;
/// Includes disabled tombstones. Entries are never evicted or implicitly revived.
pub const MAX_SUPERVISOR_LINKS: usize = 256;
pub const MAX_SUPERVISOR_SESSION_ID_BYTES: usize = 256;

/// A host reference is checked against canonical identity on every operation.
/// Possession of these values is not a model-callable capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorReference {
    pub session_id: String,
    pub incarnation_id: Uuid,
}

impl From<&SupervisorBootstrapReceipt> for SupervisorReference {
    fn from(receipt: &SupervisorBootstrapReceipt) -> Self {
        Self {
            session_id: receipt.session_id.clone(),
            incarnation_id: receipt.incarnation_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorManagedLink {
    pub revision: u64,
    pub enabled: bool,
    pub target_created_at: DateTime<Utc>,
    pub target_project_id: ProjectId,
    pub target_metadata_version: u64,
}

/// An explicit Host registration, not a role label or a transferable grant.
/// Revocation preserves the exact Child's background classification forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorObserverRegistration {
    pub operation_id: Uuid,
    pub revision: u64,
    pub enabled: bool,
    pub parent_created_at: DateTime<Utc>,
    pub child_created_at: DateTime<Utc>,
    pub target_links: BTreeMap<String, SupervisorManagedLink>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorObserverReceipt {
    pub supervisor: SupervisorReference,
    pub child_session_id: String,
    pub operation_id: Uuid,
    pub state_revision: u64,
    pub registration_revision: u64,
    pub enabled: bool,
    pub changed: bool,
}

/// Only the canonical reader creates this observation. It expires with its
/// retained physical read; serialized values are selectors, never authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorObserverContext {
    pub supervisor: SupervisorReference,
    pub parent_created_at: DateTime<Utc>,
    pub child_created_at: DateTime<Utc>,
    pub operation_id: Uuid,
    pub registration_revision: u64,
    pub target_link: SupervisorManagedLink,
}

/// Only the canonical Supervisor runtime may publish this state. A missing
/// field means empty scope, no links and revision zero for legacy Supervisors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorManagementState {
    pub schema_version: u32,
    pub incarnation_id: Uuid,
    pub revision: u64,
    pub allowed_projects: BTreeSet<ProjectId>,
    pub links: BTreeMap<String, SupervisorManagedLink>,
    /// Empty v1 records serialize identically, preserving their existing proof.
    /// Any nonempty registration upgrades to v2, which old writers reject.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub observers: BTreeMap<String, SupervisorObserverRegistration>,
}

impl SupervisorManagementState {
    /// Validate before authority use or publication, including typed values
    /// deserialized from a future or externally damaged record.
    pub fn validate(&self, incarnation_id: Uuid) -> Result<(), &'static str> {
        if !matches!(
            self.schema_version,
            SUPERVISOR_MANAGEMENT_SCHEMA_VERSION | SUPERVISOR_OBSERVER_SCHEMA_VERSION
        ) || (self.schema_version == SUPERVISOR_MANAGEMENT_SCHEMA_VERSION
            && !self.observers.is_empty())
        {
            return Err("unsupported Supervisor management schema version");
        }
        if incarnation_id.is_nil() || self.incarnation_id != incarnation_id || self.revision == 0 {
            return Err("invalid Supervisor management incarnation or revision");
        }
        if self.allowed_projects.len() > MAX_SUPERVISOR_PROJECTS
            || self.links.len() > MAX_SUPERVISOR_LINKS
            || self.observers.len() > MAX_SUPERVISOR_OBSERVERS
        {
            return Err("Supervisor management capacity exceeded");
        }
        for (id, link) in &self.links {
            validate_supervisor_target_id(id)?;
            if id == DEFAULT_SUPERVISOR_SESSION_ID {
                return Err("the Supervisor cannot hold a self-management link");
            }
            if link.revision == 0 || link.revision > self.revision {
                return Err("invalid Supervisor link revision");
            }
            if link.enabled && !self.allowed_projects.contains(&link.target_project_id) {
                return Err("enabled Supervisor link is outside Project scope");
            }
        }
        let mut operations = BTreeSet::new();
        for (id, observer) in &self.observers {
            validate_supervisor_target_id(id)?;
            if id == DEFAULT_SUPERVISOR_SESSION_ID
                || observer.operation_id.is_nil()
                || !operations.insert(observer.operation_id)
                || observer.revision == 0
                || observer.revision > self.revision
                || observer.target_links.is_empty()
                || observer.target_links.len() > MAX_SUPERVISOR_OBSERVER_TARGETS
            {
                return Err("invalid Supervisor Observer registration");
            }
            for (target, link) in &observer.target_links {
                validate_supervisor_target_id(target)?;
                if target == id || !link.enabled || link.revision == 0 {
                    return Err("invalid Supervisor Observer target");
                }
                if observer.enabled && self.links.get(target) != Some(link) {
                    return Err("enabled Supervisor Observer has a stale target link");
                }
            }
        }
        // Leave room for the enclosing canonical proof within its 256 KiB cap.
        if self.schema_version == SUPERVISOR_OBSERVER_SCHEMA_VERSION
            && serde_json::to_vec_pretty(self)
                .map_err(|_| "invalid Supervisor Observer encoding")?
                .len()
                > 224 * 1024
        {
            return Err("Supervisor Observer state exceeds its canonical byte budget");
        }
        Ok(())
    }

    /// Historical classification is distinct from current observation access.
    /// Disabled tombstones never exempt a replacement Child or parent lifetime.
    pub fn classifies_observer(&self, parent: &crate::Session, child: &crate::Session) -> bool {
        self.validate(self.incarnation_id).is_ok()
            && parent.authority_identity
                == (crate::SessionAuthorityIdentity::Supervisor {
                    incarnation_id: self.incarnation_id,
                })
            && parent.id == DEFAULT_SUPERVISOR_SESSION_ID
            && parent.kind == crate::SessionKind::Root
            && child.kind == crate::SessionKind::Child
            && child.authority_identity.is_ordinary()
            && child.parent_session_id.as_deref() == Some(parent.id.as_str())
            && child.root_session_id == parent.id
            && child.parent_created_at == Some(parent.created_at)
            && self.observers.get(&child.id).is_some_and(|observer| {
                observer.parent_created_at == parent.created_at
                    && observer.child_created_at == child.created_at
            })
    }
}

/// The existing path-safe Session ID rules with an additional byte bound.
pub fn validate_supervisor_target_id(id: &str) -> Result<(), &'static str> {
    if id.is_empty()
        || id.len() > MAX_SUPERVISOR_SESSION_ID_BYTES
        || id == "."
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.chars().any(char::is_control)
    {
        return Err("invalid or oversized Supervisor target Session ID");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SupervisorManagementMutation {
    ConfigureProjectScope {
        allowed_projects: BTreeSet<ProjectId>,
    },
    Attach {
        target_session_id: String,
    },
    Detach {
        target_session_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorManagementRequest {
    pub supervisor: SupervisorReference,
    pub expected_state_revision: u64,
    pub mutation: SupervisorManagementMutation,
}

/// Receipts and observations intentionally exclude Session snapshots/history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorManagementReceipt {
    pub supervisor: SupervisorReference,
    pub state_revision: u64,
    pub changed: bool,
    pub link_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorScopeObservation {
    pub supervisor: SupervisorReference,
    pub state_revision: u64,
    pub allowed_projects: BTreeSet<ProjectId>,
}

/// A strict observation only while storage holds its locks. The returned value
/// is not authorization for a later inbox write or any other command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorLinkObservation {
    pub supervisor: SupervisorReference,
    pub state_revision: u64,
    pub target_session_id: String,
    pub link: Option<SupervisorManagedLink>,
    pub authorized: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Session, SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};

    #[test]
    fn supervisor_management_public_serde_is_legacy_compatible_and_children_drop_state() {
        let mut root = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let legacy = serde_json::to_value(&root).unwrap();
        assert!(legacy.get("supervisor_management").is_none());
        assert!(serde_json::from_value::<Session>(legacy)
            .unwrap()
            .supervisor_management
            .is_none());
        let incarnation_id = Uuid::new_v4();
        root.authority_identity = SessionAuthorityIdentity::Supervisor { incarnation_id };
        let state = SupervisorManagementState {
            schema_version: 1,
            incarnation_id,
            revision: 1,
            allowed_projects: BTreeSet::from(["project-a".parse().unwrap()]),
            links: BTreeMap::new(),
            observers: BTreeMap::new(),
        };
        state.validate(incarnation_id).unwrap();
        root.supervisor_management = Some(state);
        let encoded = serde_json::to_value(&root).unwrap();
        let decoded: Session = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.supervisor_management, root.supervisor_management);
        for child in [
            Session::new_child_of("child", &root, "model", "child"),
            Session::new_child("flat", root.id.clone(), "model", "flat"),
        ] {
            assert!(child.authority_identity.is_ordinary());
            assert!(child.supervisor_management.is_none());
        }
        let reference = SupervisorReference {
            session_id: root.id,
            incarnation_id,
        };
        let request = SupervisorManagementRequest {
            supervisor: reference,
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::Attach {
                target_session_id: "target-a".into(),
            },
        };
        assert_eq!(
            serde_json::from_value::<SupervisorManagementRequest>(
                serde_json::to_value(&request).unwrap()
            )
            .unwrap(),
            request
        );
        for invalid in [
            "",
            ".",
            "a/b",
            "a\\b",
            "a..b",
            "\0",
            &"a".repeat(MAX_SUPERVISOR_SESSION_ID_BYTES + 1),
        ] {
            assert!(validate_supervisor_target_id(invalid).is_err());
        }
        assert!(
            validate_supervisor_target_id(&"a".repeat(MAX_SUPERVISOR_SESSION_ID_BYTES)).is_ok()
        );
    }
}
