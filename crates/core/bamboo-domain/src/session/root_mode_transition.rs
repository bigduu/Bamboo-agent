//! Terminal, bounded host-owned Root mode operations.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::Session;

pub const ROOT_MODE_OPERATION_HISTORY_LIMIT: usize = 8;

/// Bind the operation identity to its durable CAS epoch. An old operation ID
/// cannot be reused with a newer epoch after its bounded receipt is evicted.
pub fn root_mode_operation_id_matches_epoch(value: &str, expected_epoch: u64) -> bool {
    let Some((epoch, uuid)) = value.split_once(':') else {
        return false;
    };
    epoch == expected_epoch.to_string()
        && uuid::Uuid::parse_str(uuid).is_ok_and(|parsed| parsed.to_string() == uuid)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootModeOperationAction {
    Select,
    Recover,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootModeOperationRequest {
    pub session_id: String,
    pub operation_id: String,
    pub birth_token: String,
    pub expected_epoch: u64,
    pub requested_enabled: bool,
    pub action: RootModeOperationAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootModeOperationDecision {
    Terminal(RootModeOperationReceipt),
    /// A later terminal epoch already prevents this request from committing.
    FencedBySuccessor {
        operation_id: String,
        expected_epoch: u64,
        current_epoch: u64,
        current_enabled: bool,
        current_tool_revision: u64,
    },
    NotFound,
    NotRoot,
    BirthMismatch,
    OperationConflict,
    FutureEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootModeOperationOutcome {
    Committed,
    Fenced,
    RejectedIncompatible,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootModeOperationReceipt {
    pub operation_id: String,
    pub expected_epoch: u64,
    pub resulting_epoch: u64,
    pub requested_enabled: bool,
    pub enabled_at_completion: bool,
    pub tool_authority_revision: u64,
    pub outcome: RootModeOperationOutcome,
}

impl Session {
    /// Opaque lifetime token, so a deleted Root ID cannot reuse an old CAS.
    pub fn root_mode_birth_token(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"bamboo.root-mode-birth.v1\0");
        digest.update(self.id.as_bytes());
        digest.update(b"\0");
        digest.update(self.created_at.to_rfc3339().as_bytes());
        digest.update(b"\0");
        digest.update(
            serde_json::to_vec(&self.authority_identity)
                .expect("Session authority identity is serializable"),
        );
        hex::encode(digest.finalize())
    }

    pub fn root_mode_operation(&self, operation_id: &str) -> Option<&RootModeOperationReceipt> {
        self.root_mode_operations
            .iter()
            .find(|receipt| receipt.operation_id == operation_id)
    }

    pub fn record_root_mode_operation(
        &mut self,
        mut receipt: RootModeOperationReceipt,
    ) -> Result<(), super::RootToolAuthorityError> {
        if self.kind != super::SessionKind::Root || self.parent_session_id.is_some() {
            return Err(super::RootToolAuthorityError::NotRoot);
        }
        let next = self
            .root_mode_transition_epoch
            .checked_add(1)
            .ok_or(super::RootToolAuthorityError::RevisionOverflow)?;
        receipt.expected_epoch = self.root_mode_transition_epoch;
        receipt.resulting_epoch = next;
        receipt.enabled_at_completion = self.root_orchestration_only_enabled();
        receipt.tool_authority_revision = self.root_tool_authority_revision;
        self.root_mode_transition_epoch = next;
        self.root_mode_operations.push(receipt);
        self.updated_at = chrono::Utc::now();
        if self.root_mode_operations.len() > ROOT_MODE_OPERATION_HISTORY_LIMIT {
            self.root_mode_operations.remove(0);
        }
        Ok(())
    }
}
