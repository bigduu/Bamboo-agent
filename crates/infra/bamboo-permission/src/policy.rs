//! Stable, machine-readable permission request and decision contract.

use serde::{Deserialize, Serialize};

use crate::{PermissionMode, PermissionType, RiskLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecisionKind {
    AllowOnce,
    AllowSession,
    AllowWorkspace,
    AllowGlobal,
    DenyOnce,
    DenySession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionReasonCode {
    HardDangerous,
    ConfiguredAlwaysAsk,
    ExplicitDeny,
    RiskThreshold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMatcherKind {
    ExactResource,
    PathSubtree,
    CommandPrefix,
    HttpOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionMatcher {
    /// Opaque identifier echoed by clients when selecting this suggestion.
    pub id: String,
    pub kind: PermissionMatcherKind,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub request_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    pub tool_name: String,
    pub permission_type: PermissionType,
    pub resource: String,
    pub operation_summary: String,
    pub risk_level: RiskLevel,
    pub reason_code: PermissionReasonCode,
    pub effective_mode: PermissionMode,
    pub bypass_requested: bool,
    pub policy_revision: u64,
    pub allowed_decisions: Vec<PermissionDecisionKind>,
    pub suggested_matchers: Vec<PermissionMatcher>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionDecision {
    pub request_id: String,
    pub decision: PermissionDecisionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matcher_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_policy_revision: Option<u64>,
}

impl PermissionRequest {
    pub fn migration_decisions() -> Vec<PermissionDecisionKind> {
        // Phase 1 only wires one-shot legacy-compatible responses. Remembered
        // scopes are deliberately omitted until the typed decision endpoint can
        // validate matcher ids and persist them atomically.
        vec![
            PermissionDecisionKind::AllowOnce,
            PermissionDecisionKind::DenyOnce,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_requests_never_offer_remembered_scopes() {
        assert_eq!(
            PermissionRequest::migration_decisions(),
            vec![
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::DenyOnce
            ]
        );
    }

    #[test]
    fn decision_wire_uses_typed_ids() {
        let value = serde_json::to_value(PermissionDecision {
            request_id: "req-1".into(),
            decision: PermissionDecisionKind::AllowWorkspace,
            matcher_id: Some("path-subtree-1".into()),
            expected_policy_revision: Some(7),
        })
        .expect("serialize");
        assert_eq!(value["decision"], "allow_workspace");
        assert_eq!(value["matcher_id"], "path-subtree-1");
    }
}
