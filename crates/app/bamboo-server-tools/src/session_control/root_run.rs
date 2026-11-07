//! Optional Host port for observing and requesting cancellation of one Root run.
//!
//! A caller/target observation is a selector, not authority. Implementations
//! must retain canonical Supervisor and target-lifetime guards through the
//! exact-owner signal. A signal is not proof of final termination.

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{
    AdmissionGate, SessionCallerReference, SessionInboxReceipt, SupervisorCurrentUserTarget,
};

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedOutcomeTarget {
    pub target_ref: SupervisorCurrentUserTarget,
    pub expected_run_id: String,
    pub after_sequence: u64,
    #[serde(default)]
    pub after_blocker_cursor: Option<u64>,
}

#[async_trait]
pub trait SupervisedRootRunPort: Send + Sync {
    async fn observe_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
    ) -> Result<ToolResult, ToolError>;

    async fn observe_admission(
        &self,
        _caller: &SessionCallerReference,
        _target: &SupervisorCurrentUserTarget,
        _receipt: &SessionInboxReceipt,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "managed_root_admission_unavailable".into(),
        ))
    }

    async fn wait_run(
        &self,
        _caller: &SessionCallerReference,
        _target: &SupervisorCurrentUserTarget,
        _expected_run_id: &str,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution("managed_root_wait_unavailable".into()))
    }

    async fn wait_outcomes(
        &self,
        _caller: &SessionCallerReference,
        _targets: &[ManagedOutcomeTarget; 2],
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Execution(
            "managed_root_outcome_wait_unavailable".into(),
        ))
    }

    async fn cancel_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
        expected_run_id: &str,
        gate: AdmissionGate,
    ) -> Result<ToolResult, ToolError>;
}
