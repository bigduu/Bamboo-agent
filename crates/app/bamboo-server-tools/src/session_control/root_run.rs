//! Optional Host port for observing and requesting cancellation of one Root run.
//!
//! A caller/target observation is a selector, not authority. Implementations
//! must retain canonical Supervisor and target-lifetime guards through the
//! exact-owner signal. A signal is not proof of final termination.

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{AdmissionGate, SessionCallerReference, SupervisorCurrentUserTarget};

#[async_trait]
pub trait SupervisedRootRunPort: Send + Sync {
    async fn observe_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
    ) -> Result<ToolResult, ToolError>;

    async fn cancel_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
        expected_run_id: &str,
        gate: AdmissionGate,
    ) -> Result<ToolResult, ToolError>;
}
