//! Host implementation for one observed Supervisor-managed Root run.
//!
//! The storage fence verifies the original caller and exact target before the
//! blocking job retains the Router owner. The runner table is only tried while
//! those fences are held, so no async wait or lock-order inversion is added.

use std::{collections::HashMap, io, sync::Arc};

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{
    AdmissionCommit, AdmissionGate, SessionActivationPort, SessionAuthorityIdentity,
    SessionCallerReference, SupervisorCurrentUserTarget, SupervisorReference,
};
use bamboo_server_tools::session_control::root_run::SupervisedRootRunPort;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use super::{live_retry::LiveRetryBridge, AgentRunner, AgentStatus};

pub(super) struct SupervisedRootRunController {
    pub(super) store: Arc<SessionStoreV2>,
    pub(super) router: Arc<bamboo_engine::SessionActivationRouter>,
    pub(super) runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub(super) live_retry: Arc<LiveRetryBridge>,
}

fn busy() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "Root runner registry is busy")
}

fn conflict(target: &str, run: Option<&str>, reason: &str) -> Value {
    json!({"status":"run_conflict", "accepted":false, "target_session_id":target,
        "run_id":run, "reason":reason, "completion_verified":false})
}

fn finalized(target: &SupervisorCurrentUserTarget, runner: &AgentRunner) -> Value {
    json!({"status":"already_finished", "accepted":false, "target_session_id":target.session_id,
        "run_id":runner.run_id, "completion_verified":false,
        "note":"The exact execution finalizer ran; durable outcome and physical resource cleanup require their own evidence."})
}

fn run_control(
    target: &SupervisorCurrentUserTarget,
    expected: Option<&str>,
    gate: Option<&AdmissionGate>,
    router: &bamboo_engine::SessionActivationRouter,
    runners: &RwLock<HashMap<String, AgentRunner>>,
    live_retry: &LiveRetryBridge,
) -> io::Result<Value> {
    let candidate = {
        let entries = runners.try_read().map_err(|_| busy())?;
        let Some(runner) = entries.get(&target.session_id) else {
            return Ok(conflict(&target.session_id, expected, "no_current_run"));
        };
        if expected.is_some_and(|run| runner.run_id != run) {
            return Ok(conflict(&target.session_id, expected, "run_changed"));
        }
        if runner.exact_execution_finalized() {
            return Ok(if runner.supervisor_run_is_finalized_for(target) {
                finalized(target, runner)
            } else {
                conflict(&target.session_id, expected, "finalized_unobserved")
            });
        }
        runner.run_id.clone()
    };

    // The router owns the per-target publication fence and checks birth, exact
    // run and non-finalizing registered ownership. It is never a model grant.
    let owner = match router.retain_expected_run_blocking(
        &target.session_id,
        target.created_at,
        &candidate,
    ) {
        Ok(owner) => owner,
        Err(reason) => {
            return Ok(conflict(
                &target.session_id,
                expected,
                &format!("owner_{:?}", reason.reason),
            ));
        }
    };
    if !owner.matches(&target.session_id, target.created_at, &candidate) {
        return Ok(conflict(&target.session_id, expected, "owner_changed"));
    }
    // A reservation/finalizer can hold the registry while waiting for its
    // router fence. Never block on it under that fence; a retry is safe.
    let mut entries = runners.try_write().map_err(|_| busy())?;
    let Some(runner) = entries.get_mut(&target.session_id) else {
        return Ok(conflict(&target.session_id, expected, "run_removed"));
    };
    if runner.run_id != candidate {
        return Ok(conflict(&target.session_id, expected, "run_changed"));
    }
    // A finalizer can win after the candidate read but before this registry
    // acquisition. Never signal or relabel a finalized, unobserved owner.
    if runner.exact_execution_finalized() {
        return Ok(if runner.supervisor_run_is_finalized_for(target) {
            finalized(target, runner)
        } else {
            conflict(&target.session_id, expected, "finalized_unobserved")
        });
    }
    if !matches!(runner.status, AgentStatus::Pending | AgentStatus::Running)
        && !runner.cancel_token.is_cancelled()
    {
        return Ok(conflict(
            &target.session_id,
            expected,
            "terminal_unconfirmed",
        ));
    }
    if expected.is_some() {
        // This is the existing exact Retry/runner cancellation signal. Do not
        // set a terminal status or discard any pending Inbox input here.
        let gate = gate.ok_or_else(|| io::Error::other("missing Root cancel admission"))?;
        match gate.commit(|| {
            runner.bind_supervisor_run_target(target);
            live_retry.cancel_and_token(&target.session_id, &candidate, &runner.cancel_token);
            Ok::<(), io::Error>(())
        })? {
            AdmissionCommit::Committed(()) => {}
            AdmissionCommit::Cancelled => {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "Root cancel caller dropped before signal",
                ));
            }
            AdmissionCommit::AlreadyCommitted => {
                return Err(io::Error::other(
                    "Root cancel admission was already consumed",
                ));
            }
        }
        Ok(
            json!({"status":"cancel_requested", "accepted":true, "target_session_id":target.session_id,
            "run_id":candidate, "completion_verified":false}),
        )
    } else {
        runner.bind_supervisor_run_target(target);
        let status = if runner.cancel_token.is_cancelled() {
            "cancel_requested"
        } else {
            "running"
        };
        Ok(
            json!({"status":status, "target_session_id":target.session_id,
            "run_id":candidate, "completion_verified":false}),
        )
    }
}

impl SupervisedRootRunController {
    async fn control(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
        expected: Option<&str>,
        gate: Option<AdmissionGate>,
    ) -> Result<ToolResult, ToolError> {
        let SessionAuthorityIdentity::Supervisor { incarnation_id } = &caller.authority_identity
        else {
            return Err(ToolError::Execution("original Supervisor required".into()));
        };
        let supervisor = SupervisorReference {
            session_id: caller.session_id.clone(),
            incarnation_id: *incarnation_id,
        };
        let selected = target.clone();
        let expected = expected.map(str::to_owned);
        let router = self.router.clone();
        let runners = self.runners.clone();
        let live_retry = self.live_retry.clone();
        let output = self
            .store
            .with_unscoped_supervisor_root_control(caller, &supervisor, target, move || {
                run_control(
                    &selected,
                    expected.as_deref(),
                    gate.as_ref(),
                    &router,
                    &runners,
                    &live_retry,
                )
            })
            .await
            .map_err(|error| ToolError::Execution(format!("Root run control failed: {error}")))?;
        let success = output["status"] != "run_conflict";
        Ok(ToolResult::text(success, output.to_string()))
    }
}

#[async_trait]
impl SupervisedRootRunPort for SupervisedRootRunController {
    async fn observe_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
    ) -> Result<ToolResult, ToolError> {
        self.control(caller, target, None, None).await
    }

    async fn cancel_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
        expected_run_id: &str,
        gate: AdmissionGate,
    ) -> Result<ToolResult, ToolError> {
        self.control(caller, target, Some(expected_run_id), Some(gate))
            .await
    }
}
