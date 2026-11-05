//! Host implementation for one observed Supervisor-managed Root run.
//!
//! The storage fence verifies the original caller and exact target before the
//! blocking job retains the Router owner. The runner table is only tried while
//! those fences are held, so no async wait or lock-order inversion is added.

use std::{collections::HashMap, io, sync::Arc};

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{
    AdmissionCommit, AdmissionGate, ObserverRootOutcomeRead, SessionActivationPort,
    SessionAuthorityIdentity, SessionCallerReference, SupervisorCurrentUserTarget,
    SupervisorReference,
};
use bamboo_server_tools::session_control::root_run::{ManagedOutcomeTarget, SupervisedRootRunPort};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use super::{fresh_sync::FreshSyncRegistry, live_retry::LiveRetryBridge, AgentRunner, AgentStatus};

pub(super) struct SupervisedRootRunController {
    pub(super) store: Arc<SessionStoreV2>,
    pub(super) inbox: Arc<FileSessionInbox>,
    pub(super) router: Arc<bamboo_engine::SessionActivationRouter>,
    pub(super) runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub(super) live_retry: Arc<LiveRetryBridge>,
    pub(super) fresh_sync: Arc<FreshSyncRegistry>,
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
    fresh_sync: &FreshSyncRegistry,
    workspace_path: Option<&str>,
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
            return Ok(
                if runner.supervisor_run_is_finalized_for(target, workspace_path) {
                    finalized(target, runner)
                } else {
                    conflict(&target.session_id, expected, "finalized_unobserved")
                },
            );
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
        Ok(owner) => Some(owner),
        Err(reason)
            if matches!(
                reason.reason,
                bamboo_domain::SessionRunConflictReason::UnboundLifetime
            ) =>
        {
            None
        }
        Err(reason) => {
            return Ok(conflict(
                &target.session_id,
                expected,
                &format!("owner_{:?}", reason.reason),
            ));
        }
    };
    if owner
        .as_ref()
        .is_some_and(|owner| !owner.matches(&target.session_id, target.created_at, &candidate))
    {
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
        return Ok(
            if runner.supervisor_run_is_finalized_for(target, workspace_path) {
                finalized(target, runner)
            } else {
                conflict(&target.session_id, expected, "finalized_unobserved")
            },
        );
    }
    // Fresh-input sync retains a real Root reservation before its generic
    // Router lifetime is bound. Accept only that exact private reservation,
    // under the runner lock and canonical storage fence; never manufacture a
    // generic Router delivery or steer capability from this observation.
    if owner.is_none()
        && (!(matches!(runner.status, AgentStatus::Pending)
            || (matches!(runner.status, AgentStatus::Cancelled)
                && runner.cancel_token.is_cancelled()))
            || !fresh_sync.matches_control_owner(target, &candidate, workspace_path))
    {
        return Ok(conflict(
            &target.session_id,
            expected,
            "pending_owner_changed",
        ));
    }
    match runner.supervisor_run_observation_matches(target, workspace_path) {
        Some(false) => {
            return Ok(conflict(
                &target.session_id,
                expected,
                "observed_scope_changed",
            ));
        }
        None if owner.is_some()
            && (!matches!(runner.status, AgentStatus::Running)
                || runner.cancel_token.is_cancelled()) =>
        {
            // A bound owner which was never observed cannot acquire a new
            // scope after Stop has exposed it as non-running. FreshSync has
            // its own original scope proof and is handled above instead.
            return Ok(conflict(
                &target.session_id,
                expected,
                "unobserved_nonrunning_owner",
            ));
        }
        _ => {}
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
    if expected.is_some()
        && gate.is_none()
        && runner.supervisor_run_observation_matches(target, workspace_path) != Some(true)
    {
        return Ok(conflict(&target.session_id, expected, "run_not_observed"));
    }
    if let Some(gate) = gate {
        // This is the existing exact Retry/runner cancellation signal. Do not
        // set a terminal status or discard any pending Inbox input here.
        match gate.commit(|| {
            runner.bind_supervisor_run_target(target, workspace_path);
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
        runner.bind_supervisor_run_target(target, workspace_path);
        let status = if runner.cancel_token.is_cancelled() {
            "cancel_requested"
        } else if matches!(runner.status, AgentStatus::Pending) {
            "pending"
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
    async fn read_outcome(
        &self,
        caller: &SessionCallerReference,
        supervisor: &SupervisorReference,
        target: &ManagedOutcomeTarget,
    ) -> Result<ObserverRootOutcomeRead, ToolError> {
        self.inbox
            .read_managed_root_outcome(
                caller,
                supervisor,
                &target.target_ref,
                &target.expected_run_id,
                target.after_sequence,
            )
            .await
            .map_err(|error| {
                tracing::debug!(%error, "Managed Root outcome evidence unavailable");
                ToolError::Execution("managed_root_outcome_evidence_unavailable".into())
            })
    }

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
        let fresh_sync = self.fresh_sync.clone();
        let output = self
            .store
            .with_unscoped_supervisor_root_control(caller, &supervisor, target, move |workspace| {
                run_control(
                    &selected,
                    expected.as_deref(),
                    gate.as_ref(),
                    &router,
                    &runners,
                    &live_retry,
                    &fresh_sync,
                    workspace,
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

    async fn wait_run(
        &self,
        caller: &SessionCallerReference,
        target: &SupervisorCurrentUserTarget,
        expected_run_id: &str,
    ) -> Result<ToolResult, ToolError> {
        let expected = Some(expected_run_id);
        let initial = self.control(caller, target, expected, None).await?;
        let initial_status: Value = serde_json::from_str(&initial.result)
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        if !initial.success || initial_status["status"] == "already_finished" {
            return Ok(initial);
        }

        let signal = self
            .runners
            .read()
            .await
            .get(&target.session_id)
            .filter(|runner| runner.run_id == expected_run_id)
            .map(AgentRunner::wait_for_exact_finalization);
        if let Some(signal) = signal {
            bamboo_orchestration::managed_session::wait_for_finalization(signal).await;
        }

        // The signal alone cannot authorize a read or name an outcome. Recheck
        // the canonical target and the exact run, even on timeout or replacement.
        let final_observation = self.control(caller, target, expected, None).await?;
        let final_status: Value = serde_json::from_str(&final_observation.result)
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        if !final_observation.success || final_status["status"] == "already_finished" {
            return Ok(final_observation);
        }

        Ok(ToolResult::text(
            true,
            json!({
                "status": "wait_timeout",
                "target_session_id": target.session_id,
                "run_id": expected_run_id,
                "completion_verified": false
            })
            .to_string(),
        ))
    }

    async fn wait_outcomes(
        &self,
        caller: &SessionCallerReference,
        targets: &[ManagedOutcomeTarget; 2],
    ) -> Result<ToolResult, ToolError> {
        let SessionAuthorityIdentity::Supervisor { incarnation_id } = &caller.authority_identity
        else {
            return Err(ToolError::Execution("original Supervisor required".into()));
        };
        let supervisor = SupervisorReference {
            session_id: caller.session_id.clone(),
            incarnation_id: *incarnation_id,
        };
        // Subscribe before inspecting Main to cover completion during the read.
        // A signal is only a hint. The read revalidates the exact authority and ACK.
        let [first_signal, second_signal] = {
            let runners = self.runners.read().await;
            targets.each_ref().map(|target| {
                runners
                    .get(&target.target_ref.session_id)
                    .filter(|runner| runner.run_id == target.expected_run_id)
                    .map(AgentRunner::wait_for_exact_finalization)
            })
        };
        let [first, second] = targets;
        use bamboo_orchestration::managed_session::{read_when_verified, wait_for_outcome};
        let outcome = wait_for_outcome(
            read_when_verified(
                || self.read_outcome(caller, &supervisor, first),
                first_signal,
            ),
            read_when_verified(
                || self.read_outcome(caller, &supervisor, second),
                second_signal,
            ),
        )
        .await?;
        let response = match outcome {
            Some(outcome) => json!({"status":"recorded_outcome", "outcome":outcome}),
            None => json!({"status":"outcome_unknown"}),
        };
        Ok(ToolResult::text(true, response.to_string()))
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
