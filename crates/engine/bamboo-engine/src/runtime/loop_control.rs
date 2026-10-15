//! Host-authorized, live self-control over the existing counted runner (#1991).
//! No target lookup, timer, restart manager or execution authority lives here.

use std::sync::{Arc, OnceLock};

use bamboo_agent_core::{tools::ToolResult, AgentError, Message, Session};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

pub use bamboo_tools::tools::loop_control::LoopControlRequest;

use super::config::AgentLoopConfig;
use super::counted_loop::{
    self, CountedLoopConfig, LoopState, LoopStatus, MAX_LOOP_ITERATIONS, MAX_LOOP_PROMPT_BYTES,
    MAX_LOOP_ROUNDS,
};

pub(crate) mod output_limits;
pub(crate) mod suspension;

const MAX_OPERATIONS: usize = 64;

/// Explicit host ceilings for a model-managed loop in one existing activation.
/// Opting in does not start a loop. Clones share only the host's sticky stop;
/// consuming a real lease creates fresh per-execution activation slots.
#[derive(Clone)]
pub struct LoopControlConfig {
    pub max_iterations: u32,
    pub max_total_rounds: u32,
    stop: CancellationToken,
    owner: CountedLoopConfig,
    active: Arc<OnceLock<ManagedActivation>>,
    execution: Arc<OnceLock<ControlExecution>>,
}

struct ManagedActivation {
    generation: String,
    counted: CountedLoopConfig,
}

struct ControlExecution {
    id: String,
    cancel: CancellationToken,
}

impl std::fmt::Debug for LoopControlConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopControlConfig")
            .field("max_iterations", &self.max_iterations)
            .field("max_total_rounds", &self.max_total_rounds)
            .field("stopped", &self.stop.is_cancelled())
            .field("activated", &self.active.get().is_some())
            .finish_non_exhaustive()
    }
}

impl LoopControlConfig {
    pub fn new(max_iterations: u32, max_total_rounds: u32) -> Self {
        Self {
            max_iterations,
            max_total_rounds,
            stop: CancellationToken::new(),
            owner: CountedLoopConfig::new(max_iterations, max_total_rounds),
            active: Arc::new(OnceLock::new()),
            execution: Arc::new(OnceLock::new()),
        }
    }

    /// Revoke this host capability and stop any loop using its child token.
    /// A model stop never cancels this parent or another host request.
    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub(crate) fn bind_owner(
        &mut self,
        ownership: crate::session_activation::SessionRunOwnership,
        session: &Session,
    ) {
        self.owner = CountedLoopConfig::new(self.max_iterations, self.max_total_rounds);
        self.owner.bind_owner(ownership, session);
        self.active = Arc::new(OnceLock::new());
        self.execution = Arc::new(OnceLock::new());
    }

    pub(crate) async fn validate_current_owner(&self, session: &Session) -> Result<(), AgentError> {
        if !(1..=MAX_LOOP_ITERATIONS).contains(&self.max_iterations)
            || !(self.max_iterations..=MAX_LOOP_ROUNDS).contains(&self.max_total_rounds)
            || session.kind != bamboo_domain::SessionKind::Root
            || session.parent_session_id.is_some()
            || !session.authority_identity.is_ordinary()
            || session.supervisor_incarnation_id().is_some()
            || session.root_orchestration_only_enabled()
            || !self.owner.has_current_owner(session).await
        {
            return Err(fatal(
                "current ordinary Root owner and finite host ceilings required",
            ));
        }
        Ok(())
    }

    pub(crate) async fn validate_admission(
        &self,
        session: &Session,
        router: Option<&Arc<crate::session_activation::SessionActivationRouter>>,
    ) -> Result<(), AgentError> {
        if !(1..=MAX_LOOP_ITERATIONS).contains(&self.max_iterations)
            || !(self.max_iterations..=MAX_LOOP_ROUNDS).contains(&self.max_total_rounds)
        {
            return Err(fatal("invalid host ceilings"));
        }
        if session.kind != bamboo_domain::SessionKind::Root
            || session.parent_session_id.is_some()
            || !session.authority_identity.is_ordinary()
            || session.supervisor_incarnation_id().is_some()
            || session.root_orchestration_only_enabled()
            || !self.owner.owns_session(router, session).await
        {
            return Err(fatal("current ordinary Root activation owner required"));
        }
        Ok(())
    }

    pub(crate) fn bind_execution(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<(), AgentError> {
        self.execution
            .set(ControlExecution {
                id: id.into(),
                cancel,
            })
            .map_err(|_| fatal("execution capability already consumed"))
    }

    pub(crate) fn execution_cancel(&self) -> Result<&CancellationToken, AgentError> {
        self.execution
            .get()
            .map(|execution| &execution.cancel)
            .ok_or_else(|| fatal("execution identity absent"))
    }

    pub(crate) fn active_counted(&self) -> Option<&CountedLoopConfig> {
        self.active.get().map(|active| &active.counted)
    }

    pub(crate) fn generation(&self) -> Option<&str> {
        self.active.get().map(|active| active.generation.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopControlReceipt {
    pub operation_id: String,
    pub generation: String,
    pub accepted_revision: u64,
    pub applied_revision_at_acceptance: u64,
    pub objective_revision: u64,
    pub effective_iteration: Option<u32>,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredLoopOperation {
    pub payload_sha256: String,
    pub receipt: LoopControlReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLoopObjective {
    pub objective: String,
    pub objective_revision: u64,
}

/// Schema-two management data inside the existing LoopState checkpoint.
/// State revision advances on every checkpoint; control revision only on a
/// newly accepted command. Receipts are compact and bounded, never a backlog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedLoopState {
    pub control_revision: u64,
    pub applied_revision: u64,
    pub iteration_objective_revision: u64,
    pub stop_requested: bool,
    pub pending_objective: Option<PendingLoopObjective>,
    pub operations: Vec<StoredLoopOperation>,
}

impl ManagedLoopState {
    pub(crate) fn validate(&self, state: &LoopState) -> Result<(), AgentError> {
        if self.control_revision == 0
            || self.control_revision == u64::MAX
            || self.applied_revision > self.control_revision
            || self.iteration_objective_revision > state.objective_revision
            || (self.iteration_objective_revision == 0) != (state.iterations_started == 0)
            || self.operations.is_empty()
            || self.operations.len() > MAX_OPERATIONS
        {
            return Err(fatal("malformed management state"));
        }
        if let Some(pending) = &self.pending_objective {
            if pending.objective.trim().is_empty()
                || pending.objective.len() > MAX_LOOP_PROMPT_BYTES
                || state.objective_revision.checked_add(1) != Some(pending.objective_revision)
                || self.applied_revision >= self.control_revision
                || self.stop_requested
            {
                return Err(fatal("malformed pending objective"));
            }
        } else if state.iterations_started > 0 && self.applied_revision != self.control_revision {
            return Err(fatal("inconsistent applied revision"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for operation in &self.operations {
            let receipt = &operation.receipt;
            if uuid::Uuid::parse_str(&receipt.operation_id).is_err()
                || !seen.insert(&receipt.operation_id)
                || receipt.generation != state.generation
                || receipt.accepted_revision == 0
                || receipt.accepted_revision > self.control_revision
                || receipt.applied_revision_at_acceptance > receipt.accepted_revision
                || operation.payload_sha256.len() != 64
                || !operation
                    .payload_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit())
            {
                return Err(fatal("malformed operation receipt"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LoopStatusSnapshot {
    pub generation: Option<String>,
    pub state_revision: u64,
    pub control_revision: u64,
    pub applied_revision: u64,
    pub objective_revision: u64,
    pub admitted_objective_revision: Option<u64>,
    pub objective: Option<String>,
    pub pending_objective: Option<String>,
    pub status: Option<LoopStatus>,
    pub stop_requested: bool,
    pub iterations_started: u32,
    pub iterations_completed: u32,
    pub rounds_started: u32,
    pub max_iterations: u32,
    pub max_total_rounds: u32,
    pub remaining_iterations: u32,
    pub remaining_rounds: u32,
}

/// Inspect only an already-authorized Session value. This grants no target read,
/// writer, activation or recovery capability.
pub fn inspect_loop(session: &Session) -> Result<LoopStatusSnapshot, AgentError> {
    let state = counted_loop::read_loop_state(session)?;
    let control = state.as_ref().and_then(|state| state.control.as_ref());
    Ok(LoopStatusSnapshot {
        generation: state.as_ref().map(|s| s.generation.clone()),
        state_revision: state.as_ref().map_or(0, |s| s.revision),
        control_revision: control.map_or(0, |c| c.control_revision),
        applied_revision: control.map_or(0, |c| c.applied_revision),
        objective_revision: state.as_ref().map_or(0, |s| s.objective_revision),
        admitted_objective_revision: control.and_then(|c| {
            (c.iteration_objective_revision > 0).then_some(c.iteration_objective_revision)
        }),
        objective: state.as_ref().map(|s| s.objective.clone()),
        pending_objective: control
            .and_then(|c| c.pending_objective.as_ref())
            .map(|p| p.objective.clone()),
        status: state.as_ref().map(|s| s.status),
        stop_requested: control.is_some_and(|c| c.stop_requested),
        iterations_started: state.as_ref().map_or(0, |s| s.iterations_started),
        iterations_completed: state.as_ref().map_or(0, |s| s.iterations_completed),
        rounds_started: state.as_ref().map_or(0, |s| s.rounds_started),
        max_iterations: state.as_ref().map_or(0, |s| s.max_iterations),
        max_total_rounds: state.as_ref().map_or(0, |s| s.max_total_rounds),
        remaining_iterations: state
            .as_ref()
            .map_or(0, |s| s.max_iterations.saturating_sub(s.iterations_started)),
        remaining_rounds: state
            .as_ref()
            .map_or(0, |s| s.max_total_rounds.saturating_sub(s.rounds_started)),
    })
}

fn fatal(reason: &str) -> AgentError {
    AgentError::Budget(format!("loop_control: {reason}"))
}

fn rejected(code: &str) -> ToolResult {
    ToolResult::text(
        false,
        serde_json::json!({"error":code,"applied":false}).to_string(),
    )
}

fn receipt_result(receipt: &LoopControlReceipt) -> ToolResult {
    ToolResult::text(
        true,
        serde_json::to_string(receipt).expect("typed receipt serializes"),
    )
}

fn operation_id(command: &LoopControlRequest) -> Option<&str> {
    match command {
        LoopControlRequest::Status => None,
        LoopControlRequest::Start { operation_id, .. }
        | LoopControlRequest::Update { operation_id, .. }
        | LoopControlRequest::Stop { operation_id, .. } => Some(operation_id),
    }
}

fn fingerprint(command: &LoopControlRequest) -> String {
    let bytes = serde_json::to_vec(command).expect("typed command serializes");
    hex::encode(Sha256::digest(bytes))
}

fn control_revision(state: Option<&LoopState>) -> u64 {
    state
        .and_then(|s| s.control.as_ref())
        .map_or(0, |c| c.control_revision)
}

/// Called only for an exact admitted loop_control identity, after the executor's
/// permission decision, on the existing runner's serial apply path. The tool
/// itself can validate a command but cannot invoke this authority boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_current(
    session: &mut Session,
    config: &AgentLoopConfig,
    command: &LoopControlRequest,
    tool_call_id: &str,
    round: usize,
    approved_replay: bool,
    plan_read_only: bool,
    max_tool_output_tokens: u32,
) -> Result<ToolResult, AgentError> {
    let policy = config
        .loop_control
        .as_ref()
        .ok_or_else(|| fatal("host capability absent"))?;
    policy.validate_current_owner(session).await?;
    let execution = policy
        .execution
        .get()
        .ok_or_else(|| fatal("execution identity absent"))?;
    if execution.cancel.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    if config.persistence.is_none() {
        return Err(fatal("durable checkpoint required"));
    }
    if config.ticket_worker_plan.is_some() || config.goal_loop_active() {
        return Err(fatal("incompatible execution mode"));
    }
    if session.messages.iter().any(|message| {
        message.role == bamboo_domain::Role::Tool
            && message.tool_call_id.as_deref() == Some(tool_call_id)
    }) {
        return Err(fatal("tool result already paired"));
    }
    let current = counted_loop::read_loop_state(session)?;
    let start_refusal = if policy.active.get().is_none() {
        let refusal = suspension::start_refusal(session, config).await;
        // The child/status observation may yield. It grants no stale owner or
        // cancelled execution permission to install a generation afterwards.
        let authority = policy.validate_current_owner(session).await;
        if execution.cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        authority?;
        refusal
    } else {
        None
    };
    let result = if matches!(command, LoopControlRequest::Status) {
        ToolResult::text(true, serde_json::json!({
            "loop":inspect_loop(session)?,
            "host_max_iterations":policy.max_iterations,
            "host_max_total_rounds":policy.max_total_rounds.min(config.run_budget.max_rounds.unwrap_or(MAX_LOOP_ROUNDS)),
            "activation_rounds_spent":round.saturating_add(1),
            "host_remaining_rounds":policy.max_total_rounds.min(config.run_budget.max_rounds.unwrap_or(MAX_LOOP_ROUNDS)).saturating_sub(u32::try_from(round.saturating_add(1)).unwrap_or(u32::MAX)),
            "can_start":policy.active.get().is_none() && !policy.stop.is_cancelled() && start_refusal.is_none(),
        }).to_string())
    } else if plan_read_only {
        rejected("plan_read_only")
    } else if approved_replay {
        rejected("fresh_owner_command_required")
    } else {
        let hash = fingerprint(command);
        let prior = current
            .as_ref()
            .and_then(|s| s.control.as_ref())
            .and_then(|c| {
                c.operations
                    .iter()
                    .find(|o| Some(o.receipt.operation_id.as_str()) == operation_id(command))
            });
        if let Some(prior) = prior {
            // Idempotency precedes CAS only for this live activation's generation.
            // A retired receipt never makes an old command current again.
            if policy.generation() != Some(prior.receipt.generation.as_str())
                || current
                    .as_ref()
                    .is_none_or(|s| s.owner_execution_id != execution.id)
            {
                rejected("generation_not_current_activation")
            } else if prior.payload_sha256 == hash {
                receipt_result(&prior.receipt)
            } else {
                rejected("operation_payload_conflict")
            }
        } else if policy.stop.is_cancelled() && !matches!(command, LoopControlRequest::Stop { .. })
        {
            rejected("host_stop_requested")
        } else if let Some(reason) =
            start_refusal.filter(|_| matches!(command, LoopControlRequest::Start { .. }))
        {
            rejected(reason)
        } else {
            match accept_command(policy, config, current, command, &execution.id, round, hash) {
                Ok(state) => {
                    let result = receipt_result(
                        &state
                            .control
                            .as_ref()
                            .expect("managed state")
                            .operations
                            .last()
                            .expect("accepted operation")
                            .receipt,
                    );
                    if !output_limits::fits(&result, max_tool_output_tokens) {
                        // No command, OnceLock or stop side effect is installed
                        // unless its entire immutable acknowledgement fits.
                        output_limits::budget_failure(max_tool_output_tokens)
                    } else if let Err(code) = install_command_effects(policy, &state, command) {
                        rejected(code)
                    } else {
                        return checkpoint_result(
                            session,
                            config,
                            policy,
                            tool_call_id,
                            result,
                            Some(state),
                            max_tool_output_tokens,
                        )
                        .await;
                    }
                }
                Err(code) => rejected(code),
            }
        }
    };
    checkpoint_result(
        session,
        config,
        policy,
        tool_call_id,
        result,
        None,
        max_tool_output_tokens,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn accept_command(
    policy: &LoopControlConfig,
    config: &AgentLoopConfig,
    current: Option<LoopState>,
    command: &LoopControlRequest,
    execution_id: &str,
    round: usize,
    hash: String,
) -> Result<LoopState, &'static str> {
    if !matches!(command, LoopControlRequest::Start { .. })
        && current
            .as_ref()
            .and_then(|s| s.control.as_ref())
            .is_some_and(|c| c.operations.len() >= MAX_OPERATIONS)
    {
        return Err("operation_limit_exhausted");
    }
    let mut state = match command {
        LoopControlRequest::Start {
            expected_generation,
            expected_revision,
            objective,
            max_iterations,
            max_total_rounds,
            ..
        } => {
            if current.as_ref().map(|s| s.generation.as_str()) != expected_generation.as_deref()
                || control_revision(current.as_ref()) != *expected_revision
            {
                return Err("stale_generation_or_revision");
            }
            if policy.active.get().is_some() {
                return Err("generation_already_started_this_activation");
            }
            if current.as_ref().is_some_and(|s| {
                matches!(
                    s.status,
                    LoopStatus::Running | LoopStatus::Finalizing | LoopStatus::RecoveryUnsupported
                )
            }) {
                return Err("prior_generation_not_retired");
            }
            let spent = u32::try_from(round.saturating_add(1)).map_err(|_| "budget_exhausted")?;
            let ceiling = policy
                .max_total_rounds
                .min(config.run_budget.max_rounds.unwrap_or(MAX_LOOP_ROUNDS));
            if *max_iterations > policy.max_iterations
                || *max_total_rounds > ceiling
                || *max_total_rounds <= spent
                || *max_iterations > max_total_rounds.saturating_sub(spent)
            {
                return Err("host_or_remaining_budget_exceeded");
            }
            let generation = uuid::Uuid::new_v4().to_string();
            LoopState {
                schema_version: 2,
                generation,
                revision: 1,
                objective_revision: 1,
                owner_execution_id: execution_id.into(),
                objective: objective.clone(),
                max_iterations: *max_iterations,
                max_total_rounds: *max_total_rounds,
                iterations_started: 0,
                iterations_completed: 0,
                rounds_started: spent,
                status: LoopStatus::Running,
                control: Some(ManagedLoopState {
                    control_revision: 1,
                    applied_revision: 0,
                    iteration_objective_revision: 0,
                    stop_requested: false,
                    pending_objective: None,
                    operations: Vec::new(),
                }),
            }
        }
        LoopControlRequest::Update {
            generation,
            expected_revision,
            objective,
            max_iterations,
            max_total_rounds,
            ..
        } => {
            let mut state = current.ok_or("loop_absent")?;
            validate_current(policy, &state, execution_id, generation, *expected_revision)?;
            let control = state
                .control
                .as_mut()
                .ok_or("managed_generation_required")?;
            if control.stop_requested {
                return Err("stop_is_terminal");
            }
            if objective.is_some() && control.pending_objective.is_some() {
                return Err("objective_update_pending");
            }
            let iterations = max_iterations.unwrap_or(state.max_iterations);
            let rounds = max_total_rounds.unwrap_or(state.max_total_rounds);
            if iterations > state.max_iterations
                || rounds > state.max_total_rounds
                || iterations < state.iterations_started
                || rounds < state.rounds_started
                || iterations == 0
                || rounds < iterations
            {
                return Err("bounds_may_only_tighten_without_reset");
            }
            if (control.pending_objective.is_some()
                || objective
                    .as_ref()
                    .is_some_and(|text| *text != state.objective))
                && (iterations <= state.iterations_started || rounds <= state.rounds_started)
            {
                return Err("no_budget_for_new_objective");
            }
            control.control_revision = control
                .control_revision
                .checked_add(1)
                .filter(|r| *r < u64::MAX)
                .ok_or("revision_exhausted")?;
            state.max_iterations = iterations;
            state.max_total_rounds = rounds;
            if let Some(objective) = objective.as_ref().filter(|text| **text != state.objective) {
                control.pending_objective = Some(PendingLoopObjective {
                    objective: objective.clone(),
                    objective_revision: state
                        .objective_revision
                        .checked_add(1)
                        .filter(|r| *r < u64::MAX)
                        .ok_or("revision_exhausted")?,
                });
            } else if control.pending_objective.is_none() && state.iterations_started > 0 {
                control.applied_revision = control.control_revision;
            }
            state
        }
        LoopControlRequest::Stop {
            generation,
            expected_revision,
            ..
        } => {
            let mut state = current.ok_or("loop_absent")?;
            validate_current(policy, &state, execution_id, generation, *expected_revision)?;
            let control = state
                .control
                .as_mut()
                .ok_or("managed_generation_required")?;
            if !control.stop_requested {
                control.control_revision = control
                    .control_revision
                    .checked_add(1)
                    .filter(|r| *r < u64::MAX)
                    .ok_or("revision_exhausted")?;
            }
            control.stop_requested = true;
            control.pending_objective = None;
            control.applied_revision = control.control_revision;
            state
        }
        LoopControlRequest::Status => unreachable!("status is not a mutation"),
    };
    let control = state.control.as_mut().expect("managed state");
    if control.operations.len() >= MAX_OPERATIONS {
        return Err("operation_limit_exhausted");
    }
    let pending = control.pending_objective.as_ref();
    let applied = control.applied_revision == control.control_revision;
    control.operations.push(StoredLoopOperation {
        payload_sha256: hash,
        receipt: LoopControlReceipt {
            operation_id: operation_id(command).expect("mutation id").into(),
            generation: state.generation.clone(),
            accepted_revision: control.control_revision,
            applied_revision_at_acceptance: control.applied_revision,
            objective_revision: pending.map_or(state.objective_revision, |p| p.objective_revision),
            effective_iteration: (!applied).then_some(state.iterations_started.saturating_add(1)),
            applied,
        },
    });
    // A valid text-byte bound can expand under JSON escaping. Check the exact
    // persisted representation before any OnceLock or cancellation side effect.
    counted_loop::encode_next_state(&state).map_err(|_| "state_checkpoint_bound_exceeded")?;
    Ok(state)
}

fn install_command_effects(
    policy: &LoopControlConfig,
    state: &LoopState,
    command: &LoopControlRequest,
) -> Result<(), &'static str> {
    if matches!(command, LoopControlRequest::Start { .. }) {
        let mut counted = policy.owner.clone();
        counted.max_iterations = state.max_iterations;
        counted.max_total_rounds = state.max_total_rounds;
        counted.replace_stop(policy.stop.child_token());
        policy
            .active
            .set(ManagedActivation {
                generation: state.generation.clone(),
                counted,
            })
            .map_err(|_| "generation_already_started_this_activation")?;
    } else if matches!(command, LoopControlRequest::Stop { .. }) {
        policy
            .active_counted()
            .ok_or("managed_generation_required")?
            .stop();
    }
    Ok(())
}

fn validate_current(
    policy: &LoopControlConfig,
    state: &LoopState,
    execution_id: &str,
    generation: &str,
    expected_revision: u64,
) -> Result<(), &'static str> {
    if state.generation != generation || control_revision(Some(state)) != expected_revision {
        return Err("stale_generation_or_revision");
    }
    if policy.generation() != Some(generation) || state.owner_execution_id != execution_id {
        return Err("generation_not_current_activation");
    }
    if state.status != LoopStatus::Running {
        return Err("generation_not_running");
    }
    Ok(())
}

async fn checkpoint_result(
    session: &mut Session,
    config: &AgentLoopConfig,
    policy: &LoopControlConfig,
    tool_call_id: &str,
    result: ToolResult,
    state: Option<LoopState>,
    max_tool_output_tokens: u32,
) -> Result<ToolResult, AgentError> {
    let result = output_limits::bound_result(result, max_tool_output_tokens);
    let mut staged = session.clone();
    if let Some(state) = state {
        counted_loop::stage_state(&mut staged, state)?;
    }
    staged.add_message(Message::tool_result_with_status(
        tool_call_id,
        result.result.clone(),
        result.success,
    ));
    persist_staged(session, config, policy, staged).await?;
    Ok(result)
}

/// Keep the first-checkpoint pair immutable. Native post-tool feedback uses
/// the existing volatile hook-context plane; the append-safe checkpoint cannot
/// rewrite a durable ToolResult and no history-rewrite authority is granted.
pub(crate) async fn finish_checkpointed_result(
    session: &mut Session,
    config: &AgentLoopConfig,
    tool_call_id: &str,
    result: &ToolResult,
    runtime_state: &mut bamboo_domain::AgentRuntimeState,
    feedback_context: Option<String>,
) -> Result<(), AgentError> {
    let policy = config
        .loop_control
        .as_ref()
        .ok_or_else(|| fatal("host capability absent"))?;
    let authority = policy.validate_current_owner(session).await;
    if policy.execution_cancel()?.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    authority?;
    validate_checkpointed_pair(session, tool_call_id, result)?;
    if let Some(context) = feedback_context {
        let expected_index = runtime_state.hook_contexts.len();
        let mut next_runtime = runtime_state.clone();
        next_runtime.hook_contexts.push(context.clone());
        let mut staged = session.clone();
        crate::runtime::runner::state_bridge::write_runtime_state(&mut staged, &next_runtime);
        let checkpoint = persist_staged(session, config, policy, staged).await;
        // Preserve the same conservative context in the loop's local state as
        // in its staged Session even if the checkpoint outcome was uncertain.
        if let Some(saved_runtime) =
            crate::runtime::runner::state_bridge::read_runtime_state(session)
        {
            *runtime_state = saved_runtime;
        }
        checkpoint?;
        if runtime_state.hook_contexts.len() != expected_index + 1
            || runtime_state.hook_contexts.get(expected_index) != Some(&context)
        {
            return Err(fatal("hook feedback checkpoint did not retain context"));
        }
        validate_checkpointed_pair(session, tool_call_id, result)?;
    }
    // Includes the no-feedback path: cancellation during AfterToolUse wins.
    if policy.execution_cancel()?.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    Ok(())
}

fn validate_checkpointed_pair(
    session: &Session,
    tool_call_id: &str,
    result: &ToolResult,
) -> Result<(), AgentError> {
    let mut pairs = session.messages.iter().filter(|message| {
        message.role == bamboo_domain::Role::Tool
            && message.tool_call_id.as_deref() == Some(tool_call_id)
    });
    let pair = pairs
        .next()
        .ok_or_else(|| fatal("checkpointed tool result absent"))?;
    if pairs.next().is_some()
        || pair.content != result.result
        || pair.tool_success != Some(result.success)
    {
        return Err(fatal("checkpointed tool result identity changed"));
    }
    Ok(())
}

async fn persist_staged(
    session: &mut Session,
    config: &AgentLoopConfig,
    policy: &LoopControlConfig,
    mut staged: Session,
) -> Result<(), AgentError> {
    let checkpoint =
        counted_loop::persist_checkpoint(&mut staged, config.persistence.as_deref()).await;
    // An uncertain write reserves this operation/consumption conservatively.
    // The original error aborts the runner; its one known generation is Failed
    // (or Cancelled), never replayed or silently started a second time.
    *session = staged;
    if policy
        .execution
        .get()
        .is_some_and(|e| e.cancel.is_cancelled())
    {
        if let Err(error) = &checkpoint {
            tracing::warn!(%error, "loop control checkpoint failed during user cancellation");
        }
        return Err(AgentError::Cancelled);
    }
    checkpoint
}

#[cfg(test)]
mod tests;
