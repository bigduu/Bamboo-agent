//! Live, bounded repetition inside an existing execution owner (#898).
//!
//! This is an engine primitive, not a scheduler or a management API. Each
//! iteration retains the same Session and provider context. Persisted live
//! state is inspectable but is never automatically replayed by a new activation.

use bamboo_agent_core::{AgentError, Message, Session};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::config::AgentLoopConfig;

pub const LOOP_STATE_METADATA_KEY: &str = "runtime.counted_loop";
pub const MAX_LOOP_ITERATIONS: u32 = 100;
pub const MAX_LOOP_ROUNDS: u32 = 1_000;
pub const MAX_LOOP_PROMPT_BYTES: usize = 16 * 1024;
const MAX_STATE_BYTES: usize = MAX_LOOP_PROMPT_BYTES * 8;

/// Host-supplied bounds for one live generation. There is no implicit default.
/// Clones share the stop signal. Stop drains an already admitted iteration's
/// current round/tool batch; it admits no subsequent round or iteration.
#[derive(Clone)]
pub struct CountedLoopConfig {
    pub max_iterations: u32,
    pub max_total_rounds: u32,
    stop: CancellationToken,
    owner: Option<CountedLoopOwner>,
}

#[derive(Clone)]
struct CountedLoopOwner {
    run: crate::session_activation::SessionRunOwnership,
    session_birth: chrono::DateTime<chrono::Utc>,
}

impl std::fmt::Debug for CountedLoopConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountedLoopConfig")
            .field("max_iterations", &self.max_iterations)
            .field("max_total_rounds", &self.max_total_rounds)
            .field("stopped", &self.stop.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl CountedLoopConfig {
    pub fn new(max_iterations: u32, max_total_rounds: u32) -> Self {
        Self {
            max_iterations,
            max_total_rounds,
            stop: CancellationToken::new(),
            owner: None,
        }
    }

    /// Bind only the request value after consuming its real execution lease.
    /// Earlier stop/config clones remain unbound and convey no ownership.
    pub(crate) fn bind_owner(
        &mut self,
        run: crate::session_activation::SessionRunOwnership,
        session: &Session,
    ) {
        self.owner = Some(CountedLoopOwner {
            run,
            session_birth: session.created_at,
        });
    }

    pub(crate) async fn owns_session(
        &self,
        router: Option<&std::sync::Arc<crate::session_activation::SessionActivationRouter>>,
        session: &Session,
    ) -> bool {
        let Some(owner) = self.owner.as_ref() else {
            return false;
        };
        owner.session_birth == session.created_at
            && router.is_some_and(|router| owner.run.matches_router(router))
            && owner.run.is_current(&session.id).await
    }

    /// Idempotent, live-only stop. Ordinary Session cancellation remains
    /// independent and uses the existing cancellation/error path.
    pub fn stop(&self) {
        self.stop.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatus {
    Running,
    Finalizing,
    Completed,
    Stopped,
    Cancelled,
    BudgetExhausted,
    Failed,
    RecoveryUnsupported,
}

/// Persisted through the existing Session checkpoint, with no separate store.
/// Objective revision is fixed at one in this slice. Future edits must use the
/// generation and revision instead of treating a free-text followup as an edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopState {
    pub schema_version: u32,
    pub generation: String,
    pub revision: u64,
    pub objective_revision: u64,
    pub owner_execution_id: String,
    pub objective: String,
    pub max_iterations: u32,
    pub max_total_rounds: u32,
    pub iterations_started: u32,
    pub iterations_completed: u32,
    /// Conservative admissions, including a round whose preparation later fails.
    /// Existing provider retries stay within one round and retain their limits.
    pub rounds_started: u32,
    pub status: LoopStatus,
}

fn invalid(reason: &str) -> AgentError {
    AgentError::Budget(format!("counted loop: {reason}"))
}

fn validate_bounds(iterations: u32, rounds: u32, objective: &str) -> Result<(), AgentError> {
    if !(1..=MAX_LOOP_ITERATIONS).contains(&iterations)
        || !(iterations..=MAX_LOOP_ROUNDS).contains(&rounds)
        || objective.trim().is_empty()
        || objective.len() > MAX_LOOP_PROMPT_BYTES
    {
        return Err(invalid("invalid iteration, total-round, or prompt bound"));
    }
    Ok(())
}

impl LoopState {
    fn validate(&self) -> Result<(), AgentError> {
        validate_bounds(self.max_iterations, self.max_total_rounds, &self.objective)?;
        if self.schema_version != 1
            || self.revision == 0
            || self.revision == u64::MAX
            || self.objective_revision != 1
            || uuid::Uuid::parse_str(&self.generation).is_err()
            || self.owner_execution_id.is_empty()
            || self.owner_execution_id.len() > 128
            || self.iterations_started > self.max_iterations
            || self.iterations_completed > self.iterations_started
            || self.iterations_started - self.iterations_completed > 1
            || self.rounds_started > self.max_total_rounds
            || self.rounds_started < self.iterations_started
            || (matches!(self.status, LoopStatus::Finalizing | LoopStatus::Completed)
                && self.iterations_completed != self.max_iterations)
            || (self.status == LoopStatus::Running
                && self.iterations_completed == self.max_iterations)
        {
            return Err(invalid("malformed or unsupported persisted state"));
        }
        Ok(())
    }

    fn live(&self) -> bool {
        matches!(self.status, LoopStatus::Running | LoopStatus::Finalizing)
    }
}

/// Fallible inspection: malformed state must never look like an absent loop.
pub fn read_loop_state(session: &Session) -> Result<Option<LoopState>, AgentError> {
    let Some(raw) = session.metadata.get(LOOP_STATE_METADATA_KEY) else {
        return Ok(None);
    };
    if raw.len() > MAX_STATE_BYTES {
        return Err(invalid("persisted state exceeds its byte bound"));
    }
    let state: LoopState = serde_json::from_str(raw)
        .map_err(|_| invalid("malformed or unsupported persisted state"))?;
    state.validate()?;
    Ok(Some(state))
}

async fn checkpoint(
    session: &mut Session,
    config: &AgentLoopConfig,
    state: LoopState,
) -> Result<(), AgentError> {
    checkpoint_state(session, config.persistence.as_deref(), state).await
}

async fn checkpoint_state(
    session: &mut Session,
    persistence: Option<&dyn bamboo_domain::RuntimeSessionPersistence>,
    state: LoopState,
) -> Result<(), AgentError> {
    stage_state(session, state)?;
    persist_checkpoint(session, persistence).await
}

fn stage_state(session: &mut Session, mut state: LoopState) -> Result<String, AgentError> {
    state.revision = state
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid("state revision exhausted"))?;
    state.validate()?;
    let encoded = serde_json::to_string(&state).map_err(|_| invalid("cannot serialize state"))?;
    session
        .metadata
        .insert(LOOP_STATE_METADATA_KEY.into(), encoded.clone());
    Ok(encoded)
}

async fn persist_checkpoint(
    session: &mut Session,
    persistence: Option<&dyn bamboo_domain::RuntimeSessionPersistence>,
) -> Result<(), AgentError> {
    if let Some(persistence) = persistence {
        persistence
            .checkpoint_runtime_session(session)
            .await
            .map_err(|_| invalid("Session checkpoint failed; execution not admitted"))?;
    }
    Ok(())
}

/// Read-only refusal, also used before outer runtime/adapter finalizers.
/// A raw caller never gains a lease or write authority by presenting metadata.
pub(crate) fn reject_recovery(session: &Session) -> Result<(), AgentError> {
    if read_loop_state(session)?.is_some_and(|state| {
        state.live()
            || state.status == LoopStatus::RecoveryUnsupported
            || has_pending_repeat(session, &state)
    }) {
        return Err(invalid(
            "recovery unsupported; the prior admission is not replayed",
        ));
    }
    Ok(())
}

// A generated continuation is not a new external admission. Match the exact
// unfinished iteration; older completed repeats in this generation are history.
fn has_pending_repeat(session: &Session, state: &LoopState) -> bool {
    if state.iterations_started == state.iterations_completed {
        return false;
    }
    let counted = |message: &Message| {
        message
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("runtime_kind"))
            .and_then(serde_json::Value::as_str)
            == Some("counted_loop_iteration")
    };
    let pending = session.messages.iter().rposition(|message| {
        message.role == bamboo_agent_core::Role::User
            && counted(message)
            && message.metadata.as_ref().is_some_and(|meta| {
                meta.get("loop_generation")
                    .and_then(serde_json::Value::as_str)
                    == Some(state.generation.as_str())
                    && meta
                        .get("objective_revision")
                        .and_then(serde_json::Value::as_u64)
                        == Some(state.objective_revision)
                    && meta.get("iteration").and_then(serde_json::Value::as_u64)
                        == Some(u64::from(state.iterations_started))
            })
    });
    pending.is_some_and(|position| {
        !session.messages[position + 1..].iter().any(|message| {
            message.role == bamboo_agent_core::Role::User
                && !counted(message)
                && !bamboo_domain::session::is_system_resume_message(message)
        })
    })
}

/// A failed error save can leave an unfinished terminal clone without the
/// repeat already committed on disk. Public raw execution checks this before
/// any hooks/finalizers. Reading never grants a lease or checkpoint authority.
pub(crate) async fn reject_ambiguous_reentry(
    session: &Session,
    persistence: &dyn bamboo_domain::RuntimeSessionPersistence,
) -> Result<(), AgentError> {
    reject_recovery(session)?;
    let Some(state) = read_loop_state(session)? else {
        return Ok(());
    };
    if state.iterations_started == state.iterations_completed {
        return Ok(());
    }
    let durable = persistence
        .load_runtime_admission_snapshot(&session.id)
        .await
        .map_err(|_| invalid("cannot inspect durable state; recovery unsupported"))?;
    let Some(durable) = durable else {
        return Err(invalid(
            "recovery unsupported; durable state missing; reload required",
        ));
    };
    reject_recovery(&durable)?;
    if durable.created_at != session.created_at
        || read_loop_state(&durable)?.as_ref() != Some(&state)
    {
        return Err(invalid(
            "recovery unsupported; durable generation or revision changed; reload required",
        ));
    }
    Ok(())
}

fn interrupted_runtime_matches(
    session_id: &str,
    runtime: &bamboo_domain::AgentRuntimeState,
    state: &LoopState,
) -> bool {
    match runtime.execution_id.as_deref() {
        Some(id) => id == state.owner_execution_id,
        // Only old snapshots without the typed identity may use the complete
        // round namespace. A Session-id run_id or absent round proves nothing.
        None => runtime.round.last_round_id.as_ref().is_some_and(|round| {
            *round
                == crate::runtime::runner::round_prelude::build_round_id(
                    session_id,
                    &state.owner_execution_id,
                    runtime.round.current_round as usize,
                )
        }),
    }
}

fn fail_interrupted_runtime(session: &mut Session, state: &LoopState) {
    if let Some(runtime) = session.agent_runtime_state.as_mut() {
        if interrupted_runtime_matches(&session.id, runtime, state)
            && matches!(
                runtime.status,
                bamboo_domain::AgentStatusState::Initializing
                    | bamboo_domain::AgentStatusState::Running
                    | bamboo_domain::AgentStatusState::Finalizing
                    | bamboo_domain::AgentStatusState::Completed
            )
        {
            runtime.status = bamboo_domain::AgentStatusState::Failed;
        }
    }
}

/// Called only at an existing consumed/reserved lease boundary, before any
/// preparation, tool replay, hooks or outer final save. No admission is minted.
/// A real registration proves logical exclusion, never Actor write authority:
/// callers must supply their already-bound execution persistence.
pub(crate) async fn admit_registered(
    session: &mut Session,
    ownership: Option<&crate::session_activation::SessionRunOwnership>,
    router: Option<&std::sync::Arc<crate::session_activation::SessionActivationRouter>>,
    persistence: &dyn bamboo_domain::RuntimeSessionPersistence,
) -> Result<(), AgentError> {
    let presented = read_loop_state(session)?;
    // Always read, even when the caller predates the first counted admission.
    let durable = persistence
        .load_runtime_admission_snapshot(&session.id)
        .await
        .map_err(|_| invalid("cannot inspect durable state; execution not admitted"))?;
    let stored = durable.as_ref().map(read_loop_state).transpose()?.flatten();
    if presented != stored
        || (stored.is_some()
            && durable
                .as_ref()
                .is_some_and(|fresh| fresh.created_at != session.created_at))
    {
        return Err(invalid(
            "recovery unsupported; durable generation or revision changed; reload required",
        ));
    }
    let Some(mut state) = stored else {
        return Ok(());
    };
    if state.status == LoopStatus::RecoveryUnsupported {
        return reject_recovery(session);
    }
    if !state.live() {
        reject_recovery(session)?;
        // A failed error-save can leave the live clone without the repeat
        // that was already committed. Only canonical input can clear that
        // ambiguity; a preappend SDK submission is not a recovery capability.
        if let Some(fresh) = durable.as_ref() {
            reject_recovery(fresh)?;
        }
        return Ok(());
    }
    let Some(ownership) = ownership else {
        return reject_recovery(session);
    };
    if !router.is_some_and(|router| ownership.matches_router(router))
        || !ownership.is_current(&session.id).await
    {
        return Err(invalid(
            "recovery unsupported; exact activation owner required",
        ));
    }
    if durable
        .as_ref()
        .is_some_and(|fresh| persistence.root_actor_execution_required(fresh))
    {
        return Err(invalid("recovery unsupported; bound Actor writer required"));
    }
    // Stage from canonical bytes, never from the rejected caller's clone. Its
    // pending User, older waits or permission changes must not be checkpointed.
    let mut staged = durable.expect("stored state requires durable Session");
    fail_interrupted_runtime(&mut staged, &state);
    state.status = LoopStatus::RecoveryUnsupported;
    checkpoint_state(&mut staged, Some(persistence), state.clone()).await?;
    // Reflect only this generation's outcome, preserving caller-owned input
    // and unrelated runtime/wait state. Every adapter returns before saving it.
    session.metadata.insert(
        LOOP_STATE_METADATA_KEY.into(),
        staged.metadata[LOOP_STATE_METADATA_KEY].clone(),
    );
    fail_interrupted_runtime(session, &state);
    reject_recovery(session)
}

/// Runs before submission hooks or transcript mutation. A new activation cannot
/// mistake an ambiguous persisted admission for permission to repeat it.
pub(crate) async fn preflight(
    session: &mut Session,
    config: &AgentLoopConfig,
    prompt: &str,
) -> Result<(), AgentError> {
    reject_recovery(session)?;
    if let Some(counted) = config.counted_loop.as_ref() {
        validate_bounds(counted.max_iterations, counted.max_total_rounds, prompt)?;
        validate_bounds(
            counted.max_iterations,
            effective_round_cap(counted, config),
            prompt,
        )?;
        // Combining the two objective/continuation contracts needs #897. Fail
        // before either gate/provider runs instead of silently changing Gold.
        if config.gold_config.as_ref().is_some_and(|gold| gold.enabled) {
            return Err(invalid(
                "enabled Gold and counted repetition cannot be combined",
            ));
        }
        if counted.stop.is_cancelled() {
            return Err(invalid("the supplied generation was already stopped"));
        }
        if config.skip_initial_user_message
            && !session.messages.last().is_some_and(|message| {
                message.role == bamboo_agent_core::Role::User
                    && message.content == prompt
                    && !bamboo_domain::session::is_system_resume_message(message)
            })
        {
            return Err(invalid(
                "the initial prompt must be the current admitted User",
            ));
        }
    }
    Ok(())
}

fn effective_round_cap(counted: &CountedLoopConfig, config: &AgentLoopConfig) -> u32 {
    config
        .run_budget
        .max_rounds
        .map_or(counted.max_total_rounds, |cap| {
            cap.min(counted.max_total_rounds)
        })
}

pub(crate) async fn begin(
    session: &mut Session,
    config: &AgentLoopConfig,
    execution_id: &str,
    prompt: &str,
) -> Result<Option<String>, AgentError> {
    let Some(counted) = config.counted_loop.as_ref() else {
        return Ok(None);
    };
    validate_bounds(
        counted.max_iterations,
        effective_round_cap(counted, config),
        prompt,
    )?;
    let generation = uuid::Uuid::new_v4().to_string();
    let state = LoopState {
        schema_version: 1,
        generation: generation.clone(),
        revision: 1,
        objective_revision: 1,
        owner_execution_id: execution_id.into(),
        objective: prompt.into(),
        max_iterations: counted.max_iterations,
        max_total_rounds: effective_round_cap(counted, config),
        iterations_started: 0,
        iterations_completed: 0,
        rounds_started: 0,
        status: LoopStatus::Running,
    };
    if let Err(error) = checkpoint(session, config, state).await {
        // The generation exists in memory before its first save returns. Keep
        // the original failure, but fence and terminate that exact generation
        // so the host's later error save cannot persist a phantom live loop.
        let failure = Err(error);
        if let Err(checkpoint_error) = finish(session, config, Some(&generation), &failure).await {
            tracing::warn!(%checkpoint_error, "counted-loop initial failure checkpoint failed");
        }
        return failure.map(|()| Some(generation));
    }
    Ok(Some(generation))
}

fn active_state(
    session: &Session,
    config: &AgentLoopConfig,
) -> Result<Option<LoopState>, AgentError> {
    if config.counted_loop.is_none() {
        return Ok(None);
    }
    Ok(Some(
        read_loop_state(session)?.ok_or_else(|| invalid("missing live state"))?,
    ))
}

/// The single serial admission boundary. Stop precedes prompt injection; a
/// repeated User is never left queued when stop wins this boundary.
pub(crate) async fn before_round(
    session: &mut Session,
    config: &AgentLoopConfig,
    execution_id: &str,
) -> Result<bool, AgentError> {
    let Some(mut state) = active_state(session, config)? else {
        return Ok(true);
    };
    if state.owner_execution_id != execution_id {
        return Err(invalid("execution owner changed"));
    }
    if let Some(owner) = config
        .counted_loop
        .as_ref()
        .and_then(|counted| counted.owner.as_ref())
    {
        if owner.session_birth != session.created_at || !owner.run.is_current(&session.id).await {
            return Err(invalid("execution ownership is no longer current"));
        }
    }
    if !state.live() {
        return Ok(false);
    }
    if config
        .counted_loop
        .as_ref()
        .expect("counted config")
        .stop
        .is_cancelled()
    {
        state.status = LoopStatus::Stopped;
        checkpoint(session, config, state).await?;
        return Ok(false);
    }
    if state.rounds_started == state.max_total_rounds {
        state.status = LoopStatus::BudgetExhausted;
        session.metadata.insert(
            "runtime.completion_reason".into(),
            "counted_loop_round_limit".into(),
        );
        checkpoint(session, config, state).await?;
        return Ok(false);
    }
    let mut staged_repeat = None;
    if state.status == LoopStatus::Running && state.iterations_started == state.iterations_completed
    {
        if state.iterations_started > 0 {
            let mut message = Message::user(state.objective.clone());
            message.metadata = Some(serde_json::json!({
                "runtime_kind": "counted_loop_iteration",
                "loop_generation": state.generation,
                "objective_revision": state.objective_revision,
                "iteration": state.iterations_started + 1,
            }));
            let mut staged = session.clone();
            staged.add_message(message);
            staged_repeat = Some(staged);
        }
        state.iterations_started += 1;
    }
    state.rounds_started += 1;
    if let Some(mut staged) = staged_repeat {
        // Do not expose a queued User before its checkpoint succeeds. Retain
        // conservative counters on an indeterminate write, without publishing
        // uncommitted input into the live execution or its later error save.
        let admitted_state = stage_state(&mut staged, state)?;
        if let Err(error) = persist_checkpoint(&mut staged, config.persistence.as_deref()).await {
            session
                .metadata
                .insert(LOOP_STATE_METADATA_KEY.into(), admitted_state);
            return Err(error);
        }
        *session = staged;
    } else {
        checkpoint(session, config, state).await?;
    }
    Ok(true)
}

pub(crate) enum TerminalDecision {
    Continue,
    Stop,
    Finalize,
}

pub(crate) async fn exhaust_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<(), AgentError> {
    let Some(mut state) = active_state(session, config)? else {
        return Ok(());
    };
    state.status = LoopStatus::BudgetExhausted;
    session.metadata.insert(
        "runtime.completion_reason".into(),
        "counted_loop_budget_exhausted".into(),
    );
    checkpoint(session, config, state).await
}

pub(crate) async fn terminal(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<TerminalDecision, AgentError> {
    let Some(mut state) = active_state(session, config)? else {
        return Ok(TerminalDecision::Finalize);
    };
    if state.status == LoopStatus::Finalizing {
        return Ok(TerminalDecision::Finalize);
    }
    if state.status != LoopStatus::Running
        || state.iterations_started != state.iterations_completed + 1
    {
        return Err(invalid("terminal without one admitted iteration"));
    }
    state.iterations_completed += 1;
    let decision = if config
        .counted_loop
        .as_ref()
        .expect("counted config")
        .stop
        .is_cancelled()
    {
        state.status = LoopStatus::Stopped;
        TerminalDecision::Stop
    } else if state.iterations_completed == state.max_iterations {
        state.status = LoopStatus::Finalizing;
        TerminalDecision::Finalize
    } else {
        TerminalDecision::Continue
    };
    checkpoint(session, config, state).await?;
    Ok(decision)
}

/// Covers setup hooks, provider/tool errors, cancellation, and suspension as
/// well as ordinary completion. No terminal state grants another admission.
pub(crate) async fn finish(
    session: &mut Session,
    config: &AgentLoopConfig,
    generation: Option<&str>,
    result: &Result<(), AgentError>,
) -> Result<(), AgentError> {
    let Some(generation) = generation else {
        return Ok(());
    };
    let Some(mut state) = read_loop_state(session)? else {
        return Err(invalid("missing terminal state"));
    };
    if state.generation != generation {
        return Err(invalid("generation changed during execution"));
    }
    // These are the original execution's final outcomes, not new admissions.
    // A bounded exit can have been checkpointed before fallible AfterRound or
    // finalization work; the Result still governs its unpublished outcome.
    state.status = match result {
        Err(error) if error.is_cancelled() => LoopStatus::Cancelled,
        Err(_) => match state.status {
            LoopStatus::Failed | LoopStatus::Cancelled | LoopStatus::RecoveryUnsupported => {
                state.status
            }
            _ => LoopStatus::Failed,
        },
        Ok(()) if !state.live() => state.status,
        Ok(())
            if session.metadata.contains_key("runtime.suspend_reason")
                || session.agent_runtime_state.as_ref().is_some_and(|runtime| {
                    matches!(runtime.status, bamboo_domain::AgentStatusState::Suspended)
                }) =>
        {
            LoopStatus::RecoveryUnsupported
        }
        Ok(()) => {
            if let Some(reason) = session.metadata.get("runtime.completion_reason") {
                match reason.as_str() {
                    "counted_loop_round_limit"
                    | "counted_loop_budget_exhausted"
                    | "budget_exceeded"
                    | "max_rounds_reached"
                    | "stop_hook_continuation_cap" => LoopStatus::BudgetExhausted,
                    // Question/wait diagnostics do not imply target completion.
                    _ => LoopStatus::RecoveryUnsupported,
                }
            } else if state.iterations_completed == state.max_iterations {
                LoopStatus::Completed
            } else {
                LoopStatus::Failed
            }
        }
    };
    // Always persist the final Runtime mirror as well, even when a prior
    // boundary already wrote the same Stopped/Budget/deny LoopStatus. Complete
    // is published only after this full final snapshot succeeds.
    checkpoint(session, config, state).await
}
