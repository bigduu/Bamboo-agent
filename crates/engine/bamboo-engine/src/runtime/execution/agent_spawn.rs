//! Core agent execution spawning logic.
//!
//! Provides [`spawn_session_execution`] which handles the full lifecycle of a
//! background agent run: spawn task → execute → finalize runner → persist session.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_llm::LLMProvider;

use crate::runtime::config::{
    AuxiliaryModelConfig, BashCompletionSink, BashResumeHook, DisabledFilterResolver, GoldConfig,
    GuardianConfig, GuardianSpawner, ImageFallbackConfig,
};
use crate::runtime::execution::child_completion::ChildCompletion;
use crate::runtime::execution::runner_lifecycle::{
    finalize_rejected_runner_if_distinct, finalize_runner, finalize_runner_exact,
    reserve_runner_core, ReserveOutcome, RunnerReservation,
};
use crate::runtime::execution::runner_state::AgentRunner;
use crate::runtime::model_roster::ModelRoster;
use crate::runtime::Agent;
use crate::runtime::{ExecuteRequest, ExecuteRequestBuilder};
use crate::session_activation::{
    SessionActivationRouter, SessionRunRegistration, SessionRunRegistrationError,
};

/// Shared, per-session-locked session cache.
///
/// A `DashMap` gives each session id its own shard-level lock (so unrelated
/// sessions never contend), and the inner `parking_lot::RwLock` is a *sync*
/// lock held only to briefly clone-out or mutate-and-write-back a single
/// `Session` — never across an `.await`. Using a sync lock makes "no guard
/// across await" a compile-time guarantee in Send futures.
pub type SessionCache = std::sync::Arc<
    dashmap::DashMap<String, std::sync::Arc<parking_lot::RwLock<bamboo_agent_core::Session>>>,
>;

enum SessionExecutionActivationOwnership {
    /// This runtime was built without a SessionInbox activation router.
    Unrouted,
    /// The runtime spawner reserved an external runner, but the router has not
    /// yet committed the matching owner or invoked the launch closure.
    UnpublishedActivation(Arc<SessionActivationRouter>),
    /// This exact raw runner is waiting to acquire or adopt its router
    /// registration. Manual/server callers enter this state before awaiting an
    /// in-flight activation token; router-dispatched launches enter it after
    /// publishing their zero-registration placeholder.
    RegistrationPending(Arc<SessionActivationRouter>),
    /// Manual/server reservation acquired router ownership before any
    /// execution-specific mutation or external side effect.
    Registered(SessionRunRegistration),
}

/// One exact runner reservation plus its logical-session activation ownership.
///
/// Callers must obtain this through [`reserve_session_execution`]. The only
/// exception is the router's own activation launcher, which uses the
/// crate-private placeholder constructor while assembling its two-phase launch.
pub struct SessionExecutionReservation {
    session_id: String,
    run_id: String,
    cancel_token: CancellationToken,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    activation: SessionExecutionActivationOwnership,
    armed: bool,
}

impl SessionExecutionReservation {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Build the handoff owned by a router activation launch.
    ///
    /// The value starts unpublished. Its launch closure must call
    /// [`mark_activation_published`](Self::mark_activation_published) after the
    /// router commits the matching zero-registration owner.
    pub(crate) fn from_activation_placeholder(
        session_id: impl Into<String>,
        reservation: RunnerReservation,
        router: Arc<SessionActivationRouter>,
        runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            run_id: reservation.run_id,
            cancel_token: reservation.cancel_token,
            runners,
            activation: SessionExecutionActivationOwnership::UnpublishedActivation(router),
            armed: true,
        }
    }

    /// Build a manual/queued execution handoff immediately after reserving its
    /// raw runner. If a router exists, Drop can safely wait for and adopt an
    /// activation placeholder that observed this exact runner.
    pub(crate) fn from_pending_registration(
        session_id: impl Into<String>,
        reservation: RunnerReservation,
        router: Option<Arc<SessionActivationRouter>>,
        runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    ) -> Self {
        let activation = match router {
            Some(router) => SessionExecutionActivationOwnership::RegistrationPending(router),
            None => SessionExecutionActivationOwnership::Unrouted,
        };
        Self {
            session_id: session_id.into(),
            run_id: reservation.run_id,
            cancel_token: reservation.cancel_token,
            runners,
            activation,
            armed: true,
        }
    }

    /// Mark that the router has published this exact owner and invoked its
    /// launch closure.
    pub(crate) fn mark_activation_published(&mut self) {
        let activation = std::mem::replace(
            &mut self.activation,
            SessionExecutionActivationOwnership::Unrouted,
        );
        self.activation = match activation {
            SessionExecutionActivationOwnership::UnpublishedActivation(router) => {
                SessionExecutionActivationOwnership::RegistrationPending(router)
            }
            other => other,
        };
    }

    /// Roll back an external runner whose router launch was never published.
    ///
    /// The router reservation lease exclusively owns token release and retry
    /// ordering here, so this path removes only the exact raw runner slot.
    pub(crate) async fn rollback_unpublished_activation(mut self) {
        self.armed = false;
        self.cancel_token.cancel();
        let activation = std::mem::replace(
            &mut self.activation,
            SessionExecutionActivationOwnership::Unrouted,
        );
        debug_assert!(matches!(
            activation,
            SessionExecutionActivationOwnership::UnpublishedActivation(_)
        ));
        remove_runner_exact(&self.runners, &self.session_id, &self.run_id).await;
    }

    /// Adopt a router-published activation placeholder before any adapter-side
    /// tool replay, workspace mutation, persistence, relay, or spawned task.
    ///
    /// Reservations returned by [`reserve_session_execution`] are already
    /// registered, so calling this on the normal manual/server path is a no-op.
    pub async fn ensure_registered(&mut self) -> Result<(), SessionRunRegistrationError> {
        let router = match &self.activation {
            SessionExecutionActivationOwnership::RegistrationPending(router) => router.clone(),
            SessionExecutionActivationOwnership::UnpublishedActivation(_) => {
                unreachable!("unpublished activation entered an execution adapter")
            }
            SessionExecutionActivationOwnership::Unrouted
            | SessionExecutionActivationOwnership::Registered(_) => return Ok(()),
        };
        match register_reserved_activation(router, &self.runners, &self.session_id, &self.run_id)
            .await
        {
            Ok(registration) => {
                self.activation = SessionExecutionActivationOwnership::Registered(registration);
                Ok(())
            }
            Err(error) => {
                // The helper already terminalized only a distinct attempted
                // runner. Disarm Drop here as part of the shared API so every
                // adapter preserves a same-run live owner's cancellation
                // token, even when it returns immediately on the collision.
                self.disarm_after_registration_rejection(error.existing_run_id());
                Err(error)
            }
        }
    }

    /// Release this exact runner/router reservation before a failed startup is
    /// reported as retryable. The cleanup owns its state in a detached task so
    /// cancellation of this explicit wait cannot strand either ownership plane.
    pub async fn abandon(mut self) {
        self.armed = false;
        self.cancel_token.cancel();
        let activation = std::mem::replace(
            &mut self.activation,
            SessionExecutionActivationOwnership::Unrouted,
        );
        let runners = self.runners.clone();
        let session_id = self.session_id.clone();
        let run_id = self.run_id.clone();
        let cleanup_session_id = session_id.clone();
        let cleanup_run_id = run_id.clone();
        let cleanup = tokio::spawn(async move {
            cleanup_execution_reservation(activation, runners, cleanup_session_id, cleanup_run_id)
                .await;
        });
        if let Err(error) = cleanup.await {
            tracing::error!(
                %session_id,
                %run_id,
                %error,
                "detached execution-reservation cleanup failed"
            );
        }
    }

    fn disarm_after_registration_rejection(&mut self, existing_run_id: &str) {
        if existing_run_id != self.run_id {
            self.cancel_token.cancel();
        }
        self.armed = false;
    }

    pub(crate) fn matches_execution_target(
        &self,
        session_id: &str,
        domain_session_id: &str,
        runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    ) -> bool {
        self.session_id == session_id
            && domain_session_id == session_id
            && Arc::ptr_eq(&self.runners, runners)
    }

    pub(crate) fn disarm_for_execution(
        &mut self,
    ) -> (CancellationToken, Option<SessionRunRegistration>) {
        self.armed = false;
        let registration = match std::mem::replace(
            &mut self.activation,
            SessionExecutionActivationOwnership::Unrouted,
        ) {
            SessionExecutionActivationOwnership::Registered(registration) => Some(registration),
            SessionExecutionActivationOwnership::Unrouted => None,
            SessionExecutionActivationOwnership::UnpublishedActivation(_) => {
                unreachable!("unpublished activation cannot transfer to execution")
            }
            SessionExecutionActivationOwnership::RegistrationPending(_) => {
                unreachable!("ensure_registered adopts every pending router registration")
            }
        };
        (self.cancel_token.clone(), registration)
    }
}

impl Drop for SessionExecutionReservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.cancel_token.cancel();
        let activation = std::mem::replace(
            &mut self.activation,
            SessionExecutionActivationOwnership::Unrouted,
        );
        let runners = self.runners.clone();
        let session_id = self.session_id.clone();
        let run_id = self.run_id.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                cleanup_execution_reservation(activation, runners, session_id, run_id).await;
            });
        }
    }
}

/// Combined runner/router reservation result.
pub enum SessionExecutionReserveOutcome {
    Reserved(SessionExecutionReservation),
    AlreadyRunning { run_id: String },
}

async fn register_reserved_activation(
    router: Arc<SessionActivationRouter>,
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    run_id: &str,
) -> Result<SessionRunRegistration, SessionRunRegistrationError> {
    let mut registration = match router.register_run(session_id, run_id).await {
        Ok(registration) => registration,
        Err(error) => {
            let collision = Err(AgentError::LLM(error.to_string()));
            finalize_rejected_runner_if_distinct(
                runners,
                session_id,
                error.existing_run_id(),
                run_id,
                &collision,
            )
            .await;
            return Err(error);
        }
    };
    let cleanup_runners = runners.clone();
    let cleanup_session_id = session_id.to_string();
    let cleanup_run_id = run_id.to_string();
    registration.set_abort_cleanup(move || async move {
        let abandoned = Err(AgentError::Cancelled);
        finalize_runner_exact(
            &cleanup_runners,
            &cleanup_session_id,
            &cleanup_run_id,
            &abandoned,
        )
        .await;
    });
    Ok(registration)
}

async fn cleanup_execution_reservation(
    activation: SessionExecutionActivationOwnership,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: String,
    run_id: String,
) {
    match activation {
        SessionExecutionActivationOwnership::Registered(registration) => {
            registration.abandon().await;
        }
        SessionExecutionActivationOwnership::RegistrationPending(router) => {
            match register_reserved_activation(router, &runners, &session_id, &run_id).await {
                Ok(registration) => registration.abandon().await,
                Err(error) => {
                    tracing::debug!(
                        %session_id,
                        %run_id,
                        existing_run_id = %error.existing_run_id(),
                        "abandoned activation placeholder was already adopted or superseded"
                    );
                }
            }
        }
        SessionExecutionActivationOwnership::UnpublishedActivation(_) => {
            remove_runner_exact(&runners, &session_id, &run_id).await;
        }
        SessionExecutionActivationOwnership::Unrouted => {
            let abandoned = Err(AgentError::Cancelled);
            finalize_runner_exact(&runners, &session_id, &run_id, &abandoned).await;
        }
    }
}

async fn remove_runner_exact(
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_id: &str,
    run_id: &str,
) -> bool {
    let mut runners = runners.write().await;
    if runners
        .get(session_id)
        .is_some_and(|runner| runner.run_id == run_id)
    {
        runners.remove(session_id);
        true
    } else {
        false
    }
}

/// Reserve the shared runner slot and logical-session router as one handoff.
///
/// The raw runner reservation remains the common low-level primitive used by
/// the router's own two-phase activation protocol. Every manual/server entry
/// point uses this wrapper so an SDK owner or another entry surface collides
/// before approved tools, workspace state, persistence, relays, or a
/// background execution task can run.
pub async fn reserve_session_execution(
    agent: &Arc<Agent>,
    runners: &Arc<RwLock<HashMap<String, AgentRunner>>>,
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
    event_sender: &broadcast::Sender<AgentEvent>,
) -> SessionExecutionReserveOutcome {
    let reservation = match reserve_runner_core(runners, senders, session_id, event_sender).await {
        ReserveOutcome::AlreadyRunning(run_id) => {
            return SessionExecutionReserveOutcome::AlreadyRunning { run_id };
        }
        ReserveOutcome::Reserved(reservation) => reservation,
    };

    // Construct the RAII handoff immediately after the raw runner mutation.
    // If an activation router exists, publish `RegistrationPending` before the
    // registration await. Cancellation cleanup then waits through any
    // in-flight router token, adopts an exact placeholder if the activation
    // observed this raw runner, and abandons both ownership planes together.
    let mut execution_reservation = SessionExecutionReservation {
        session_id: session_id.to_string(),
        run_id: reservation.run_id,
        cancel_token: reservation.cancel_token,
        runners: runners.clone(),
        activation: SessionExecutionActivationOwnership::Unrouted,
        armed: true,
    };

    if let Some(router) = agent.activation_router().cloned() {
        execution_reservation.activation =
            SessionExecutionActivationOwnership::RegistrationPending(router);
        if let Err(error) = execution_reservation.ensure_registered().await {
            tracing::warn!(
                %session_id,
                attempted_run_id = %execution_reservation.run_id(),
                existing_run_id = %error.existing_run_id(),
                %error,
                "runner reservation collided with an existing logical-session owner"
            );
            return SessionExecutionReserveOutcome::AlreadyRunning {
                run_id: error.existing_run_id().to_string(),
            };
        }
    }
    SessionExecutionReserveOutcome::Reserved(execution_reservation)
}

/// Read a session out of the in-memory cache, cloning it out from under the
/// brief sync read-lock. Returns `None` on a cache miss.
///
/// This is the single canonical cache-read used everywhere a caller holds a
/// `SessionCache` (HTTP handlers, server tools, the app-state loader). It
/// replaced ~13 verbatim copies of the
/// `cache.get(id).map(|e| e.value().clone()).map(|a| a.read().clone())` idiom.
pub fn read_cached_session(cache: &SessionCache, id: &str) -> Option<bamboo_agent_core::Session> {
    cache
        .get(id)
        .map(|e| e.value().clone())
        .map(|a| a.read().clone())
}

const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";

/// Outcome of an agent execution, handed to an optional
/// [`SessionCompletionHook`].
///
/// Deliberately decoupled from the runtime's internal error type so the hook
/// API stays stable across crates and callers don't need to match on engine
/// error variants.
pub struct SessionExecutionOutcome {
    /// The run finished without error.
    pub success: bool,
    /// The run ended because it was cancelled (a non-success subset).
    pub cancelled: bool,
    /// Stringified error, present when `!success`.
    pub error: Option<String>,
}

impl SessionExecutionOutcome {
    fn from_result(result: &Result<(), AgentError>) -> Self {
        match result {
            Ok(()) => Self {
                success: true,
                cancelled: false,
                error: None,
            },
            Err(error) => Self {
                success: false,
                cancelled: error.is_cancelled(),
                error: Some(error.to_string()),
            },
        }
    }
}

/// Optional post-execution hook for [`spawn_session_execution`].
///
/// Invoked after the runner is finalized but **before** the session is
/// persisted, so a caller can record bespoke terminal bookkeeping (e.g. a
/// scheduled-run status) and/or append a closing message that is then saved
/// with the session. Receives the execution outcome plus a mutable handle to
/// the session. This is how callers with extra finalization (the schedule
/// manager) route through the single canonical execution path instead of
/// forking their own spawn + `execute` + persist sequence.
pub type SessionCompletionHook = Box<
    dyn for<'a> FnOnce(
            SessionExecutionOutcome,
            &'a mut Session,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
        + Send,
>;

/// Arguments for spawning a background agent execution.
///
/// This is the crate-agnostic equivalent of the server's `SpawnAgentExecution`.
/// It holds everything needed to run the agent loop and persist the result,
/// without depending on HTTP types or `AppState`.
pub struct SessionExecutionArgs {
    // Core execution.
    pub agent: Arc<Agent>,
    pub session_id: String,
    pub session: Session,
    /// Exact shared runner/router ownership acquired before caller-side
    /// execution mutations.
    pub execution_reservation: SessionExecutionReservation,

    // Execution parameters.
    pub tools_override: Option<Arc<dyn ToolExecutor>>,
    pub provider_override: Option<Arc<dyn LLMProvider>>,
    /// Cohesive primary + auxiliary model/provider selection. The primary
    /// `model` is required for a spawn (see [`ModelRoster::model`]); the three
    /// auxiliary roles default to their `Config::get_*` fallbacks when `None`.
    pub model_roster: ModelRoster,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub reasoning_effort_source: String,
    pub auxiliary_model_resolver:
        Option<Arc<dyn Fn() -> crate::runtime::config::AuxiliaryModelConfig + Send + Sync>>,
    /// Optional per-round live resolver for the disabled tool/skill sets (#136).
    /// When `None` the per-run snapshot is used (sub-agent spawns pass `None`, so
    /// short-lived children keep the spawn-time snapshot — by design).
    pub disabled_filter_resolver: Option<DisabledFilterResolver>,
    pub disabled_tools: Option<BTreeSet<String>>,
    pub disabled_skill_ids: Option<BTreeSet<String>>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub selected_skill_mode: Option<String>,
    pub mpsc_tx: mpsc::Sender<AgentEvent>,
    pub image_fallback: Option<ImageFallbackConfig>,
    pub gold_config: Option<GoldConfig>,
    /// Optional guardian adversarial-review gate configuration.
    pub guardian_config: Option<GuardianConfig>,
    /// Late-bound guardian reviewer spawner (server-provided; the runner cannot
    /// construct a child directly).
    pub guardian_spawner: Option<Arc<dyn GuardianSpawner>>,
    /// Late-bound bash self-resume hook (issue #84 Phase 2b).
    pub bash_resume_hook: Option<Arc<dyn BashResumeHook>>,
    /// Late-bound bash completion sink (issue #84 Phase 2b follow-up).
    pub bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
    pub app_data_dir: Option<std::path::PathBuf>,
    /// Per-run resource guardrail override (issue #221). `None` uses the
    /// config-level `Config::run_budget` default unmodified.
    pub run_budget: Option<bamboo_config::RunBudgetConfig>,

    // Post-execution resources.
    pub runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub sessions_cache: SessionCache,

    /// Optional bespoke finalization, run after the runner is finalized and
    /// before the session is persisted. See [`SessionCompletionHook`].
    pub on_complete: Option<SessionCompletionHook>,

    /// Child-completion publisher for CHILD sessions driven through this path
    /// (issue #546). The canonical first run of a child goes through
    /// `run_child_spawn`, which publishes its own terminal completion — but a
    /// child RESUMED later (after an approval, a clarification answer, or a
    /// nested child-parent woken by its own children) runs through
    /// `spawn_session_execution` and previously published nothing, so the
    /// waiting parent was never woken. When set and `session.kind == Child`
    /// with a parent id, the terminal block invokes this handler after the
    /// final persist + runner finalization. Non-terminal suspends publish the
    /// non-terminal "suspended" status, which the coordinator's terminality
    /// guard ignores.
    pub child_completion_handler: Option<Arc<dyn super::ChildCompletionHandler>>,
}

/// The per-request parameter subset of [`SessionExecutionArgs`] — everything
/// that maps onto an [`ExecuteRequest`], minus the three required positional
/// fields (`initial_message`, `event_tx`, `cancel_token`) and the post-execution
/// resources (runners, sessions cache, completion hook).
///
/// Grouping these here lets [`build_execute_request`] perform the
/// `SessionExecutionArgs` → [`ExecuteRequest`] mapping through the canonical
/// [`ExecuteRequestBuilder`] in one place, instead of a hand-written struct
/// literal that must be kept field-aligned with [`ExecuteRequest`] by hand.
struct ExecuteRequestParams {
    tools: Option<Arc<dyn ToolExecutor>>,
    provider_override: Option<Arc<dyn LLMProvider>>,
    model_roster: ModelRoster,
    reasoning_effort: Option<ReasoningEffort>,
    auxiliary_model_resolver: Option<Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>>,
    disabled_filter_resolver: Option<DisabledFilterResolver>,
    disabled_tools: Option<BTreeSet<String>>,
    disabled_skill_ids: Option<BTreeSet<String>>,
    selected_skill_ids: Option<Vec<String>>,
    selected_skill_mode: Option<String>,
    image_fallback: Option<ImageFallbackConfig>,
    gold_config: Option<GoldConfig>,
    guardian_config: Option<GuardianConfig>,
    guardian_spawner: Option<Arc<dyn GuardianSpawner>>,
    bash_resume_hook: Option<Arc<dyn BashResumeHook>>,
    bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
    app_data_dir: Option<std::path::PathBuf>,
    run_budget: Option<bamboo_config::RunBudgetConfig>,
}

/// Assemble an [`ExecuteRequest`] from the resolved spawn parameters via the
/// canonical [`ExecuteRequestBuilder`].
///
/// Centralizing this mapping keeps every optional field threaded with exactly
/// the same value the old struct literal carried (the builder defaults each
/// unset field to `None`), while removing the field-by-field duplication.
fn build_execute_request(
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    cancel_token: CancellationToken,
    params: ExecuteRequestParams,
) -> ExecuteRequest {
    let ExecuteRequestParams {
        tools,
        provider_override,
        model_roster,
        reasoning_effort,
        auxiliary_model_resolver,
        disabled_filter_resolver,
        disabled_tools,
        disabled_skill_ids,
        selected_skill_ids,
        selected_skill_mode,
        image_fallback,
        gold_config,
        guardian_config,
        guardian_spawner,
        bash_resume_hook,
        bash_completion_sink,
        app_data_dir,
        run_budget,
    } = params;

    let mut builder = ExecuteRequestBuilder::new(initial_message, event_tx, cancel_token)
        .model_roster(model_roster)
        .gold_config(gold_config)
        .guardian_config(guardian_config)
        .guardian_spawner(guardian_spawner)
        .bash_resume_hook(bash_resume_hook)
        .bash_completion_sink(bash_completion_sink);

    if let Some(run_budget) = run_budget {
        builder = builder.run_budget(run_budget);
    }

    if let Some(tools) = tools {
        builder = builder.tools(tools);
    }
    if let Some(provider_override) = provider_override {
        builder = builder.provider_override(provider_override);
    }
    if let Some(reasoning_effort) = reasoning_effort {
        builder = builder.reasoning_effort(reasoning_effort);
    }
    if let Some(disabled_filter_resolver) = disabled_filter_resolver {
        builder = builder.disabled_filter_resolver(disabled_filter_resolver);
    }
    if let Some(auxiliary_model_resolver) = auxiliary_model_resolver {
        builder = builder.auxiliary_model_resolver(auxiliary_model_resolver);
    }
    if let Some(disabled_tools) = disabled_tools {
        builder = builder.disabled_tools(disabled_tools);
    }
    if let Some(disabled_skill_ids) = disabled_skill_ids {
        builder = builder.disabled_skill_ids(disabled_skill_ids);
    }
    if let Some(selected_skill_ids) = selected_skill_ids {
        builder = builder.selected_skill_ids(selected_skill_ids);
    }
    if let Some(selected_skill_mode) = selected_skill_mode {
        builder = builder.selected_skill_mode(selected_skill_mode);
    }
    if let Some(image_fallback) = image_fallback {
        builder = builder.image_fallback(image_fallback);
    }
    if let Some(app_data_dir) = app_data_dir {
        builder = builder.app_data_dir(app_data_dir);
    }

    builder.build()
}

/// Spawn a background agent execution task.
///
/// This function spawns a tokio task that:
/// 1. Executes the agent loop via `agent.execute()`
/// 2. Sends a terminal error event if the execution fails
/// 3. Finalizes the runner status
/// 4. Persists the session via merge-save (preserves concurrent UI title/pin edits)
/// 5. Updates the in-memory session cache
pub fn spawn_session_execution(args: SessionExecutionArgs) {
    let span_session_id = args.session_id.clone();
    let session_span = tracing::info_span!("agent_execution", session_id = %span_session_id);

    tokio::spawn(
        async move {
            let SessionExecutionArgs {
                agent,
                session_id,
                mut session,
                mut execution_reservation,
                tools_override,
                provider_override,
                model_roster,
                reasoning_effort,
                reasoning_effort_source,
                auxiliary_model_resolver,
                disabled_filter_resolver,
                disabled_tools,
                disabled_skill_ids,
                selected_skill_ids,
                selected_skill_mode,
                mpsc_tx,
                image_fallback,
                gold_config,
                guardian_config,
                guardian_spawner,
                bash_resume_hook,
                bash_completion_sink,
                app_data_dir,
                run_budget,
                runners,
                sessions_cache,
                on_complete,
                child_completion_handler,
            } = args;

            if !execution_reservation.matches_execution_target(&session_id, &session.id, &runners) {
                tracing::error!(
                    %session_id,
                    domain_session_id = %session.id,
                    reservation_session_id = %execution_reservation.session_id(),
                    run_id = %execution_reservation.run_id(),
                    same_runner_registry = Arc::ptr_eq(&execution_reservation.runners, &runners),
                    "refusing mismatched session execution reservation"
                );
                execution_reservation.abandon().await;
                return;
            }
            if let Err(error) = execution_reservation.ensure_registered().await {
                tracing::warn!(
                    %session_id,
                    run_id = %execution_reservation.run_id(),
                    %error,
                    "session execution could not adopt its router activation owner"
                );
                return;
            }
            let (cancel_token, mut activation_registration) =
                execution_reservation.disarm_for_execution();

            // The primary model is required for a spawn; the roster stores it as
            // `Option<String>` for uniformity, so recover the owned String here
            // for session attribution / logging (same value the caller set).
            let model = model_roster.model.clone().unwrap_or_default();

            let initial_message = initial_user_message_for_session(&session);
            let selected_skill_ids =
                selected_skill_ids.or_else(|| selected_skill_ids_for_session(&session));
            let selected_skill_mode =
                selected_skill_mode.or_else(|| selected_skill_mode_for_session(&session));

            tracing::info!(
                "[{}] Using resolved session model: {}, reasoning_effort={}, reasoning_source={}",
                session_id,
                model,
                reasoning_effort
                    .map(ReasoningEffort::as_str)
                    .unwrap_or("none"),
                reasoning_effort_source
            );

            // Set the resolved model via the single authoritative pre-execution
            // mutation point. The caller already placed the system prompt on the
            // session, so pass `None` for `system_prompt` (the subsequent
            // `system_prompt_for_session` read below sees the caller's message).
            // This must run before that read / logging so the observable
            // sequence (model set, then prompt snapshot) is identical.
            crate::session_app::execution_prep::prepare_session_for_execution(
                &mut session,
                None,
                Some(&model),
            );

            let system_prompt = system_prompt_for_session(&session);
            if let Some(prompt) = system_prompt.as_ref() {
                log_base_system_prompt_snapshot(&session_id, prompt);
            }

            let execute_request = build_execute_request(
                initial_message,
                mpsc_tx.clone(),
                cancel_token,
                ExecuteRequestParams {
                    tools: tools_override,
                    provider_override,
                    model_roster,
                    reasoning_effort,
                    auxiliary_model_resolver,
                    disabled_filter_resolver,
                    disabled_tools,
                    disabled_skill_ids,
                    selected_skill_ids,
                    selected_skill_mode,
                    image_fallback,
                    gold_config,
                    guardian_config,
                    guardian_spawner,
                    bash_resume_hook,
                    bash_completion_sink,
                    app_data_dir,
                    run_budget,
                },
            );

            // Panic containment (issue #546): everything below — the terminal
            // status persist, finalize_runner, and the child-completion
            // publish — only runs if this task survives execution. An
            // unwinding panic would leave a zombie Running runner entry (which
            // blinds liveness checks) and, for a child session, strand its
            // waiting parent. Map a panic to a terminal error instead.
            let result = {
                use futures::FutureExt;
                match std::panic::AssertUnwindSafe(agent.execute(&mut session, execute_request))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => result,
                    Err(panic) => {
                        let message = panic
                            .downcast_ref::<&str>()
                            .map(|s| (*s).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "non-string panic payload".to_string());
                        tracing::error!(
                            "[{}] agent execution panicked; finalizing as terminal error: {}",
                            session_id,
                            message
                        );
                        Err(AgentError::LLM(format!(
                            "agent execution panicked: {message}"
                        )))
                    }
                }
            };

            // Send terminal event for all error cases (including cancellation).
            if let Some(error_event) = terminal_error_event_for_result(&result) {
                let _ = mpsc_tx.send(error_event).await;
            }

            // Record the terminal run status on the session BEFORE persisting so
            // the session summary reports a real `last_run_status`. Top-level
            // sessions otherwise never set it, so summaries show
            // `last_run_status: null`; the frontend then cannot confirm the run
            // finished and falls back on a ~5s optimistic-settle window, leaving
            // a phantom "thinking" indicator after the reply is already done
            // (notably on a session's first turn). Same Ok/cancelled/error
            // mapping `status_from_execution_result` applies to the runner.
            //
            // A suspended run also returns `Ok(())` but is NOT terminal: it
            // stamped `runtime.suspend_reason` (awaiting_clarification /
            // waiting_for_children / waiting_for_bash / awaiting_parent_approval)
            // and will resume later (which removes the reason — see respond.rs /
            // child_completion_coordinator). Mark it "suspended" rather than
            // "completed" so a session waiting on the user or on children isn't
            // reported as finished (mirrors the child path in `sdk::spawn`).
            let suspended_non_terminal = result.is_ok()
                && session
                    .metadata
                    .get("runtime.suspend_reason")
                    .is_some_and(|reason| !reason.trim().is_empty());
            match &result {
                Ok(()) if suspended_non_terminal => {
                    session.set_last_run_status("suspended");
                    session.clear_last_run_error();
                }
                Ok(()) => {
                    session.set_last_run_status("completed");
                    session.clear_last_run_error();
                }
                Err(error) if error.is_cancelled() => {
                    session.set_last_run_status("cancelled");
                    session.set_last_run_error(error.to_string());
                }
                Err(error) => {
                    session.set_last_run_status("error");
                    session.set_last_run_error(error.to_string());
                }
            }

            // Bespoke terminal bookkeeping (e.g. a scheduled-run status) runs
            // here — before persistence — so any closing message the hook
            // appends is saved with the session below.
            if let Some(on_complete) = on_complete {
                on_complete(SessionExecutionOutcome::from_result(&result), &mut session).await;
            }

            // Freeze the generation actually consumed by provider reasoning.
            // Terminal compatibility migration below may enqueue newer work,
            // but must never make that work look executed.
            let executed_admitted_generation = session
                .session_inbox_admission()
                .map_or(0, |state| state.last_admitted_sequence);
            let legacy_migration =
                crate::runtime::runner::state_bridge::migrate_legacy_pending_only(
                    &mut session,
                    Some(agent.storage()),
                    Some(agent.persistence()),
                    agent.session_inbox(),
                )
                .await;
            let pending_boundary_generation = session
                .session_inbox_admission()
                .and_then(|state| state.pending_activation_generation());
            let pending_generation = match (
                pending_boundary_generation,
                legacy_migration.highest_generation,
            ) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (left, right) => left.or(right),
            };
            if let (Some(generation), Some(router)) =
                (pending_generation, agent.activation_router())
            {
                let activation_ready = if let Some(inbox) = agent.session_inbox() {
                    match inbox
                        .mark_activation_eligible(
                            &session_id,
                            generation,
                            bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                        )
                        .await
                    {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::error!(
                                session_id = %session_id,
                                %error,
                                "failed to persist unadmitted SessionInbox activation watermark"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
                if activation_ready {
                    if let Err(error) = bamboo_domain::SessionActivationPort::request_activation(
                        router.as_ref(),
                        &session_id,
                        generation,
                    )
                    .await
                    {
                        tracing::error!(
                            session_id = %session_id,
                            %error,
                            "failed to hand unadmitted SessionInbox generation to activation router"
                        );
                    }
                }
            }

            // Close the save→terminal race before the final persistence write:
            // a delivery from this point onward is coalesced for a successor
            // rather than being "notified" to a loop that has already exited.
            if let Some(registration) = activation_registration.as_mut() {
                registration.begin_finalization().await;
            }

            // Save session via merge-save so any concurrent UI edits to
            // title / title_generated / pinned / title_version are preserved (the runtime is not
            // an authoritative title writer).
            if let Err(error) = agent.persistence().save_runtime_session(&mut session).await {
                tracing::warn!("[{}] Failed to save session: {}", session_id, error);
            }

            // Flip the runner registry to a terminal status (which makes session
            // summaries report `is_running: false`) ONLY AFTER the run status is
            // persisted above, so `is_running` and `last_run_status` become
            // visible together and the frontend settles immediately instead of
            // lingering in its optimistic-settle window.
            finalize_runner(&runners, &session_id, &result).await;

            let finalization = if let Some(registration) = activation_registration.take() {
                registration.finish(executed_admitted_generation).await
            } else {
                Ok(None)
            };
            if let Err(error) = finalization {
                // Delivery is durable even if activation infrastructure is
                // temporarily unavailable; startup/backlog reconciliation
                // can retry without loss.
                tracing::error!(
                    session_id = %session_id,
                    %error,
                    "failed to activate successor for finalization-racing SessionInbox delivery"
                );
            }

            // A CHILD session finishing through this path (a resumed child, or
            // a nested child-parent woken by its own children) must wake its
            // waiting parent (issue #546). Publish AFTER the final persist and
            // runner finalization so the coordinator reads the child's settled
            // terminal state — the resume message folds in the child's final
            // assistant content from storage. The status mirrors the
            // `last_run_status` mapping above; a non-terminal "suspended" is
            // published too and ignored by the coordinator's terminality guard.
            let child_completion = child_completion_handler.filter(|_| {
                session.kind == bamboo_agent_core::SessionKind::Child
                    && session.parent_session_id.is_some()
            });
            let parent_session_id = session.parent_session_id.clone();
            let child_status = session.last_run_status();
            let child_error = session.last_run_error();

            // Update memory cache.
            sessions_cache.insert(
                session_id.clone(),
                Arc::new(parking_lot::RwLock::new(session)),
            );

            if let (Some(handler), Some(parent_session_id), Some(status)) =
                (child_completion, parent_session_id, child_status)
            {
                use futures::FutureExt;
                let completion = ChildCompletion {
                    parent_session_id: parent_session_id.clone(),
                    child_session_id: session_id.clone(),
                    status,
                    error: child_error,
                    completed_at: chrono::Utc::now(),
                };
                if std::panic::AssertUnwindSafe(handler.on_child_completed(completion))
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    tracing::error!(
                        %parent_session_id,
                        child_session_id = %session_id,
                        "child completion handler panicked on resumed-child terminal"
                    );
                }
            }

            tracing::info!("[{}] Agent execution completed", session_id);
        }
        .instrument(session_span),
    );
}

/// Log a snapshot of the base system prompt for debugging.
pub fn log_base_system_prompt_snapshot(session_id: &str, prompt: &str) {
    tracing::info!(
        "[{}] Base system prompt snapshot: len={} chars, has_skill={}, has_tool_guide={}, has_external_memory={}, has_task_list={}",
        session_id,
        prompt.len(),
        prompt.contains(SKILL_CONTEXT_START_MARKER),
        prompt.contains(TOOL_GUIDE_START_MARKER),
        prompt.contains(EXTERNAL_MEMORY_START_MARKER),
        prompt.contains(TASK_LIST_START_MARKER),
    );

    tracing::debug!(
        "[{}] ========== BASE SYSTEM PROMPT SNAPSHOT ==========",
        session_id
    );
    tracing::debug!("[{}] Snapshot length: {} chars", session_id, prompt.len());
    tracing::debug!("[{}] -----------------------------------", session_id);
    tracing::debug!("[{}] {}", session_id, prompt);
    tracing::debug!(
        "[{}] ========== END BASE SYSTEM PROMPT SNAPSHOT ==========",
        session_id
    );
}

/// Map an execution result to a terminal error event.
pub fn terminal_error_event_for_result(result: &Result<(), AgentError>) -> Option<AgentEvent> {
    match result {
        Ok(_) => None,
        Err(error) if error.is_cancelled() => Some(AgentEvent::Error {
            message: "Agent execution cancelled by user".to_string(),
        }),
        Err(error) => Some(AgentEvent::Error {
            message: error.to_string(),
        }),
    }
}

// Session metadata helpers (pure functions, no server dependency).

fn system_prompt_for_session(session: &Session) -> Option<String> {
    session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        .map(|message| message.content.clone())
}

fn initial_user_message_for_session(session: &Session) -> String {
    session
        .messages
        .last()
        .filter(|message| matches!(message.role, bamboo_agent_core::Role::User))
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

fn selected_skill_ids_for_session(session: &Session) -> Option<Vec<String>> {
    session
        .metadata
        .get("selected_skill_ids")
        .and_then(|raw| bamboo_skills::selection::parse_selected_skill_ids_metadata(raw))
}

fn selected_skill_mode_for_session(session: &Session) -> Option<String> {
    let value = session
        .metadata
        .get("skill_mode")
        .or_else(|| session.metadata.get("mode"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod reservation_tests {
    use super::*;
    use crate::runtime::execution::runner_state::AgentStatus;

    #[test]
    fn reservation_target_requires_domain_id_and_exact_runner_registry() {
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let other_runners = Arc::new(RwLock::new(HashMap::new()));
        let mut reservation = SessionExecutionReservation {
            session_id: "session-a".to_string(),
            run_id: "run-a".to_string(),
            cancel_token: CancellationToken::new(),
            runners: runners.clone(),
            activation: SessionExecutionActivationOwnership::Unrouted,
            armed: true,
        };

        assert!(reservation.matches_execution_target("session-a", "session-a", &runners));
        assert!(!reservation.matches_execution_target("session-b", "session-a", &runners));
        assert!(!reservation.matches_execution_target("session-a", "session-b", &runners));
        assert!(!reservation.matches_execution_target("session-a", "session-a", &other_runners));
        reservation.armed = false;
    }

    #[tokio::test]
    async fn forced_same_run_registration_rejection_never_cancels_live_owner() {
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let run_id = runner.run_id.clone();
        let live_cancel_token = runner.cancel_token.clone();
        runners.write().await.insert("same-run".to_string(), runner);

        let router = SessionActivationRouter::new();
        let mut live_registration = router
            .register_run("same-run", &run_id)
            .await
            .expect("first registration owns the run");
        let mut rejected = SessionExecutionReservation {
            session_id: "same-run".to_string(),
            run_id: run_id.clone(),
            cancel_token: live_cancel_token.clone(),
            runners: runners.clone(),
            activation: SessionExecutionActivationOwnership::RegistrationPending(router.clone()),
            armed: true,
        };

        let error = match rejected.ensure_registered().await {
            Ok(()) => panic!("a duplicate registration for the same run must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.existing_run_id(), run_id);
        drop(rejected);

        assert!(!live_cancel_token.is_cancelled());
        assert!(matches!(
            runners
                .read()
                .await
                .get("same-run")
                .map(|runner| &runner.status),
            Some(AgentStatus::Running)
        ));
        assert!(router.owns_run("same-run", &run_id).await);
        live_registration.begin_finalization().await;
        assert_eq!(live_registration.finish(0).await.unwrap(), None);
    }
}
