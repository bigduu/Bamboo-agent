//! Core agent execution spawning logic.
//!
//! Provides [`spawn_session_execution`] which handles the full lifecycle of a
//! background agent run: spawn task → execute → finalize runner → persist session.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_llm::LLMProvider;

use crate::runtime::config::{AuxiliaryModelConfig, GoldConfig, ImageFallbackConfig};
use crate::runtime::execution::runner_lifecycle::finalize_runner;
use crate::runtime::execution::runner_state::AgentRunner;
use crate::runtime::model_roster::ModelRoster;
use crate::runtime::Agent;
use crate::runtime::{ExecuteRequest, ExecuteRequestBuilder};

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
    pub disabled_tools: Option<BTreeSet<String>>,
    pub disabled_skill_ids: Option<BTreeSet<String>>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub selected_skill_mode: Option<String>,
    pub cancel_token: CancellationToken,
    pub mpsc_tx: mpsc::Sender<AgentEvent>,
    pub image_fallback: Option<ImageFallbackConfig>,
    pub gold_config: Option<GoldConfig>,
    pub app_data_dir: Option<std::path::PathBuf>,

    // Post-execution resources.
    pub runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub sessions_cache: SessionCache,

    /// Optional bespoke finalization, run after the runner is finalized and
    /// before the session is persisted. See [`SessionCompletionHook`].
    pub on_complete: Option<SessionCompletionHook>,
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
    disabled_tools: Option<BTreeSet<String>>,
    disabled_skill_ids: Option<BTreeSet<String>>,
    selected_skill_ids: Option<Vec<String>>,
    selected_skill_mode: Option<String>,
    image_fallback: Option<ImageFallbackConfig>,
    gold_config: Option<GoldConfig>,
    app_data_dir: Option<std::path::PathBuf>,
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
        disabled_tools,
        disabled_skill_ids,
        selected_skill_ids,
        selected_skill_mode,
        image_fallback,
        gold_config,
        app_data_dir,
    } = params;

    let mut builder = ExecuteRequestBuilder::new(initial_message, event_tx, cancel_token)
        .model_roster(model_roster)
        .gold_config(gold_config);

    if let Some(tools) = tools {
        builder = builder.tools(tools);
    }
    if let Some(provider_override) = provider_override {
        builder = builder.provider_override(provider_override);
    }
    if let Some(reasoning_effort) = reasoning_effort {
        builder = builder.reasoning_effort(reasoning_effort);
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
                tools_override,
                provider_override,
                model_roster,
                reasoning_effort,
                reasoning_effort_source,
                auxiliary_model_resolver,
                disabled_tools,
                disabled_skill_ids,
                selected_skill_ids,
                selected_skill_mode,
                cancel_token,
                mpsc_tx,
                image_fallback,
                gold_config,
                app_data_dir,
                runners,
                sessions_cache,
                on_complete,
            } = args;

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
                    disabled_tools,
                    disabled_skill_ids,
                    selected_skill_ids,
                    selected_skill_mode,
                    image_fallback,
                    gold_config,
                    app_data_dir,
                },
            );

            let result = agent.execute(&mut session, execute_request).await;

            // Send terminal event for all error cases (including cancellation).
            if let Some(error_event) = terminal_error_event_for_result(&result) {
                let _ = mpsc_tx.send(error_event).await;
            }

            // Update runner status.
            finalize_runner(&runners, &session_id, &result).await;

            // Bespoke terminal bookkeeping (e.g. a scheduled-run status) runs
            // here — after the runner is finalized but before persistence — so
            // any closing message the hook appends is saved with the session
            // below.
            if let Some(on_complete) = on_complete {
                on_complete(SessionExecutionOutcome::from_result(&result), &mut session).await;
            }

            // Save session via merge-save so any concurrent UI edits to
            // title / pinned / title_version are preserved (the runtime is not
            // an authoritative title writer).
            if let Err(error) = agent.persistence().save_runtime_session(&mut session).await {
                tracing::warn!("[{}] Failed to save session: {}", session_id, error);
            }

            // Update memory cache.
            sessions_cache.insert(
                session_id.clone(),
                Arc::new(parking_lot::RwLock::new(session)),
            );

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
