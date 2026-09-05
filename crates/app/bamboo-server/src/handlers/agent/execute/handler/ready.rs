//! Orchestration for the `Ready` execute-preparation outcome.
//!
//! Extracted from `handler()` so the HTTP entrypoint stays a thin
//! parse → prepare → match → map. Everything here is the use-case work that
//! runs once the engine's `prepare_execute` says the session is ready to spawn:
//! reserve the runner, persist + cache, kick auto-title-gen, resolve the
//! session-effective provider/area models, and spawn the agent loop.
//!
//! Behavior is identical to the prior inline branch — this is a pure extraction.

use actix_web::{web, HttpResponse};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::build_sync_info_from_session;
use super::response::{
    already_running_response, bad_request_error_response, internal_server_error_response,
    started_response,
};
use crate::app_state::AppState;
use crate::handlers::agent::execute::runtime::{
    reserve_runner, spawn_agent_execution, spawn_event_forwarder, RunnerReservation,
    SpawnAgentExecution,
};
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{
    resolve_gold_config, resolve_provider_routing_key, GOLD_CONFIG_METADATA_KEY,
};
use bamboo_engine::session_app::provider_model::{persist_model_ref, session_effective_model_ref};
use bamboo_engine::session_app::types::ExecutionConfigSnapshot;
use bamboo_engine::ImageFallbackConfig;
use bamboo_llm::Config;

/// The fields the engine's `ExecutePreparationOutcome::Ready` carries.
pub(super) struct ReadyExecution<'a> {
    pub session: bamboo_agent_core::Session,
    pub startup_guard: &'a mut crate::handlers::agent::events::ExecuteStartupGuard,
    /// Durable message id owned by the pending execute handoff, if any.
    pub startup_turn_id: Option<String>,
    pub effective_model: String,
    pub effective_reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub model_source: &'static str,
    pub reasoning_source: &'static str,
    pub is_child_session: bool,
    /// The `no_human_approver` posture from THIS execute request. #74: re-derived
    /// per user-initiated execute and written over the session's persisted flag.
    pub no_human_approver: bool,
    /// Optional per-run resource guardrail override from THIS execute request
    /// (issue #221). `None` uses the config-level default.
    pub run_budget: Option<bamboo_config::RunBudgetConfig>,
}

pub(super) struct ExecuteReadyContext<'a> {
    pub state: &'a web::Data<AppState>,
    pub session_id: &'a str,
    pub ready: ReadyExecution<'a>,
    pub config: &'a ExecutionConfigSnapshot,
    pub config_snapshot: &'a Config,
    pub image_fallback: Option<ImageFallbackConfig>,
    pub disabled_tools: Vec<String>,
    pub disabled_skill_ids: Vec<String>,
}

/// Reserve the runner, persist, and spawn the agent loop for a ready session.
pub(super) async fn handle_execute_ready(context: ExecuteReadyContext<'_>) -> HttpResponse {
    let ExecuteReadyContext {
        state,
        session_id,
        ready,
        config,
        config_snapshot,
        image_fallback,
        disabled_tools,
        disabled_skill_ids,
    } = context;
    let mut session = ready.session;

    // Old sessions and compatibility clients may still persist a built-in
    // provider type (for example `openai`) while the registry is keyed by a
    // custom instance id. Canonicalize before reserving or saving the run, and
    // require the exact target to be live. An unresolved explicit/session
    // target must never fall through to the process-wide default provider.
    let provider_override = if let Some(mut model_ref) = session_effective_model_ref(&session) {
        let routing_key = match resolve_provider_routing_key(
            config_snapshot,
            &model_ref.provider,
            &state.provider_registry,
        ) {
            Ok(routing_key) => routing_key,
            Err(error) => {
                let detail = format!("provider routing failed: {error}");
                super::fail_pending_startup(
                    state,
                    session_id,
                    ready.startup_turn_id.as_deref(),
                    &detail,
                    &mut *ready.startup_guard,
                )
                .await;
                return bad_request_error_response(detail);
            }
        };
        model_ref.provider = routing_key;
        persist_model_ref(&mut session, &model_ref);
        match state.provider_router.route(&model_ref) {
            Ok(provider) => Some(provider),
            Err(error) => {
                let detail = format!("provider routing failed: {error}");
                super::fail_pending_startup(
                    state,
                    session_id,
                    ready.startup_turn_id.as_deref(),
                    &detail,
                    &mut *ready.startup_guard,
                )
                .await;
                return bad_request_error_response(detail);
            }
        }
    } else {
        None
    };

    // #74: re-derive the "no interactive human approver" posture per
    // user-initiated execute, OVERWRITING the session's persisted flag (see
    // `apply_no_human_approver`). Done before the `merge_save_runtime` persist
    // below so the corrected posture is what's stored and handed to the spawn.
    apply_no_human_approver(&mut session, ready.no_human_approver);

    // ---- Reserve runner ----
    let session_tx = state.get_session_event_sender(session_id).await;
    let (execution_reservation, run_id) =
        match reserve_runner(state.get_ref(), session_id, &session_tx).await {
            RunnerReservation::Started(reservation) => {
                let rid = reservation.run_id().to_string();
                tracing::debug!(
                    "[{}] Execute Ready -> runner reserved & STARTING (run_id={})",
                    session_id,
                    rid
                );
                (reservation, rid)
            }
            RunnerReservation::AlreadyRunning(rid) => {
                tracing::debug!(
                    "[{}] Execute Ready -> runner AlreadyRunning (run_id={}); not spawning",
                    session_id,
                    rid
                );
                let sync_info = build_sync_info_from_session(&session);
                return already_running_response(session_id, sync_info, Some(rid));
            }
        };

    // The reservation owns this exact turn now. Moving the owned marker out of
    // `pending` before persistence prevents an old terminal runner from being
    // used to classify the newly-starting turn.
    session.set_last_run_status("running");
    session.clear_last_run_error();
    crate::handlers::agent::events::clear_pending_turn(&mut session);

    // ---- Save session before spawn (metadata-group merge) ----
    if let Err(error) = state.persistence.merge_save_runtime(&mut session).await {
        execution_reservation.abandon().await;
        rollback_startup(
            state,
            session_id,
            &run_id,
            ready.startup_turn_id.as_deref(),
            &error.to_string(),
            ready.startup_guard,
        )
        .await;
        return internal_server_error_response(format!(
            "Failed to persist session config before execute: {}",
            error
        ));
    }
    state.sessions.insert(
        session_id.to_string(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(session.clone())),
    );

    let disabled_tools: BTreeSet<String> = disabled_tools.into_iter().collect();
    let disabled_skill_ids: BTreeSet<String> = disabled_skill_ids.into_iter().collect();
    let resolved_provider_name = session_effective_model_ref(&session)
        .map(|model_ref| model_ref.provider)
        .unwrap_or_else(|| config.provider_name.clone());
    let resolved_provider_type = bamboo_engine::model_config_helper::resolve_provider_type(
        config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );
    // Auxiliary models for the spawn: global config, keyed to the session's
    // resolved provider for fallback — never session-derived model values.
    let areas = resolve_global_area_models(
        config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );

    // Build sync info before moving session into SpawnAgentExecution.
    let sync_info = build_sync_info_from_session(&session);

    tracing::info!(
        "[{}] Starting agent execution with provider={}, model={}, model_source={}, reasoning_effort={}, reasoning_source={}",
        session_id,
        resolved_provider_name,
        ready.effective_model,
        ready.model_source,
        ready
            .effective_reasoning_effort
            .map(bamboo_domain::reasoning::ReasoningEffort::as_str)
            .unwrap_or("none"),
        ready.reasoning_source
    );

    let gold_config = resolve_gold_config(
        config_snapshot,
        session
            .metadata
            .get(GOLD_CONFIG_METADATA_KEY)
            .map(String::as_str),
    );

    // Create mpsc channel for agent loop.
    let (mpsc_tx, mpsc_rx) = mpsc::channel::<bamboo_agent_core::AgentEvent>(100);

    spawn_event_forwarder(
        state.clone(),
        session_id.to_string(),
        run_id.clone(),
        mpsc_rx,
        session_tx.clone(),
        gold_config.clone(),
    );
    let model_roster = bamboo_engine::ModelRoster::from_areas(
        Some(ready.effective_model),
        Some(resolved_provider_name.clone()),
        resolved_provider_type,
        areas,
    );
    spawn_agent_execution(SpawnAgentExecution {
        state: state.clone(),
        session_id: session_id.to_string(),
        session,
        execution_reservation,
        is_child_session: ready.is_child_session,
        provider_name: resolved_provider_name,
        provider_override,
        model_roster,
        reasoning_effort: ready.effective_reasoning_effort,
        reasoning_effort_source: ready.reasoning_source.to_string(),
        disabled_tools,
        disabled_skill_ids,
        mpsc_tx,
        image_fallback,
        gold_config,
        app_data_dir: Some(state.app_data_dir.clone()),
        run_budget: ready.run_budget,
    });

    started_response(session_id, sync_info, run_id)
}

async fn rollback_startup(
    state: &web::Data<AppState>,
    session_id: &str,
    run_id: &str,
    expected_turn_id: Option<&str>,
    detail: &str,
    startup_guard: &mut crate::handlers::agent::events::ExecuteStartupGuard,
) {
    // Remove only our reservation. A concurrent retry may already have replaced
    // it, and must not be disturbed.
    let mut runners = state.agent_runners.write().await;
    if runners
        .get(session_id)
        .is_some_and(|runner| runner.run_id == run_id)
    {
        bamboo_engine::runtime::execution::runner_lifecycle::remove_runner_entry(
            &mut runners,
            session_id,
        )
        .await;
    }
    drop(runners);

    let Some(expected_turn_id) = expected_turn_id else {
        return;
    };
    crate::handlers::agent::events::transition_startup_failure_if_owned(
        state,
        session_id,
        crate::handlers::agent::events::StartupFailureTarget::WorkId {
            work_id: expected_turn_id,
            startup_guard,
        },
        detail,
    )
    .await;
}

/// #74: re-derive the "no interactive human approver" posture for a
/// user-initiated execute, OVERWRITING the session's persisted flag with
/// `no_human_approver` from THIS request.
///
/// Why an overwrite (not a sticky carry-forward): a session first run
/// headlessly/scheduled persists `true`; if it is later reopened INTERACTIVELY,
/// the UI omits the field (→ `false`), so this resets the session to the
/// human-present posture. Otherwise its sub-agents (which inherit the flag)
/// would silently model-review approvals that a now-present human should answer.
///
/// Safe w.r.t. suspend/resume: a within-run resume (answering a pending
/// question, the waiting_for_children resume, gold auto-answer) does NOT
/// re-enter the execute handler — it goes through `resume_session_execution`,
/// which reloads the persisted `runtime_state` and never touches an
/// `ExecuteRequest`. So this overwrite only fires on a fresh user execute; the
/// startup carry-forward then preserves the posture across the run's segments.
fn apply_no_human_approver(session: &mut bamboo_agent_core::Session, no_human_approver: bool) {
    session
        .agent_runtime_state
        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
        .no_human_approver = no_human_approver;
}

#[cfg(test)]
mod ready_tests {
    use super::apply_no_human_approver;
    use bamboo_agent_core::Session;
    use bamboo_domain::AgentRuntimeState;

    #[test]
    fn interactive_execute_resets_persisted_no_human_approver_to_false() {
        // Cross-mode resume (#74): a session first run headlessly persists
        // `no_human_approver = true`. Reopening it INTERACTIVELY (the UI omits
        // the field, so the request carries `false`) must OVERWRITE the stale
        // `true` back to `false` so approvals reach the now-present human.
        let mut session = Session::new("sess-cross-mode", "test-model");
        let mut prev = AgentRuntimeState::new("run-prev");
        prev.no_human_approver = true;
        session.agent_runtime_state = Some(prev);

        apply_no_human_approver(&mut session, false);

        assert!(
            !session
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .no_human_approver,
            "interactive execute (false) must reset a persisted true"
        );
    }

    #[test]
    fn headless_execute_yields_no_human_approver_true() {
        // Headless `-p` sends `no_human_approver = true` on the request; the
        // overwrite must set it on a session with no prior runtime state.
        let mut session = Session::new("sess-headless", "test-model");
        assert!(session.agent_runtime_state.is_none());

        apply_no_human_approver(&mut session, true);

        assert!(
            session
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .no_human_approver,
            "headless execute (true) must set the posture"
        );
    }

    #[test]
    fn execute_overwrites_regardless_of_prior_value() {
        // The overwrite is unconditional (not OR-sticky): an interactive run
        // (false) over a persisted false stays false, and a true-over-true stays
        // true — the request value is authoritative each execute.
        for (prev_flag, req_flag) in [(false, false), (true, true), (false, true), (true, false)] {
            let mut session = Session::new("sess", "test-model");
            let mut prev = AgentRuntimeState::new("run-prev");
            prev.no_human_approver = prev_flag;
            session.agent_runtime_state = Some(prev);

            apply_no_human_approver(&mut session, req_flag);

            assert_eq!(
                session
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .no_human_approver,
                req_flag,
                "request value must be authoritative (prev={prev_flag}, req={req_flag})"
            );
        }
    }
}
