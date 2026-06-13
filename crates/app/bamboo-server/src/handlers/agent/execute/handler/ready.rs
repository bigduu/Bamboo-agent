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
use super::response::{already_running_response, internal_server_error_response, started_response};
use crate::app_state::AppState;
use crate::handlers::agent::execute::runtime::{
    reserve_runner, spawn_agent_execution, spawn_event_forwarder, RunnerReservation,
    SpawnAgentExecution,
};
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{resolve_gold_config, GOLD_CONFIG_METADATA_KEY};
use bamboo_engine::session_app::provider_model::session_effective_model_ref;
use bamboo_engine::session_app::types::ExecutionConfigSnapshot;
use bamboo_engine::ImageFallbackConfig;
use bamboo_llm::Config;

/// The fields the engine's `ExecutePreparationOutcome::Ready` carries.
pub(super) struct ReadyExecution {
    pub session: bamboo_agent_core::Session,
    pub effective_model: String,
    pub effective_reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub model_source: &'static str,
    pub reasoning_source: &'static str,
    pub is_child_session: bool,
}

/// Reserve the runner, persist, and spawn the agent loop for a ready session.
pub(super) async fn handle_execute_ready(
    state: &web::Data<AppState>,
    session_id: &str,
    ready: ReadyExecution,
    config: &ExecutionConfigSnapshot,
    config_snapshot: &Config,
    image_fallback: Option<ImageFallbackConfig>,
    disabled_tools_vec: Vec<String>,
    disabled_skill_ids_vec: Vec<String>,
) -> HttpResponse {
    let mut session = ready.session;
    // ---- Reserve runner ----
    let session_tx = state.get_session_event_sender(session_id).await;
    let (cancel_token, run_id) =
        match reserve_runner(state.get_ref(), session_id, &session_tx).await {
            RunnerReservation::Started(token, rid) => {
                tracing::debug!(
                    "[{}] Execute Ready -> runner reserved & STARTING (run_id={})",
                    session_id,
                    rid
                );
                (token, rid)
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

    // ---- Save session before spawn (metadata-group merge) ----
    if let Err(error) = state.persistence.merge_save_runtime(&mut session).await {
        return internal_server_error_response(format!(
            "Failed to persist session config before execute: {}",
            error
        ));
    }
    state.sessions.insert(
        session_id.to_string(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    // Kick off async auto-title generation for fresh, untitled sessions.
    if crate::title_gen::is_untitled(&session.title)
        && session
            .messages
            .iter()
            .any(|m| matches!(m.role, bamboo_agent_core::Role::User))
    {
        crate::title_gen::spawn_title_generation(
            state.clone().into_inner(),
            session_id.to_string(),
        );
    }

    let disabled_tools: BTreeSet<String> = disabled_tools_vec.into_iter().collect();
    let disabled_skill_ids: BTreeSet<String> = disabled_skill_ids_vec.into_iter().collect();
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
        is_child_session: ready.is_child_session,
        provider_name: resolved_provider_name,
        provider_override: None,
        model_roster,
        reasoning_effort: ready.effective_reasoning_effort,
        reasoning_effort_source: ready.reasoning_source.to_string(),
        disabled_tools,
        disabled_skill_ids,
        cancel_token,
        mpsc_tx,
        image_fallback,
        gold_config,
        app_data_dir: Some(state.app_data_dir.clone()),
    });

    started_response(session_id, sync_info, run_id)
}
