use crate::config::GoldConfig;
use crate::model_areas::resolve_global_area_models;
use crate::model_config_helper::{
    resolve_gold_config, resolve_provider_type, GOLD_CONFIG_METADATA_KEY,
};
use bamboo_agent_core::{AgentEvent, Session};

use crate::app_context::AgentSessionContext;
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::respond::PlanModeTransition;
use crate::session_app::types::ResumeConfigSnapshot;

pub(crate) async fn build_resume_config_snapshot(
    state: &dyn AgentSessionContext,
    session: &Session,
    gold_config_override: Option<GoldConfig>,
) -> ResumeConfigSnapshot {
    let config_snapshot = state.config().read().await.clone();
    let resolved_provider_name = session_effective_model_ref(session)
        .map(|model_ref| model_ref.provider)
        .unwrap_or_else(|| config_snapshot.provider.clone());
    let resolved_provider_type = resolve_provider_type(
        &config_snapshot,
        &resolved_provider_name,
        state.provider_registry(),
    );
    // Auxiliary models are global (config-derived), never session-bound.
    let areas = resolve_global_area_models(
        &config_snapshot,
        &resolved_provider_name,
        state.provider_registry(),
    );

    ResumeConfigSnapshot {
        provider_name: resolved_provider_name,
        provider_type: resolved_provider_type,
        fast_model: areas.fast.as_ref().map(|model| model.model_name.clone()),
        fast_model_ref: areas.fast_ref.clone(),
        background_model: areas
            .background
            .as_ref()
            .map(|model| model.model_name.clone()),
        background_model_ref: areas.background_ref.clone(),
        background_model_provider: areas.background.map(|model| model.provider),
        summarization_model: areas
            .summarization
            .as_ref()
            .map(|model| model.model_name.clone()),
        summarization_model_ref: areas.summarization_ref.clone(),
        summarization_model_provider: areas.summarization.map(|model| model.provider),
        disabled_tools: config_snapshot.disabled_tool_names(),
        disabled_skill_ids: config_snapshot.disabled_skill_ids(),
        image_fallback: crate::model_config_helper::resolve_image_fallback(&config_snapshot)
            .ok()
            .flatten(),
        gold_config: gold_config_override.or_else(|| {
            resolve_gold_config(
                &config_snapshot,
                session
                    .metadata
                    .get(GOLD_CONFIG_METADATA_KEY)
                    .map(String::as_str),
            )
        }),
    }
}

pub(crate) fn plan_mode_transition_event(
    session_id: &str,
    transition: Option<&PlanModeTransition>,
) -> Option<AgentEvent> {
    transition.map(|transition| match transition {
        PlanModeTransition::Entered {
            reason,
            pre_permission_mode,
            entered_at,
            status,
            plan_file_path,
        } => AgentEvent::PlanModeEntered {
            session_id: session_id.to_string(),
            reason: reason.clone(),
            pre_permission_mode: pre_permission_mode.clone(),
            entered_at: *entered_at,
            status: *status,
            plan_file_path: plan_file_path.clone(),
        },
        PlanModeTransition::Exited {
            approved,
            restored_mode,
            plan,
        } => AgentEvent::PlanModeExited {
            session_id: session_id.to_string(),
            approved: *approved,
            restored_mode: restored_mode.clone(),
            plan: plan.clone(),
        },
    })
}
