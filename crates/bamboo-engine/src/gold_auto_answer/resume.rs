use crate::config::GoldConfig;
use bamboo_agent_core::{AgentEvent, Session};

use crate::app_context::AgentSessionContext;
use crate::session_app::resolution::resolve_resume_config_snapshot;
use crate::session_app::respond::PlanModeTransition;
use crate::session_app::types::ResumeConfigSnapshot;

pub(crate) async fn build_resume_config_snapshot(
    state: &dyn AgentSessionContext,
    session: &Session,
    gold_config_override: Option<GoldConfig>,
) -> ResumeConfigSnapshot {
    let config_snapshot = state.config().read().await.clone();
    resolve_resume_config_snapshot(
        &config_snapshot,
        state.provider_registry(),
        session,
        gold_config_override,
    )
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
