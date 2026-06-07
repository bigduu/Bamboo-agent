use std::collections::HashSet;

use actix_web::web;

use crate::app_state::{AgentStatus, AppState};

pub(super) async fn running_session_ids(state: &web::Data<AppState>) -> HashSet<String> {
    let runners = state.agent_runners.read().await;
    runners
        .iter()
        .filter_map(|(session_id, runner)| {
            if matches!(runner.status, AgentStatus::Running) {
                Some(session_id.clone())
            } else {
                None
            }
        })
        .collect()
}

pub(super) async fn is_session_running(state: &web::Data<AppState>, session_id: &str) -> bool {
    let runners = state.agent_runners.read().await;
    runners
        .get(session_id)
        .map(|runner| matches!(runner.status, AgentStatus::Running))
        .unwrap_or(false)
}
