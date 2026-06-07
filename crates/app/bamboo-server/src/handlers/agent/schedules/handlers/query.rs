use actix_web::{web, HttpResponse, Result};
use std::collections::HashSet;

use crate::app_state::{AgentStatus, AppState};

use super::super::types::{
    ListScheduleRunsResponse, ListScheduleSessionsResponse, ListSchedulesResponse,
    ScheduleRunRecordView, ScheduleView,
};

/// `GET /api/v1/schedules`
pub async fn list_schedules(state: web::Data<AppState>) -> Result<HttpResponse> {
    let items = state
        .schedule_store
        .list_schedules()
        .await
        .into_iter()
        .map(ScheduleView::from)
        .collect();
    Ok(HttpResponse::Ok().json(ListSchedulesResponse { schedules: items }))
}

/// `GET /api/v1/schedules/{schedule_id}/sessions`
pub async fn list_sessions_for_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let schedule_id = path.into_inner();
    let running = running_session_ids(&state).await;

    let sessions = state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .filter(|entry| entry.created_by_schedule_id.as_deref() == Some(schedule_id.as_str()))
        .map(|entry| {
            let is_running = running.contains(&entry.id);
            crate::handlers::agent::sessions::SessionSummary::from_entry(entry, is_running)
        })
        .collect();

    Ok(HttpResponse::Ok().json(ListScheduleSessionsResponse {
        schedule_id,
        sessions,
    }))
}

/// `GET /api/v1/schedules/{schedule_id}/runs`
pub async fn list_runs_for_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let schedule_id = path.into_inner();
    let runs = state
        .schedule_store
        .list_run_records_for_schedule(&schedule_id)
        .await
        .into_iter()
        .map(ScheduleRunRecordView::from)
        .collect();

    Ok(HttpResponse::Ok().json(ListScheduleRunsResponse { schedule_id, runs }))
}

async fn running_session_ids(state: &web::Data<AppState>) -> HashSet<String> {
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
