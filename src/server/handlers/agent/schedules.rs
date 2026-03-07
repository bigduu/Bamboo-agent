//! Schedule management endpoints.

use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};

use crate::server::app_state::AgentStatus;
use crate::server::app_state::AppState;
use crate::server::model_config_helper::get_default_model_from_config;
use crate::server::schedules::store::ScheduleRunConfig;
use crate::server::schedules::{ScheduleEntry, ScheduleRunJob};
use std::collections::HashSet;

#[derive(Debug, Serialize)]
pub struct ListSchedulesResponse {
    pub schedules: Vec<ScheduleEntry>,
}

/// `GET /api/v1/schedules`
pub async fn list_schedules(state: web::Data<AppState>) -> Result<HttpResponse> {
    let items = state.schedule_store.list_schedules().await;
    Ok(HttpResponse::Ok().json(ListSchedulesResponse { schedules: items }))
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub interval_seconds: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
}

/// `POST /api/v1/schedules`
pub async fn create_schedule(
    state: web::Data<AppState>,
    req: web::Json<CreateScheduleRequest>,
) -> Result<HttpResponse> {
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "name is required"
        })));
    }
    if req.interval_seconds == 0 {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "interval_seconds must be > 0"
        })));
    }

    // If auto_execute is enabled, we must have task_message.
    if req.run_config.auto_execute {
        let has_task = req
            .run_config
            .task_message
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .is_some();
        if !has_task {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "run_config.task_message is required when auto_execute is true"
            })));
        }
        // If model isn't provided, ensure server config has a provider model configured.
        let has_explicit_model = req
            .run_config
            .model
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .is_some();
        if !has_explicit_model {
            let snapshot = state.config.read().await.clone();
            if let Err(e) = get_default_model_from_config(&snapshot) {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": format!("run_config.model not provided and no default model configured for provider {}: {}", snapshot.provider, e)
                })));
            }
        }
    }

    let created = state
        .schedule_store
        .create_schedule(
            name,
            req.interval_seconds,
            req.enabled,
            req.run_config.clone(),
        )
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to create schedule: {e}"))
        })?;

    Ok(HttpResponse::Ok().json(created))
}

#[derive(Debug, Deserialize)]
pub struct PatchScheduleRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub interval_seconds: Option<u64>,
    #[serde(default)]
    pub run_config: Option<ScheduleRunConfig>,
}

/// `PATCH /api/v1/schedules/{schedule_id}`
pub async fn patch_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchScheduleRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let name = req
        .name
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    if let Some(cfg) = req.run_config.as_ref() {
        if cfg.auto_execute {
            let has_task = cfg
                .task_message
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .is_some();
            if !has_task {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "run_config.task_message is required when auto_execute is true"
                })));
            }

            let has_explicit_model = cfg
                .model
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .is_some();
            if !has_explicit_model {
                let snapshot = state.config.read().await.clone();
                if let Err(e) = get_default_model_from_config(&snapshot) {
                    return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                        "error": format!("run_config.model not provided and no default model configured for provider {}: {}", snapshot.provider, e)
                    })));
                }
            }
        }
    }

    let updated = state
        .schedule_store
        .patch_schedule(
            &id,
            name,
            req.enabled,
            req.interval_seconds,
            req.run_config.clone(),
        )
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to patch schedule: {e}"))
        })?;

    match updated {
        Some(s) => Ok(HttpResponse::Ok().json(s)),
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Schedule not found",
            "schedule_id": id
        }))),
    }
}

/// `DELETE /api/v1/schedules/{schedule_id}`
pub async fn delete_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let deleted = state
        .schedule_store
        .delete_schedule(&id)
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to delete schedule: {e}"))
        })?;

    if deleted {
        Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
    } else {
        Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Schedule not found",
            "schedule_id": id
        })))
    }
}

/// `POST /api/v1/schedules/{schedule_id}/run`
pub async fn run_now(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let Some(claimed) = state
        .schedule_store
        .create_run_now(&id)
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to create run job: {e}"))
        })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Schedule not found",
            "schedule_id": id
        })));
    };

    let now = chrono::Utc::now();
    state
        .schedule_manager
        .enqueue_run_now(ScheduleRunJob {
            schedule_id: claimed.schedule_id.clone(),
            schedule_name: claimed.schedule_name.clone(),
            run_config: claimed.run_config.clone(),
            claimed_at: now,
        })
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to enqueue run: {e}"))
        })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "schedule_id": claimed.schedule_id,
        "enqueued_at": now
    })))
}

#[derive(Debug, Serialize)]
pub struct ListScheduleSessionsResponse {
    pub schedule_id: String,
    pub sessions: Vec<crate::server::handlers::agent::sessions::SessionSummary>,
}

/// `GET /api/v1/schedules/{schedule_id}/sessions`
pub async fn list_sessions_for_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let schedule_id = path.into_inner();

    let running: HashSet<String> = {
        let runners = state.agent_runners.read().await;
        runners
            .iter()
            .filter_map(|(sid, runner)| {
                if matches!(runner.status, AgentStatus::Running) {
                    Some(sid.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    let sessions = state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .filter(|e| e.created_by_schedule_id.as_deref() == Some(schedule_id.as_str()))
        .map(|e| {
            let is_running = running.contains(&e.id);
            crate::server::handlers::agent::sessions::SessionSummary::from_entry(e, is_running)
        })
        .collect();

    Ok(HttpResponse::Ok().json(ListScheduleSessionsResponse {
        schedule_id,
        sessions,
    }))
}
