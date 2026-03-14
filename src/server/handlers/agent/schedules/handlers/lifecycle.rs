use actix_web::{web, HttpResponse, Result};

use crate::server::app_state::AppState;
use crate::server::schedules::ScheduleRunJob;

use super::super::types::{CreateScheduleRequest, PatchScheduleRequest};
use super::super::validation::{
    validate_auto_execute_run_config, validate_create_interval_seconds, validate_schedule_name,
};
use super::response::{internal_server_error, schedule_not_found};

/// `POST /api/v1/schedules`
pub async fn create_schedule(
    state: web::Data<AppState>,
    req: web::Json<CreateScheduleRequest>,
) -> Result<HttpResponse> {
    let name = match validate_schedule_name(&req.name) {
        Ok(name) => name,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_create_interval_seconds(req.interval_seconds) {
        return Ok(response);
    }
    if let Err(response) = validate_auto_execute_run_config(&state, &req.run_config).await {
        return Ok(response);
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
        .map_err(|error| internal_server_error("create schedule", error))?;

    Ok(HttpResponse::Ok().json(created))
}

/// `PATCH /api/v1/schedules/{schedule_id}`
pub async fn patch_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchScheduleRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let name = normalize_optional_name(req.name.as_deref());

    if let Some(run_config) = req.run_config.as_ref() {
        if let Err(response) = validate_auto_execute_run_config(&state, run_config).await {
            return Ok(response);
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
        .map_err(|error| internal_server_error("patch schedule", error))?;

    match updated {
        Some(schedule) => Ok(HttpResponse::Ok().json(schedule)),
        None => Ok(schedule_not_found(&id)),
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
        .map_err(|error| internal_server_error("delete schedule", error))?;

    if deleted {
        Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
    } else {
        Ok(schedule_not_found(&id))
    }
}

/// `POST /api/v1/schedules/{schedule_id}/run`
pub async fn run_now(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let Some(claimed) = state
        .schedule_store
        .create_run_now(&id)
        .await
        .map_err(|error| internal_server_error("create run job", error))?
    else {
        return Ok(schedule_not_found(&id));
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
        .map_err(|error| internal_server_error("enqueue run", error))?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "schedule_id": claimed.schedule_id,
        "enqueued_at": now
    })))
}

pub(in crate::server::handlers::agent::schedules) fn normalize_optional_name(
    value: Option<&str>,
) -> Option<String> {
    value
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::normalize_optional_name;

    #[test]
    fn normalize_optional_name_trims_non_empty_values() {
        let normalized = normalize_optional_name(Some("  hourly sweep  "));
        assert_eq!(normalized.as_deref(), Some("hourly sweep"));
    }

    #[test]
    fn normalize_optional_name_drops_blank_values() {
        let normalized = normalize_optional_name(Some("   "));
        assert_eq!(normalized, None);
    }
}
