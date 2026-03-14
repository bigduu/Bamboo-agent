use actix_web::{web, HttpResponse};

use crate::server::app_state::AppState;
use crate::server::model_config_helper::get_default_model_from_config;
use crate::server::schedules::store::ScheduleRunConfig;

pub(super) fn validate_schedule_name(name: &str) -> Result<String, HttpResponse> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "name is required"
        })));
    }
    Ok(trimmed.to_string())
}

pub(super) fn validate_create_interval_seconds(interval_seconds: u64) -> Result<(), HttpResponse> {
    if interval_seconds == 0 {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "interval_seconds must be > 0"
        })));
    }
    Ok(())
}

pub(super) async fn validate_auto_execute_run_config(
    state: &web::Data<AppState>,
    run_config: &ScheduleRunConfig,
) -> Result<(), HttpResponse> {
    if !run_config.auto_execute {
        return Ok(());
    }

    let has_task = run_config
        .task_message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    if !has_task {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "run_config.task_message is required when auto_execute is true"
        })));
    }

    let has_explicit_model = run_config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    if has_explicit_model {
        return Ok(());
    }

    let snapshot = state.config.read().await.clone();
    if let Err(error) = get_default_model_from_config(&snapshot) {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": format!(
                "run_config.model not provided and no default model configured for provider {}: {}",
                snapshot.provider, error
            )
        })));
    }

    Ok(())
}
