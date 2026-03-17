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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_schedule_name_accepts_valid_name() {
        let result = validate_schedule_name("Daily Backup");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Daily Backup");
    }

    #[test]
    fn validate_schedule_name_trims_whitespace() {
        let result = validate_schedule_name("  Weekly Report  ");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Weekly Report");
    }

    #[test]
    fn validate_schedule_name_rejects_empty_string() {
        let result = validate_schedule_name("");
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedule_name_rejects_whitespace_only() {
        let result = validate_schedule_name("   ");
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedule_name_rejects_tabs_only() {
        let result = validate_schedule_name("\t\t");
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedule_name_rejects_newlines_only() {
        let result = validate_schedule_name("\n\n");
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedule_name_rejects_mixed_whitespace() {
        let result = validate_schedule_name(" \t\n ");
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedule_name_accepts_single_character() {
        let result = validate_schedule_name("A");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "A");
    }

    #[test]
    fn validate_schedule_name_accepts_numbers() {
        let result = validate_schedule_name("Schedule 123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Schedule 123");
    }

    #[test]
    fn validate_schedule_name_accepts_special_characters() {
        let result = validate_schedule_name("Backup (Daily) - v2.0!");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Backup (Daily) - v2.0!");
    }

    #[test]
    fn validate_schedule_name_accepts_unicode() {
        let result = validate_schedule_name("任务计划 🎯");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "任务计划 🎯");
    }

    #[test]
    fn validate_create_interval_seconds_accepts_positive_values() {
        assert!(validate_create_interval_seconds(1).is_ok());
        assert!(validate_create_interval_seconds(60).is_ok());
        assert!(validate_create_interval_seconds(3600).is_ok());
        assert!(validate_create_interval_seconds(86400).is_ok());
        assert!(validate_create_interval_seconds(u64::MAX).is_ok());
    }

    #[test]
    fn validate_create_interval_seconds_rejects_zero() {
        let result = validate_create_interval_seconds(0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_create_interval_seconds_accepts_minimum_value() {
        // 1 second is the minimum allowed
        assert!(validate_create_interval_seconds(1).is_ok());
    }

    #[test]
    fn validate_schedule_name_preserves_internal_whitespace() {
        let result = validate_schedule_name("Multi  Word  Name");
        assert!(result.is_ok());
        // Note: trim() only removes leading/trailing whitespace, not internal
        assert_eq!(result.unwrap(), "Multi  Word  Name");
    }

    #[test]
    fn validate_schedule_name_accepts_long_names() {
        let long_name = "A".repeat(1000);
        let result = validate_schedule_name(&long_name);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1000);
    }
}
